"""
Google Ads `TargetingResolver` — translates platform-neutral cohort signals
into Google Display targeting (geo_target_constants + audience segments).

Output shape consumed by `GoogleAdsClient.create_campaign(..., targeting=...)`:

  {
    "geo_targets":        [resource_name, ...],   # countries
    "audience_segments":  [resource_name, ...],   # in-market / affinity
    "demographics":       {"education": [...]},
  }

The Google Ads API doesn't have a `targetingsearch` analog as forgiving as
Meta's. We use:
  - `GeoTargetConstantService.suggest_geo_target_constants` for country
    lookups (cached).
  - For audience segments, we look up curated in-market / affinity segments
    by their canonical name (cached on disk). The full segment taxonomy is
    queryable via `audience_finder_service` but we keep a small allowlist
    for v1 to avoid overshooting.
"""
from __future__ import annotations

import json
import logging
import re
from pathlib import Path
from typing import Any

import config
from src.targeting_resolver import TargetingResolver

log = logging.getLogger(__name__)


_CACHE_PATH = Path("data/google_segment_cache.json")


# Curated allowlist mapping cohort signal terms → Google in-market /
# affinity segment IDs. Filled on demand. The names map to Google's
# canonical taxonomy strings; segment_resource_name is filled at runtime
# via search and cached.
#
# v1 covers the most common Outlier cohorts. v2 will expand to a larger
# taxonomy via `AudienceInsightsService.list_audience_insights_attributes`.
_DEFAULT_AFFINITY_KEYWORDS = {
    # term : [segment search hints]
    "software engineer": ["Technology Influencers", "Software Developers"],
    "data scientist":    ["Technology Early Adopters", "Data Analytics"],
    "machine learning":  ["Technology Early Adopters"],
    "python":            ["Technology Early Adopters"],
    "javascript":        ["Technology Early Adopters"],
    "physician":         ["Healthcare & Medical Services"],
    "nurse":             ["Healthcare & Medical Services"],
    "lawyer":            ["Legal Services"],
    "teacher":           ["Education"],
}


# Reuse the same degree → Meta education map but emit Google's enum values.
# Google's CriterionTypeEnum has UserInterest (audience) only; demographic
# education is not directly addressable on Display, so for v1 we drop the
# education facet here and rely on audience segments + content keywords.


class GoogleSegmentResolver(TargetingResolver):
    """Translate cohort signals into a Google Ads targeting payload."""

    name = "google"

    def __init__(
        self,
        client_id:        str | None = None,
        client_secret:    str | None = None,
        developer_token:  str | None = None,
        refresh_token:    str | None = None,
        customer_id:      str | None = None,
        cache_path:       Path | None = None,
    ):
        self._client_id        = client_id        or config.GOOGLE_ADS_CLIENT_ID
        self._client_secret    = client_secret    or config.GOOGLE_ADS_CLIENT_SECRET
        self._developer_token  = developer_token  or config.GOOGLE_ADS_DEVELOPER_TOKEN
        self._refresh_token    = refresh_token    or config.GOOGLE_ADS_REFRESH_TOKEN
        self._customer_id      = customer_id      or config.GOOGLE_ADS_CUSTOMER_ID
        self._cache_path       = cache_path       or _CACHE_PATH
        self._cache: dict[str, list[str]] = {}
        self._cache_loaded = False
        self._client = None

    # ── Public API ───────────────────────────────────────────────────────────

    def resolve_cohort(
        self,
        cohort: Any,
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        rules = list(getattr(cohort, "rules", None) or [])
        out: dict[str, Any] = {
            "geo_targets":       self._resolve_geos(geos or []),
            "audience_segments": [],
            "demographics":      {},
        }

        # Aggregate signal terms from cohort rules
        terms: list[str] = []
        for feat, val in rules:
            if feat.startswith("skills__") or feat.startswith("job_titles_norm__"):
                term = self._human_value(feat, val)
                if term:
                    terms.append(term)

        seen_segments: set[str] = set()
        for term in terms:
            for seg_resource in self._resolve_audience(term):
                if seg_resource not in seen_segments:
                    seen_segments.add(seg_resource)
                    out["audience_segments"].append(seg_resource)
                    if len(out["audience_segments"]) >= 5:
                        break
            if len(out["audience_segments"]) >= 5:
                break

        log.info(
            "Google targeting resolved: cohort=%s geos=%d segments=%d",
            getattr(cohort, "name", "?"),
            len(out["geo_targets"]),
            len(out["audience_segments"]),
        )
        return out

    # ── Geo lookup w/ on-disk cache ──────────────────────────────────────────

    def _resolve_geos(self, geos: list[str]) -> list[str]:
        """Map ISO country codes (e.g. "US", "CA") to Google geo_target_constant
        resource names (e.g. `geoTargetConstants/2840`). Cached by ISO code."""
        if not geos:
            return []
        self._load_cache()
        out: list[str] = []
        missing: list[str] = []
        for iso in geos:
            key = f"geo:{iso.upper()}"
            cached = self._cache.get(key)
            if cached:
                out.extend(cached)
            else:
                missing.append(iso)
        if missing:
            try:
                resolved = self._lookup_geos_via_api(missing)
                for iso, resources in resolved.items():
                    self._cache[f"geo:{iso.upper()}"] = resources
                    out.extend(resources)
                self._save_cache()
            except Exception as exc:
                log.warning("Google geo lookup failed (%s) — proceeding with cached only", exc)
        return out

    def _lookup_geos_via_api(self, isos: list[str]) -> dict[str, list[str]]:
        client = self._ensure_client()
        gtc_service = client.get_service("GeoTargetConstantService")
        out: dict[str, list[str]] = {}
        for iso in isos:
            try:
                req = client.get_type("SuggestGeoTargetConstantsRequest")
                req.country_code = iso.upper()
                req.locale = "en"
                resp = gtc_service.suggest_geo_target_constants(request=req)
                resources = []
                for sugg in resp.geo_target_constant_suggestions[:1]:
                    resources.append(sugg.geo_target_constant.resource_name)
                out[iso] = resources
            except Exception as exc:
                log.warning("Google geo suggest for %s failed: %s", iso, exc)
                out[iso] = []
        return out

    # ── Audience segment lookup ──────────────────────────────────────────────

    def _resolve_audience(self, term: str) -> list[str]:
        """Map a cohort signal term to Google audience segment resource names."""
        self._load_cache()
        key = f"aud:{term.lower()}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # v1: light-touch — we look up the curated keyword hints; if no hint,
        # we skip rather than spam the audience-finder API.
        hints = _DEFAULT_AFFINITY_KEYWORDS.get(term.lower(), [])
        if not hints:
            self._cache[key] = []
            self._save_cache()
            return []

        try:
            resources = self._search_user_interest(hints)
        except Exception as exc:
            log.warning("Google audience search for %r failed: %s", term, exc)
            resources = []
        self._cache[key] = resources
        self._save_cache()
        return resources

    def _search_user_interest(self, hints: list[str]) -> list[str]:
        client = self._ensure_client()
        ga_service = client.get_service("GoogleAdsService")
        out: list[str] = []
        for hint in hints:
            # Escape single quotes in the hint to avoid GAQL injection. Google
            # Ads doesn't support parameterized queries — we filter by exact name.
            safe = hint.replace("'", "\\'")
            query = (
                "SELECT user_interest.resource_name, user_interest.name "
                "FROM user_interest "
                f"WHERE user_interest.name = '{safe}'"
            )
            stream = ga_service.search_stream(
                customer_id=str(self._customer_id).replace("-", ""),
                query=query,
            )
            for batch in stream:
                for row in batch.results:
                    out.append(row.user_interest.resource_name)
                    break  # One match per hint keeps the audience focused.
                if out:
                    break
            if out:
                break
        return out

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        creds = {
            "developer_token":  self._developer_token,
            "refresh_token":    self._refresh_token,
            "client_id":        self._client_id,
            "client_secret":    self._client_secret,
            "use_proto_plus":   True,
        }
        login_id = getattr(config, "GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
        if login_id:
            creds["login_customer_id"] = str(login_id).replace("-", "")
        from google.ads.googleads.client import GoogleAdsClient as _SDKClient
        self._client = _SDKClient.load_from_dict(creds)
        return self._client

    def _load_cache(self) -> None:
        if self._cache_loaded:
            return
        if self._cache_path.exists():
            try:
                self._cache = json.loads(self._cache_path.read_text())
            except Exception as exc:
                log.warning("Could not load Google segment cache (%s) — starting fresh", exc)
                self._cache = {}
        self._cache_loaded = True

    def _save_cache(self) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(self._cache, indent=2))
        except Exception as exc:
            log.warning("Could not write Google segment cache: %s", exc)

    @staticmethod
    def _human_value(feature_col: str, fallback: str) -> str:
        if "__" in feature_col:
            tail = feature_col.split("__", 1)[1]
            return re.sub(r"[_]+", " ", tail).strip()
        return (fallback or "").strip()

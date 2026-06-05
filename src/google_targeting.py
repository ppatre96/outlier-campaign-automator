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
# 2026-05-24 — the old `_DEFAULT_AFFINITY_KEYWORDS` curated dict is gone.
# It had 9 hardcoded entries and made `_resolve_audience()` return [] for
# every cohort signal that wasn't one of those 9 strings — which was all
# of them in practice (cohort signals are "deep learning", "video editing",
# "applied mathematics", etc. — not "software engineer"/"physician"/etc.).
#
# New flow: cohort signal term → KeywordPlanIdeaService.generate_keyword_ideas()
# → returns Google's keyword expansion (related concepts derived from real
# search data) → user_interest search uses LIKE %term% (partial match, not
# exact) so the user_interest taxonomy doesn't need to match Google's
# wording byte-for-byte.

# Hard fallback when the keyword-idea API itself fails. Lets the Display
# arm degrade gracefully to its prior behaviour on auth/quota errors.
_FALLBACK_AFFINITY_KEYWORDS = {
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

# Baseline Display audience layer for cohorts that produce no skill/title
# signal terms — chiefly the generalist/i18n LOCALE cohorts (their only rule
# is ("interface_locale", …), which yields zero terms, so the Display ad group
# would otherwise ship geo-only with no audience targeting at all). The name
# resolves via the live user_interest LIKE lookup (verified 2026-06-05:
# "Business Professionals" → userInterests/92913 AFFINITY). Affinity is
# EMPLOYMENT special-ad-category SAFE (Google permits affinity + in-market
# under Employment; only age/gender/parental/marital + ZIP + remarketing/
# custom/lookalike are prohibited). Kept to the single clean on-target affinity
# — broader hints like "Employment" also LIKE-match noise ("Labor & Employment
# Law" vertical), which mis-targets the generalist contributor audience.
_GENERALIST_AUDIENCE_HINTS = ["Business Professionals"]


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
            "keyword_ideas":     [],   # 2026-05-24 — populated for Search arm
            # 2026-05-25 — sum of avg_monthly_searches across the cohort's
            # keyword pool. Used as the Search arm's "reach estimate" since
            # Google's user_interest taxonomy doesn't cover most technical
            # cohort terms (java / deep learning / quantum physics return 0
            # audience_segments). Not a literal audience size — closest
            # signal Google provides for keyword-targeted campaigns.
            "keyword_volume_estimate": 0,
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
        seen_keywords: set[str] = set()
        for term in terms:
            # Audience segments (Display arm — capped at 5 to keep targeting focused)
            for seg_resource in self._resolve_audience(term):
                if seg_resource not in seen_segments:
                    seen_segments.add(seg_resource)
                    out["audience_segments"].append(seg_resource)
                    if len(out["audience_segments"]) >= 5:
                        break
            # Keyword ideas (Search arm — collect up to 30 per cohort, capped
            # downstream by _apply_keyword_criteria). Include the bare cohort
            # term as the first keyword so the most-specific intent always
            # gets a slot.
            for kw in [term] + self._generate_keyword_ideas(term, max_ideas=12):
                kw_norm = (kw or "").strip().lower()
                if kw_norm and kw_norm not in seen_keywords:
                    seen_keywords.add(kw_norm)
                    out["keyword_ideas"].append(kw)
                    if len(out["keyword_ideas"]) >= 30:
                        break
            if len(out["audience_segments"]) >= 5 and len(out["keyword_ideas"]) >= 30:
                break

        # Generalist/i18n locale targeting (Bug 2). The synthetic
        # ("interface_locale", …) rule yields no cohort terms, so seed the
        # Search arm with localized generic keywords + a campaign language
        # constant. Reviewers refine the keywords via the console keyword card.
        _gen_locale = (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale")
        if _gen_locale:
            from src.locales import get_locale
            _lt = get_locale(_gen_locale)
            if _lt:
                if _lt.google_language_const is not None:
                    out["language_constant"] = f"languageConstants/{_lt.google_language_const}"
                for kw in _lt.generic_keywords:
                    kw_norm = (kw or "").strip().lower()
                    if kw_norm and kw_norm not in seen_keywords and len(out["keyword_ideas"]) < 30:
                        seen_keywords.add(kw_norm)
                        out["keyword_ideas"].append(kw)

        # Display audience baseline. Generalist-locale cohorts (and any cohort
        # whose signals didn't resolve to a single segment) would otherwise
        # ship the Display ad group geo-only — no audience layer. Seed a broad
        # EMPLOYMENT-safe professional affinity + in-market layer so the RDA
        # actually targets working professionals in-locale. Resolved by name
        # via the live user_interest lookup; best-effort (empty on API failure).
        if not out["audience_segments"]:
            try:
                baseline = self._search_user_interest(_GENERALIST_AUDIENCE_HINTS)
            except Exception as exc:
                log.warning("Google baseline audience lookup failed (%s) — geo-only", exc)
                baseline = []
            for seg in baseline:
                if seg not in seen_segments:
                    seen_segments.add(seg)
                    out["audience_segments"].append(seg)
                    if len(out["audience_segments"]) >= 5:
                        break

        # Sum keyword-volume signal across all cohort terms. Reads from the
        # cache populated by _generate_keyword_ideas above (no extra API
        # call). Provides the Search arm's reach-estimate path with a
        # numeric signal even when audience_segments is empty.
        for term in terms:
            out["keyword_volume_estimate"] += self.get_keyword_volume_for_term(term)

        log.info(
            "Google targeting resolved: cohort=%s geos=%d segments=%d keywords=%d kw_volume=%d",
            getattr(cohort, "name", "?"),
            len(out["geo_targets"]),
            len(out["audience_segments"]),
            len(out["keyword_ideas"]),
            out["keyword_volume_estimate"],
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
        """Resolve ISO country codes to Google Ads `geoTargetConstants/<id>`
        resource names.

        Implementation note (2026-05-18): originally used
        `GeoTargetConstantService.suggest_geo_target_constants` with just
        `country_code`, but google-ads v25+ requires the `oneof query` to be
        populated (`location_names` OR `geo_targets`); setting only
        `country_code` fails with `REQUEST_PARAMETERS_UNSET`. Switched to a
        direct GAQL search against the `geo_target_constant` resource, which
        is the canonical pattern when you already have ISO codes and only
        care about country-level resources.
        """
        client = self._ensure_client()
        ga_service = client.get_service("GoogleAdsService")
        customer_id = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")
        # Target-type preference: 'Country' covers most ISO codes (US, GB,
        # MX, etc.); some entities Google models as 'Region' (e.g. Puerto
        # Rico, US Virgin Islands) — fall back to those when no Country row
        # exists, so we don't drop territories from targeting silently.
        type_priority = {"Country": 0, "Region": 1, "Territory": 2}
        out: dict[str, list[str]] = {}
        for iso in isos:
            iso_upper = iso.upper()
            try:
                query = (
                    "SELECT geo_target_constant.resource_name, "
                    "geo_target_constant.target_type "
                    "FROM geo_target_constant "
                    f"WHERE geo_target_constant.country_code = '{iso_upper}' "
                    "AND geo_target_constant.status = 'ENABLED' "
                    "AND geo_target_constant.target_type IN "
                    "('Country', 'Region', 'Territory')"
                )
                resp = ga_service.search(customer_id=customer_id, query=query)
                candidates: list[tuple[int, str]] = []
                for row in resp:
                    g = row.geo_target_constant
                    pri = type_priority.get(g.target_type, 99)
                    candidates.append((pri, g.resource_name))
                candidates.sort()
                resources = [candidates[0][1]] if candidates else []
                out[iso] = resources
                if not resources:
                    log.warning("Google geo lookup for %s returned no rows", iso)
            except Exception as exc:
                log.warning("Google geo search for %s failed: %s", iso, exc)
                out[iso] = []
        return out

    # ── Audience segment lookup ──────────────────────────────────────────────

    def _resolve_audience(self, term: str) -> list[str]:
        """Map a cohort signal term to Google audience segment resource names.

        Flow (2026-05-24):
          1. KeywordPlanIdeaService.generate_keyword_ideas(term) — Google
             returns the top related concepts derived from real search data
             (e.g. term='video editing' → ['voice over jobs', 'video editor
             jobs', 'adobe premiere training', ...]).
          2. For each keyword idea, query user_interest WHERE name LIKE
             '%idea%' (partial match) — the taxonomy doesn't need to match
             Google's wording byte-for-byte.
          3. Dedupe + cap at 5 resources. Cache per-term.

        Fallback chain on failure: keyword-idea API error → fall back to
        `_FALLBACK_AFFINITY_KEYWORDS` curated dict (9 entries) for the
        well-known terms; everything else returns [] gracefully.
        """
        self._load_cache()
        key = f"aud:{term.lower()}"
        cached = self._cache.get(key)
        if cached is not None:
            return cached

        # Step 1 — pull keyword ideas from Google
        try:
            ideas = self._generate_keyword_ideas(term, max_ideas=10)
        except Exception as exc:
            log.warning(
                "Google keyword-idea expansion for %r failed (%s) — falling "
                "back to the curated dict",
                term, exc,
            )
            ideas = []

        # Add the bare term itself as a starting hint — sometimes the LLM
        # cohort signal is already a Google-taxonomy match (e.g. "marketing").
        hints = [term] + ideas
        # Fallback shim for the 9 well-known terms when keyword-idea
        # expansion returned nothing.
        if not ideas:
            hints.extend(_FALLBACK_AFFINITY_KEYWORDS.get(term.lower(), []))

        try:
            resources = self._search_user_interest(hints)
        except Exception as exc:
            log.warning("Google user_interest search for %r failed: %s", term, exc)
            resources = []
        self._cache[key] = resources
        self._save_cache()
        return resources

    def _search_user_interest(self, hints: list[str]) -> list[str]:
        """Look up user_interest segments matching any of the hints.

        2026-05-24 — partial match (LIKE '%hint%') replaces exact match. The
        exact-match path was returning [] because Google's user_interest
        taxonomy names ("Software Developers", "Technology Early Adopters")
        rarely match cohort/keyword-idea strings byte-for-byte. LIKE catches
        a broader class — e.g. hint='video editor jobs' hits user_interest
        names containing 'video editor', 'video', or 'editor'.

        Caps at 5 resources total to avoid bloating ad-group targeting.
        """
        client = self._ensure_client()
        ga_service = client.get_service("GoogleAdsService")
        seen: set[str] = set()
        out: list[str] = []
        for hint in hints:
            if not (hint or "").strip():
                continue
            safe = hint.replace("'", "\\'").replace("%", "")  # strip GAQL LIKE wildcards
            query = (
                "SELECT user_interest.resource_name, user_interest.name "
                "FROM user_interest "
                f"WHERE user_interest.name LIKE '%{safe}%' "
                "LIMIT 5"
            )
            try:
                stream = ga_service.search_stream(
                    customer_id=str(self._customer_id).replace("-", ""),
                    query=query,
                )
                for batch in stream:
                    for row in batch.results:
                        rn = row.user_interest.resource_name
                        if rn and rn not in seen:
                            seen.add(rn)
                            out.append(rn)
                            if len(out) >= 5:
                                return out
            except Exception as exc:
                # Single-hint failure shouldn't kill the whole resolve; log
                # and try the next hint.
                log.debug("user_interest LIKE %r failed (%s) — trying next hint", hint, exc)
                continue
        return out

    def _generate_keyword_ideas(self, term: str, *, max_ideas: int = 10) -> list[str]:
        """Call KeywordPlanIdeaService for related search concepts.

        Returns the top `max_ideas` keyword phrases by avg_monthly_searches.
        Empty list on any failure (caller falls back to the curated dict).

        Cached per-term in the on-disk segment cache so both consumers
        (Display audience resolution + Search keyword criteria) reuse one
        API call.

        Cost: 1 Google Ads API call per UNIQUE cohort signal term. Typical
        ramp has 3 cohorts × 3-5 signal terms = ~12 calls per ramp once,
        then served from cache on re-runs.
        """
        if not (term or "").strip():
            return []
        self._load_cache()
        key = f"kwideas:{term.lower()}"
        cached = self._cache.get(key)
        if cached is not None:
            # 2026-05-25: cache format is now `[[volume, text], ...]` pairs.
            # Old caches were `["text", ...]` — handle both shapes so this
            # change doesn't invalidate the on-disk cache. Return just text
            # strings to keep the public signature unchanged.
            if cached and isinstance(cached[0], (list, tuple)) and len(cached[0]) == 2:
                return [text for _, text in cached[:max_ideas]]
            return cached[:max_ideas]

        client = self._ensure_client()
        svc = client.get_service("KeywordPlanIdeaService")
        request = client.get_type("GenerateKeywordIdeasRequest")
        request.customer_id = str(self._customer_id).replace("-", "")
        # English + US baseline — keyword-idea expansion needs language +
        # geo to anchor relevance. Audience targeting itself remains scoped
        # by the per-campaign geo_target_constants from resolve_cohort.
        request.language = "languageConstants/1000"   # English
        request.geo_target_constants.append("geoTargetConstants/2840")  # US
        request.include_adult_keywords = False
        request.keyword_seed.keywords.append(term)

        try:
            response = svc.generate_keyword_ideas(request=request)
        except Exception as exc:
            log.warning("Google keyword-idea expansion for %r failed: %s", term, exc)
            self._cache[key] = []
            self._save_cache()
            return []

        candidates: list[tuple[int, str]] = []
        for idea in response:
            text = (idea.text or "").strip()
            if not text or text.lower() == term.lower():
                continue
            volume = int(getattr(idea.keyword_idea_metrics, "avg_monthly_searches", 0) or 0)
            candidates.append((volume, text))

        # Highest-volume first; volume==0 still allowed but ranked last.
        # Cache the full pool as (volume, text) pairs so the Search arm's
        # reach-estimate path can read volumes without a second API call.
        # Stored as nested lists for JSON-safety; readback handles tuples too.
        candidates.sort(key=lambda x: -x[0])
        pool_pairs = [[vol, text] for vol, text in candidates[:30]]
        self._cache[key] = pool_pairs
        self._save_cache()
        pool_texts = [text for _, text in candidates[:30]]
        log.info(
            "Google keyword-ideas for %r → %d ideas (top: %s)",
            term, len(pool_texts), pool_texts[:3],
        )
        return pool_texts[:max_ideas]

    def get_keyword_volume_for_term(self, term: str) -> int:
        """Sum of avg_monthly_searches across the cached keyword-idea pool
        for `term`. Used by the Search arm's reach-estimate path as the
        closest equivalent to "audience size" for keyword targeting.

        Reads from the on-disk cache populated by `_generate_keyword_ideas`.
        Returns 0 when the term hasn't been resolved yet OR when the cache
        format is the legacy strings-only shape (predates the volume
        cache 2026-05-25). Caller should treat 0 as "no signal" — not
        "audience is zero".
        """
        if not (term or "").strip():
            return 0
        self._load_cache()
        cached = self._cache.get(f"kwideas:{term.lower()}")
        if not cached or not isinstance(cached, list):
            return 0
        # Legacy strings-only cache → no volume signal available
        if cached and not isinstance(cached[0], (list, tuple)):
            return 0
        total = 0
        for pair in cached:
            try:
                total += int(pair[0])
            except (TypeError, ValueError, IndexError):
                continue
        return total

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

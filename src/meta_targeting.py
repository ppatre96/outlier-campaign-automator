"""
Meta Ads `TargetingResolver` — translates platform-neutral cohort signals
into Meta detailed-targeting payloads (interests + demographics + geo).

The pipeline produces cohort.rules as `[(feature_col, value), ...]` —
e.g. `("skills__python", "python")`, `("job_titles_norm__data_scientist",
"data scientist")`. Meta doesn't have a URN-based skill taxonomy; instead,
its detailed-targeting API exposes ~tens-of-thousands of interest/behavior
audiences indexed by string. We use the Marketing API's `targetingsearch`
endpoint to look up matching interest IDs for each cohort signal.

Results are cached to `data/meta_interest_cache.json` so repeat ramps don't
hammer the API.

Output shape (Meta `Targeting` object):

  {
    "geo_locations":    {"countries": ["US", "CA", ...]},
    "flexible_spec":    [{"interests": [{"id": "...", "name": "..."}, ...]}],
    "education_statuses": [4, 5, 6],   # bachelor / master / phd
    "age_min": 21,
    "age_max": 65,
  }

When `config.SPECIAL_AD_CATEGORY == "EMPLOYMENT"` the resolver omits age
and gender (Meta enforces — narrow demographic targeting is blocked under
Employment).
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


_CACHE_PATH = Path("data/meta_interest_cache.json")

# Map raw degree feature names to Meta `education_statuses` enum codes.
# Reference: https://developers.facebook.com/docs/marketing-api/audiences/reference/advanced-targeting
# 1 HS, 2 Some College, 3 Associate, 4 Bachelor, 5 Master, 6 PhD, 7 Some HS,
# 8 HS Grad, 9 Some Grad School, 10 In College, 11 In Grad School,
# 12 Unspecified, 13 Professional Degree
_DEGREE_EDU_MAP = {
    "highest_degree_level__bachelors":  [4],
    "highest_degree_level__bachelor":   [4],
    "highest_degree_level__masters":    [5],
    "highest_degree_level__master":     [5],
    "highest_degree_level__phd":        [6],
    "highest_degree_level__doctorate":  [6],
    "highest_degree_level__doctoral":   [6],
}


class MetaInterestResolver(TargetingResolver):
    """Translate cohort signals into a Meta `Targeting` dict.

    Lazy initialisation — the facebook_business SDK is only imported when
    `resolve_cohort()` is called, so importing this module is cheap.
    """

    name = "meta"

    def __init__(
        self,
        access_token: str | None = None,
        api_version:  str | None = None,
        cache_path:   Path | None = None,
    ):
        self._access_token = access_token or config.META_ACCESS_TOKEN
        self._api_version  = api_version  or config.META_API_VERSION
        self._cache_path   = cache_path   or _CACHE_PATH
        self._cache: dict[str, list[dict]] = {}
        self._cache_loaded = False
        self._initialized  = False

    # ── Public API ───────────────────────────────────────────────────────────

    def resolve_cohort(
        self,
        cohort: Any,
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        rules: list[tuple] = list(getattr(cohort, "rules", None) or [])
        out: dict[str, Any] = {
            "geo_locations": {"countries": list(geos or [])},
        }

        # Demographics: education from degree features.
        edu_codes: list[int] = []
        for feat, _val in rules:
            if feat in _DEGREE_EDU_MAP:
                edu_codes.extend(_DEGREE_EDU_MAP[feat])
        if edu_codes:
            out["education_statuses"] = sorted(set(edu_codes))

        # Interests: skill names + job titles → Meta interest IDs.
        interest_terms = []
        for feat, val in rules:
            if feat.startswith("skills__") or feat.startswith("job_titles_norm__"):
                term = self._human_value(feat, val)
                if term:
                    interest_terms.append(term)
        # De-dup while preserving order.
        seen: set[str] = set()
        interests: list[dict[str, str]] = []
        for term in interest_terms:
            if term.lower() in seen:
                continue
            seen.add(term.lower())
            for hit in self._lookup_interests(term):
                interests.append({"id": hit["id"], "name": hit["name"]})
                # One match per term keeps the audience focused; widening would
                # add false positives from Meta's fuzzy lookup.
                break

        if interests:
            out["flexible_spec"] = [{"interests": interests}]

        # Default age range — gated by SPECIAL_AD_CATEGORY rules.
        category = (config.SPECIAL_AD_CATEGORY or "NONE").upper()
        if category != "EMPLOYMENT":
            out["age_min"] = 21
            out["age_max"] = 65
        # Under EMPLOYMENT/HOUSING/CREDIT: skip age + gender (Meta enforces).

        log.info(
            "Meta targeting resolved: cohort=%s geos=%d interests=%d edu=%s",
            getattr(cohort, "name", "?"),
            len(out["geo_locations"]["countries"]),
            len(interests),
            edu_codes or "none",
        )
        return out

    # ── Interest lookup w/ on-disk cache ─────────────────────────────────────

    def _lookup_interests(self, term: str) -> list[dict]:
        """Return Meta interest matches for a single search term, cached."""
        self._load_cache()
        key = term.strip().lower()
        if key in self._cache:
            return self._cache[key]
        try:
            self._ensure_init()
            from facebook_business.adobjects.targetingsearch import TargetingSearch
            results = TargetingSearch.search(params={
                "q":       term,
                "type":    TargetingSearch.TargetingSearchTypes.interest,
                "limit":   3,
            })
            hits = []
            for r in results:
                hits.append({
                    "id":          str(r.get("id", "")),
                    "name":        r.get("name", ""),
                    "audience_size": r.get("audience_size", None),
                })
            self._cache[key] = hits
            self._save_cache()
            return hits
        except Exception as exc:
            log.warning("Meta targetingsearch failed for %r: %s", term, exc)
            self._cache[key] = []
            return []

    # ── Helpers ──────────────────────────────────────────────────────────────

    def _ensure_init(self) -> None:
        if self._initialized:
            return
        from facebook_business.api import FacebookAdsApi
        FacebookAdsApi.init(
            access_token=self._access_token,
            api_version=self._api_version or "v21.0",
        )
        self._initialized = True

    def _load_cache(self) -> None:
        if self._cache_loaded:
            return
        if self._cache_path.exists():
            try:
                self._cache = json.loads(self._cache_path.read_text())
            except Exception as exc:
                log.warning("Could not load Meta interest cache (%s) — starting fresh", exc)
                self._cache = {}
        self._cache_loaded = True

    def _save_cache(self) -> None:
        try:
            self._cache_path.parent.mkdir(parents=True, exist_ok=True)
            self._cache_path.write_text(json.dumps(self._cache, indent=2))
        except Exception as exc:
            log.warning("Could not write Meta interest cache: %s", exc)

    @staticmethod
    def _human_value(feature_col: str, fallback: str) -> str:
        """Extract the human-readable signal from a binary feature column.

        e.g. `skills__python` → "python"; `job_titles_norm__data_scientist`
        → "data scientist". Falls back to the raw `value` column when the
        feature has no `__` suffix.
        """
        if "__" in feature_col:
            tail = feature_col.split("__", 1)[1]
            return re.sub(r"[_]+", " ", tail).strip()
        return (fallback or "").strip()

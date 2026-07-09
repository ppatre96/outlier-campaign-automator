"""
TikTok targeting resolver.

Translates a platform-neutral cohort + geo list into a TikTok ad-group
targeting payload that `TikTokClient.create_campaign` consumes. No network
call — geo maps to TikTok's numeric location IDs via a static table (TikTok
does NOT accept ISO codes; it uses GeoNames-style country IDs), age/gender are
config-driven. Interest/behavior category targeting is left broad in v1 (adding
it needs a TikTok category-ID lookup — tracked as a fast-follow); the ad group
still optimizes toward the pixel conversion event, so broad + conversion
optimization is a sound starting point (mirrors the Meta broad-control ad set).

Returned dict schema (all keys optional; omitted → TikTok default / broad):
  {
    "location_ids":          [int, ...],   # TikTok numeric location IDs
    "age_groups":            [str, ...],   # AGE_18_24 … AGE_55_100
    "genders":               str,          # GENDER_UNLIMITED
    "interest_category_ids": [str, ...],   # (empty in v1)
    "languages":             [str, ...],   # (empty in v1 → broad)
    "unmapped_geos":         [str, ...],   # ISO codes we couldn't map (for logs)
  }
"""
from __future__ import annotations

import logging
from typing import Any

import config
from src.targeting_resolver import TargetingResolver

log = logging.getLogger(__name__)


# ISO-3166 alpha-2 → TikTok location ID (GeoNames country IDs, which TikTok's
# Marketing API uses for country-level `location_ids`). Covers Outlier's main
# acquisition markets; extend as new geos ship. Unmapped geos are dropped from
# targeting (→ broader reach) and surfaced in `unmapped_geos` for the logs.
_ISO_TO_TIKTOK_LOCATION: dict[str, int] = {
    "US": 6252001,   "GB": 2635167,   "CA": 6251999,   "AU": 2077456,
    "IE": 2963597,   "NZ": 2186224,   "IN": 1269750,   "PK": 1168579,
    "BD": 1210997,   "PH": 1694008,   "NG": 2328926,   "KE": 192950,
    "ZA": 953987,    "DE": 2921044,   "FR": 3017382,   "ES": 2510769,
    "IT": 3175395,   "BR": 3469034,   "MX": 3996063,   "AR": 3865483,
    "ID": 1643084,   "MY": 1733045,   "VN": 1562822,   "TH": 1605651,
    "JP": 1861060,   "KR": 1835841,   "TR": 298795,    "EG": 357994,
    "PL": 798544,    "NL": 2750405,   "SE": 2661886,   "PT": 2264397,
}

# Adult age buckets (exclude 13-17). Outlier contributors are 18+.
_ADULT_AGE_GROUPS = ["AGE_18_24", "AGE_25_34", "AGE_35_44", "AGE_45_54", "AGE_55_100"]


class TikTokTargetingResolver(TargetingResolver):
    """Cohort → TikTok ad-group targeting. Config-driven, no network."""

    name = "tiktok"

    def resolve_cohort(
        self,
        cohort: Any,
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        location_ids: list[int] = []
        unmapped: list[str] = []
        for g in (geos or []):
            iso = (g or "").strip().upper()
            if not iso:
                continue
            loc = _ISO_TO_TIKTOK_LOCATION.get(iso)
            if loc:
                location_ids.append(loc)
            else:
                unmapped.append(iso)
        if unmapped:
            log.warning(
                "tiktok resolver: no TikTok location ID for %s — dropped from "
                "targeting (broader reach). Add to _ISO_TO_TIKTOK_LOCATION.",
                ", ".join(sorted(set(unmapped))),
            )

        out: dict[str, Any] = {
            "age_groups": list(_ADULT_AGE_GROUPS),
            "genders":    "GENDER_UNLIMITED",
            "interest_category_ids": [],
            "languages":  [],
            "unmapped_geos": sorted(set(unmapped)),
        }
        if location_ids:
            out["location_ids"] = sorted(set(location_ids))
        return out

"""Smart Ramp v2 — pipe-delimited campaign naming convention.

Generates names that match the spec at
  https://genai-smart-ramp-v2.vercel.app/ramps/<ramp_id>/campaigns

Format (12 segments separated by " | "):

    Scale-<ramp_id>
    | <Channel>                                 # LinkedIn | Meta | Google
    | <type of CBs>                             # specialist | generalist | coders | languages
    | <field requirement>                       # e.g. "Finance & Quantitative Analysis"
    | <locale>                                  # e.g. en-US
    | <country>                                 # ISO-2 e.g. US
    | <geo-tier>                                # HCC | ALL | Region | Individual | Main Country
    | <LI facets>                               # comma-joined human-readable facets
    | <Language>                                # display language e.g. EN
    | <Format>                                  # Single Image | Inmail
    | <ramp date>                               # MM/DD/YYYY
    | Agent                                     # literal

Example: `Scale-GMR-0020 | LinkedIn | specialist | Finance & Quantitative Analysis | en-US | US | HCC | PHDs | EN | Inmail | 05/11/2026 | Agent`

Maintainers: Smart Ramp may add new naming fields; keep `build_campaign_name`
backwards-compatible by defaulting missing fields to safe strings rather than
breaking the create-campaign path.
"""
from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Optional

log = logging.getLogger(__name__)


# Smart Ramp `campaign_state.linkedin.groupingType` → display label per spec.
_GROUPING_TYPE_LABEL = {
    "cost_country":  "HCC",
    "none":          "ALL",
    "region":        "Region",
    "individual":    "Individual",
    "main_country":  "Main Country",
}

# Fallback "type of CB" mapping when `cohort.job_post_pod` is missing. Maps the
# classify_tg() output (DATA_ANALYST/ML_ENGINEER/MEDICAL/LANGUAGE/...) to the
# 4 pod buckets the Smart Ramp spec uses.
_POD_FROM_TG = {
    "SOFTWARE_ENGINEER": "coders",
    "LANGUAGE":          "languages",
    "GENERAL":           "generalist",
    "DATA_ANALYST":      "specialist",
    "ML_ENGINEER":       "specialist",
    "MATH":              "specialist",
    "MEDICAL":           "specialist",
}

# Channel display names per spec.
_CHANNEL_LABEL = {"linkedin": "LinkedIn", "meta": "Meta", "google": "Google"}

# Format display names per spec.
_FORMAT_LABEL_FROM_CAMPAIGN_TYPE = {"static": "Single Image", "inmail": "Inmail"}


def _safe(value: Any, default: str = "") -> str:
    """Coerce to a pipe-safe string (no internal " | " sequences)."""
    s = str(value or "").strip()
    # Replace embedded pipes — they'd break downstream parsers.
    return s.replace(" | ", " / ").replace("|", "/") or default


def _format_date_mdy(submitted_at: str) -> str:
    """Smart Ramp `submittedAt` is ISO-8601; spec wants MM/DD/YYYY."""
    if not submitted_at:
        return ""
    for fmt in ("%Y-%m-%dT%H:%M:%S.%fZ", "%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%d"):
        try:
            dt = datetime.strptime(submitted_at, fmt)
            return dt.strftime("%m/%d/%Y")
        except ValueError:
            continue
    # Last-resort: assume the first 10 chars are YYYY-MM-DD.
    try:
        dt = datetime.strptime(submitted_at[:10], "%Y-%m-%d")
        return dt.strftime("%m/%d/%Y")
    except ValueError:
        log.warning("Could not parse submitted_at=%r as date", submitted_at)
        return ""


def _grouping_label(grouping_type: Optional[str]) -> str:
    return _GROUPING_TYPE_LABEL.get((grouping_type or "").lower(), "ALL")


def _pod_label(cohort: Any, pod_override: Optional[str]) -> str:
    """Prefer Smart Ramp's `job_post_pod`; fall back to classify_tg() output."""
    if pod_override:
        return _safe(pod_override).lower() or "generalist"
    # Late import to avoid circular dep (figma_creative ↔ campaign_name).
    try:
        from src.figma_creative import classify_tg
        tg = classify_tg(getattr(cohort, "name", ""), getattr(cohort, "rules", []) or [])
        return _POD_FROM_TG.get((tg or "").upper(), "generalist")
    except Exception:
        return "generalist"


def _li_state(campaign_state: Optional[dict]) -> dict:
    """Pull campaign_state.linkedin if available, else {}."""
    state = campaign_state or {}
    if not isinstance(state, dict):
        return {}
    li = state.get("linkedin") or {}
    return li if isinstance(li, dict) else {}


def build_campaign_name(
    *,
    ramp_id: str = "",
    submitted_at: str = "",
    cohort=None,                  # resolved/data-mined cohort (for classify_tg fallback)
    geo_group=None,               # GeoCampaignGroup — for country fallback
    platform: str = "linkedin",   # "linkedin" | "meta" | "google"
    campaign_type: str = "static",  # "static" | "inmail"
    format_override: Optional[str] = None,
    # ── Smart Ramp metadata (per-row, sourced from CohortSpec / row dict) ────
    pod: Optional[str] = None,           # job_post_pod
    domain: Optional[str] = None,        # matched_domain
    locale: Optional[str] = None,        # job_post_language_code
    included_geos: Optional[list] = None,  # row-level geos (fallback for country)
    campaign_state: Optional[dict] = None,  # full campaign_state dict for liTargetingFacet etc.
) -> str:
    """Build a pipe-delimited campaign name per Smart Ramp v2 spec.

    Falls back to safe defaults when Smart Ramp metadata is missing — name
    creation must NOT block campaign creation. Returns the joined string.
    """
    li_state = _li_state(campaign_state)

    # Segment 1 — Scale-<ramp_id>
    seg_ramp = f"Scale-{_safe(ramp_id, 'GMR-XXXX')}"

    # Segment 2 — channel
    seg_channel = _CHANNEL_LABEL.get(platform.lower(), platform.title())

    # Segment 3 — type of CB
    seg_pod = _pod_label(cohort, pod)

    # Segment 4 — field requirement (Smart Ramp matched_domain, e.g. "Finance & Quantitative Analysis")
    seg_field = _safe(domain, "General")

    # Segment 5 — locale (en-US style)
    seg_locale = _safe(locale, "en-US")

    # Segment 6 — country (ISO-2). Prefer Smart Ramp's main_country, else first geo.
    main_country = (li_state.get("mainCountry") or "").strip()
    if not main_country:
        geos = list(included_geos or [])
        if not geos and geo_group is not None:
            geos = list(getattr(geo_group, "geos", []) or [])
        main_country = geos[0] if geos else ""
    seg_country = _safe(main_country.upper(), "US")

    # Segment 7 — geo-tier label
    seg_geo_tier = _grouping_label(li_state.get("groupingType"))

    # Segment 8 — LI facets (comma-joined string from Smart Ramp, e.g. "PHDs, MBAs")
    seg_facets = _safe(li_state.get("liTargetingFacet"), "")

    # Segment 9 — language code for ad copy (e.g. EN)
    seg_language = _safe(li_state.get("liAdLanguage"), seg_locale.split("-")[0].upper())

    # Segment 10 — ad format
    if format_override:
        seg_format = _safe(format_override, "Single Image")
    else:
        seg_format = _FORMAT_LABEL_FROM_CAMPAIGN_TYPE.get(campaign_type.lower(), "Single Image")

    # Segment 11 — ramp date (MM/DD/YYYY)
    seg_date = _format_date_mdy(submitted_at or "")

    # Segment 12 — literal "Agent"
    seg_agent = "Agent"

    parts = [
        seg_ramp, seg_channel, seg_pod, seg_field, seg_locale,
        seg_country, seg_geo_tier, seg_facets, seg_language, seg_format,
        seg_date, seg_agent,
    ]
    return " | ".join(parts)

"""Smart Ramp v2 — pipe-delimited campaign naming convention.

Generates names that match the spec at
  https://genai-smart-ramp-v2.vercel.app/ramps/<ramp_id>/campaigns

Format (8 segments separated by " | ", matching the Smart Ramp tool's
generated campaign name; InMail appends a 9th " | Inmail" segment so the two
LinkedIn arms stay distinct):

    Scale-<ramp_id>
    | <Channel>                                 # LinkedIn | Meta | Google
    | <pod>                                      # job_post_pod, e.g. "language"
    | <domain>                                   # job_post_domain, e.g. "bn-IN"
    | <locale>                                   # job_post_language_code, e.g. "bn-IN"
    | <country>                                  # ISO-2 from locale region, e.g. IN
    | <run date>                                 # MM/DD/YYYY (today)
    | <geo-tier>                                 # ALL | HCC | Region | Individual | Main Country

Example: `Scale-GMR-0023 | LinkedIn | language | bn-IN | bn-IN | IN | 06/04/2026 | ALL`
         (InMail: same + ` | Inmail`)

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
    "ACCESSIBILITY":     "specialist",
}

# Channel display names per spec.
_CHANNEL_LABEL = {"linkedin": "LinkedIn", "meta": "Meta", "google": "Google", "reddit": "Reddit"}

# Format display names per spec.
_FORMAT_LABEL_FROM_CAMPAIGN_TYPE = {"static": "Single Image", "inmail": "Inmail"}


_EMPTY = "—"


def _safe(value: Any, default: str = _EMPTY) -> str:
    """Coerce to a pipe-safe string (no internal " | " sequences). Empty
    values render as "—" (em-dash) per the marketing team's convention in
    Slack examples (Bryan's GMR-0016 utm_campaign uses "—" for unused
    segments)."""
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

    # Segment 2 — channel (LinkedIn | Meta | Google)
    seg_channel = _CHANNEL_LABEL.get(platform.lower(), platform.title())

    # Segment 3 — pod / type of CB (Smart Ramp job_post_pod, e.g. "language")
    seg_pod = _pod_label(cohort, pod)

    # Segment 4 — domain (Smart Ramp job_post_domain, e.g. "bn-IN")
    seg_domain = _safe(domain, "General")

    # Segment 5 — locale (Smart Ramp job_post_language_code, e.g. "bn-IN")
    seg_locale = _safe(locale, "en-US")

    # Segment 6 — country (ISO-2). The Smart Ramp tool uses the locale's region
    # (bn-IN → IN), falling back to campaign_state.mainCountry then first geo.
    country = ""
    if locale and "-" in str(locale):
        country = str(locale).split("-")[-1].strip().upper()
    if not country:
        country = (li_state.get("mainCountry") or "").strip().upper()
    if not country:
        geos = list(included_geos or [])
        if not geos and geo_group is not None:
            geos = list(getattr(geo_group, "geos", []) or [])
        country = (geos[0].upper() if geos else "")
    seg_country = _safe(country, "US")

    # Run date (MM/DD/YYYY), i.e. today (when the campaign is built).
    seg_date = datetime.now().strftime("%m/%d/%Y")

    # Geo-tier label (ALL when no grouping override).
    seg_geo_tier = _grouping_label(li_state.get("groupingType"))

    # LinkedIn — full Smart Ramp v2 order, with the channel-manager fields
    # (liTargetingFacet / liAdLanguage / liAdFormat) the Smart Ramp tool puts in
    # the name and the run date LAST. All sourced from campaign_state.linkedin —
    # authoritative, never invented (reviewer feedback GMR-0024, 2026-06-11):
    #   Scale-<ramp> | LinkedIn | <pod> | <domain> | <locale> | <country>
    #   | <geo-tier> | <facet> | <lang> | <format> | <date>
    # e.g. Scale-GMR-0024 | LinkedIn | specialist | Media & Communications
    #      | en-US | US | ALL | BLV | EN | Message ads | 06/11/2026
    if platform.lower() == "linkedin":
        # Language: liAdLanguage (uppercase, e.g. "EN"); fall back to the
        # locale's language subtag (en-US → EN) when the channel manager left
        # it blank.
        ad_lang = (li_state.get("liAdLanguage") or "").strip().upper()
        if not ad_lang and locale and "-" in str(locale):
            ad_lang = str(locale).split("-")[0].strip().upper()
        seg_lang = _safe(ad_lang, "EN")

        # Format: groups carry the override marker; leaf InMail prefers
        # liAdFormat ("Message ads"); leaf static stays "Single Image".
        if format_override:
            seg_format = format_override
        elif campaign_type.lower() == "inmail":
            seg_format = (li_state.get("liAdFormat") or "").strip() or "Message ads"
        else:
            seg_format = "Single Image"

        parts = [
            seg_ramp, seg_channel, seg_pod, seg_domain, seg_locale,
            seg_country, seg_geo_tier,
        ]
        facet = (li_state.get("liTargetingFacet") or "").strip()
        if facet:
            parts.append(_safe(facet))
        parts += [seg_lang, seg_format, seg_date]
        return " | ".join(parts)

    # Meta / Google — unchanged legacy 8-segment order (date before geo-tier);
    # these channels don't carry the LinkedIn channel-manager facet/lang fields.
    parts = [
        seg_ramp, seg_channel, seg_pod, seg_domain, seg_locale,
        seg_country, seg_date, seg_geo_tier,
    ]
    name = " | ".join(parts)
    fmt = (format_override or _FORMAT_LABEL_FROM_CAMPAIGN_TYPE.get(campaign_type.lower(), "")).strip()
    if campaign_type.lower() == "inmail" or fmt.lower() == "inmail":
        name = f"{name} | Inmail"
    return name

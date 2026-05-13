"""UTM URL builder — matches the format the marketing team uses in Slack.

Reference examples from the team (#growth-marketing-internal):

  GMR-0005 (Diego):
    https://outlier.ai/experts/law-sgp
      ?utm_source=LinkedIn
      &utm_medium=paid
      &pod=specialist
      &domain=law
      &locale=US
      &utm_campaign=Scale-GMR-0005_LinkedIn_specialist_Law_en-US_US_SG_FieldofStudy_EN_Message_04/08/2026
      &language=EN

  GMR-0016 (Bryan):
    https://outlier.ai/experts/cardiology-ctrl
      ?utm_source=LinkedIn
      &utm_medium=paid
      &pod=specialist
      &domain=clinical_medicine
      &locale=US
      &utm_campaign=Scale-GMR-0016 | LinkedIn | specialist | Clinical Medicine | en-US | US | ALL | EXP-CTRL | — | — | 04/20/2026
      &language=EN
      &utm_content=cardiology-ctrl-static
      &utm_concept=Clinical Acquisition - Control ($150/hr)

This module produces URLs of that shape. The base URL comes from Smart Ramp's
`campaign_state.utm_<channel>.<field>` when filled in; otherwise falls back to
`config.LINKEDIN_DESTINATION` so the pipeline never blocks on a missing LP.
"""
from __future__ import annotations

import logging
from typing import Optional
from urllib.parse import quote

log = logging.getLogger(__name__)


_PLATFORM_TO_UTM_SOURCE = {
    "linkedin": "LinkedIn",
    "meta":     "Facebook",   # Meta Ads sources tag as Facebook on the LP side
    "google":   "Google",
}


def _domain_param(domain: str) -> str:
    """Lowercase + underscore-join to match the marketing team's convention
    (e.g. "Clinical Medicine" → "clinical_medicine")."""
    return (domain or "").strip().lower().replace(" & ", "_").replace(" ", "_") or "general"


def _enc(value: str) -> str:
    """URL-encode keeping the pipe-character segment dividers in the
    utm_campaign value (Slack examples leave them as `%7C`)."""
    return quote(str(value or ""), safe="")


def build_utm_url(
    *,
    base_url: str,
    platform: str,           # "linkedin" | "meta" | "google"
    campaign_name: str,      # full pipe-delimited Smart Ramp v2 spec
    pod: Optional[str] = None,
    domain: Optional[str] = None,
    locale: Optional[str] = None,   # "en-US" → maps to locale=US (country part only)
    language: Optional[str] = None, # "EN"
    utm_content: Optional[str] = None,
    utm_concept: Optional[str] = None,
) -> str:
    """Build the full UTM-laden destination URL.

    `base_url` is the LP slug (e.g. `https://outlier.ai/experts/arxiv-v2`).
    Pass the same `campaign_name` you used to name the campaign — that string
    becomes the `utm_campaign` value, so the marketing team's downstream
    attribution joins on it cleanly.
    """
    if not base_url:
        log.warning("build_utm_url: empty base_url, returning empty string")
        return ""

    # locale=US (country part), not full en-US (Diego's example shows `locale=US`).
    locale_country = ""
    if locale:
        parts = locale.split("-")
        locale_country = parts[1].upper() if len(parts) > 1 else parts[0].upper()

    params: list[tuple[str, str]] = [
        ("utm_source", _PLATFORM_TO_UTM_SOURCE.get(platform.lower(), platform.title())),
        ("utm_medium", "paid"),
    ]
    if pod:
        params.append(("pod", pod))
    if domain:
        params.append(("domain", _domain_param(domain)))
    if locale_country:
        params.append(("locale", locale_country))
    params.append(("utm_campaign", campaign_name))
    if language:
        params.append(("language", language))
    if utm_content:
        params.append(("utm_content", utm_content))
    if utm_concept:
        params.append(("utm_concept", utm_concept))

    query = "&".join(f"{k}={_enc(v)}" for k, v in params)
    sep = "&" if "?" in base_url else "?"
    return f"{base_url}{sep}{query}"


def resolve_base_lp_url(
    *,
    campaign_state: Optional[dict],
    platform: str,
    fallback: str,
    matched_domain: Optional[str] = None,
) -> str:
    """Pull the per-cohort landing page URL.

    Resolution order:
      1. `campaign_state.utm_<channel>.<base_url|url|...>` — when the marketing
         team fills this in via Smart Ramp.
      2. `matched_domain` → LP slug via `config.LP_URL_BY_DOMAIN`. Defaults
         seeded with the slugs the team shared for GMR-0020 (qfinance / ml / cs).
      3. `fallback` (typically `config.LINKEDIN_DESTINATION`).

    Smart Ramp uses `utm_joveo` for the Google bucket; mapped here.
    """
    # 1) Smart Ramp campaign_state — preferred when filled
    if isinstance(campaign_state, dict):
        channel_key = "utm_joveo" if platform == "google" else f"utm_{platform}"
        block = campaign_state.get(channel_key)
        if isinstance(block, dict):
            for k in ("base_url", "url", "lp_url", "landing_page"):
                v = (block.get(k) or "").strip()
                if v:
                    return v

    # 2) Domain → LP slug map (config-driven, env-overridable)
    if matched_domain:
        try:
            import config as _cfg
            lp_map = getattr(_cfg, "LP_URL_BY_DOMAIN", {}) or {}
            v = lp_map.get(matched_domain)
            if v:
                return v
        except Exception:
            pass

    return fallback

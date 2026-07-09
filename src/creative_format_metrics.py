"""Meta creative-format (video vs static) daily delivery.

Powers the Analytics dashboard's "Creative Format" panel. The warehouse has no
creative-format tag for Meta (issue #94) and activations aren't format-
attributable (AD_ID ~1.6% populated, issue #95), so this is DELIVERY-ONLY
(impressions / clicks / spend) and sourced straight from the Meta Marketing API
— the only place creative type lives reliably.

For each delivering ad in the window we read its creative object to classify
video vs static (object_type / video_id / object_story_spec.video_data),
parse the ramp id + language from the campaign name, and aggregate to
(ramp × language × format × day) in meta_creative_format_daily.

Entry point: build_meta_creative_format_daily(window_days).
"""
from __future__ import annotations

import collections
import logging
import re
from datetime import datetime, timedelta, timezone

import config

log = logging.getLogger(__name__)

_RAMP_RE = re.compile(r"(GMR-\d{3,4})", re.IGNORECASE)

# locale/language token in the campaign name → display language
_LANG_TOKENS = {
    "Bengali": ("bn-in", "bn_in", "bengali"),
    "German":  ("de-de", "de_de", "german"),
    "French":  ("fr-fr", "fr_fr", "french"),
    "Thai":    ("th-th", "th_th", "thai"),
    "Korean":  ("ko-kr", "ko_kr", "korean"),
    "Hindi":   ("hi-in", "hi_in", "hindi"),
    "Spanish": ("es-mx", "es-es", "es_mx", "spanish"),
    "Vietnamese": ("vi-vn", "vi_vn", "vietnamese"),
    "Japanese": ("ja-jp", "ja_jp", "japanese"),
}


def _lang_of(name: str) -> str:
    n = (name or "").lower()
    for lang, toks in _LANG_TOKENS.items():
        if any(t in n for t in toks):
            return lang
    return ""


def _ramp_of(name: str) -> str:
    m = _RAMP_RE.search(name or "")
    return m.group(1).upper() if m else ""


def build_meta_creative_format_daily(window_days: int = 30) -> int:
    """Pull Meta ad-level daily delivery, classify each ad's creative format,
    and upsert (ramp × language × format × day) rows. Returns rows written.
    No-op (returns 0) without Meta creds. Best-effort — never raises."""
    if not (config.META_ACCESS_TOKEN and config.META_AD_ACCOUNT_ID):
        log.info("meta creative-format: skipped — META creds not set")
        return 0
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.adcreative import AdCreative
        from facebook_business.adobjects.ad import Ad
    except Exception as exc:  # noqa: BLE001
        log.warning("meta creative-format: facebook_business unavailable: %s", exc)
        return 0

    acct_id = config.META_AD_ACCOUNT_ID
    if acct_id and not acct_id.startswith("act_"):
        acct_id = f"act_{acct_id}"
    since = (datetime.now(timezone.utc) - timedelta(days=int(window_days))).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    try:
        FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                            api_version=config.META_API_VERSION or "v21.0")
        rows = AdAccount(acct_id).get_insights(params={
            "time_range": {"since": since, "until": until},
            "level": "ad",
            "time_increment": 1,   # one row per ad per day
            "fields": ["ad_id", "campaign_name", "spend", "impressions", "clicks"],
            "limit": 500,
        })
    except Exception as exc:  # noqa: BLE001
        log.warning("meta creative-format: insights fetch failed: %s", exc)
        return 0

    _fmt_cache: dict[str, str] = {}

    def _classify(ad_id: str) -> str:
        if ad_id in _fmt_cache:
            return _fmt_cache[ad_id]
        fmt = "static"
        try:
            cid = (Ad(ad_id).api_get(fields=["creative"]).get("creative") or {}).get("id")
            if cid:
                c = AdCreative(cid).api_get(fields=["object_type", "video_id", "object_story_spec"])
                if c.get("object_type") == "VIDEO" or c.get("video_id") \
                   or (c.get("object_story_spec") or {}).get("video_data"):
                    fmt = "video"
        except Exception as exc:  # noqa: BLE001
            log.debug("meta creative-format: classify %s failed: %s", ad_id, exc)
            fmt = "unknown"
        _fmt_cache[ad_id] = fmt
        return fmt

    agg: dict[tuple, dict] = collections.defaultdict(
        lambda: {"impressions": 0, "clicks": 0, "spend_usd": 0.0})
    for r in rows:
        ramp = _ramp_of(r.get("campaign_name", ""))
        lang = _lang_of(r.get("campaign_name", ""))
        if not ramp or not lang:
            continue   # only ramp+language campaigns feed the per-ramp panel
        fmt = _classify(r["ad_id"])
        if fmt == "unknown":
            continue
        day = r.get("date_start") or r.get("date_stop")
        key = (ramp, lang, fmt, day)
        a = agg[key]
        a["impressions"] += int(float(r.get("impressions") or 0))
        a["clicks"] += int(float(r.get("clicks") or 0))
        a["spend_usd"] += float(r.get("spend") or 0)

    batch = [
        {"ramp_id": ramp, "language": lang, "creative_format": fmt, "metric_date": day,
         "impressions": v["impressions"], "clicks": v["clicks"], "spend_usd": round(v["spend_usd"], 2)}
        for (ramp, lang, fmt, day), v in agg.items()
    ]
    from src.ui_decisions import upsert_meta_creative_format_batch
    written = upsert_meta_creative_format_batch(batch)
    log.info("meta creative-format: wrote %d (ramp×lang×format×day) rows (window=%dd, %d ads classified)",
             written, window_days, len(_fmt_cache))
    return written

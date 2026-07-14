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


def _action_val(lst) -> float:
    """Sum the value(s) of a Meta 'actions'-style field (list of
    {action_type, value}). Video-engagement fields return one video_view entry."""
    total = 0.0
    if isinstance(lst, (list, tuple)):
        for a in lst:
            try:
                total += float(a.get("value", 0) or 0)
            except (TypeError, ValueError, AttributeError):
                pass
    return total


def _action_by_type(actions, atype: str) -> int:
    """Sum the value of one action_type from a Meta `actions` list."""
    total = 0
    if isinstance(actions, (list, tuple)):
        for a in actions:
            if isinstance(a, dict) and a.get("action_type") == atype:
                try:
                    total += int(float(a.get("value") or 0))
                except (TypeError, ValueError):
                    pass
    return total


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
            "fields": ["ad_id", "campaign_name", "spend", "impressions", "clicks",
                       "video_play_actions", "video_thruplay_watched_actions",
                       "video_p25_watched_actions", "video_p50_watched_actions",
                       "video_p75_watched_actions", "video_p100_watched_actions",
                       "video_avg_time_watched_actions", "actions"],
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

    _VZERO = {"video_plays": 0, "video_thruplays": 0, "video_p25": 0, "video_p50": 0,
              "video_p75": 0, "video_p100": 0, "video_watch_seconds": 0, "video_3sec": 0,
              "reactions": 0, "comments": 0, "shares": 0, "saves": 0}
    agg: dict[tuple, dict] = collections.defaultdict(
        lambda: {"impressions": 0, "clicks": 0, "spend_usd": 0.0, **dict(_VZERO)})
    for r in rows:
        ramp = _ramp_of(r.get("campaign_name", ""))
        lang = _lang_of(r.get("campaign_name", ""))
        if not ramp or not lang:
            continue   # only ramp+language campaigns feed the per-ramp panel
        fmt = _classify(r["ad_id"])
        if fmt == "unknown":
            continue
        day = r.get("date_start") or r.get("date_stop")
        a = agg[(ramp, lang, fmt, day)]
        a["impressions"] += int(float(r.get("impressions") or 0))
        a["clicks"] += int(float(r.get("clicks") or 0))
        a["spend_usd"] += float(r.get("spend") or 0)
        # video engagement (0 for static ads — fields simply absent)
        plays = _action_val(r.get("video_play_actions"))
        a["video_plays"] += int(plays)
        a["video_thruplays"] += int(_action_val(r.get("video_thruplay_watched_actions")))
        a["video_p25"] += int(_action_val(r.get("video_p25_watched_actions")))
        a["video_p50"] += int(_action_val(r.get("video_p50_watched_actions")))
        a["video_p75"] += int(_action_val(r.get("video_p75_watched_actions")))
        a["video_p100"] += int(_action_val(r.get("video_p100_watched_actions")))
        # avg watch time is per-ad seconds → total seconds = avg * plays (weighted later)
        a["video_watch_seconds"] += int(_action_val(r.get("video_avg_time_watched_actions")) * plays)
        # 3-second views (hook) live in the actions array as video_view
        acts = r.get("actions")
        a["video_3sec"] += _action_by_type(acts, "video_view")
        # social engagement (both formats) from the actions list
        a["reactions"] += _action_by_type(acts, "post_reaction")
        a["comments"] += _action_by_type(acts, "comment")
        a["shares"] += _action_by_type(acts, "post")               # post = shares
        a["saves"] += _action_by_type(acts, "onsite_conversion.post_save")

    batch = [
        {"ramp_id": ramp, "language": lang, "creative_format": fmt, "metric_date": day,
         "impressions": v["impressions"], "clicks": v["clicks"], "spend_usd": round(v["spend_usd"], 2),
         **{k: v[k] for k in _VZERO}}
        for (ramp, lang, fmt, day), v in agg.items()
    ]
    from src.ui_decisions import upsert_meta_creative_format_batch
    written = upsert_meta_creative_format_batch(batch)
    log.info("meta creative-format: wrote %d (ramp×lang×format×day) rows (window=%dd, %d ads classified)",
             written, window_days, len(_fmt_cache))
    return written


# ── live ad-set targeting for video ads (exact params set on Meta) ──────────────
def _ad_is_video(ad_id: str, cache: dict) -> bool:
    """True when an ad's creative is a video (cached)."""
    if ad_id in cache:
        return cache[ad_id]
    v = False
    try:
        from facebook_business.adobjects.ad import Ad
        from facebook_business.adobjects.adcreative import AdCreative
        cid = (Ad(ad_id).api_get(fields=["creative"]).get("creative") or {}).get("id")
        if cid:
            c = AdCreative(cid).api_get(fields=["object_type", "video_id", "object_story_spec"])
            v = (c.get("object_type") == "VIDEO" or bool(c.get("video_id"))
                 or bool((c.get("object_story_spec") or {}).get("video_data")))
    except Exception as exc:  # noqa: BLE001
        log.debug("meta targeting: classify %s failed: %s", ad_id, exc)
    cache[ad_id] = v
    return v


def _targeting_components(data: dict) -> dict:
    """Pull the human-relevant knobs out of one ad-set targeting spec."""
    gl = data.get("geo_locations") or {}
    geo = set(gl.get("countries") or [])
    for k in ("regions", "cities"):
        geo.update(x["name"] for x in (gl.get(k) or []) if isinstance(x, dict) and x.get("name"))
    interests = set()
    for spec in data.get("flexible_spec") or []:
        for grp in ("interests", "behaviors", "life_events", "industries",
                    "work_positions", "education_majors"):
            interests.update(x["name"] for x in (spec.get(grp) or [])
                             if isinstance(x, dict) and x.get("name"))
    interests.update(x["name"] for x in (data.get("interests") or [])
                     if isinstance(x, dict) and x.get("name"))
    custom = {x["name"] for x in (data.get("custom_audiences") or [])
              if isinstance(x, dict) and x.get("name")}
    excluded = {x["name"] for x in (data.get("excluded_custom_audiences") or [])
                if isinstance(x, dict) and x.get("name")}
    genders = data.get("genders")
    gtxt = "Men" if genders == [1] else "Women" if genders == [2] else "All genders"
    return {"geo": geo, "age_min": data.get("age_min"), "age_max": data.get("age_max"),
            "genders": gtxt, "interests": interests, "custom": custom, "excluded": excluded,
            "advantage": bool((data.get("targeting_automation") or {}).get("advantage_audience"))}


def _render_targeting(comps: list[dict]) -> str:
    """Fold one-or-more ad-set component dicts into one readable summary."""
    geo, interests, custom, excluded, genders = set(), set(), set(), set(), set()
    amins, amaxs, advantage = [], [], False
    for c in comps:
        geo |= c["geo"]; interests |= c["interests"]; custom |= c["custom"]; excluded |= c["excluded"]
        genders.add(c["genders"]); advantage = advantage or c["advantage"]
        if c["age_min"]:
            amins.append(c["age_min"])
        if c["age_max"]:
            amaxs.append(c["age_max"])
    parts = [f"{len(comps)} ad set{'s' if len(comps) != 1 else ''}"]
    if geo:
        parts.append("Geo: " + ", ".join(sorted(geo)))
    if amins and amaxs:
        parts.append(f"Age {min(amins)}–{max(amaxs)}")
    parts.append(next(iter(genders)) if len(genders) == 1 else "All genders")
    if interests:
        parts.append("Interests: " + ", ".join(sorted(interests)))
    if custom:
        parts.append("Custom/Lookalike: " + ", ".join(sorted(custom)))
    if advantage:
        parts.append("Advantage+ audience ON")
    if excluded:
        parts.append(f"Excludes {len(excluded)} audience(s)")
    return " · ".join(parts)


def fetch_meta_video_targeting(window_days: int = 30) -> dict:
    """Return {(ramp, language): targeting summary} — the EXACT targeting set on
    the ad sets that contain delivering video ads, read live from the Meta API.
    Best-effort: returns {} without creds or on failure."""
    if not (config.META_ACCESS_TOKEN and config.META_AD_ACCOUNT_ID):
        return {}
    try:
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.adset import AdSet
    except Exception as exc:  # noqa: BLE001
        log.warning("meta targeting: facebook_business unavailable: %s", exc)
        return {}
    acct_id = config.META_AD_ACCOUNT_ID
    if acct_id and not acct_id.startswith("act_"):
        acct_id = f"act_{acct_id}"
    since = (datetime.now(timezone.utc) - timedelta(days=int(window_days))).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()
    try:
        FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                            api_version=config.META_API_VERSION or "v21.0")
        rows = AdAccount(acct_id).get_insights(params={
            "time_range": {"since": since, "until": until}, "level": "ad",
            "fields": ["ad_id", "adset_id", "campaign_name"], "limit": 500})
    except Exception as exc:  # noqa: BLE001
        log.warning("meta targeting: insights fetch failed: %s", exc)
        return {}

    vid_cache: dict = {}
    adsets_by_key: dict[tuple, set] = collections.defaultdict(set)
    for r in rows:
        ramp = _ramp_of(r.get("campaign_name", ""))
        lang = _lang_of(r.get("campaign_name", ""))
        adset_id = r.get("adset_id")
        if not (ramp and lang and adset_id):
            continue
        if _ad_is_video(r["ad_id"], vid_cache):
            adsets_by_key[(ramp, lang)].add(adset_id)

    out: dict = {}
    tcache: dict = {}
    for key, adset_ids in adsets_by_key.items():
        comps = []
        for aid in adset_ids:
            if aid not in tcache:
                try:
                    tg = AdSet(aid).api_get(fields=["targeting"]).get("targeting")
                    data = tg.export_all_data() if hasattr(tg, "export_all_data") else dict(tg or {})
                    tcache[aid] = _targeting_components(data)
                except Exception as exc:  # noqa: BLE001
                    log.debug("meta targeting: adset %s fetch failed: %s", aid, exc)
                    tcache[aid] = None
            if tcache[aid]:
                comps.append(tcache[aid])
        if comps:
            out[key] = _render_targeting(comps)
    log.info("meta targeting: resolved %d (ramp×language) video cohorts", len(out))
    return out

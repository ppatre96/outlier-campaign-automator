"""Reddit + YouTube video-ad delivery, keyed by ramp id.

Companion to `creative_format_metrics` (Meta). The agency's video creatives go
live on Reddit and Google/YouTube under campaigns named with the same
`GMR-####` ramp convention Meta uses; these builders pull each platform's
reporting API, keep only video campaigns that carry a ramp id, parse ramp +
locale from the campaign name, and aggregate to per-(ramp × locale) rows the
agency Google Sheet consumes directly (no persistence — the video program is
new, so a wide look-back window covers lifetime-to-date).

Campaigns that pre-date the GMR naming convention have no ramp id and are
intentionally skipped (no backfill). Both builders are best-effort: gated on
the platform's creds/flag and never raise — they return [] on any failure.

Metric mapping into the shared sheet columns (`m` keys, see the sheet's
_row_cells): unsupported metrics are left None → rendered blank, never faked.
  Reddit  : plays=video_started, v3=video_watched_3_seconds, thru=video_fully_viewed,
            p25..p100=video_watched_XX_percent. No avg-watch-seconds / social.
  YouTube : impressions, clicks, and quartile counts (rate × impressions) only.
            No 3-sec / ThruPlay / plays / avg-watch — YouTube's model differs
            (quartiles are % of impressions, not of plays), so those stay blank.
"""
from __future__ import annotations

import logging

import config
from src.creative_format_metrics import _lang_of, _ramp_of

log = logging.getLogger(__name__)

# Metric keys the sheet's `m` dict understands. None = "not supported by this
# channel" → blank cell (distinct from 0 = "supported, measured zero").
_M_KEYS = ("imp", "plays", "v3", "thru", "p25", "p50", "p75", "p100",
           "ws", "clk", "spend", "rx", "cm", "sh", "sv")


def _locale_of(name: str) -> str:
    return _lang_of(name) or "(unspecified)"


def _blank_m() -> dict:
    return {k: None for k in _M_KEYS}


def _fold(entries: list[dict]) -> list[dict]:
    """Aggregate per-(ramp × locale) rows. `entries` are dicts with ramp_id,
    channel, locale, metric_date, and a raw `m` (values may be None)."""
    agg: dict[tuple, dict] = {}
    for e in entries:
        key = (e["ramp_id"], e["locale"])
        cur = agg.get(key)
        if cur is None:
            cur = {"ramp_id": e["ramp_id"], "channel": e["channel"], "locale": e["locale"],
                   "lang": e.get("lang", e["locale"]),
                   "launched": e["metric_date"], "last": e["metric_date"], "days": set(),
                   "m": _blank_m()}
            agg[key] = cur
        cur["launched"] = min(cur["launched"], e["metric_date"])
        cur["last"] = max(cur["last"], e["metric_date"])
        cur["days"].add(e["metric_date"])
        for k, v in e["m"].items():
            if v is None:
                continue
            cur["m"][k] = (cur["m"][k] or 0) + v
    out = []
    for c in agg.values():
        c["days"] = len(c["days"])
        out.append(c)
    return out


# ── Reddit ────────────────────────────────────────────────────────────────────
def _reddit_metrics(row: dict) -> dict:
    """Map one Reddit video report row → sheet `m` dict (pure, unit-tested)."""
    m = _blank_m()
    m["imp"] = int(row.get("impressions") or 0)
    m["clk"] = int(row.get("clicks") or 0)
    m["spend"] = float(row.get("spend") or 0) / 1_000_000
    m["plays"] = int(row.get("video_started") or 0)
    m["v3"] = int(row.get("video_watched_3_seconds") or 0)
    m["thru"] = int(row.get("video_watched_100_percent") or 0)  # Reddit "fully viewed" == 100%
    m["p25"] = int(row.get("video_watched_25_percent") or 0)
    m["p50"] = int(row.get("video_watched_50_percent") or 0)
    m["p75"] = int(row.get("video_watched_75_percent") or 0)
    m["p100"] = int(row.get("video_watched_100_percent") or 0)
    return m


def build_reddit_video_rows(window_days: int = 180) -> list[dict]:
    if not config.REDDIT_API_ENABLED:
        log.info("reddit video: skipped — REDDIT_API_ENABLED off")
        return []
    try:
        from src.reddit_api import RedditClient
        client = RedditClient()
        names = client.list_campaigns()
        report = client.fetch_video_metrics_daily(window_days=window_days)
    except Exception as exc:  # noqa: BLE001 — best-effort
        log.warning("reddit video: fetch failed (non-fatal): %s", exc)
        return []

    entries = []
    for r in report:
        if int(r.get("video_started") or 0) <= 0:
            continue  # not a video campaign that day
        name = names.get(r["campaign_id"], "")
        ramp = _ramp_of(name)
        if not ramp:
            continue  # pre-convention / non-ramp campaign — skip (no backfill)
        entries.append({"ramp_id": ramp, "channel": "Reddit", "locale": _locale_of(name),
                        "lang": _lang_of(name), "metric_date": r["metric_date"],
                        "m": _reddit_metrics(r)})
    rows = _fold(entries)
    log.info("reddit video: %d (ramp × locale) rows from %d report rows", len(rows), len(report))
    return rows


# ── YouTube (Google Ads video / demand-gen) ─────────────────────────────────────
_YT_QUERY = (
    "SELECT campaign.name, segments.date, metrics.impressions, metrics.clicks, "
    "metrics.video_quartile_p25_rate, metrics.video_quartile_p50_rate, "
    "metrics.video_quartile_p75_rate, metrics.video_quartile_p100_rate, "
    "metrics.cost_micros "
    "FROM campaign "
    "WHERE campaign.advertising_channel_type IN ('VIDEO','DEMAND_GEN') "
    "AND metrics.impressions > 0 "
    "AND segments.date BETWEEN '{since}' AND '{until}'"
)


def _youtube_metrics(impressions: int, clicks: int, cost_micros: int,
                     p25r: float, p50r: float, p75r: float, p100r: float) -> dict:
    """Map Google video metrics → sheet `m` dict (pure, unit-tested). Quartile
    rates are proportions of impressions → counts = round(rate × impressions).
    Play-denominated columns stay None (YouTube has no comparable metric)."""
    m = _blank_m()
    m["imp"] = int(impressions or 0)
    m["clk"] = int(clicks or 0)
    m["spend"] = int(cost_micros or 0) / 1_000_000
    m["p25"] = round((p25r or 0) * impressions)
    m["p50"] = round((p50r or 0) * impressions)
    m["p75"] = round((p75r or 0) * impressions)
    m["p100"] = round((p100r or 0) * impressions)
    return m


def build_youtube_video_rows(window_days: int = 180) -> list[dict]:
    if not (config.GOOGLE_ADS_DEVELOPER_TOKEN and config.GOOGLE_ADS_CUSTOMER_ID):
        log.info("youtube video: skipped — Google Ads creds not set")
        return []
    from datetime import datetime, timedelta, timezone
    until = datetime.now(timezone.utc).date()
    since = until - timedelta(days=int(window_days))
    try:
        creds = {"developer_token": config.GOOGLE_ADS_DEVELOPER_TOKEN,
                 "refresh_token": config.GOOGLE_ADS_REFRESH_TOKEN,
                 "client_id": config.GOOGLE_ADS_CLIENT_ID,
                 "client_secret": config.GOOGLE_ADS_CLIENT_SECRET,
                 "use_proto_plus": True}
        if config.GOOGLE_ADS_LOGIN_CUSTOMER_ID:
            creds["login_customer_id"] = str(config.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace("-", "")
        from google.ads.googleads.client import GoogleAdsClient
        sdk = GoogleAdsClient.load_from_dict(creds)
        svc = sdk.get_service("GoogleAdsService")
        cid = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")
        query = _YT_QUERY.format(since=since.isoformat(), until=until.isoformat())
        entries = []
        for batch in svc.search_stream(customer_id=cid, query=query):
            for row in batch.results:
                name = row.campaign.name or ""
                ramp = _ramp_of(name)
                if not ramp:
                    continue  # pre-convention / non-ramp campaign — skip (no backfill)
                mm = row.metrics
                m = _youtube_metrics(mm.impressions, mm.clicks, mm.cost_micros,
                                     mm.video_quartile_p25_rate, mm.video_quartile_p50_rate,
                                     mm.video_quartile_p75_rate, mm.video_quartile_p100_rate)
                entries.append({"ramp_id": ramp, "channel": "YouTube",
                                "locale": _locale_of(name), "lang": _lang_of(name),
                                "metric_date": str(row.segments.date), "m": m})
    except Exception as exc:  # noqa: BLE001 — best-effort
        log.warning("youtube video: fetch failed (non-fatal): %s", exc)
        return []
    rows = _fold(entries)
    log.info("youtube video: %d (ramp × locale) rows", len(rows))
    return rows

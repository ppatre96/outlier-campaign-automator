"""Daily campaign metrics — the day-over-day time-series behind the console's
Analytics dashboard (DoD activation / spend / click trend charts).

The registry only keeps a single CUMULATIVE snapshot per campaign; DoD charts
need per-day rows. This module builds them into the campaign_daily_metrics
Postgres table from two day-grained sources, both of which carry real history so
the dashboard has data immediately (not just going forward):

  • Funnel-by-day  — signups / screening / activations per (campaign × day) from
    SCALE_PROD.VIEW.APPLICATION_CONVERSION, attributed to APPLICATION_DAY. All
    channels (LinkedIn / Meta / Reddit via UTM, Google via CAMPAIGN/ADGROUP id).
  • Delivery-by-day — impressions / clicks / spend per (campaign × day) from each
    platform's reporting. v1 covers LinkedIn (AD_ANALYTICS_BY_CREATIVE.DAY, the
    biggest spender) + Meta (Insights time_increment=1). Google is billing-frozen
    ($0) and Reddit is tiny — their delivery-by-day is a follow-up; their
    funnel-by-day (activations) is already covered above.

Key unification: both passes key on ONE stable `campaign_key` per campaign
(canonical UTM for LinkedIn/Meta/Reddit, platform-id tail for Google), resolved
through the registry, so funnel + delivery for the same campaign land on the
same day-row. Entry point: build_daily_metrics(window_days).
"""
from __future__ import annotations

import logging
import math
import re
from datetime import datetime, timedelta, timezone

import config

log = logging.getLogger(__name__)

_UTM_CHANNELS = ("linkedin", "meta", "reddit", "tiktok")
_RAMP_RE = re.compile(r"(GMR-\d{3,4})", re.IGNORECASE)


def _ramp_of(s) -> str:
    m = _RAMP_RE.search(str(s or ""))
    return m.group(1).upper() if m else ""


def _num(v) -> float:
    """NaN/None-safe numeric coercion. pandas returns NaN for missing cells, and
    `nan or 0` evaluates to nan (NaN is truthy) — which then poisons SUM() as a
    NUMERIC 'NaN' in Postgres. Always return a real number."""
    try:
        f = float(v)
    except (TypeError, ValueError):
        return 0.0
    return 0.0 if math.isnan(f) else f


# ── Registry resolution ─────────────────────────────────────────────────────

class _Identity:
    __slots__ = ("ramp_id", "platform", "campaign_name", "campaign_key")

    def __init__(self, ramp_id, platform, campaign_name, campaign_key):
        self.ramp_id = ramp_id
        self.platform = platform
        self.campaign_name = campaign_name
        self.campaign_key = campaign_key


def _build_indexes():
    """Lookups over the registry so every pass resolves to the SAME campaign_key:
      • by_utm     — canonical UTM → identity (funnel li/meta).
      • by_id      — platform-id tail → identity (funnel google + ALL delivery).
      • reddit_rep — ramp → the ramp's representative reddit identity (highest
        impressions). Reddit's warehouse UTM collapses all geos to one "—|—|—"
        string and CAMPAIGN_ID is null, so reddit funnel can only be attributed
        at the ramp level; it lands on the ramp's main reddit campaign row (which
        also carries that campaign's delivery, so both coexist on one row).
    campaign_key = canonical UTM for UTM channels, id-tail for Google."""
    from src.campaign_registry import _canonical_utm, _id_tail
    from src.ui_decisions import list_all_campaign_data

    by_utm: dict[str, _Identity] = {}
    by_id: dict[str, _Identity] = {}
    reddit_rep: dict[str, _Identity] = {}
    reddit_rep_impr: dict[str, int] = {}
    for row in list_all_campaign_data():
        platform = (row.get("platform") or "").lower()
        if not platform or platform == "parent" or row.get("campaign_type") == "parent":
            continue
        ramp = row.get("smart_ramp_id") or ""
        name = row.get("campaign_name") or ""
        canon = _canonical_utm(row.get("utm_campaign") or name)
        idtail = _id_tail(row.get("platform_campaign_id") or "")
        key = canon if platform in _UTM_CHANNELS else idtail
        if not (ramp and key):
            continue
        ident = _Identity(ramp, platform, name, key)
        if canon:
            by_utm.setdefault(canon, ident)
        if idtail:
            by_id.setdefault(idtail, ident)
        if platform == "reddit":
            impr = int(row.get("impressions") or 0)
            if ramp not in reddit_rep or impr > reddit_rep_impr.get(ramp, -1):
                reddit_rep[ramp] = ident
                reddit_rep_impr[ramp] = impr
    return by_utm, by_id, reddit_rep


# ── Funnel-by-day (all channels) ────────────────────────────────────────────

def _backfill_funnel_daily(window_days, by_utm, by_id, reddit_rep) -> int:
    from src.campaign_registry import _canonical_utm, _id_tail
    from src.redash_db import RedashClient
    from src.ui_decisions import upsert_daily_metrics_batch

    client = RedashClient()
    batch: list[dict] = []
    # ad_key resolver differs by channel family.
    channels = [("linkedin", "utm"), ("meta", "utm"), ("reddit", "reddit"),
                ("tiktok", "utm"), ("google", "id"), ("google_adgroup", "id")]
    for chan, family in channels:
        try:
            df = client.query_campaign_funnel_daily(chan, days_back=window_days)
        except Exception as exc:  # noqa: BLE001
            log.warning("daily funnel %s query failed: %s", chan, exc)
            continue
        for _, r in df.iterrows():
            raw = str(r.get("ad_key") or "")
            if family == "utm":
                ident = by_utm.get(_canonical_utm(raw))
            elif family == "reddit":
                # Ramp-level: warehouse reddit UTM collapses geos + CAMPAIGN_ID
                # is null, so attribute to the ramp's representative reddit row.
                ident = reddit_rep.get(_ramp_of(raw))
            else:
                ident = by_id.get(_id_tail(raw))
            if ident is None:
                continue   # a conversion for a campaign not in our registry
            batch.append({
                "ramp_id": ident.ramp_id, "platform": ident.platform,
                "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
                "metric_date": r.get("metric_date"),
                "signups": int(r.get("applications") or 0),
                "screening_passes": int(r.get("screening_passes") or 0),
                "activations": int(r.get("activations") or 0),
            })
    written = upsert_daily_metrics_batch(batch, ["signups", "screening_passes", "activations"])
    log.info("daily funnel: wrote %d (campaign × day) rows", written)
    return written


# ── Delivery-by-day: LinkedIn (Redash) ──────────────────────────────────────

# CREATIVE_HISTORY + CAMPAIGN_HISTORY are *history* tables (many versioned rows
# per entity). Joining them raw fans out AD_ANALYTICS_BY_CREATIVE by the version
# count — a ~270× spend/click inflation. Dedup BOTH to their latest row (rn=1)
# before joining, per the fan-out rule (mirrors _METRICS_SQL in
# campaign_feedback_agent).
_LINKEDIN_DAILY_SQL = """
WITH cr AS (
    SELECT ID AS creative_id, CAMPAIGN_ID,
           ROW_NUMBER() OVER (PARTITION BY ID ORDER BY LAST_MODIFIED_AT DESC) AS rn
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CREATIVE_HISTORY
    WHERE ACCOUNT_ID = {account_id}
),
camp AS (
    SELECT ID, MAX(NAME) AS NAME
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY
    GROUP BY ID
)
SELECT
    camp.NAME                    AS campaign_name,
    aa.DAY                       AS metric_date,
    SUM(aa.IMPRESSIONS)          AS impressions,
    SUM(COALESCE(aa.LANDING_PAGE_CLICKS, aa.CLICKS)) AS clicks,
    SUM(aa.COST_IN_USD)          AS spend_usd
FROM PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE aa
JOIN cr   ON aa.CREATIVE_ID = cr.creative_id AND cr.rn = 1
JOIN camp ON cr.CAMPAIGN_ID = camp.ID
WHERE aa.DAY >= CURRENT_DATE - INTERVAL '{window} days'
GROUP BY 1, 2
"""


def _backfill_linkedin_delivery_daily(window_days, by_utm) -> int:
    from src.campaign_registry import _canonical_utm
    from src.redash_db import RedashClient
    from src.ui_decisions import upsert_daily_metrics_batch

    try:
        client = RedashClient()
        sql = _LINKEDIN_DAILY_SQL.format(account_id=config.LINKEDIN_AD_ACCOUNT_ID, window=window_days)
        df = client._run_query(sql, label=f"delivery-daily-linkedin-{window_days}d")
    except Exception as exc:  # noqa: BLE001
        log.warning("daily LinkedIn delivery query failed: %s", exc)
        return 0
    batch: list[dict] = []
    for _, r in df.iterrows():
        ident = by_utm.get(_canonical_utm(r.get("campaign_name")))
        if ident is None:
            continue
        batch.append({
            "ramp_id": ident.ramp_id, "platform": "linkedin",
            "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
            "metric_date": r.get("metric_date"),
            "impressions": int(_num(r.get("impressions"))),
            "clicks": int(_num(r.get("clicks"))),
            "spend_usd": _num(r.get("spend_usd")),
        })
    written = upsert_daily_metrics_batch(batch, ["impressions", "clicks", "spend_usd"])
    log.info("daily LinkedIn delivery: wrote %d rows", written)
    return written


# ── Delivery-by-day: Meta (Insights time_increment=1) ───────────────────────

def _backfill_meta_delivery_daily(window_days, by_id) -> int:
    if not config.META_ACCESS_TOKEN:
        return 0
    from src.campaign_registry import _id_tail
    from src.ui_decisions import list_all_campaign_data, upsert_daily_metrics_batch
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adset import AdSet

    FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                        api_version=config.META_API_VERSION or "v21.0")
    since = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    # Distinct live Meta ad-set ids present in the registry.
    ad_set_ids = {
        str(row.get("platform_campaign_id") or "")
        for row in list_all_campaign_data()
        if (row.get("platform") or "").lower() == "meta"
        and (row.get("status") or "").lower() not in ("removed", "deleted", "archived", "superseded")
        and row.get("platform_campaign_id")
    }
    batch: list[dict] = []
    for asid in ad_set_ids:
        ident = by_id.get(_id_tail(asid))
        if ident is None:
            continue
        try:
            rows = AdSet(asid).get_insights(params={
                "time_range": {"since": since, "until": until},
                "time_increment": 1,
                "fields": ["impressions", "clicks", "spend"],
                "level": "adset",
            })
        except Exception as exc:  # noqa: BLE001
            log.debug("meta daily insights failed for %s: %s", asid, exc)
            continue
        for row in rows:
            day = row.get("date_start")
            if not day:
                continue
            batch.append({
                "ramp_id": ident.ramp_id, "platform": "meta",
                "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
                "metric_date": day,
                "impressions": int(_num(row.get("impressions"))),
                "clicks": int(_num(row.get("clicks"))),
                "spend_usd": _num(row.get("spend")),
            })
    written = upsert_daily_metrics_batch(batch, ["impressions", "clicks", "spend_usd"])
    log.info("daily Meta delivery: wrote %d rows", written)
    return written


# ── Delivery-by-day: Reddit (reports CAMPAIGN_ID × DATE) ────────────────────

def _backfill_reddit_delivery_daily(window_days, by_id) -> int:
    if not config.REDDIT_API_ENABLED:
        return 0
    from src.campaign_registry import _id_tail
    from src.ui_decisions import upsert_daily_metrics_batch
    try:
        from src.reddit_api import RedditClient
        rows = RedditClient().fetch_campaign_metrics_daily(window_days)
    except Exception as exc:  # noqa: BLE001
        log.warning("daily Reddit delivery fetch failed: %s", exc)
        return 0
    batch: list[dict] = []
    for m in rows:
        ident = by_id.get(_id_tail(m.get("campaign_id")))
        if ident is None:
            continue
        batch.append({
            "ramp_id": ident.ramp_id, "platform": "reddit",
            "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
            "metric_date": m.get("metric_date"),
            "impressions": int(_num(m.get("impressions"))),
            "clicks": int(_num(m.get("clicks"))),
            "spend_usd": _num(m.get("spend_usd")),
        })
    written = upsert_daily_metrics_batch(batch, ["impressions", "clicks", "spend_usd"])
    log.info("daily Reddit delivery: wrote %d rows", written)
    return written


# ── Delivery-by-day: TikTok (report/integrated/get, stat_time_day) ──────────

def _backfill_tiktok_delivery_daily(window_days, by_id) -> int:
    if not config.TIKTOK_API_ENABLED:
        return 0
    from src.campaign_registry import _id_tail
    from src.ui_decisions import upsert_daily_metrics_batch
    try:
        from src.tiktok_api import TikTokClient
        rows = TikTokClient().fetch_campaign_metrics_daily(window_days)
    except Exception as exc:  # noqa: BLE001
        log.warning("daily TikTok delivery fetch failed: %s", exc)
        return 0
    batch: list[dict] = []
    for m in rows:
        ident = by_id.get(_id_tail(m.get("campaign_id")))
        if ident is None:
            continue
        batch.append({
            "ramp_id": ident.ramp_id, "platform": "tiktok",
            "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
            "metric_date": m.get("metric_date"),
            "impressions": int(_num(m.get("impressions"))),
            "clicks": int(_num(m.get("clicks"))),
            "spend_usd": _num(m.get("spend_usd")),
        })
    written = upsert_daily_metrics_batch(batch, ["impressions", "clicks", "spend_usd"])
    log.info("daily TikTok delivery: wrote %d rows", written)
    return written


# ── Delivery-by-day: Google (GAQL segments.date) ────────────────────────────

def _google_daily_query(ref: str, since: str, until: str) -> str | None:
    """Per-day GAQL for a Google resource (adGroup / campaign resource / bare id)."""
    ref = (ref or "").strip()
    select = ("SELECT metrics.impressions, metrics.clicks, metrics.cost_micros, "
              "segments.date")
    date = f"AND segments.date BETWEEN '{since}' AND '{until}'"
    if "/adGroups/" in ref:
        return f"{select} FROM ad_group WHERE ad_group.resource_name = '{ref}' {date}"
    if "/campaigns/" in ref:
        return f"{select} FROM campaign WHERE campaign.resource_name = '{ref}' {date}"
    if ref.isdigit():
        return f"{select} FROM campaign WHERE campaign.id = {ref} {date}"
    return None


def _backfill_google_delivery_daily(window_days, by_id) -> int:
    if not (config.GOOGLE_ADS_DEVELOPER_TOKEN and config.GOOGLE_ADS_CUSTOMER_ID):
        return 0
    from src.campaign_registry import _id_tail
    from src.platform_metrics import _customer_id_from_ref
    from src.ui_decisions import list_all_campaign_data, upsert_daily_metrics_batch

    since = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()
    refs = {
        str(row.get("platform_campaign_id") or "")
        for row in list_all_campaign_data()
        if (row.get("platform") or "").lower() in ("google", "google_search")
        and (row.get("status") or "").lower() not in ("removed", "deleted", "archived", "superseded")
        and row.get("platform_campaign_id")
    }
    if not refs:
        return 0
    creds = {
        "developer_token": config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "refresh_token": config.GOOGLE_ADS_REFRESH_TOKEN,
        "client_id": config.GOOGLE_ADS_CLIENT_ID,
        "client_secret": config.GOOGLE_ADS_CLIENT_SECRET,
        "use_proto_plus": True,
    }
    if config.GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        creds["login_customer_id"] = str(config.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace("-", "")
    try:
        from google.ads.googleads.client import GoogleAdsClient as _SDKClient
        sdk = _SDKClient.load_from_dict(creds)
        ga = sdk.get_service("GoogleAdsService")
    except Exception as exc:  # noqa: BLE001
        log.warning("daily Google delivery: client init failed: %s", exc)
        return 0
    batch: list[dict] = []
    for ref in refs:
        ident = by_id.get(_id_tail(ref))
        if ident is None:
            continue
        query = _google_daily_query(ref, since, until)
        if not query:
            continue
        cid = (_customer_id_from_ref(ref) or str(config.GOOGLE_ADS_CUSTOMER_ID)).replace("-", "")
        try:
            for b in ga.search_stream(customer_id=cid, query=query):
                for r in b.results:
                    batch.append({
                        "ramp_id": ident.ramp_id, "platform": ident.platform,
                        "campaign_key": ident.campaign_key, "campaign_name": ident.campaign_name,
                        "metric_date": r.segments.date,
                        "impressions": int(r.metrics.impressions or 0),
                        "clicks": int(r.metrics.clicks or 0),
                        "spend_usd": (r.metrics.cost_micros or 0) / 1_000_000.0,
                    })
        except Exception as exc:  # noqa: BLE001
            log.debug("daily Google delivery failed for %s: %s", ref, exc)
            continue
    written = upsert_daily_metrics_batch(batch, ["impressions", "clicks", "spend_usd"])
    log.info("daily Google delivery: wrote %d rows", written)
    return written


# ── Entry point ─────────────────────────────────────────────────────────────

def build_daily_metrics(window_days: int = 90) -> dict:
    """Populate campaign_daily_metrics for the last `window_days`. Funnel-by-day
    covers all channels (reddit at ramp level); delivery-by-day covers LinkedIn,
    Meta, Google, and Reddit. Each pass is best-effort — one failing never blocks
    the others. Returns a summary."""
    from src.ui_decisions import reddit_representative_by_spend

    by_utm, by_id, reddit_rep_cum = _build_indexes()
    log.info("daily metrics: registry index — %d utm keys, %d id keys, %d reddit ramps",
             len(by_utm), len(by_id), len(reddit_rep_cum))

    # Delivery first — reddit funnel (ramp-level) then attributes to whichever
    # reddit campaign actually delivered most in-window, so spend + funnel sit on
    # the same row (falls back to the cumulative-impressions pick for ramps with
    # no in-window reddit delivery).
    summary = {
        "linkedin_rows": _backfill_linkedin_delivery_daily(window_days, by_utm),
        "meta_rows":     _backfill_meta_delivery_daily(window_days, by_id),
        "google_rows":   _backfill_google_delivery_daily(window_days, by_id),
        "reddit_rows":   _backfill_reddit_delivery_daily(window_days, by_id),
        "tiktok_rows":   _backfill_tiktok_delivery_daily(window_days, by_id),
    }
    reddit_rep = dict(reddit_rep_cum)
    for ramp, (key, name) in reddit_representative_by_spend().items():
        reddit_rep[ramp] = _Identity(ramp, "reddit", name, key)
    summary["funnel_rows"] = _backfill_funnel_daily(window_days, by_utm, by_id, reddit_rep)

    _alert_funnel_anomaly()

    log.info("build_daily_metrics done (window=%dd): %s", window_days, summary)
    return summary


def _alert_funnel_anomaly(lookback_days: int = 7) -> None:
    """Alert when the funnel flatlines while delivery keeps flowing — the exact
    signature of a stale funnel source (e.g. `VIEW.APPLICATION_CONVERSION` lag)
    that otherwise writes 0 sign-ups/activations SILENTLY, so the Analytics tab
    shows zeros with no error (a "success" run with bad data). Compares the last
    `lookback_days` COMPLETE days (excludes today, which is partial): if delivery
    is high but sign-ups are 0 across ALL campaigns, Slack the team. Best-effort —
    never raises into the metrics run. Gate: config.FUNNEL_ANOMALY_ALERT_ENABLED."""
    if not getattr(config, "FUNNEL_ANOMALY_ALERT_ENABLED", True):
        return
    try:
        from src.ui_decisions import _connect
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT coalesce(sum(impressions),0), coalesce(sum(signups),0), "
                "       count(DISTINCT metric_date) FILTER (WHERE impressions > 0 AND signups = 0) "
                "FROM campaign_daily_metrics "
                "WHERE metric_date >= (CURRENT_DATE - %s) AND metric_date < CURRENT_DATE",
                (int(lookback_days),),
            )
            row = cur.fetchone() or (0, 0, 0)
        impr, signups, zero_days = int(row[0] or 0), int(row[1] or 0), int(row[2] or 0)
        min_impr = int(getattr(config, "FUNNEL_ANOMALY_MIN_IMPRESSIONS", 500_000))
        if impr >= min_impr and signups == 0:
            msg = (
                f":rotating_light: *Funnel data anomaly (GMR metrics)* — the last "
                f"{lookback_days} complete days have *{impr:,} impressions* but "
                f"*0 sign-ups* across all campaigns ({zero_days} day(s) with delivery "
                f"yet zero funnel). Delivery is healthy, so this is almost certainly the "
                f"funnel source `SCALE_PROD.VIEW.APPLICATION_CONVERSION` being stale/"
                f"lagging — the console Analytics tab will show 0 sign-ups/activations "
                f"until it catches up. Action: confirm the view's freshness with the data "
                f"team, then re-run `build_daily_metrics(window_days=30)` (idempotent)."
            )
            from src.smart_ramp_notifier import _send_to_all_targets
            _send_to_all_targets(msg, ramp_id="")
            log.warning("funnel anomaly ALERT sent: last %dd impr=%d signups=0 zero_days=%d",
                        lookback_days, impr, zero_days)
        else:
            log.info("funnel anomaly check OK: last %dd impr=%d signups=%d zero_days=%d",
                     lookback_days, impr, signups, zero_days)
    except Exception as exc:  # noqa: BLE001
        log.warning("funnel anomaly check failed (non-fatal): %s", exc)

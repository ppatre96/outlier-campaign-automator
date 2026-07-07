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
from datetime import datetime, timedelta, timezone

import config

log = logging.getLogger(__name__)

_UTM_CHANNELS = ("linkedin", "meta", "reddit")


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
    """Two lookups over the registry so both passes resolve to the SAME
    campaign_key: by canonical UTM (funnel li/meta/reddit) and by platform-id
    tail (funnel google + ALL delivery). campaign_key = canonical UTM for
    UTM channels, id-tail for Google."""
    from src.campaign_registry import _canonical_utm, _id_tail
    from src.ui_decisions import list_all_campaign_data

    by_utm: dict[str, _Identity] = {}
    by_id: dict[str, _Identity] = {}
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
    return by_utm, by_id


# ── Funnel-by-day (all channels) ────────────────────────────────────────────

def _backfill_funnel_daily(window_days, by_utm, by_id) -> int:
    from src.campaign_registry import _canonical_utm, _id_tail
    from src.redash_db import RedashClient
    from src.ui_decisions import upsert_daily_metrics_batch

    client = RedashClient()
    batch: list[dict] = []
    # ad_key resolver differs by channel family.
    channels = [("linkedin", "utm"), ("meta", "utm"), ("reddit", "utm"),
                ("google", "id"), ("google_adgroup", "id")]
    for chan, family in channels:
        try:
            df = client.query_campaign_funnel_daily(chan, days_back=window_days)
        except Exception as exc:  # noqa: BLE001
            log.warning("daily funnel %s query failed: %s", chan, exc)
            continue
        for _, r in df.iterrows():
            raw = str(r.get("ad_key") or "")
            ident = by_utm.get(_canonical_utm(raw)) if family == "utm" else by_id.get(_id_tail(raw))
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


# ── Entry point ─────────────────────────────────────────────────────────────

def build_daily_metrics(window_days: int = 90) -> dict:
    """Populate campaign_daily_metrics for the last `window_days`. Funnel-by-day
    covers all channels; delivery-by-day covers LinkedIn + Meta (v1). Each pass
    is best-effort — one failing never blocks the others. Returns a summary."""
    by_utm, by_id = _build_indexes()
    log.info("daily metrics: registry index — %d utm keys, %d id keys", len(by_utm), len(by_id))
    summary = {
        "funnel_rows":   _backfill_funnel_daily(window_days, by_utm, by_id),
        "linkedin_rows": _backfill_linkedin_delivery_daily(window_days, by_utm),
        "meta_rows":     _backfill_meta_delivery_daily(window_days, by_id),
    }
    log.info("build_daily_metrics done (window=%dd): %s", window_days, summary)
    return summary

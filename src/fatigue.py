"""Creative-fatigue detection.

`compute_fatigue(ramp_id)` scores each LIVE Meta ad set for a ramp and writes a
row to `ramp_fatigue` (read by the console "Fatigue" tab + the weekly Slack
report). A campaign is:

  - "reached"  — act now: 7-day frequency ≥ FATIGUE_FREQ_REACHED, OR CTR down
                 ≥ FATIGUE_CTR_WOW_REACHED week-over-week.
  - "reaching" — early warning: frequency ≥ FATIGUE_FREQ_REACHING, OR CTR down
                 ≥ FATIGUE_CTR_WOW_REACHING.
  - "healthy"  — neither (not persisted as actionable).

Frequency (avg impressions per person, 7d) is the canonical fatigue signal and
is Meta-only; CTR week-over-week is the cross-platform proxy. Both come straight
from the Meta Insights API. The recommended action is `refresh` (always — add
fresh creatives), plus `pause_weak` when weak ads are found (bottom-CTR ads with
enough impressions), i.e. `both`.

Meta-only for now (frequency has no Google/LinkedIn equivalent); extend later.
"""
from __future__ import annotations

import logging
from datetime import date, timedelta

import config
from src.ui_decisions import _connect, upsert_fatigue, UIDecisionsUnavailable

log = logging.getLogger(__name__)

_META_ACT = None  # ad-account id cache for link building


def _live_meta_adsets(ramp_id: str) -> list[dict]:
    """Distinct live Meta ad sets for a ramp from the `campaigns` table
    (campaign_type='static', non-dead, non-empty platform_campaign_id)."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT platform_campaign_id, "
                "       max(campaign_name)     AS campaign_name, "
                "       max(cohort_signature)  AS cohort_signature, "
                "       max(geo_cluster)       AS geo_cluster, "
                "       max(coalesce(data->>'locale','')) AS locale "
                "FROM campaigns "
                "WHERE ramp_id = %s AND platform = 'meta' AND campaign_type = 'static' "
                "  AND coalesce(platform_campaign_id,'') <> '' "
                "  AND coalesce(data->>'status','') NOT IN ('deleted','archived') "
                "GROUP BY platform_campaign_id",
                (ramp_id,),
            )
            rows = cur.fetchall()
            return [
                {
                    "adset_id": str(r[0]),
                    "campaign_name": r[1] or "",
                    "cohort_signature": r[2] or "",
                    "geo_cluster": r[3] or "",
                    "locale": r[4] or "",
                }
                for r in rows
            ]
    except UIDecisionsUnavailable as exc:
        log.warning("fatigue: campaigns table unavailable for %s: %s", ramp_id, exc)
        return []


def _classify(frequency, ctr_wow) -> tuple[str, int]:
    """Return (classification, score 0-100) from the two primary signals."""
    reached = (
        (frequency is not None and frequency >= config.FATIGUE_FREQ_REACHED)
        or (ctr_wow is not None and ctr_wow <= config.FATIGUE_CTR_WOW_REACHED)
    )
    reaching = (
        (frequency is not None and frequency >= config.FATIGUE_FREQ_REACHING)
        or (ctr_wow is not None and ctr_wow <= config.FATIGUE_CTR_WOW_REACHING)
    )
    # Score: blend frequency (capped at the reached threshold) and CTR decline.
    freq_component = 0.0
    if frequency is not None and config.FATIGUE_FREQ_REACHED > 0:
        freq_component = min(1.0, frequency / config.FATIGUE_FREQ_REACHED)
    ctr_component = 0.0
    if ctr_wow is not None and ctr_wow < 0 and config.FATIGUE_CTR_WOW_REACHED < 0:
        ctr_component = min(1.0, ctr_wow / config.FATIGUE_CTR_WOW_REACHED)
    score = int(round(100 * max(freq_component, ctr_component)))
    if reached:
        return "reached", max(score, 67)
    if reaching:
        return "reaching", max(score, 34)
    return "healthy", score


def _weak_ad_ids(per_ad: list[dict]) -> list[str]:
    """Bottom-CTR ads with enough impressions to judge (CTR below
    FATIGUE_WEAK_AD_CTR_RATIO × the campaign's median ad CTR)."""
    judged = [
        a for a in per_ad
        if a.get("ctr") is not None
        and (a.get("impressions") or 0) >= config.FATIGUE_WEAK_AD_MIN_IMPRESSIONS
    ]
    if len(judged) < 2:
        return []
    ctrs = sorted(a["ctr"] for a in judged)
    mid = len(ctrs) // 2
    median = ctrs[mid] if len(ctrs) % 2 else (ctrs[mid - 1] + ctrs[mid]) / 2
    if median <= 0:
        return []
    floor = median * config.FATIGUE_WEAK_AD_CTR_RATIO
    weak = [a["ad_id"] for a in judged if a["ctr"] < floor and a.get("ad_id")]
    return weak


def compute_fatigue(ramp_id: str, *, persist: bool = True) -> list[dict]:
    """Score every live Meta ad set for a ramp; upsert to ramp_fatigue (unless
    persist=False). Returns the list of fatigue dicts (fatigued ones first)."""
    if not getattr(config, "FATIGUE_ENABLED", True):
        log.info("fatigue: FATIGUE_ENABLED is off — skipping %s", ramp_id)
        return []

    adsets = _live_meta_adsets(ramp_id)
    if not adsets:
        log.info("fatigue: no live Meta ad sets for %s", ramp_id)
        return []

    from src.meta_api import MetaClient
    client = MetaClient()
    client._ensure_init()
    acct = (config.META_AD_ACCOUNT_ID or "").replace("act_", "")

    today = date.today()
    cur_since = (today - timedelta(days=7)).isoformat()
    cur_until = (today - timedelta(days=1)).isoformat()   # yesterday (complete day)
    prev_since = (today - timedelta(days=14)).isoformat()
    prev_until = (today - timedelta(days=8)).isoformat()

    results: list[dict] = []
    for a in adsets:
        adset_id = a["adset_id"]
        cur_agg = client.get_insights_window(adset_id, "adset", since=cur_since, until=cur_until)
        prev_agg = client.get_insights_window(adset_id, "adset", since=prev_since, until=prev_until)

        frequency = cur_agg.get("frequency")
        spend = cur_agg.get("spend") or 0.0
        ctr_now = cur_agg.get("ctr")
        ctr_prev = prev_agg.get("ctr")
        ctr_wow = None
        if ctr_now is not None and ctr_prev not in (None, 0):
            ctr_wow = (ctr_now - ctr_prev) / ctr_prev

        # Skip campaigns without enough spend to judge.
        if spend < config.FATIGUE_MIN_SPEND_USD:
            log.debug("fatigue: %s spend $%.2f < floor — skipping", adset_id, spend)
            continue

        classification, score = _classify(frequency, ctr_wow)
        if classification == "healthy":
            continue

        per_ad = client.get_ad_insights_7d(adset_id, "adset")
        weak_ad_ids = _weak_ad_ids(per_ad)
        action = "both" if weak_ad_ids else "refresh"

        parent_campaign_id = cur_agg.get("campaign_id") or prev_agg.get("campaign_id") or ""
        campaign_link = ""
        if acct and parent_campaign_id:
            campaign_link = (
                f"https://business.facebook.com/adsmanager/manage/ads?act={acct}"
                f"&selected_campaign_ids={parent_campaign_id}&selected_adset_ids={adset_id}"
            )

        entry = {
            "ramp_id": ramp_id,
            "platform": "meta",
            "campaign_id": adset_id,          # the ad set is the console "campaign" unit
            "adset_id": adset_id,
            "campaign_name": a["campaign_name"],
            "cohort_signature": a["cohort_signature"],
            "geo_cluster": a["geo_cluster"],
            "locale": a["locale"],
            "classification": classification,
            "fatigue_score": score,
            "recommended_action": action,
            "campaign_link": campaign_link,
            "signals": {
                "frequency": round(frequency, 2) if frequency is not None else None,
                "reach": cur_agg.get("reach"),
                "ctr_now_pct": round(ctr_now, 3) if ctr_now is not None else None,
                "ctr_prev_pct": round(ctr_prev, 3) if ctr_prev is not None else None,
                "ctr_wow_pct": round(ctr_wow * 100, 1) if ctr_wow is not None else None,
                "spend_usd": round(spend, 2),
                "weak_ad_ids": weak_ad_ids,
                "weak_ad_count": len(weak_ad_ids),
                "frequency_available": frequency is not None,
            },
        }
        if persist:
            upsert_fatigue(entry)
        results.append(entry)
        log.info(
            "fatigue: %s cohort=%s geo=%s → %s (score=%d freq=%s ctr_wow=%s weak=%d)",
            adset_id, a["cohort_signature"], a["geo_cluster"], classification, score,
            entry["signals"]["frequency"], entry["signals"]["ctr_wow_pct"], len(weak_ad_ids),
        )

    log.info("fatigue: %s → %d fatigued of %d live ad sets", ramp_id, len(results), len(adsets))
    return results

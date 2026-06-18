"""
src/live_angle_stats.py
=======================

Produce angle-performance rows for `angle_performance.analyze_angles(rows=…)`
from **live** ad-platform metrics, sourced off the Postgres `campaigns` table
(the authoritative store since the console-empty fix) rather than the legacy
local `data/campaign_registry.json` that `campaign_registry.get_active_campaigns`
reads — which in CI only ever contains a handful of old ramps and carries no
refreshed metrics.

Why this exists
---------------
The angle double-down loop was silently a no-op for every recent ramp: its data
source (`get_active_campaigns` → local JSON) has no GMR-0023+ rows, and even the
rows it does have never get metrics refreshed onto them. So `analyze_angles`
saw zero qualified angles → zero recommendations → the console "Live performance
& recommendations" section stayed empty for all ramps.

This module closes both gaps for Meta (the platform with real delivery):
  1. campaign list comes from Postgres `campaigns` (has GMR-0023+),
  2. metrics are pulled LIVE from the Meta Insights API at analysis time.

Meta angle attribution
-----------------------
On Meta the three angles (A/B/C) are NOT separate campaigns — each language is
one campaign and the angles live at the AD level (e.g. "… | A", "hi-IN Ad C").
So we pull insights at `level=ad`, parse the angle from the ad name, and
aggregate per (campaign, angle). Ads whose name carries no recognizable angle
(hand-created / "- Copy" duplicates) are EXCLUDED from the comparison and the
dropped impression share is logged — we only judge cleanly-attributable
creatives. `applications` is the `fb_pixel_lead` count (real signup intent),
so the verdict is CPA-primary, not CTR-primary (CTR on cheap-CPM language geos
is a junk signal).

Each emitted row is one (campaign × angle), keyed with the cohort_signature /
geo_cluster from Postgres so `analyze_angles` groups A/B/C within a language and
picks that language's winning angle.
"""
from __future__ import annotations

import logging
import os
import re
from collections import defaultdict
from typing import Any, Optional

log = logging.getLogger("live_angle_stats")

# Angle from an ad name: pipeline names end "… | A"; hand-named ads read
# "<lang> Ad B (…)". Anything else is unattributable.
_ANGLE_RE = re.compile(r"\| ([ABC])\b")
_ANGLE_RE_ALT = re.compile(r"\bAd ([ABC])\b")

# Meta lead action types that count as a worker application (signup intent).
_LEAD_ACTION_TYPES = {"lead", "offsite_conversion.fb_pixel_lead", "onsite_web_lead"}


def _angle_of(ad_name: Optional[str]) -> Optional[str]:
    n = ad_name or ""
    m = _ANGLE_RE.search(n) or _ANGLE_RE_ALT.search(n)
    return m.group(1) if m else None


# Locale in a campaign name: "Scale-GMR-0023 | Meta | language | hi-IN | …".
_LOCALE_RE = re.compile(r"\b([a-z]{2}-[A-Z]{2})\b")


def _meta_campaign_index(ramp_id: str) -> dict[str, dict[str, str]]:
    """platform_campaign_id → {cohort_signature, geo_cluster, cohort_id} from
    Postgres `campaigns` for this ramp's Meta rows. Empty dict on any DB error
    (caller degrades to no Meta rows).

    NOTE: on Meta each language is its own campaign but the stored
    cohort_signature is a generic ramp-wide string and geo_cluster is often
    blank — so grouping A/B/C by (cohort_signature, geo_cluster) would wrongly
    merge every language into one bucket and compare angles ACROSS languages.
    We therefore key each campaign by its own locale (parsed from the campaign
    name, falling back to the campaign id) so analyze_angles compares A/B/C
    WITHIN a language and picks that language's winning angle."""
    from src import ui_decisions
    try:
        with ui_decisions._connect() as conn, conn.cursor() as cur:
            cur.execute(
                "SELECT DISTINCT platform_campaign_id, cohort_signature, geo_cluster, "
                "cohort_id, campaign_name "
                "FROM campaigns WHERE ramp_id = %s AND platform = 'meta' "
                "AND coalesce(platform_campaign_id, '') <> ''",
                (ramp_id,),
            )
            out: dict[str, dict[str, str]] = {}
            for cid, cohort, geo, cohort_id, name in cur.fetchall():
                m = _LOCALE_RE.search(name or "")
                locale = m.group(1) if m else str(cid)
                # cohort_signature carries the language so each is its own group;
                # keep the original ramp signature as a readable prefix.
                base = (cohort or "Meta").split("|")[0].strip() or "Meta"
                out[str(cid)] = {
                    "cohort_signature": f"{base} · {locale}",
                    "geo_cluster": (geo or "").strip() or locale,
                    "cohort_id": cohort_id or "",
                }
            return out
    except Exception as exc:  # missing table / no DATABASE_URL
        log.warning("live_angle_stats: Postgres campaign index unavailable for %s: %s", ramp_id, exc)
        return {}


def meta_angle_rows(ramp_id: str, *, window_days: int = 30) -> list[dict[str, Any]]:
    """Return analyze_angles-shaped rows (one per campaign × angle) from live
    Meta ad-level insights. Empty list when Meta isn't configured, the ramp has
    no Meta campaigns, or the API is unreachable — all non-fatal."""
    import config
    if not config.META_ACCESS_TOKEN:
        log.info("live_angle_stats: META_ACCESS_TOKEN unset — no Meta angle rows")
        return []

    index = _meta_campaign_index(ramp_id)
    if not index:
        return []

    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.campaign import Campaign
    FacebookAdsApi.init(
        access_token=config.META_ACCESS_TOKEN,
        api_version=config.META_API_VERSION or "v21.0",
    )

    rows: list[dict[str, Any]] = []
    dropped_imp = 0
    kept_imp = 0
    for cid, meta in index.items():
        # Campaign-level daily budget (cents). None on no-CBO campaigns where the
        # budget lives on the ad set — the scale executor treats None as
        # "can't scale at campaign level" and skips. Shared across the campaign's
        # angles, so it rides on every row for this campaign.
        budget_cents: Optional[int] = None
        try:
            from facebook_business.adobjects.campaign import Campaign as _C
            _camp = _C(cid).api_get(fields=["daily_budget"])
            _db = _camp.get("daily_budget")
            budget_cents = int(_db) if _db not in (None, "") else None
        except Exception:
            pass
        # angle → [imp, clk, spend, leads]
        agg: dict[str, list[float]] = defaultdict(lambda: [0.0, 0.0, 0.0, 0.0])
        try:
            insights = Campaign(cid).get_insights(params={
                "date_preset": "maximum" if window_days <= 0 else "last_30d",
                "level": "ad",
                "fields": ["ad_name", "impressions", "clicks", "spend", "actions"],
            })
        except Exception as exc:
            log.debug("live_angle_stats: insights failed for campaign %s: %s", cid, exc)
            continue
        for r in insights:
            imp = float(r.get("impressions", 0) or 0)
            angle = _angle_of(r.get("ad_name"))
            if angle is None:
                dropped_imp += int(imp)
                continue
            kept_imp += int(imp)
            leads = 0.0
            for act in (r.get("actions") or []):
                if act.get("action_type") in _LEAD_ACTION_TYPES:
                    leads = max(leads, float(act.get("value", 0) or 0))
            a = agg[angle]
            a[0] += imp
            a[1] += float(r.get("clicks", 0) or 0)
            a[2] += float(r.get("spend", 0) or 0)
            a[3] += leads
        for angle, (imp, clk, spend, leads) in agg.items():
            rows.append({
                "smart_ramp_id": ramp_id,
                "platform": "meta",
                "platform_campaign_id": cid,
                "cohort_signature": meta["cohort_signature"],
                "cohort_id": meta["cohort_id"],
                "geo_cluster": meta["geo_cluster"],
                "angle": angle,
                "status": "active",
                "impressions": imp,
                "clicks": clk,
                "spend_usd": spend,
                "applications": leads,
                "daily_budget_cents": budget_cents,
            })

    total = kept_imp + dropped_imp
    if total:
        log.info(
            "live_angle_stats(%s): Meta %d attributable rows across %d campaigns; "
            "%.0f%% of impressions attributable (%d dropped as un-angled ads)",
            ramp_id, len(rows), len(index), kept_imp / total * 100, dropped_imp,
        )
    return rows

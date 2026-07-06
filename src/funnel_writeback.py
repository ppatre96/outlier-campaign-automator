"""All-channel funnel writeback.

Attributes Outlier sign-ups → screening passes → activations back onto campaign
registry rows, filling the columns that ad reporting APIs never provide
(reporting APIs give impressions/clicks/spend only).

Attribution source is SCALE_PROD.VIEW.APPLICATION_CONVERSION:
  - LinkedIn: per-creative via AD_ID (FEED-15, analyze_funnel_by_cohort).
  - Meta / Google: campaign-level via UTM_CAMPAIGN = our campaign_name.
  - Reddit / TikTok: creative-only channels whose conversions carry no joinable
    ad id in the view — nothing to write (reported, not silently skipped).

Called daily by scripts/refresh_metrics.py alongside the platform metric fetch,
and by the weekly feedback loop for its Slack "Activations" section.
"""

from __future__ import annotations

import logging

log = logging.getLogger(__name__)

# Channels with no joinable conversion attribution in APPLICATION_CONVERSION
# (verified 2026-07-07): creative-only, no programmatic campaigns / no ad id.
_NO_ATTRIBUTION_CHANNELS = ("reddit", "tiktok")


def backfill_funnel_metrics_all_channels(window_days: int = 7) -> dict:
    """Write funnel outcomes onto registry rows for every channel that supports
    attribution. Returns a per-channel summary:
        {channel: {rows_written, applications, skill_passes, activations, note?}}
    Best-effort per channel — one channel failing never blocks the others."""
    from src.campaign_registry import update_funnel_metrics

    out: dict[str, dict] = {}

    # LinkedIn — per-creative (AD_ID).
    try:
        from src.feedback_agent import FeedbackAgent
        from src.redash_db import RedashClient

        client = RedashClient()
        li_rows = FeedbackAgent(client).analyze_funnel_by_cohort(days_back=window_days)
        summary = _blank()
        for r in li_rows:
            cid = r.get("creative_id")
            if cid in (None, "", "None"):
                continue
            apps, passes, acts = _triple(r)
            w = update_funnel_metrics(
                str(cid), by="creative", applications=apps, skill_passes=passes, activations=acts,
            )
            summary["rows_written"] += w
            if w:  # only count what we actually attributed to our campaigns
                _accumulate(summary, apps, passes, acts)
        out["linkedin"] = summary
    except Exception as exc:  # noqa: BLE001
        log.exception("funnel writeback: linkedin failed")
        out["linkedin"] = {**_blank(), "note": f"error: {type(exc).__name__}"}

    # Meta / Google — campaign-level (UTM_CAMPAIGN = campaign_name).
    for chan in ("meta", "google"):
        try:
            from src.redash_db import RedashClient

            df = RedashClient().query_campaign_funnel(chan, days_back=window_days)
            summary = _blank()
            for _, row in df.iterrows():
                name = row.get("campaign_name")
                if not name:
                    continue
                apps, passes, acts = _triple(row)
                w = update_funnel_metrics(
                    str(name), by="name", applications=apps, skill_passes=passes, activations=acts,
                )
                summary["rows_written"] += w
                if w:  # only count campaigns that matched a registry row
                    _accumulate(summary, apps, passes, acts)
            out[chan] = summary
        except Exception as exc:  # noqa: BLE001
            log.exception("funnel writeback: %s failed", chan)
            out[chan] = {**_blank(), "note": f"error: {type(exc).__name__}"}

    # Reddit / TikTok — no joinable attribution.
    for chan in _NO_ATTRIBUTION_CHANNELS:
        log.info("funnel writeback: %s skipped — creative-only, no joinable ad id in "
                 "APPLICATION_CONVERSION", chan)
        out[chan] = {**_blank(), "note": "no attribution available (creative-only channel)"}

    return out


def _blank() -> dict:
    return {"rows_written": 0, "applications": 0, "skill_passes": 0, "activations": 0}


def _triple(row) -> tuple[int, int, int]:
    return (
        int(row.get("applications") or 0),
        int(row.get("screening_passes") or 0),
        int(row.get("activations") or 0),
    )


def _accumulate(summary: dict, apps: int, passes: int, acts: int) -> None:
    summary["applications"] += apps
    summary["skill_passes"] += passes
    summary["activations"] += acts

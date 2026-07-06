"""All-channel funnel writeback.

Attributes Outlier sign-ups → screening passes → activations back onto campaign
registry rows, filling the columns that ad reporting APIs never provide
(reporting APIs give impressions/clicks/spend only).

Attribution source is SCALE_PROD.VIEW.APPLICATION_CONVERSION. Join keys were
validated live 2026-07-07 against GMR-0023 (a relaunched ramp whose delivering
ads carry a different date token / platform id than the registry rows):
  - LinkedIn / Meta / Reddit: normalized UTM_CAMPAIGN = campaign_name
    ("name_norm" — strips the drifting date token + format-spelling variants so
    relaunches still match). Campaign-level; angle granularity is unrecoverable.
  - Google: CAMPAIGN_ID for campaign/bare rows, ADGROUP_ID for relaunch rows
    stored as ".../adGroups/<id>".
  - TikTok: creative-only — no joinable id — reported, not silently skipped.

Called daily by scripts/refresh_metrics.py alongside the platform metric fetch,
and read back (from Postgres) by the weekly feedback loop's Slack summary.
"""

from __future__ import annotations

import logging
from collections import defaultdict

log = logging.getLogger(__name__)


def backfill_funnel_metrics_all_channels(window_days: int = 7) -> dict:
    """Write funnel outcomes onto registry rows for every attributable channel.
    Returns {channel: {rows_written, applications, skill_passes, activations, note?}}.
    Best-effort per channel — one failing never blocks the others."""
    from src.campaign_registry import update_funnel_metrics, _normalize_campaign_name
    from src.redash_db import RedashClient

    client = RedashClient()
    out: dict[str, dict] = {}

    # Name-normalized channels (UTM_CAMPAIGN = campaign_name).
    for chan in ("linkedin", "meta", "reddit"):
        out[chan] = _write_grouped(
            client, chan, window_days, update_funnel_metrics,
            by="name_norm", key_fn=_normalize_campaign_name,
        )

    # Google: campaign-id rows + adGroup-id relaunch rows (disjoint registry rows).
    google = _blank()
    for chan_key, by in (("google", "campaign"), ("google_adgroup", "adgroup")):
        part = _write_grouped(client, chan_key, window_days, update_funnel_metrics, by=by)
        for k in ("rows_written", "applications", "skill_passes", "activations"):
            google[k] += part.get(k, 0)
        if part.get("note"):
            google["note"] = part["note"]
    out["google"] = google

    # TikTok — creative-only, no joinable id.
    out["tiktok"] = {**_blank(), "note": "no attribution available (creative-only)"}
    return out


def _write_grouped(client, chan, window_days, update_fn, *, by, key_fn=None) -> dict:
    """Fetch a channel's funnel rows, aggregate by the (optionally normalized)
    join key so relaunch/date variants sum instead of overwrite, and write each
    key once. `key_fn` normalizes the raw ad_key (None = use it verbatim)."""
    summary = _blank()
    try:
        df = client.query_campaign_funnel(chan, days_back=window_days)
    except Exception as exc:  # noqa: BLE001
        log.exception("funnel writeback: %s query failed", chan)
        return {**_blank(), "note": f"error: {type(exc).__name__}"}

    agg: dict[str, list[int]] = defaultdict(lambda: [0, 0, 0])
    for _, row in df.iterrows():
        raw = row.get("ad_key")
        key = key_fn(raw) if key_fn else (str(raw).strip() if raw not in (None, "") else "")
        if not key or key == "None":
            continue
        apps, passes, acts = _triple(row)
        agg[key][0] += apps
        agg[key][1] += passes
        agg[key][2] += acts

    for key, (apps, passes, acts) in agg.items():
        w = update_fn(key, by=by, applications=apps, skill_passes=passes, activations=acts)
        summary["rows_written"] += w
        if w:  # only count what we actually attributed to our campaigns
            _accumulate(summary, apps, passes, acts)
    return summary


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

"""All-channel campaign metrics refresh.

Pulls fresh impressions / clicks / spend / conversions for every current
campaign and writes them to the campaign registry AND Postgres (the console
dashboard rolls up the Postgres `campaigns` table).

Order:
  1. hydrate_from_postgres() — Postgres is the authoritative campaign store;
     the local JSON registry is stale in CI. Hydrating first means the refresh
     covers ALL current campaigns, not just the committed subset.
  2. refresh_linkedin_metrics()  — LinkedIn via Redash.
  3. fetch_metrics_for_active_extra_platforms() — Meta, Google Display,
     Google Search, and Reddit via their reporting APIs.
  4. backfill_funnel_metrics_all_channels() — sign-ups / skill passes /
     activations from the Outlier funnel (LinkedIn/Meta/Google/Reddit).

TikTok is creative-only (no platform campaigns) — nothing to fetch.

Invoked by .github/workflows/daily_feedback.yml before the recommendation pass
so recommendations read fresh metrics. Safe to run standalone:

    doppler run -- python3 scripts/refresh_metrics.py
    doppler run -- python3 scripts/refresh_metrics.py --window 14
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402  (needs _PROJECT_ROOT on sys.path first)

log = logging.getLogger("refresh_metrics")


def main() -> int:
    ap = argparse.ArgumentParser(description="Refresh live campaign metrics for all channels.")
    ap.add_argument("--window", type=int, default=30,
                    help="Look-back window in days for platform delivery metrics (impressions/clicks/sends/spend) "
                         "across ALL channels (LinkedIn/Meta/Google/Reddit). 30 so older ramps keep meaningful "
                         "cumulative delivery — a 7-day window undercounts anything older than a week. Default 30.")
    ap.add_argument("--funnel-window", type=int, default=30,
                    help="Look-back window in days for funnel outcomes (sign-ups/skill-passes/activations). Default 30.")
    args = ap.parse_args()

    from src.campaign_registry import hydrate_from_postgres
    from src.campaign_feedback_agent import refresh_linkedin_metrics
    from src.platform_metrics import fetch_metrics_for_active_extra_platforms
    from src.funnel_writeback import backfill_funnel_metrics_all_channels
    from src.daily_metrics import build_daily_metrics

    hydrated = hydrate_from_postgres()
    linkedin = refresh_linkedin_metrics(window=args.window)
    extra = fetch_metrics_for_active_extra_platforms(window_days=args.window)

    # Sign-ups / skill passes / activations — the funnel leg that ad reporting
    # APIs don't provide. LinkedIn per-creative; Meta/Google campaign-level.
    # Uses the wider funnel window so cumulative activations for older ramps
    # don't fall out of view.
    funnel = backfill_funnel_metrics_all_channels(window_days=args.funnel_window)
    funnel_written = sum(v.get("rows_written", 0) for v in funnel.values())

    # Day-over-day time-series for the console Analytics dashboard. Writes the
    # separate campaign_daily_metrics table (never touches the cumulative
    # `campaigns` rows). Best-effort — a failure here never blocks the cumulative
    # refresh above.
    try:
        daily = build_daily_metrics(window_days=args.funnel_window)
    except Exception as exc:  # noqa: BLE001
        log.warning("build_daily_metrics failed (non-fatal): %s", exc)
        daily = {}

    log.info(
        "refresh_metrics done: hydrated=%d linkedin=%d meta+google=%d funnel_rows=%d "
        "(delivery_window=%dd funnel_window=%dd)",
        hydrated, linkedin, extra, funnel_written, args.window, args.funnel_window,
    )
    for chan, s in funnel.items():
        log.info("  funnel[%s]: rows=%d sign-ups=%d skill_passes=%d activations=%d%s",
                 chan, s.get("rows_written", 0), s.get("applications", 0),
                 s.get("skill_passes", 0), s.get("activations", 0),
                 f" ({s['note']})" if s.get("note") else "")
    if daily:
        log.info("  daily_metrics: funnel=%d linkedin_delivery=%d meta_delivery=%d (campaign×day rows)",
                 daily.get("funnel_rows", 0), daily.get("linkedin_rows", 0), daily.get("meta_rows", 0))
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())

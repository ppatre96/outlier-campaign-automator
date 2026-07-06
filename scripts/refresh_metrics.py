"""All-channel campaign metrics refresh.

Pulls fresh impressions / clicks / spend / conversions for every current
campaign and writes them to the campaign registry AND Postgres (the console
dashboard rolls up the Postgres `campaigns` table).

Order:
  1. hydrate_from_postgres() — Postgres is the authoritative campaign store;
     the local JSON registry is stale in CI. Hydrating first means the refresh
     covers ALL current campaigns, not just the committed subset.
  2. refresh_linkedin_metrics()  — LinkedIn via Redash.
  3. fetch_metrics_for_active_extra_platforms() — Meta, Google Display, and
     Google Search via their reporting APIs.

Reddit and TikTok are creative-only (no platform campaigns) — nothing to fetch.

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
    ap.add_argument("--window", type=int, default=7, help="Look-back window in days (default 7).")
    args = ap.parse_args()

    from src.campaign_registry import hydrate_from_postgres
    from src.campaign_feedback_agent import refresh_linkedin_metrics
    from src.platform_metrics import fetch_metrics_for_active_extra_platforms
    from src.funnel_writeback import backfill_funnel_metrics_all_channels

    hydrated = hydrate_from_postgres()
    linkedin = refresh_linkedin_metrics(window=args.window)
    extra = fetch_metrics_for_active_extra_platforms(window_days=args.window)

    # Sign-ups / skill passes / activations — the funnel leg that ad reporting
    # APIs don't provide. LinkedIn per-creative; Meta/Google campaign-level.
    funnel = backfill_funnel_metrics_all_channels(window_days=args.window)
    funnel_written = sum(v.get("rows_written", 0) for v in funnel.values())

    log.info(
        "refresh_metrics done: hydrated=%d linkedin=%d meta+google=%d funnel_rows=%d (window=%dd)",
        hydrated, linkedin, extra, funnel_written, args.window,
    )
    for chan, s in funnel.items():
        log.info("  funnel[%s]: rows=%d sign-ups=%d skill_passes=%d activations=%d%s",
                 chan, s.get("rows_written", 0), s.get("applications", 0),
                 s.get("skill_passes", 0), s.get("activations", 0),
                 f" ({s['note']})" if s.get("note") else "")
    return 0


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    raise SystemExit(main())

#!/usr/bin/env python3
"""
Refresh competitor intelligence for a given TG label.

Triggers `src.competitor_intel.run_competitor_intel()` — which scrapes
Meta Ads Library, Reddit, Trustpilot, SEO terms, and Turing/Surge/Handshake
task listings — and writes the structured output to
`data/competitor_intel/latest.json`. That file is the source of truth the
pipeline's prep stage snapshots into Postgres `competitor_intel_snapshots`
for the console's "Competitor landscape" card.

When the console shows stale competitor data, it's because no prep run has
touched it AND no fresh scrape has updated latest.json. This script does
the scrape so the next prep run lands fresh data in Postgres.

Usage:
    doppler run -p outlier-campaign-agent -c dev -- \\
        python3 scripts/refresh_competitor_intel.py \\
            --tg-label "Short-Form Video Creators"

Expected runtime: 2-4 minutes (each of ~8 competitor scrapes runs sequentially
with rate-limit pauses). Token cost: minimal — most scrapes are unauthenticated
HTTP; only Reddit / SEO use API keys when configured.

Skip flags trim scope to debug a single source quickly.
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--tg-label", default="general",
                        help='Audience TG, e.g. "Short-Form Video Creators". '
                             'Used to scope SEO/Reddit queries.')
    parser.add_argument("--competitors", nargs="+", default=None,
                        help="Competitor keys to scan (default: top 4 ad competitors). "
                             "Choices include: dataannotation mercor alignerr micro1 "
                             "appen surge turing handshake")
    parser.add_argument("--no-reddit", action="store_true",
                        help="Skip Reddit signal pull (saves ~30s)")
    parser.add_argument("--no-trustpilot", action="store_true",
                        help="Skip Trustpilot scrape (saves ~1min)")
    parser.add_argument("--no-seo", action="store_true",
                        help="Skip SEO search-term pull")
    parser.add_argument("--no-task-listings", action="store_true",
                        help="Skip Turing/Surge/Handshake task listing scrape (saves ~2min)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("refresh_competitor_intel")

    log.info("Refreshing competitor intel for tg_label=%r", args.tg_label)

    from src.competitor_intel import run_competitor_intel, save_intel_json

    try:
        intel = run_competitor_intel(
            tg_label=args.tg_label,
            target_competitors=args.competitors,
            include_reddit=not args.no_reddit,
            include_trustpilot=not args.no_trustpilot,
            include_seo=not args.no_seo,
            include_task_listings=not args.no_task_listings,
        )
    except Exception as exc:
        log.exception("run_competitor_intel failed: %s", exc)
        return 1

    save_intel_json(intel, tg_label=args.tg_label)

    log.info("-" * 60)
    log.info("Summary:")
    log.info("  competitor_ads scraped: %d", len(intel.competitor_ads))
    log.info("  copy_recommendations:   %d", len(intel.copy_recommendations))
    log.info("  experiment_ideas:       %d", len(intel.copy_recommendations))
    log.info("  hot_domains:            %s", ", ".join(intel.hot_domains[:5]) or "(none)")
    log.info("  hot_tgs:                %s", ", ".join(intel.hot_tgs[:5]) or "(none)")
    log.info("Wrote data/competitor_intel/latest.json")
    log.info(
        "Next: run scripts/prep_smoke_test.py --ramp <RAMP_ID> to push to Postgres + console."
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

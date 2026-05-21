#!/usr/bin/env python3
"""
Focused prep-only smoke test for a single ramp.

Runs the pipeline's prep stages (cohort mining → brief → ICP enrichment →
competitor intel snapshot) WITHOUT touching the LinkedIn / Meta / Google
campaign-create APIs. Useful when you want to repopulate the console's
data-driven sections (ICP card, angles, competitor landscape, audience
badges) without burning real ad spend on a dry-run that still pings every
platform's reach API.

Cost ballpark per ramp:
  - Snowflake: 5-10 queries (resume data + ICP samples)
  - Anthropic / Claude: ~3-5 calls (brief + ICP enrichment per cohort)
  - LinkedIn: ~5-10 audienceCounts calls (Stage C)
  - Meta / Google reach: NOT called (prep_only=True skips campaign-create)
  - Gemini: NOT called (no image gen at prep)

Usage:
    doppler run -p outlier-campaign-agent -c dev -- \\
        python3 scripts/prep_smoke_test.py --ramp GMR-0021

Pass --skip-competitor to skip the slowest step (web scraping for
competitor intel — usually 2-3 minutes).
"""
import argparse
import logging
import sys
from pathlib import Path

# Make the project importable when invoked from scripts/.
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ramp", required=True, help="Smart Ramp ID, e.g. GMR-0021")
    parser.add_argument("--skip-competitor", action="store_true",
                        help="Skip the competitor intel scrape (saves ~2-3 min)")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("prep_smoke_test")

    log.info("=" * 72)
    log.info("Prep-only smoke test for ramp=%s", args.ramp)
    log.info("Will write to: cohort_brief_rationale, cohort_icp, "
             "competitor_intel_snapshots, Triggers Sheet")
    log.info("Will NOT call: LinkedIn create_campaign, Meta create_*, Google "
             "create_*, Gemini image gen")
    log.info("=" * 72)

    # Optional competitor intel skip via env var (read by competitor_intel.py
    # if we add the gate later — for now just log).
    if args.skip_competitor:
        import os
        os.environ["SKIP_COMPETITOR_INTEL"] = "1"
        log.info("SKIP_COMPETITOR_INTEL=1 set")

    # Import after sys.path tweak + env vars set.
    from main import _prep_ramp

    try:
        result = _prep_ramp(args.ramp)
    except Exception as exc:
        log.exception("prep_ramp failed for %s: %s", args.ramp, exc)
        return 1

    # Concise summary of what landed.
    log.info("-" * 72)
    log.info("Prep complete. Result summary:")
    for k, v in (result or {}).items():
        if isinstance(v, (list, dict)):
            log.info("  %s = %s (len=%d)", k, type(v).__name__, len(v))
        else:
            log.info("  %s = %s", k, v)
    log.info("-" * 72)

    # Quick verification of the new tables.
    try:
        import psycopg
        import os
        db_url = os.environ.get("DATABASE_URL", "")
        if db_url:
            with psycopg.connect(db_url, autocommit=True) as conn, conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM cohort_icp WHERE ramp_id = %s", (args.ramp,))
                icp_count = cur.fetchone()[0]
                cur.execute("SELECT count(*) FROM cohort_brief_rationale WHERE ramp_id = %s", (args.ramp,))
                rat_count = cur.fetchone()[0]
                cur.execute("SELECT captured_at FROM competitor_intel_snapshots WHERE ramp_id = %s", (args.ramp,))
                row = cur.fetchone()
                competitor_captured = row[0] if row else "(no snapshot)"
                log.info("Postgres verification for %s:", args.ramp)
                log.info("  cohort_icp rows: %d", icp_count)
                log.info("  cohort_brief_rationale rows: %d", rat_count)
                log.info("  competitor_intel_snapshots captured_at: %s", competitor_captured)
    except Exception as exc:
        log.warning("Postgres verification skipped: %s", exc)

    log.info("Done. Refresh https://outlier-campaign-console.vercel.app/ramps/%s "
             "to see the populated sections.", args.ramp)
    return 0


if __name__ == "__main__":
    sys.exit(main())

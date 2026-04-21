#!/usr/bin/env python3
"""
CLI Script: Manual Reanalysis Trigger

Entry point for manually triggering the ReanalysisOrchestrator outside of
the Slack reaction webhook flow. Useful for:
  - Weekly batch reanalysis (cron job)
  - Debugging cohort discovery without Slack
  - Collecting test variant results on demand

Usage:
    # Reanalyze excluding a paused cohort
    python scripts/trigger_reanalysis.py --exclude DATA_ANALYST

    # Discover variants for a specific cohort
    python scripts/trigger_reanalysis.py --focus ML_ENGINEER

    # Collect test results for specific cohorts
    python scripts/trigger_reanalysis.py --collect-results --cohorts DATA_ANALYST,ML_ENGINEER

    # Dry run — show what would happen without executing
    python scripts/trigger_reanalysis.py --focus ML_ENGINEER --dry-run

    # Full weekly refresh with no filters
    python scripts/trigger_reanalysis.py
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

# Ensure repo root is on the path when running as a script
_REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(_REPO_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
log = logging.getLogger("trigger_reanalysis")


def main() -> None:
    """CLI entry point for manual reanalysis triggering."""
    parser = argparse.ArgumentParser(
        description="Trigger reanalysis and cohort discovery via ReanalysisOrchestrator",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  Reanalyze, excluding a paused cohort:
    python scripts/trigger_reanalysis.py --exclude DATA_ANALYST

  Discover variants focused on a specific cohort:
    python scripts/trigger_reanalysis.py --focus ML_ENGINEER

  Collect test results for specific cohorts:
    python scripts/trigger_reanalysis.py --collect-results --cohorts DATA_ANALYST,ML_ENGINEER

  Dry run (print what would happen; don't execute):
    python scripts/trigger_reanalysis.py --focus ML_ENGINEER --dry-run
        """,
    )
    parser.add_argument(
        "--exclude",
        metavar="COHORT_NAME",
        help="Cohort name to exclude from reanalysis (e.g. DATA_ANALYST)",
    )
    parser.add_argument(
        "--focus",
        metavar="COHORT_NAME",
        help="Cohort name to focus variant discovery on (e.g. ML_ENGINEER)",
    )
    parser.add_argument(
        "--collect-results",
        action="store_true",
        default=False,
        help="Collect test variant results only (requires --cohorts)",
    )
    parser.add_argument(
        "--cohorts",
        metavar="COHORT1,COHORT2",
        help="Comma-separated cohort names for result collection (used with --collect-results)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Print what would happen; don't actually execute reanalysis",
    )

    args = parser.parse_args()

    # ── Dry-run mode ──────────────────────────────────────────────────────────
    if args.dry_run:
        if args.collect_results:
            cohort_list = [c.strip() for c in (args.cohorts or "").split(",") if c.strip()]
            print(json.dumps({
                "dry_run": True,
                "action": "collect_test_results",
                "cohorts": cohort_list,
                "note": "Would call ReanalysisOrchestrator.collect_test_results(cohort_list)",
            }, indent=2))
        else:
            print(json.dumps({
                "dry_run": True,
                "action": "trigger_reanalysis",
                "cohort_to_exclude": args.exclude,
                "cohort_to_focus": args.focus,
                "reason": "manual_cli",
                "note": "Would call ReanalysisOrchestrator.trigger_reanalysis(...)",
            }, indent=2))
        return

    # ── Import orchestrator (deferred to avoid import-time side effects) ──────
    try:
        from src.reanalysis_loop import ReanalysisOrchestrator
    except ImportError as e:
        log.error("Failed to import ReanalysisOrchestrator: %s", str(e))
        log.error("Ensure you are running from the repo root: python scripts/trigger_reanalysis.py")
        sys.exit(1)

    orchestrator = ReanalysisOrchestrator()

    # ── Collect test results mode ──────────────────────────────────────────────
    if args.collect_results:
        if not args.cohorts:
            log.error("--collect-results requires --cohorts to be specified")
            sys.exit(1)

        cohort_list = [c.strip() for c in args.cohorts.split(",") if c.strip()]
        if not cohort_list:
            log.error("--cohorts value is empty or invalid: %s", args.cohorts)
            sys.exit(1)

        log.info("Collecting test results for cohorts: %s", cohort_list)
        results = orchestrator.collect_test_results(cohort_list)
        print(json.dumps(results, indent=2))
        return

    # ── Reanalysis mode ────────────────────────────────────────────────────────
    log.info(
        "Triggering reanalysis: exclude=%s, focus=%s, reason=manual_cli",
        args.exclude,
        args.focus,
    )

    new_cohorts = asyncio.run(
        orchestrator.trigger_reanalysis(
            cohort_to_exclude=args.exclude,
            cohort_to_focus=args.focus,
            reason="manual_cli",
        )
    )

    print(f"Discovered {len(new_cohorts)} new cohort(s):")
    for cohort in new_cohorts:
        name = cohort.get("name", "UNKNOWN")
        pass_rate = cohort.get("pass_rate", "N/A")
        tg = cohort.get("tg_category", "")
        pass_rate_str = f"{pass_rate:.1%}" if isinstance(pass_rate, float) else str(pass_rate)
        print(f"  - {name} [{tg}]: pass_rate={pass_rate_str}")

    if not new_cohorts:
        print("  (no new cohorts found — check logs for details)")

    print()
    print(json.dumps({"status": "ok", "cohorts_discovered": len(new_cohorts)}, indent=2))


if __name__ == "__main__":
    main()

"""One-shot CLI to backfill `creative_image_path` in the campaign registry.

Walks Drive at `<ramp>/<platform>/<cohort_geo>/<angle>.png` and patches any
registry row that ended up with an empty `creative_image_path` because the
PNG render + Drive upload happened AFTER the row was logged (Gemini retry,
async upload).

Usage:
    # Reconcile every ramp + every platform present in the registry.
    doppler run -- python scripts/backfill_creative_paths.py

    # Limit to a specific ramp / platform.
    doppler run -- python scripts/backfill_creative_paths.py --ramp GMR-0020
    doppler run -- python scripts/backfill_creative_paths.py \
        --ramp GMR-0020 --platform linkedin

    # Preview without writing (no patches applied).
    doppler run -- python scripts/backfill_creative_paths.py --dry-run

Idempotent — running twice patches nothing new on the second pass.
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.campaign_registry import (  # noqa: E402
    _load,
    _save,
    _registry_lock,
    reconcile_creative_paths,
)


def _ramps_and_platforms_in_registry() -> dict[str, set[str]]:
    """Scan registry for every (ramp, platform) combo so we don't try to
    reconcile ramps that don't exist in this run's data."""
    out: dict[str, set[str]] = {}
    for rec in _load():
        ramp = rec.get("smart_ramp_id") or ""
        platform = (rec.get("platform") or "linkedin")
        if not ramp:
            continue
        out.setdefault(ramp, set()).add(platform)
    return out


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument(
        "--ramp", type=str, default="",
        help="Reconcile only this ramp_id (e.g. GMR-0020). Default: every ramp in the registry.",
    )
    parser.add_argument(
        "--platform", type=str, default="",
        help="Reconcile only this platform (linkedin|meta|google). Default: every platform present for the ramp.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Preview which (ramp, platform) combos would be reconciled without writing.",
    )
    parser.add_argument(
        "--legacy-positional", action="store_true",
        help=(
            "Enable best-effort positional match for rows without `cohort_geo` "
            "(legacy rows from before the cohort_geo column existed). "
            "Pairs PNGs to rows by (geo_cluster, angle) ordered by created_at, "
            "guarded by a 60-minute time window. Can mis-assign a PNG when "
            "multiple Smart Ramp rows mined the same (cohort × geo). "
            "Safe default: OFF — only exact-match patches land."
        ),
    )
    parser.add_argument(
        "--legacy-window-minutes", type=int, default=60,
        help="Time-window guard for legacy positional fallback (default: 60 minutes).",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
        datefmt="%H:%M:%S",
    )
    log = logging.getLogger("backfill")

    by_ramp = _ramps_and_platforms_in_registry()
    if not by_ramp:
        log.warning("Registry is empty — nothing to do.")
        return 0

    # Filter to the requested ramp / platform.
    if args.ramp:
        by_ramp = {k: v for k, v in by_ramp.items() if k == args.ramp}
        if not by_ramp:
            log.error("No registry rows for ramp_id=%s", args.ramp)
            return 1
    if args.platform:
        by_ramp = {
            k: ({args.platform} & v) for k, v in by_ramp.items()
            if args.platform in v
        }
        by_ramp = {k: v for k, v in by_ramp.items() if v}
        if not by_ramp:
            log.error("No registry rows for platform=%s", args.platform)
            return 1

    if args.dry_run:
        log.info("DRY RUN — would reconcile:")
        for ramp, plats in sorted(by_ramp.items()):
            for plat in sorted(plats):
                log.info("  %s / %s", ramp, plat)
        return 0

    totals = {"patched": 0, "unmatched": 0, "ambiguous_legacy": 0}
    for ramp, plats in sorted(by_ramp.items()):
        for plat in sorted(plats):
            log.info(
                "reconciling %s / %s (legacy_positional=%s) ...",
                ramp, plat, args.legacy_positional,
            )
            stats = reconcile_creative_paths(
                ramp, plat,
                legacy_positional=args.legacy_positional,
                legacy_window_minutes=args.legacy_window_minutes,
            )
            for k in totals:
                totals[k] += stats.get(k, 0)
            log.info(
                "  %s / %s: patched=%d unmatched=%d ambiguous_legacy=%d",
                ramp, plat, stats["patched"], stats["unmatched"],
                stats["ambiguous_legacy"],
            )

    log.info("=" * 60)
    log.info(
        "DONE — patched=%d unmatched=%d ambiguous_legacy=%d",
        totals["patched"], totals["unmatched"], totals["ambiguous_legacy"],
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

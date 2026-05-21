#!/usr/bin/env python3
"""
Backfill cohort_brief_rationale rows from the Campaign Registry sheet.

Usage:
    doppler run -p outlier-campaign-agent -c dev -- \\
        python3 scripts/backfill_rationale_from_registry.py --ramp GMR-0021

When the cleanup-on-prep wipe removes rationale rows for a ramp whose
launch has already shipped, this script reconstructs them from the
registry sheet (which is the source of truth for what actually shipped).
Maps registry columns → rationale schema 1:1 for every static-campaign row
on the ramp.

Idempotent: ON CONFLICT (ramp_id, cohort_id, channel, angle, geo_cluster)
DO UPDATE — re-running safely overwrites with the latest registry state.
"""
import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--ramp", required=True, help="Smart Ramp ID, e.g. GMR-0021")
    parser.add_argument("--dry-run", action="store_true",
                        help="Print rows that would be inserted, don't write")
    parser.add_argument("--log-level", default="INFO",
                        choices=["DEBUG", "INFO", "WARNING", "ERROR"])
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    log = logging.getLogger("backfill_rationale")

    from src.campaign_registry import _load as _load_registry
    from src.ui_decisions import upsert_cohort_brief_rationale

    log.info("Reading local Campaign Registry JSON for ramp=%s", args.ramp)
    all_rows = _load_registry()
    rows = [r for r in all_rows
            if (r.get("smart_ramp_id") or "").strip() == args.ramp
               and (r.get("campaign_type") or "").strip() == "static"
               and (r.get("angle") or "").strip()]
    log.info("Found %d static-campaign rows for ramp=%s (total registry rows: %d)",
             len(rows), args.ramp, len(all_rows))

    if not rows:
        log.warning("No static-campaign rows for %s — nothing to backfill", args.ramp)
        return 1

    seen: set[tuple] = set()
    n_persisted = 0
    for r in rows:
        cohort_id = (r.get("cohort_id") or "").strip()
        cohort_signature = (r.get("cohort_signature") or "").strip()
        geo_cluster = (r.get("geo_cluster") or "").strip()
        channel = (r.get("platform") or r.get("channel") or "").strip().lower()
        angle = (r.get("angle") or "").strip()
        if not all([cohort_id, cohort_signature, channel, angle]):
            continue
        # The cohort_brief_rationale table's UNIQUE key is
        # (ramp_id, cohort_id, channel, angle, geo_cluster). Dedupe locally
        # to avoid sending duplicate upserts within this run.
        key = (cohort_id, channel, angle, geo_cluster or None)
        if key in seen:
            continue
        seen.add(key)

        if args.dry_run:
            log.info(
                "[dry-run] would upsert: cohort=%s channel=%s angle=%s geo=%s headline=%r",
                cohort_signature, channel, angle, geo_cluster or "—",
                (r.get("headline") or "")[:60],
            )
            continue
        try:
            upsert_cohort_brief_rationale(
                ramp_id=args.ramp,
                cohort_id=cohort_id,
                cohort_signature=cohort_signature,
                geo_cluster=geo_cluster or None,
                channel=channel,
                angle=angle,
                angle_label=None,
                headline=(r.get("headline") or "").strip() or None,
                subheadline=(r.get("subheadline") or "").strip() or None,
                photo_subject=(r.get("photo_subject") or "").strip() or None,
                rationale=None,        # original rationale text not in registry
                competitor_signal=None,
                expected_uplift_pp=None,
            )
            n_persisted += 1
        except Exception as exc:
            log.warning("Upsert failed for cohort=%s angle=%s geo=%s: %s",
                        cohort_signature, angle, geo_cluster, exc)

    log.info("Backfill complete: persisted %d rationale rows for ramp=%s "
             "(channels: %s)",
             n_persisted, args.ramp,
             sorted({(r.get("platform") or r.get("channel") or "").lower() for r in rows}))
    return 0


if __name__ == "__main__":
    sys.exit(main())

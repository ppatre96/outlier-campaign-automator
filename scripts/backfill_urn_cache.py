#!/usr/bin/env python3
"""Seed the Postgres LinkedIn URN cache from the legacy URN Google Sheet.

One-time (re-runnable, idempotent) migration. After this runs, `UrnResolver`
reads URNs from Postgres and only touches the sheet if a facet is missing —
so a Google Sheets outage can no longer take a launch down before it reaches
LinkedIn (GMR-0029, 2026-09-04).

    doppler run -- python3 scripts/backfill_urn_cache.py
    doppler run -- python3 scripts/backfill_urn_cache.py --dry-run
    doppler run -- python3 scripts/backfill_urn_cache.py --tabs Skills,Titles
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.linkedin_urn import FACET_TAB_MAP  # noqa: E402
from src.sheets import SheetsClient  # noqa: E402
from src.urn_store import counts, upsert_facet  # noqa: E402

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s %(message)s",
)
log = logging.getLogger("backfill_urn_cache")


def _rows_to_pairs(rows: list[dict]) -> list[dict]:
    """Normalise a sheet tab's rows to `{name, urn}` (same logic as _load_tab).

    The name column is titled differently per tab ('Skills', 'Job Titles',
    'Country', ...), so it's identified by elimination rather than by name.
    """
    out = []
    for row in rows or []:
        urn = str(row.get("urn") or row.get("URN") or "").strip()
        skip = {"urn", "fetched at", "fetched_at"}
        name_key = next((k for k in row.keys() if k.lower() not in skip), None)
        name = str(row.get(name_key, "")).strip() if name_key else ""
        if name and urn:
            out.append({"name": name, "urn": urn})
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--dry-run", action="store_true",
                    help="read the sheet and report counts without writing")
    ap.add_argument("--tabs", default="",
                    help="comma-separated tab subset (default: every mapped tab)")
    args = ap.parse_args()

    tabs = (
        [t.strip() for t in args.tabs.split(",") if t.strip()]
        if args.tabs else sorted(set(FACET_TAB_MAP.values()))
    )
    log.info("Backfilling %d URN tab(s): %s", len(tabs), ", ".join(tabs))

    sheets = SheetsClient()
    total = 0
    failures = 0
    for tab in tabs:
        try:
            pairs = _rows_to_pairs(sheets.read_urn_tab(tab))
        except Exception as exc:
            log.error("Tab %r: could not read from the sheet — %s", tab, exc)
            failures += 1
            continue
        if args.dry_run:
            log.info("Tab %r: %d row(s) would be written", tab, len(pairs))
            total += len(pairs)
            continue
        try:
            n = upsert_facet(tab, pairs)
        except Exception as exc:
            log.error("Tab %r: could not write to Postgres — %s", tab, exc)
            failures += 1
            continue
        log.info("Tab %r: wrote %d row(s)", tab, n)
        total += n

    if args.dry_run:
        log.info("Dry run complete — %d row(s) across %d tab(s), nothing written",
                 total, len(tabs))
        return 1 if failures else 0

    try:
        log.info("Postgres URN cache now holds: %s", counts())
    except Exception as exc:
        log.warning("Could not read back cache counts: %s", exc)
    if failures:
        log.error("Backfill finished with %d tab failure(s)", failures)
        return 1
    log.info("Backfill complete — %d row(s) written", total)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

#!/usr/bin/env python3
"""Clear stale `Master Campaign ID` cells in the Triggers sheet for rows
pointing at archived/missing LinkedIn campaign groups.

Why this exists
---------------
`_retry_li_campaign` (main.py:1477) re-attempts LinkedIn campaign creation
for rows where the cohort was already mined (tg_status=Completed) but the
LinkedIn arm failed or is pending. It reads `master_campaign` and tries to
attach a new campaign under that group URN. If the group has been archived,
LinkedIn returns 400 FIELD_VALUE_DOES_NOT_EXIST and the retry path short-
circuits — but the row stays broken across runs.

The fix is structural (clear master_campaign so the next launch creates a
fresh group under a current name). This script does that cleanup:

  1. Reads every Triggers-sheet row.
  2. Filters to retry-eligible rows (tg_status=COMPLETED,
     li_status in {FAILED, PENDING}) that still reference a non-empty
     master_campaign.
  3. Probes each distinct group URN against LinkedIn once.
  4. Reports/clears the master_campaign column for rows pointing at
     dead groups (404 / unreachable).

Usage
-----
    doppler run -- python3 scripts/clear_stale_master_campaign.py
        # dry-run by default

    doppler run -- python3 scripts/clear_stale_master_campaign.py --apply
        # writes empty strings to the master_campaign cells

    doppler run -- python3 scripts/clear_stale_master_campaign.py --all-rows
        # also probes/clears rows outside the retry-eligible set
        # (use sparingly — typically the retry-eligible filter is what
        # you want, since live campaigns SHOULD keep their master_campaign)
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Optional

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config
from src.linkedin_api import LinkedInClient
from src.sheets import SheetsClient, COL


def _group_alive(client: LinkedInClient, grp_id: str) -> Optional[bool]:
    """Return True if the campaign group is reachable on LinkedIn, False if
    it 404s, None on any other (transient) error — caller treats None as
    "leave it alone" rather than guess."""
    url = (
        f"https://api.linkedin.com/rest/adAccounts/"
        f"{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaignGroups/{grp_id}"
    )
    try:
        resp = client._req("GET", url)
    except Exception as exc:
        print(f"  WARN: GET {grp_id} raised {type(exc).__name__}: {exc}")
        return None
    if resp.ok:
        return True
    if resp.status_code == 404:
        return False
    # 400/403/5xx — uncertain. Don't auto-clear; surface to caller.
    print(f"  WARN: GET {grp_id} → {resp.status_code} {resp.text[:120]!r}")
    return None


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--apply", action="store_true",
                        help="Write empty strings to the cells (default: dry-run)")
    parser.add_argument("--all-rows", action="store_true",
                        help="Probe master_campaign for ALL rows, not just retry-eligible ones")
    args = parser.parse_args()

    sheets = SheetsClient()
    if not config.LINKEDIN_TOKEN:
        print("ERROR: LINKEDIN_TOKEN unset — wrap the command in `doppler run --` "
              "or export the token directly.", file=sys.stderr)
        return 2
    li = LinkedInClient(token=config.LINKEDIN_TOKEN)

    ws = sheets._triggers.worksheet(config.TRIGGERS_TAB)
    all_rows = ws.get_all_values()
    if len(all_rows) < 2:
        print("Triggers sheet is empty — nothing to do.")
        return 0

    mc_idx = COL["master_campaign"]
    tg_idx = COL["tg_status"]
    li_idx = COL["li_status"]
    max_col = max(COL.values())

    # Collect (group_id → [(row_idx_1based, stg_id, status_pair)])
    candidates: dict[str, list[tuple[int, str, str]]] = {}
    skipped_non_retry = 0
    for row_idx, row in enumerate(all_rows[1:], start=2):
        # Pad to handle short rows
        while len(row) <= max_col:
            row.append("")
        mc = row[mc_idx].strip()
        if not mc:
            continue
        tg = row[tg_idx].strip().upper()
        li_st = row[li_idx].strip().upper()
        retry_eligible = tg == "COMPLETED" and li_st in ("FAILED", "PENDING")
        if not retry_eligible and not args.all_rows:
            skipped_non_retry += 1
            continue
        stg_id = row[COL["stg_id"]].strip() or "?"
        candidates.setdefault(mc, []).append((row_idx, stg_id, f"{tg}/{li_st}"))

    if skipped_non_retry:
        print(f"Skipped {skipped_non_retry} row(s) with master_campaign set "
              f"but li_status outside FAILED/PENDING (pass --all-rows to include).")

    if not candidates:
        print("\nNo retry-eligible rows reference a master_campaign. Nothing to do.")
        return 0

    total_rows = sum(len(v) for v in candidates.values())
    print(f"\nProbing {len(candidates)} distinct campaign group(s) "
          f"across {total_rows} row(s)...")

    dead: dict[str, list[tuple[int, str, str]]] = {}
    for grp, rows in sorted(candidates.items()):
        alive = _group_alive(li, grp)
        if alive is True:
            print(f"  [ALIVE] {grp}: {len(rows)} row(s) — keeping")
        elif alive is False:
            print(f"  [DEAD ] {grp}: {len(rows)} row(s)")
            for row_idx, stg_id, status in rows:
                print(f"           row {row_idx} (stg_id={stg_id}, status={status})")
            dead[grp] = rows
        else:
            print(f"  [SKIP ] {grp}: probe inconclusive — not clearing")

    if not dead:
        print("\nNo dead groups found. Sheet is clean.")
        return 0

    total_dead = sum(len(v) for v in dead.values())
    print(f"\n{total_dead} row(s) reference {len(dead)} dead group(s).")
    if not args.apply:
        print("Dry-run: re-run with --apply to clear the cells.")
        return 0

    col_1based = mc_idx + 1
    cleared = 0
    for grp, rows in dead.items():
        for row_idx, stg_id, _status in rows:
            ws.update_cell(row_idx, col_1based, "")
            cleared += 1
            print(f"  cleared row {row_idx} (was {grp}, stg_id={stg_id})")
    print(f"\nDone — cleared {cleared} master_campaign cell(s). "
          f"Next launch will create a fresh group for each row.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

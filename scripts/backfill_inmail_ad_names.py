#!/usr/bin/env python3
"""One-off backfill: rename existing InMail ads from the legacy `inmail_<ts>`
label to the readable pipe-delimited spec name + angle (see PR #25).

For every linkedin/inmail creative in the Postgres campaign registry it:
  1. GETs the sponsoredCreative to resolve its backing inMailContents URN
     (`content.reference`),
  2. computes the new name = "<campaign_name> | Angle <A|B|C>",
  3. PATCHes the inMailContents name (Rest.li PARTIAL_UPDATE).

Dry-run by default (prints the plan, changes nothing). Pass --apply to write.
Scope to one ramp with --ramp-id GMR-0023. Per-creative failures are logged
and skipped (one archived/missing ad never aborts the rest).

    doppler run -- python3 scripts/backfill_inmail_ad_names.py            # dry-run all
    doppler run -- python3 scripts/backfill_inmail_ad_names.py --ramp-id GMR-0023 --apply
"""
from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.ui_decisions import _connect  # noqa: E402
from src.linkedin_api import LinkedInClient  # noqa: E402
import config  # noqa: E402


def _rows(ramp_id: str | None) -> list[dict]:
    sql = (
        "SELECT ramp_id, campaign_name, angle, cohort_signature, platform_creative_id "
        "FROM campaigns "
        "WHERE platform='linkedin' AND campaign_type='inmail' "
        "AND coalesce(platform_creative_id,'') <> '' "
    )
    args: list = []
    if ramp_id:
        sql += "AND ramp_id = %s "
        args.append(ramp_id)
    sql += "GROUP BY ramp_id, campaign_name, angle, cohort_signature, platform_creative_id "
    sql += "ORDER BY ramp_id, campaign_name, angle"
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(sql, tuple(args))
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, r)) for r in cur.fetchall()]


def _new_name(row: dict) -> str:
    base = (row.get("campaign_name") or "").strip()
    if not base:
        base = f"Scale-{row['ramp_id']} | LinkedIn | InMail | {(row.get('cohort_signature') or '').strip()}"
    angle = (row.get("angle") or "").strip() or "A"
    return f"{base} | Angle {angle}"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ramp-id", default=None)
    ap.add_argument("--apply", action="store_true", help="actually PATCH (default: dry-run)")
    args = ap.parse_args()

    rows = _rows(args.ramp_id)
    if not rows:
        print("No InMail creatives found in the registry for that scope.")
        return 0

    client = LinkedInClient(token=config.LINKEDIN_TOKEN)
    # dedup creatives (the registry can hold >1 row per creative)
    seen: set[str] = set()
    ok = fail = skipped = 0
    for row in rows:
        cre = row["platform_creative_id"]
        if cre in seen:
            continue
        seen.add(cre)
        new_name = _new_name(row)
        try:
            creative = client.get_creative(cre)
            content_ref = (creative.get("content") or {}).get("reference")
            if not content_ref:
                print(f"SKIP  {cre} — no content.reference (not a Message Ad?)")
                skipped += 1
                continue
            if args.apply:
                client.rename_inmail_content(content_ref, new_name)
                print(f"DONE  {content_ref}  →  {new_name}")
                ok += 1
            else:
                print(f"PLAN  {cre}  ({content_ref})  →  {new_name}")
                ok += 1
        except Exception as exc:
            print(f"FAIL  {cre} — {type(exc).__name__}: {str(exc)[:160]}")
            fail += 1

    mode = "APPLIED" if args.apply else "DRY-RUN"
    print(f"\n{mode}: {ok} {'renamed' if args.apply else 'planned'}, {skipped} skipped, {fail} failed "
          f"({len(seen)} distinct creatives).")
    return 0 if fail == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())

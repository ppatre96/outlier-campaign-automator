"""Daily snapshot of per-project RTP config → Google Sheet "RTP Config Snapshots".

Why: `PUBLIC.REFERTOPROJECTCONFIGS` is one row per project, overwritten in place. There is no
history anywhere in Snowflake (no Time Travel, no changelog — see memory
reference_rtp_config_history_absent), so the only way to know *when* RTP or Global RTP was
switched on/off for a project is to snapshot it ourselves. This job starts that record.

Tabs:
  snapshots      full snapshot appended once per UTC date (snapshot_date + every column).
  changes        one row per field that differs from the previous snapshot. This is the on/off
                 log — filter field = enabled / global_enabled. A project appearing for the first
                 time logs `_project: "" -> added` plus its enabled/global_enabled values; one
                 disappearing logs `_project: present -> removed`.
  current_state  overwritten each run with the latest snapshot, for eyeballing.

Idempotent per date: if `snapshots` already has rows for the date, the run is a no-op. The
snapshot is appended LAST, so it acts as the commit marker; any `changes` rows a failed run left
behind for that date are deleted before being rewritten.

Run (CI: .github/workflows/rtp_config_snapshot.yml, daily):
    doppler run -- python3 scripts/rtp_config_snapshot.py
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import time
from datetime import datetime, timezone

import gspread
import requests

log = logging.getLogger("rtp_config_snapshot")

# Shared Drive "Outlier Campaign Agent" (0ALHAgK4RPbnfUk9PVA), owned by the pipeline's service
# account. Not a secret, so it lives here; override with RTP_SNAPSHOT_GSHEET_ID.
DEFAULT_SHEET_ID = "1YoflKvs-lD7OrnQIP75rBtt5huGivhW63IKwkxrdRFI"

REDASH_URL = os.getenv("REDASH_URL", "https://redash.scale.com").rstrip("/")
# redash.scale.com's WAF 403s the ?api_key= form and non-browser User-Agents on POST.
BROWSER_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
              "(KHTML, like Gecko) Chrome/124.0 Safari/537.36")

SQL = """
SELECT
    c.PROJECT_ID                                AS project_id,
    p.PROJECT_NAME                              AS project_name,
    p.PERMISSION_GROUP_NAME                     AS permission_group_name,
    p.OPS_POD                                   AS ops_pod,
    p.PROJECT_STATUS                            AS project_status,
    c.ENABLED                                   AS enabled,
    c.GLOBAL_ENABLED                            AS global_enabled,
    c.GLOBAL_DESCRIPTION                        AS global_description,
    c.ELIGIBILITY_THRESHOLD                     AS eligibility_threshold,
    c.REWARD_THRESHOLD                          AS reward_threshold,
    c.REWARD_AMOUNT_HUNDREDTHS_OF_CENTS         AS reward_amount_hundredths_of_cents,
    TO_JSON(c.FILTERS)                          AS filters,
    c.SIGNUP_FLOW_ID                            AS signup_flow_id,
    TO_VARCHAR(c.CREATED_AT,  'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM') AS cfg_created_at,
    TO_VARCHAR(c.UPDATED_AT,  'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM') AS cfg_updated_at,
    TO_VARCHAR(c.__DUMP_TIME, 'YYYY-MM-DD"T"HH24:MI:SS.FF3TZH:TZM') AS source_dump_time
FROM SCALE_PROD.PUBLIC.REFERTOPROJECTCONFIGS c
LEFT JOIN SCALE_PROD.VIEW.DIM_PROJECTS p
    ON p.PROJECT_ID = c.PROJECT_ID
WHERE COALESCE(c.__DELETED, FALSE) = FALSE
ORDER BY c.PROJECT_ID
"""

DATA_COLUMNS = [
    "project_id", "project_name", "permission_group_name", "ops_pod", "project_status",
    "enabled", "global_enabled", "global_description", "eligibility_threshold",
    "reward_threshold", "reward_amount_hundredths_of_cents", "filters", "signup_flow_id",
    "cfg_created_at", "cfg_updated_at", "source_dump_time",
]
SNAPSHOT_COLUMNS = ["snapshot_date", "snapshot_at_utc"] + DATA_COLUMNS
CHANGE_COLUMNS = ["snapshot_date", "project_id", "project_name", "field", "old_value",
                  "new_value", "cfg_updated_at"]
# Sync/bookkeeping columns move on every write and would drown the real changes.
NOT_DIFFED = {"project_id", "cfg_created_at", "cfg_updated_at", "source_dump_time"}
DIFFED = [c for c in DATA_COLUMNS if c not in NOT_DIFFED]


def fmt(v) -> str:
    """One string form per value, so a value read back from the sheet compares equal."""
    if v is None:
        return ""
    if isinstance(v, bool):
        return "true" if v else "false"
    if isinstance(v, float) and v.is_integer():
        return str(int(v))
    if isinstance(v, (dict, list)):
        return json.dumps(v, sort_keys=True, separators=(",", ":"))
    return str(v)


def fetch_config(timeout: int = 300) -> list[dict]:
    key = os.environ["REDASH_API_KEY"]
    headers = {"Authorization": f"Key {key}", "User-Agent": BROWSER_UA,
               "Content-Type": "application/json"}

    def call(method, path, body=None):
        r = requests.request(method, REDASH_URL + path, headers=headers, json=body, timeout=120)
        r.raise_for_status()
        return r.json()

    res = call("POST", "/api/query_results",
               {"query": SQL, "data_source_id": int(os.getenv("REDASH_DATA_SOURCE_ID", "30")),
                "max_age": 0})
    if "job" in res:
        job_id, began = res["job"]["id"], time.time()
        while True:
            job = call("GET", f"/api/jobs/{job_id}")["job"]
            if job["status"] == 3:
                res = call("GET", f"/api/query_results/{job['query_result_id']}")
                break
            if job["status"] == 4:
                raise RuntimeError(f"Redash query failed: {job.get('error')}")
            if time.time() - began > timeout:
                raise RuntimeError(f"Redash query timed out after {timeout}s")
            time.sleep(3)
    rows = [{k.lower(): fmt(v) for k, v in r.items()} for r in res["query_result"]["data"]["rows"]]
    if not rows:
        raise RuntimeError("REFERTOPROJECTCONFIGS returned 0 rows — refusing to snapshot nothing")
    if len({r["project_id"] for r in rows}) != len(rows):
        raise RuntimeError("duplicate project_id in source — the DIM_PROJECTS join fanned out")
    return rows


def tab(sh, title: str, columns: list[str]):
    try:
        ws = sh.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=title, rows=1000, cols=len(columns))
    if ws.row_values(1) != columns:
        if ws.row_values(1):
            raise RuntimeError(f"tab {title!r} header differs from the code — migrate it by hand")
        ws.update(values=[columns], range_name="A1")
        ws.freeze(rows=1)
    return ws


def previous_snapshot(ws, date: str) -> tuple[bool, list[dict]]:
    """(already_done_for_date, rows of the latest earlier snapshot)."""
    dates = ws.col_values(1)[1:]          # snapshots are appended in date order
    if date in dates:
        return True, []
    if not dates:
        return False, []
    last = dates[-1]
    first_line = dates.index(last) + 2    # +1 header, +1 for 1-based rows
    header = ws.row_values(1)
    values = ws.get(f"A{first_line}:{gspread.utils.rowcol_to_a1(len(dates) + 1, len(header))}")
    return False, [dict(zip(header, list(r) + [""] * (len(header) - len(r)))) for r in values]


def diff(prev: list[dict], cur: list[dict], date: str) -> list[list[str]]:
    before = {r["project_id"]: r for r in prev}
    after = {r["project_id"]: r for r in cur}
    out = []
    for pid, new in after.items():
        old = before.get(pid)
        row = lambda f, o, n: [date, pid, new["project_name"], f, o, n, new["cfg_updated_at"]]
        if old is None:
            out.append(row("_project", "", "added"))
            out += [row(f, "", new[f]) for f in ("enabled", "global_enabled")]
            continue
        out += [row(f, old.get(f, ""), new[f]) for f in DIFFED if old.get(f, "") != new[f]]
    for pid, old in before.items():
        if pid not in after:
            out.append([date, pid, old.get("project_name", ""), "_project", "present", "removed",
                        old.get("cfg_updated_at", "")])
    return out


def drop_rows_for_date(ws, date: str) -> None:
    """Delete `changes` rows a failed earlier run left for `date` (always the trailing block)."""
    lines = [i + 1 for i, d in enumerate(ws.col_values(1)) if i and d == date]
    if lines:
        assert lines == list(range(lines[0], lines[-1] + 1)), "leftover rows are not contiguous"
        ws.delete_rows(lines[0], lines[-1])
        log.warning("removed %d leftover changes rows for %s", len(lines), date)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--sheet-id", default=os.getenv("RTP_SNAPSHOT_GSHEET_ID") or DEFAULT_SHEET_ID)
    ap.add_argument("--credentials", default=os.getenv("GOOGLE_CREDENTIALS", "credentials.json"))
    args = ap.parse_args()

    now = datetime.now(timezone.utc)
    date = now.strftime("%Y-%m-%d")

    sh = gspread.service_account(filename=args.credentials).open_by_key(args.sheet_id)
    snaps = tab(sh, "snapshots", SNAPSHOT_COLUMNS)
    changes = tab(sh, "changes", CHANGE_COLUMNS)
    current = tab(sh, "current_state", SNAPSHOT_COLUMNS)

    done, prev = previous_snapshot(snaps, date)
    if done:
        log.info("snapshot for %s already exists — nothing to do", date)
        return 0

    cur = fetch_config()
    stamp = now.strftime("%Y-%m-%dT%H:%M:%SZ")
    snap_rows = [[date, stamp] + [r[c] for c in DATA_COLUMNS] for r in cur]

    drop_rows_for_date(changes, date)
    if prev:
        change_rows = diff(prev, cur, date)
        if change_rows:
            changes.append_rows(change_rows, value_input_option="RAW")
    else:
        change_rows = []
        changes.update_note("A1", f"baseline {date}: first snapshot ({len(cur)} projects). "
                                  "Changes are logged from the next day's run onward.")
    # Appended last: its presence for `date` is what marks the run complete.
    snaps.append_rows(snap_rows, value_input_option="RAW")

    current.batch_clear(["A2:Z"])
    current.update(values=snap_rows, range_name="A2", value_input_option="RAW")

    flips = [r for r in change_rows if r[3] in ("enabled", "global_enabled")]
    log.info("%s: %d projects snapshotted, %d field changes (%d enabled/global_enabled)%s",
             date, len(cur), len(change_rows), len(flips), "" if prev else " — baseline")
    for r in flips:
        log.info("  %s %s (%s): %s -> %s", r[3], r[1], r[2], r[4] or "∅", r[5] or "∅")
    return 0


if __name__ == "__main__":
    sys.exit(main())

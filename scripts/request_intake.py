#!/usr/bin/env python3
"""Poll the Slack-workflow request sheet and queue new requests as GitHub issues.

Diego, Bryan and Tuan raise requests through a Slack workflow form in the group
DM (C0B34UJ6D7H). The workflow appends each response to a Google Sheet. This
script is the cloud half of the intake loop, run by
`.github/workflows/request_intake.yml` every 15 minutes:

    sheet row  →  GitHub issue  →  (Claude picks it up)  →  PR + reply in the
                                                            request's Slack thread

It deliberately does NOT try to post to Slack. The account's stored
SLACK_BOT_TOKEN is a rotating user token that is currently `invalid_auth`, so CI
has no working Slack credential. All Slack traffic is handled by Claude, which
does.

Slack shape (decided with Pranav 2026-08-04): the Slack workflow appends to the
sheet and posts NOTHING in the channel — verified by a channel read plus two
bot-inclusive searches, neither of which found the submission. So there is no
"workflow message" to reply under. Instead the worker posts ONE anchor message per
ticket into the group DM and confines every reply to that thread, recording the
anchor's `ts` as a `slack-thread-ts:` comment on this issue so a re-run reuses the
thread rather than posting a second anchor.

State lives in the SHEET, not on disk — CI runners are ephemeral, and a Status
column is also visible to the people who filed the request. A row with a
non-empty Status is never queued twice.

Usage:
    python3 scripts/request_intake.py            # queue new rows
    python3 scripts/request_intake.py --dry-run  # report only, touch nothing
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

log = logging.getLogger("request_intake")

SHEET_ID = os.getenv(
    "REQUEST_SHEET_ID", "1g-c8hVWDpXreDAOQ8IA8CFjYYYex563BfWMga0QJgD0"
)
REPO = os.getenv("REQUEST_INTAKE_REPO", "ppatre96/outlier-campaign-automator")
ISSUE_LABEL = "campaign-tool-request"
SLACK_CHANNEL = "C0B34UJ6D7H"

# Header names the Slack workflow writes. Matched case-insensitively on a
# normalized form so a renamed-but-recognisable column still resolves.
COL_PRIORITY = "priority"
COL_TYPE = "type of request"
COL_BODY = "describe your request"
COL_WHO = "submitted by"
COL_WHEN = "timestamp"
# Columns this script owns.
COL_STATUS = "status"
COL_ISSUE = "issue"


def _norm(s: str) -> str:
    return " ".join((s or "").strip().lower().split())


def _resolve_columns(header: list[str]) -> dict[str, int]:
    """Map our logical column names to 0-based indices in the sheet header."""
    idx = {_norm(h): i for i, h in enumerate(header)}
    out: dict[str, int] = {}
    for key in (COL_PRIORITY, COL_TYPE, COL_BODY, COL_WHO, COL_WHEN, COL_STATUS, COL_ISSUE):
        if key in idx:
            out[key] = idx[key]
    # "Nature of request" is the header on the other draft of this form — accept
    # either so a swap of the bound sheet doesn't silently drop the field.
    if COL_TYPE not in out and "nature of request" in idx:
        out[COL_TYPE] = idx["nature of request"]
    missing = [c for c in (COL_BODY, COL_WHO, COL_WHEN) if c not in out]
    if missing:
        raise SystemExit(
            f"request_intake: sheet header is missing required column(s) {missing}. "
            f"Found: {header}"
        )
    return out


def _cell(row: list[str], cols: dict[str, int], key: str) -> str:
    i = cols.get(key)
    if i is None or i >= len(row):
        return ""
    return (row[i] or "").strip()


def _issue_body(row: list[str], cols: dict[str, int], row_number: int) -> str:
    who = _cell(row, cols, COL_WHO) or "(unknown)"
    when = _cell(row, cols, COL_WHEN) or "(no timestamp)"
    prio = _cell(row, cols, COL_PRIORITY) or "(unset)"
    kind = _cell(row, cols, COL_TYPE) or "(unset)"
    body = _cell(row, cols, COL_BODY) or "(empty)"
    return "\n".join([
        f"**Raised by:** {who}",
        f"**Submitted:** {when}",
        f"**Priority:** {prio}",
        f"**Type:** {kind}",
        "",
        "## Request",
        "",
        body,
        "",
        "---",
        "",
        "## How to work this",
        "",
        "1. **Read the code before concluding anything.** Several past requests turned"
        " out to be features that already existed — if so, do not open a PR; reply in"
        " the Slack thread saying where it lives.",
        "2. If it is a real gap, branch, implement, add tests, run the suite, open a PR."
        " Never merge — Pranav reviews.",
        "3. If it is ambiguous, needs a product decision, reverses an earlier deliberate"
        " choice, or asks for an unverifiable claim in ad copy, open no PR and ask in"
        " the thread instead.",
        f"4. **Slack: one anchor message per ticket in <#{SLACK_CHANNEL}>, then reply"
        " only in its thread.** The Slack workflow writes to the sheet and posts"
        " nothing in the channel (verified 2026-08-04), so there is no workflow"
        " message to reply to — the worker posts the anchor itself. If this issue"
        " already has a `slack-thread-ts:` comment, REUSE that thread instead of"
        " posting a second anchor. Never DM.",
        "",
        f"_Queued automatically from sheet row {row_number} by"
        " `scripts/request_intake.py`._",
    ])


def _issue_title(row: list[str], cols: dict[str, int]) -> str:
    body = _cell(row, cols, COL_BODY) or "request"
    kind = _cell(row, cols, COL_TYPE)
    who = _cell(row, cols, COL_WHO)
    first = " ".join(body.split())
    if len(first) > 80:
        first = first[:77].rstrip() + "…"
    prefix = f"[{kind}] " if kind else ""
    suffix = f" ({who})" if who else ""
    return f"{prefix}{first}{suffix}"[:240]


def _ensure_label() -> None:
    """Create the label once; ignore "already exists"."""
    subprocess.run(
        ["gh", "label", "create", ISSUE_LABEL, "--repo", REPO,
         "--description", "Raised by Diego/Bryan/Tuan via the Slack workflow form",
         "--color", "1D76DB"],
        capture_output=True, text=True, check=False,
    )


def _create_issue(title: str, body: str) -> str:
    _ensure_label()
    res = subprocess.run(
        ["gh", "issue", "create", "--repo", REPO, "--title", title,
         "--body", body, "--label", ISSUE_LABEL],
        capture_output=True, text=True, check=False,
    )
    if res.returncode != 0:
        raise RuntimeError(f"gh issue create failed: {res.stderr.strip()[:300]}")
    return (res.stdout or "").strip().splitlines()[-1].strip()


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true",
                    help="report new rows without creating issues or writing back")
    args = ap.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from src.sheets import SheetsClient

    try:
        ss = SheetsClient()._gc.open_by_key(SHEET_ID)
    except Exception as exc:
        log.error(
            "cannot open the request sheet (%s: %s). Share %s with the service "
            "account as Editor: the sheet is owned by Pranav and the runner "
            "authenticates as outlier-sheets-agent@outlier-campaign-agent.iam."
            "gserviceaccount.com",
            type(exc).__name__, str(exc)[:160], SHEET_ID,
        )
        return 1

    ws = ss.get_worksheet(0)
    values = ws.get_all_values()
    if not values:
        log.info("sheet is empty (no header) — nothing to do")
        return 0

    header = values[0]
    cols = _resolve_columns(header)

    # Add our own columns on first run so the team sees status in the sheet.
    appended = False
    for own in (COL_STATUS, COL_ISSUE):
        if own not in cols:
            header.append(own.title())
            cols[own] = len(header) - 1
            appended = True
    if appended and not args.dry_run:
        ws.update(range_name=f"A1:{chr(ord('A') + len(header) - 1)}1", values=[header])
        log.info("added Status/Issue columns to the sheet header")

    queued = 0
    for row_number, row in enumerate(values[1:], start=2):
        # A row with any content in the required fields but no Status is new.
        if not _cell(row, cols, COL_BODY):
            continue
        if _cell(row, cols, COL_STATUS):
            continue

        title = _issue_title(row, cols)
        body = _issue_body(row, cols, row_number)
        who = _cell(row, cols, COL_WHO)
        if args.dry_run:
            log.info("[dry-run] would queue row %d: %s", row_number, title)
            queued += 1
            continue

        try:
            url = _create_issue(title, body)
        except Exception as exc:
            log.error("row %d: %s", row_number, exc)
            continue

        # Write status back BEFORE anything else can re-read the sheet, so a
        # retry or an overlapping run cannot double-queue this row.
        status_col = chr(ord("A") + cols[COL_STATUS])
        issue_col = chr(ord("A") + cols[COL_ISSUE])
        ws.update(range_name=f"{status_col}{row_number}", values=[["QUEUED"]])
        ws.update(range_name=f"{issue_col}{row_number}", values=[[url]])
        log.info("row %d queued → %s (%s)", row_number, url, who or "unknown")
        queued += 1

    log.info("%d new request(s) queued", queued)
    # Emit a machine-readable line so the workflow step can surface it.
    print(json.dumps({"queued": queued}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

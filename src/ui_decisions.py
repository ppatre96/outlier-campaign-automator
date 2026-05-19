"""Postgres-backed approval gate for the outlier-campaign-console UI.

Wraps the `ramp_decisions` + `ramp_audit_log` tables (schema in
`scripts/sql/001_ramp_decisions.sql`) with a small typed API that the
pipeline + Vercel app both consume. Connects to Vercel Postgres via the
`DATABASE_URL` env var (Doppler-injected in CI + locally).

Design notes:
- Connection per call. No pool. Pipeline is batch + serverless-friendly.
  Connection cost is amortised across a multi-minute ramp run.
- `UIDecisionsUnavailable` is raised when the DB can't be reached.
  Callers decide fail-open vs fail-closed based on `UI_GATE_ENABLED`.
  This module NEVER guesses for them.
- `claim_ramp` is the atomic anti-double-launch primitive: a single
  UPDATE … WHERE status IN ('approved','yolo') RETURNING flips to
  'launching' so the poller and the UI can't both fire the workflow.
"""
from __future__ import annotations

import json
import logging
import os
from dataclasses import dataclass
from typing import Optional

try:
    import psycopg
except ImportError:                                            # pragma: no cover
    psycopg = None                                             # type: ignore

log = logging.getLogger(__name__)


class UIDecisionsUnavailable(Exception):
    """Raised when the Postgres backend isn't reachable.

    Callers must decide fail-open (proceed with legacy behavior) vs
    fail-closed (skip the ramp) based on `config.UI_GATE_ENABLED`.
    """


@dataclass
class Decision:
    ramp_id:        str
    status:         str           # ramp_status enum value as string
    channels:       list[str]     # subset of {'linkedin','meta','google'}
    budgets:        dict[str, int]  # cents/day per channel
    decided_by:     Optional[str] = None
    decided_at:     Optional[str] = None   # ISO 8601
    version:        int = 1
    matched_domain: Optional[str] = None
    requester_name: Optional[str] = None
    summary:        Optional[str] = None
    submitted_at:   Optional[str] = None


def _connect():
    if psycopg is None:
        raise UIDecisionsUnavailable("psycopg not installed (pip install 'psycopg[binary]')")
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise UIDecisionsUnavailable("DATABASE_URL is not set")
    try:
        return psycopg.connect(url, autocommit=False, connect_timeout=10)
    except psycopg.OperationalError as exc:                    # pragma: no cover
        raise UIDecisionsUnavailable(f"connection failed: {exc}") from exc


_DECISION_COLS = (
    "ramp_id, status::text, channels, budgets, decided_by, decided_at, "
    "version, matched_domain, requester_name, summary, submitted_at"
)


def _row_to_decision(row) -> Decision:
    return Decision(
        ramp_id=row[0],
        status=row[1],
        channels=list(row[2] or []),
        budgets=dict(row[3] or {}),
        decided_by=row[4],
        decided_at=row[5].isoformat() if row[5] else None,
        version=row[6],
        matched_domain=row[7],
        requester_name=row[8],
        summary=row[9],
        submitted_at=row[10].isoformat() if row[10] else None,
    )


# ── Read ──────────────────────────────────────────────────────────────────────

def get_decision(ramp_id: str) -> Optional[Decision]:
    """Fetch the current decision row. Returns None if the ramp hasn't
    been prepped yet."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(f"SELECT {_DECISION_COLS} FROM ramp_decisions WHERE ramp_id = %s",
                    (ramp_id,))
        row = cur.fetchone()
        return _row_to_decision(row) if row else None


# ── Write (pipeline-side) ─────────────────────────────────────────────────────

def upsert_awaiting_approval(
    ramp_id: str,
    *,
    matched_domain: str = "",
    requester_name: str = "",
    summary: str = "",
    submitted_at=None,
    prep_summary: Optional[dict] = None,
) -> None:
    """Pipeline calls this after `_prep_ramp` finishes. Inserts a new row at
    `awaiting_approval`, or updates prep metadata on an existing row WITHOUT
    downgrading an already-decided status (approved/yolo/launching/completed).
    Also writes a `prep_complete` audit-log row."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ramp_decisions (
                ramp_id, status, matched_domain, requester_name,
                summary, submitted_at
            )
            VALUES (%s, 'awaiting_approval'::ramp_status, %s, %s, %s, %s)
            ON CONFLICT (ramp_id) DO UPDATE SET
                matched_domain = EXCLUDED.matched_domain,
                requester_name = EXCLUDED.requester_name,
                summary        = EXCLUDED.summary,
                submitted_at   = EXCLUDED.submitted_at,
                status = CASE
                    WHEN ramp_decisions.status = 'prep_running'::ramp_status
                        THEN 'awaiting_approval'::ramp_status
                    ELSE ramp_decisions.status
                END
            """,
            (ramp_id, matched_domain, requester_name, summary, submitted_at),
        )
        cur.execute(
            "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
            "VALUES (%s, %s, %s::jsonb, %s)",
            (ramp_id, "prep_complete", json.dumps(prep_summary or {}), None),
        )
        conn.commit()


def claim_ramp(ramp_id: str) -> Optional[Decision]:
    """Atomic claim. Returns the Decision if status was approved/yolo and
    is now launching; returns None otherwise. The poller calls this just
    before invoking `_launch_ramp` so concurrent poller ticks / UI clicks
    can never double-launch."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE ramp_decisions
               SET status = 'launching'::ramp_status,
                   version = version + 1
             WHERE ramp_id = %s
               AND status IN ('approved'::ramp_status, 'yolo'::ramp_status)
         RETURNING {_DECISION_COLS}
            """,
            (ramp_id,),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        cur.execute(
            "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
            "VALUES (%s, 'launching', %s::jsonb, %s)",
            (ramp_id, json.dumps({"prior_status": "approved_or_yolo"}), None),
        )
        conn.commit()
        return _row_to_decision(row)


def update_status(
    ramp_id: str,
    new_status: str,
    *,
    by_user: Optional[str] = None,
    payload: Optional[dict] = None,
) -> None:
    """Flip the decision row's status (launching → completed/failed, etc.)
    and append an audit-log row in the same transaction."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE ramp_decisions SET status = %s::ramp_status WHERE ramp_id = %s",
            (new_status, ramp_id),
        )
        cur.execute(
            "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
            "VALUES (%s, %s, %s::jsonb, %s)",
            (ramp_id, f"status_{new_status}", json.dumps(payload or {}), by_user),
        )
        conn.commit()


def log_event(
    ramp_id: str,
    event_type: str,
    payload: dict,
    *,
    by_user: Optional[str] = None,
) -> None:
    """Append an audit-log row without touching `ramp_decisions`. Best-effort
    — swallows UIDecisionsUnavailable so calling sites can be liberal with
    instrumentation without worrying about a DB outage halting the pipeline."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
                "VALUES (%s, %s, %s::jsonb, %s)",
                (ramp_id, event_type, json.dumps(payload), by_user),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("ramp_audit_log: skipping %s/%s (%s)", ramp_id, event_type, exc)


# ── Write (UI-side) ───────────────────────────────────────────────────────────
#
# The Next.js console uses @vercel/postgres directly — it doesn't import
# this module. Documenting the expected statements here so both sides stay
# in sync:
#
#   approve:   UPDATE ramp_decisions SET status='approved', channels=$1,
#              budgets=$2, decided_by=$3, decided_at=NOW(), version=version+1
#              WHERE ramp_id=$4 AND status='awaiting_approval'
#              (Console also INSERTs a 'approved' row into ramp_audit_log.)
#
#   yolo:      same shape with status='yolo', channels=['linkedin','meta','google']
#              budgets=defaults_from_constants
#
#   reset:     UPDATE ramp_decisions SET status='awaiting_approval', ...
#              WHERE status IN ('approved','yolo')  -- can't un-claim once launching

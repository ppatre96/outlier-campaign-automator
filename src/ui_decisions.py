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
from dataclasses import dataclass, field
from typing import Any, Optional

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


def upsert_cohort_audience(
    *,
    ramp_id: str,
    cohort_id: str,
    cohort_signature: str,
    platform: str,
    audience_size: Optional[int],
    status: str,
    geos_used: Optional[list[str]] = None,
    rules_dropped: int = 0,
) -> None:
    """Persist a per-channel audience estimate for a (ramp × cohort × platform).

    Idempotent on (ramp_id, cohort_signature, platform). Best-effort: swallows
    UIDecisionsUnavailable so a Postgres outage never blocks cohort selection.
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cohort_audience (
                    ramp_id, cohort_id, cohort_signature, platform,
                    audience_size, status, geos_used, rules_dropped
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb, %s)
                ON CONFLICT (ramp_id, cohort_signature, platform) DO UPDATE SET
                    cohort_id     = EXCLUDED.cohort_id,
                    audience_size = EXCLUDED.audience_size,
                    status        = EXCLUDED.status,
                    geos_used     = EXCLUDED.geos_used,
                    rules_dropped = EXCLUDED.rules_dropped,
                    measured_at   = NOW()
                """,
                (
                    ramp_id, cohort_id, cohort_signature, platform,
                    audience_size, status,
                    json.dumps(geos_used or []),
                    rules_dropped,
                ),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_cohort_audience skipped (%s/%s/%s): %s",
                  ramp_id, cohort_signature, platform, exc)


def upsert_cohort_icp(
    *,
    ramp_id: str,
    cohort_id: str,
    cohort_signature: str,
    icp_dict: dict,
) -> None:
    """Phase 6 — persist the LLM-enriched ICP for a (ramp × cohort).

    `icp_dict` is the output of icp_enrichment.CohortIcp.to_dict().
    Idempotent on (ramp_id, cohort_signature). Best-effort: swallows
    UIDecisionsUnavailable so enrichment outages don't block cohort
    selection.
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cohort_icp (
                    ramp_id, cohort_id, cohort_signature,
                    cohort_description, top_motivations, content_prefs,
                    creative_liberty, language_pref, decision_drivers,
                    skill_priorities, sample_size_n, model_version
                ) VALUES (%s, %s, %s, %s, %s::jsonb, %s::jsonb, %s, %s, %s::jsonb, %s::jsonb, %s, %s)
                ON CONFLICT (ramp_id, cohort_signature) DO UPDATE SET
                    cohort_id          = EXCLUDED.cohort_id,
                    cohort_description = EXCLUDED.cohort_description,
                    top_motivations    = EXCLUDED.top_motivations,
                    content_prefs      = EXCLUDED.content_prefs,
                    creative_liberty   = EXCLUDED.creative_liberty,
                    language_pref      = EXCLUDED.language_pref,
                    decision_drivers   = EXCLUDED.decision_drivers,
                    skill_priorities   = EXCLUDED.skill_priorities,
                    sample_size_n      = EXCLUDED.sample_size_n,
                    model_version      = EXCLUDED.model_version,
                    updated_at         = NOW()
                """,
                (
                    ramp_id, cohort_id, cohort_signature,
                    icp_dict.get("cohort_description", ""),
                    json.dumps(icp_dict.get("top_motivations", []) or []),
                    json.dumps(icp_dict.get("content_prefs", []) or []),
                    icp_dict.get("creative_liberty", "medium"),
                    icp_dict.get("language_pref", ""),
                    json.dumps(icp_dict.get("decision_drivers", []) or []),
                    json.dumps(icp_dict.get("skill_priorities", []) or []),
                    icp_dict.get("sample_size_n"),
                    icp_dict.get("model_version", ""),
                ),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_cohort_icp skipped (%s/%s): %s",
                  ramp_id, cohort_signature, exc)


def upsert_cohort_brief_rationale(
    *,
    ramp_id: str,
    cohort_id: str,
    cohort_signature: str,
    channel: str,
    angle: str,
    geo_cluster: Optional[str] = None,
    angle_label: Optional[str] = None,
    headline: Optional[str] = None,
    subheadline: Optional[str] = None,
    photo_subject: Optional[str] = None,
    rationale: Optional[str] = None,
    competitor_signal: Optional[str] = None,
    expected_uplift_pp: Optional[float] = None,
) -> None:
    """Phase 5 — persist the brief-agent's per-angle reasoning so the console
    can render "Angles we'd test" with rationale above the timeline.

    Idempotent: ON CONFLICT (ramp_id, cohort_id, channel, angle, geo_cluster)
    DO UPDATE so re-running prep with the same inputs overwrites cleanly.

    Best-effort: swallows UIDecisionsUnavailable so a Postgres outage never
    blocks copy generation. Caller logs the failure.
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cohort_brief_rationale (
                    ramp_id, cohort_id, cohort_signature, geo_cluster, channel,
                    angle, angle_label, headline, subheadline, photo_subject,
                    rationale, competitor_signal, expected_uplift_pp
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ramp_id, cohort_id, channel, angle, geo_cluster) DO UPDATE SET
                    angle_label        = EXCLUDED.angle_label,
                    headline           = EXCLUDED.headline,
                    subheadline        = EXCLUDED.subheadline,
                    photo_subject      = EXCLUDED.photo_subject,
                    rationale          = EXCLUDED.rationale,
                    competitor_signal  = EXCLUDED.competitor_signal,
                    expected_uplift_pp = EXCLUDED.expected_uplift_pp
                """,
                (
                    ramp_id, cohort_id, cohort_signature, geo_cluster, channel,
                    angle, angle_label, headline, subheadline, photo_subject,
                    rationale, competitor_signal, expected_uplift_pp,
                ),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_cohort_brief_rationale skipped (%s/%s/%s): %s",
                  ramp_id, cohort_id, angle, exc)


# ── Brief-review gate (2026-05-22) ───────────────────────────────────────────
#
# Schema lives in scripts/sql/006_cohort_briefs.sql. The pipeline writes one
# row per (ramp × cohort × geo_cluster × channel × angle) at the END of prep,
# then flips ramp_decisions.status='awaiting_brief_review'. Reviewer drops a
# free-text comment per row in the console; clicking Confirm flips status to
# 'awaiting_approval'. Auto-confirm sweep flips stale rows after
# config.BRIEF_REVIEW_AUTO_CONFIRM_HOURS.


@dataclass
class CohortBrief:
    id:                int
    ramp_id:           str
    cohort_id:         str
    cohort_signature:  str
    geo_cluster:       str
    channel:           str
    angle:             str
    brief:             dict
    reviewer_comment:  str = ""
    reviewed_by:       Optional[str] = None
    reviewed_at:       Optional[str] = None
    generated_at:      Optional[str] = None
    updated_at:        Optional[str] = None


_BRIEF_COLS = (
    "id, ramp_id, cohort_id, cohort_signature, geo_cluster, channel, angle, "
    "brief, reviewer_comment, reviewed_by, reviewed_at, generated_at, updated_at"
)


def _row_to_brief(row) -> CohortBrief:
    return CohortBrief(
        id=row[0],
        ramp_id=row[1],
        cohort_id=row[2],
        cohort_signature=row[3],
        geo_cluster=row[4],
        channel=row[5],
        angle=row[6],
        brief=dict(row[7] or {}),
        reviewer_comment=row[8] or "",
        reviewed_by=row[9],
        reviewed_at=row[10].isoformat() if row[10] else None,
        generated_at=row[11].isoformat() if row[11] else None,
        updated_at=row[12].isoformat() if row[12] else None,
    )


def upsert_awaiting_brief_review(
    ramp_id: str,
    *,
    matched_domain: str = "",
    requester_name: str = "",
    summary: str = "",
    submitted_at=None,
    prep_summary: Optional[dict] = None,
) -> None:
    """Pipeline calls this at the END of prep — AFTER the cohort_briefs rows
    have been written. Behaves like upsert_awaiting_approval but transitions
    to 'awaiting_brief_review' instead. Idempotent + status-downgrade-safe.

    A `brief_review_pending` audit-log row is appended in the same
    transaction so the console's audit log shows the gate opened."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO ramp_decisions (
                ramp_id, status, matched_domain, requester_name,
                summary, submitted_at
            )
            VALUES (%s, 'awaiting_brief_review'::ramp_status, %s, %s, %s, %s)
            ON CONFLICT (ramp_id) DO UPDATE SET
                matched_domain = EXCLUDED.matched_domain,
                requester_name = EXCLUDED.requester_name,
                summary        = EXCLUDED.summary,
                submitted_at   = EXCLUDED.submitted_at,
                status = CASE
                    WHEN ramp_decisions.status = 'prep_running'::ramp_status
                        THEN 'awaiting_brief_review'::ramp_status
                    ELSE ramp_decisions.status
                END
            """,
            (ramp_id, matched_domain, requester_name, summary, submitted_at),
        )
        cur.execute(
            "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
            "VALUES (%s, %s, %s::jsonb, %s)",
            (ramp_id, "brief_review_pending", json.dumps(prep_summary or {}), None),
        )
        conn.commit()


def upsert_brief(
    *,
    ramp_id: str,
    cohort_id: str,
    cohort_signature: str,
    geo_cluster: str,
    channel: str,
    angle: str,
    brief: dict,
) -> None:
    """Persist ONE structured brief (Phase-1 output) for review. Idempotent on
    (ramp_id, cohort_id, geo_cluster, channel, angle). The ON CONFLICT branch
    overwrites `brief` + `generated_at` but PRESERVES `reviewer_comment`,
    `reviewed_by`, `reviewed_at` so a re-prep doesn't wipe reviewer edits.

    Best-effort: swallows UIDecisionsUnavailable so a DB outage doesn't block
    prep — the brief survives in logs and can be replayed."""
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO cohort_briefs (
                    ramp_id, cohort_id, cohort_signature, geo_cluster,
                    channel, angle, brief
                ) VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)
                ON CONFLICT (ramp_id, cohort_id, geo_cluster, channel, angle) DO UPDATE SET
                    brief        = EXCLUDED.brief,
                    generated_at = NOW()
                    -- reviewer_comment, reviewed_by, reviewed_at preserved
                """,
                (
                    ramp_id, cohort_id, cohort_signature, geo_cluster,
                    channel, angle, json.dumps(brief),
                ),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_brief skipped (%s/%s/%s/%s/%s): %s",
                  ramp_id, cohort_id, geo_cluster, channel, angle, exc)


def list_briefs_for_ramp(
    ramp_id: str,
    *,
    channel: Optional[str] = None,
) -> list[CohortBrief]:
    """Read all briefs for a ramp, optionally filtered by channel. Used by
    `_launch_ramp` to feed Phase 2 (build_copy_from_brief) and by the console
    UI via the parallel TS reader in lib/db.ts."""
    with _connect() as conn, conn.cursor() as cur:
        if channel is None:
            cur.execute(
                f"SELECT {_BRIEF_COLS} FROM cohort_briefs "
                "WHERE ramp_id = %s ORDER BY cohort_signature, geo_cluster, angle",
                (ramp_id,),
            )
        else:
            cur.execute(
                f"SELECT {_BRIEF_COLS} FROM cohort_briefs "
                "WHERE ramp_id = %s AND channel = %s "
                "ORDER BY cohort_signature, geo_cluster, angle",
                (ramp_id, channel),
            )
        return [_row_to_brief(r) for r in cur.fetchall()]


def confirm_briefs(ramp_id: str, *, by_user: Optional[str] = None) -> Optional[Decision]:
    """Atomic CAS: flip ramp_decisions.status from 'awaiting_brief_review' to
    'awaiting_approval'. Returns the updated Decision on success, None if
    the row wasn't in awaiting_brief_review (already confirmed / wrong state).

    Writes a 'briefs_confirmed' audit-log row in the same transaction. The
    console's confirm-briefs API route calls this from the UI."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE ramp_decisions
               SET status = 'awaiting_approval'::ramp_status,
                   version = version + 1
             WHERE ramp_id = %s
               AND status = 'awaiting_brief_review'::ramp_status
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
            "VALUES (%s, 'briefs_confirmed', %s::jsonb, %s)",
            (ramp_id, json.dumps({"trigger": "manual"}), by_user),
        )
        conn.commit()
        return _row_to_decision(row)


def auto_confirm_stale_brief_reviews(
    *,
    threshold_hours: int = 4,
) -> list[str]:
    """Sweep ramps stuck in 'awaiting_brief_review' for longer than
    `threshold_hours` and flip them to 'awaiting_approval'. Called from the
    poller (scripts/smart_ramp_poller.py) on every tick.

    Threshold uses the latest cohort_briefs.generated_at for the ramp (or
    ramp_decisions.updated_at if no briefs exist — defensive). Returns the
    list of ramp_ids that were auto-confirmed so the caller can log them.

    Pass threshold_hours <= 0 to disable (ramps then block indefinitely)."""
    if threshold_hours <= 0:
        return []
    confirmed: list[str] = []
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                WITH stale AS (
                    SELECT d.ramp_id
                      FROM ramp_decisions d
                     WHERE d.status = 'awaiting_brief_review'::ramp_status
                       AND d.updated_at < NOW() - (%s || ' hours')::INTERVAL
                )
                UPDATE ramp_decisions
                   SET status = 'awaiting_approval'::ramp_status,
                       version = version + 1
                 WHERE ramp_id IN (SELECT ramp_id FROM stale)
             RETURNING ramp_id
                """,
                (str(threshold_hours),),
            )
            for r in cur.fetchall():
                confirmed.append(r[0])
            for rid in confirmed:
                cur.execute(
                    "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
                    "VALUES (%s, 'brief_review_auto_confirmed', %s::jsonb, %s)",
                    (rid, json.dumps({"threshold_hours": threshold_hours}), None),
                )
            conn.commit()
            if confirmed:
                log.info("Auto-confirmed %d stale brief-review ramp(s): %s",
                         len(confirmed), confirmed)
    except UIDecisionsUnavailable as exc:
        log.debug("auto_confirm_stale_brief_reviews skipped: %s", exc)
    return confirmed


def upsert_competitor_role_ads(
    ramp_id: str, role_query: str, ads: list[dict]
) -> None:
    """Persist Meta Ad Library role-based lookups for a ramp's targeted role.

    Schema (idempotent CREATE TABLE on first call):
      competitor_role_ads(
        id          BIGSERIAL PRIMARY KEY,
        ramp_id     TEXT NOT NULL,
        role_query  TEXT NOT NULL,
        page_name   TEXT NOT NULL,
        ad_body     TEXT,
        pay_rate    TEXT,
        impressions_lower BIGINT, impressions_upper BIGINT,
        spend_lower_usd NUMERIC, spend_upper_usd NUMERIC,
        ad_snapshot_url TEXT,
        delivery_start_time TIMESTAMPTZ,
        captured_at TIMESTAMPTZ DEFAULT NOW(),
        UNIQUE (ramp_id, role_query, page_name, ad_body)
      )

    Re-running for the same (ramp_id, role_query, page_name, ad_body) is a
    no-op via ON CONFLICT DO NOTHING. The captured_at timestamp on the first
    write preserves the original capture; downstream queries treat the
    presence of any row as evidence the competitor ran the ad at that time.

    Best-effort — Postgres outage never blocks the pipeline.
    """
    if not ads:
        return
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                CREATE TABLE IF NOT EXISTS competitor_role_ads (
                  id BIGSERIAL PRIMARY KEY,
                  ramp_id    TEXT NOT NULL,
                  role_query TEXT NOT NULL,
                  page_name  TEXT NOT NULL,
                  ad_body    TEXT,
                  pay_rate   TEXT,
                  impressions_lower BIGINT,
                  impressions_upper BIGINT,
                  spend_lower_usd   NUMERIC,
                  spend_upper_usd   NUMERIC,
                  ad_snapshot_url   TEXT,
                  delivery_start_time TIMESTAMPTZ,
                  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                  UNIQUE (ramp_id, role_query, page_name, md5(coalesce(ad_body, '')))
                )
                """
            )
            for ad in ads:
                cur.execute(
                    """
                    INSERT INTO competitor_role_ads
                      (ramp_id, role_query, page_name, ad_body, pay_rate,
                       impressions_lower, impressions_upper,
                       spend_lower_usd, spend_upper_usd,
                       ad_snapshot_url, delivery_start_time)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                    """,
                    (
                        ramp_id, role_query,
                        ad.get("page_name", ""),
                        ad.get("ad_body", ""),
                        ad.get("pay_rate"),
                        ad.get("impressions_lower"),
                        ad.get("impressions_upper"),
                        ad.get("spend_lower_usd"),
                        ad.get("spend_upper_usd"),
                        ad.get("ad_snapshot_url", ""),
                        ad.get("delivery_start_time") or None,
                    ),
                )
            conn.commit()
            log.info("Persisted %d Meta role-ads for ramp=%s role=%r",
                     len(ads), ramp_id, role_query)
    except UIDecisionsUnavailable as exc:
        log.debug(
            "upsert_competitor_role_ads skipped (ramp=%s role=%r): %s",
            ramp_id, role_query, exc,
        )


def upsert_competitor_intel_snapshot(ramp_id: str, snapshot: dict) -> None:
    """Phase 5 — snapshot data/competitor_intel/latest.json against a ramp at
    prep time. The console reads this to render the "Competitor landscape"
    card alongside the angles. ON CONFLICT updates so re-running prep
    refreshes the snapshot.

    Best-effort — Postgres outage never blocks the pipeline.
    """
    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO competitor_intel_snapshots (ramp_id, snapshot)
                VALUES (%s, %s::jsonb)
                ON CONFLICT (ramp_id) DO UPDATE SET
                    snapshot = EXCLUDED.snapshot
                """,
                (ramp_id, json.dumps(snapshot)),
            )
            conn.commit()
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_competitor_intel_snapshot skipped (%s): %s", ramp_id, exc)


# ── Phase 6 — recommendations ────────────────────────────────────────────────
#
# Schema lives in scripts/sql/003_recommendations.sql:
#   ramp_recommendations(id, ramp_id, campaign_urn, cohort_signature, channel,
#                        angle, classification, action, rationale,
#                        metric_signal jsonb, replacement_brief_id,
#                        decision, decided_by, decided_at, generated_at)
# UNIQUE (ramp_id, campaign_urn) so a re-run of recommend_actions overwrites
# the previous row in place. We never keep history — intent + outcome live in
# ramp_audit_log via log_event.

VALID_CLASSIFICATIONS = {"working", "underperforming", "failing", "insufficient_data"}
VALID_ACTIONS         = {"keep", "pause", "replace"}
VALID_DECISIONS       = {"pending", "accepted", "rejected"}


@dataclass
class Recommendation:
    id:                   Optional[int] = None
    ramp_id:              str = ""
    campaign_urn:         str = ""
    cohort_signature:     Optional[str] = None
    channel:              Optional[str] = None
    angle:                Optional[str] = None
    classification:       str = "insufficient_data"
    action:               str = "keep"
    rationale:            Optional[str] = None
    metric_signal:        dict = field(default_factory=dict)
    replacement_brief_id: Optional[int] = None
    decision:             str = "pending"
    decided_by:           Optional[str] = None
    decided_at:           Optional[str] = None
    generated_at:         Optional[str] = None


_RECOMMENDATION_COLS = (
    "id, ramp_id, campaign_urn, cohort_signature, channel, angle, "
    "classification::text, action::text, rationale, metric_signal, "
    "replacement_brief_id, decision::text, decided_by, decided_at, generated_at"
)


def _row_to_recommendation(row) -> Recommendation:
    return Recommendation(
        id=row[0],
        ramp_id=row[1],
        campaign_urn=row[2],
        cohort_signature=row[3],
        channel=row[4],
        angle=row[5],
        classification=row[6],
        action=row[7],
        rationale=row[8],
        metric_signal=dict(row[9] or {}),
        replacement_brief_id=row[10],
        decision=row[11],
        decided_by=row[12],
        decided_at=row[13].isoformat() if row[13] else None,
        generated_at=row[14].isoformat() if row[14] else None,
    )


def upsert_recommendation(
    *,
    ramp_id: str,
    campaign_urn: str,
    classification: str,
    action: str,
    cohort_signature: Optional[str] = None,
    channel: Optional[str] = None,
    angle: Optional[str] = None,
    rationale: Optional[str] = None,
    metric_signal: Optional[dict[str, Any]] = None,
    replacement_brief_id: Optional[int] = None,
) -> Optional[Recommendation]:
    """Insert or update a per-campaign recommendation. Idempotent on
    (ramp_id, campaign_urn) — re-running classification overwrites the
    previous classification, action, rationale, and metric_signal.

    Preserves `decision` + `decided_by` + `decided_at` on conflict so a human's
    accept/reject doesn't get wiped by the next daily evaluation pass.

    Best-effort: returns None and logs when Postgres is unreachable so the
    feedback agent can keep iterating other campaigns.
    """
    if classification not in VALID_CLASSIFICATIONS:
        raise ValueError(f"invalid classification: {classification!r}")
    if action not in VALID_ACTIONS:
        raise ValueError(f"invalid action: {action!r}")

    try:
        with _connect() as conn, conn.cursor() as cur:
            cur.execute(
                f"""
                INSERT INTO ramp_recommendations (
                    ramp_id, campaign_urn, cohort_signature, channel, angle,
                    classification, action, rationale, metric_signal,
                    replacement_brief_id
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    %s::recommendation_classification,
                    %s::recommendation_action,
                    %s, %s::jsonb, %s
                )
                ON CONFLICT (ramp_id, campaign_urn) DO UPDATE SET
                    cohort_signature     = EXCLUDED.cohort_signature,
                    channel              = EXCLUDED.channel,
                    angle                = EXCLUDED.angle,
                    classification       = EXCLUDED.classification,
                    action               = EXCLUDED.action,
                    rationale            = EXCLUDED.rationale,
                    metric_signal        = EXCLUDED.metric_signal,
                    replacement_brief_id = EXCLUDED.replacement_brief_id,
                    generated_at         = NOW()
                RETURNING {_RECOMMENDATION_COLS}
                """,
                (
                    ramp_id, campaign_urn, cohort_signature, channel, angle,
                    classification, action, rationale,
                    json.dumps(metric_signal or {}),
                    replacement_brief_id,
                ),
            )
            row = cur.fetchone()
            conn.commit()
            return _row_to_recommendation(row) if row else None
    except UIDecisionsUnavailable as exc:
        log.debug("upsert_recommendation skipped (%s/%s): %s",
                  ramp_id, campaign_urn, exc)
        return None


def list_recommendations(
    ramp_id: str,
    *,
    decision: Optional[str] = None,
) -> list[Recommendation]:
    """Return all recommendations for a ramp, most-recent first. When
    `decision` is provided, filters to that decision state (e.g. 'pending')."""
    if decision is not None and decision not in VALID_DECISIONS:
        raise ValueError(f"invalid decision filter: {decision!r}")
    with _connect() as conn, conn.cursor() as cur:
        if decision is None:
            cur.execute(
                f"SELECT {_RECOMMENDATION_COLS} FROM ramp_recommendations "
                "WHERE ramp_id = %s ORDER BY generated_at DESC, id DESC",
                (ramp_id,),
            )
        else:
            cur.execute(
                f"SELECT {_RECOMMENDATION_COLS} FROM ramp_recommendations "
                "WHERE ramp_id = %s AND decision = %s::recommendation_decision "
                "ORDER BY generated_at DESC, id DESC",
                (ramp_id, decision),
            )
        return [_row_to_recommendation(r) for r in cur.fetchall()]


def set_recommendation_decision(
    recommendation_id: int,
    decision: str,
    *,
    by_user: Optional[str] = None,
) -> Optional[Recommendation]:
    """Flip a recommendation's decision (pending → accepted/rejected). The
    UI calls this from the Accept / Reject buttons. Returns the updated row
    or None if `recommendation_id` doesn't exist."""
    if decision not in VALID_DECISIONS:
        raise ValueError(f"invalid decision: {decision!r}")
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"""
            UPDATE ramp_recommendations
               SET decision   = %s::recommendation_decision,
                   decided_by = %s,
                   decided_at = NOW()
             WHERE id = %s
         RETURNING {_RECOMMENDATION_COLS}
            """,
            (decision, by_user, recommendation_id),
        )
        row = cur.fetchone()
        if not row:
            conn.rollback()
            return None
        rec = _row_to_recommendation(row)
        cur.execute(
            "INSERT INTO ramp_audit_log (ramp_id, event_type, payload, by_user) "
            "VALUES (%s, %s, %s::jsonb, %s)",
            (
                rec.ramp_id,
                f"recommendation_{decision}",
                json.dumps({
                    "recommendation_id": recommendation_id,
                    "campaign_urn":      rec.campaign_urn,
                    "classification":    rec.classification,
                    "action":            rec.action,
                }),
                by_user,
            ),
        )
        conn.commit()
        return rec


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

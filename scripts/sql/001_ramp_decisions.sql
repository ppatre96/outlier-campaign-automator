-- 001_ramp_decisions.sql
--
-- Schema for the outlier-campaign-console approval gate + audit log.
-- Applied once to the Vercel Postgres database backing the UI.
--
-- Apply locally:
--   psql "$DATABASE_URL" -f scripts/sql/001_ramp_decisions.sql
--
-- Idempotent: every DDL statement is IF NOT EXISTS / DO $$ guarded so
-- re-running this against an already-migrated DB is a no-op.

-- ── Enums ────────────────────────────────────────────────────────────────────

DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'ramp_status') THEN
        CREATE TYPE ramp_status AS ENUM (
            'prep_running',         -- pipeline is mining cohorts + generating briefs
            'awaiting_approval',    -- prep done; UI controls live; cron will not launch
            'approved',             -- Diego/Bryan picked channels + budgets; poller may claim
            'yolo',                 -- "do the default" — same as approved, but for audit clarity
            'launching',            -- poller has atomically claimed and is calling platform APIs
            'completed',            -- all selected arms ran; campaigns live in DRAFT on each platform
            'failed'                -- _launch_ramp raised; see ramp_audit_log payload for details
        );
    END IF;
END $$;


-- ── ramp_decisions ───────────────────────────────────────────────────────────
--
-- One row per Smart Ramp. The poller writes this row when prep completes;
-- the UI updates `status`, `channels`, `budgets` when Diego/Bryan approve.
-- The poller's atomic `claim_ramp` UPDATE flips status approved/yolo →
-- launching in a single statement so concurrent pollers/UI clicks can't
-- double-launch.

CREATE TABLE IF NOT EXISTS ramp_decisions (
    ramp_id         TEXT        PRIMARY KEY,                -- e.g. "GMR-0021"
    status          ramp_status NOT NULL DEFAULT 'prep_running',
    channels        TEXT[]      NOT NULL DEFAULT '{}',      -- subset of {'linkedin','meta','google','google_search','reddit'}
    budgets         JSONB       NOT NULL DEFAULT '{}'::jsonb, -- {'linkedin': 5000, 'meta': 100, 'google': 0} in cents/day
    decided_by      TEXT,                                    -- Google email of the approver (null until first decision)
    decided_at      TIMESTAMPTZ,
    version         INT         NOT NULL DEFAULT 1,          -- bumped each decision edit; for optimistic concurrency
    -- Prep snapshot: small fields surfaced on the ramps list page so the
    -- UI doesn't have to round-trip to Smart Ramp upstream just to render
    -- the list. Heavier prep artifacts (cohorts, briefs, creatives) live
    -- in the existing Triggers Sheet + Campaign Registry — those are the
    -- canonical source.
    matched_domain  TEXT,
    requester_name  TEXT,
    summary         TEXT,
    submitted_at    TIMESTAMPTZ,
    -- Bookkeeping
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ramp_decisions_status_idx ON ramp_decisions (status);
CREATE INDEX IF NOT EXISTS ramp_decisions_submitted_at_idx ON ramp_decisions (submitted_at DESC);


-- ── ramp_audit_log ───────────────────────────────────────────────────────────
--
-- Append-only log of every state transition. Written by both the pipeline
-- (status: prep_running → awaiting_approval, launching → completed/failed)
-- and the UI (approve, yolo, recommendation accept/reject, budget edit).
--
-- payload schema by event_type:
--   prep_complete:       {cohort_count, brief_count, creative_count, drive_folder}
--   approved:            {channels: string[], budgets: {channel: cents}}
--   yolo:                {channels: string[], budgets: {channel: cents}}
--   launching:           {claimed_at}
--   completed:           {campaign_count, platforms: string[]}
--   failed:              {error_class, error_message, traceback}
--   recommendation_accept|reject: {recommendation_id, action}
--   budget_edit:         {campaign_urn, old_cents, new_cents}

CREATE TABLE IF NOT EXISTS ramp_audit_log (
    id          BIGSERIAL    PRIMARY KEY,
    ramp_id     TEXT         NOT NULL,
    event_type  TEXT         NOT NULL,
    payload     JSONB        NOT NULL DEFAULT '{}'::jsonb,
    by_user     TEXT,                                   -- null when written by the pipeline; email when via UI
    ts          TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ramp_audit_log_ramp_id_idx ON ramp_audit_log (ramp_id, ts DESC);
CREATE INDEX IF NOT EXISTS ramp_audit_log_event_type_idx ON ramp_audit_log (event_type, ts DESC);


-- ── updated_at trigger ───────────────────────────────────────────────────────

CREATE OR REPLACE FUNCTION ramp_decisions_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS ramp_decisions_touch_updated_at_trig ON ramp_decisions;
CREATE TRIGGER ramp_decisions_touch_updated_at_trig
    BEFORE UPDATE ON ramp_decisions
    FOR EACH ROW
    EXECUTE FUNCTION ramp_decisions_touch_updated_at();

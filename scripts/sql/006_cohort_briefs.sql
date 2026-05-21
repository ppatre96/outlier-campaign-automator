-- 006_cohort_briefs.sql
--
-- Brief-review gate (added 2026-05-22 per Pranav rule):
-- After prep finishes, the pipeline persists one creative brief per
-- (ramp × cohort × geo_cluster × channel × angle) and flips ramp_status
-- to 'awaiting_brief_review'. Reviewer drops a free-text comment per row
-- in the console; clicking "Confirm briefs" flips status to
-- 'awaiting_approval' (the existing channels + budget gate). The poller
-- also auto-confirms after BRIEF_REVIEW_AUTO_CONFIRM_HOURS (default 4)
-- so a busy reviewer doesn't strand a ramp.
--
-- v1 scope: linkedin channel only. Meta + Google will reuse the same
-- schema in a follow-up — channel='meta'|'google' rows just need the
-- pipeline to produce them.
--
-- Apply with:
--   doppler run -p outlier-campaign-agent -c dev -- bash -c \
--     'psql "$DATABASE_URL" -f scripts/sql/006_cohort_briefs.sql'
--
-- Idempotent (CREATE TABLE IF NOT EXISTS + ADD VALUE IF NOT EXISTS).

-- ── Enum extension ──────────────────────────────────────────────────────────
-- Postgres ADD VALUE cannot run inside a DO $$ block / transaction, so this
-- has to live at top level. IF NOT EXISTS makes it safe to re-run.
ALTER TYPE ramp_status ADD VALUE IF NOT EXISTS 'awaiting_brief_review'
    BEFORE 'awaiting_approval';


-- ── cohort_briefs ───────────────────────────────────────────────────────────
--
-- One row per (ramp × cohort × geo_cluster × channel × angle). The
-- structured `brief` JSONB is the Phase-1 LLM output; `reviewer_comment`
-- is the free-text overlay the reviewer drops in the console.
--
-- brief JSONB shape (set by src/brief_generator.py:build_brief):
--   {
--     "angle":              "A" | "B" | "C",
--     "angle_hook":         str,
--     "headline_direction": str,
--     "subheadline_direction": str,
--     "photo_direction":    str,
--     "tone":               str,
--     "proof_points":       [str, ...],
--     "language_hint":      str,
--     "competitor_signal":  str,
--     "must_include":       [str, ...],
--     "must_avoid":         [str, ...]
--   }

CREATE TABLE IF NOT EXISTS cohort_briefs (
    id                BIGSERIAL    PRIMARY KEY,
    ramp_id           TEXT         NOT NULL,
    cohort_id         TEXT         NOT NULL,            -- _stg_id
    cohort_signature  TEXT         NOT NULL,            -- human cohort name (matches cohort_icp)
    geo_cluster       TEXT         NOT NULL,            -- e.g. 'south_asian', 'global_mix'
    channel           TEXT         NOT NULL,            -- 'linkedin' | 'meta' | 'google'
    angle             TEXT         NOT NULL,            -- 'A' | 'B' | 'C'
    brief             JSONB        NOT NULL,
    reviewer_comment  TEXT         NOT NULL DEFAULT '',
    reviewed_by       TEXT,                              -- email when reviewer hits Save on this row
    reviewed_at       TIMESTAMPTZ,
    generated_at      TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, cohort_id, geo_cluster, channel, angle)
);

CREATE INDEX IF NOT EXISTS cohort_briefs_ramp_idx
    ON cohort_briefs (ramp_id);

-- updated_at touch trigger so any UPDATE bumps the timestamp without the
-- caller having to remember (mirrors ramp_decisions_touch_updated_at_trig
-- from 001).
CREATE OR REPLACE FUNCTION cohort_briefs_touch_updated_at() RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS cohort_briefs_touch_updated_at_trig ON cohort_briefs;
CREATE TRIGGER cohort_briefs_touch_updated_at_trig
    BEFORE UPDATE ON cohort_briefs
    FOR EACH ROW
    EXECUTE FUNCTION cohort_briefs_touch_updated_at();

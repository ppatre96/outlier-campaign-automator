-- 005_cohort_audience.sql
--
-- Per-channel audience estimate per (ramp × cohort × platform). Written
-- during the PREP stage by src/prep_audience.py so reviewers see audience
-- size before approving — not after launch.
--
-- LinkedIn estimate comes from src/stage_c.py:get_audience_count (always
-- runs in prep). Meta + Google estimates come from each client's
-- get_reach_estimate() (added in 2026-05-21 audience_check work) — best
-- effort, status='skipped' when creds missing or API fails.
--
-- Distinct from the meta_audience_size / google_audience_size columns on
-- the Campaign Registry sheet (those are per-shipped-campaign per-geo —
-- this is per-cohort × channel, measured at prep time, before any
-- campaign exists).
--
-- Apply with:
--   doppler run -p outlier-campaign-agent -c dev -- bash -c \
--     'psql "$DATABASE_URL" -f scripts/sql/005_cohort_audience.sql'
--
-- Idempotent — CREATE TABLE IF NOT EXISTS + unique key.

CREATE TABLE IF NOT EXISTS cohort_audience (
    id                 BIGSERIAL    PRIMARY KEY,
    ramp_id            TEXT         NOT NULL,
    cohort_id          TEXT         NOT NULL,            -- _stg_id from Triggers Sheet
    cohort_signature   TEXT         NOT NULL,            -- human cohort name (matches cohort_icp)
    platform           TEXT         NOT NULL,            -- 'linkedin' | 'meta' | 'google'
    audience_size      BIGINT,                            -- null when status='skipped'
    status             TEXT         NOT NULL DEFAULT 'measured',  -- 'measured' | 'denarrowed' | 'below_floor' | 'skipped'
    geos_used          JSONB        NOT NULL DEFAULT '[]'::jsonb, -- which country codes were passed to the reach API
    rules_dropped      INTEGER      NOT NULL DEFAULT 0,  -- count of rules removed during de-narrow (0 = no de-narrow)
    measured_at        TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, cohort_signature, platform)
);

CREATE INDEX IF NOT EXISTS cohort_audience_ramp_idx
    ON cohort_audience (ramp_id);

-- 004_cohort_icp.sql
--
-- Structured ICP (Ideal Customer Profile) per (ramp × cohort). Produced
-- post-Stage-C by src/icp_enrichment.py — an LLM step that consumes the
-- Cohort dataclass + a Snowflake resume sample and emits the cohort's
-- priorities, content preferences, creative-liberty appetite, language pref,
-- and decision drivers.
--
-- This becomes the BASIS for brief + copy generation: the brief-agent reads
-- cohort_icp and forks per-channel (LinkedIn vs Meta vs Google) instead of
-- emitting a single LinkedIn-shaped angle set. The console renders this
-- table above the AnglesCard so reviewers see the ICP traits driving the
-- angles.
--
-- Apply with:
--   psql "$DATABASE_URL" -f scripts/sql/004_cohort_icp.sql
--
-- Idempotent — CREATE TABLE IF NOT EXISTS.

CREATE TABLE IF NOT EXISTS cohort_icp (
    id                 BIGSERIAL    PRIMARY KEY,
    ramp_id            TEXT         NOT NULL,
    cohort_id          TEXT         NOT NULL,            -- _stg_id from Triggers Sheet
    cohort_signature   TEXT         NOT NULL,            -- human cohort name
    cohort_description TEXT,                              -- one-line LLM summary of who they are
    top_motivations    JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- e.g. ["fair pay", "remote flexibility"]
    content_prefs      JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- e.g. ["technical depth", "case studies"]
    creative_liberty   TEXT,                              -- "high" | "medium" | "low"
    language_pref      TEXT,                              -- BCP-47 ish — "en-US" | "en-IN" | "es-419" ...
    decision_drivers   JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- e.g. ["brand legitimacy", "task interest"]
    skill_priorities   JSONB        NOT NULL DEFAULT '[]'::jsonb,   -- e.g. ["python", "data labeling QA"]
    sample_size_n      INTEGER,                           -- how many resume rows informed the enrichment
    model_version      TEXT,                              -- which Claude model produced this row
    generated_at       TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, cohort_signature)
);

CREATE INDEX IF NOT EXISTS cohort_icp_ramp_idx ON cohort_icp (ramp_id);

CREATE OR REPLACE FUNCTION cohort_icp_touch_updated_at()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS cohort_icp_touch_updated_at_trig ON cohort_icp;
CREATE TRIGGER cohort_icp_touch_updated_at_trig
    BEFORE UPDATE ON cohort_icp
    FOR EACH ROW
    EXECUTE FUNCTION cohort_icp_touch_updated_at();

-- 002_brief_rationale.sql
--
-- Phase 5: structured persistence of the brief-agent's per-angle reasoning
-- and a per-ramp snapshot of competitor intel. Both are populated during
-- the pipeline's prep phase so the console can render them above the
-- timeline before Diego/Bryan click Approve.
--
-- Apply with:
--   psql "$DATABASE_URL" -f scripts/sql/002_brief_rationale.sql
--
-- Idempotent — DO $$/IF NOT EXISTS guards everywhere.

-- ── cohort_brief_rationale ───────────────────────────────────────────────────
--
-- One row per (ramp × cohort × channel × angle). Captures the brief-agent's
-- reasoning + a snippet of the produced copy for quick UI rendering. The
-- canonical copy still lives in the Triggers Sheet's Campaign Registry tab
-- via campaign_registry.log_campaign; this table is a lightweight queryable
-- mirror of just the "why this angle?" decision data.

CREATE TABLE IF NOT EXISTS cohort_brief_rationale (
    id                 BIGSERIAL    PRIMARY KEY,
    ramp_id            TEXT         NOT NULL,
    cohort_id          TEXT         NOT NULL,        -- _stg_id from Triggers Sheet, e.g. STG-20260515-40599
    cohort_signature   TEXT         NOT NULL,        -- human cohort name, e.g. skills__deep_learning
    geo_cluster        TEXT,                          -- e.g. anglo / latin_american
    channel            TEXT         NOT NULL,        -- 'linkedin' | 'meta' | 'google'
    angle              TEXT         NOT NULL,        -- 'A' | 'B' | 'C'
    angle_label        TEXT,                          -- e.g. 'Expertise Hook', 'Earnings Hook', 'Flexibility Hook'
    headline           TEXT,
    subheadline        TEXT,
    photo_subject      TEXT,
    rationale          TEXT,                          -- the brief-agent's reasoning sentence(s)
    competitor_signal  TEXT,                          -- reference tag, e.g. "Mercor data breach 2026-04"
    expected_uplift_pp NUMERIC,                       -- optional, percentage points vs baseline
    created_at         TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, cohort_id, channel, angle, geo_cluster)
);

CREATE INDEX IF NOT EXISTS cohort_brief_rationale_ramp_idx
    ON cohort_brief_rationale (ramp_id, channel, angle);


-- ── competitor_intel_snapshots ───────────────────────────────────────────────
--
-- One row per ramp (latest snapshot wins via ON CONFLICT). The pipeline
-- captures the contents of data/competitor_intel/latest.json at the moment
-- prep runs so the UI shows the intel that informed THIS ramp's brief.

CREATE TABLE IF NOT EXISTS competitor_intel_snapshots (
    ramp_id      TEXT         PRIMARY KEY,
    snapshot     JSONB        NOT NULL,
    captured_at  TIMESTAMPTZ  NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS competitor_intel_snapshots_captured_idx
    ON competitor_intel_snapshots (captured_at DESC);

CREATE OR REPLACE FUNCTION competitor_intel_snapshots_touch_updated_at()
    RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS competitor_intel_snapshots_touch_updated_at_trig
    ON competitor_intel_snapshots;
CREATE TRIGGER competitor_intel_snapshots_touch_updated_at_trig
    BEFORE UPDATE ON competitor_intel_snapshots
    FOR EACH ROW
    EXECUTE FUNCTION competitor_intel_snapshots_touch_updated_at();

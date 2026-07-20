-- ramp_fatigue — per-campaign creative-fatigue surface (feature: weekly fatigue
-- report + console "Fatigue" tab). Written by src/fatigue.compute_fatigue, read
-- by the console (@vercel/postgres) and the weekly Slack report.
--
-- This table SELF-HEALS at runtime via src/ui_decisions._ensure_fatigue_table
-- (CREATE TABLE IF NOT EXISTS on every upsert/list), so applying this file is
-- optional — it's the canonical doc mirror. Plain TEXT status columns (no SQL
-- enum) keep it migration-free in CI.

CREATE TABLE IF NOT EXISTS ramp_fatigue (
    id                 BIGSERIAL PRIMARY KEY,
    ramp_id            TEXT NOT NULL,
    platform           TEXT NOT NULL DEFAULT 'meta',
    campaign_id        TEXT NOT NULL DEFAULT '',   -- the ad-set id (console "campaign" unit)
    adset_id           TEXT NOT NULL DEFAULT '',
    campaign_name      TEXT NOT NULL DEFAULT '',
    cohort_signature   TEXT NOT NULL DEFAULT '',
    geo_cluster        TEXT NOT NULL DEFAULT '',
    locale             TEXT NOT NULL DEFAULT '',
    classification     TEXT NOT NULL DEFAULT 'healthy',  -- healthy | reaching | reached
    fatigue_score      INT  NOT NULL DEFAULT 0,          -- 0..100
    recommended_action TEXT NOT NULL DEFAULT '',         -- refresh | pause_weak | both
    signals            JSONB NOT NULL DEFAULT '{}'::jsonb,
    decision           TEXT NOT NULL DEFAULT 'pending',  -- pending | refresh_approved | pause_approved | rejected | dismissed
    decided_by         TEXT,
    decided_at         TIMESTAMPTZ,
    campaign_link      TEXT NOT NULL DEFAULT '',
    generated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, platform, campaign_id)
);

CREATE INDEX IF NOT EXISTS ramp_fatigue_ramp_idx ON ramp_fatigue (ramp_id, updated_at DESC);

-- 011_launch_progress.sql
--
-- Per-unit launch progress. One row per (channel x locale x cohort) unit the
-- launch run creates, so the console can show a real status
--   queued -> creating -> created -> failed
-- instead of inferring it from channel_locks (running) + campaign rows (exists).
--
--   channel           — console's granular key: linkedin | linkedin_inmail |
--                       meta | google | google_search | reddit | tiktok
--   locale            — BCP-47 (job_post_language_code); '' = whole-channel
--   cohort_signature  — cohort.name; geo_cluster — geo_group.cluster
--   status            — queued | creating | created | failed
--   error             — str(exc) on failure, else NULL
--   UNIQUE (ramp_id, channel, locale, cohort_signature, geo_cluster)
--     = the `campaigns` unique key minus `angle` (one row per lock-covered unit)
--
-- Written by src/ui_decisions.upsert_launch_progress at each per-unit create
-- site in main.py's launch arms; mark_launch_progress_failed sweeps lingering
-- 'creating' rows to 'failed' in the launch run's finally block.
--
-- Apply:  psql "$DATABASE_URL" -f scripts/sql/011_launch_progress.sql
-- Idempotent. ui_decisions.upsert_launch_progress also self-creates this table
-- with CREATE TABLE IF NOT EXISTS, so applying this file is optional — but the
-- console reads it directly, so apply it to the shared DB before the console
-- change deploys (else the console read no-ops until the first pipeline write).

CREATE TABLE IF NOT EXISTS launch_progress (
    ramp_id          TEXT NOT NULL,
    channel          TEXT NOT NULL,
    locale           TEXT NOT NULL DEFAULT '',
    cohort_id        TEXT NOT NULL DEFAULT '',
    cohort_signature TEXT NOT NULL DEFAULT '',
    geo_cluster      TEXT NOT NULL DEFAULT '',
    status           TEXT NOT NULL DEFAULT 'queued',
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, channel, locale, cohort_signature, geo_cluster)
);

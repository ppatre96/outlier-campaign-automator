-- 014_daily_metrics_funnel_cols.sql
--
-- Add two funnel columns to campaign_daily_metrics for the Analytics dashboard:
--   skill_grants   distinct contributors granted ≥1 VERIFIED worker skill
--                  (GENAI_POSTGRES.WORKER_SKILL_ENTRY, STATUS='verified',
--                  OVERALL_STATUS NOT IN ('unranked','selfReported'), granted
--                  on/after signup) — i.e. "assigned ≥1 worker skill".
--   ocp_completes  distinct contributors whose first OCP ended in success
--                  (CONTRIBUTOR_EARLY_SUCCESS_FUNNEL.FIRST_OCP_ENDED_IN_SUCCESS
--                  = TRUE).
-- Both attributed per (campaign × day) via the same UTM/APPLICATION_DAY spine as
-- signups/screening_passes/activations.
--
-- Also drops the short-lived skill_screening_passes column: an earlier revision
-- used APPLICATION_CONVERSION.SKILL_SCREENING_PASS_DAY, but that column is
-- near-empty for paid traffic (~0), so the metric was switched to verified
-- worker-skill grants above. DROP IF EXISTS makes this idempotent.
--
-- Apply:  psql "$DATABASE_URL" -f scripts/sql/014_daily_metrics_funnel_cols.sql
-- Idempotent. src/ui_decisions.upsert_daily_metrics_batch() also self-heals the
-- two ADD COLUMNs via _DAILY_METRICS_MIGRATE once per process — but apply before
-- deploy so the console's new column reads are populated.

ALTER TABLE campaign_daily_metrics ADD COLUMN IF NOT EXISTS skill_grants INTEGER NOT NULL DEFAULT 0;
ALTER TABLE campaign_daily_metrics ADD COLUMN IF NOT EXISTS ocp_completes INTEGER NOT NULL DEFAULT 0;
ALTER TABLE campaign_daily_metrics DROP COLUMN IF EXISTS skill_screening_passes;

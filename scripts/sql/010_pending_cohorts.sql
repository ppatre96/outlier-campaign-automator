-- 010_pending_cohorts.sql
--
-- New-cohort review+launch feature. Two ADDITIVE columns on ramp_decisions,
-- orthogonal to the ramp_status enum (so the existing approval gate is
-- untouched):
--
--   prepped_cohort_ids  — snapshot of the Smart Ramp CohortSpec.id values that
--                         have already been prepped for this ramp. Written at
--                         first prep (all then-existing cohorts) and unioned at
--                         each scoped per-cohort prep. A Smart Ramp cohort.id
--                         absent from this set is "new". (cohort_icp.cohort_id
--                         can't be used for this — it stores the per-run
--                         _stg_id of the BEAM-derived cohort, not the stable
--                         Smart Ramp CohortSpec.id.)
--
--   pending_cohorts     — one JSON entry per newly-detected cohort awaiting
--                         user-driven review+launch:
--                           {cohort_id, label, detected_at, status}
--                         status ∈ detected | prepping | awaiting_review
--                                | awaiting_launch | launched
--                         The poller writes 'detected'; the console + scoped
--                         pipeline runs advance the rest.
--
-- Apply:  psql "$DATABASE_URL" -f scripts/sql/010_pending_cohorts.sql
-- Idempotent (ADD COLUMN IF NOT EXISTS). The ui_decisions helpers also
-- self-heal these columns once per process, but the console reads them
-- directly so this migration MUST be applied to the shared DB before the
-- console changes deploy.

ALTER TABLE ramp_decisions
    ADD COLUMN IF NOT EXISTS prepped_cohort_ids TEXT[] NOT NULL DEFAULT '{}';

ALTER TABLE ramp_decisions
    ADD COLUMN IF NOT EXISTS pending_cohorts JSONB NOT NULL DEFAULT '[]'::jsonb;

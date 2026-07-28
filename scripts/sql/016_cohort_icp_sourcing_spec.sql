-- 016_cohort_icp_sourcing_spec.sql
--
-- Sourcing-spec half of the ICP (2026-07-28). Before this, cohort_icp held only
-- the creative-facing traits (motivations, content prefs, creative liberty), so
-- a sizing analysis answered "how big is this audience" but not "who exactly do
-- we source, and who do we skip". These columns hold the readable spec a human
-- decides from — the same shape a recruiter writes by hand:
--
--   summary            who they are and what work they actually do
--   location           plain-language sourcing geo ("United States only")
--   core_requirements  [{name, description, titles[]}] — "must have at least one"
--   strong_signals     certifications / designations / tenure markers
--   exclusions         adjacent profiles we should NOT source
--   evidence           the frequency mine the spec was derived from, as
--                      {n_contributors, top_titles|top_skills|top_fields|
--                       top_companies: [[name, count], ...]} so every claim in
--                      the spec is auditable as "shared by k of n"
--
-- Written by src/icp_enrichment.py (see _ICP_SYSTEM_PROMPT) via
-- ui_decisions.upsert_cohort_icp, which also runs these ALTERs itself so older
-- DBs self-heal on the first upsert. Rendered by the console's IcpSummaryCard.
--
-- Apply with:
--   psql "$DATABASE_URL" -f scripts/sql/016_cohort_icp_sourcing_spec.sql
--
-- Idempotent — ADD COLUMN IF NOT EXISTS.

ALTER TABLE cohort_icp
    ADD COLUMN IF NOT EXISTS summary           TEXT,
    ADD COLUMN IF NOT EXISTS location          TEXT,
    ADD COLUMN IF NOT EXISTS core_requirements JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS strong_signals    JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS exclusions        JSONB NOT NULL DEFAULT '[]'::jsonb,
    ADD COLUMN IF NOT EXISTS evidence          JSONB NOT NULL DEFAULT '{}'::jsonb;

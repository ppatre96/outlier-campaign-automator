-- 017_cohort_icp_exclude_titles.sql
--
-- The ICP's "do not source" list, as targetable titles (2026-07-28).
--
-- 016 gave the ICP an `exclusions` list, but it was prose — so the ICP could say
-- "do not source attorneys or account executives" while the resolved LinkedIn
-- targeting cheerfully included both, and the audience number was measured
-- against targeting we'd never run. `exclude_titles` is the same judgment in
-- facet-able form: real LinkedIn titles, which sizing_analysis._apply_icp_exclusions
-- turns into the cohort's negative facets before the audience is measured.
--
-- Written by src/icp_enrichment.py via ui_decisions.upsert_cohort_icp, which runs
-- this ALTER itself so older DBs self-heal on the first upsert.
--
-- Apply with:
--   psql "$DATABASE_URL" -f scripts/sql/017_cohort_icp_exclude_titles.sql
--
-- Idempotent — ADD COLUMN IF NOT EXISTS.

ALTER TABLE cohort_icp
    ADD COLUMN IF NOT EXISTS exclude_titles JSONB NOT NULL DEFAULT '[]'::jsonb;

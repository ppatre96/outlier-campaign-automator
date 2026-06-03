-- Resolved targeting facets per (ramp × cohort × platform).
-- Populated at prep time by main.py's per-channel audience block via
-- src.ui_decisions.upsert_cohort_targeting (which also self-creates this
-- table with CREATE TABLE IF NOT EXISTS, so applying this file is optional).
--
-- `facets` holds the channel's resolver output:
--   meta     → {geo_locations, education_statuses, flexible_spec[].interests[], excluded_custom_audiences}
--   google   → {geo_targets, audience_segments, keyword_ideas, keyword_volume_estimate}
--   linkedin → {rules: [cohort rule feature names]}
--
-- Read by the console (lib/db.ts listCohortTargetingForRamp) to render the
-- per-channel Targeting panel on the ramp detail page.

CREATE TABLE IF NOT EXISTS cohort_targeting (
    id               BIGSERIAL PRIMARY KEY,
    ramp_id          TEXT NOT NULL,
    cohort_id        TEXT,
    cohort_signature TEXT NOT NULL,
    platform         TEXT NOT NULL,           -- 'linkedin' | 'meta' | 'google'
    facets           JSONB NOT NULL DEFAULT '{}'::jsonb,
    measured_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, cohort_signature, platform)
);

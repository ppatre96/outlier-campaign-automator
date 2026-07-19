-- 012_campaigns_generation.sql
--
-- Additive-launch support: add a `generation` dimension to the campaigns table
-- so an additive relaunch (console "Launch a new variation") creates a NEW
-- generation (v2, v3 …) that COEXISTS with prior ones instead of upserting over
-- them. Each generation keeps its own row / attribution.
--
--   generation  INT NOT NULL DEFAULT 1   (existing rows → 1)
--
-- The unique key changes from 6 cols to 7 (adds generation):
--   (ramp_id, platform, campaign_type, cohort_signature, geo_cluster, angle,
--    generation)
-- so upsert_campaign's ON CONFLICT targets the 7-col key and additive rows with
-- a higher generation insert rather than overwrite.
--
-- Apply:  psql "$DATABASE_URL" -f scripts/sql/012_campaigns_generation.sql
-- Idempotent. src/ui_decisions._ensure_campaigns_generation() also self-heals
-- this once per process, so applying is optional — but apply it before the
-- console change deploys so the console's generation column reads are populated
-- and additive upserts never hit a missing ON CONFLICT target.

ALTER TABLE campaigns ADD COLUMN IF NOT EXISTS generation INT NOT NULL DEFAULT 1;

-- Drop the legacy 6-col unique constraint (whatever its name) so the 7-col
-- index below becomes the ON CONFLICT target.
DO $$
DECLARE c text;
BEGIN
  SELECT conname INTO c FROM pg_constraint
   WHERE conrelid = 'campaigns'::regclass AND contype = 'u'
     AND array_length(conkey, 1) = 6;
  IF c IS NOT NULL THEN
    EXECUTE format('ALTER TABLE campaigns DROP CONSTRAINT %I', c);
  END IF;
END $$;

CREATE UNIQUE INDEX IF NOT EXISTS campaigns_gen_key ON campaigns
  (ramp_id, platform, campaign_type, cohort_signature, geo_cluster, angle, generation);

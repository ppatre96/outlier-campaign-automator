-- One row per created campaign (ramp × platform × cohort × geo × angle).
-- Populated by src.campaign_registry.log_campaign via
-- src.ui_decisions.upsert_campaign (which also self-creates this table with
-- CREATE TABLE IF NOT EXISTS, so applying this file is optional).
--
-- Why this exists: the Campaign Registry Google Sheet silently no-ops in CI
-- whenever credentials.json is absent (SheetsClient → NullSheetsClient), which
-- left the console's Briefs & Campaigns browser empty even though the campaigns
-- were created on every platform. Postgres uses DATABASE_URL (no
-- credentials.json), so this is the resilient source the console reads first.
--
-- `data` holds the full registry entry (asdict(CampaignEntry)) — the same shape
-- as the Sheet's RegistryRow — so the console maps it back to RegistryRow and
-- renders the CampaignBrowser unchanged.
--
-- Read by the console (lib/db.ts listCampaignsForRamp) on the ramp detail page.
-- Idempotent on (ramp_id, platform, campaign_type, cohort_signature,
-- geo_cluster, angle): a re-run updates the slot in place (latest campaign id /
-- creative wins), mirroring how the console dedups the append-only sheet.

CREATE TABLE IF NOT EXISTS campaigns (
    id                   BIGSERIAL PRIMARY KEY,
    ramp_id              TEXT NOT NULL,
    platform             TEXT NOT NULL DEFAULT '',   -- 'linkedin' | 'meta' | 'google'
    campaign_type        TEXT NOT NULL DEFAULT '',   -- 'static' | 'inmail' | 'parent'
    cohort_signature     TEXT NOT NULL DEFAULT '',
    geo_cluster          TEXT NOT NULL DEFAULT '',
    angle                TEXT NOT NULL DEFAULT '',
    cohort_id            TEXT,
    platform_campaign_id TEXT,
    platform_creative_id TEXT,
    campaign_name        TEXT,
    creative_image_path  TEXT,
    data                 JSONB NOT NULL DEFAULT '{}'::jsonb,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE (ramp_id, platform, campaign_type, cohort_signature, geo_cluster, angle)
);

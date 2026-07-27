-- 015_sizing_analyses.sql
--
-- Ad-hoc "sizing analysis" registry (console homepage Sizing section). Each row
-- is a size-up-without-launching run: mine the ICP (from a job post / project /
-- qualified CB IDs) + measure per-channel audience. Results reuse the existing
-- cohort_audience / cohort_icp / cohort_targeting tables keyed by
-- ramp_id = <this row's id> (a synthetic 'SZ-...' analysis id), so the console
-- renders them exactly like a ramp's sizing. This table holds the list + input
-- + status only.
--
-- Apply:  psql "$DATABASE_URL" -f scripts/sql/015_sizing_analyses.sql
-- Idempotent. src/ui_decisions.py also self-heals this via _SIZING_ANALYSES_DDL.

CREATE TABLE IF NOT EXISTS sizing_analyses (
    id           TEXT PRIMARY KEY,
    input_type   TEXT NOT NULL,          -- 'job_post' | 'project' | 'cb_ids'
    input_text   TEXT DEFAULT '',        -- job-post JD text
    project_id   TEXT DEFAULT '',        -- project id (input_type='project')
    cb_ids       TEXT DEFAULT '',        -- comma/newline contributor ids (input_type='cb_ids')
    geos         TEXT DEFAULT '',        -- comma ISO-2 / locale; '' = broad fallback
    platforms    TEXT DEFAULT '',        -- comma channels; '' = config.ENABLED_PLATFORMS
    label        TEXT DEFAULT '',
    status       TEXT NOT NULL DEFAULT 'running',   -- running | done | failed
    error        TEXT DEFAULT '',
    created_by   TEXT DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

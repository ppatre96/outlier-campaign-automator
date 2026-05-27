-- =============================================================================
-- region_funnel (Redash query — not deployed as a view)
-- Originally drafted as a CREATE VIEW DDL; switched to Redash-query form on
-- 2026-05-28 per Quintin's feedback (no downstream consumers needed yet, and
-- repo convention is one .sql per Redash query, not deployed views).
-- Grain: one row per (region × channel)
-- region ∈ {NA, LATAM, WEU, EEU, MENA, SSA, SEA, EA, INDIA, SA, OC, Other}
--
-- Region mapping originally from Quintin Au's lib/country-regions.ts
-- (genai-smart-ramp-v2 repo, 2026-05-27, 8 regions).
-- Extended 2026-05-28 with MENA + SSA + EA (Quintin approved; PR pending
-- in Quintin's repo). Adding these 3 regions recaptures ~10.6% of total CB
-- volume (748k CBs) that was previously falling into 'Other' — primarily
-- KE (847k), EG (520k), ZA (264k), NG (187k), and East Asia (JP/KR/CN).
--
-- Canonical display order:
--   NA, LATAM, WEU, EEU, MENA, SSA, SEA, EA, INDIA, SA, OC, Other.
--
-- Naming caveat: 'SA' is used for South Asia (region label) and 'SA' is
-- also the ISO-2 for Saudi Arabia (which lives in MENA region). These are
-- in different CTE columns (iso2 vs region) so SQL is unambiguous, but a
-- reader needs to know the context.
--
-- A country lives in exactly one region and can appear in REGION_FUNNEL_VIEW
-- and COST_TIER_FUNNEL_VIEW simultaneously (independent group-by axes).
--
-- IMPORTANT — known limitations (identical to COST_TIER_FUNNEL_VIEW):
-- 1. spend_usd, cpa_*:
--      Apportioned via COST_PER_APPLICATION_DIRECT (per-user spend proxy).
--      SPEND_EFFICIENCY is not country/region-grained; brand/retargeting spend
--      not captured. Treat as directional.
-- 2. cpc_usd, ctr_pct:
--      Channel-level aggregates from CAMPAIGN_COSTS — same for all region rows
--      sharing a channel. Not region-grained.
-- 3. resume_uploads:
--      Proxy: users who hit any resume screening event on APPLICATION_CONVERSION.
-- 4. Country source:
--      CESF.IP_COUNTRY_CODE (LEFT JOIN — NULL users → 'Other' region).
--
-- Paste artifacts to verify before deploying (see reference_outlier_country_geo_split.md):
--   - FR: classified here as WEU (almost certainly correct per repo SSOT;
--          Slack paste showed "EU" label — likely typo/truncation).
--   - CH: classified here as WEU (WEU region) + HCC (cost tier in companion view).
--          Slack paste had truncated GROUP_NO key; assignment is SSOT-consistent.
--
-- TODO: Run a spot-check query on live data after deploy:
--   SELECT region, COUNT(DISTINCT USER_ID) FROM REGION_FUNNEL_VIEW GROUP BY 1 ORDER BY 2 DESC
--   Expected top regions: LATAM, INDIA, NA (matches CESF top-20 country distribution).
--
-- Schema-verified 2026-05-28 — same verified columns as COST_TIER_FUNNEL_VIEW.
-- =============================================================================

WITH

-- ── 1. Country → region mapping ───────────────────────────────────────────────
-- Source: Quintin Au, genai-smart-ramp-v2/lib/country-regions.ts, 2026-05-27
country_regions (iso2, region) AS (
    SELECT iso2, region FROM (VALUES
        -- NA — North America (US + CA only; MX is LATAM per SSOT)
        ('US', 'NA'), ('CA', 'NA'),
        -- LATAM — Mexico, Central America, South America, Caribbean
        ('MX', 'LATAM'), ('BR', 'LATAM'), ('AR', 'LATAM'), ('CO', 'LATAM'),
        ('CL', 'LATAM'), ('PE', 'LATAM'), ('VE', 'LATAM'), ('EC', 'LATAM'),
        ('BO', 'LATAM'), ('PY', 'LATAM'), ('UY', 'LATAM'), ('GY', 'LATAM'),
        ('SR', 'LATAM'), ('GF', 'LATAM'), ('GT', 'LATAM'), ('HN', 'LATAM'),
        ('SV', 'LATAM'), ('NI', 'LATAM'), ('CR', 'LATAM'), ('PA', 'LATAM'),
        ('BZ', 'LATAM'), ('CU', 'LATAM'), ('DO', 'LATAM'), ('PR', 'LATAM'),
        ('JM', 'LATAM'), ('HT', 'LATAM'), ('TT', 'LATAM'), ('BB', 'LATAM'),
        ('LC', 'LATAM'), ('VC', 'LATAM'), ('GD', 'LATAM'), ('AG', 'LATAM'),
        ('DM', 'LATAM'), ('KN', 'LATAM'), ('BS', 'LATAM'), ('TC', 'LATAM'),
        ('KY', 'LATAM'),
        -- WEU — Western Europe
        -- NOTE: FR classified WEU (paste artifact flagged; verify against repo SSOT)
        -- NOTE: CH classified WEU (paste artifact flagged; verify against repo SSOT)
        ('GB', 'WEU'), ('IE', 'WEU'), ('FR', 'WEU'), ('NL', 'WEU'),
        ('BE', 'WEU'), ('LU', 'WEU'), ('CH', 'WEU'), ('AT', 'WEU'),
        ('LI', 'WEU'), ('MC', 'WEU'), ('AD', 'WEU'), ('PT', 'WEU'),
        ('ES', 'WEU'), ('IT', 'WEU'), ('SM', 'WEU'), ('VA', 'WEU'),
        ('MT', 'WEU'), ('CY', 'WEU'), ('GR', 'WEU'), ('IS', 'WEU'),
        ('NO', 'WEU'), ('SE', 'WEU'), ('DK', 'WEU'), ('FI', 'WEU'),
        -- EEU — Eastern Europe + former Soviet states
        ('PL', 'EEU'), ('CZ', 'EEU'), ('SK', 'EEU'), ('HU', 'EEU'),
        ('RO', 'EEU'), ('BG', 'EEU'), ('HR', 'EEU'), ('SI', 'EEU'),
        ('BA', 'EEU'), ('RS', 'EEU'), ('ME', 'EEU'), ('MK', 'EEU'),
        ('AL', 'EEU'), ('XK', 'EEU'), ('MD', 'EEU'), ('UA', 'EEU'),
        ('BY', 'EEU'), ('RU', 'EEU'), ('GE', 'EEU'), ('AM', 'EEU'),
        ('AZ', 'EEU'), ('KZ', 'EEU'), ('UZ', 'EEU'), ('TM', 'EEU'),
        ('TJ', 'EEU'), ('KG', 'EEU'),
        -- SEA — Southeast Asia
        ('PH', 'SEA'), ('ID', 'SEA'), ('MY', 'SEA'), ('SG', 'SEA'),
        ('TH', 'SEA'), ('VN', 'SEA'), ('MM', 'SEA'), ('KH', 'SEA'),
        ('LA', 'SEA'), ('BN', 'SEA'), ('TL', 'SEA'),
        -- INDIA — India only (its own region per SSOT)
        ('IN', 'INDIA'),
        -- SA — South Asia excluding India
        ('LK', 'SA'), ('BD', 'SA'), ('PK', 'SA'), ('NP', 'SA'),
        ('MV', 'SA'), ('BT', 'SA'),
        -- OC — Oceania
        ('AU', 'OC'), ('NZ', 'OC'), ('PG', 'OC'), ('FJ', 'OC'),
        ('SB', 'OC'), ('VU', 'OC'), ('WS', 'OC'), ('TO', 'OC'),
        ('KI', 'OC'), ('NR', 'OC'), ('TV', 'OC'), ('MH', 'OC'),
        ('FM', 'OC'), ('CK', 'OC'), ('NU', 'OC'), ('TK', 'OC'),
        -- MENA — Middle East + North Africa (added 2026-05-28 after Quintin
        -- approved extension; PR for lib/country-regions.ts pending).
        -- Covers Gulf states, Levant, Iran, Turkey, Egypt + North African coast.
        ('SA', 'MENA'), ('AE', 'MENA'), ('QA', 'MENA'), ('KW', 'MENA'),
        ('BH', 'MENA'), ('OM', 'MENA'), ('IL', 'MENA'), ('JO', 'MENA'),
        ('LB', 'MENA'), ('SY', 'MENA'), ('IQ', 'MENA'), ('IR', 'MENA'),
        ('YE', 'MENA'), ('TR', 'MENA'), ('PS', 'MENA'),
        ('EG', 'MENA'), ('MA', 'MENA'), ('DZ', 'MENA'), ('TN', 'MENA'),
        ('LY', 'MENA'), ('SD', 'MENA'),
        -- Note: 'SA' iso2 above is Saudi Arabia (MENA region); 'SA' STRING
        -- as a region label means South Asia — two different uses of 'SA'.
        -- The CTE column types disambiguate (iso2 vs region).
        -- SSA — Sub-Saharan Africa (added 2026-05-28).
        -- KE 847k + ZA 264k + NG 187k = ~1.3M of the prior 'Other' bucket.
        ('NG', 'SSA'), ('KE', 'SSA'), ('ZA', 'SSA'), ('GH', 'SSA'),
        ('ET', 'SSA'), ('UG', 'SSA'), ('TZ', 'SSA'), ('RW', 'SSA'),
        ('ZM', 'SSA'), ('ZW', 'SSA'), ('MZ', 'SSA'), ('AO', 'SSA'),
        ('CD', 'SSA'), ('CM', 'SSA'), ('CI', 'SSA'), ('SN', 'SSA'),
        ('MG', 'SSA'), ('BJ', 'SSA'), ('BF', 'SSA'), ('NE', 'SSA'),
        ('ML', 'SSA'), ('MR', 'SSA'), ('BW', 'SSA'), ('NA', 'SSA'),
        ('SS', 'SSA'), ('MW', 'SSA'), ('LS', 'SSA'), ('SZ', 'SSA'),
        ('GA', 'SSA'), ('GN', 'SSA'), ('GM', 'SSA'), ('SL', 'SSA'),
        ('LR', 'SSA'), ('CG', 'SSA'), ('CF', 'SSA'), ('TD', 'SSA'),
        ('ER', 'SSA'), ('DJ', 'SSA'), ('SO', 'SSA'), ('BI', 'SSA'),
        ('ST', 'SSA'), ('CV', 'SSA'), ('TG', 'SSA'),
        -- EA — East Asia (added 2026-05-28).
        ('JP', 'EA'), ('KR', 'EA'), ('CN', 'EA'), ('TW', 'EA'),
        ('HK', 'EA'), ('MO', 'EA'), ('MN', 'EA'),
        -- Sentinel — real unmapped countries fall through COALESCE to 'Other'.
        ('XX', 'Other')
    ) AS t (iso2, region)
),

-- ── 2. Per-user channel classification (same precedence rule as COST_TIER view)
user_channel AS (
    SELECT
        ac.USER_ID,
        CASE
            WHEN u.WORKER_SOURCE = 'inqa_coder'    THEN 'MWF'
            WHEN u.WORKER_SOURCE = 'latam_coder'   THEN 'LATAM Coders'
            WHEN u.WORKER_SOURCE = 'in_squads'     THEN 'Squads'
            WHEN ac.ATTRIBUTED_SOURCE = 'organic' AND ac.SOURCED_FROM = 'organic'
                                                   THEN 'Organic - Direct'
            WHEN ac.ATTRIBUTED_SOURCE = 'referrals' THEN 'Referrals'
            WHEN ac.SOURCED_FROM = 'meta' OR ac.ATTRIBUTED_SOURCE = 'meta'
                                                   THEN 'Paid - Meta'
            WHEN (ac.SOURCED_FROM = 'linkedin' OR ac.ATTRIBUTED_SOURCE LIKE '%linkedin%')
                AND (ac.UTM_MEDIUM IN ('paid', 'cpc', 'ppc')
                     OR ac.ATTRIBUTED_SOURCE = 'linkedin_ads')
                                                   THEN 'Paid - LinkedIn'
            WHEN ac.SOURCED_FROM = 'joveo' OR ac.ATTRIBUTED_SOURCE = 'joveo'
                                                   THEN 'Paid - Joveo'
            WHEN ac.UTM_MEDIUM IN ('paid', 'cpc', 'ppc')
                                                   THEN 'Paid - Other'
            ELSE 'Other'
        END                                        AS channel,
        COALESCE(ac.COST_PER_APPLICATION_DIRECT, 0) AS spend_per_user
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN SCALE_PROD.PUBLIC.USERS u
        ON ac.USER_ID = u._ID
    WHERE ac.SIGNUP_DAY < CURRENT_DATE
),

-- ── 3. Core per-user funnel spine ─────────────────────────────────────────────
user_funnel AS (
    SELECT
        ac.USER_ID,
        ac.SIGNUP_DAY,
        COALESCE(cesf.IP_COUNTRY_CODE, 'XX')       AS ip_country_code,
        uc.channel,
        uc.spend_per_user,
        CASE WHEN ac.RESUME_SCREENING_PASS_DAY IS NOT NULL
              OR  ac.RESUME_SCREENING_FAIL_DAY IS NOT NULL
             THEN 1 ELSE 0 END                     AS had_resume_upload,
        CASE WHEN ac.ACTIVATION_DAY IS NOT NULL
             THEN 1 ELSE 0 END                     AS activated,
        COALESCE(ac.TOTAL_PRODUCTIVE_HOURS, 0)     AS active_hours
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN SCALE_PROD.VIEW.CONTRIBUTOR_EARLY_SUCCESS_FUNNEL cesf
        ON ac.USER_ID = cesf.USER_ID
    LEFT JOIN user_channel uc
        ON ac.USER_ID = uc.USER_ID
    WHERE ac.SIGNUP_DAY < CURRENT_DATE
),

-- ── 4. Per-user task count ────────────────────────────────────────────────────
user_tasks AS (
    SELECT
        ta.ATTEMPTED_BY                            AS USER_ID,
        COUNT(*)                                   AS tasks_done
    FROM SCALE_PROD.PUBLIC.TASKATTEMPTS ta
    WHERE ta.IS_PRODUCTIVE = TRUE
    GROUP BY ta.ATTEMPTED_BY
),

-- ── 5. Channel-level CTR / CPC from CAMPAIGN_COSTS ───────────────────────────
channel_perf AS (
    SELECT
        CASE
            WHEN cc.SOURCE = 'meta'                THEN 'Paid - Meta'
            WHEN cc.SOURCE LIKE '%linkedin%'       THEN 'Paid - LinkedIn'
            WHEN cc.SOURCE = 'joveo'               THEN 'Paid - Joveo'
            ELSE 'Paid - Other'
        END                                         AS channel,
        SUM(cc.CLICKS)                              AS total_clicks,
        SUM(cc.IMPRESSIONS)                         AS total_impressions,
        CASE WHEN SUM(cc.IMPRESSIONS) > 0
             THEN SUM(cc.CLICKS) * 100.0 / SUM(cc.IMPRESSIONS)
             ELSE NULL END                          AS ctr_pct,
        CASE WHEN SUM(cc.CLICKS) > 0
             THEN SUM(cc.COST) / SUM(cc.CLICKS)
             ELSE NULL END                          AS cpc_usd
    FROM SCALE_PROD.VIEW.CAMPAIGN_COSTS cc
    GROUP BY cc.SOURCE
),

-- ── 6. Aggregate by (region × channel) ───────────────────────────────────────
final AS (
    SELECT
        -- Countries not in country_regions VALUES → COALESCE to 'Other'
        COALESCE(cr.region, 'Other')                AS region,
        COALESCE(uf.channel, 'Other')               AS channel,
        COUNT(DISTINCT uf.USER_ID)                  AS cb_count,
        COUNT(DISTINCT uf.USER_ID)                  AS signups,
        SUM(uf.had_resume_upload)                   AS resume_uploads,
        SUM(uf.activated)                           AS project_activations,
        SUM(COALESCE(ut.tasks_done, 0))             AS tasks_done,
        SUM(uf.active_hours)                        AS active_hours,
        SUM(uf.spend_per_user)                      AS spend_usd,
        CASE WHEN COUNT(DISTINCT uf.USER_ID) > 0
             THEN SUM(uf.spend_per_user) / COUNT(DISTINCT uf.USER_ID)
             ELSE NULL END                          AS cpa_signup,
        CASE WHEN SUM(uf.activated) > 0
             THEN SUM(uf.spend_per_user) / NULLIF(SUM(uf.activated), 0)
             ELSE NULL END                          AS cpa_activation,
        CASE WHEN SUM(COALESCE(ut.tasks_done, 0)) > 0
             THEN SUM(uf.spend_per_user) / NULLIF(SUM(CASE WHEN ut.tasks_done > 0 THEN 1 ELSE 0 END), 0)
             ELSE NULL END                          AS cpa_task_started
    FROM user_funnel uf
    LEFT JOIN country_regions cr
        ON uf.ip_country_code = cr.iso2
    LEFT JOIN user_tasks ut
        ON uf.USER_ID = ut.USER_ID
    GROUP BY 1, 2
)

SELECT
    -- Region in canonical display order
    f.region,
    f.channel,
    f.cb_count,
    f.signups,
    f.resume_uploads,
    f.project_activations,
    f.tasks_done,
    f.active_hours,
    f.spend_usd,
    -- CPC / CTR: channel-level only (not region-grained)
    cp.cpc_usd,
    cp.ctr_pct,
    f.cpa_signup,
    f.cpa_activation,
    f.cpa_task_started
FROM final f
LEFT JOIN channel_perf cp
    ON f.channel = cp.channel
ORDER BY
    CASE f.region
        WHEN 'NA'    THEN 1
        WHEN 'LATAM' THEN 2
        WHEN 'WEU'   THEN 3
        WHEN 'EEU'   THEN 4
        WHEN 'MENA'  THEN 5
        WHEN 'SSA'   THEN 6
        WHEN 'SEA'   THEN 7
        WHEN 'EA'    THEN 8
        WHEN 'INDIA' THEN 9
        WHEN 'SA'    THEN 10
        WHEN 'OC'    THEN 11
        ELSE 12
    END,
    f.channel
;

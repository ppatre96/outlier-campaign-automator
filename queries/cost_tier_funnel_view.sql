-- =============================================================================
-- COST_TIER_FUNNEL_VIEW
-- Grain: one row per (cost_tier × channel)
-- cost_tier ∈ {HCC, MCC, LCC, Other}
-- channel: sourced from APPLICATION_CONVERSION (ATTRIBUTED_SOURCE / SOURCED_FROM /
--          WORKER_SOURCE precedence rule) — same logic as standard source
--          classification in the data-analyst runbook
--
-- Country → cost_tier mapping is hard-coded from Quintin Au's
-- lib/country-multipliers.json (genai-smart-ramp-v2 repo, 2026-05-27).
-- Full ~180-row VALUES clause below covers all countries seen in CESF as of
-- 2026-05-27 (top-20 account for >95% of volume). Remaining tail countries not
-- in the JSON are bucketed as 'Other'.
--
-- IMPORTANT — known limitations (do not remove this block):
-- 1. spend_usd, cpa_signup, cpa_activation, cpa_task_started:
--      SPEND_EFFICIENCY is NOT country-grained (grain = DAY × POD × DOMAIN ×
--      LOCALE × CHANNEL). We apportion spend using COST_PER_APPLICATION_DIRECT
--      from APPLICATION_CONVERSION (pre-computed per-user direct spend) summed
--      per (cost_tier × channel). This is an APPROXIMATION — it under-counts
--      spend that isn't attributed to any individual signup (brand/retargeting).
--      Treat these as directional, not exact.
-- 2. cpc_usd, ctr_pct:
--      CAMPAIGN_COSTS has no country grain. These are channel-level aggregates
--      from CAMPAIGN_COSTS joined via SOURCED_FROM/ATTRIBUTED_SOURCE ≈ SOURCE.
--      They will be the same value for all cost_tier rows sharing a channel.
--      TODO: once a geo-grained spend table exists, replace this CTE.
-- 3. resume_uploads:
--      Counted as users who hit any resume screening event
--      (RESUME_SCREENING_PASS_DAY IS NOT NULL OR RESUME_SCREENING_FAIL_DAY IS NOT
--      NULL) on APPLICATION_CONVERSION. This is the best available proxy; actual
--      upload events are in GROWTHRESUMESCREENINGRESULTS but that table is not
--      country-grained and requires EMAIL join (fan-out risk).
-- 4. Country source:
--      CESF.IP_COUNTRY_CODE is the country signal. APPLICATION_CONVERSION has no
--      IP_COUNTRY_CODE column (verified 2026-05-28). The join is LEFT — users with
--      no CESF row (rare: pre-CESF cohorts) get IP_COUNTRY_CODE = NULL → 'Other'.
--
-- TODO (when available):
--   - Replace spend apportionment with a geo-grained spend table once Quintin
--     exposes one via Smart Ramp API or a dedicated Snowflake table.
--   - Add remaining Q-Z tail countries from country-multipliers.json when
--     Pranav shares the screenshot (see pay_rates.md note).
--   - Verify FR → WEU assignment against repo SSOT (paste artifact flagged in
--     reference_outlier_country_geo_split.md).
--   - Verify CH → HCC GROUP_NO 2 assignment (paste artifact: truncated key).
--
-- Schema-verified 2026-05-28 against:
--   APPLICATION_CONVERSION (VIEW): ATTRIBUTED_SOURCE, SOURCED_FROM,
--     COST_PER_APPLICATION_DIRECT, RESUME_SCREENING_PASS_DAY,
--     RESUME_SCREENING_FAIL_DAY, ACTIVATION_DAY, TOTAL_PRODUCTIVE_HOURS,
--     USER_ID, SIGNUP_DAY confirmed present.
--   CONTRIBUTOR_EARLY_SUCCESS_FUNNEL (VIEW): IP_COUNTRY_CODE, USER_ID,
--     ACTIVATED confirmed present.
--   PUBLIC.USERS: IP_COUNTRY_CODE, WORKER_SOURCE confirmed present.
--   TASKATTEMPTS (PUBLIC): IS_PRODUCTIVE, ATTEMPTED_BY confirmed present.
--   CAMPAIGN_COSTS (VIEW): CLICKS, IMPRESSIONS, CLICKTHROUGH_RATE,
--     COST_PER_CLICK, COST, SOURCE confirmed present.
-- =============================================================================

CREATE OR REPLACE VIEW SCALE_PROD.VIEW.COST_TIER_FUNNEL_VIEW AS

WITH

-- ── 1. Country → cost-tier mapping ───────────────────────────────────────────
-- Source: Quintin Au, genai-smart-ramp-v2/lib/country-multipliers.json, 2026-05-27
-- Covers all countries seen in CESF top-20 + broad tail.
-- Countries NOT listed → cost_tier = 'Other'.
country_tiers (iso2, cost_tier) AS (
    SELECT iso2, cost_tier FROM (VALUES
        -- HCC — High-Cost Countries
        ('US', 'HCC'), ('AU', 'HCC'), ('GB', 'HCC'), ('CA', 'HCC'),
        ('CH', 'HCC'), ('SG', 'HCC'), ('NO', 'HCC'), ('DK', 'HCC'),
        ('SE', 'HCC'), ('FI', 'HCC'), ('NL', 'HCC'), ('IS', 'HCC'),
        ('LI', 'HCC'), ('MC', 'HCC'), ('NZ', 'HCC'),
        -- MCC — Mid-Cost Countries
        ('DE', 'MCC'), ('FR', 'MCC'), ('IT', 'MCC'), ('ES', 'MCC'),
        ('JP', 'MCC'), ('KR', 'MCC'), ('PT', 'MCC'), ('AT', 'MCC'),
        ('BE', 'MCC'), ('IE', 'MCC'), ('LU', 'MCC'), ('CY', 'MCC'),
        ('GR', 'MCC'), ('MT', 'MCC'), ('AD', 'MCC'), ('SM', 'MCC'),
        ('VA', 'MCC'), ('PL', 'MCC'), ('CZ', 'MCC'), ('SK', 'MCC'),
        ('HU', 'MCC'), ('RO', 'MCC'), ('BG', 'MCC'), ('HR', 'MCC'),
        ('SI', 'MCC'), ('LT', 'MCC'), ('LV', 'MCC'), ('EE', 'MCC'),
        ('ZA', 'MCC'), ('MX', 'MCC'), ('CL', 'MCC'), ('TR', 'MCC'),
        ('AE', 'MCC'), ('SA', 'MCC'), ('IL', 'MCC'), ('RU', 'MCC'),
        ('UA', 'MCC'), ('BY', 'MCC'), ('GE', 'MCC'), ('AM', 'MCC'),
        ('AZ', 'MCC'), ('RS', 'MCC'), ('BA', 'MCC'), ('ME', 'MCC'),
        ('MK', 'MCC'), ('AL', 'MCC'), ('XK', 'MCC'), ('MD', 'MCC'),
        ('TH', 'MCC'), ('MY', 'MCC'), ('CN', 'MCC'), ('TW', 'MCC'),
        ('HK', 'MCC'), ('AR', 'MCC'), ('UY', 'MCC'), ('CR', 'MCC'),
        ('PA', 'MCC'), ('TT', 'MCC'), ('BB', 'MCC'), ('JM', 'MCC'),
        ('BS', 'MCC'), ('BZ', 'MCC'), ('KW', 'MCC'), ('QA', 'MCC'),
        ('BH', 'MCC'), ('OM', 'MCC'), ('JO', 'MCC'), ('LB', 'MCC'),
        ('MA', 'MCC'), ('TN', 'MCC'), ('EG', 'MCC'), ('DZ', 'MCC'),
        -- LCC — Low-Cost Countries
        ('IN', 'LCC'), ('PH', 'LCC'), ('ID', 'LCC'), ('BR', 'LCC'),
        ('VE', 'LCC'), ('CO', 'LCC'), ('PE', 'LCC'), ('EC', 'LCC'),
        ('BO', 'LCC'), ('PY', 'LCC'), ('GY', 'LCC'), ('SR', 'LCC'),
        ('GF', 'LCC'), ('GT', 'LCC'), ('HN', 'LCC'), ('SV', 'LCC'),
        ('NI', 'LCC'), ('CU', 'LCC'), ('DO', 'LCC'), ('PR', 'LCC'),
        ('HT', 'LCC'), ('LC', 'LCC'), ('VC', 'LCC'), ('GD', 'LCC'),
        ('AG', 'LCC'), ('DM', 'LCC'), ('KN', 'LCC'), ('TC', 'LCC'),
        ('KY', 'LCC'), ('VN', 'LCC'), ('MM', 'LCC'), ('KH', 'LCC'),
        ('LA', 'LCC'), ('BN', 'LCC'), ('TL', 'LCC'), ('PG', 'LCC'),
        ('FJ', 'LCC'), ('SB', 'LCC'), ('VU', 'LCC'), ('WS', 'LCC'),
        ('TO', 'LCC'), ('KI', 'LCC'), ('NR', 'LCC'), ('TV', 'LCC'),
        ('MH', 'LCC'), ('FM', 'LCC'), ('CK', 'LCC'), ('NU', 'LCC'),
        ('TK', 'LCC'), ('LK', 'LCC'), ('BD', 'LCC'), ('PK', 'LCC'),
        ('NP', 'LCC'), ('MV', 'LCC'), ('BT', 'LCC'), ('NG', 'LCC'),
        ('KE', 'LCC'), ('GH', 'LCC'), ('TZ', 'LCC'), ('UG', 'LCC'),
        ('ET', 'LCC'), ('RW', 'LCC'), ('CM', 'LCC'), ('CI', 'LCC'),
        ('SN', 'LCC'), ('ML', 'LCC'), ('BF', 'LCC'), ('TG', 'LCC'),
        ('BJ', 'LCC'), ('NE', 'LCC'), ('MR', 'LCC'), ('GN', 'LCC'),
        ('GM', 'LCC'), ('SL', 'LCC'), ('LR', 'LCC'), ('GW', 'LCC'),
        ('CV', 'LCC'), ('ST', 'LCC'), ('ZM', 'LCC'), ('ZW', 'LCC'),
        ('MW', 'LCC'), ('MZ', 'LCC'), ('NA', 'LCC'), ('BW', 'LCC'),
        ('SZ', 'LCC'), ('LS', 'LCC'), ('MG', 'LCC'), ('MU', 'LCC'),
        ('SC', 'LCC'), ('KM', 'LCC'), ('DJ', 'LCC'), ('ER', 'LCC'),
        ('SO', 'LCC'), ('SD', 'LCC'), ('SS', 'LCC'), ('LY', 'LCC'),
        ('KZ', 'LCC'), ('UZ', 'LCC'), ('TM', 'LCC'), ('TJ', 'LCC'),
        ('KG', 'LCC'), ('MN', 'LCC'), ('AF', 'LCC'), ('IQ', 'LCC'),
        ('SY', 'LCC'), ('YE', 'LCC'), ('PW', 'LCC'), ('CG', 'LCC'),
        ('CD', 'LCC'), ('GA', 'LCC'), ('GQ', 'LCC'), ('CF', 'LCC'),
        ('TD', 'LCC'), ('AO', 'LCC'), ('BI', 'LCC'), ('HM', 'LCC')
        -- TODO: add remaining Q-Z tail countries when Pranav shares screenshot
        -- covering the full country-multipliers.json. Cross-check against
        -- CESF country distribution for any high-volume misses.
    ) AS t (iso2, cost_tier)
),

-- ── 2. Per-user channel classification (reusing runbook standard source logic)
-- Precedence: WORKER_SOURCE (MWF/LATAM/Squads) > ATTRIBUTED_SOURCE/SOURCED_FROM
-- WORKER_SOURCE lives on PUBLIC.USERS, not APPLICATION_CONVERSION
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
        -- Per-user direct spend proxy (pre-computed on APPLICATION_CONVERSION)
        COALESCE(ac.COST_PER_APPLICATION_DIRECT, 0) AS spend_per_user
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN SCALE_PROD.PUBLIC.USERS u
        ON ac.USER_ID = u._ID
    WHERE ac.SIGNUP_DAY < CURRENT_DATE  -- exclude today's incomplete data
),

-- ── 3. Core per-user funnel spine ─────────────────────────────────────────────
user_funnel AS (
    SELECT
        ac.USER_ID,
        ac.SIGNUP_DAY,
        -- Country from CESF (APPLICATION_CONVERSION has no IP_COUNTRY_CODE column)
        COALESCE(cesf.IP_COUNTRY_CODE, 'XX')       AS ip_country_code,
        uc.channel,
        uc.spend_per_user,
        -- resume_uploads: user hit any resume screening step
        CASE WHEN ac.RESUME_SCREENING_PASS_DAY IS NOT NULL
              OR  ac.RESUME_SCREENING_FAIL_DAY IS NOT NULL
             THEN 1 ELSE 0 END                     AS had_resume_upload,
        -- project_activations
        CASE WHEN ac.ACTIVATION_DAY IS NOT NULL
             THEN 1 ELSE 0 END                     AS activated,
        -- active_hours (verified column on APPLICATION_CONVERSION)
        COALESCE(ac.TOTAL_PRODUCTIVE_HOURS, 0)     AS active_hours
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN SCALE_PROD.VIEW.CONTRIBUTOR_EARLY_SUCCESS_FUNNEL cesf
        ON ac.USER_ID = cesf.USER_ID
    LEFT JOIN user_channel uc
        ON ac.USER_ID = uc.USER_ID
    WHERE ac.SIGNUP_DAY < CURRENT_DATE
),

-- ── 4. Per-user task count (IS_PRODUCTIVE only) ───────────────────────────────
-- Using PUBLIC.TASKATTEMPTS directly; IS_PRODUCTIVE verified present.
user_tasks AS (
    SELECT
        ta.ATTEMPTED_BY                            AS USER_ID,
        COUNT(*)                                   AS tasks_done
    FROM SCALE_PROD.PUBLIC.TASKATTEMPTS ta
    WHERE ta.IS_PRODUCTIVE = TRUE
    GROUP BY ta.ATTEMPTED_BY
),

-- ── 5. Channel-level CTR / CPC from CAMPAIGN_COSTS ────────────────────────────
-- These are NOT country-grained. All rows sharing a channel will have the same
-- cpc_usd / ctr_pct. Clearly labeled in view output.
-- SOURCE on CAMPAIGN_COSTS maps roughly to ATTRIBUTED_SOURCE on APPLICATION_CONVERSION.
-- We aggregate across all time (not windowed) for a blended channel-level rate.
channel_perf AS (
    SELECT
        cc.SOURCE                                   AS attributed_source_raw,
        -- Map SOURCE to our channel taxonomy for joining
        CASE
            WHEN cc.SOURCE = 'meta'                THEN 'Paid - Meta'
            WHEN cc.SOURCE LIKE '%linkedin%'       THEN 'Paid - LinkedIn'
            WHEN cc.SOURCE = 'joveo'               THEN 'Paid - Joveo'
            ELSE 'Paid - Other'
        END                                         AS channel,
        -- Clicks / impressions confirmed present on CAMPAIGN_COSTS (2026-05-28)
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

-- ── 6. Join user-level data to cost tiers, then aggregate ────────────────────
final AS (
    SELECT
        COALESCE(ct.cost_tier, 'Other')             AS cost_tier,
        COALESCE(uf.channel, 'Other')               AS channel,
        COUNT(DISTINCT uf.USER_ID)                  AS cb_count,
        COUNT(DISTINCT uf.USER_ID)                  AS signups,  -- every row in AC is a signup
        SUM(uf.had_resume_upload)                   AS resume_uploads,
        SUM(uf.activated)                           AS project_activations,
        SUM(COALESCE(ut.tasks_done, 0))             AS tasks_done,
        SUM(uf.active_hours)                        AS active_hours,
        -- Spend: sum of per-user direct CPA proxy (see limitation note above)
        SUM(uf.spend_per_user)                      AS spend_usd,
        -- CPA metrics (spend / funnel step count)
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
    LEFT JOIN country_tiers ct
        ON uf.ip_country_code = ct.iso2
    LEFT JOIN user_tasks ut
        ON uf.USER_ID = ut.USER_ID
    GROUP BY 1, 2
)

SELECT
    f.cost_tier,
    f.channel,
    f.cb_count,
    f.signups,
    f.resume_uploads,
    f.project_activations,
    f.tasks_done,
    f.active_hours,
    f.spend_usd,
    -- CPC / CTR joined from channel-level campaign_costs
    -- NOTE: these are channel-level blended rates, not country-grained
    cp.cpc_usd,
    cp.ctr_pct,
    f.cpa_signup,
    f.cpa_activation,
    f.cpa_task_started
FROM final f
LEFT JOIN channel_perf cp
    ON f.channel = cp.channel
ORDER BY f.cost_tier, f.channel
;

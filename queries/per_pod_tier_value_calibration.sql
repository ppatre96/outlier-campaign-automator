-- Per-Pod Tier Value Calibration
-- Purpose: give Quentin the per-(pod × tier) lifetime-value distribution so the
--          flat $50/$75/$100 T1/T2/T3 constants in ifnd_6a161027c1c2ad7aff752e7a
--          can be replaced with pod-specific values.
--
-- Schema verified 2026-05-28 via INFORMATION_SCHEMA + direct sampling:
--   APPLICATION_CONVERSION.WORKER_TOTAL_PAY          — lifetime CB earnings (USD)
--   APPLICATION_CONVERSION.ACTIVATION_PROJECT_POD    — pod label (set only after activation)
--   APPLICATION_CONVERSION.ONBOARDING_FLOW_POD       — pod label pre-activation
--   APPLICATION_CONVERSION.ACTIVATION_PROJECT_ID     — project at first activation
--   APPLICATION_CONVERSION.ACTIVATION_DAY            — date of first activation
--   APPLICATION_CONVERSION.TOTAL_PRODUCTIVE_HOURS    — lifetime productive hours
--   CONTRIBUTOR_EARLY_SUCCESS_FUNNEL.EVER_PASSED_SKILL_SCREENING — T1 flag
--   CONTRIBUTOR_EARLY_SUCCESS_FUNNEL.FIRST_OCP_ENDED_IN_SUCCESS  — T2 flag
--   CONTRIBUTOR_EARLY_SUCCESS_FUNNEL.ACP_STARTED                 — T2.5 flag
--   CONTRIBUTOR_EARLY_SUCCESS_FUNNEL.ACTIVATED                   — T3 flag
--   DIM_PROJECTS.PROJECT_ID (NOT _ID)  — join key confirmed
--   DIM_PROJECTS.OPS_POD               — secondary pod label
--   PAY_HOURLY_RATES.WORKER            — joins to APPLICATION_CONVERSION.USER_ID
--   PAY_HOURLY_RATES.PAY_HOURS         — hours column (confirmed, not BASE_PAY_HOURS)
--   PAY_HOURLY_RATES.GROSS_PAY         — earnings column (confirmed)
--
-- Tier definitions (CESF funnel flags — NOT prestige tiers in profile_tiering.py):
--   T1   = EVER_PASSED_SKILL_SCREENING = TRUE   (resume/skill screen passed)
--   T2   = FIRST_OCP_ENDED_IN_SUCCESS  = TRUE   (onboarding course passed; T1 implied)
--   T2.5 = ACP_STARTED                 = TRUE   (activated course project started; T2 implied)
--   T3   = ACTIVATED                   = TRUE   (first productive task completed; T2.5 implied)
--
-- Assignment: each CB receives their HIGHEST achieved tier so cells are mutually
-- exclusive and sum to the total CESF population for that pod.
--
-- T4 NOT DEFINED in CESF. No equivalent boolean flag exists.
-- TODO(Quentin): if you want a T4 (e.g. "promoted to reviewer" or "experienced"),
--   define it and we can add a flag from USERPROJECTEVENTS or
--   DIM_PAYABLES.ENGAGEMENT_LEVEL.
--
-- Pod assignment strategy (verified 2026-05-28 via Redash sampling):
--   Activated CBs (T3): use ACTIVATION_PROJECT_POD (lowercase-normalised).
--     Values confirmed: 'Languages', 'Experts', 'Generalists', 'Coding'
--   Pre-activation CBs (T1/T2/T2.5): ACTIVATION_PROJECT_POD is NULL; use
--     ONBOARDING_FLOW_POD (lowercase: 'languages', 'specialists', 'coders',
--     'generalist', 'not-set', NULL). Map to canonical labels.
--   CBs with NULL pod in both columns are excluded (can't be attributed to a pod).
--
-- Canonical pod mapping confirmed from APPLICATION_CONVERSION sampling:
--   ACTIVATION_PROJECT_POD  →  canonical
--   'Languages'             →  Languages
--   'Experts'               →  Experts
--   'Generalists'           →  Generalists
--   'Coding'                →  Coding
--
--   ONBOARDING_FLOW_POD     →  canonical
--   'languages'             →  Languages
--   'specialists'           →  Experts
--   'coders'                →  Coding
--   'generalist'            →  Generalists
--
-- Excluded pods: 'Non Allocated Training', 'OTS', 'Pilots', 'Scale Eval',
--   'Other', 'Training/Screening', 'Mathgen', 'Enterprise', 'not-set', NULL
--   These have thin or ambiguous semantics for value-calibration purposes.
--
-- Lifetime-value source: APPLICATION_CONVERSION.WORKER_TOTAL_PAY
--   Canonical realized-earnings column, already in USD. Pre-activation CBs
--   (T1/T2/T2.5) will have WORKER_TOTAL_PAY = 0 or NULL — that IS the
--   signal (they haven't earned yet). Quentin can read the T3 median as
--   the realized LTV and the T1/T2 medians as $0/$near-$0 to understand
--   the economic gap between funnel tiers.
--
-- sample_n_jobs: COUNT(DISTINCT ACTIVATION_PROJECT_ID) per (pod × tier) —
--   tells Quentin how many distinct projects contributed data to each cell.
--   For T1/T2, this is the number of distinct onboarding projects touched.
--
-- Cohort: all CBs with a CESF row signed up before today.
--   No 180d closure filter applied — Quentin needs the full distribution
--   for recalibration; closure would undersample growing pods.
--   Median (not AVG) is used for the central estimate to resist tail distortion.
--
-- Authored: 2026-05-28 by outlier-data-analyst.
-- Revised:  2026-05-28 — removed ACTIVATION_DAY IS NOT NULL filter (was
--           collapsing all CBs to T3); now uses ONBOARDING_FLOW_POD fallback
--           for pre-activation CBs; both pod columns normalised to canonical labels.

WITH cesf AS (
    SELECT
        USER_ID,
        COALESCE(EVER_PASSED_SKILL_SCREENING, FALSE) AS is_t1,
        COALESCE(FIRST_OCP_ENDED_IN_SUCCESS,  FALSE) AS is_t2,
        COALESCE(ACP_STARTED,                 FALSE) AS is_t25,
        COALESCE(ACTIVATED,                   FALSE) AS is_t3
    FROM SCALE_PROD.VIEW.CONTRIBUTOR_EARLY_SUCCESS_FUNNEL
),

-- One row per CB from APPLICATION_CONVERSION.
-- We do NOT filter on ACTIVATION_DAY here so that T1/T2 CBs are included.
all_cbs AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID,
        -- Pod assignment: prefer activation pod (post-T3); fall back to
        -- onboarding flow pod (available for T1/T2/T2.5).
        CASE
            -- Post-activation pod labels (as found in ACTIVATION_PROJECT_POD)
            WHEN ac.ACTIVATION_PROJECT_POD = 'Coding'       THEN 'Coding'
            WHEN ac.ACTIVATION_PROJECT_POD = 'Languages'    THEN 'Languages'
            WHEN ac.ACTIVATION_PROJECT_POD = 'Experts'      THEN 'Experts'
            WHEN ac.ACTIVATION_PROJECT_POD = 'Generalists'  THEN 'Generalists'
            -- Onboarding flow pod fallback (pre-activation; lowercase values)
            WHEN ac.ONBOARDING_FLOW_POD = 'coders'          THEN 'Coding'
            WHEN ac.ONBOARDING_FLOW_POD = 'languages'       THEN 'Languages'
            WHEN ac.ONBOARDING_FLOW_POD = 'specialists'     THEN 'Experts'
            WHEN ac.ONBOARDING_FLOW_POD = 'generalist'      THEN 'Generalists'
            ELSE NULL   -- excluded (Non Allocated Training / OTS / not-set / NULL)
        END                                 AS pod,
        ac.WORKER_TOTAL_PAY                 AS lifetime_value_usd,
        ac.TOTAL_PRODUCTIVE_HOURS           AS lifetime_productive_hours
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    WHERE ac.SIGNUP_DAY < CURRENT_DATE      -- exclude today's incomplete data
),

-- Assign highest achieved tier per CB using CESF flags.
cb_tiers AS (
    SELECT
        acb.USER_ID,
        acb.pod,
        acb.ACTIVATION_PROJECT_ID,
        acb.lifetime_value_usd,
        acb.lifetime_productive_hours,
        CASE
            WHEN cesf.is_t3   THEN 'T3'
            WHEN cesf.is_t25  THEN 'T2.5'
            WHEN cesf.is_t2   THEN 'T2'
            WHEN cesf.is_t1   THEN 'T1'
            ELSE 'T0'  -- signed up but no CESF match or all flags false
        END                                 AS tier
    FROM all_cbs acb
    -- INNER JOIN: only CBs with a CESF row are usable for calibration.
    -- CBs who signed up but were never processed by the funnel have no
    -- tier signal and should not inflate the T0/NULL bucket.
    INNER JOIN cesf ON acb.USER_ID = cesf.USER_ID
    WHERE acb.pod IS NOT NULL               -- drop unattributed pods
)

-- Final aggregation: one row per (tier × pod).
SELECT
    tier,
    pod,
    COUNT(DISTINCT USER_ID)                                      AS n_cbs,
    -- For T3: n_activations = n_cbs (by definition).
    -- For T1/T2: n_activations shows how many of this tier later activated
    --   on ANY project (useful for Quentin to see conversion headroom).
    COUNT(DISTINCT CASE WHEN lifetime_value_usd > 0
                        THEN USER_ID END)                        AS n_with_earnings,
    COUNT(DISTINCT ACTIVATION_PROJECT_ID)                        AS sample_n_jobs,

    -- Lifetime value distribution (USD, realized earnings to date).
    -- T1/T2 CBs who have not activated will show $0 — intentional.
    ROUND(AVG(COALESCE(lifetime_value_usd, 0)), 2)               AS avg_lifetime_value_usd,
    ROUND(MEDIAN(COALESCE(lifetime_value_usd, 0)), 2)            AS median_lifetime_value_usd,
    ROUND(PERCENTILE_CONT(0.25) WITHIN GROUP
        (ORDER BY COALESCE(lifetime_value_usd, 0)), 2)           AS p25_lifetime_value_usd,
    ROUND(PERCENTILE_CONT(0.75) WITHIN GROUP
        (ORDER BY COALESCE(lifetime_value_usd, 0)), 2)           AS p75_lifetime_value_usd,

    -- Activated-only LTV (excludes $0 earners) — gives a cleaner per-activated-CB
    -- baseline for Quentin to compare against the flat $50/$75/$100 constants.
    ROUND(MEDIAN(CASE WHEN lifetime_value_usd > 0
                      THEN lifetime_value_usd END), 2)           AS median_ltv_earners_only_usd,

    -- Productive hours (sanity: should correlate with earnings).
    ROUND(MEDIAN(COALESCE(lifetime_productive_hours, 0)), 1)     AS median_productive_hours,
    ROUND(AVG(COALESCE(lifetime_productive_hours, 0)), 1)        AS avg_productive_hours

FROM cb_tiers
GROUP BY
    tier,
    pod
ORDER BY
    -- Sort by pod (Coding first — highest expected variance), then tier (T3 → T1)
    CASE pod WHEN 'Coding'      THEN 1 WHEN 'Experts'      THEN 2
             WHEN 'Generalists' THEN 3 WHEN 'Languages'    THEN 4 END,
    CASE tier WHEN 'T3' THEN 1 WHEN 'T2.5' THEN 2
              WHEN 'T2' THEN 3 WHEN 'T1'   THEN 4 WHEN 'T0' THEN 5 END

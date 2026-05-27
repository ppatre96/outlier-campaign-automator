-- Snowflake auto pay-rate resolver — signup_flow_id → primary qualification T1 USD rate.
--
-- Replaces the `OUTLIER_BASE_RATE_USD` env-var manual override with a Snowflake
-- source-of-truth lookup. Called per-ramp by src/pay_rate_resolver.py before
-- group_geos_for_campaigns(); falls back to None on no-data (rate-free copy).
--
-- Authored: 2026-05-24 by outlier-data-analyst.
-- Schema-corrected: 2026-05-27 — `QUALIFICATION_PAY_RATES` is flat (one row per
-- tier with `PAY_RATE` column, NOT T1/T2/T3/T4 columns) and the join key for
-- `PROJECT_QUALIFICATIONS_LONG` is `_ID` (NOT `QUALIFICATION_ID`).
-- Schema-corrected again 2026-05-27 (T1-rate bug surfaced via smoke test): each
-- TIER of a qualification is a SEPARATE qualification_id in
-- `QUALIFICATION_PAY_RATES`. The tier label lives in the `QUALIFICATION_NAME`
-- suffix `… overall: T1/T2/T3/T4`. Previous pivot (ROW_NUMBER PARTITION BY
-- QUALIFICATION_ID ORDER BY PAY_RATE ASC) was wrong — every partition had size 1
-- → every row got tier_rank=1, so a T3 rate was labelled T1_RATE_USD. We now
-- extract tier_rank from the name suffix and pivot by `base_name` (the prefix
-- before `: Tn`). PROJECT_QUALIFICATIONS_LONG._ID resolves to a SPECIFIC tier's
-- qual_id; we follow that → its family base_name → the full T1/T2/T3/T4 row.
--
-- Primary-qualification heuristic (tie-break order in the final ROW_NUMBER):
--   1. Exclude IS_PAY_MULTIPLIER = TRUE  (those are adjustments, not base role)
--   2. Exclude IS_ASSESSMENT     = TRUE  (gates access, doesn't set rate)
--   3. Prefer non-language qualifications (language quals are locale-priced)
--   4. Highest T1_RATE_USD               (highest-value billed skill = advertised role)
--   5. Alphabetic qualification_id       (determinism on ties)
--
-- Fallback: when SIGNUPFLOWS.JOB_POST_IDS is NULL OR [] (~20-30% of internal
-- flows + most new Meta paid flows), resolve project_id via
-- APPLICATION_CONVERSION.STARTING_PROJECT_ID.

WITH flow_project AS (
    SELECT DISTINCT
        sf._id                            AS signup_flow_id,
        TRIM(jp_arr.value::STRING)        AS project_id
    FROM PUBLIC.SIGNUPFLOWS sf,
    LATERAL FLATTEN(input => sf.JOB_POST_IDS) jp_arr   -- column is JOB_POST_IDS (with underscore)
    WHERE sf._id = '{signup_flow_id}'
),
qpr_decoded AS (
    -- Each row in QUALIFICATION_PAY_RATES is a tier-specific qualification.
    -- The tier number lives in the NAME suffix like "Mathematics overall: T3".
    -- Extract tier_rank + base_name so we can pivot the 4 tier rows of a family.
    SELECT
        QUALIFICATION_ID,
        QUALIFICATION_NAME,
        PAY_RATE,
        TRY_CAST(REGEXP_SUBSTR(QUALIFICATION_NAME, ':\\s*T([0-9]+)\\s*$', 1, 1, 'e', 1) AS INT) AS tier_rank,
        -- Strip the ": Tn" suffix → family base_name (e.g. "Mathematics overall").
        -- Rows without a tier suffix keep their full name as base_name; they
        -- collapse to a single T1-only family below.
        TRIM(REGEXP_REPLACE(QUALIFICATION_NAME, '\\s*:\\s*T[0-9]+\\s*$', '')) AS base_name
    FROM SCALE_PROD.VIEW.QUALIFICATION_PAY_RATES
),
qual_family_rates AS (
    -- Pivot all 4 tier rows of a family (matched by base_name) into T1-T4 columns.
    -- COALESCE NULL tier_rank → 1 so single-row families (no tier suffix) keep
    -- their rate as T1 rather than vanishing.
    SELECT
        base_name,
        MAX(CASE WHEN COALESCE(tier_rank, 1) = 1 THEN PAY_RATE END) AS T1_RATE_USD,
        MAX(CASE WHEN tier_rank = 2 THEN PAY_RATE END)              AS T2_RATE_USD,
        MAX(CASE WHEN tier_rank = 3 THEN PAY_RATE END)              AS T3_RATE_USD,
        MAX(CASE WHEN tier_rank = 4 THEN PAY_RATE END)              AS T4_RATE_USD
    FROM qpr_decoded
    GROUP BY base_name
),
qpr_id_to_family AS (
    -- Map each tier-specific qual_id back to its family + bound tier so we can
    -- resolve PROJECT_QUALIFICATIONS_LONG._ID → family rates without losing
    -- which specific tier the project was actually contracted at.
    SELECT QUALIFICATION_ID, base_name, tier_rank AS bound_tier
    FROM qpr_decoded
),
qual_rates_pivoted AS (
    -- One row per (project, qualification) with the full T1-T4 rate set for
    -- the qual family — NOT the single bound-tier rate (that was the prior bug).
    SELECT
        pq._ID                            AS qualification_id,
        pq.NAME                           AS qualification_name,
        pq.PROJECT_ID                     AS project_id,
        pq.QUALIFICATION_TYPE,
        pq.IS_ASSESSMENT,
        pq.IS_PAY_MULTIPLIER,
        qif.base_name                     AS pay_rate_family,
        qif.bound_tier                    AS bound_tier,
        qfr.T1_RATE_USD,
        qfr.T2_RATE_USD,
        qfr.T3_RATE_USD,
        qfr.T4_RATE_USD
    FROM SCALE_PROD.VIEW.PROJECT_QUALIFICATIONS_LONG pq
    LEFT JOIN qpr_id_to_family qif
        ON qif.QUALIFICATION_ID = pq._ID  -- join key is _ID, not QUALIFICATION_ID
    LEFT JOIN qual_family_rates qfr
        ON qfr.base_name = qif.base_name
    WHERE pq._ID IS NOT NULL
),
qualifications AS (
    SELECT
        fp.signup_flow_id,
        qrp.project_id,
        qrp.qualification_id,
        qrp.qualification_name,
        qrp.QUALIFICATION_TYPE,
        qrp.IS_ASSESSMENT,
        qrp.IS_PAY_MULTIPLIER,
        qrp.T1_RATE_USD, qrp.T2_RATE_USD, qrp.T3_RATE_USD, qrp.T4_RATE_USD,
        ROW_NUMBER() OVER (
            PARTITION BY fp.signup_flow_id
            ORDER BY
                CASE WHEN qrp.IS_PAY_MULTIPLIER = TRUE THEN 1 ELSE 0 END ASC,
                CASE WHEN qrp.IS_ASSESSMENT     = TRUE THEN 1 ELSE 0 END ASC,
                CASE WHEN LOWER(qrp.QUALIFICATION_TYPE) = 'language' THEN 1 ELSE 0 END ASC,
                COALESCE(qrp.T1_RATE_USD, 0) DESC,
                qrp.qualification_id ASC
        )                                 AS PRIMARY_RANK
    FROM flow_project fp
    JOIN qual_rates_pivoted qrp
        ON qrp.project_id = fp.project_id
    WHERE qrp.qualification_id IS NOT NULL
),
ac_project AS (
    -- A single signup_flow_id can map to MULTIPLE starting_project_id values
    -- in APPLICATION_CONVERSION (confirmed by Quintin Au 2026-05-28: not a 1:1
    -- mapping). Previous `LIMIT 1` with no ORDER BY was non-deterministic —
    -- different project_id picked across runs. Using MODE() to pick the most
    -- common starting_project_id across all users with this flow_id (the most
    -- statistically representative project for the fallback qual lookup).
    -- Alternative would be MAX_BY(starting_project_id, application_day) to
    -- bias toward the most recently-used project; mode is more stable across
    -- runs and matches the "what are users on this flow typically doing"
    -- semantic of this fallback CTE.
    SELECT
        '{signup_flow_id}'                              AS signup_flow_id,
        MODE(ac.STARTING_PROJECT_ID)                    AS project_id
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    WHERE ac.SIGNUP_FLOW_ID = '{signup_flow_id}'
      AND ac.STARTING_PROJECT_ID IS NOT NULL
),
fallback_quals AS (
    SELECT
        ap.signup_flow_id,
        qrp.project_id,
        qrp.qualification_id,
        qrp.qualification_name,
        qrp.QUALIFICATION_TYPE,
        qrp.IS_ASSESSMENT,
        qrp.IS_PAY_MULTIPLIER,
        qrp.T1_RATE_USD, qrp.T2_RATE_USD, qrp.T3_RATE_USD, qrp.T4_RATE_USD,
        ROW_NUMBER() OVER (
            PARTITION BY ap.signup_flow_id
            ORDER BY
                CASE WHEN qrp.IS_PAY_MULTIPLIER = TRUE THEN 1 ELSE 0 END ASC,
                CASE WHEN qrp.IS_ASSESSMENT     = TRUE THEN 1 ELSE 0 END ASC,
                CASE WHEN LOWER(qrp.QUALIFICATION_TYPE) = 'language' THEN 1 ELSE 0 END ASC,
                COALESCE(qrp.T1_RATE_USD, 0) DESC,
                qrp.qualification_id ASC
        )                                 AS PRIMARY_RANK
    FROM ac_project ap
    JOIN qual_rates_pivoted qrp
        ON qrp.project_id = ap.project_id
    WHERE NOT EXISTS (
        SELECT 1 FROM flow_project fp2
        WHERE fp2.signup_flow_id = '{signup_flow_id}'
    )
    AND qrp.qualification_id IS NOT NULL
)
SELECT
    signup_flow_id                        AS SIGNUP_FLOW_ID,
    project_id                            AS PROJECT_ID,
    qualification_id                      AS QUALIFICATION_ID,
    qualification_name                    AS QUALIFICATION_NAME,
    QUALIFICATION_TYPE,
    IS_PAY_MULTIPLIER,
    IS_ASSESSMENT,
    T1_RATE_USD, T2_RATE_USD, T3_RATE_USD, T4_RATE_USD,
    PRIMARY_RANK
FROM qualifications
UNION ALL
SELECT
    signup_flow_id, project_id, qualification_id, qualification_name,
    QUALIFICATION_TYPE, IS_PAY_MULTIPLIER, IS_ASSESSMENT,
    T1_RATE_USD, T2_RATE_USD, T3_RATE_USD, T4_RATE_USD, PRIMARY_RANK
FROM fallback_quals
ORDER BY PRIMARY_RANK ASC
LIMIT 5

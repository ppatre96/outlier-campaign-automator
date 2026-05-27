-- Snowflake auto pay-rate resolver — signup_flow_id → primary qualification T1 USD rate.
--
-- Replaces the `OUTLIER_BASE_RATE_USD` env-var manual override with a Snowflake
-- source-of-truth lookup. Called per-ramp by src/pay_rate_resolver.py before
-- group_geos_for_campaigns(); falls back to None on no-data (rate-free copy).
--
-- Authored: 2026-05-24 by outlier-data-analyst.
-- Schema-corrected: 2026-05-27 — `QUALIFICATION_PAY_RATES` is flat (one row per
-- tier with `PAY_RATE` column, NOT T1/T2/T3/T4 columns) and the join key for
-- `PROJECT_QUALIFICATIONS_LONG` is `_ID` (NOT `QUALIFICATION_ID`). We pivot
-- tiers via ROW_NUMBER ordered ASC on PAY_RATE → tier_rank 1=T1 (cheapest,
-- newest CB) through 4=T4 (most-experienced CB).
--
-- Primary-qualification heuristic (tie-break order in the final ROW_NUMBER):
--   1. Exclude IS_PAY_MULTIPLIER = TRUE  (those are adjustments, not base role)
--   2. Exclude IS_ASSESSMENT     = TRUE  (gates access, doesn't set rate)
--   3. Prefer non-language qualifications (language quals are locale-priced)
--   4. Highest T1_RATE_USD               (highest-value billed skill = advertised role)
--   5. Alphabetic qualification_id       (determinism on ties)
--
-- Fallback: when SIGNUPFLOWS.JOBPOST_IDS is NULL (~20-30% of internal flows),
-- resolve project_id via APPLICATION_CONVERSION.STARTING_PROJECT_ID.

WITH flow_project AS (
    SELECT DISTINCT
        sf._id                            AS signup_flow_id,
        TRIM(jp_arr.value::STRING)        AS project_id
    FROM PUBLIC.SIGNUPFLOWS sf,
    LATERAL FLATTEN(input => sf.JOBPOST_IDS) jp_arr
    WHERE sf._id = '{signup_flow_id}'
),
qpr_with_tier AS (
    -- Annotate the flat PAY_RATE table with per-qualification tier rank
    -- (1 = lowest rate = T1 newest CB, 4 = highest rate = T4 most-experienced).
    SELECT
        QUALIFICATION_ID,
        PAY_RATE,
        ROW_NUMBER() OVER (
            PARTITION BY QUALIFICATION_ID
            ORDER BY PAY_RATE ASC
        )                                 AS tier_rank
    FROM SCALE_PROD.VIEW.QUALIFICATION_PAY_RATES
),
qual_rates_pivoted AS (
    -- Collapse multi-row tiers into one row per qualification with T1-T4 cols.
    SELECT
        pq._ID                            AS qualification_id,
        pq.NAME                           AS qualification_name,
        pq.PROJECT_ID                     AS project_id,
        pq.QUALIFICATION_TYPE,
        pq.IS_ASSESSMENT,
        pq.IS_PAY_MULTIPLIER,
        MAX(CASE WHEN qpr.tier_rank = 1 THEN qpr.PAY_RATE END) AS T1_RATE_USD,
        MAX(CASE WHEN qpr.tier_rank = 2 THEN qpr.PAY_RATE END) AS T2_RATE_USD,
        MAX(CASE WHEN qpr.tier_rank = 3 THEN qpr.PAY_RATE END) AS T3_RATE_USD,
        MAX(CASE WHEN qpr.tier_rank = 4 THEN qpr.PAY_RATE END) AS T4_RATE_USD
    FROM SCALE_PROD.VIEW.PROJECT_QUALIFICATIONS_LONG pq
    LEFT JOIN qpr_with_tier qpr
        ON qpr.QUALIFICATION_ID = pq._ID   -- ⚠️ join key is _ID, not QUALIFICATION_ID
    WHERE pq._ID IS NOT NULL
    GROUP BY
        pq._ID, pq.NAME, pq.PROJECT_ID, pq.QUALIFICATION_TYPE,
        pq.IS_ASSESSMENT, pq.IS_PAY_MULTIPLIER
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
    SELECT DISTINCT
        '{signup_flow_id}'                AS signup_flow_id,
        ac.STARTING_PROJECT_ID            AS project_id
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    WHERE ac.SIGNUP_FLOW_ID = '{signup_flow_id}'
      AND ac.STARTING_PROJECT_ID IS NOT NULL
    LIMIT 1
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

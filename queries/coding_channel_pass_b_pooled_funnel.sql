-- Pass B: Pooled coding-project funnel metrics per channel
-- Replace PROJECT_IDS list with qualifying project IDs from Pass A
-- Data source: _Snowflake (GenAI Ops), DS ID 30

WITH channel_classified AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID AS project_id,
        ac.ACTIVATION_DAY,
        ac.DISABLED,
        ac.QUALITY_ESTIMATE,
        ac.AVERAGE_QMS_FIRST_3_TASKS,
        ac.AVG_QC_SCORE_FIRST_3_TASKS,
        CASE
            WHEN u.worker_source = 'inqa_coder'  THEN 'MWF'
            WHEN u.worker_source = 'in_squads'   THEN 'Squads'
            WHEN u.worker_source = 'latam_coder' THEN 'LATAM'
            WHEN LOWER(ac.UTM_SOURCE) LIKE '%joveo%'   THEN 'Joveo'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
            WHEN ac.UTM_SOURCE IS NULL
              OR LOWER(ac.UTM_SOURCE) IN ('organic','organic-social','organic-socials') THEN 'Organic'
            ELSE 'Other'
        END AS channel_bucket
    FROM VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN PUBLIC.USERS u ON ac.USER_ID = u._id
    WHERE ac.ACTIVATION_PROJECT_ID IN (
        '69978f2116e00455a4fd2977',  -- RFP - Master Project
        '67ccdb9bedd9cfae8214ddfc',  -- SWEAP Augmentation - Public Repo
        '683e729ecaf6d16374e6fc5b',  -- Agent Completion Process Supervision Pt 3
        '69b09791e4d5a9e4419c35ab',  -- Code Checkpoint Evals
        '67ae58c1c9f428dcb51ca14e',  -- [SCALE_CODE_SFT] Coding Physics Simulation
        '684b44b20401b17af2427d5d',  -- SWE Full Trace - Entry Level Tasks
        '68cb05ef58bc6f7919b6f099'   -- Data Analysis Agents - Rubrics
    )
      AND ac.ACTIVATION_DAY < CURRENT_DATE
      AND ac.ACTIVATION_DAY IS NOT NULL
),
cbpr_joined AS (
    SELECT
        cc.USER_ID,
        cc.project_id,
        cc.channel_bucket,
        cc.DISABLED,
        cc.QUALITY_ESTIMATE,
        cc.AVERAGE_QMS_FIRST_3_TASKS,
        cc.AVG_QC_SCORE_FIRST_3_TASKS,
        cc.ACTIVATION_DAY,
        s.TOTAL_HOURS,
        s.ACTIVE_DAYS
    FROM channel_classified cc
    LEFT JOIN VIEW.CBPR__USER_PROJECT_STATS s
        ON cc.USER_ID = s.USER_ID AND cc.project_id = s.PROJECT_ID
),
retention_30d AS (
    SELECT
        channel_bucket,
        COUNT(*) AS cohort_30d,
        SUM(CASE WHEN COALESCE(ACTIVE_DAYS,0) >= 5 THEN 1 ELSE 0 END) AS retained_30d
    FROM cbpr_joined
    WHERE ACTIVATION_DAY <= DATEADD(day, -30, CURRENT_DATE)
    GROUP BY channel_bucket
),
retention_60d AS (
    SELECT
        channel_bucket,
        COUNT(*) AS cohort_60d,
        SUM(CASE WHEN COALESCE(ACTIVE_DAYS,0) >= 10 THEN 1 ELSE 0 END) AS retained_60d
    FROM cbpr_joined
    WHERE ACTIVATION_DAY <= DATEADD(day, -60, CURRENT_DATE)
    GROUP BY channel_bucket
),
main_agg AS (
    SELECT
        channel_bucket,
        COUNT(DISTINCT USER_ID) AS activated_cbs,
        AVG(COALESCE(TOTAL_HOURS, 0)) AS avg_total_hours,
        AVG(COALESCE(ACTIVE_DAYS, 0)) AS avg_active_days,
        AVG(CASE WHEN COALESCE(ACTIVE_DAYS,0) > 0 THEN COALESCE(TOTAL_HOURS,0)/ACTIVE_DAYS ELSE NULL END) AS avg_hours_per_active_day,
        SUM(CASE WHEN DISABLED = TRUE THEN 1 ELSE 0 END)::FLOAT / COUNT(*) AS disabled_rate,
        SUM(CASE WHEN QUALITY_ESTIMATE = 'good' THEN 1 ELSE 0 END) AS quality_good,
        SUM(CASE WHEN QUALITY_ESTIMATE = 'bad'  THEN 1 ELSE 0 END) AS quality_bad,
        SUM(CASE WHEN QUALITY_ESTIMATE = 'pending' THEN 1 ELSE 0 END) AS quality_pending,
        COUNT(CASE WHEN AVERAGE_QMS_FIRST_3_TASKS IS NOT NULL THEN 1 END) AS qms_non_null,
        AVG(AVERAGE_QMS_FIRST_3_TASKS) AS avg_qms_first3,
        COUNT(CASE WHEN AVG_QC_SCORE_FIRST_3_TASKS IS NOT NULL THEN 1 END) AS qc_non_null,
        AVG(AVG_QC_SCORE_FIRST_3_TASKS) AS avg_qc_first3
    FROM cbpr_joined
    GROUP BY channel_bucket
)
SELECT
    m.*,
    r30.cohort_30d,
    r30.retained_30d,
    CASE WHEN r30.cohort_30d > 0 THEN r30.retained_30d::FLOAT/r30.cohort_30d ELSE NULL END AS retention_30d_rate,
    r60.cohort_60d,
    r60.retained_60d,
    CASE WHEN r60.cohort_60d > 0 THEN r60.retained_60d::FLOAT/r60.cohort_60d ELSE NULL END AS retention_60d_rate
FROM main_agg m
LEFT JOIN retention_30d r30 ON m.channel_bucket = r30.channel_bucket
LEFT JOIN retention_60d r60 ON m.channel_bucket = r60.channel_bucket
ORDER BY m.activated_cbs DESC

-- Table A2: Activated CBs per channel per project (no Multimango, no Squads)
-- Grain: one row per project; columns = activated CB count per channel bucket
-- Activation = ACTIVATION_PROJECT_ID match (first project CB activated on)
-- Data source: _Snowflake (GenAI Ops), DS ID 30

WITH coding_projects AS (
    SELECT PROJECT_ID, PROJECT_NAME,
           CREATED_AT_UTC::DATE AS project_created,
           PROJECT_STATUS
    FROM VIEW.DIM_PROJECTS
    WHERE (OPS_POD = 'GenAI: Code'
       OR LOWER(PROJECT_NAME) LIKE '%code%'
       OR LOWER(PROJECT_NAME) LIKE '%coder%'
       OR LOWER(PROJECT_NAME) LIKE '%swe%'
       OR LOWER(PROJECT_NAME) LIKE '%python%'
       OR LOWER(PROJECT_NAME) LIKE '%programming%'
       OR LOWER(PROJECT_NAME) LIKE '%software%'
       OR LOWER(PROJECT_NAME) LIKE '%coding%')
      AND LOWER(PROJECT_NAME) NOT LIKE '%multimango%'
      AND PROJECT_NAME NOT LIKE '[Pay] HA%'
      AND PROJECT_NAME NOT LIKE '[Pay] TL%'
      AND PROJECT_NAME NOT LIKE 'TEST%'
      AND PROJECT_NAME NOT LIKE 'Test Project%'
),
activation_channel AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID AS project_id,
        ac.ACTIVATION_DAY,
        CASE
            WHEN u.worker_source = 'inqa_coder'  THEN 'MWF'
            WHEN u.worker_source = 'latam_coder' THEN 'LATAM'
            WHEN LOWER(ac.UTM_SOURCE) LIKE '%joveo%'   THEN 'Joveo'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
            WHEN ac.UTM_SOURCE IS NULL
              OR LOWER(ac.UTM_SOURCE) IN ('organic','organic-social','organic-socials') THEN 'Organic'
            ELSE 'Other'
        END AS channel_bucket
    FROM VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN PUBLIC.USERS u ON ac.USER_ID = u._id
    WHERE ac.ACTIVATION_DAY IS NOT NULL
      AND ac.ACTIVATION_DAY < CURRENT_DATE
      AND ac.ACTIVATION_PROJECT_ID IS NOT NULL
      AND (u.worker_source != 'in_squads' OR u.worker_source IS NULL)
),
pcc AS (
    SELECT
        project_id,
        SUM(CASE WHEN channel_bucket = 'MWF'      THEN 1 ELSE 0 END) AS mwf_activated,
        SUM(CASE WHEN channel_bucket = 'LATAM'    THEN 1 ELSE 0 END) AS latam_activated,
        SUM(CASE WHEN channel_bucket = 'Joveo'    THEN 1 ELSE 0 END) AS joveo_activated,
        SUM(CASE WHEN channel_bucket = 'LinkedIn' THEN 1 ELSE 0 END) AS linkedin_activated,
        SUM(CASE WHEN channel_bucket = 'Organic'  THEN 1 ELSE 0 END) AS organic_activated,
        COUNT(DISTINCT USER_ID) AS total_activated
    FROM activation_channel
    GROUP BY project_id
)
SELECT
    cp.PROJECT_NAME, cp.PROJECT_ID, cp.project_created, cp.PROJECT_STATUS,
    pcc.mwf_activated, pcc.latam_activated, pcc.joveo_activated,
    pcc.linkedin_activated, pcc.organic_activated, pcc.total_activated
FROM coding_projects cp
JOIN pcc ON cp.PROJECT_ID = pcc.project_id
WHERE pcc.mwf_activated >= 5 OR pcc.latam_activated >= 10
ORDER BY (pcc.mwf_activated + pcc.latam_activated) DESC
LIMIT 20

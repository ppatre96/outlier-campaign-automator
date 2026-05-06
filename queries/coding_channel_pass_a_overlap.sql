-- Pass A: Top coding projects (all-time) with program channel representation
-- Classifier: OPS_POD = 'GenAI: Code' OR name pattern; excludes [Pay] HA/TL sub-projects
-- Threshold: MWF>=30 OR LATAM>=30 OR Squads>=10
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
      AND PROJECT_NAME NOT LIKE '[Pay] HA%'
      AND PROJECT_NAME NOT LIKE '[Pay] TL%'
      AND PROJECT_NAME NOT LIKE 'TEST%'
      AND PROJECT_NAME NOT LIKE 'Test Project%'
),
channel_classified AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID AS project_id,
        ac.ACTIVATION_DAY,
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
    WHERE ac.ACTIVATION_DAY < CURRENT_DATE
      AND ac.ACTIVATION_PROJECT_ID IS NOT NULL
),
project_channel_counts AS (
    SELECT
        cc.project_id,
        SUM(CASE WHEN cc.channel_bucket = 'MWF'      THEN 1 ELSE 0 END) AS mwf_n,
        SUM(CASE WHEN cc.channel_bucket = 'LATAM'    THEN 1 ELSE 0 END) AS latam_n,
        SUM(CASE WHEN cc.channel_bucket = 'Squads'   THEN 1 ELSE 0 END) AS squads_n,
        SUM(CASE WHEN cc.channel_bucket = 'Joveo'    THEN 1 ELSE 0 END) AS joveo_n,
        SUM(CASE WHEN cc.channel_bucket = 'LinkedIn' THEN 1 ELSE 0 END) AS linkedin_n,
        SUM(CASE WHEN cc.channel_bucket = 'Organic'  THEN 1 ELSE 0 END) AS organic_n,
        COUNT(DISTINCT cc.USER_ID) AS total_n,
        MIN(cc.ACTIVATION_DAY) AS first_activation,
        MAX(cc.ACTIVATION_DAY) AS last_activation
    FROM channel_classified cc
    GROUP BY cc.project_id
)
SELECT
    cp.PROJECT_NAME,
    cp.PROJECT_ID,
    cp.project_created,
    cp.PROJECT_STATUS,
    pcc.mwf_n,
    pcc.latam_n,
    pcc.squads_n,
    pcc.joveo_n,
    pcc.linkedin_n,
    pcc.organic_n,
    pcc.total_n,
    pcc.first_activation,
    pcc.last_activation
FROM coding_projects cp
JOIN project_channel_counts pcc ON cp.PROJECT_ID = pcc.project_id
WHERE pcc.mwf_n >= 10 OR pcc.latam_n >= 10 OR pcc.squads_n >= 5
ORDER BY (pcc.mwf_n + pcc.latam_n + pcc.squads_n) DESC
LIMIT 30

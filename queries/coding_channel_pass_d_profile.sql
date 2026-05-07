-- Pass D: Profile comparison — MWF+LATAM ("program") vs LinkedIn activated CBs on 7 coding projects
-- Treatment: inqa_coder + latam_coder (pooled as "program")
-- Control: LinkedIn UTM
-- Sources: WORKER_RESUME_SUMMARY, TNS_WORKER_LINKEDIN, TNS_WORKER_ALL_SIGNALS (for geo)
-- Data source: _Snowflake (GenAI Ops), DS ID 30

WITH coding_activated AS (
    SELECT
        ac.USER_ID,
        CASE
            WHEN u.worker_source = 'inqa_coder'  THEN 'program'
            WHEN u.worker_source = 'latam_coder' THEN 'program'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'linkedin'
            ELSE NULL
        END AS bucket,
        u.worker_source
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
      AND ac.ACTIVATION_DAY IS NOT NULL
      AND ac.ACTIVATION_DAY < CURRENT_DATE
      AND (u.worker_source != 'in_squads' OR u.worker_source IS NULL)
),
filtered AS (
    SELECT * FROM coding_activated WHERE bucket IS NOT NULL
)
SELECT
    f.USER_ID,
    f.bucket,
    f.worker_source,
    r.RESUME_DEGREE,
    r.RESUME_FIELD,
    r.RESUME_JOB_TITLE,
    r.RESUME_JOB_COMPANY,
    r.RESUME_JOB_SKILLS,
    li.LINKEDIN_URL,
    li.LINKEDIN_EDUCATION,
    li.LINKEDIN_CERTIFICATIONS,
    tns.GEO_GROUP,
    tns.IP_COUNTRY_CODE AS tns_country_code
FROM filtered f
LEFT JOIN SCALE_PROD.VIEW.WORKER_RESUME_SUMMARY r ON f.USER_ID = r.USER_ID
LEFT JOIN SCALE_PROD.VIEW.TNS_WORKER_LINKEDIN li ON f.USER_ID = li.WORKER
LEFT JOIN VIEW.TNS_WORKER_ALL_SIGNALS tns ON f.USER_ID = tns.WORKER
ORDER BY f.bucket, f.USER_ID

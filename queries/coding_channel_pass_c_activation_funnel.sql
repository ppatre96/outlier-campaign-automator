-- Pass C: Activation funnel per channel
-- Spine: APPLICATION_CONVERSION (all signups, not pre-filtered to activated)
-- Screening: GROWTHRESUMESCREENINGRESULTS joined on EMAIL (latest result per user)
-- Activation proxy: ACTIVATION_PROJECT_ID IN (7 qualifying coding project IDs)
-- NOTE: Program channels (MWF/LATAM/Squads) use direct placement, not catalog browsing.
--       Activation rate proxy is NOT directly comparable across program vs. paid populations.
-- Data source: _Snowflake (GenAI Ops), DS ID 30

WITH channel_classified AS (
    SELECT
        ac.USER_ID,
        ac.EMAIL,
        ac.SIGNUP_DAY,
        ac.ACTIVATION_PROJECT_ID,
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
    WHERE ac.SIGNUP_DAY < CURRENT_DATE
),
screening AS (
    SELECT
        g.CANDIDATE_EMAIL,
        MAX(CASE WHEN g.RESULT = 'pass' THEN 1 ELSE 0 END) AS ever_passed_screening
    FROM PUBLIC.GROWTHRESUMESCREENINGRESULTS g
    GROUP BY g.CANDIDATE_EMAIL
),
joined AS (
    SELECT
        cc.channel_bucket,
        cc.USER_ID,
        cc.EMAIL,
        CASE WHEN s.CANDIDATE_EMAIL IS NOT NULL THEN 1 ELSE 0 END AS was_screened,
        COALESCE(s.ever_passed_screening, 0) AS screening_passed,
        CASE WHEN cc.ACTIVATION_PROJECT_ID IN (
            '69978f2116e00455a4fd2977',  -- RFP - Master Project
            '67ccdb9bedd9cfae8214ddfc',  -- SWEAP Augmentation - Public Repo
            '683e729ecaf6d16374e6fc5b',  -- Agent Completion Process Supervision Pt 3
            '69b09791e4d5a9e4419c35ab',  -- Code Checkpoint Evals
            '67ae58c1c9f428dcb51ca14e',  -- [SCALE_CODE_SFT] Coding Physics Simulation
            '684b44b20401b17af2427d5d',  -- SWE Full Trace - Entry Level Tasks
            '68cb05ef58bc6f7919b6f099'   -- Data Analysis Agents - Rubrics
        ) THEN 1 ELSE 0 END AS activated_coding
    FROM channel_classified cc
    LEFT JOIN screening s ON cc.EMAIL = s.CANDIDATE_EMAIL
)
SELECT
    channel_bucket,
    COUNT(DISTINCT USER_ID)                                             AS total_signups,
    SUM(was_screened)                                                    AS n_screened,
    SUM(screening_passed)                                                AS n_screening_passed,
    SUM(activated_coding)                                                AS n_activated_coding,
    CASE WHEN SUM(was_screened) > 0
         THEN SUM(screening_passed)::FLOAT / SUM(was_screened) ELSE NULL END AS screening_pass_rate,
    SUM(activated_coding)::FLOAT / COUNT(DISTINCT USER_ID)              AS activation_rate_proxy
FROM joined
GROUP BY channel_bucket
ORDER BY total_signups DESC

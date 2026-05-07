-- Pass G: Course pass rate by channel across 7 coding projects.
-- Answers: does capability (course pass rate) explain the MWF/LATAM quality gap vs LinkedIn?
--
-- Schema findings:
--   VIEW.DIM_PROJECT_COURSES : (OWNER_ID, COURSE_ID) — maps project → required courses
--   VIEW.DIM_COURSE_PROGRESSES : (USER_ID, COURSE_ID, PROJECT_ID, IS_COMPLETED, STATUS, ...)
--     IS_COMPLETED = TRUE covers BOTH pass and fail completions.
--     STATUS = 'pass' is the true pass signal (distinct from 'failed' and 'in_progress').
--     IS_SKIPPED = TRUE with STATUS = 'pass' = pre-vetted / exempted pass (counts as pass).
--   "Enrolled/allocated" is defined as: has ≥1 row in DIM_COURSE_PROGRESSES for a course
--     belonging to the project via DIM_PROJECT_COURSES. There is no separate enrollment table;
--     the progress table is populated when a CB is assigned to / attempts a course.
--
-- Key data note:
--   RFP - Master Project (69978f2116e00455a4fd2977) has 0 entries in DIM_PROJECT_COURSES —
--   no required courses are configured in this table. All RFP CBs show 0% enrollment in this
--   analysis. It is excluded from the per-project pass rate table.
--
--   "all required courses passed" is a very high bar: Agent Completion Pt3 requires 30 courses,
--   SWEAP requires 10, etc. Virtually no CB across any channel clears this threshold.
--   The meaningful metric is AVG_ATTEMPT_PASS_RATE (STATUS='pass' / total encountered courses)
--   and the conditional any-pass rate among those with course records.
--
-- Data source: _Snowflake (GenAI Ops), DS ID 30
-- Redash query IDs: pooled = 303226, per-project = 303227

WITH channel_classified AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID AS project_id,
        ac.ACTIVATION_DAY,
        CASE
            WHEN u.WORKER_SOURCE = 'inqa_coder'  THEN 'MWF'
            WHEN u.WORKER_SOURCE = 'latam_coder' THEN 'LATAM'
            WHEN LOWER(ac.UTM_SOURCE) LIKE '%joveo%'   THEN 'Joveo'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
            WHEN ac.UTM_SOURCE IS NULL
              OR LOWER(ac.UTM_SOURCE) IN ('organic','organic-social','organic-socials') THEN 'Organic'
            ELSE 'Other'
        END AS channel_bucket
    FROM VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN PUBLIC.USERS u ON ac.USER_ID = u._ID
    WHERE ac.ACTIVATION_PROJECT_ID IN (
        '69978f2116e00455a4fd2977',  -- RFP - Master Project (no DIM_PROJECT_COURSES entries)
        '67ccdb9bedd9cfae8214ddfc',  -- SWEAP Augmentation - Public Repo (10 courses)
        '683e729ecaf6d16374e6fc5b',  -- Agent Completion Process Supervision Pt 3 (30 courses)
        '69b09791e4d5a9e4419c35ab',  -- Code Checkpoint Evals (10 courses)
        '67ae58c1c9f428dcb51ca14e',  -- Coding Physics Simulation (9 courses)
        '684b44b20401b17af2427d5d',  -- SWE Full Trace - Entry Level Tasks (2 courses)
        '68cb05ef58bc6f7919b6f099'   -- Data Analysis Agents - Rubrics (17 courses)
    )
      AND ac.ACTIVATION_DAY IS NOT NULL
      AND ac.ACTIVATION_DAY < CURRENT_DATE
      AND (u.WORKER_SOURCE != 'in_squads' OR u.WORKER_SOURCE IS NULL)
),
required_courses AS (
    SELECT OWNER_ID AS project_id, COUNT(DISTINCT COURSE_ID) AS n_required
    FROM VIEW.DIM_PROJECT_COURSES
    WHERE OWNER_ID IN (
        '69978f2116e00455a4fd2977','67ccdb9bedd9cfae8214ddfc',
        '683e729ecaf6d16374e6fc5b','69b09791e4d5a9e4419c35ab',
        '67ae58c1c9f428dcb51ca14e','684b44b20401b17af2427d5d',
        '68cb05ef58bc6f7919b6f099'
    )
    GROUP BY OWNER_ID
),
course_progress AS (
    SELECT
        dcp.USER_ID,
        dpc.OWNER_ID AS project_id,
        COUNT(DISTINCT dcp.COURSE_ID) AS n_courses_encountered,
        COUNT(DISTINCT CASE WHEN dcp.STATUS = 'pass'   THEN dcp.COURSE_ID END) AS n_passed,
        COUNT(DISTINCT CASE WHEN dcp.STATUS = 'failed' THEN dcp.COURSE_ID END) AS n_failed,
        COUNT(DISTINCT CASE WHEN dcp.STATUS = 'in_progress' THEN dcp.COURSE_ID END) AS n_in_progress
    FROM VIEW.DIM_COURSE_PROGRESSES dcp
    JOIN VIEW.DIM_PROJECT_COURSES dpc ON dcp.COURSE_ID = dpc.COURSE_ID
    WHERE dpc.OWNER_ID IN (
        '69978f2116e00455a4fd2977','67ccdb9bedd9cfae8214ddfc',
        '683e729ecaf6d16374e6fc5b','69b09791e4d5a9e4419c35ab',
        '67ae58c1c9f428dcb51ca14e','684b44b20401b17af2427d5d',
        '68cb05ef58bc6f7919b6f099'
    )
    GROUP BY dcp.USER_ID, dpc.OWNER_ID
),
channel_course AS (
    SELECT
        cc.channel_bucket,
        cc.project_id,
        cc.USER_ID,
        COALESCE(cp.n_courses_encountered, 0) AS n_encountered,
        COALESCE(cp.n_passed, 0)              AS n_passed,
        COALESCE(cp.n_failed, 0)              AS n_failed,
        COALESCE(cp.n_in_progress, 0)         AS n_in_progress,
        rc.n_required
    FROM channel_classified cc
    LEFT JOIN course_progress cp ON cc.USER_ID = cp.USER_ID AND cc.project_id = cp.project_id
    LEFT JOIN required_courses rc ON cc.project_id = rc.project_id
),
pooled AS (
    SELECT
        channel_bucket,
        COUNT(DISTINCT USER_ID)                                                           AS n_activated,
        COUNT(DISTINCT CASE WHEN n_encountered > 0 THEN USER_ID END)                     AS n_has_course_record,
        COUNT(DISTINCT CASE WHEN n_passed >= 1    THEN USER_ID END)                      AS n_passed_any,
        COUNT(DISTINCT CASE WHEN n_failed >= 1 AND n_passed = 0 THEN USER_ID END)        AS n_failed_only,
        COUNT(DISTINCT CASE WHEN n_failed >= 1    THEN USER_ID END)                      AS n_ever_failed,
        COUNT(DISTINCT CASE WHEN n_required IS NOT NULL
                             AND n_passed >= n_required THEN USER_ID END)                AS n_passed_all_required,
        SUM(n_passed)      AS total_course_passes,
        SUM(n_failed)      AS total_course_fails,
        SUM(n_encountered) AS total_courses_encountered
    FROM channel_course
    WHERE channel_bucket IN ('MWF','LATAM','Joveo','LinkedIn','Organic')
    GROUP BY channel_bucket
)
SELECT
    channel_bucket,
    n_activated,
    n_has_course_record,
    n_passed_any,
    n_failed_only,
    n_ever_failed,
    n_passed_all_required,
    total_course_passes,
    total_course_fails,
    total_courses_encountered,
    -- Coverage: what fraction of activated CBs even have course records
    ROUND(n_has_course_record::FLOAT / NULLIF(n_activated,0) * 100, 1)           AS pct_reached_course_stage,
    -- Conditional: among those with course records, % who passed at least one course
    ROUND(n_passed_any::FLOAT / NULLIF(n_has_course_record,0) * 100, 1)          AS cond_any_pass_rate,
    -- Conditional: among those with course records, % who ever failed at least one
    ROUND(n_ever_failed::FLOAT / NULLIF(n_has_course_record,0) * 100, 1)         AS cond_ever_failed_rate,
    -- Conditional: among those with course records, % who cleared ALL required courses
    ROUND(n_passed_all_required::FLOAT / NULLIF(n_has_course_record,0) * 100, 1) AS cond_all_pass_rate,
    -- Attempt-level: STATUS='pass' / total course encounters (per-attempt pass rate)
    ROUND(total_course_passes::FLOAT / NULLIF(total_courses_encountered,0) * 100, 1) AS avg_attempt_pass_rate
FROM pooled
ORDER BY n_activated DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- Per-project breakdown (for projects with DIM_PROJECT_COURSES entries)
-- Excludes RFP Master (no course config). Min n_activated >= 5 per cell.
-- ─────────────────────────────────────────────────────────────────────────────
WITH channel_classified AS (
    SELECT
        ac.USER_ID,
        ac.ACTIVATION_PROJECT_ID AS project_id,
        ac.ACTIVATION_DAY,
        CASE
            WHEN u.WORKER_SOURCE = 'inqa_coder'  THEN 'MWF'
            WHEN u.WORKER_SOURCE = 'latam_coder' THEN 'LATAM'
            WHEN LOWER(ac.UTM_SOURCE) LIKE '%joveo%'   THEN 'Joveo'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
            WHEN ac.UTM_SOURCE IS NULL
              OR LOWER(ac.UTM_SOURCE) IN ('organic','organic-social','organic-socials') THEN 'Organic'
            ELSE 'Other'
        END AS channel_bucket
    FROM VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN PUBLIC.USERS u ON ac.USER_ID = u._ID
    WHERE ac.ACTIVATION_PROJECT_ID IN (
        '69978f2116e00455a4fd2977','67ccdb9bedd9cfae8214ddfc',
        '683e729ecaf6d16374e6fc5b','69b09791e4d5a9e4419c35ab',
        '67ae58c1c9f428dcb51ca14e','684b44b20401b17af2427d5d',
        '68cb05ef58bc6f7919b6f099'
    )
      AND ac.ACTIVATION_DAY IS NOT NULL
      AND ac.ACTIVATION_DAY < CURRENT_DATE
      AND (u.WORKER_SOURCE != 'in_squads' OR u.WORKER_SOURCE IS NULL)
),
project_labels AS (
    SELECT * FROM VALUES
        ('69978f2116e00455a4fd2977', 'RFP Master',              0),
        ('67ccdb9bedd9cfae8214ddfc', 'SWEAP Augmentation',     10),
        ('683e729ecaf6d16374e6fc5b', 'Agent Completion Pt3',   30),
        ('69b09791e4d5a9e4419c35ab', 'Code Checkpoint Evals',  10),
        ('67ae58c1c9f428dcb51ca14e', 'Coding Physics Sim',      9),
        ('684b44b20401b17af2427d5d', 'SWE Full Trace Entry',    2),
        ('68cb05ef58bc6f7919b6f099', 'Data Analysis Agents',   17)
    AS t(project_id, project_name, n_required)
),
course_progress AS (
    SELECT
        dcp.USER_ID,
        dpc.OWNER_ID AS project_id,
        COUNT(DISTINCT CASE WHEN dcp.STATUS = 'pass'   THEN dcp.COURSE_ID END) AS n_passed,
        COUNT(DISTINCT CASE WHEN dcp.STATUS = 'failed' THEN dcp.COURSE_ID END) AS n_failed,
        COUNT(DISTINCT dcp.COURSE_ID)                                           AS n_encountered
    FROM VIEW.DIM_COURSE_PROGRESSES dcp
    JOIN VIEW.DIM_PROJECT_COURSES dpc ON dcp.COURSE_ID = dpc.COURSE_ID
    WHERE dpc.OWNER_ID IN (
        '69978f2116e00455a4fd2977','67ccdb9bedd9cfae8214ddfc',
        '683e729ecaf6d16374e6fc5b','69b09791e4d5a9e4419c35ab',
        '67ae58c1c9f428dcb51ca14e','684b44b20401b17af2427d5d',
        '68cb05ef58bc6f7919b6f099'
    )
    GROUP BY dcp.USER_ID, dpc.OWNER_ID
),
channel_course AS (
    SELECT
        cc.channel_bucket,
        cc.project_id,
        pl.project_name,
        pl.n_required,
        cc.USER_ID,
        COALESCE(cp.n_encountered, 0) AS n_encountered,
        COALESCE(cp.n_passed, 0)      AS n_passed,
        COALESCE(cp.n_failed, 0)      AS n_failed
    FROM channel_classified cc
    JOIN project_labels pl ON cc.project_id = pl.project_id
    LEFT JOIN course_progress cp ON cc.USER_ID = cp.USER_ID AND cc.project_id = cp.project_id
)
SELECT
    project_name,
    n_required,
    channel_bucket,
    COUNT(DISTINCT USER_ID)                                                          AS n_activated,
    COUNT(DISTINCT CASE WHEN n_encountered > 0 THEN USER_ID END)                    AS n_has_course_record,
    COUNT(DISTINCT CASE WHEN n_passed >= 1 THEN USER_ID END)                        AS n_passed_any,
    COUNT(DISTINCT CASE WHEN n_required > 0 AND n_passed >= n_required
                         THEN USER_ID END)                                           AS n_passed_all,
    ROUND(COUNT(DISTINCT CASE WHEN n_encountered > 0 THEN USER_ID END)::FLOAT
          / NULLIF(COUNT(DISTINCT USER_ID),0) * 100, 1)                             AS pct_enrolled,
    ROUND(COUNT(DISTINCT CASE WHEN n_passed >= 1 THEN USER_ID END)::FLOAT
          / NULLIF(COUNT(DISTINCT CASE WHEN n_encountered > 0 THEN USER_ID END),0) * 100, 1)
                                                                                     AS cond_any_pass_pct,
    ROUND(COUNT(DISTINCT CASE WHEN n_required > 0 AND n_passed >= n_required
                               THEN USER_ID END)::FLOAT
          / NULLIF(COUNT(DISTINCT CASE WHEN n_encountered > 0 THEN USER_ID END),0) * 100, 1)
                                                                                     AS cond_all_pass_pct
FROM channel_course
WHERE channel_bucket IN ('MWF','LATAM','LinkedIn','Organic')
GROUP BY project_name, n_required, channel_bucket
HAVING n_activated >= 5
ORDER BY project_name, n_activated DESC;

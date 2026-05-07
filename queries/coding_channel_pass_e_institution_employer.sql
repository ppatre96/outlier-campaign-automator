-- Pass E: Institution tier + employer tier deep-dive
-- Same cohort as Pass D: MWF+LATAM ("program") vs LinkedIn activated CBs on 7 coding projects
-- Run date: 2026-04-23
--
-- NOTE: TNS_WORKER_LINKEDIN.LINKEDIN_EDUCATION is entirely null for this cohort (0% coverage).
-- Institution data sourced from RESUMEMETADATAS.EDUCATIONS JSON array only.
-- Employer data from WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY.
-- Data source: _Snowflake (GenAI Ops), DS ID 30
-- Redash query IDs: 303182 (initial LinkedIn attempt), 303186 (RESUMEMETADATAS fallback)

-- ── Cohort definition (identical to Pass D) ────────────────────────────────
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
),

-- ── Latest resume per contributor (SCD dedup) ──────────────────────────────
resume_meta AS (
    SELECT *
    FROM (
        SELECT rm.*,
               ROW_NUMBER() OVER (
                   PARTITION BY rm.CONTRIBUTOR_ID
                   ORDER BY COALESCE(rm.UPDATED_AT, rm.CREATED_AT) DESC, rm._id DESC
               ) AS rn
        FROM PUBLIC.RESUMEMETADATAS rm
    ) WHERE rn = 1
),

-- ── Flatten education entries from RESUMEMETADATAS ─────────────────────────
-- EDUCATIONS is a JSON array. Key names vary by source system.
-- We try all known variants: school/schoolName/institution.
-- We pick the bachelor's-level entry first; fall back to earliest grad year.
edu_flat AS (
    SELECT
        f.USER_ID,
        f.bucket,
        f.worker_source,
        COALESCE(
            edu.value:school::STRING,
            edu.value:schoolName::STRING,
            edu.value:institution::STRING
        ) AS school_name,
        COALESCE(
            edu.value:degree::STRING,
            edu.value:degreeName::STRING,
            edu.value:degreeTitle::STRING
        ) AS degree_name,
        COALESCE(
            edu.value:fieldOfStudy::STRING,
            edu.value:field::STRING,
            edu.value:major::STRING
        ) AS field_of_study,
        TRY_CAST(
            COALESCE(
                edu.value:endYear::STRING,
                edu.value:yearOfGraduation::STRING,
                edu.value:year::STRING
            ) AS INTEGER
        ) AS grad_year,
        -- Bachelor's degree flag: covers English, Spanish, Portuguese degree names
        CASE
            WHEN LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%bachelor%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%b.tech%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%b.e.%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%b.sc%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%licenciat%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%engenharia%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%ingenier%'
              OR LOWER(COALESCE(edu.value:degree::STRING, edu.value:degreeName::STRING, '')) LIKE '%undergraduate%'
            THEN 1 ELSE 0
        END AS is_bachelors_flag
    FROM filtered f
    JOIN resume_meta rm ON f.USER_ID = rm.CONTRIBUTOR_ID::STRING
    , LATERAL FLATTEN(input => rm.EDUCATIONS, outer => TRUE) edu
    WHERE rm.EDUCATIONS IS NOT NULL
),

-- ── Pick best undergraduate entry per user ─────────────────────────────────
-- Priority 1: explicit bachelor's flag. Priority 2: earliest grad year (proxy).
edu_ranked AS (
    SELECT
        USER_ID,
        bucket,
        worker_source,
        school_name,
        degree_name,
        field_of_study,
        grad_year,
        is_bachelors_flag,
        ROW_NUMBER() OVER (
            PARTITION BY USER_ID
            ORDER BY is_bachelors_flag DESC,
                     grad_year ASC NULLS LAST
        ) AS rn
    FROM edu_flat
    WHERE school_name IS NOT NULL
),
edu_best AS (
    SELECT * FROM edu_ranked WHERE rn = 1
)

-- ── Final output: one row per CB ───────────────────────────────────────────
SELECT
    f.USER_ID,
    f.bucket,
    f.worker_source,
    eb.school_name,
    eb.degree_name,
    eb.field_of_study,
    eb.grad_year,
    eb.is_bachelors_flag,
    r.RESUME_JOB_COMPANY AS employer_raw,
    r.RESUME_JOB_TITLE   AS job_title_raw,
    r.RESUME_JOB_SKILLS  AS skills_raw
FROM filtered f
LEFT JOIN edu_best eb      ON f.USER_ID = eb.USER_ID
LEFT JOIN SCALE_PROD.VIEW.WORKER_RESUME_SUMMARY r ON f.USER_ID = r.USER_ID
ORDER BY f.bucket, f.USER_ID;

-- ── Tier assignment logic (applied in Python post-processing) ──────────────
-- Institution tiers:
--   India T1: IITs (any), IISc, IIIT-H/D/B, BITS Pilani/Goa/Hyd
--   India T2: NITs (any), other IIITs, DTU, NSIT, COEP, VJTI, PSG, Thapar,
--             VIT Vellore, SRM, Manipal, Anna University
--   India T3: all other Indian engineering colleges
--   LATAM T1: UNAM, USP, UNICAMP, ITA, U de los Andes, U Nacional Colombia,
--             UBA, UFRGS, Instituto Balseiro, ITAM, Tec de Monterrey
--   LATAM T2: federal/state universities, known private engineering schools
--   LATAM T3: all other LATAM institutions
--   Other: non-India, non-LATAM (Telkom, US/CA/UK/AU/EU universities)
--   Unknown: no education data
--
-- Employer tiers:
--   A: FAANG, top finance (Goldman, JPMorgan), top-pay unicorns (Flipkart, Razorpay etc.)
--   B: Series B+ startups, MNC captive centres, Globant, Thoughtworks, Stefanini etc.
--   C: IT services (Infosys, TCS, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini etc.)
--   D: small/unknown/freelance/student/Outlier/Scale AI (AI labeling work itself)

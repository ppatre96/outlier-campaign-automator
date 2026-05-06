-- Pass F: India-aware employer tier re-analysis + full employment history extraction
-- Same cohort as Pass E (MWF+LATAM "program" vs LinkedIn activated CBs on 7 coding projects)
-- Run date: 2026-04-23
--
-- Purpose:
--   1. Extract ALL past employers from RESUMEMETADATAS.JOB_EXPERIENCES (full array, not just current)
--      to answer: "did this CB ever work at a Tier A or B company before going freelance?"
--   2. Apply India-aware tier mapping (replaces v1 US-centric tiers in Pass E §7.2)
--   3. Split MWF vs LATAM separately within the program bucket
--
-- Tier definitions (India-aware, applied in-SQL via CASE/ILIKE):
--   Tier A — top pay in India (₹30L+ entry / ₹50L+ senior):
--     Quant/trading: Jane Street, HRT, Graviton, Quadeye, DE Shaw, Two Sigma,
--                    Citadel, Optiver, Tower Research, AQR, Millennium
--     Top product unicorns: Razorpay, Cred, Zerodha, PhonePe, Flipkart, Groww,
--                           Rippling, Stripe, Uber, Nvidia, OpenAI, Anthropic, Scale AI (product role only)
--     US tech India offices: Google, Microsoft, Meta, Amazon, Adobe (senior), Salesforce,
--                            Atlassian, Cloudflare, Databricks, Snowflake, Airbnb, Roblox, LinkedIn corp
--   Tier B — mid-high pay (₹15–30L INR):
--     Mid-size product: Swiggy, Zomato, Paytm, Ola, Oyo, BYJU'S, Meesho, Unacademy,
--                       Dream11, Nykaa, Postman, Chargebee, Freshworks, Zoho, Airtel Digital, Jio
--     Captive centres: Goldman Sachs India, JPMorgan India, Morgan Stanley India,
--                      Wells Fargo India, BofA India, Deutsche Bank India, BNY Mellon, BlackRock India
--     Premium IT consulting: ThoughtWorks, Nagarro, GlobalLogic, EPAM, Publicis Sapient,
--                             Hashedin, Tiger Analytics, Mu Sigma, Sigmoid, Tredence, Globant
--   Tier C — IT services (₹3–12L INR):
--     TCS, Infosys, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini, Accenture,
--     IBM (India services), Mindtree, L&T Infotech, Mphasis, Persistent, Hexaware,
--     Birlasoft, Coforge, CGI, DXC, NTT Data, Zensar, Stefanini (LATAM IT services proxy)
--   Tier D — small / unknown / self-employed / student / freelance / research:
--     Everyone else; also Outlier / Scale AI listed as current employer (AI labeling gig)
--
-- LATAM note: unrecognized LATAM companies default to Tier D.
--   Stefanini treated as Tier C (LATAM's closest equivalent to Indian IT services).
--   Globant treated as Tier B (premium IT consulting, well-funded).
--
-- Data source: _Snowflake (GenAI Ops), DS ID 30
-- Prior query: coding_channel_pass_e_institution_employer.sql

-- ── Cohort definition ───────────────────────────────────────────────────────
WITH coding_activated AS (
    SELECT
        ac.USER_ID,
        CASE
            WHEN u.worker_source = 'inqa_coder'  THEN 'MWF'
            WHEN u.worker_source = 'latam_coder' THEN 'LATAM'
            WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
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

-- ── Latest resume per contributor (SCD dedup) ───────────────────────────────
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

-- ── Flatten ALL past employment entries from JOB_EXPERIENCES ────────────────
-- JOB_EXPERIENCES is a JSON array — one row per job entry per CB.
-- Each entry has: jobTitle, companyName (or company), startDate, endDate,
-- yearsOfExperience (sometimes). We extract all entries, not just current.
-- This is the key improvement over Pass E (which used RESUME_JOB_COMPANY = last job only).
all_jobs AS (
    SELECT
        f.USER_ID,
        f.bucket,
        f.worker_source,
        COALESCE(
            j.value:companyName::STRING,
            j.value:company::STRING,
            j.value:employer::STRING,
            j.value:organization::STRING
        ) AS company_raw,
        j.value:jobTitle::STRING AS job_title,
        TRY_CAST(
            COALESCE(j.value:startDate::STRING, j.value:startYear::STRING) AS VARCHAR
        ) AS start_date_raw,
        TRY_CAST(
            COALESCE(j.value:endDate::STRING, j.value:endYear::STRING) AS VARCHAR
        ) AS end_date_raw,
        -- Detect if this appears to be the most recent job
        -- (will order by index or date below)
        j.index AS job_array_index
    FROM filtered f
    JOIN resume_meta rm ON f.USER_ID = rm.CONTRIBUTOR_ID::STRING
    , LATERAL FLATTEN(input => rm.JOB_EXPERIENCES, outer => TRUE) j
    WHERE rm.JOB_EXPERIENCES IS NOT NULL
      AND COALESCE(
              j.value:companyName::STRING,
              j.value:company::STRING,
              j.value:employer::STRING,
              j.value:organization::STRING
          ) IS NOT NULL
),

-- ── India-aware tier assignment (applied per company row) ───────────────────
jobs_tiered AS (
    SELECT
        USER_ID,
        bucket,
        worker_source,
        company_raw,
        job_title,
        start_date_raw,
        end_date_raw,
        job_array_index,
        -- Identify if this is the most-recent job (index 0 = most recent in most resume JSON schemas)
        CASE WHEN job_array_index = 0 THEN TRUE ELSE FALSE END AS is_current_job,

        CASE
            -- Tier A: quant / trading
            WHEN LOWER(company_raw) ILIKE '%jane street%'
              OR LOWER(company_raw) ILIKE '%hudson river%' OR LOWER(company_raw) ILIKE '% hrt%'
              OR LOWER(company_raw) ILIKE '%graviton%'
              OR LOWER(company_raw) ILIKE '%quadeye%'
              OR LOWER(company_raw) ILIKE '%de shaw%' OR LOWER(company_raw) ILIKE '%d.e. shaw%'
              OR LOWER(company_raw) ILIKE '%two sigma%'
              OR LOWER(company_raw) ILIKE '%citadel%'
              OR LOWER(company_raw) ILIKE '%optiver%'
              OR LOWER(company_raw) ILIKE '%tower research%'
              OR LOWER(company_raw) ILIKE '% aqr%'
              OR LOWER(company_raw) ILIKE '%millennium%'
            THEN 'A'
            -- Tier A: top product unicorns + US tech India offices
            WHEN LOWER(company_raw) ILIKE '%razorpay%'
              OR LOWER(company_raw) ILIKE '%cred%'
              OR LOWER(company_raw) ILIKE '%zerodha%'
              OR LOWER(company_raw) ILIKE '%phonepe%'
              OR LOWER(company_raw) ILIKE '%flipkart%'
              OR LOWER(company_raw) ILIKE '%groww%'
              OR LOWER(company_raw) ILIKE '%rippling%'
              OR LOWER(company_raw) ILIKE '%uber%'
              OR LOWER(company_raw) ILIKE '%nvidia%'
              OR LOWER(company_raw) ILIKE '%openai%'
              OR LOWER(company_raw) ILIKE '%anthropic%'
              OR (LOWER(company_raw) ILIKE '%scale ai%' AND LOWER(job_title) NOT ILIKE '%outlier%' AND LOWER(job_title) NOT ILIKE '%annotator%' AND LOWER(job_title) NOT ILIKE '%labeler%')
              OR LOWER(company_raw) ILIKE '%google%'
              OR LOWER(company_raw) ILIKE '%microsoft%'
              OR (LOWER(company_raw) ILIKE '%amazon%' AND LOWER(company_raw) NOT ILIKE '%accenture%')
              OR LOWER(company_raw) ILIKE '%meta %' OR LOWER(company_raw) = 'meta'
              OR LOWER(company_raw) ILIKE '%facebook%'
              OR LOWER(company_raw) ILIKE '%atlassian%'
              OR LOWER(company_raw) ILIKE '%cloudflare%'
              OR LOWER(company_raw) ILIKE '%databricks%'
              OR LOWER(company_raw) ILIKE '%snowflake%'
              OR LOWER(company_raw) ILIKE '%airbnb%'
              OR LOWER(company_raw) ILIKE '%roblox%'
              OR LOWER(company_raw) ILIKE '%stripe%'
              OR LOWER(company_raw) ILIKE '%samsung%'
            THEN 'A'

            -- Tier B: captive centres (foreign bank / firm India tech arms)
            WHEN LOWER(company_raw) ILIKE '%goldman sachs%'
              OR LOWER(company_raw) ILIKE '%jpmorgan%' OR LOWER(company_raw) ILIKE '%jp morgan%'
              OR LOWER(company_raw) ILIKE '%morgan stanley%'
              OR LOWER(company_raw) ILIKE '%wells fargo%'
              OR LOWER(company_raw) ILIKE '%bank of america%'
              OR LOWER(company_raw) ILIKE '%deutsche bank%'
              OR LOWER(company_raw) ILIKE '%bny mellon%'
              OR LOWER(company_raw) ILIKE '%state street%'
              OR LOWER(company_raw) ILIKE '%fidelity%'
              OR LOWER(company_raw) ILIKE '%blackrock%'
            THEN 'B'
            -- Tier B: mid-size Indian product companies
            WHEN LOWER(company_raw) ILIKE '%swiggy%'
              OR LOWER(company_raw) ILIKE '%zomato%'
              OR LOWER(company_raw) ILIKE '%paytm%'
              OR LOWER(company_raw) ILIKE '% ola%' OR LOWER(company_raw) = 'ola'
              OR LOWER(company_raw) ILIKE '%oyo%'
              OR LOWER(company_raw) ILIKE '%byju%'
              OR LOWER(company_raw) ILIKE '%meesho%'
              OR LOWER(company_raw) ILIKE '%unacademy%'
              OR LOWER(company_raw) ILIKE '%dream11%'
              OR LOWER(company_raw) ILIKE '%nykaa%'
              OR LOWER(company_raw) ILIKE '%postman%'
              OR LOWER(company_raw) ILIKE '%chargebee%'
              OR LOWER(company_raw) ILIKE '%freshworks%'
              OR LOWER(company_raw) ILIKE '%zoho%'
              OR LOWER(company_raw) ILIKE '%jio%'
              OR LOWER(company_raw) ILIKE '%airtel%'
            THEN 'B'
            -- Tier B: premium IT consulting / staffing (not commodity services)
            WHEN LOWER(company_raw) ILIKE '%thoughtworks%'
              OR LOWER(company_raw) ILIKE '%nagarro%'
              OR LOWER(company_raw) ILIKE '%globallogic%'
              OR LOWER(company_raw) ILIKE '%epam%'
              OR LOWER(company_raw) ILIKE '%publicis sapient%'
              OR LOWER(company_raw) ILIKE '%hashedin%'
              OR LOWER(company_raw) ILIKE '%tiger analytics%'
              OR LOWER(company_raw) ILIKE '%mu sigma%'
              OR LOWER(company_raw) ILIKE '%sigmoid%'
              OR LOWER(company_raw) ILIKE '%tredence%'
              OR LOWER(company_raw) ILIKE '%globant%'
              OR LOWER(company_raw) ILIKE '%anyone ai%'
            THEN 'B'

            -- Tier C: IT services (commodity)
            WHEN LOWER(company_raw) ILIKE '%infosys%'
              OR LOWER(company_raw) ILIKE '%tata consultancy%' OR LOWER(company_raw) ILIKE '% tcs%' OR LOWER(company_raw) = 'tcs'
              OR LOWER(company_raw) ILIKE '%wipro%'
              OR LOWER(company_raw) ILIKE '%hcl%'
              OR LOWER(company_raw) ILIKE '%tech mahindra%'
              OR LOWER(company_raw) ILIKE '%cognizant%'
              OR LOWER(company_raw) ILIKE '%capgemini%'
              OR LOWER(company_raw) ILIKE '%accenture%'
              OR LOWER(company_raw) ILIKE '% ibm%' OR LOWER(company_raw) = 'ibm'
              OR LOWER(company_raw) ILIKE '%mindtree%'
              OR LOWER(company_raw) ILIKE '%mphasis%'
              OR LOWER(company_raw) ILIKE '%persistent%'
              OR LOWER(company_raw) ILIKE '%hexaware%'
              OR LOWER(company_raw) ILIKE '%birlasoft%'
              OR LOWER(company_raw) ILIKE '%coforge%'
              OR LOWER(company_raw) ILIKE '%zensar%'
              OR LOWER(company_raw) ILIKE '%dxc%'
              OR LOWER(company_raw) ILIKE '%ntt data%'
              OR LOWER(company_raw) ILIKE '%stefanini%'
              OR LOWER(company_raw) ILIKE '%l&t infotech%' OR LOWER(company_raw) ILIKE '%larsen%'
            THEN 'C'

            -- Tier D: AI labeling platforms (Outlier / Scale AI as gig, not product role)
            WHEN LOWER(company_raw) ILIKE '%outlier%'
              OR (LOWER(company_raw) ILIKE '%scale ai%' AND (
                  LOWER(job_title) ILIKE '%annotator%' OR
                  LOWER(job_title) ILIKE '%labeler%' OR
                  LOWER(job_title) ILIKE '%rater%' OR
                  LOWER(job_title) ILIKE '%tasker%' OR
                  LOWER(job_title) IS NULL
              ))
            THEN 'D'

            -- Tier D: everything else
            ELSE 'D'
        END AS india_tier
    FROM all_jobs
),

-- ── Best tier per CB across ALL historical jobs ─────────────────────────────
-- Rank: A > B > C > D. Report the best tier a CB has ever reached.
best_tier_ever AS (
    SELECT
        USER_ID,
        bucket,
        worker_source,
        -- Best tier ever across career history
        MAX(CASE india_tier WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 ELSE 1 END) AS best_tier_score,
        CASE MAX(CASE india_tier WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 ELSE 1 END)
            WHEN 4 THEN 'A' WHEN 3 THEN 'B' WHEN 2 THEN 'C' ELSE 'D'
        END AS best_tier_ever,
        -- Current employer tier (job_array_index = 0)
        MAX(CASE WHEN is_current_job THEN
            CASE india_tier WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 ELSE 1 END
        ELSE NULL END) AS current_tier_score,
        CASE MAX(CASE WHEN is_current_job THEN
            CASE india_tier WHEN 'A' THEN 4 WHEN 'B' THEN 3 WHEN 'C' THEN 2 ELSE 1 END
        ELSE NULL END)
            WHEN 4 THEN 'A' WHEN 3 THEN 'B' WHEN 2 THEN 'C'
            WHEN 1 THEN 'D' ELSE 'D'
        END AS current_tier,
        -- Flag: ever worked at Tier A or B (regardless of current employer)
        MAX(CASE WHEN india_tier IN ('A','B') THEN 1 ELSE 0 END) AS ever_tier_ab,
        -- Count of distinct companies per tier ever
        COUNT(DISTINCT CASE WHEN india_tier = 'A' THEN company_raw END) AS n_tier_a_companies,
        COUNT(DISTINCT CASE WHEN india_tier = 'B' THEN company_raw END) AS n_tier_b_companies,
        COUNT(DISTINCT CASE WHEN india_tier = 'C' THEN company_raw END) AS n_tier_c_companies,
        COUNT(DISTINCT company_raw) AS n_total_companies
    FROM jobs_tiered
    GROUP BY USER_ID, bucket, worker_source
),

-- ── CBs with no job history in resume ──────────────────────────────────────
no_job_history AS (
    SELECT f.USER_ID, f.bucket, f.worker_source
    FROM filtered f
    WHERE NOT EXISTS (
        SELECT 1 FROM all_jobs aj WHERE aj.USER_ID = f.USER_ID
    )
),

-- ── Union: tiered CBs + CBs with no job data ───────────────────────────────
all_tiered AS (
    SELECT USER_ID, bucket, worker_source,
           best_tier_ever, current_tier, ever_tier_ab,
           n_tier_a_companies, n_tier_b_companies, n_tier_c_companies, n_total_companies
    FROM best_tier_ever

    UNION ALL

    SELECT USER_ID, bucket, worker_source,
           'D' AS best_tier_ever, 'D' AS current_tier, 0 AS ever_tier_ab,
           0, 0, 0, 0
    FROM no_job_history
)

-- ── Aggregate summary: tier distribution per bucket ─────────────────────────
-- Output 1: distribution table (current employer tier + best-ever tier)
SELECT
    bucket,
    COUNT(*) AS n_cbs,

    -- Current employer tier distribution
    SUM(CASE WHEN current_tier = 'A' THEN 1 ELSE 0 END) AS current_tier_a,
    SUM(CASE WHEN current_tier = 'B' THEN 1 ELSE 0 END) AS current_tier_b,
    SUM(CASE WHEN current_tier = 'C' THEN 1 ELSE 0 END) AS current_tier_c,
    SUM(CASE WHEN current_tier = 'D' THEN 1 ELSE 0 END) AS current_tier_d,

    -- Best-ever tier distribution (key metric: IIT grad who went independent)
    SUM(CASE WHEN best_tier_ever = 'A' THEN 1 ELSE 0 END) AS ever_tier_a,
    SUM(CASE WHEN best_tier_ever = 'B' THEN 1 ELSE 0 END) AS ever_tier_b,
    SUM(CASE WHEN best_tier_ever = 'C' THEN 1 ELSE 0 END) AS ever_tier_c,
    SUM(CASE WHEN best_tier_ever = 'D' THEN 1 ELSE 0 END) AS ever_tier_d,

    -- Ever-worked-at-A-or-B rate (the key hypothesis check)
    SUM(ever_tier_ab) AS n_ever_tier_ab,
    ROUND(100.0 * SUM(ever_tier_ab) / COUNT(*), 1) AS pct_ever_tier_ab

FROM all_tiered
GROUP BY bucket
ORDER BY CASE bucket WHEN 'MWF' THEN 1 WHEN 'LATAM' THEN 2 WHEN 'LinkedIn' THEN 3 ELSE 4 END;

-- ── Supplemental: raw employer list with India tiers, per bucket ─────────────
-- Run separately as a second query for sanity-checking tier assignments
-- SELECT bucket, company_raw, india_tier, COUNT(*) AS n
-- FROM jobs_tiered
-- WHERE is_current_job = TRUE
-- GROUP BY bucket, company_raw, india_tier
-- ORDER BY bucket, n DESC
-- LIMIT 100;

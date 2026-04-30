"""
Redash API client — drop-in replacement for SnowflakeClient.

Uses the Redash 3-step polling pattern:
  1. POST /api/queries          → create ad-hoc query
  2. POST /api/queries/{id}/results → trigger execution → job
  3. Poll GET /api/jobs/{job_id} until status=3 (done)
  4. GET /api/query_results/{qrid} → fetch rows

All public methods return pandas DataFrames with lowercase column names,
matching the interface of SnowflakeClient exactly.
"""
import logging
import time
from typing import Any

import pandas as pd
import requests

import config

log = logging.getLogger(__name__)

_POLL_INTERVAL = 4   # seconds between job status polls
_MAX_POLLS     = 60  # ~4 minutes max wait


# ── SQL definitions (same queries as snowflake_db.py) ─────────────────────────

RESUME_SQL = """
WITH resume_meta AS (
  SELECT *
  FROM (
    SELECT
      rm.*,
      ROW_NUMBER() OVER (
        PARTITION BY rm.CONTRIBUTOR_ID
        ORDER BY COALESCE(rm.UPDATED_AT, rm.CREATED_AT) DESC, rm._id DESC
      ) AS rn
    FROM PUBLIC.RESUMEMETADATAS rm
  )
  WHERE rn = 1
),
job_titles_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    LISTAGG(DISTINCT LOWER(TRIM(j.value:jobTitle::STRING)), '; ')
      WITHIN GROUP (ORDER BY LOWER(TRIM(j.value:jobTitle::STRING))) AS job_titles
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.JOB_EXPERIENCES) j
  WHERE j.value:jobTitle IS NOT NULL
  GROUP BY rm.CONTRIBUTOR_ID
),
fields_of_study_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    LISTAGG(DISTINCT LOWER(TRIM(e.value:fieldOfStudy::STRING)), '; ')
      WITHIN GROUP (ORDER BY LOWER(TRIM(e.value:fieldOfStudy::STRING))) AS fields_of_study
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.EDUCATIONS) e
  WHERE e.value:fieldOfStudy IS NOT NULL
  GROUP BY rm.CONTRIBUTOR_ID
),
skills_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    LISTAGG(DISTINCT LOWER(TRIM(s.value::STRING)), '; ')
      WITHIN GROUP (ORDER BY LOWER(TRIM(s.value::STRING))) AS skills_str
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.JOB_EXPERIENCES) j,
  LATERAL FLATTEN(input => j.value:skills) s
  WHERE s.value IS NOT NULL
    AND TRIM(s.value::STRING) != ''
  GROUP BY rm.CONTRIBUTOR_ID
),
experience_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    COALESCE(SUM(
      CASE
        WHEN j.value:yearsOfExperience IS NOT NULL
          THEN TRY_TO_DOUBLE(j.value:yearsOfExperience::STRING)
        WHEN j.value:startYear IS NOT NULL
          THEN GREATEST(0, COALESCE(TRY_TO_NUMBER(j.value:endYear::STRING), YEAR(CURRENT_DATE())) - TRY_TO_NUMBER(j.value:startYear::STRING))
        ELSE 0
      END
    ), 0) AS total_years_experience,
    COUNT(*) AS role_count,
    CASE
      WHEN COALESCE(SUM(
        CASE
          WHEN j.value:yearsOfExperience IS NOT NULL THEN TRY_TO_DOUBLE(j.value:yearsOfExperience::STRING)
          WHEN j.value:startYear IS NOT NULL THEN GREATEST(0, COALESCE(TRY_TO_NUMBER(j.value:endYear::STRING), YEAR(CURRENT_DATE())) - TRY_TO_NUMBER(j.value:startYear::STRING))
          ELSE 0
        END
      ), 0) <= 1  THEN '0-1'
      WHEN COALESCE(SUM(
        CASE
          WHEN j.value:yearsOfExperience IS NOT NULL THEN TRY_TO_DOUBLE(j.value:yearsOfExperience::STRING)
          WHEN j.value:startYear IS NOT NULL THEN GREATEST(0, COALESCE(TRY_TO_NUMBER(j.value:endYear::STRING), YEAR(CURRENT_DATE())) - TRY_TO_NUMBER(j.value:startYear::STRING))
          ELSE 0
        END
      ), 0) <= 4  THEN '2-4'
      WHEN COALESCE(SUM(
        CASE
          WHEN j.value:yearsOfExperience IS NOT NULL THEN TRY_TO_DOUBLE(j.value:yearsOfExperience::STRING)
          WHEN j.value:startYear IS NOT NULL THEN GREATEST(0, COALESCE(TRY_TO_NUMBER(j.value:endYear::STRING), YEAR(CURRENT_DATE())) - TRY_TO_NUMBER(j.value:startYear::STRING))
          ELSE 0
        END
      ), 0) <= 7  THEN '5-7'
      WHEN COALESCE(SUM(
        CASE
          WHEN j.value:yearsOfExperience IS NOT NULL THEN TRY_TO_DOUBLE(j.value:yearsOfExperience::STRING)
          WHEN j.value:startYear IS NOT NULL THEN GREATEST(0, COALESCE(TRY_TO_NUMBER(j.value:endYear::STRING), YEAR(CURRENT_DATE())) - TRY_TO_NUMBER(j.value:startYear::STRING))
          ELSE 0
        END
      ), 0) <= 10 THEN '8-10'
      ELSE '10+'
    END AS experience_band
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.JOB_EXPERIENCES) j
  GROUP BY rm.CONTRIBUTOR_ID
),
degree_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    MAX(CASE
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%phd%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%doctorat%' THEN 4
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%master%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%mba%' THEN 3
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%bachelor%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%undergraduate%' THEN 2
      ELSE 1
    END) AS degree_rank,
    CASE MAX(CASE
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%phd%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%doctorat%' THEN 4
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%master%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%mba%' THEN 3
      WHEN LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%bachelor%'
        OR LOWER(COALESCE(e.value:degree::STRING, e.value:degreeName::STRING, '')) LIKE '%undergraduate%' THEN 2
      ELSE 1
    END)
      WHEN 4 THEN 'Phd'
      WHEN 3 THEN 'Masters'
      WHEN 2 THEN 'Bachelors'
      ELSE 'Other'
    END AS highest_degree_level
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.EDUCATIONS) e
  GROUP BY rm.CONTRIBUTOR_ID
),
country_agg AS (
  SELECT CONTRIBUTOR_ID, country FROM (
    SELECT rm.CONTRIBUTOR_ID,
           TRIM(e.value:country::STRING) AS country,
           ROW_NUMBER() OVER (PARTITION BY rm.CONTRIBUTOR_ID ORDER BY e.index) AS rn
    FROM resume_meta rm,
    LATERAL FLATTEN(input => rm.EDUCATIONS) e
    WHERE TRIM(e.value:country::STRING) IS NOT NULL AND TRIM(e.value:country::STRING) != ''
  ) WHERE rn = 1
  UNION ALL
  SELECT CONTRIBUTOR_ID, country FROM (
    SELECT rm.CONTRIBUTOR_ID,
           COALESCE(
             TRIM(j.value:companyLocation::STRING),
             TRIM(j.value:location::STRING)
           ) AS country,
           ROW_NUMBER() OVER (PARTITION BY rm.CONTRIBUTOR_ID ORDER BY j.index) AS rn
    FROM resume_meta rm,
    LATERAL FLATTEN(input => rm.JOB_EXPERIENCES) j
    WHERE COALESCE(TRIM(j.value:companyLocation::STRING), TRIM(j.value:location::STRING)) IS NOT NULL
      AND COALESCE(TRIM(j.value:companyLocation::STRING), TRIM(j.value:location::STRING)) != ''
  ) WHERE rn = 1
),
accreditations_agg AS (
  SELECT
    rm.CONTRIBUTOR_ID,
    LISTAGG(DISTINCT LOWER(TRIM(COALESCE(a.value:name::STRING, a.value:title::STRING, ''))), '; ')
      WITHIN GROUP (ORDER BY LOWER(TRIM(COALESCE(a.value:name::STRING, a.value:title::STRING, '')))) AS accreditations_str
  FROM resume_meta rm,
  LATERAL FLATTEN(input => rm.ACCREDITATIONS) a
  WHERE COALESCE(TRIM(a.value:name::STRING), TRIM(a.value:title::STRING)) IS NOT NULL
    AND COALESCE(TRIM(a.value:name::STRING), TRIM(a.value:title::STRING)) != ''
  GROUP BY rm.CONTRIBUTOR_ID
),
ScreeningData AS (
  SELECT
    g.CANDIDATE_EMAIL,
    g.RESUME_SCREENING_CONFIG_ID,
    g.RESULT AS SCREENING_RESULT,
    g.CREATED_AT AS SCREENING_DATE,
    r.NAME AS CONFIG_NAME,
    r.POD_TYPE
  FROM PUBLIC.GROWTHRESUMESCREENINGRESULTS g
  JOIN PUBLIC.RESUMESCREENINGCONFIGS r
    ON g.RESUME_SCREENING_CONFIG_ID = r._ID
  WHERE g.CREATED_AT >= '{start_date}'
    AND g.CREATED_AT <= '{end_date}'
)
SELECT
  ac.SIGNUP_FLOW_ID,
  ac.SIGNUP_FLOW_NAME,
  ac.UTM_SOURCE,
  ac.USER_ID,
  ac.EMAIL,
  sd.CONFIG_NAME AS resume_screening_config_name,
  sd.POD_TYPE,
  sd.SCREENING_RESULT AS resume_screening_result,
  sd.SCREENING_DATE AS resume_screening_date,
  jt.job_titles,
  fos.fields_of_study,
  sa.skills_str,
  ea.experience_band,
  ea.total_years_experience,
  ea.role_count,
  da.highest_degree_level,
  COALESCE(ca.country, 'UNKNOWN') AS country,
  acr.accreditations_str
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN ScreeningData sd
  ON ac.EMAIL = sd.CANDIDATE_EMAIL
LEFT JOIN resume_meta rm
  ON ac.USER_ID = rm.CONTRIBUTOR_ID::STRING
LEFT JOIN job_titles_agg jt
  ON ac.USER_ID = jt.CONTRIBUTOR_ID::STRING
LEFT JOIN fields_of_study_agg fos
  ON ac.USER_ID = fos.CONTRIBUTOR_ID::STRING
LEFT JOIN skills_agg sa
  ON ac.USER_ID = sa.CONTRIBUTOR_ID::STRING
LEFT JOIN experience_agg ea
  ON ac.USER_ID = ea.CONTRIBUTOR_ID::STRING
LEFT JOIN degree_agg da
  ON ac.USER_ID = da.CONTRIBUTOR_ID::STRING
LEFT JOIN (
  SELECT CONTRIBUTOR_ID, MIN(country) AS country
  FROM country_agg
  GROUP BY CONTRIBUTOR_ID
) ca ON ac.USER_ID = ca.CONTRIBUTOR_ID::STRING
LEFT JOIN accreditations_agg acr
  ON ac.USER_ID = acr.CONTRIBUTOR_ID::STRING
WHERE ac.SIGNUP_FLOW_ID = '{signup_flow_id}'
  AND sd.CONFIG_NAME = '{config_name}'
ORDER BY sd.SCREENING_RESULT DESC, ac.USER_ID
"""

# ── Project-ID → signup_flow_id + config_name mapping ───────────────────────
# Used when the caller has an Outlier project_id (activation_project_id /
# starting_project_id) instead of a signup_flow_id.
# Returns the dominant signup flow + config for that project,
# i.e. the (signup_flow_id, config_name) pair with the most PASS results.
PROJECT_FLOW_LOOKUP_SQL = """
SELECT
  ac.SIGNUP_FLOW_ID,
  ac.SIGNUP_FLOW_NAME,
  r.NAME AS config_name,
  COUNT(CASE WHEN UPPER(g.RESULT) = 'PASS' THEN 1 END) AS passes,
  COUNT(*) AS n
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN PUBLIC.GROWTHRESUMESCREENINGRESULTS g
  ON ac.EMAIL = g.CANDIDATE_EMAIL
JOIN PUBLIC.RESUMESCREENINGCONFIGS r
  ON g.RESUME_SCREENING_CONFIG_ID = r._ID
WHERE (
  ac.STARTING_PROJECT_ID = '{project_id}'
  OR ac.ACTIVATION_PROJECT_ID = '{project_id}'
)
  AND g.CREATED_AT >= '{start_date}'
GROUP BY 1, 2, 3
ORDER BY passes DESC, n DESC
LIMIT 20
"""

JOB_POST_SQL = """
SELECT description
FROM public.jobposts
WHERE signup_flow_id = '{signup_flow_id}'
LIMIT 1
"""

# Canonical "audience requirements" query — combines all 4 signal sources for a
# project into one row. Built 2026-04-29 after the outlier-data-analyst audit
# of GMR-0016 surfaced that `JOB_POST_SQL` returned zero rows for many ramps
# (signup_flow.JOB_POST_IDS is empty array) AND that we were ignoring the
# richest fields entirely (RESUMESCREENINGCONFIGS.QUESTIONS_TO_ASK_G_P_T,
# JOBPOSTS.{JOB_NAME, DOMAIN}, SIGNUPFLOWS.NAME).
#
# Any of (project_id, signup_flow_id, config_name) may be empty — the unmatched
# join CTEs return null and the SELECT just gets blank fields. Pass all three
# when available for the richest result.
AUDIENCE_REQUIREMENTS_SQL = """
WITH flow AS (
  SELECT
    sf._id                              AS signup_flow_id,
    sf.name                             AS flow_name,
    sf.intended_worker_skills::STRING   AS intended_worker_skills
  FROM PUBLIC.SIGNUPFLOWS sf
  WHERE sf._id = '{signup_flow_id}'
),
job AS (
  SELECT
    jp._id                              AS jobpost_id,
    jp.job_name,
    jp.domain,
    jp.pod_group,
    jp.language_code,
    jp.resume_screening_config_id,
    jp.description                      AS jobpost_description
  FROM PUBLIC.JOBPOSTS jp
  WHERE jp.signup_flow_id = '{signup_flow_id}'
  LIMIT 1
),
screening AS (
  SELECT
    rc._id                              AS config_id,
    rc.name                             AS config_name,
    rc.pod_type,
    rc.assistant_description,
    rc.questions_to_ask_g_p_t::STRING   AS screening_questions
  FROM PUBLIC.RESUMESCREENINGCONFIGS rc
  WHERE rc.name = '{config_name}'
  LIMIT 1
),
project AS (
  SELECT
    p._id                               AS project_id,
    p.name                              AS project_name,
    LEFT(COALESCE(p.description, ''), 1000) AS project_description
  FROM PUBLIC.PROJECTS p
  WHERE p._id = '{project_id}'
  LIMIT 1
)
SELECT
  COALESCE(f.signup_flow_id, '')        AS signup_flow_id,
  COALESCE(f.flow_name, '')             AS flow_name,
  COALESCE(f.intended_worker_skills, '') AS intended_worker_skills,
  COALESCE(j.jobpost_id, '')            AS jobpost_id,
  COALESCE(j.job_name, '')              AS job_name,
  COALESCE(j.domain, '')                AS domain,
  COALESCE(j.pod_group, '')             AS pod_group,
  COALESCE(j.jobpost_description, '')   AS jobpost_description,
  COALESCE(s.config_name, '')           AS config_name,
  COALESCE(s.pod_type, '')              AS pod_type,
  COALESCE(s.assistant_description, '') AS assistant_description,
  COALESCE(s.screening_questions, '')   AS screening_questions,
  COALESCE(pr.project_name, '')         AS project_name,
  COALESCE(pr.project_description, '')  AS project_description
FROM (SELECT 1) seed
LEFT JOIN flow      f  ON TRUE
LEFT JOIN job       j  ON TRUE
LEFT JOIN screening s  ON TRUE
LEFT JOIN project   pr ON TRUE
LIMIT 1
"""

# Sidecar fetch for prestige tiering: pulls the resume + LinkedIn columns
# RESUME_SQL doesn't include. Joins WORKER_RESUME_SUMMARY (most-recent employer
# pipe-list) + TNS_WORKER_LINKEDIN (full education JSON). Keyed on a list of
# user_ids so we only fetch what we need (Stage A target tier's positives).
PRESTIGE_COLUMNS_SQL = """
SELECT
    r.USER_ID                   AS cb_id,
    r.RESUME_JOB_COMPANY        AS resume_job_company,
    li.LINKEDIN_EDUCATION       AS linkedin_education
FROM SCALE_PROD.VIEW.WORKER_RESUME_SUMMARY r
LEFT JOIN SCALE_PROD.VIEW.TNS_WORKER_LINKEDIN li
    ON r.USER_ID = li.WORKER
WHERE r.USER_ID IN ({user_ids_csv})
"""

# -- Creative performance analysis for feedback loop (FEED-01)
CREATIVE_PERFORMANCE_SQL = """
SELECT
  c.CREATIVE_ID            AS creative_id,
  c.CREATIVE_URN           AS creative_urn,
  c.CAMPAIGN_NAME          AS cohort_name,
  COALESCE(c.ANGLE, 'unknown')         AS angle,
  COALESCE(c.PHOTO_SUBJECT, 'unknown') AS photo_subject,
  SUM(c.IMPRESSIONS)       AS impressions,
  SUM(c.CLICKS)            AS clicks,
  CASE WHEN SUM(c.IMPRESSIONS) > 0
    THEN ROUND(SUM(c.CLICKS)::FLOAT / SUM(c.IMPRESSIONS) * 100, 4)
    ELSE 0
  END                      AS ctr,
  SUM(c.SPEND_USD)         AS spend,
  SUM(c.CONVERSIONS)       AS conversions,
  CASE WHEN SUM(c.CONVERSIONS) > 0
    THEN ROUND(SUM(c.SPEND_USD) / SUM(c.CONVERSIONS), 2)
    ELSE NULL
  END                      AS cpa,
  MIN(c.DATE)              AS created_date
FROM VIEW.LINKEDIN_CREATIVE_COSTS c
WHERE c.DATE >= CURRENT_DATE - INTERVAL '{days_back} days'
GROUP BY 1, 2, 3, 4, 5
HAVING SUM(c.IMPRESSIONS) > 100
ORDER BY cohort_name, cpa DESC NULLS LAST
"""

# -- Cohort performance and trend analysis for feedback loop (FEED-05/FEED-06)
COHORT_METRICS_SQL = """
WITH weekly AS (
  SELECT
    ac.SIGNUP_FLOW_NAME                       AS cohort_name,
    DATE_TRUNC('week', ac.APPLICATION_DAY)    AS week_of,
    COUNT(DISTINCT ac.EMAIL)                  AS n_impressions,
    SUM(CASE WHEN ac.LINKEDIN_CLICK THEN 1 ELSE 0 END)      AS n_clicks,
    CASE WHEN COUNT(DISTINCT ac.EMAIL) > 0
      THEN ROUND(
        SUM(CASE WHEN ac.LINKEDIN_CLICK THEN 1 ELSE 0 END)::FLOAT
        / COUNT(DISTINCT ac.EMAIL) * 100, 4)
      ELSE 0
    END                                       AS ctr,
    COUNT(DISTINCT CASE WHEN ac.ACTIVATION_DAY IS NOT NULL
                        THEN ac.EMAIL END)    AS n_conversions,
    COALESCE(SUM(ac.SPEND_USD), 0)            AS spend_usd,
    CASE WHEN COUNT(DISTINCT CASE WHEN ac.ACTIVATION_DAY IS NOT NULL
                                  THEN ac.EMAIL END) > 0
      THEN ROUND(
        SUM(ac.SPEND_USD) /
        COUNT(DISTINCT CASE WHEN ac.ACTIVATION_DAY IS NOT NULL
                            THEN ac.EMAIL END), 2)
      ELSE NULL
    END                                       AS cpa
  FROM VIEW.APPLICATION_CONVERSION ac
  WHERE ac.APPLICATION_DAY >= CURRENT_DATE - INTERVAL '{weeks_back} weeks'
    AND ac.UTM_SOURCE ILIKE '%linkedin%'
  GROUP BY 1, 2
),
with_trend AS (
  SELECT
    w.*,
    LAG(w.ctr, 1) OVER (PARTITION BY w.cohort_name ORDER BY w.week_of)  AS prev_ctr,
    CASE
      WHEN LAG(w.ctr, 1) OVER (PARTITION BY w.cohort_name ORDER BY w.week_of) > 0
        THEN ROUND((w.ctr - LAG(w.ctr, 1) OVER (PARTITION BY w.cohort_name ORDER BY w.week_of))
                   / LAG(w.ctr, 1) OVER (PARTITION BY w.cohort_name ORDER BY w.week_of) * 100, 2)
      ELSE NULL
    END                                       AS trend_indicator
  FROM weekly w
)
SELECT
  cohort_name,
  week_of,
  n_impressions,
  n_clicks,
  ctr,
  n_conversions,
  cpa,
  trend_indicator
FROM with_trend
ORDER BY cohort_name, week_of DESC
"""

PASS_RATES_SQL = """
SELECT
  ac.SIGNUP_FLOW_ID,
  ac.UTM_SOURCE,
  COUNT(*) AS n,
  SUM(CASE WHEN UPPER(sd.RESULT) = 'PASS' THEN 1 ELSE 0 END) AS passes,
  passes / NULLIF(n, 0) * 100 AS pass_rate
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN PUBLIC.GROWTHRESUMESCREENINGRESULTS sd
  ON ac.EMAIL = sd.CANDIDATE_EMAIL
WHERE ac.SIGNUP_FLOW_ID = '{flow_id}'
  AND sd.CREATED_AT >= '{since_date}'
GROUP BY 1, 2
ORDER BY pass_rate DESC NULLS LAST
"""

# -- Full-funnel decomposition for the V2 feedback loop (FEED-15)
# Source: adapted from src/campaign_feedback_agent.py:60-127 (_METRICS_SQL)
# + screening leg from src/redash_db.py:264-277 (ScreeningData CTE)
# FEED-15 full-funnel decomposition: click → signup → screening-pass → activation
FUNNEL_METRICS_SQL = """
-- Source: adapted from src/campaign_feedback_agent.py:60-127 (_METRICS_SQL)
-- + screening leg from src/redash_db.py:264-277 (ScreeningData CTE)
-- FEED-15 full-funnel decomposition: click → signup → screening-pass → activation
WITH creatives AS (
    SELECT cr.ID AS creative_id, cr.CAMPAIGN_ID
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CREATIVE_HISTORY cr
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp ON cr.CAMPAIGN_ID = camp.ID
    WHERE cr.ACCOUNT_ID = {account_id}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY cr.ID ORDER BY cr.LAST_MODIFIED_AT DESC) = 1
),
metrics AS (
    SELECT c.creative_id, camp.NAME AS cohort_name,
           SUM(aa.IMPRESSIONS) AS impressions,
           SUM(aa.CLICKS) AS clicks,
           SUM(aa.COST_IN_USD) AS spend
    FROM creatives c
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE aa ON c.creative_id = aa.CREATIVE_ID
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp ON c.CAMPAIGN_ID = camp.ID
    WHERE aa.DAY >= CURRENT_DATE - INTERVAL '{days} days'
    GROUP BY 1, 2
),
funnel AS (
    SELECT
        TRY_TO_NUMBER(ac.AD_ID) AS creative_id,
        COUNT(DISTINCT ac.EMAIL) AS applications,
        COUNT(DISTINCT CASE WHEN UPPER(g.RESULT) = 'PASS' THEN ac.EMAIL END) AS screening_passes,
        COUNT(DISTINCT CASE WHEN ac.ACTIVATION_DAY IS NOT NULL THEN ac.EMAIL END) AS activations
    FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
    LEFT JOIN PUBLIC.GROWTHRESUMESCREENINGRESULTS g
        ON ac.EMAIL = g.CANDIDATE_EMAIL
    WHERE ac.UTM_SOURCE ILIKE '%linkedin%'
      AND ac.UTM_MEDIUM  = 'paid'
      AND ac.APPLICATION_DAY >= CURRENT_DATE - INTERVAL '{days} days'
      AND TRY_TO_NUMBER(ac.AD_ID) IS NOT NULL
    GROUP BY 1
)
SELECT m.cohort_name, m.creative_id,
       m.impressions, m.clicks, m.spend,
       COALESCE(f.applications, 0) AS applications,
       COALESCE(f.screening_passes, 0) AS screening_passes,
       COALESCE(f.activations, 0) AS activations,
       ROUND(m.clicks::FLOAT / NULLIF(m.impressions, 0), 4) AS ctr,
       ROUND(f.applications::FLOAT / NULLIF(m.clicks, 0), 4) AS click_to_signup,
       ROUND(f.screening_passes::FLOAT / NULLIF(f.applications, 0), 4) AS signup_to_screen,
       ROUND(f.activations::FLOAT / NULLIF(f.screening_passes, 0), 4) AS screen_to_activate
FROM metrics m
LEFT JOIN funnel f ON m.creative_id = f.creative_id
ORDER BY m.cohort_name, m.impressions DESC
"""


class RedashClient:
    """
    Executes Snowflake SQL via Redash's REST API.
    Drop-in replacement for SnowflakeClient — same public method signatures.
    """

    def __init__(self):
        self._base    = (config.REDASH_URL or "https://redash.scale.com").rstrip("/")
        self._api_key = config.REDASH_API_KEY
        self._ds_id   = config.REDASH_DATA_SOURCE_ID
        if not self._api_key:
            raise ValueError("REDASH_API_KEY is not set")
        log.info("RedashClient ready → %s (data_source_id=%s)", self._base, self._ds_id)

    # ── Public interface (mirrors SnowflakeClient) ────────────────────────────

    def fetch_screenings(
        self,
        signup_flow_id: str,
        config_name: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        sql = RESUME_SQL.format(
            signup_flow_id=_esc(signup_flow_id),
            config_name=_esc(config_name),
            start_date=start_date or config.SCREENING_START_DATE,
            end_date=end_date or config.SCREENING_END_DATE,
        )
        log.info("Fetching screenings via Redash for flow=%s config=%s", signup_flow_id, config_name)
        df = self._run_query(sql, label=f"screenings-{signup_flow_id}")
        log.info("Fetched %d screening rows", len(df))
        return df

    def resolve_project_to_flow(
        self,
        project_id: str,
        start_date: str | None = None,
    ) -> tuple[str, str] | None:
        """
        Given an Outlier project_id (activation_project_id or starting_project_id),
        return the (signup_flow_id, config_name) pair with the most PASS results.
        Returns None if no data found.
        """
        sql = PROJECT_FLOW_LOOKUP_SQL.format(
            project_id=_esc(project_id),
            start_date=start_date or config.SCREENING_START_DATE,
        )
        log.info("Resolving project_id=%s to signup_flow_id + config_name", project_id)
        df = self._run_query(sql, label=f"proj-lookup-{project_id[:12]}")
        if df.empty:
            return None
        row = df.iloc[0]
        signup_flow_id = row.get("signup_flow_id") or row.get("SIGNUP_FLOW_ID")
        config_name    = row.get("config_name")    or row.get("CONFIG_NAME")
        log.info(
            "Resolved project → signup_flow_id=%s config='%s' (passes=%s)",
            signup_flow_id, config_name, row.get("passes"),
        )
        return str(signup_flow_id), str(config_name)

    def fetch_screenings_by_project(
        self,
        project_id: str,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> tuple[pd.DataFrame, str, str]:
        """
        Fetch screening data by Outlier project_id instead of signup_flow_id.

        Automatically resolves the dominant signup_flow_id + config_name for
        the project, then delegates to fetch_screenings().

        Returns:
            (df, signup_flow_id, config_name)  — df may be empty if no data found.
        """
        resolved = self.resolve_project_to_flow(project_id, start_date=start_date)
        if not resolved:
            log.warning("No signup flow found for project_id=%s", project_id)
            return pd.DataFrame(), "", ""

        signup_flow_id, config_name = resolved
        df = self.fetch_screenings(
            signup_flow_id=signup_flow_id,
            config_name=config_name,
            start_date=start_date,
            end_date=end_date,
        )
        return df, signup_flow_id, config_name

    def fetch_audience_requirements(
        self,
        project_id: str = "",
        signup_flow_id: str = "",
        config_name: str = "",
    ) -> dict:
        """
        Pull all audience-requirement signal for a project in one query. Joins
        SIGNUPFLOWS + JOBPOSTS + RESUMESCREENINGCONFIGS + PROJECTS. Empty fields
        stay as empty strings.

        Returns dict with keys:
          flow_name, intended_worker_skills, jobpost_id, job_name, domain,
          pod_group, jobpost_description, config_name, pod_type,
          assistant_description, screening_questions, project_name,
          project_description.

        Built 2026-04-29 after GMR-0016 audit revealed that the previous
        JOB_POST_SQL returned empty for ramps where signup_flow.JOB_POST_IDS
        is empty, AND that we were ignoring the richest field entirely
        (RESUMESCREENINGCONFIGS.QUESTIONS_TO_ASK_G_P_T = formal qualification
        spec like "Does this candidate have a doctorate in Medicine, ...").
        """
        sql = AUDIENCE_REQUIREMENTS_SQL.format(
            signup_flow_id=_esc(signup_flow_id),
            config_name=_esc(config_name),
            project_id=_esc(project_id),
        )
        label = f"audience-req-{(signup_flow_id or project_id or config_name)[:8]}"
        try:
            df = self._run_query(sql, label=label)
        except Exception as exc:
            log.warning("fetch_audience_requirements failed (%s) — returning empty dict", exc)
            return {}
        if df.empty:
            return {}
        row = df.iloc[0]
        out: dict = {}
        for col in df.columns:
            val = row.get(col)
            try:
                if pd.isna(val):
                    val = ""
            except (TypeError, ValueError):
                pass
            out[col] = "" if val is None else str(val)
        return out

    def fetch_job_post_meta(self, signup_flow_id: str) -> dict:
        """
        Legacy-shape wrapper for callers that expect a job_post_meta dict.
        Built 2026-04-29 to fix a latent AttributeError — every prior call
        site (main.py:1734, icp_from_jobpost:244, etc.) was silently catching
        AttributeError and dropping the data.

        Returns {description, job_name, domain, pod_group} — the fields the
        downstream extract_base_role_candidates / family_exclusions_for code
        looks at. The richer 'description' here is the concatenation of
        flow_name + job_name + domain + screening_questions when available,
        which is the strongest signal we have.
        """
        req = self.fetch_audience_requirements(signup_flow_id=signup_flow_id)
        if not req:
            return {}
        # Build a richer description by concatenating the best signals in
        # priority order. Downstream LLM ICP derivation reads this as one blob.
        parts = [
            req.get("flow_name", ""),
            req.get("job_name", ""),
            req.get("domain", ""),
            req.get("assistant_description", ""),
            req.get("screening_questions", ""),
            req.get("jobpost_description", ""),
        ]
        description = "\n\n".join(p.strip() for p in parts if p and p.strip())
        return {
            "description": description,
            "job_name": req.get("job_name", ""),
            "domain": req.get("domain", ""),
            "pod_group": req.get("pod_group", ""),
            "flow_name": req.get("flow_name", ""),
            "screening_questions": req.get("screening_questions", ""),
        }

    def fetch_project_meta(self, project_id: str) -> dict:
        """
        Legacy-shape wrapper for callers that expect a project_meta dict.
        Same fix-the-latent-AttributeError purpose as fetch_job_post_meta.

        Returns {description, name} — fields downstream code reads.
        """
        req = self.fetch_audience_requirements(project_id=project_id)
        if not req:
            return {}
        return {
            "description": req.get("project_description", ""),
            "name": req.get("project_name", ""),
        }

    def fetch_prestige_columns(self, user_ids: list[str]) -> pd.DataFrame:
        """
        Pull `resume_job_company` and `linkedin_education` for the given USER_IDs
        from the flat WORKER_RESUME_SUMMARY + TNS_WORKER_LINKEDIN views (per
        CLAUDE.md). Returns DataFrame with columns: cb_id, resume_job_company,
        linkedin_education. Empty DF if user_ids is empty or query returns nothing.

        Used by Stage A prestige tiering — the columns aren't in RESUME_SQL because
        most callers don't need them.
        """
        if not user_ids:
            return pd.DataFrame(columns=["cb_id", "resume_job_company", "linkedin_education"])
        # Snowflake IN-list — escape and quote each user_id.
        ids_csv = ", ".join(f"'{_esc(str(uid))}'" for uid in user_ids)
        sql = PRESTIGE_COLUMNS_SQL.format(user_ids_csv=ids_csv)
        log.info("Fetching prestige columns for %d cb_ids", len(user_ids))
        df = self._run_query(sql, label=f"prestige-{len(user_ids)}cbs")
        log.info("Fetched %d prestige rows", len(df))
        return df

    def fetch_job_post(self, signup_flow_id: str) -> str:
        sql = JOB_POST_SQL.format(signup_flow_id=_esc(signup_flow_id))
        df  = self._run_query(sql, label=f"jobpost-{signup_flow_id}")
        if df.empty:
            return ""
        col = df.columns[0]
        return str(df.iloc[0][col]) if df.iloc[0][col] else ""

    def fetch_pass_rates_since(self, flow_id: str, since_date: str) -> pd.DataFrame:
        sql = PASS_RATES_SQL.format(
            flow_id=_esc(flow_id),
            since_date=_esc(since_date),
        )
        log.info("Fetching pass rates via Redash for flow=%s since=%s", flow_id, since_date)
        df = self._run_query(sql, label=f"pass-rates-{flow_id}")
        log.info("Pass rate rows: %d", len(df))
        return df

    def query_creative_performance(
        self,
        days_back: int = 7,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Return creative-level performance metrics for the feedback loop.

        Columns: creative_id, creative_urn, cohort_name, angle, photo_subject,
                 impressions, clicks, ctr, spend, conversions, cpa, created_date

        Filters to last `days_back` days and campaigns with > 100 impressions.
        Source: VIEW.LINKEDIN_CREATIVE_COSTS (FEED-01)
        """
        # end_date parameter reserved for future date-range filtering; SQL uses CURRENT_DATE
        sql = CREATIVE_PERFORMANCE_SQL.format(
            days_back=int(days_back),
        )
        label = f"creative-perf-{days_back}"
        log.info("Querying creative performance (days_back=%d)", days_back)
        _expected_cols = [
            "creative_id", "creative_urn", "cohort_name", "angle", "photo_subject",
            "impressions", "clicks", "ctr", "spend", "conversions", "cpa", "created_date",
        ]
        df = self._run_query(sql, label=label)
        if df.empty:
            log.warning("query_creative_performance returned no rows")
            return pd.DataFrame(columns=_expected_cols)
        log.info("Fetched %d rows from creative performance query", len(df))
        return df

    def query_cohort_metrics(
        self,
        days_back: int = 7,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Return cohort-level weekly performance metrics for the feedback loop.

        Columns: cohort_name, week_of, n_impressions, n_clicks, ctr,
                 n_conversions, cpa, trend_indicator

        Fetches current week + 7 prior weeks (8 weeks total) for trend analysis.
        Source: VIEW.APPLICATION_CONVERSION grouped by cohort × week (FEED-05/FEED-06)
        """
        # Use 8 weeks (56 days) to ensure current + 7 prior weeks are captured
        weeks_back = max(8, (days_back // 7) + 1)
        sql = COHORT_METRICS_SQL.format(
            weeks_back=int(weeks_back),
        )
        label = f"cohort-metrics-{weeks_back}"
        log.info("Querying cohort metrics (weeks_back=%d)", weeks_back)
        _expected_cols = [
            "cohort_name", "week_of", "n_impressions", "n_clicks", "ctr",
            "n_conversions", "cpa", "trend_indicator",
        ]
        df = self._run_query(sql, label=label)
        if df.empty:
            log.warning("query_cohort_metrics returned no rows")
            return pd.DataFrame(columns=_expected_cols)
        log.info("Fetched %d rows from cohort metrics query", len(df))
        return df

    def query_funnel_metrics(
        self,
        days_back: int = 7,
        account_id: int | None = None,
    ) -> pd.DataFrame:
        """
        FEED-15: Per-creative-per-cohort funnel decomposition.

        Returns DataFrame with columns: cohort_name, creative_id, impressions,
        clicks, spend, applications, screening_passes, activations, ctr,
        click_to_signup, signup_to_screen, screen_to_activate.
        Window = last `days_back` days (default 7).

        Source: FUNNEL_METRICS_SQL — joins
            PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE
            ↔ SCALE_PROD.VIEW.APPLICATION_CONVERSION
            ↔ PUBLIC.GROWTHRESUMESCREENINGRESULTS (LEFT JOIN on EMAIL = CANDIDATE_EMAIL)
        """
        acct = (
            account_id
            if account_id is not None
            else int(config.LINKEDIN_AD_ACCOUNT_ID)
        )
        sql = FUNNEL_METRICS_SQL.format(account_id=acct, days=int(days_back))
        label = f"funnel-metrics-{days_back}d"
        log.info(
            "Querying funnel metrics (account_id=%s, days_back=%d)",
            acct, days_back,
        )
        _expected_cols = [
            "cohort_name", "creative_id", "impressions", "clicks", "spend",
            "applications", "screening_passes", "activations",
            "ctr", "click_to_signup", "signup_to_screen", "screen_to_activate",
        ]
        df = self._run_query(sql, label=label)
        if df is None or df.empty:
            log.warning(
                "query_funnel_metrics returned no rows for window=%d days",
                days_back,
            )
            return pd.DataFrame(columns=_expected_cols)
        log.info("Fetched %d funnel rows for window=%d days", len(df), days_back)
        return df

    def close(self) -> None:
        pass  # no persistent connection to close

    # ── Internal helpers ──────────────────────────────────────────────────────

    def _run_query(self, sql: str, label: str = "query") -> pd.DataFrame:
        """Create ad-hoc query, trigger, poll, fetch — returns DataFrame."""
        qid  = self._create_query(sql, label)
        qrid = self._trigger_and_poll(qid, label)
        return self._fetch_result(qrid)

    def _create_query(self, sql: str, label: str) -> int:
        resp = requests.post(
            f"{self._base}/api/queries?api_key={self._api_key}",
            json={
                "name":           f"outlier-agent-{label}",
                "query":          sql,
                "data_source_id": self._ds_id,
                "options":        {},
            },
            timeout=30,
        )
        resp.raise_for_status()
        qid = resp.json()["id"]
        log.debug("Created Redash query id=%s label=%s", qid, label)
        return qid

    def _trigger_and_poll(self, query_id: int, label: str) -> int:
        """Trigger execution; return query_result_id."""
        resp = requests.post(
            f"{self._base}/api/queries/{query_id}/results?api_key={self._api_key}",
            json={"parameters": {}},
            timeout=30,
        )
        resp.raise_for_status()
        body = resp.json()

        # Cached result returned immediately
        if "query_result" in body:
            qrid = body["query_result"]["id"]
            log.debug("Redash cache hit for query_id=%s → qrid=%s", query_id, qrid)
            return qrid

        job_id = body["job"]["id"]
        log.debug("Redash job started: %s for label=%s", job_id, label)

        for attempt in range(_MAX_POLLS):
            time.sleep(_POLL_INTERVAL)
            job_resp = requests.get(
                f"{self._base}/api/jobs/{job_id}?api_key={self._api_key}",
                timeout=30,
            )
            job_resp.raise_for_status()
            job = job_resp.json()["job"]
            status = job["status"]

            if status == 3:   # done
                qrid = job["query_result_id"]
                log.debug("Redash job done: %s → qrid=%s (polls=%d)", job_id, qrid, attempt + 1)
                return qrid
            if status == 4:   # error
                raise RuntimeError(f"Redash query failed [{label}]: {job.get('error')}")

        raise TimeoutError(f"Redash job {job_id} did not complete after {_MAX_POLLS} polls")

    def _fetch_result(self, query_result_id: int) -> pd.DataFrame:
        resp = requests.get(
            f"{self._base}/api/query_results/{query_result_id}?api_key={self._api_key}",
            timeout=30,
        )
        resp.raise_for_status()
        data = resp.json()["query_result"]["data"]
        cols = [c["name"].lower() for c in data["columns"]]
        rows = data["rows"]
        if not rows:
            return pd.DataFrame(columns=cols)
        # Redash returns rows as dicts with UPPERCASE keys; normalise
        records = [{k.lower(): v for k, v in r.items()} for r in rows]
        return pd.DataFrame(records, columns=cols)


def _esc(val: str) -> str:
    """Minimal SQL string escaping — replace single quotes."""
    return str(val).replace("'", "''")

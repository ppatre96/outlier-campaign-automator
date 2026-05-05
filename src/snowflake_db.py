"""
Snowflake connector — runs the resume screening SQL and returns a DataFrame.
"""
import logging

import pandas as pd
import snowflake.connector

import config

log = logging.getLogger(__name__)

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
  WHERE g.CREATED_AT >= %(start_date)s
    AND g.CREATED_AT <= %(end_date)s
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
  rm.EDUCATIONS,
  rm.JOB_EXPERIENCES,
  rm.ACCREDITATIONS
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN ScreeningData sd
  ON ac.EMAIL = sd.CANDIDATE_EMAIL
LEFT JOIN resume_meta rm
  ON ac.USER_ID = rm.CONTRIBUTOR_ID::STRING
LEFT JOIN job_titles_agg jt
  ON ac.USER_ID = jt.CONTRIBUTOR_ID::STRING
LEFT JOIN fields_of_study_agg fos
  ON ac.USER_ID = fos.CONTRIBUTOR_ID::STRING
WHERE ac.SIGNUP_FLOW_ID = %(signup_flow_id)s
  AND sd.CONFIG_NAME = %(config_name)s
ORDER BY sd.SCREENING_RESULT DESC, ac.USER_ID
"""

JOB_POST_SQL = """
SELECT description
FROM public.jobposts
WHERE signup_flow_id = %(signup_flow_id)s
LIMIT 1
"""


class SnowflakeClient:
    def __init__(self):
        self._conn = snowflake.connector.connect(
            account=config.SNOWFLAKE_ACCOUNT,
            user=config.SNOWFLAKE_USER,
            password=config.SNOWFLAKE_PASSWORD,
            warehouse=config.SNOWFLAKE_WAREHOUSE,
            database=config.SNOWFLAKE_DATABASE,
            schema=config.SNOWFLAKE_SCHEMA,
            role=config.SNOWFLAKE_ROLE or None,
        )
        log.info("Snowflake connected")

    def fetch_screenings(
        self,
        signup_flow_id: str,
        config_name: str,
        project_id: str | None = None,
        start_date: str | None = None,
        end_date: str | None = None,
    ) -> pd.DataFrame:
        """
        Matches RedashClient.fetch_screenings() signature. `project_id` scopes
        the T3 (tasking) + T2 (course-pass) tier columns. Pass None only if you
        want to fall back to T1 (resume-pass) only — pick_target_tier will warn.

        Note: this SnowflakeClient path uses the LEGACY RESUME_SQL (no tier CTEs).
        The tier-aware SQL lives in `src/redash_db.py:RESUME_SQL`. If you need
        tier columns from direct Snowflake, mirror that SQL here.
        """
        if project_id:
            log.warning(
                "SnowflakeClient.fetch_screenings received project_id=%s but the "
                "legacy RESUME_SQL in snowflake_db.py doesn't include tier CTEs — "
                "T2/T3 columns will be missing. Use RedashClient for tiered analysis.",
                project_id,
            )
        params = {
            "signup_flow_id": signup_flow_id,
            "config_name":    config_name,
            "start_date":     start_date or config.SCREENING_START_DATE,
            "end_date":       end_date   or config.SCREENING_END_DATE,
        }
        log.info("Fetching screenings for flow=%s config=%s", signup_flow_id, config_name)
        cur = self._conn.cursor()
        cur.execute(RESUME_SQL, params)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df   = pd.DataFrame(rows, columns=cols)
        log.info("Fetched %d screening rows", len(df))
        return df

    def fetch_job_post(self, signup_flow_id: str) -> str:
        cur = self._conn.cursor()
        cur.execute(JOB_POST_SQL, {"signup_flow_id": signup_flow_id})
        row = cur.fetchone()
        return row[0] if row else ""

    def fetch_pass_rates_since(self, flow_id: str, since_date: str) -> pd.DataFrame:
        """
        Return pass rates per UTM source for a signup flow since a given date.
        Used by campaign_monitor to score active campaigns.
        """
        sql = """
SELECT
  ac.SIGNUP_FLOW_ID,
  ac.UTM_SOURCE,
  COUNT(*) AS n,
  SUM(CASE WHEN UPPER(sd.RESULT) = 'PASS' THEN 1 ELSE 0 END) AS passes,
  passes / NULLIF(n, 0) * 100 AS pass_rate
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN PUBLIC.GROWTHRESUMESCREENINGRESULTS sd
  ON ac.EMAIL = sd.CANDIDATE_EMAIL
WHERE ac.SIGNUP_FLOW_ID = %(flow_id)s
  AND sd.CREATED_AT >= %(since_date)s
GROUP BY 1, 2
ORDER BY pass_rate DESC NULLS LAST
"""
        params = {"flow_id": flow_id, "since_date": since_date}
        log.info("Fetching pass rates for flow=%s since=%s", flow_id, since_date)
        cur = self._conn.cursor()
        cur.execute(sql, params)
        cols = [d[0].lower() for d in cur.description]
        rows = cur.fetchall()
        df   = pd.DataFrame(rows, columns=cols)
        log.info("Pass rate rows: %d", len(df))
        return df

    def close(self):
        self._conn.close()

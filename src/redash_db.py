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
WHERE ac.SIGNUP_FLOW_ID = '{signup_flow_id}'
  AND sd.CONFIG_NAME = '{config_name}'
ORDER BY sd.SCREENING_RESULT DESC, ac.USER_ID
"""

JOB_POST_SQL = """
SELECT description
FROM public.jobposts
WHERE signup_flow_id = '{signup_flow_id}'
LIMIT 1
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

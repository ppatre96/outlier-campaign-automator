-- Snowflake per-signup-flow activations + skill_passes attribution.
--
-- Replaces the "—" in the campaign registry's `activations` and
-- `skill_passes` columns with real Snowflake-attributed counts. Called by
-- `src/activations_resolver.py` once per unique `signup_flow_id` in the
-- registry (multiple rows share a signup_flow_id — fetch once, fan out).
--
-- Authored: 2026-05-24 by outlier-data-analyst.
-- Wired: 2026-05-27.
--
-- Anchor decisions (confirmed):
--   - `CESF.ACTIVATED = TRUE` is the activation flag (maps to `t3_activated`
--     in analysis.py:6). Broader than first-OCP-passed.
--   - `CESF.EVER_PASSED_SKILL_SCREENING = TRUE` is the skill_passes signal
--     (platform's official screening-pass event).
--   - Spine: `view.APPLICATION_CONVERSION` anchored on `SIGNUP_FLOW_ID`,
--     joined to `CONTRIBUTOR_EARLY_SUCCESS_FUNNEL` on `USER_ID`.
--
-- Freshness: `CONTRIBUTOR_EARLY_SUCCESS_FUNNEL` refreshes daily (overnight
-- ETL). Safe nightly re-query. Do NOT call mid-day.

SELECT
    ac.SIGNUP_FLOW_ID                                                   AS SIGNUP_FLOW_ID,
    COUNT(DISTINCT CASE WHEN cesf.ACTIVATED = TRUE  THEN ac.USER_ID END)
                                                                        AS ACTIVATIONS,
    COUNT(DISTINCT CASE WHEN cesf.EVER_PASSED_SKILL_SCREENING = TRUE
                        THEN ac.USER_ID END)                            AS SKILL_PASSES
FROM SCALE_PROD.VIEW.APPLICATION_CONVERSION ac
LEFT JOIN SCALE_PROD.VIEW.CONTRIBUTOR_EARLY_SUCCESS_FUNNEL cesf
    ON ac.USER_ID = cesf.USER_ID
WHERE ac.SIGNUP_FLOW_ID = '{signup_flow_id}'
  AND ac.SIGNUP_DAY < CURRENT_DATE     -- exclude today's incomplete data
GROUP BY 1

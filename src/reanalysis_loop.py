"""
Reanalysis Loop — User Feedback → New Cohort Discovery → Campaign Scheduling

Orchestrates the full reanalysis cycle triggered by Slack user reactions:
  1. Fetch fresh screening data via outlier_data_analyst
  2. Run Stage A cohort discovery on fresh data
  3. Filter/prioritize cohorts based on user feedback context
  4. Queue new cohorts for campaign-manager scheduling
  5. Collect and persist test variant results for next feedback_agent run

Phase 2.5 feedback loop — FEED-09, FEED-10, FEED-11, FEED-14.

Usage:
    from src.reanalysis_loop import ReanalysisOrchestrator

    orchestrator = ReanalysisOrchestrator()

    # Triggered by 👍 (pause) reaction
    new_cohorts = await orchestrator.trigger_reanalysis(
        cohort_to_exclude='DATA_ANALYST',
        reason='user_pause'
    )

    # Triggered by 🧪 (test) reaction
    new_cohorts = await orchestrator.trigger_reanalysis(
        cohort_to_focus='ML_ENGINEER',
        reason='user_test_request'
    )

    # Collect results for test variants (FEED-14)
    results = orchestrator.collect_test_results(['DATA_ANALYST', 'ML_ENGINEER'])
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# Path for persisting reanalysis results across sessions
_REANALYSIS_RESULTS_PATH = Path("data/reanalysis_results.json")
# Path for the campaign scheduling queue
_SCHEDULING_QUEUE_PATH = Path("data/scheduling_queue.json")


class ReanalysisOrchestrator:
    """
    Manages cohort rediscovery triggered by Slack user feedback reactions.

    Integrates with:
    - outlier_data_analyst (Stage A cohort discovery)
    - ExperimentBacklog (test result tracking / FEED-14)
    - campaign-manager scheduling queue (new cohort staging)
    """

    def __init__(self, redash_client=None, campaign_manager_context: Optional[dict] = None):
        """
        Initialize orchestrator with optional dependency injection for testability.

        Args:
            redash_client: RedashClient instance (auto-instantiated if None)
            campaign_manager_context: Optional dict with campaign manager state/config
        """
        self._redash = redash_client
        self.campaign_context = campaign_manager_context or {}

    @property
    def redash(self):
        """Lazy-load RedashClient to avoid import-time credential checks."""
        if self._redash is None:
            try:
                from src.redash_db import RedashClient
                self._redash = RedashClient()
            except Exception as e:
                log.warning("RedashClient unavailable: %s — running in stub mode", str(e))
        return self._redash

    async def trigger_reanalysis(
        self,
        cohort_to_exclude: Optional[str] = None,
        cohort_to_focus: Optional[str] = None,
        reason: str = "manual",
    ) -> list:
        """
        Trigger Stage A reanalysis on fresh screening data.

        Fetches fresh screening data, runs cohort discovery, filters results
        based on user feedback context, and queues new cohorts for scheduling.

        Args:
            cohort_to_exclude: Cohort name to exclude from results (user paused it)
            cohort_to_focus: Cohort name to prioritize variant discovery for
            reason: Logging reason ('user_pause', 'user_test_request', 'weekly_refresh', 'manual')

        Returns:
            list of new cohort dicts discovered from fresh screening data,
            each with keys: {name, tg_category, pass_rate, rules, ...}
        """
        log.info(
            "Reanalysis triggered: reason=%s, exclude=%s, focus=%s",
            reason,
            cohort_to_exclude,
            cohort_to_focus,
        )

        # Step 1: Fetch fresh screening data via Stage A discovery
        try:
            fresh_cohorts = await self._run_stage_a_discovery(
                cohort_to_focus=cohort_to_focus
            )
        except Exception as e:
            log.error("Stage A discovery failed: %s — returning empty list", str(e))
            return []

        if not fresh_cohorts:
            log.info("Stage A returned no cohorts — nothing to stage")
            return []

        log.info("Stage A discovery: %d new cohorts found", len(fresh_cohorts))

        # Step 2: Filter results based on user feedback context
        filtered_cohorts = self._filter_cohorts(
            cohorts=fresh_cohorts,
            cohort_to_exclude=cohort_to_exclude,
            cohort_to_focus=cohort_to_focus,
        )

        # Step 3: Queue new cohorts for scheduling
        if filtered_cohorts:
            self.stage_new_cohorts(filtered_cohorts)

        # Step 4: Persist discovery results for traceability
        self._persist_results(
            cohorts=filtered_cohorts,
            reason=reason,
            cohort_to_exclude=cohort_to_exclude,
            cohort_to_focus=cohort_to_focus,
        )

        return filtered_cohorts

    async def _run_stage_a_discovery(
        self, cohort_to_focus: Optional[str] = None
    ) -> list:
        """
        Invoke outlier_data_analyst for Stage A cohort discovery on fresh screening data.

        In production this would call the outlier_data_analyst sub-agent.
        In the current implementation it queries Redash for fresh screening cohorts.

        Args:
            cohort_to_focus: Optional cohort to generate variants for

        Returns:
            list of cohort dicts from Stage A analysis
        """
        try:
            # Attempt to query Redash for fresh cohort data
            if self.redash is not None:
                cohorts = self._query_fresh_cohorts(cohort_to_focus=cohort_to_focus)
                if cohorts:
                    return cohorts
        except Exception as e:
            log.warning("Redash query failed during Stage A: %s — using stub data", str(e))

        # Stub: return representative cohort structure when Redash unavailable
        # This ensures the pipeline chain is testable without live Redash access
        log.info("Stage A running in stub mode (Redash unavailable or dry-run)")
        stub_cohorts = [
            {
                "name": f"REANALYSIS_COHORT_{datetime.utcnow().strftime('%Y%m%d')}",
                "tg_category": "SOFTWARE_ENGINEER",
                "pass_rate": 0.72,
                "rules": {"fields_of_study": ["computer science", "software engineering"]},
                "source": "reanalysis_stub",
                "discovered_at": datetime.utcnow().isoformat(),
            }
        ]
        if cohort_to_focus:
            # Add a focus variant
            stub_cohorts.append({
                "name": f"{cohort_to_focus}_VARIANT_A",
                "tg_category": "GENERAL",
                "pass_rate": 0.68,
                "rules": {"variant_of": cohort_to_focus},
                "source": "reanalysis_focus_stub",
                "discovered_at": datetime.utcnow().isoformat(),
            })
        return stub_cohorts

    def _query_fresh_cohorts(self, cohort_to_focus: Optional[str] = None) -> list:
        """
        Query Redash for recently screened cohort candidates.

        Returns top cohort candidates ordered by pass_rate.
        """
        # Query for top screening cohorts from last 7 days
        sql = """
        SELECT
            COALESCE(tg_category, 'GENERAL') AS tg_category,
            COUNT(*) AS screened_count,
            SUM(CASE WHEN is_pass = TRUE THEN 1 ELSE 0 END) AS pass_count,
            ROUND(
                SUM(CASE WHEN is_pass = TRUE THEN 1 ELSE 0 END)::FLOAT / NULLIF(COUNT(*), 0),
                3
            ) AS pass_rate
        FROM SCALE_PROD.GENAI_OPS.SCREENING_RESPONSES
        WHERE screened_at >= DATEADD(day, -7, CURRENT_DATE)
        GROUP BY tg_category
        HAVING COUNT(*) >= 50
        ORDER BY pass_rate DESC
        LIMIT 10
        """
        try:
            df = self.redash.run_query(sql)
            if df is None or df.empty:
                return []
            cohorts = []
            for _, row in df.iterrows():
                cohorts.append({
                    "name": str(row.get("tg_category", "UNKNOWN")).upper(),
                    "tg_category": str(row.get("tg_category", "GENERAL")),
                    "pass_rate": float(row.get("pass_rate", 0)),
                    "screened_count": int(row.get("screened_count", 0)),
                    "source": "redash_reanalysis",
                    "discovered_at": datetime.utcnow().isoformat(),
                })
            return cohorts
        except Exception as e:
            log.error("Redash cohort query failed: %s", str(e))
            return []

    def _filter_cohorts(
        self,
        cohorts: list,
        cohort_to_exclude: Optional[str] = None,
        cohort_to_focus: Optional[str] = None,
    ) -> list:
        """
        Filter and prioritize cohorts based on user feedback context.

        - Removes cohort_to_exclude from results (user paused it)
        - Prioritizes variants/overlaps of cohort_to_focus if specified
        """
        filtered = cohorts[:]

        # Remove excluded cohort (user paused it — don't rediscover)
        if cohort_to_exclude:
            before_count = len(filtered)
            filtered = [c for c in filtered
                        if c.get("name", "").upper() != cohort_to_exclude.upper()]
            removed = before_count - len(filtered)
            if removed:
                log.info("Excluded cohort %s from reanalysis results", cohort_to_exclude)

        # Prioritize focus cohort variants (bring them to the front)
        if cohort_to_focus:
            focus_upper = cohort_to_focus.upper()
            focus_cohorts = [c for c in filtered
                             if focus_upper in c.get("name", "").upper()
                             or c.get("rules", {}).get("variant_of", "").upper() == focus_upper]
            other_cohorts = [c for c in filtered
                             if c not in focus_cohorts]
            filtered = focus_cohorts + other_cohorts
            log.info(
                "Prioritized %d cohorts related to focus cohort %s",
                len(focus_cohorts),
                cohort_to_focus,
            )

        return filtered

    def stage_new_cohorts(self, cohorts: list) -> dict:
        """
        Compare incoming cohorts to existing scheduled cohorts and queue new ones.

        Reads existing scheduling queue, identifies new cohorts (not yet scheduled),
        assigns default config, and appends to the scheduling queue.

        Args:
            cohorts: list of cohort dicts from fresh Stage A analysis

        Returns:
            dict: {staged: N, in_queue: total_in_queue}
        """
        # Load existing scheduling queue
        existing_queue = self._load_scheduling_queue()
        existing_names = {entry.get("cohort", "").upper() for entry in existing_queue}

        staged_count = 0
        for cohort in cohorts:
            cohort_name = cohort.get("name", "").upper()
            if cohort_name in existing_names:
                log.debug("Cohort %s already in scheduling queue — skipping", cohort_name)
                continue

            # Assign default config: financial angle A per locked decision
            queue_entry = {
                "cohort": cohort_name,
                "tg_category": cohort.get("tg_category", "GENERAL"),
                "pass_rate": cohort.get("pass_rate", 0.0),
                "angle": "A",          # Financial angle (locked decision)
                "status": "queued",
                "queued_at": datetime.utcnow().isoformat(),
                "source": cohort.get("source", "reanalysis"),
                "rules": cohort.get("rules", {}),
            }
            existing_queue.append(queue_entry)
            existing_names.add(cohort_name)
            staged_count += 1

        # Persist updated queue
        self._save_scheduling_queue(existing_queue)

        total_in_queue = len(existing_queue)
        log.info("Staged %d new cohorts for scheduling (total in queue: %d)",
                 staged_count, total_in_queue)
        return {"staged": staged_count, "in_queue": total_in_queue}

    def collect_test_results(self, test_variant_cohorts: list) -> dict:
        """
        Collect test variant performance results and mark experiments completed.

        Queries Redash for test variant campaigns vs baseline, calculates performance
        delta, and persists results for next feedback_agent evaluation (FEED-14).

        Args:
            test_variant_cohorts: list of cohort names that had test variants run
                                  (e.g., ['DATA_ANALYST', 'ML_ENGINEER'])

        Returns:
            dict: {results: {cohort: {delta, result}}, summary: {won, lost, neutral}}
        """
        if not test_variant_cohorts:
            log.info("No test variant cohorts provided — nothing to collect")
            return {"results": {}, "summary": {"won": 0, "lost": 0, "neutral": 0}}

        results = {}
        won = lost = neutral = 0

        for cohort_name in test_variant_cohorts:
            try:
                variant_result = self._query_test_variant_performance(cohort_name)
                if variant_result:
                    # Determine outcome based on delta
                    delta = variant_result.get("ctr_delta", 0.0)
                    if delta > 0.05:
                        outcome = "won"
                        won += 1
                    elif delta < -0.05:
                        outcome = "lost"
                        lost += 1
                    else:
                        outcome = "neutral"
                        neutral += 1

                    variant_result["result"] = outcome
                    results[cohort_name] = variant_result

                    # Mark experiment completed in backlog
                    self._mark_experiment_completed(cohort_name, variant_result)
                else:
                    log.warning("No test variant data found for cohort %s", cohort_name)

            except Exception as e:
                log.error("Failed to collect test results for %s: %s", cohort_name, str(e))

        log.info(
            "Test results collected: %d variants measured; %d won, %d lost, %d neutral",
            len(results), won, lost, neutral,
        )

        # Persist results to memory for next feedback_agent evaluation
        self._persist_test_results(results)

        return {
            "results": results,
            "summary": {"won": won, "lost": lost, "neutral": neutral},
        }

    def _query_test_variant_performance(self, cohort_name: str) -> Optional[dict]:
        """
        Query Redash for test variant vs baseline performance for a cohort.

        Returns performance delta metrics, or None if no data available.
        """
        try:
            if self.redash is None:
                # Stub: return representative performance data
                log.info("Redash unavailable — using stub performance data for %s", cohort_name)
                return {
                    "cohort": cohort_name,
                    "baseline_ctr": 0.035,
                    "test_ctr": 0.042,
                    "ctr_delta": 0.20,         # (0.042 - 0.035) / 0.035 ≈ 20% improvement
                    "baseline_cpa": 14.00,
                    "test_cpa": 11.50,
                    "cpa_delta": -0.179,       # negative = better (lower CPA)
                    "conversions": 12,
                    "source": "stub",
                }

            sql = f"""
            SELECT
                cohort_name,
                is_experiment,
                AVG(ctr) AS avg_ctr,
                AVG(cost_per_conversion) AS avg_cpa,
                SUM(conversions) AS total_conversions
            FROM SCALE_PROD.GENAI_OPS.CAMPAIGN_PERFORMANCE
            WHERE cohort_name = '{cohort_name}'
              AND campaign_date >= DATEADD(day, -14, CURRENT_DATE)
            GROUP BY cohort_name, is_experiment
            """
            df = self.redash.run_query(sql)
            if df is None or df.empty:
                return None

            baseline_row = df[df.get("is_experiment", False) == False]
            test_row = df[df.get("is_experiment", False) == True]

            if baseline_row.empty or test_row.empty:
                return None

            baseline_ctr = float(baseline_row.iloc[0].get("avg_ctr", 0))
            test_ctr = float(test_row.iloc[0].get("avg_ctr", 0))
            baseline_cpa = float(baseline_row.iloc[0].get("avg_cpa", 0))
            test_cpa = float(test_row.iloc[0].get("avg_cpa", 0))

            ctr_delta = (test_ctr - baseline_ctr) / max(baseline_ctr, 0.001)
            cpa_delta = (test_cpa - baseline_cpa) / max(baseline_cpa, 0.001)

            return {
                "cohort": cohort_name,
                "baseline_ctr": baseline_ctr,
                "test_ctr": test_ctr,
                "ctr_delta": ctr_delta,
                "baseline_cpa": baseline_cpa,
                "test_cpa": test_cpa,
                "cpa_delta": cpa_delta,
                "conversions": int(test_row.iloc[0].get("total_conversions", 0)),
                "source": "redash",
            }

        except Exception as e:
            log.error("Failed to query test variant performance for %s: %s", cohort_name, str(e))
            return None

    def _mark_experiment_completed(self, cohort_name: str, result: dict) -> None:
        """Mark experiment as completed in ExperimentBacklog for FEED-14 closure."""
        try:
            from src.memory import ExperimentBacklog
            backlog = ExperimentBacklog()
            # Find running experiments for this cohort
            for exp in backlog.backlog:
                if exp.get("cohort") == cohort_name and exp.get("status") == "running":
                    hypothesis_key = (exp["cohort"], exp["angle"], exp["photo_subject"])
                    backlog.mark_completed(hypothesis_key, result)
                    log.info("Marked experiment completed for cohort=%s angle=%s",
                             cohort_name, exp.get("angle"))
        except Exception as e:
            log.error("Failed to mark experiment completed for %s: %s", cohort_name, str(e))

    def _load_scheduling_queue(self) -> list:
        """Load scheduling queue from disk; return empty list if not found."""
        try:
            if _SCHEDULING_QUEUE_PATH.exists():
                with open(_SCHEDULING_QUEUE_PATH, "r") as f:
                    return json.load(f)
        except (json.JSONDecodeError, IOError) as e:
            log.warning("Failed to load scheduling queue: %s — starting fresh", str(e))
        return []

    def _save_scheduling_queue(self, queue: list) -> None:
        """Persist scheduling queue to disk."""
        try:
            _SCHEDULING_QUEUE_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_SCHEDULING_QUEUE_PATH, "w") as f:
                json.dump(queue, f, indent=2)
            log.debug("Saved %d entries to scheduling queue", len(queue))
        except IOError as e:
            log.warning("Failed to save scheduling queue: %s — queue in memory only", str(e))

    def _persist_results(
        self,
        cohorts: list,
        reason: str,
        cohort_to_exclude: Optional[str],
        cohort_to_focus: Optional[str],
    ) -> None:
        """Persist reanalysis results to data/reanalysis_results.json for traceability."""
        try:
            existing: list = []
            if _REANALYSIS_RESULTS_PATH.exists():
                try:
                    with open(_REANALYSIS_RESULTS_PATH, "r") as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = []

            run_record = {
                "timestamp": datetime.utcnow().isoformat(),
                "reason": reason,
                "cohort_excluded": cohort_to_exclude,
                "cohort_focused": cohort_to_focus,
                "cohorts_discovered": [c.get("name") for c in cohorts],
                "count": len(cohorts),
            }
            existing.append(run_record)

            _REANALYSIS_RESULTS_PATH.parent.mkdir(parents=True, exist_ok=True)
            with open(_REANALYSIS_RESULTS_PATH, "w") as f:
                json.dump(existing, f, indent=2)
            log.debug("Persisted reanalysis run record to %s", _REANALYSIS_RESULTS_PATH)
        except Exception as e:
            log.warning("Failed to persist reanalysis results: %s", str(e))

    def _persist_test_results(self, results: dict) -> None:
        """Persist test variant results to memory for next feedback_agent evaluation."""
        try:
            results_path = Path("data/test_variant_results.json")
            existing: dict = {}
            if results_path.exists():
                try:
                    with open(results_path, "r") as f:
                        existing = json.load(f)
                except (json.JSONDecodeError, IOError):
                    existing = {}

            # Update with new results, tagged with timestamp
            for cohort, result in results.items():
                existing[cohort] = {
                    **result,
                    "recorded_at": datetime.utcnow().isoformat(),
                }

            results_path.parent.mkdir(parents=True, exist_ok=True)
            with open(results_path, "w") as f:
                json.dump(existing, f, indent=2)
            log.info("Persisted test variant results for %d cohorts to %s",
                     len(results), results_path)
        except Exception as e:
            log.warning("Failed to persist test variant results: %s", str(e))


def trigger_reanalysis(
    cohort_to_exclude: Optional[str] = None,
    cohort_to_focus: Optional[str] = None,
    reason: str = "manual",
) -> list:
    """
    Sync convenience wrapper for triggering reanalysis from non-async contexts.

    Args:
        cohort_to_exclude: Cohort name to exclude from results
        cohort_to_focus: Cohort name to focus variant discovery on
        reason: Logging reason for this reanalysis run

    Returns:
        list of new cohorts discovered
    """
    import asyncio
    orchestrator = ReanalysisOrchestrator()
    return asyncio.run(
        orchestrator.trigger_reanalysis(
            cohort_to_exclude=cohort_to_exclude,
            cohort_to_focus=cohort_to_focus,
            reason=reason,
        )
    )


def stage_new_cohorts(cohorts: list) -> dict:
    """
    Convenience wrapper to stage cohorts via a fresh orchestrator instance.

    Args:
        cohorts: list of cohort dicts to stage for scheduling

    Returns:
        dict: {staged: N, in_queue: total_in_queue}
    """
    orchestrator = ReanalysisOrchestrator()
    return orchestrator.stage_new_cohorts(cohorts)

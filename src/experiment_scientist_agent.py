"""
Experiment Scientist Agent - Consolidates feedback into prioritized test queue.

Ingests hypotheses from feedback_agent + competitor intelligence, maintains
a priority-ordered experiment backlog, and generates test directives for
the creative brief generator at 80/20 baseline/test split.
"""

import json
import logging
import re
from pathlib import Path
from typing import Optional, List

from src.memory import ExperimentBacklog

log = logging.getLogger(__name__)


class ExperimentScientistAgent:
    """
    Consolidates insights and generates test directives.

    Maintains experiment backlog, integrates competitor intelligence,
    and decides what to test next based on feedback + competitor data.
    """

    def __init__(self, backlog: Optional[ExperimentBacklog] = None, redash_client=None):
        self.backlog = backlog or ExperimentBacklog()
        self.redash = redash_client

    def ingest_feedback(self, hypotheses: List[dict], competitor_intel: dict) -> None:
        """
        Ingest feedback_agent hypotheses and competitor intelligence.

        Args:
            hypotheses: List of {cohort, angle, photo_subject, reason, expected_impact, ...}
            competitor_intel: {experiment_ideas: [...], competitor_hooks: [...], avoid: [...]}
        """
        if not hypotheses:
            log.info("No hypotheses to ingest")
            return

        added_count = 0

        # Process feedback_agent hypotheses
        for hyp in hypotheses:
            score = self._compute_priority_score(hyp)
            hyp['priority_score'] = score
            self.backlog.add_hypothesis(hyp)
            added_count += 1

        # Process competitor intelligence
        competitor_ideas = competitor_intel.get('experiment_ideas', [])
        for idea in competitor_ideas:
            # Check if matches existing cohort
            cohort = idea.get('cohort', 'GENERAL')
            angle = idea.get('angle', 'A')
            photo_subject = idea.get('photo_subject', 'baseline')

            # Look for existing hypothesis to boost
            found = False
            for h in self.backlog.backlog:
                if h['cohort'] == cohort and h['angle'] == angle:
                    h['priority_score'] += 0.5
                    log.debug("Boosted existing hypothesis: %s/%s (competitor match)", cohort, angle)
                    found = True
                    break

            # If new idea, add it
            if not found:
                new_hyp = {
                    'cohort': cohort,
                    'angle': angle,
                    'photo_subject': photo_subject,
                    'reason': f"Competitor idea: {idea.get('description', 'unknown')}",
                    'expected_impact': idea.get('expected_impact', '3%'),
                    'priority_score': 0.7,
                    'status': 'pending'
                }
                self.backlog.add_hypothesis(new_hyp)
                added_count += 1

        log.info("Ingested %d hypotheses + %d competitor ideas; backlog now %d experiments",
                 len(hypotheses), len(competitor_ideas), len(self.backlog.backlog))

    def _compute_priority_score(self, hypothesis: dict) -> float:
        """
        Compute priority_score = expected_impact × confidence × feasibility.

        Args:
            hypothesis: dict with 'expected_impact' string like "CTR+5%" or "CPA-10%"

        Returns:
            Float score (0.0 to 10.0)
        """
        # Extract impact percentage from string like "CTR+5%" or "CPA-10%"
        impact_str = hypothesis.get('expected_impact', '3%')
        try:
            match = re.search(r'([+-]?\d+(?:\.\d+)?)\s*%?', impact_str)
            if match:
                base_score = float(match.group(1))
            else:
                base_score = 3.0
        except (ValueError, TypeError):
            base_score = 3.0

        # Confidence: assume 0.9 (high confidence in feedback loop)
        confidence = 0.9

        # Feasibility: assume 0.9 (all are feasible unless noted)
        feasibility = 0.9

        priority_score = base_score * confidence * feasibility
        return min(priority_score, 10.0)  # Cap at 10.0

    def generate_test_directive(self, cohort_name: str) -> dict:
        """
        Generate test directive for creative brief generator.

        Args:
            cohort_name: Cohort to generate test for (e.g., "DATA_ANALYST")

        Returns:
            dict: {cohort, angle, photo_subject, priority, expected_impact, test_allocation}
        """
        # Find pending experiments for this cohort
        pending = [h for h in self.backlog.backlog
                   if h.get('status') == 'pending' and h.get('cohort') == cohort_name]

        if not pending:
            # No test available; return baseline directive
            return {
                'cohort': cohort_name,
                'angle': 'A',
                'photo_subject': 'baseline',
                'test_allocation': 100,  # 100% baseline (no test)
                'priority': 0.0
            }

        # Use highest priority pending experiment
        test = pending[0]
        self.backlog.mark_running((test['cohort'], test['angle'], test['photo_subject']))

        directive = {
            'cohort': test['cohort'],
            'angle': test['angle'],
            'photo_subject': test['photo_subject'],
            'priority': test.get('priority_score', 0.0),
            'expected_impact': test.get('expected_impact', '3%'),
            'test_allocation': 20,  # 20% test variant, 80% baseline
            'reason': test.get('reason', 'Unknown')
        }
        log.info("Test directive for %s: angle=%s, photo=%s (priority=%.1f)",
                 cohort_name, test['angle'], test['photo_subject'],
                 test.get('priority_score', 0.0))
        return directive

    def get_backlog_status(self) -> dict:
        """Return summary of experiment queue status."""
        total = len(self.backlog.backlog)
        pending = sum(1 for h in self.backlog.backlog if h.get('status') == 'pending')
        running = sum(1 for h in self.backlog.backlog if h.get('status') == 'running')
        completed = sum(1 for h in self.backlog.backlog if h.get('status') == 'completed')

        # Get next experiments to test
        next_to_test = self.backlog.peek_next(3)

        return {
            'total': total,
            'pending': pending,
            'running': running,
            'completed': completed,
            'next_to_test': [
                {
                    'cohort': h['cohort'],
                    'angle': h['angle'],
                    'photo_subject': h['photo_subject'],
                    'priority_score': h.get('priority_score', 0.0)
                }
                for h in next_to_test
            ]
        }

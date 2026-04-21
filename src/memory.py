"""
Persistent storage for experiment backlog and hypothesis tracking.

The ExperimentBacklog maintains a priority-ordered queue of test hypotheses
across process restarts, enabling continuous optimization feedback loops.
"""

import json
import logging
from pathlib import Path
from datetime import datetime
from typing import Optional, List

log = logging.getLogger(__name__)


class ExperimentBacklog:
    """
    Manages experiment hypothesis queue with persistence.

    Stores experiments as priority-sorted JSON and provides load/save
    for cross-session state preservation.
    """

    def __init__(self, filepath: str = "data/experiment_backlog.json"):
        self.filepath = filepath
        self.backlog: List[dict] = []
        self.load()

    def load(self) -> None:
        """Load backlog from disk; create empty if missing."""
        try:
            if Path(self.filepath).exists():
                with open(self.filepath, 'r') as f:
                    self.backlog = json.load(f)
                    # Ensure sorted by priority
                    self.backlog.sort(key=lambda h: h.get('priority_score', 0), reverse=True)
                log.info("Loaded %d experiments from backlog", len(self.backlog))
            else:
                log.info("No existing backlog; starting fresh")
                self.backlog = []
        except (json.JSONDecodeError, IOError) as e:
            log.error("Failed to load backlog: %s; starting fresh", str(e))
            self.backlog = []

    def save(self) -> None:
        """Write backlog to disk."""
        try:
            Path(self.filepath).parent.mkdir(parents=True, exist_ok=True)
            with open(self.filepath, 'w') as f:
                json.dump(self.backlog, f, indent=2)
            log.info("Saved %d experiments to backlog", len(self.backlog))
        except IOError as e:
            log.error("Failed to save backlog: %s (backlog remains in memory)", str(e))

    def add_hypothesis(self, hypothesis: dict) -> None:
        """
        Add hypothesis to backlog if not already present.

        Args:
            hypothesis: {cohort, angle, photo_subject, reason, expected_impact, priority_score, status}
                status: 'pending' | 'running' | 'completed' | 'failed'
        """
        key = (hypothesis['cohort'], hypothesis['angle'], hypothesis['photo_subject'])

        # Check for duplicate
        for h in self.backlog:
            if (h['cohort'] == key[0] and
                h['angle'] == key[1] and
                h['photo_subject'] == key[2]):
                log.debug("Hypothesis already exists: %s/%s", hypothesis['cohort'], hypothesis['angle'])
                return

        # Add with defaults
        hypothesis.setdefault('status', 'pending')
        hypothesis.setdefault('created_at', datetime.utcnow().isoformat())
        self.backlog.append(hypothesis)
        self.backlog.sort(key=lambda h: h.get('priority_score', 0), reverse=True)
        log.info("Added hypothesis: %s/%s (priority %.2f)",
                 hypothesis['cohort'], hypothesis['angle'], hypothesis.get('priority_score', 0))
        self.save()

    def peek_next(self, n: int = 1) -> List[dict]:
        """Return top N pending experiments without removing."""
        pending = [h for h in self.backlog if h.get('status') == 'pending']
        return pending[:n]

    def mark_running(self, hypothesis_key: tuple) -> None:
        """Mark hypothesis as 'running' (test in progress)."""
        cohort, angle, photo = hypothesis_key
        for h in self.backlog:
            if (h['cohort'] == cohort and
                h['angle'] == angle and
                h['photo_subject'] == photo):
                h['status'] = 'running'
                h['started_at'] = datetime.utcnow().isoformat()
                log.debug("Marked running: %s/%s", cohort, angle)
                self.save()
                return

    def mark_completed(self, hypothesis_key: tuple, result: dict) -> None:
        """Mark hypothesis as 'completed' and store result."""
        cohort, angle, photo = hypothesis_key
        for h in self.backlog:
            if (h['cohort'] == cohort and
                h['angle'] == angle and
                h['photo_subject'] == photo):
                h['status'] = 'completed'
                h['completed_at'] = datetime.utcnow().isoformat()
                h['result'] = result  # {ctr, cpa, conversions, etc.}
                log.debug("Marked completed: %s/%s with result %s", cohort, angle, result)
                self.save()
                return

    def clear_pending(self) -> None:
        """Clear all pending experiments (for reset or testing)."""
        pending_count = sum(1 for h in self.backlog if h.get('status') == 'pending')
        self.backlog = [h for h in self.backlog if h.get('status') != 'pending']
        log.info("Cleared %d pending experiments", pending_count)
        self.save()


def load_backlog(filepath: str = "data/experiment_backlog.json") -> ExperimentBacklog:
    """Convenience function to load existing backlog from disk."""
    return ExperimentBacklog(filepath)

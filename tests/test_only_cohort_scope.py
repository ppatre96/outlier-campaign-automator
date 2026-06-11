"""Feature 010 — new-cohort review+launch. Unit tests for the isolation core
(`_ramp_to_rows` ONLY_COHORT scope filter) and the poller's new-cohort
detection logic (`_detect_new_cohorts` + `_cohort_label`).

No DB, no HTTP: the DELETE guards and the pending_cohorts SQL helpers are
exercised by the E2E (they need a live Postgres). These tests pin the two
pure-Python pieces that guarantee a scoped run only ever touches the target
cohort, and that a cohort added after first prep is detected exactly once.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _ramp(n_cohorts: int = 3):
    from src.smart_ramp_client import RampRecord, CohortSpec
    cohorts = [
        CohortSpec(
            id=f"cohort{i}",
            cohort_description=f"Cohort {i} description",
            signup_flow_id=f"flow_{i}",
            selected_lp_url=f"https://outlier.ai/c{i}",
            included_geos=["US"],
            matched_locales=None,
            target_activations=100,
            job_post_id=None,
        )
        for i in range(n_cohorts)
    ]
    return RampRecord(
        id="GMR-TEST", project_id="p", project_name="Test", requester_name="Pranav",
        summary="s", submitted_at="t", updated_at="t", status="submitted",
        linear_issue_id=None, linear_url=None, cohorts=cohorts,
    )


# ── _ramp_to_rows ONLY_COHORT scope filter (the isolation core) ───────────────

def test_ramp_to_rows_keeps_only_the_target_cohort(monkeypatch):
    import config, main as M
    monkeypatch.setattr(config, "ONLY_COHORT", "cohort1", raising=False)
    rows = M._ramp_to_rows(_ramp(3))
    assert [r["cohort_id"] for r in rows] == ["cohort1"]


def test_ramp_to_rows_unfiltered_when_only_cohort_unset(monkeypatch):
    import config, main as M
    monkeypatch.setattr(config, "ONLY_COHORT", "", raising=False)
    rows = M._ramp_to_rows(_ramp(3))
    assert {r["cohort_id"] for r in rows} == {"cohort0", "cohort1", "cohort2"}


def test_ramp_to_rows_unknown_only_cohort_keeps_nothing(monkeypatch):
    """A typo'd / stale cohort id scopes to zero rows — never falls back to all
    (which would re-touch every existing cohort)."""
    import config, main as M
    monkeypatch.setattr(config, "ONLY_COHORT", "does-not-exist", raising=False)
    rows = M._ramp_to_rows(_ramp(3))
    assert rows == []


# ── Poller new-cohort detection ───────────────────────────────────────────────

class _FakeUIDecisions:
    """Captures the ui_decisions calls _detect_new_cohorts makes."""
    def __init__(self, prepped, pending=None):
        self._prepped = list(prepped)
        self._pending = list(pending or [])
        self.set_prepped_calls = []
        self.added_pending = []

    def get_prepped_cohort_ids(self, ramp_id): return list(self._prepped)
    def set_prepped_cohort_ids(self, ramp_id, ids): self.set_prepped_calls.append(list(ids))
    def get_pending_cohorts(self, ramp_id): return list(self._pending)
    def add_pending_cohorts(self, ramp_id, entries): self.added_pending.extend(entries)


def _install_fake(monkeypatch, fake):
    import src.ui_decisions as U
    for name in ("get_prepped_cohort_ids", "set_prepped_cohort_ids",
                 "get_pending_cohorts", "add_pending_cohorts"):
        monkeypatch.setattr(U, name, getattr(fake, name))


def test_detect_bootstraps_when_snapshot_empty(monkeypatch):
    """A ramp prepped before the feature shipped → baseline to current cohorts,
    flag nothing as new."""
    import scripts.smart_ramp_poller as P
    fake = _FakeUIDecisions(prepped=[])
    _install_fake(monkeypatch, fake)
    P._detect_new_cohorts(_ramp(3), dry_run=False)
    assert fake.set_prepped_calls == [["cohort0", "cohort1", "cohort2"]]
    assert fake.added_pending == []


def test_detect_records_only_the_new_cohort(monkeypatch):
    import scripts.smart_ramp_poller as P
    fake = _FakeUIDecisions(prepped=["cohort0", "cohort1"])
    _install_fake(monkeypatch, fake)
    P._detect_new_cohorts(_ramp(3), dry_run=False)  # cohort2 is new
    assert fake.set_prepped_calls == []              # not a bootstrap
    assert [e["cohort_id"] for e in fake.added_pending] == ["cohort2"]
    assert fake.added_pending[0]["status"] == "detected"
    assert fake.added_pending[0]["label"]            # non-empty label


def test_detect_noop_when_nothing_new(monkeypatch):
    import scripts.smart_ramp_poller as P
    fake = _FakeUIDecisions(prepped=["cohort0", "cohort1", "cohort2"])
    _install_fake(monkeypatch, fake)
    P._detect_new_cohorts(_ramp(3), dry_run=False)
    assert fake.added_pending == []
    assert fake.set_prepped_calls == []


def test_detect_skips_already_pending(monkeypatch):
    """A cohort already in pending_cohorts is not re-added."""
    import scripts.smart_ramp_poller as P
    fake = _FakeUIDecisions(
        prepped=["cohort0", "cohort1"],
        pending=[{"cohort_id": "cohort2", "status": "awaiting_review"}],
    )
    _install_fake(monkeypatch, fake)
    P._detect_new_cohorts(_ramp(3), dry_run=False)
    assert fake.added_pending == []


def test_detect_dry_run_is_noop(monkeypatch):
    import scripts.smart_ramp_poller as P
    fake = _FakeUIDecisions(prepped=[])
    _install_fake(monkeypatch, fake)
    P._detect_new_cohorts(_ramp(2), dry_run=True)
    assert fake.set_prepped_calls == []
    assert fake.added_pending == []


def test_cohort_label_truncates(monkeypatch):
    import scripts.smart_ramp_poller as P
    from src.smart_ramp_client import CohortSpec
    c = CohortSpec(
        id="x", cohort_description="A" * 200, signup_flow_id=None,
        selected_lp_url=None, included_geos=[], matched_locales=None,
        target_activations=None, job_post_id=None,
    )
    label = P._cohort_label(c)
    assert len(label) <= 80
    assert label.endswith("…")

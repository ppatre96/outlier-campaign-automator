"""Tests for the brief-review gate (2026-05-22):
- poller transitions ramp to 'awaiting_brief_review' when prep wrote briefs.
- poller falls back to 'awaiting_approval' when briefs_generated == 0.
- poller skips ramps already in 'awaiting_brief_review'.
- src.ui_decisions.confirm_briefs CAS atomicity (mocked psycopg)."""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)


def _fake_record(ramp_id: str = "GMR-BR-TEST") -> SimpleNamespace:
    return SimpleNamespace(
        id=ramp_id,
        project_id="",
        project_name=None,
        requester_name="Test Requester",
        summary="test summary",
        submitted_at="2026-05-22T00:00:00Z",
        updated_at="",
        status="submitted",
        linear_issue_id=None,
        linear_url=None,
        cohorts=[],
    )


# ── Poller: brief-review transition when prep generated briefs ───────────────


def test_poller_transitions_to_awaiting_brief_review_when_briefs_generated(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main

    prep_result = {
        "ok": True, "prep_only": True,
        "briefs_generated": 27,
        "cohorts_mined": [{"name": "skills__metagenomics"}],
        "campaign_groups": [], "inmail_campaigns": [],
        "static_campaigns": [], "creative_paths": {}, "per_cohort": [],
    }
    fake_prep = MagicMock(return_value=prep_result)
    fake_launch = MagicMock()
    monkeypatch.setattr(main, "_prep_ramp", fake_prep)
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)
    monkeypatch.setattr(main, "_ramp_to_rows", MagicMock(return_value=[{}]))

    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision", MagicMock(return_value=None))
    fake_brief_review = MagicMock()
    fake_legacy = MagicMock()
    monkeypatch.setattr(ui_decisions, "upsert_awaiting_brief_review", fake_brief_review)
    monkeypatch.setattr(ui_decisions, "upsert_awaiting_approval", fake_legacy)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-BR-NEW"), dry_run=False, version=1)

    assert out["ok"] is True
    fake_prep.assert_called_once_with("GMR-BR-NEW")
    fake_brief_review.assert_called_once()
    fake_legacy.assert_not_called()  # brief-review path wins when briefs were written
    fake_launch.assert_not_called()
    # prep_summary should carry briefs_generated
    assert fake_brief_review.call_args.kwargs["prep_summary"]["briefs_generated"] == 27


def test_poller_falls_back_to_awaiting_approval_when_no_briefs(monkeypatch):
    """Brief-gen failure path: prep returns briefs_generated=0 (e.g. LLM down).
    Poller must NOT block the ramp — falls back to the legacy gate."""
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main

    prep_result = {
        "ok": True, "prep_only": True,
        "briefs_generated": 0,
        "cohorts_mined": [{"name": "skills__example"}],
        "campaign_groups": [], "inmail_campaigns": [],
        "static_campaigns": [], "creative_paths": {}, "per_cohort": [],
    }
    monkeypatch.setattr(main, "_prep_ramp", MagicMock(return_value=prep_result))
    monkeypatch.setattr(main, "_launch_ramp", MagicMock())
    monkeypatch.setattr(main, "_ramp_to_rows", MagicMock(return_value=[{}]))

    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision", MagicMock(return_value=None))
    fake_brief_review = MagicMock()
    fake_legacy = MagicMock()
    monkeypatch.setattr(ui_decisions, "upsert_awaiting_brief_review", fake_brief_review)
    monkeypatch.setattr(ui_decisions, "upsert_awaiting_approval", fake_legacy)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    run_ramp_pipeline(_fake_record("GMR-NO-BRIEFS"), dry_run=False, version=1)

    fake_brief_review.assert_not_called()
    fake_legacy.assert_called_once()


def test_poller_skips_ramps_in_awaiting_brief_review(monkeypatch):
    """Once a ramp is in awaiting_brief_review, the poller must NOT re-run prep
    or fire the launch — it just waits for the UI to confirm or the auto-sweep
    to flip the status."""
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    fake_prep = MagicMock()
    fake_launch = MagicMock()
    monkeypatch.setattr(main, "_prep_ramp", fake_prep)
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)

    from src.ui_decisions import Decision
    decision = Decision(
        ramp_id="GMR-BR-WAIT", status="awaiting_brief_review",
        channels=[], budgets={}, version=1,
    )
    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision",
                        MagicMock(return_value=decision))

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-BR-WAIT"), dry_run=False, version=1)

    assert out["ok"] is True
    assert out["ui_gated"] is True
    assert out["status"] == "awaiting_brief_review"
    fake_prep.assert_not_called()
    fake_launch.assert_not_called()


# ── ui_decisions.confirm_briefs CAS via mocked psycopg ──────────────────────


def _install_fake_psycopg(monkeypatch, *, update_returns):
    """Mock psycopg so confirm_briefs runs against an in-memory fake.

    `update_returns` is the row the mocked cursor returns from the UPDATE …
    RETURNING; pass None to simulate a CAS miss (wrong starting status)."""
    from src import ui_decisions

    class _FakeCursor:
        def __init__(self):
            self._next = update_returns
            self.executed: list[tuple[str, tuple]] = []
        def execute(self, sql, params=()):
            self.executed.append((sql, params))
        def fetchone(self):
            return self._next
        def fetchall(self):
            return [self._next] if self._next else []
        def __enter__(self): return self
        def __exit__(self, *a): return False

    cursor_instance = _FakeCursor()

    class _FakeConn:
        def cursor(self): return cursor_instance
        def commit(self): self.committed = True
        def rollback(self): self.rolled_back = True
        def __enter__(self): return self
        def __exit__(self, *a): return False

    monkeypatch.setattr(ui_decisions, "_connect", lambda: _FakeConn())
    return cursor_instance


def test_confirm_briefs_returns_decision_on_successful_cas(monkeypatch):
    # Simulate UPDATE … RETURNING returning a row → CAS succeeded.
    from datetime import datetime, timezone
    row = (
        "GMR-CAS",                  # ramp_id
        "awaiting_approval",         # status::text
        ["linkedin"],                # channels
        {"linkedin": 5000},          # budgets jsonb (psycopg returns dict)
        None,                        # decided_by
        None,                        # decided_at
        2,                           # version
        "GenAI Ops",                 # matched_domain
        "Test User",                 # requester_name
        "summary",                   # summary
        datetime.now(timezone.utc),  # submitted_at
    )
    cursor = _install_fake_psycopg(monkeypatch, update_returns=row)

    from src.ui_decisions import confirm_briefs
    decision = confirm_briefs("GMR-CAS", by_user="reviewer@scale.com")

    assert decision is not None
    assert decision.ramp_id == "GMR-CAS"
    assert decision.status == "awaiting_approval"
    # Should have executed UPDATE + INSERT (audit log).
    assert len(cursor.executed) == 2
    update_sql = cursor.executed[0][0]
    assert "UPDATE ramp_decisions" in update_sql
    assert "awaiting_approval" in update_sql
    assert "awaiting_brief_review" in update_sql
    audit_sql = cursor.executed[1][0]
    assert "ramp_audit_log" in audit_sql
    assert "briefs_confirmed" in audit_sql


def test_confirm_briefs_returns_none_when_wrong_status(monkeypatch):
    cursor = _install_fake_psycopg(monkeypatch, update_returns=None)

    from src.ui_decisions import confirm_briefs
    decision = confirm_briefs("GMR-WRONG-STATE", by_user="reviewer@scale.com")

    assert decision is None
    # Should NOT have written an audit row when CAS missed.
    audit_writes = [sql for sql, _ in cursor.executed if "ramp_audit_log" in sql]
    assert audit_writes == []

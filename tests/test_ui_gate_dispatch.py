"""Tests for the outlier-campaign-console approval gate dispatch in
`scripts/smart_ramp_poller.run_ramp_pipeline`. Pins:

- UI_GATE_ENABLED=False: legacy path; calls run_launch_for_ramp directly.
- UI_GATE_ENABLED=True, no Postgres: fail-closed (returns ok=False, no launch).
- UI_GATE_ENABLED=True, no decision row: _prep_ramp + upsert awaiting_approval.
- UI_GATE_ENABLED=True, status='awaiting_approval': skip silently.
- UI_GATE_ENABLED=True, status='approved': claim_ramp + _launch_ramp + complete.
- UI_GATE_ENABLED=True, dry_run=True: still bypasses the gate.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)


def _fake_record(ramp_id: str = "GMR-TEST") -> SimpleNamespace:
    return SimpleNamespace(
        id=ramp_id,
        project_id="",
        project_name=None,
        requester_name="Test Requester",
        summary="test summary",
        submitted_at="2026-05-19T00:00:00Z",
        updated_at="",
        status="submitted",
        linear_issue_id=None,
        linear_url=None,
        cohorts=[],
    )


def _no_op_pipeline_result() -> dict:
    return {
        "ok": True,
        "campaign_groups": [], "inmail_campaigns": [], "static_campaigns": [],
        "creative_paths": {}, "per_cohort": [],
    }


# ── UI_GATE_ENABLED=False: legacy path ───────────────────────────────────────

def test_gate_off_calls_run_launch_directly(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", False)
    import main
    fake_run = MagicMock(return_value=_no_op_pipeline_result())
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record(), dry_run=False, version=1)

    assert out["ok"] is True
    fake_run.assert_called_once()
    # Modes should be the default 2-tuple
    call_kwargs = fake_run.call_args.kwargs
    assert call_kwargs.get("modes") == ("inmail", "static")
    assert call_kwargs.get("dry_run") is False


# ── UI_GATE_ENABLED=True + dry_run bypasses gate ─────────────────────────────

def test_gate_on_dry_run_skips_postgres(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    fake_run = MagicMock(return_value=_no_op_pipeline_result())
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)

    # If the gate tried to consult Postgres, this would blow up.
    monkeypatch.setenv("DATABASE_URL", "")

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record(), dry_run=True, version=1)

    assert out["ok"] is True
    fake_run.assert_called_once()
    assert fake_run.call_args.kwargs.get("dry_run") is True


# ── Gate ON + Postgres down → fail-closed ────────────────────────────────────

def test_gate_on_postgres_down_fails_closed(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    fake_run = MagicMock(return_value=_no_op_pipeline_result())
    fake_prep = MagicMock(return_value=_no_op_pipeline_result())
    fake_launch = MagicMock(return_value=_no_op_pipeline_result())
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)
    monkeypatch.setattr(main, "_prep_ramp", fake_prep)
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)

    from src import ui_decisions
    def boom(*a, **kw):
        raise ui_decisions.UIDecisionsUnavailable("DATABASE_URL not set")
    monkeypatch.setattr(ui_decisions, "get_decision", boom)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record(), dry_run=False, version=1)

    assert out["ok"] is False
    assert "ui_decisions unreachable" in out["error"]
    fake_run.assert_not_called()
    fake_prep.assert_not_called()
    fake_launch.assert_not_called()


# ── Gate ON + no decision → prep + upsert ────────────────────────────────────

def test_gate_on_no_decision_runs_prep_and_upserts(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    prep_result = {
        "ok": True, "prep_only": True,
        "cohorts_mined": [{"name": "skills__deep_learning"}],
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
    fake_upsert = MagicMock()
    monkeypatch.setattr(ui_decisions, "upsert_awaiting_approval", fake_upsert)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-NEW"), dry_run=False, version=1)

    assert out["ok"] is True
    assert out.get("prep_only") is True
    fake_prep.assert_called_once_with("GMR-NEW")
    fake_upsert.assert_called_once()
    fake_launch.assert_not_called()


# ── Gate ON + awaiting_approval → skip ───────────────────────────────────────

def test_gate_on_awaiting_approval_skips(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    fake_prep = MagicMock()
    fake_launch = MagicMock()
    monkeypatch.setattr(main, "_prep_ramp", fake_prep)
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)

    from src.ui_decisions import Decision
    decision = Decision(
        ramp_id="GMR-WAIT", status="awaiting_approval",
        channels=[], budgets={}, version=1,
    )
    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision",
                        MagicMock(return_value=decision))

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-WAIT"), dry_run=False, version=1)

    assert out["ok"] is True
    assert out["ui_gated"] is True
    assert out["status"] == "awaiting_approval"
    fake_prep.assert_not_called()
    fake_launch.assert_not_called()


# ── Gate ON + approved → claim + launch + complete ───────────────────────────

def test_gate_on_approved_claims_launches_completes(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    launch_result = {
        "ok": True,
        "campaign_groups": ["urn:li:sponsoredCampaignGroup:1"],
        "inmail_campaigns": ["urn:li:sponsoredCampaign:2"],
        "static_campaigns": ["urn:li:sponsoredCampaign:3"],
        "creative_paths": {}, "per_cohort": [],
    }
    fake_launch = MagicMock(return_value=launch_result)
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)

    from src.ui_decisions import Decision
    approved = Decision(
        ramp_id="GMR-GO", status="approved",
        channels=["linkedin"], budgets={"linkedin": 7500}, version=2,
    )
    claimed = Decision(
        ramp_id="GMR-GO", status="launching",
        channels=["linkedin"], budgets={"linkedin": 7500}, version=3,
    )
    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision",
                        MagicMock(return_value=approved))
    monkeypatch.setattr(ui_decisions, "claim_ramp",
                        MagicMock(return_value=claimed))
    fake_update_status = MagicMock()
    monkeypatch.setattr(ui_decisions, "update_status", fake_update_status)

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-GO"), dry_run=False, version=1)

    assert out["ok"] is True
    assert out["static_campaigns"] == ["urn:li:sponsoredCampaign:3"]
    fake_launch.assert_called_once()
    # Verify the decision was forwarded to _launch_ramp
    assert fake_launch.call_args.kwargs.get("decision") is claimed
    # Status flipped to completed
    fake_update_status.assert_called_once()
    assert fake_update_status.call_args.args == ("GMR-GO", "completed")


# ── Gate ON + approved but lost the claim race → skip ─────────────────────────

def test_gate_on_lost_claim_skips(monkeypatch):
    import config
    monkeypatch.setattr(config, "UI_GATE_ENABLED", True)
    import main
    fake_launch = MagicMock()
    monkeypatch.setattr(main, "_launch_ramp", fake_launch)

    from src.ui_decisions import Decision
    approved = Decision(ramp_id="GMR-LOST", status="approved",
                       channels=["linkedin"], budgets={"linkedin": 5000})
    from src import ui_decisions
    monkeypatch.setattr(ui_decisions, "get_decision",
                        MagicMock(return_value=approved))
    monkeypatch.setattr(ui_decisions, "claim_ramp", MagicMock(return_value=None))

    from scripts.smart_ramp_poller import run_ramp_pipeline
    out = run_ramp_pipeline(_fake_record("GMR-LOST"), dry_run=False, version=1)

    assert out["ok"] is True
    assert out["status"] == "claim_lost"
    fake_launch.assert_not_called()

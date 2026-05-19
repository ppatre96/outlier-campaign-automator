"""Phase 2 — per-ramp channels + budgets plumbing through run_launch_for_ramp.

Pins the contract that the outlier-campaign-console approval decision
(channels list + budgets dict) correctly drives:
  - which arms execute (linkedin / meta / google)
  - what daily_budget_cents each platform's create_campaign call receives

We stub _prep_ramp / _process_row_both_modes / _ramp_to_rows so the test
asserts on call-site kwargs rather than running the real pipeline.
"""
from __future__ import annotations

import os
import sys
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _PROJECT_ROOT)


def _fake_ramp_record(ramp_id: str = "GMR-TEST"):
    return SimpleNamespace(
        id=ramp_id, project_id="", project_name=None,
        requester_name="", summary="", submitted_at="", updated_at="",
        status="submitted", linear_issue_id=None, linear_url=None,
        cohorts=[],
    )


def test_launch_ramp_forwards_decision_channels_and_budgets(monkeypatch):
    """_launch_ramp(decision) must thread decision.channels + decision.budgets
    into run_launch_for_ramp so the lower layers can filter arms + set budgets."""
    import main
    fake_run = MagicMock(return_value={"ok": True})
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)

    from src.ui_decisions import Decision
    decision = Decision(
        ramp_id="GMR-X", status="launching",
        channels=["linkedin", "meta"],
        budgets={"linkedin": 7500, "meta": 12000},
    )

    main._launch_ramp("GMR-X", decision=decision)

    fake_run.assert_called_once()
    kwargs = fake_run.call_args.kwargs
    assert kwargs["channels"] == ["linkedin", "meta"]
    assert kwargs["budgets"] == {"linkedin": 7500, "meta": 12000}
    assert kwargs["prep_only"] is False


def test_launch_ramp_no_decision_passes_none(monkeypatch):
    """Calling _launch_ramp with no decision uses the legacy global defaults
    (channels=None, budgets=None) — env vars + per-client PLACEHOLDERs apply."""
    import main
    fake_run = MagicMock(return_value={"ok": True})
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)

    main._launch_ramp("GMR-Y", decision=None)

    kwargs = fake_run.call_args.kwargs
    assert kwargs["channels"] is None
    assert kwargs["budgets"] is None


def test_prep_ramp_passes_prep_only_true(monkeypatch):
    import main
    fake_run = MagicMock(return_value={"ok": True, "prep_only": True})
    monkeypatch.setattr(main, "run_launch_for_ramp", fake_run)

    main._prep_ramp("GMR-Z")

    kwargs = fake_run.call_args.kwargs
    assert kwargs["prep_only"] is True
    assert kwargs["dry_run"] is False


def test_run_launch_for_ramp_forwards_channels_and_budgets(monkeypatch):
    """run_launch_for_ramp(..., channels, budgets) must forward those values
    to _process_row_both_modes for every row."""
    import main

    # Stub the upstream client
    from src import smart_ramp_client
    fake_client = MagicMock()
    fake_client.fetch_ramp.return_value = _fake_ramp_record()
    monkeypatch.setattr(smart_ramp_client, "SmartRampClient",
                        MagicMock(return_value=fake_client))
    monkeypatch.setattr(main, "SmartRampClient",
                        MagicMock(return_value=fake_client))

    # Stub _ramp_to_rows to yield one synthetic row
    monkeypatch.setattr(main, "_ramp_to_rows", MagicMock(return_value=[{"unique_id": "row0"}]))

    # Stub SheetsClient + LinkedInClient + RedashClient + UrnResolver +
    # BrandVoiceValidator so we never hit external services
    fake_sheets = MagicMock()
    fake_sheets.read_config.return_value = {"LINKEDIN_TOKEN": "tok"}
    monkeypatch.setattr(main, "SheetsClient", MagicMock(return_value=fake_sheets))
    monkeypatch.setattr(main, "LinkedInClient", MagicMock())
    monkeypatch.setattr(main, "UrnResolver", MagicMock())
    monkeypatch.setattr(main, "RedashClient", MagicMock())
    monkeypatch.setattr(main, "BrandVoiceValidator", MagicMock())

    fake_process = MagicMock(return_value={"ok": True})
    monkeypatch.setattr(main, "_process_row_both_modes", fake_process)

    main.run_launch_for_ramp(
        "GMR-T",
        channels=["google"],
        budgets={"google": 8000},
    )

    fake_process.assert_called_once()
    kwargs = fake_process.call_args.kwargs
    assert kwargs["channels"] == ["google"]
    assert kwargs["budgets"] == {"google": 8000}

"""Unit tests for src/icp_drift_monitor — Phase 2.5 V2 (FEED-20, FEED-21).

Covers the four scenarios from RESEARCH-V2 Validation Architecture:
  - test_kl_synthetic_shift   (FEED-20)
  - test_cold_start           (FEED-20)
  - test_auto_trigger_on_drift (FEED-21)
  - test_rate_limit_7d        (FEED-21)

Plus a strict 7-day rate-limit boundary test (drift_state.json semantics) and
a verification that compute_drift stays finite even with novel categorical
values absent from the trailing baseline (Pitfall 4 fix).

All tests use tmp_path + monkeypatch — no writes to real data/ directories,
no live Redash calls, no live ReanalysisOrchestrator invocations.
"""
from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest


# ------------------------------------------------------------------
# FEED-20 — KL divergence on a known synthetic feature shift
# ------------------------------------------------------------------
def test_kl_synthetic_shift():
    """categorical_kl on known-shifted distributions produces expected scores."""
    from src.icp_drift_monitor import categorical_kl

    # Identical → ~0
    a = pd.Series(['x', 'y'] * 100)
    b = pd.Series(['x', 'y'] * 100)
    assert categorical_kl(a, b) < 1e-6

    # Totally disjoint → > 0.1 (epsilon keeps it finite — Pitfall 4 fix)
    c = pd.Series(['x'] * 100)
    d = pd.Series(['y'] * 100)
    kl_disjoint = categorical_kl(c, d)
    assert kl_disjoint > 0.1
    assert np.isfinite(kl_disjoint), "EPSILON must keep disjoint KL finite"

    # Partial shift: 80/20 vs 50/50 → moderate, between 0.01 and 2.0
    shifted = pd.Series(['x'] * 80 + ['y'] * 20)
    balanced = pd.Series(['x'] * 50 + ['y'] * 50)
    kl = categorical_kl(shifted, balanced)
    assert 0.01 < kl < 2.0, f"expected moderate KL for 80/20 vs 50/50, got {kl}"

    # Novel categorical value in this_week absent from baseline must stay finite
    novel = pd.Series(['x'] * 50 + ['z_NEW'] * 50)
    kl_novel = categorical_kl(novel, balanced)
    assert np.isfinite(kl_novel), "novel category must not produce inf/nan"


# ------------------------------------------------------------------
# FEED-20 — Cold-start (<2 snapshots) returns drift_score=None
# ------------------------------------------------------------------
def test_cold_start(tmp_path, monkeypatch):
    """compute_drift returns cold_start=True and drift_score=None when <2 snaps exist."""
    from src import icp_drift_monitor as idm

    monkeypatch.setattr(idm, "_SNAPSHOT_DIR", tmp_path / "snapshots")

    # No snapshots yet
    result = idm.compute_drift("proj_123")
    assert result["cold_start"] is True
    assert result["drift_score"] is None
    assert result["per_feature"] == {}

    # Single snapshot still cold-start
    snap_dir = tmp_path / "snapshots" / "proj_123"
    snap_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"worker_source": ["linkedin"] * 10}).to_parquet(
        snap_dir / "2026-04-17.parquet", index=False
    )
    result = idm.compute_drift("proj_123")
    assert result["cold_start"] is True
    assert result["drift_score"] is None

    # Cold-start path in check_and_trigger should NOT call orchestrator
    monkeypatch.setattr(idm, "_DRIFT_STATE", tmp_path / "drift_state.json")
    mock_orchestrator = MagicMock()

    def boom(**kwargs):
        pytest.fail("trigger_reanalysis must NOT fire during cold-start")

    mock_orchestrator.trigger_reanalysis = boom

    with patch("src.reanalysis_loop.ReanalysisOrchestrator", return_value=mock_orchestrator):
        out = idm.check_and_trigger("proj_123", force_snapshot=False)

    assert out["triggered"] is False
    assert out["trigger_reason"] == "cold_start"


# ------------------------------------------------------------------
# FEED-21 — Drift > threshold triggers reanalysis with reason="icp_drift"
# ------------------------------------------------------------------
def test_auto_trigger_on_drift(tmp_path, monkeypatch):
    """Drift exceeding threshold fires ReanalysisOrchestrator.trigger_reanalysis(reason='icp_drift')."""
    from src import icp_drift_monitor as idm
    import config

    monkeypatch.setattr(idm, "_SNAPSHOT_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(idm, "_DRIFT_STATE",  tmp_path / "drift_state.json")
    # Low threshold so any shift triggers; min_rows=1 to bypass the noise floor
    monkeypatch.setattr(config, "ICP_DRIFT_THRESHOLD", 0.01)
    monkeypatch.setattr(config, "ICP_DRIFT_MIN_ROWS",  1)

    pid = "proj_drift"
    base_dir = tmp_path / "snapshots" / pid
    base_dir.mkdir(parents=True, exist_ok=True)

    baseline_df = pd.DataFrame({
        "worker_source":         ["linkedin"] * 100,
        "resume_degree":         ["BS"] * 100,
        "resume_field":          ["cs"] * 100,
        "resume_job_title":      ["engineer"] * 100,
        "experience_band":       ["mid"] * 100,
        "total_payout_attempts": [10] * 100,
        "task_count_30d":        [20] * 100,
    })
    this_df = pd.DataFrame({
        "worker_source":         ["linkedin"] * 50 + ["facebook"] * 50,  # big shift
        "resume_degree":         ["BS"] * 100,
        "resume_field":          ["cs"] * 100,
        "resume_job_title":      ["engineer"] * 100,
        "experience_band":       ["mid"] * 100,
        "total_payout_attempts": [10] * 100,
        "task_count_30d":        [20] * 100,
    })
    baseline_df.to_parquet(base_dir / "2026-04-10.parquet", index=False)
    baseline_df.to_parquet(base_dir / "2026-04-17.parquet", index=False)
    this_df.to_parquet(base_dir / "2026-04-24.parquet", index=False)

    # Mock orchestrator (sync trigger to mirror current src/reanalysis_loop.py contract)
    captured: dict = {}
    mock_orchestrator = MagicMock()

    def fake_trigger(**kwargs):
        captured["kwargs"] = kwargs
        return []

    mock_orchestrator.trigger_reanalysis = fake_trigger

    with patch("src.reanalysis_loop.ReanalysisOrchestrator", return_value=mock_orchestrator):
        result = idm.check_and_trigger(pid, force_snapshot=False)

    assert result["triggered"] is True, f"expected trigger on drift; result: {result}"
    assert captured.get("kwargs", {}).get("reason") == "icp_drift", (
        f"reason kwarg must be 'icp_drift', got {captured!r}"
    )
    # State persistence: last_reanalysis_ts written to drift_state.json
    assert (tmp_path / "drift_state.json").exists()
    state = json.loads((tmp_path / "drift_state.json").read_text())
    assert pid in state
    assert "last_reanalysis_ts" in state[pid]
    assert "last_drift_score" in state[pid]


# ------------------------------------------------------------------
# FEED-21 — Rate limit: second trigger within 7 days is suppressed
# ------------------------------------------------------------------
def test_rate_limit_7d(tmp_path, monkeypatch):
    """Subsequent trigger within 7 days is rate-limited — orchestrator NOT called."""
    from src import icp_drift_monitor as idm
    import config

    monkeypatch.setattr(idm, "_SNAPSHOT_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(idm, "_DRIFT_STATE",  tmp_path / "drift_state.json")
    monkeypatch.setattr(config, "ICP_DRIFT_THRESHOLD", 0.01)
    monkeypatch.setattr(config, "ICP_DRIFT_MIN_ROWS",  1)

    pid = "proj_rl"
    # Pre-write drift state saying we triggered 2 days ago
    recent_ts = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    (tmp_path / "drift_state.json").write_text(json.dumps({
        pid: {"last_reanalysis_ts": recent_ts, "last_drift_score": 0.5}
    }))

    # Snapshots with drift
    base_dir = tmp_path / "snapshots" / pid
    base_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"worker_source": ["linkedin"] * 100}).to_parquet(
        base_dir / "2026-04-17.parquet", index=False
    )
    pd.DataFrame({"worker_source": ["facebook"] * 100}).to_parquet(
        base_dir / "2026-04-24.parquet", index=False
    )

    mock_orchestrator = MagicMock()

    def boom(**kwargs):
        pytest.fail("trigger_reanalysis must NOT be called when rate-limited")

    mock_orchestrator.trigger_reanalysis = boom

    with patch("src.reanalysis_loop.ReanalysisOrchestrator", return_value=mock_orchestrator):
        result = idm.check_and_trigger(pid, force_snapshot=False)

    assert result["rate_limited"] is True
    assert result["triggered"] is False
    assert result["trigger_reason"] == "rate_limited_7d"


# ------------------------------------------------------------------
# Boundary check — at exactly 7 days, the trigger SHOULD fire
# (strict `<` comparison contract from check_and_trigger docstring)
# ------------------------------------------------------------------
def test_rate_limit_boundary_at_7_days(tmp_path, monkeypatch):
    """A trigger exactly 7 days after the last one is allowed to re-fire (strict <)."""
    from src import icp_drift_monitor as idm
    import config

    monkeypatch.setattr(idm, "_SNAPSHOT_DIR", tmp_path / "snapshots")
    monkeypatch.setattr(idm, "_DRIFT_STATE",  tmp_path / "drift_state.json")
    monkeypatch.setattr(config, "ICP_DRIFT_THRESHOLD", 0.01)
    monkeypatch.setattr(config, "ICP_DRIFT_MIN_ROWS",  1)

    pid = "proj_boundary"
    # Pre-write drift state with last trigger exactly 7 days ago (+1 second to be safe)
    boundary_ts = (datetime.now(timezone.utc) - timedelta(days=7, seconds=1)).isoformat()
    (tmp_path / "drift_state.json").write_text(json.dumps({
        pid: {"last_reanalysis_ts": boundary_ts, "last_drift_score": 0.5}
    }))

    base_dir = tmp_path / "snapshots" / pid
    base_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({"worker_source": ["linkedin"] * 100}).to_parquet(
        base_dir / "2026-04-17.parquet", index=False
    )
    pd.DataFrame({"worker_source": ["facebook"] * 100}).to_parquet(
        base_dir / "2026-04-24.parquet", index=False
    )

    captured: dict = {}
    mock_orchestrator = MagicMock()

    def fake_trigger(**kwargs):
        captured["kwargs"] = kwargs
        return []

    mock_orchestrator.trigger_reanalysis = fake_trigger

    with patch("src.reanalysis_loop.ReanalysisOrchestrator", return_value=mock_orchestrator):
        result = idm.check_and_trigger(pid, force_snapshot=False)

    assert result["rate_limited"] is False
    assert result["triggered"] is True
    assert captured.get("kwargs", {}).get("reason") == "icp_drift"

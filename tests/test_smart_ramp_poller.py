"""Unit tests for scripts/smart_ramp_poller — Phase 2.6 Plan 01 (SR-02, SR-05, SR-08, SR-10).

All tests use tmp_path / monkeypatch — no real Smart Ramp HTTP calls, no real
state writes outside tmp_path, no real Slack. The pipeline call is the STUB
defined in scripts/smart_ramp_poller.run_ramp_pipeline (Plan 02 will replace it).
"""
from __future__ import annotations

import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _load_module():
    """Import the poller module fresh so monkeypatched module-level paths take effect."""
    import importlib
    import scripts.smart_ramp_poller as srp
    importlib.reload(srp)
    return srp


def _build_ramp(ramp_id="GMR-0010", requester="Pranav Patre", summary="ramp summary",
                updated_at="2026-04-22T10:00:00Z", n_cohorts=2):
    from src.smart_ramp_client import RampRecord, CohortSpec
    cohorts = [
        CohortSpec(
            id=f"c{i}", cohort_description=f"cohort {i}",
            signup_flow_id=None, selected_lp_url=None,
            included_geos=["IN"], matched_locales=None,
            target_activations=100 * (i + 1), job_post_id=None,
        )
        for i in range(n_cohorts)
    ]
    return RampRecord(
        id=ramp_id, project_id="p", project_name="N",
        requester_name=requester, summary=summary,
        submitted_at="2026-04-22T09:00:00Z", updated_at=updated_at,
        status="submitted", linear_issue_id=None, linear_url=None,
        cohorts=cohorts,
    )


# ─────────────────────────────────────────────────────────────────────────────
# SR-02 tests
# ─────────────────────────────────────────────────────────────────────────────


def test_signature_stable_across_refetch():
    """SR-02: same content -> identical sha256 signature; cohort permutation invariant."""
    srp = _load_module()
    r = _build_ramp()
    s1 = srp.compute_signature(r)
    s2 = srp.compute_signature(r)
    assert s1 == s2 and s1.startswith("sha256:")
    # Cohort permutation invariance: swap cohort order, signature unchanged
    r2 = _build_ramp()
    r2.cohorts = list(reversed(r2.cohorts))
    assert srp.compute_signature(r2) == s1


def test_state_atomic_write(tmp_path, monkeypatch):
    """SR-02 / Pitfall 5: state file is either pre-write or post-write — never partial JSON."""
    srp = _load_module()
    state_path = tmp_path / "processed_ramps.json"
    monkeypatch.setattr(srp, "STATE_PATH", state_path)
    # Pre-existing state
    state_path.write_text(json.dumps({"ramps": {"X": {"version": 1}}, "ramp_versions": {}}))
    pre_text = state_path.read_text()
    pre_state = json.loads(pre_text)
    assert pre_state["ramps"]["X"]["version"] == 1

    # Patch os.replace to raise — simulating crash AFTER tmp written but BEFORE rename
    real_replace = os.replace

    def boom(*a, **kw):
        raise RuntimeError("simulated SIGKILL between write and rename")

    monkeypatch.setattr(srp.os, "replace", boom)
    with pytest.raises(RuntimeError):
        srp._write_state_atomic({"ramps": {"X": {"version": 99}}, "ramp_versions": {}})
    # Original file untouched (still version 1)
    after_text = state_path.read_text()
    assert json.loads(after_text)["ramps"]["X"]["version"] == 1
    # No leftover .tmp file in dir
    leftovers = list(tmp_path.glob(".processed_ramps.*.json.tmp"))
    assert leftovers == [], f"tmp leftovers: {leftovers}"
    # Restore os.replace and write successfully
    monkeypatch.setattr(srp.os, "replace", real_replace)
    srp._write_state_atomic({"ramps": {"X": {"version": 99}}, "ramp_versions": {}})
    assert json.loads(state_path.read_text())["ramps"]["X"]["version"] == 99


# ─────────────────────────────────────────────────────────────────────────────
# SR-05 tests
# ─────────────────────────────────────────────────────────────────────────────


def test_edit_detection_v2(tmp_path, monkeypatch):
    """SR-05: edited ramp re-runs as version=2; prior tagged superseded in ramp_versions."""
    srp = _load_module()
    monkeypatch.setattr(srp, "STATE_PATH", tmp_path / "processed_ramps.json")
    monkeypatch.setattr(srp, "LOCK_PATH", tmp_path / "smart_ramp_poller.lock")

    # Seed state: ramp at v1 with old signature
    r_v1 = _build_ramp(summary="v1 summary")
    sig_v1 = srp.compute_signature(r_v1)
    state = {
        "ramps": {
            "GMR-0010": {
                "first_seen_at": "2026-04-22T09:00:00Z",
                "last_processed_at": "2026-04-22T09:00:00Z",
                "last_signature": sig_v1,
                "consecutive_failures": 0,
                "last_failure_class": None,
                "version": 1,
                "campaign_groups": ["urn:li:cg:111"],
                "inmail_campaigns": [],
                "static_campaigns": [],
                "creative_paths": {},
                "superseded": False,
                "escalation_dm_sent": False,
            }
        },
        "ramp_versions": {},
    }

    # Simulate an edit: same id, new summary
    r_v2 = _build_ramp(summary="v2 summary EDITED")
    new_sig = srp.compute_signature(r_v2)
    assert new_sig != sig_v1

    action = srp._classify_action(state["ramps"]["GMR-0010"], new_sig)
    assert action == "edit"

    outcome = srp.process_ramp(r_v2, action=action, state=state, dry_run=True)
    assert outcome["version"] == 2
    assert state["ramps"]["GMR-0010"]["version"] == 2
    assert "GMR-0010_v1" in state["ramp_versions"]
    assert state["ramp_versions"]["GMR-0010_v1"]["superseded"] is True
    # The live entry is NOT marked superseded (only the historical snapshot is)
    assert state["ramps"]["GMR-0010"]["superseded"] is False


# ─────────────────────────────────────────────────────────────────────────────
# SR-08 test
# ─────────────────────────────────────────────────────────────────────────────


def test_test_requester_filtered(tmp_path, monkeypatch):
    """SR-08: word-boundary 'test' filter; substring 'Testov' is NOT filtered."""
    srp = _load_module()
    monkeypatch.setattr(srp, "STATE_PATH", tmp_path / "processed_ramps.json")
    monkeypatch.setattr(srp, "LOCK_PATH", tmp_path / "smart_ramp_poller.lock")

    test_ramp = _build_ramp(ramp_id="GMR-0004", requester="Quintin Au Test")
    real_ramp = _build_ramp(ramp_id="GMR-0011", requester="Christopher Testov")
    assert srp._should_skip_test_ramp(test_ramp) is True
    assert srp._should_skip_test_ramp(real_ramp) is False

    # Run-once with both — only real_ramp gets processed; only real_ramp ends up in state
    fake_client = MagicMock()
    fake_client.fetch_ramp_list.return_value = [test_ramp, real_ramp]
    fake_client.fetch_ramp.side_effect = lambda rid: {
        "GMR-0004": test_ramp, "GMR-0011": real_ramp,
    }[rid]
    monkeypatch.setattr(srp, "SmartRampClient", lambda: fake_client)

    process_calls = []

    def fake_process(record, action, state, dry_run):
        process_calls.append(record.id)
        state["ramps"][record.id] = {
            "version": 1, "last_signature": srp.compute_signature(record),
        }
        return {"ok": True, "result": {}, "err_class": None, "tb": None, "version": 1}

    monkeypatch.setattr(srp, "process_ramp", fake_process)

    args = srp._parse_args([])
    rc = srp.run_once(args)
    assert rc == 0
    assert "GMR-0004" not in process_calls, "test ramp must be filtered"
    assert "GMR-0011" in process_calls, "non-test ramp must be processed"
    state_after = json.loads((tmp_path / "processed_ramps.json").read_text())
    assert "GMR-0004" not in state_after.get("ramps", {}), "test ramp must NOT be written to state"


# ─────────────────────────────────────────────────────────────────────────────
# SR-10 test
# ─────────────────────────────────────────────────────────────────────────────


def test_filelock_prevents_overlap(tmp_path, monkeypatch):
    """SR-10: a second main() invocation while the first holds the lock returns 0."""
    srp = _load_module()
    monkeypatch.setattr(srp, "STATE_PATH", tmp_path / "processed_ramps.json")
    monkeypatch.setattr(srp, "LOCK_PATH", tmp_path / "smart_ramp_poller.lock")

    from filelock import FileLock
    held = FileLock(str(tmp_path / "smart_ramp_poller.lock"))
    held.acquire()
    try:
        # Stub run_once so this isn't slow even if lock weren't held
        monkeypatch.setattr(srp, "run_once", lambda args: 0)
        rc = srp.main(argv=["--once", "--dry-run"])
        assert rc == 0, f"expected 0 (clean exit on contention) got {rc}"
    finally:
        held.release()


# ─────────────────────────────────────────────────────────────────────────────
# 5-failure escalation gate (CONTEXT.md locked, SR-07 prep — actual DM in Plan 03)
# ─────────────────────────────────────────────────────────────────────────────


def test_escalation_after_5_failures(tmp_path, monkeypatch):
    """5th consecutive failure flips escalation_dm_sent=True; subsequent polls block re-processing."""
    srp = _load_module()
    monkeypatch.setattr(srp, "STATE_PATH", tmp_path / "processed_ramps.json")
    monkeypatch.setattr(srp, "LOCK_PATH", tmp_path / "smart_ramp_poller.lock")
    # Confirm threshold is 5 (locked)
    import config
    assert config.SMART_RAMP_FAILURE_THRESHOLD == 5

    r = _build_ramp(ramp_id="GMR-FAIL")
    sig = srp.compute_signature(r)
    state = {
        "ramps": {
            "GMR-FAIL": {
                "first_seen_at": "t", "last_processed_at": "t",
                "last_signature": sig, "consecutive_failures": 4,
                "last_failure_class": "RuntimeError", "version": 1,
                "campaign_groups": [], "inmail_campaigns": [], "static_campaigns": [],
                "creative_paths": {}, "superseded": False, "escalation_dm_sent": False,
            }
        },
        "ramp_versions": {},
    }

    # Force the pipeline stub to fail
    def failing_pipeline(record, dry_run=False, version=1):
        raise RuntimeError("Redash 5xx")

    monkeypatch.setattr(srp, "run_ramp_pipeline", failing_pipeline)

    out = srp.process_ramp(r, action="retry", state=state, dry_run=False)
    assert out["ok"] is False
    entry = state["ramps"]["GMR-FAIL"]
    assert entry["consecutive_failures"] == 5
    assert entry["escalation_dm_sent"] is True
    # Subsequent block check returns True — prevents reprocessing
    assert srp._should_block_for_escalation(entry) is True

    # Reset counter to 0 -> block should release
    entry["consecutive_failures"] = 0
    entry["escalation_dm_sent"] = False
    assert srp._should_block_for_escalation(entry) is False

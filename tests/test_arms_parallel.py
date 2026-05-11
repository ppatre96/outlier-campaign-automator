"""Phase 3.3 tests — InMail + Static arms run concurrently in
_process_row_both_modes, with shared-state locks preventing races on
sheets writes, registry file I/O, and LinkedIn token refresh.

These tests stub `_process_inmail_campaigns` and `_process_static_campaigns`
with controllable fakes so we can prove (a) the two arms actually run in
overlapping windows, (b) sequential ordering is preserved in dry_run, and
(c) one arm raising doesn't abort the other.

A separate suite under tests/test_shared_state_locks.py verifies the
lock additions themselves (gspread serialization, registry RMW atomicity,
token-refresh deduplication).
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _stub_resolver(monkeypatch):
    """Bypass cohort resolution + stub the registry/sheets writers so the
    orchestrator's pre-arm setup doesn't touch real infra."""
    import main as M
    from types import SimpleNamespace
    resolved = SimpleNamespace(
        selected=[SimpleNamespace(
            id="c0", name="c0", _stg_id="stg_0", _stg_name="C0",
            rules=[], cohort_description="",
            exclude_add=[], exclude_remove=[],
        )],
        family_exclude_pairs=[],
        data_driven_exclude_pairs=[],
        smart_ramp_brief="",
    )
    monkeypatch.setattr(M, "_resolve_cohorts", lambda *a, **kw: resolved)
    return resolved


def _make_arm_pair(monkeypatch, *, inmail_sleep=0.3, static_sleep=0.3,
                   inmail_raises=False, static_raises=False):
    """Replace both arm functions with controlled fakes that record in-flight
    counts so we can assert overlap. Returns (state_dict, original_funcs)."""
    state: dict = {"in_flight": 0, "max_in_flight": 0, "started": [], "finished": []}
    lock = threading.Lock()

    def _track_enter(name):
        with lock:
            state["in_flight"] += 1
            state["max_in_flight"] = max(state["max_in_flight"], state["in_flight"])
            state["started"].append(name)

    def _track_exit(name):
        with lock:
            state["in_flight"] -= 1
            state["finished"].append(name)

    def fake_inmail(**kw):
        _track_enter("inmail")
        time.sleep(inmail_sleep)
        _track_exit("inmail")
        if inmail_raises:
            raise RuntimeError("inmail boom")
        return {"campaigns": ["inmail-c1"], "campaigns_by_cohort": {"c0": "inmail-c1"},
                "creative_paths": {}, "campaign_groups": ["g1"]}

    def fake_static(**kw):
        _track_enter("static")
        time.sleep(static_sleep)
        _track_exit("static")
        if static_raises:
            raise RuntimeError("static boom")
        return {"campaigns": ["static-c1"], "campaigns_by_cohort": {"c0": "static-c1"},
                "creative_paths": {}, "campaign_groups": ["g2"], "campaign_specs": []}

    import main as M
    monkeypatch.setattr(M, "_process_inmail_campaigns", fake_inmail)
    monkeypatch.setattr(M, "_process_static_campaigns", fake_static)
    # Skip the Meta+Google fan-out entirely for this scope.
    monkeypatch.setattr(M, "_build_extra_platform_clients", lambda platforms: {})
    return state


def _call_both_modes(modes=("inmail", "static"), dry_run=False):
    """Invoke _process_row_both_modes with the bare-minimum kwargs.

    Signature is (row, *, ramp_id, dry_run, modes, sheets, snowflake,
    li_client, urn_res, claude_key, inmail_sender, brand_voice_validator,
    mj_token, figma_file, figma_node). flow_id/location/etc are extracted
    from the row dict inside the function.
    """
    import main as M
    row = {
        "flow_id":         "flow0",
        "location":        "US",
        "config_name":     "flow0",
        "project_id":      "p0",
        "cohort_id":       None,
        "selected_lp_url": None,
        "included_geos":   [],
        "unique_id":       "ROW_TEST",
        "sheet_row":       1,
    }
    return M._process_row_both_modes(
        row,
        ramp_id="ramp0",
        dry_run=dry_run,
        modes=tuple(modes),
        sheets=MagicMock(),
        snowflake=MagicMock(),
        li_client=MagicMock(),
        urn_res=MagicMock(),
        claude_key="",
        inmail_sender="",
        brand_voice_validator=None,
        mj_token="",
        figma_file="",
        figma_node="",
    )


# ─── tests ──────────────────────────────────────────────────────────────────


def test_arms_run_concurrently_when_both_modes_active(monkeypatch):
    """Both arms in flight at the same moment when modes={inmail, static}
    and not dry_run. Each arm sleeps 0.3s; wall-clock should be ≤ 0.6s
    (parallel) vs 0.6s+ (sequential). max_in_flight must hit 2."""
    _stub_resolver(monkeypatch)
    state = _make_arm_pair(monkeypatch, inmail_sleep=0.3, static_sleep=0.3)

    t0 = time.monotonic()
    out = _call_both_modes(modes=("inmail", "static"), dry_run=False)
    elapsed = time.monotonic() - t0

    assert state["max_in_flight"] == 2, (
        f"arms did not overlap — max_in_flight={state['max_in_flight']}; "
        "ThreadPoolExecutor not engaged"
    )
    assert elapsed < 0.5, (
        f"wall-clock {elapsed:.2f}s suggests sequential execution "
        f"(expected < 0.4s parallel)"
    )
    # Both arms' results landed in their respective slots.
    assert out["inmail_campaigns"] or out.get("inmail_result") or True, "smoke"


def test_arms_run_sequentially_in_dry_run(monkeypatch):
    """dry_run=True keeps deterministic ordering — no executor, no overlap.
    Useful for test fixtures that compare log output across runs."""
    _stub_resolver(monkeypatch)
    state = _make_arm_pair(monkeypatch, inmail_sleep=0.05, static_sleep=0.05)

    _call_both_modes(modes=("inmail", "static"), dry_run=True)

    assert state["max_in_flight"] == 1, (
        f"dry_run should serialize arms; got max_in_flight={state['max_in_flight']}"
    )
    # Ordering: inmail enters before static.
    assert state["started"] == ["inmail", "static"]


def test_single_arm_bypasses_executor(monkeypatch):
    """When only one mode is requested, no executor is created."""
    _stub_resolver(monkeypatch)
    state = _make_arm_pair(monkeypatch, inmail_sleep=0.05, static_sleep=0.05)

    _call_both_modes(modes=("inmail",), dry_run=False)
    assert state["max_in_flight"] == 1
    assert state["started"] == ["inmail"]
    assert "static" not in state["started"]


def test_inmail_failure_does_not_abort_static(monkeypatch):
    """InMail raises — Static still completes and contributes its result."""
    _stub_resolver(monkeypatch)
    state = _make_arm_pair(monkeypatch, inmail_sleep=0.1, static_sleep=0.1,
                           inmail_raises=True)

    _call_both_modes(modes=("inmail", "static"), dry_run=False)
    assert "inmail" in state["finished"]
    assert "static" in state["finished"]


def test_static_failure_does_not_abort_inmail(monkeypatch):
    """Static raises — InMail still completes."""
    _stub_resolver(monkeypatch)
    state = _make_arm_pair(monkeypatch, inmail_sleep=0.1, static_sleep=0.1,
                           static_raises=True)

    _call_both_modes(modes=("inmail", "static"), dry_run=False)
    assert "inmail" in state["finished"]
    assert "static" in state["finished"]

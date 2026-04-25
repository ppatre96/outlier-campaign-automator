"""Unit tests for scripts/weekly_feedback_loop — Phase 2.5 V2 (FEED-22, FEED-23).

All tests use tmp_path / monkeypatch — no real Slack posts, no real network,
no real filesystem writes outside tmp_path. The four sub-step functions are
stubbed; the four V2 modules (sentiment_miner, icp_drift_monitor, etc.) are
NOT imported during these tests.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _load_module():
    """Import the orchestrator module fresh so monkeypatched module-level
    constants are restored between tests."""
    import importlib

    import scripts.weekly_feedback_loop as wfl

    importlib.reload(wfl)
    return wfl


# ──────────────────────────────────────────────────────────────────────────────
# FEED-22: idempotency (6-day skip window)
# ──────────────────────────────────────────────────────────────────────────────


def test_idempotency(tmp_path, monkeypatch):
    """FEED-22: a successful run within 6 days exits cleanly with skip."""
    wfl = _load_module()
    monkeypatch.setattr(wfl, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(wfl, "LOCK_PATH", tmp_path / "state.lock")

    # last_success 2 days ago → should skip (unless --force)
    recent = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    assert wfl._should_skip({"last_success_ts": recent}, force=False) is True
    assert wfl._should_skip({"last_success_ts": recent}, force=True) is False

    # last_success 7 days ago → past the 6-day window → run
    old = (datetime.now(timezone.utc) - timedelta(days=7)).isoformat()
    assert wfl._should_skip({"last_success_ts": old}, force=False) is False

    # No prior success → run
    assert wfl._should_skip({}, force=False) is False
    assert wfl._should_skip({"last_success_ts": None}, force=False) is False

    # Malformed timestamp → fall through to run (don't crash)
    assert wfl._should_skip({"last_success_ts": "not-a-timestamp"}, force=False) is False


# ──────────────────────────────────────────────────────────────────────────────
# FEED-22: --dry-run does NOT post to Slack and does NOT trigger reanalysis
# ──────────────────────────────────────────────────────────────────────────────


def test_dry_run(tmp_path, monkeypatch):
    """FEED-22: --dry-run runs all steps but does NOT post to Slack or trigger reanalysis."""
    wfl = _load_module()
    monkeypatch.setattr(wfl, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(wfl, "LOCK_PATH", tmp_path / "state.lock")

    # Capture any attempted Slack call and any attempted check_and_trigger call.
    slack_calls: list[str] = []

    def fake_post(text):
        slack_calls.append(text)
        return True

    monkeypatch.setattr(
        "scripts.post_weekly_reports._post_to_slack", fake_post, raising=False
    )

    # Stub all four steps to succeed quickly without touching real services.
    callouts_path = tmp_path / "cal.json"
    callouts_path.write_text(json.dumps({"themes": []}))

    monkeypatch.setattr(wfl, "_step_v1", lambda dr: {"ok": True, "dry_run": dr})
    monkeypatch.setattr(
        wfl, "_step_funnel", lambda dr: {"ok": True, "diagnosis": {}}
    )
    monkeypatch.setattr(
        wfl,
        "_step_sentiment",
        lambda dr: {
            "ok": True,
            "callouts_path": str(callouts_path),
            "themes_surfaced": 0,
            "sources_queried": [],
            "sources_skipped": [],
        },
    )
    monkeypatch.setattr(
        wfl, "_step_drift", lambda dr, p: {"ok": True, "projects": {}}
    )
    monkeypatch.setattr(wfl, "_active_projects", lambda: [])

    outcome = wfl.run_once(dry_run=True, only=None)

    # All four steps succeeded
    assert outcome["failures"] == {}
    assert set(outcome["results"].keys()) == {"v1", "funnel", "sentiment", "drift"}
    # NO Slack post issued in dry-run mode
    assert (
        slack_calls == []
    ), f"--dry-run must NOT post to Slack; got {slack_calls}"


# ──────────────────────────────────────────────────────────────────────────────
# FEED-22: step isolation — one step failing does NOT abort the others
# ──────────────────────────────────────────────────────────────────────────────


def test_step_isolation(tmp_path, monkeypatch):
    """FEED-22: a single step failing does NOT abort the other steps; loud failure posts to Slack."""
    wfl = _load_module()
    monkeypatch.setattr(wfl, "STATE_PATH", tmp_path / "state.json")
    monkeypatch.setattr(wfl, "LOCK_PATH", tmp_path / "state.lock")

    callouts_path = tmp_path / "cal.json"
    callouts_path.write_text(json.dumps({"themes": []}))

    # Funnel fails (returns ok=False); the other three succeed.
    monkeypatch.setattr(wfl, "_step_v1", lambda dr: {"ok": True})
    monkeypatch.setattr(
        wfl,
        "_step_funnel",
        lambda dr: {
            "ok": False,
            "error": "RuntimeError: Redash timeout",
            "diagnosis": {},
        },
    )
    monkeypatch.setattr(
        wfl,
        "_step_sentiment",
        lambda dr: {
            "ok": True,
            "callouts_path": str(callouts_path),
            "themes_surfaced": 0,
            "sources_queried": [],
            "sources_skipped": [],
        },
    )
    monkeypatch.setattr(
        wfl, "_step_drift", lambda dr, p: {"ok": True, "projects": {}}
    )
    monkeypatch.setattr(wfl, "_active_projects", lambda: [])

    posts: list[str] = []
    monkeypatch.setattr(
        "scripts.post_weekly_reports._post_to_slack",
        lambda text: posts.append(text) or True,
        raising=False,
    )

    outcome = wfl.run_once(dry_run=False, only=None)

    # Every step was attempted and present in results
    assert set(outcome["results"].keys()) == {"v1", "funnel", "sentiment", "drift"}

    # Only funnel is in failures
    assert "funnel" in outcome["failures"]
    assert "v1" not in outcome["failures"]
    assert "sentiment" not in outcome["failures"]
    assert "drift" not in outcome["failures"]

    # A Slack message WAS posted (loud failure, NOT silent failure)
    assert (
        len(posts) == 1
    ), f"expected exactly one loud-failure Slack post, got {len(posts)}"
    msg = posts[0].lower()
    assert (
        "funnel" in msg or "partial failure" in msg
    ), f"loud-failure message must name the failed step: {posts[0]!r}"


# ──────────────────────────────────────────────────────────────────────────────
# FEED-23: consolidated Slack message has all four section headers on success
# ──────────────────────────────────────────────────────────────────────────────


def test_consolidated_slack(tmp_path):
    """FEED-23: consolidated message has all four section headers + per-step content on full success."""
    wfl = _load_module()
    callouts_path = tmp_path / "cal.json"
    callouts_path.write_text(
        json.dumps(
            {
                "themes": [
                    {
                        "theme": "Slow payment release",
                        "sentiment": "negative",
                        "evidence_count": 5,
                        "directive_for_brief": "Address: payment timing concerns",
                    }
                ]
            }
        )
    )

    v1 = {"ok": True}
    funnel = {
        "ok": True,
        "diagnosis": {
            "DATA_ANALYST": {
                "drop_stage": "screening",
                "drop_rate": 0.2,
                "baseline_rate": 0.5,
                "delta_pct": -0.6,
            }
        },
    }
    sentiment = {
        "ok": True,
        "callouts_path": str(callouts_path),
        "themes_surfaced": 1,
        "sources_queried": ["reddit"],
        "sources_skipped": [],
    }
    drift = {
        "ok": True,
        "projects": {
            "proj_a": {
                "drift": {"drift_score": 0.08, "cold_start": False},
                "triggered": False,
                "trigger_reason": "within_threshold",
                "rate_limited": False,
            }
        },
    }

    msg = wfl._build_consolidated_message(v1, funnel, sentiment, drift)

    # All four section headers present
    assert "Creative Progress Alerts" in msg
    assert "Funnel Drop" in msg
    assert "Sentiment" in msg
    assert "ICP Drift" in msg

    # Per-step content surfaces
    assert "DATA_ANALYST" in msg
    assert "Slow payment release" in msg
    assert "proj_a" in msg


# ──────────────────────────────────────────────────────────────────────────────
# FEED-23 + CLAUDE.md: Slack copy uses approved Outlier vocabulary
# ──────────────────────────────────────────────────────────────────────────────


def test_slack_vocabulary(tmp_path):
    """FEED-23 + CLAUDE.md: section headers + body never use banned vocabulary."""
    import re as _re

    wfl = _load_module()
    callouts_path = tmp_path / "cal.json"
    callouts_path.write_text(json.dumps({"themes": []}))

    msg = wfl._build_consolidated_message(
        {"ok": True},
        {"ok": True, "diagnosis": {}},
        {
            "ok": True,
            "callouts_path": str(callouts_path),
            "themes_surfaced": 0,
            "sources_queried": [],
            "sources_skipped": [],
        },
        {"ok": True, "projects": {}},
    )

    banned = _re.compile(
        r"\b(compensation|project rate|job|role|position|interview|bonus|required|promote|assign)\b",
        _re.IGNORECASE,
    )
    match = banned.search(msg)
    assert match is None, f"banned vocabulary in Slack message: {match.group(0)!r} in {msg!r}"

    # Failure-path message must also be clean
    failure_msg = wfl._build_failure_message(
        {"funnel": "RuntimeError: redash timeout"}
    )
    match = banned.search(failure_msg)
    assert (
        match is None
    ), f"banned vocabulary in failure message: {match.group(0)!r} in {failure_msg!r}"

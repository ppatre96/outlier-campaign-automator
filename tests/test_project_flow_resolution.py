"""project_id → signup_flow_id resolution: structural first, behavioural fallback.

`PROJECT_FLOW_LOOKUP_SQL` answers a BEHAVIOURAL question — "which signup flow
did the people who ended up on this project originally pass screening in?".
For a project staffed by existing CBs that returns their original funnel, not
the project's own. GMR-0029 (RLI OTS Artifact Collection — graphic design,
audio/music, video, animation, game dev, web dev) resolved to a LATAM-coder
flow that way. The wrong flow id then chose BOTH the job post that seeds the
ICP and the Stage 1 screening pool Stage A mines, so the ramp launched software
engineers.

The structural link — SIGNUPFLOWS.INTENDED_PROJECTS containing the project id —
gives `[Experts] Graphic Design OCP`, whose one job post is
`[Experimental] Artifact Acquisition` (DOMAIN='Design').

These tests pin the precedence and the fallbacks, with the Redash layer stubbed
so no query ever runs.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pandas as pd
import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.redash_db import RedashClient  # noqa: E402

_PROJECT = "6a5eb8b81c0db7c383c5a01b"

_STRUCTURAL_ROW = {
    "signup_flow_id": "6a69405b8a922f69582db8ea",
    "flow_name": "[Experts] Graphic Design OCP",
    "flow_active": True,
    "jobpost_id": "6a6bd423d021af6fede66c86",
    "job_name": "[Experimental] Artifact Acquisition",
    "domain": "Design",
    "config_name": "Artifact Acquisition Screening",
}
_BEHAVIOURAL_ROW = {
    "SIGNUP_FLOW_ID": "0000coderflow0000",
    "SIGNUP_FLOW_NAME": "Coding Expertise for AI Training Remote - LATAM Countries",
    "config_name": "Tier 2 Coders",
    "passes": 4321,
    "n": 9999,
}


def _client(monkeypatch, *, structural_rows, behavioural_rows, screenings=None):
    """RedashClient with _run_query routed by query label. Records call order."""
    client = RedashClient.__new__(RedashClient)
    calls: list[str] = []

    def fake_run_query(sql, label=""):
        calls.append(label)
        if label.startswith("proj-struct"):
            return pd.DataFrame(structural_rows)
        if label.startswith("proj-lookup"):
            return pd.DataFrame(behavioural_rows)
        if label.startswith("screenings"):
            flow = label.split("screenings-", 1)[-1]
            return pd.DataFrame((screenings or {}).get(flow, []))
        raise AssertionError(f"unexpected query label {label!r}")

    monkeypatch.setattr(client, "_run_query", fake_run_query, raising=False)
    client._calls = calls
    return client


def test_structural_wins_when_the_project_declares_a_flow(monkeypatch):
    client = _client(monkeypatch, structural_rows=[_STRUCTURAL_ROW],
                     behavioural_rows=[_BEHAVIOURAL_ROW])
    assert client.resolve_project_to_flow(_PROJECT) == (
        "6a69405b8a922f69582db8ea", "Artifact Acquisition Screening",
    )
    # The behavioural query must not even run when structural answered.
    assert not any(c.startswith("proj-lookup") for c in client._calls)


def test_falls_back_to_behavioural_when_no_flow_declares_the_project(monkeypatch):
    client = _client(monkeypatch, structural_rows=[], behavioural_rows=[_BEHAVIOURAL_ROW])
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")
    assert any(c.startswith("proj-struct") for c in client._calls)
    assert any(c.startswith("proj-lookup") for c in client._calls)


def test_structural_without_a_screening_config_is_not_usable(monkeypatch):
    """RESUME_SQL filters on flow AND config — a flow with no config pulls nothing."""
    row = {**_STRUCTURAL_ROW, "config_name": ""}
    client = _client(monkeypatch, structural_rows=[row], behavioural_rows=[_BEHAVIOURAL_ROW])
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")


def test_structural_query_failure_degrades_to_behavioural(monkeypatch):
    """A schema drift on SIGNUPFLOWS must not take the whole ramp down."""
    client = RedashClient.__new__(RedashClient)

    def fake_run_query(sql, label=""):
        if label.startswith("proj-struct"):
            raise RuntimeError("no such column: INTENDED_PROJECTS")
        return pd.DataFrame([_BEHAVIOURAL_ROW])

    monkeypatch.setattr(client, "_run_query", fake_run_query, raising=False)
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")


def test_prefer_structural_false_keeps_legacy_behaviour(monkeypatch):
    client = _client(monkeypatch, structural_rows=[_STRUCTURAL_ROW],
                     behavioural_rows=[_BEHAVIOURAL_ROW])
    assert client.resolve_project_to_flow(_PROJECT, prefer_structural=False) == (
        "0000coderflow0000", "Tier 2 Coders",
    )
    assert not any(c.startswith("proj-struct") for c in client._calls)


def test_empty_structural_pool_falls_back_to_the_behavioural_flow(monkeypatch):
    """Structural flow is right but has no screening history yet — Stage A still
    needs a population, so take the behavioural pool rather than returning empty."""
    client = _client(
        monkeypatch,
        structural_rows=[_STRUCTURAL_ROW],
        behavioural_rows=[_BEHAVIOURAL_ROW],
        screenings={
            "6a69405b8a922f69582db8ea": [],
            "0000coderflow0000": [{"cb_id": "u1"}, {"cb_id": "u2"}],
        },
    )
    df, flow_id, config_name = client.fetch_screenings_by_project(_PROJECT)
    assert (flow_id, config_name) == ("0000coderflow0000", "Tier 2 Coders")
    assert len(df) == 2


def test_non_empty_structural_pool_is_kept(monkeypatch):
    client = _client(
        monkeypatch,
        structural_rows=[_STRUCTURAL_ROW],
        behavioural_rows=[_BEHAVIOURAL_ROW],
        screenings={"6a69405b8a922f69582db8ea": [{"cb_id": "designer1"}]},
    )
    df, flow_id, config_name = client.fetch_screenings_by_project(_PROJECT)
    assert flow_id == "6a69405b8a922f69582db8ea"
    assert len(df) == 1


def test_no_flow_anywhere_returns_empty(monkeypatch):
    client = _client(monkeypatch, structural_rows=[], behavioural_rows=[])
    df, flow_id, config_name = client.fetch_screenings_by_project(_PROJECT)
    assert df.empty and flow_id == "" and config_name == ""


@pytest.mark.parametrize("value", ["nan", "", None])
def test_blank_structural_flow_id_is_rejected(monkeypatch, value):
    row = {**_STRUCTURAL_ROW, "signup_flow_id": value}
    client = _client(monkeypatch, structural_rows=[row], behavioural_rows=[_BEHAVIOURAL_ROW])
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")

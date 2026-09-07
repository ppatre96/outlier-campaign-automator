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


def test_empty_structural_pool_returns_empty_for_cold_start(monkeypatch):
    """No on-requirement screening history means cold-start from the job post,
    NOT borrowing an unrelated population.

    GMR-0029's dry run showed why: with the coder pool substituted in, the
    graphic-design cohort mined `skills__api_design`, `accreditations_norm__
    design_patterns` and `skills__ant_design` — software-engineering homonyms
    of "design", which were the only "design" signals present in that pool.
    Cohorts that merely share vocabulary with the requirement are worse than no
    cohorts, because the names look plausible.
    """
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
    assert df.empty, "an off-requirement pool must not be substituted in"
    assert flow_id == "6a69405b8a922f69582db8ea"
    assert config_name == "Artifact Acquisition Screening"
    # The coder pool must never have been fetched.
    assert not any(c == "screenings-0000coderflow0000" for c in client._calls)


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


# ── confidence floor on the behavioural fallback ────────────────────────────


def test_weak_behavioural_evidence_declines_to_resolve(monkeypatch):
    """`passes` is the evidence for the pick, not the size of the pool it yields.

    GMR-0029 chose a flow on passes=2 and got a 72,524-row pool, so every
    downstream size guard (df_raw.empty, n_icp < 30) saw a healthy frame and the
    cold start never fired. Re-running the query days later returned a different
    winner (passes=5) — at those counts the ranking is noise, not a lookup.
    """
    weak = {**_BEHAVIOURAL_ROW, "passes": 2}
    client = _client(monkeypatch, structural_rows=[], behavioural_rows=[weak])
    monkeypatch.setattr("config.PROJECT_FLOW_MIN_PASSES", 25, raising=False)
    assert client.resolve_project_to_flow(_PROJECT) is None


def test_strong_behavioural_evidence_still_resolves(monkeypatch):
    strong = {**_BEHAVIOURAL_ROW, "passes": 264}
    client = _client(monkeypatch, structural_rows=[], behavioural_rows=[strong])
    monkeypatch.setattr("config.PROJECT_FLOW_MIN_PASSES", 25, raising=False)
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")


@pytest.mark.parametrize("passes", [0, 2, 8, 24])
def test_the_audited_weak_winners_all_decline(monkeypatch, passes):
    """The real winners below the cliff: GMR-0028 (0), 0029 (2), 0016/0013 (8)."""
    client = _client(monkeypatch, structural_rows=[],
                     behavioural_rows=[{**_BEHAVIOURAL_ROW, "passes": passes}])
    monkeypatch.setattr("config.PROJECT_FLOW_MIN_PASSES", 25, raising=False)
    assert client.resolve_project_to_flow(_PROJECT) is None


def test_floor_of_zero_disables_the_check(monkeypatch):
    """Escape hatch — PROJECT_FLOW_MIN_PASSES=0 restores the old behaviour."""
    client = _client(monkeypatch, structural_rows=[],
                     behavioural_rows=[{**_BEHAVIOURAL_ROW, "passes": 1}])
    monkeypatch.setattr("config.PROJECT_FLOW_MIN_PASSES", 0, raising=False)
    assert client.resolve_project_to_flow(_PROJECT) == ("0000coderflow0000", "Tier 2 Coders")


def test_structural_is_not_subject_to_the_passes_floor(monkeypatch):
    """A structural declaration is an assertion, not a vote — it needs no passes."""
    client = _client(monkeypatch, structural_rows=[_STRUCTURAL_ROW],
                     behavioural_rows=[{**_BEHAVIOURAL_ROW, "passes": 1}])
    monkeypatch.setattr("config.PROJECT_FLOW_MIN_PASSES", 25, raising=False)
    assert client.resolve_project_to_flow(_PROJECT)[0] == "6a69405b8a922f69582db8ea"


# ── explicit overrides for structurally orphaned projects ──────────────────


def test_override_wins_over_everything(monkeypatch):
    """The 4 projects no flow declares (GMR-0017/0016/0013/0009) need a pin."""
    client = _client(monkeypatch, structural_rows=[_STRUCTURAL_ROW],
                     behavioural_rows=[_BEHAVIOURAL_ROW])
    monkeypatch.setattr(
        "config.PROJECT_FLOW_OVERRIDES",
        {_PROJECT: ("pinned_flow", "Pinned Config")}, raising=False,
    )
    assert client.resolve_project_to_flow(_PROJECT) == ("pinned_flow", "Pinned Config")


def test_override_for_another_project_is_ignored(monkeypatch):
    client = _client(monkeypatch, structural_rows=[_STRUCTURAL_ROW],
                     behavioural_rows=[_BEHAVIOURAL_ROW])
    monkeypatch.setattr(
        "config.PROJECT_FLOW_OVERRIDES",
        {"some_other_project": ("pinned_flow", "Pinned Config")}, raising=False,
    )
    assert client.resolve_project_to_flow(_PROJECT)[0] == "6a69405b8a922f69582db8ea"


# ── the reconciliation tripwire ─────────────────────────────────────────────


def test_flow_mismatch_is_logged(monkeypatch, caplog):
    """The one check nobody had: is the flow we're mining this project's flow?

    NO for 9 of 20 ramps audited 2026-09-07 — GMR-0021 (US short-form video)
    mined an Italian pool, GMR-0024 (Blind Evals) mined Gulf Arabic. All
    produced plausible cohorts, so nobody looked.
    """
    import logging

    client = _client(
        monkeypatch,
        structural_rows=[_STRUCTURAL_ROW],
        behavioural_rows=[{**_BEHAVIOURAL_ROW, "passes": 36219}],
        screenings={"0000coderflow0000": [{"cb_id": "u1"}]},
    )
    # Force the behavioural path so the resolved flow differs from structural.
    monkeypatch.setattr(client, "resolve_project_to_flow",
                        lambda pid, **kw: ("0000coderflow0000", "Tier 2 Coders"),
                        raising=False)
    with caplog.at_level(logging.WARNING):
        client.fetch_screenings_by_project(_PROJECT)
    assert any("FLOW MISMATCH" in r.getMessage() for r in caplog.records), \
        "the mismatch must be surfaced"


def test_no_warning_when_flows_agree(monkeypatch, caplog):
    import logging

    client = _client(
        monkeypatch,
        structural_rows=[_STRUCTURAL_ROW],
        behavioural_rows=[_BEHAVIOURAL_ROW],
        screenings={"6a69405b8a922f69582db8ea": [{"cb_id": "designer1"}]},
    )
    with caplog.at_level(logging.WARNING):
        client.fetch_screenings_by_project(_PROJECT)
    assert not any("FLOW MISMATCH" in r.getMessage() for r in caplog.records)


def test_iac_is_the_only_pinned_orphan():
    """Of the 4 structurally orphaned projects, only IaC had evidence good
    enough to pin (job-post -> screening-config FK). The other three cold-start
    on purpose — a wrong pin trains a ramp on the wrong people, a cold start
    just targets more loosely. Guards against someone filling the gaps in with
    plausible-looking guesses."""
    import config as cfg

    assert cfg.PROJECT_FLOW_OVERRIDES == {
        "69cd7cbcbd5961a5dd02ebb1": ("69d7e8acc08f56f2b9a712e2", "IaC Experience Screening"),
    }
    for orphan in (
        "674521bd75e0c6357207f93d",   # Coding QA Technical Assessment V1 — inactive
        "697b72cae052640b8db3e22d",   # SWE Pilot 1 — disabled
        "69cf1a039ed66cc82e0fa8f3",   # EKG Multiple Choice V11 — sibling flow only
    ):
        assert orphan not in cfg.PROJECT_FLOW_OVERRIDES

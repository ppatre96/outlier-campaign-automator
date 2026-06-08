"""Frame-independent cold start (_resolve_cold_start_cohort).

When the screening frame is empty (brand-new / niche project, e.g. GMR-0024 BLV
accessibility), prep synthesizes targeted cohort(s) from the job-post/brief so
the console isn't empty. Multi-cohort (COLD_START_MULTI_COHORT) derives 1..N
specs with rules across every channel-usable prefix. Network/LLM mocked.
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as M
import config
import src.icp_from_jobpost as jp


def _patch(monkeypatch, *, cohorts=None, icp=None, multi=True, base_roles=None):
    monkeypatch.setattr(config, "COLD_START_MULTI_COHORT", multi)
    monkeypatch.setattr(jp, "resolve_job_post", lambda *a, **k: "")
    monkeypatch.setattr(jp, "derive_cohorts_from_job_post", lambda *a, **k: list(cohorts or []))
    monkeypatch.setattr(jp, "derive_icp_from_job_post", lambda *a, **k: dict(icp or {}))
    monkeypatch.setattr(jp, "extract_base_role_candidates", lambda **k: list(base_roles or []))
    monkeypatch.setattr(jp, "family_exclusions_for", lambda **k: [])


def _run(row=None):
    return M._resolve_cold_start_cohort(
        row if row is not None else {"ramp_summary": "we need contributors"},
        snowflake=MagicMock(), li_client=None, urn_res=None,
        project_id="p", flow_id="f", location="US",
    )


def test_multi_cohort_distinct_with_all_rule_prefixes(monkeypatch):
    _patch(monkeypatch, cohorts=[
        {"label": "Backend Engineers (US)", "required_skills": ["Python", "Go"],
         "job_titles": ["Software Engineer"], "fields_of_study": ["computer science"],
         "degrees": ["bachelors"], "geos": ["US"]},
        {"label": "Data Scientists (IN)", "required_skills": ["Pandas"],
         "job_titles": ["Data Scientist"], "fields_of_study": [], "degrees": ["masters"], "geos": ["IN"]},
    ])
    res = _run()
    assert len(res.selected) == 2
    assert {c.name for c in res.selected} == {"Backend Engineers (US)", "Data Scientists (IN)"}
    assert len({c._stg_id for c in res.selected}) == 2  # distinct ids
    c0 = next(c for c in res.selected if c.name.startswith("Backend"))
    rules = {r[0] for r in c0.rules}
    assert {"skills__python", "skills__go", "job_titles_norm__software_engineer",
            "fields_of_study__computer_science", "highest_degree_level__bachelors"} <= rules


def test_degrade_to_single_when_multi_empty(monkeypatch):
    # multi returns [] → single ICP fallback; flag ON → richer single cohort.
    _patch(monkeypatch, cohorts=[], icp={
        "derived_tg_label": "Statisticians", "required_skills": ["R"],
        "required_fields": ["statistics"], "required_degrees": ["PhD in Statistics"],
    })
    res = _run()
    assert len(res.selected) == 1
    rules = {r[0] for r in res.selected[0].rules}
    assert "skills__r" in rules
    assert "fields_of_study__statistics" in rules
    assert "highest_degree_level__phd" in rules  # normalized from "PhD in Statistics"


def test_flag_off_is_skills_only(monkeypatch):
    _patch(monkeypatch, cohorts=[], multi=False, icp={
        "derived_tg_label": "Statisticians", "required_skills": ["R"],
        "required_fields": ["statistics"], "required_degrees": ["PhD"],
    })
    res = _run()
    assert len(res.selected) == 1
    # legacy behavior: skills only, no fields/degrees/titles
    assert {r[0] for r in res.selected[0].rules} == {"skills__r"}


def test_degrade_to_empty_when_no_signal(monkeypatch):
    _patch(monkeypatch, cohorts=[], icp={})
    res = _run(row={})  # no brief, no job post → nothing targetable
    assert res.selected == []


def test_duplicate_labels_disambiguated(monkeypatch):
    _patch(monkeypatch, cohorts=[
        {"label": "Coders", "required_skills": ["Python"], "geos": ["US"]},
        {"label": "Coders", "required_skills": ["Java"], "geos": ["IN"]},
    ])
    res = _run()
    names = [c.name for c in res.selected]
    assert len(set(names)) == 2, f"labels must be disambiguated: {names}"
    assert "Coders" in names  # first keeps the clean label


def test_base_role_titles_folded_in(monkeypatch):
    # extract_base_role_candidates contributes title rules off the label.
    _patch(monkeypatch, cohorts=[{"label": "ML folks", "required_skills": ["pytorch"]}],
           base_roles=["ML Engineer", "Machine Learning Engineer"])
    res = _run()
    rules = {r[0] for r in res.selected[0].rules}
    assert "skills__pytorch" in rules
    assert "job_titles_norm__ml_engineer" in rules

"""Frame-independent cold start (_resolve_cold_start_cohort).

When the Snowflake screening frame is empty (brand-new / niche project, e.g.
GMR-0024 BLV accessibility), prep should still synthesize ONE cohort from the
job-post / brief ICP so the console isn't empty. Network/LLM mocked.
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as M
import src.icp_from_jobpost as jp


def test_cold_start_synthesizes_cohort_from_skills(monkeypatch):
    monkeypatch.setattr(jp, "resolve_job_post", lambda *a, **k: "")  # no snowflake post
    monkeypatch.setattr(jp, "derive_icp_from_job_post", lambda txt: {
        "derived_tg_label": "Backend Engineers (US)",
        "required_skills": ["Python", "SQL Databases", "Go or Java", "Backend Engineering", "Docker", "Kubernetes"],
    })
    row = {"ramp_summary": "need backend engineers in the US", "included_geos": ["US"]}  # no ramp_id → skip persistence

    res = M._resolve_cold_start_cohort(
        row, snowflake=MagicMock(), li_client=None, urn_res=None,
        project_id="p1", flow_id="f1", location="US",
    )

    assert len(res.selected) == 1
    c = res.selected[0]
    assert c.name == "Backend Engineers (US)"
    # capped at 5 skills, slugified to skills__ rules
    assert c.rules == [
        ("skills__python", 1),
        ("skills__sql_databases", 1),
        ("skills__go_or_java", 1),
        ("skills__backend_engineering", 1),
        ("skills__docker", 1),
    ]
    assert getattr(c, "_stg_id", None)
    assert res.flow_id == "f1" and res.project_id == "p1"


def test_cold_start_empty_when_no_jobpost_no_brief(monkeypatch):
    monkeypatch.setattr(jp, "resolve_job_post", lambda *a, **k: "")
    monkeypatch.setattr(jp, "derive_icp_from_job_post", lambda txt: {})
    row = {"included_geos": ["US"]}  # no brief, no post

    res = M._resolve_cold_start_cohort(
        row, snowflake=MagicMock(), li_client=None, urn_res=None,
        project_id="p1", flow_id="f1", location="US",
    )
    assert res.selected == []


def test_cold_start_uses_brief_label_when_icp_unlabeled(monkeypatch):
    # Job post yields skills but no label → fall back to the brief's first line.
    monkeypatch.setattr(jp, "resolve_job_post", lambda *a, **k: "")
    monkeypatch.setattr(jp, "derive_icp_from_job_post", lambda txt: {"required_skills": ["TalkBack", "Accessibility"]})
    row = {"ramp_summary": "Legally blind US TalkBack contributors for BLV eval\nmore detail"}

    res = M._resolve_cold_start_cohort(
        row, snowflake=MagicMock(), li_client=None, urn_res=None,
        project_id="p", flow_id="f", location="US",
    )
    assert len(res.selected) == 1
    assert res.selected[0].name == "Legally blind US TalkBack contributors for BLV eval"[:60]
    assert res.selected[0].rules == [("skills__talkback", 1), ("skills__accessibility", 1)]

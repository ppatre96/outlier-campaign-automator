"""Unit tests for the ICP sourcing-spec plumbing in src/icp_enrichment.py.

The LLM call itself isn't tested here — what's tested is everything that
decides WHAT the model sees and what survives its answer:

  - resume_evidence: WORKER_RESUME_SUMMARY stores titles/fields/companies as
    pipe-joined career histories. Counting them unsplit meant no title was ever
    "shared by k contributors", which is what left sizing ICPs vague.
  - the no-sample / LLM-failure path still returns a readable spec.
  - _to_requirements tolerates whatever shape the model returns.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.icp_enrichment import (  # noqa: E402
    _location_line, _to_requirements, enrich, resume_evidence,
)


class _FakeCohort:
    def __init__(self, name, rules):
        self.name = name
        self.rules = rules
        self.audience_size = None
        self.lift_pp = None
        self.pass_rate = None


def test_resume_evidence_splits_pipe_joined_career_history():
    rows = [
        {"resume_job_title": "Investment Advisor Representative | Data Analyst | Financial Advisor",
         "resume_field": "Business Administration | Economics",
         "resume_job_company": "Raymond James | Nasdaq",
         "resume_job_skills": "Audit | Research"},
        {"resume_job_title": "Seasonal Test Scorer | Financial Advisor",
         "resume_field": "Accounting",
         "resume_job_company": "H&R Block",
         "resume_job_skills": "Tax Preparation, Research"},
    ]
    ev = resume_evidence(rows)
    assert ev["n_contributors"] == 2
    counts = dict(ev["top_titles"])
    # The shared title is only visible once histories are split apart.
    assert counts["Financial Advisor"] == 2
    assert counts["Data Analyst"] == 1
    assert dict(ev["top_fields"])["Economics"] == 1
    assert dict(ev["top_companies"])["Raymond James"] == 1
    # Skills split on commas too (that column is comma or pipe separated).
    assert dict(ev["top_skills"])["Research"] == 2


def test_resume_evidence_counts_each_value_once_per_contributor():
    rows = [{"resume_job_title": "Financial Advisor | Financial Advisor | Analyst"}]
    assert dict(resume_evidence(rows)["top_titles"])["Financial Advisor"] == 1


def test_resume_evidence_keeps_commas_inside_titles():
    rows = [{"resume_job_title": "Director, Investor Relations"}]
    assert dict(resume_evidence(rows)["top_titles"]) == {"Director, Investor Relations": 1}


def test_location_line():
    assert _location_line(["US"]) == "United States only"
    assert _location_line(["IN", "PH"]) == "India and Philippines"
    assert _location_line([]) == ""
    assert _location_line(["US", "US"]) == "United States only"  # deduped


def test_enrich_without_sample_returns_readable_spec_from_rules():
    """No resume sample means no LLM call — the spec still has to be readable,
    because this is the cold-start and LLM-outage path."""
    cohort = _FakeCohort("Personal finance advisors", [
        ("skills__personal_tax", 1),
        ("job_titles_norm__financial_advisor", 1),
        ("fields_of_study__accounting", 1),
    ])
    icp = enrich(cohort, resume_sample=None, geos=["US"])
    assert icp.location == "United States only"
    assert icp.core_requirements
    assert "personal tax" in icp.core_requirements[0]["description"]
    assert icp.core_requirements[0]["titles"] == ["financial advisor"]
    assert any("accounting" in r["description"] for r in icp.core_requirements)


def test_to_requirements_coerces_loose_shapes():
    assert _to_requirements(None) == []
    assert _to_requirements("not a list") == []
    assert _to_requirements(["a bare string"]) == [
        {"name": "", "description": "a bare string", "titles": []}
    ]
    # Entries with neither name nor description are dropped; titles coerced.
    out = _to_requirements([
        {"name": "Personal tax", "description": "Prepared 1040 returns", "titles": ["CPA", 7]},
        {"name": "", "description": ""},
        {"titles": ["orphan"]},
    ])
    assert out == [{"name": "Personal tax", "description": "Prepared 1040 returns", "titles": ["CPA"]}]

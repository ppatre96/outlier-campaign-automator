"""Off-platform ramps must not inherit an unrelated job post's ICP.

GMR-0029 ("RLI OTS Artifact Collection", 2026-09-04) is off-platform: paid
traffic goes to an external artifact-submission form and the requester noted
"No Outlier job post". The pipeline still called fetch_job_post_meta(flow_id),
which resolves through `WHERE jp.signup_flow_id = '<flow>' LIMIT 1` and
returned an unrelated post — job_name "Coding Expertise for AI Training Remote
- LATAM Countries", domain "Tier 2 Coders".

That post then fed derive_icp_from_job_post (→ domain software_engineering,
geography LATAM, required_skills python/java/rust/swift/javascript/c++), the
Stage 1 brief-pool filter and base-role extraction, so all six creative-
professional cohorts (graphic design, audio/music, video, animation, game dev,
web dev) mined software-engineer segments and collapsed into one.

These tests pin the flag end to end: parsed off the Smart Ramp form, carried
onto every cohort row, and therefore available to the guard in _resolve_cohorts
and _process_row.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse_ramp(form_data: dict):
    """Call _parse_ramp without touching __init__ (no env / HTTP needed)."""
    from src.smart_ramp_client import SmartRampClient
    return SmartRampClient.__new__(SmartRampClient)._parse_ramp(
        {"id": "GMR-0029", "formData": form_data}
    )


@pytest.mark.parametrize(
    "value,expected",
    [
        ("off_platform", True),
        ("OFF_PLATFORM", True),
        (" off_platform ", True),
        ("outlier", False),
        ("", False),
        (None, False),
    ],
)
def test_off_platform_parsed_from_form(value, expected):
    ramp = _parse_ramp({"outlier_or_off_platform": value, "cohorts": []})
    assert ramp.off_platform is expected


def test_flag_absent_defaults_to_on_platform():
    """Older ramp payloads have no such field — they must keep job-post ICP."""
    assert _parse_ramp({"cohorts": []}).off_platform is False


def test_every_cohort_row_carries_the_flag():
    """_ramp_to_rows is what _resolve_cohorts / _process_row read the flag from."""
    from main import _ramp_to_rows

    cohort = SimpleNamespace(
        id="369d8f46", signup_flow_id="", cohort_description="Professional graphic designers",
        selected_lp_url="https://outlier.ai/experts/creators", included_geos=["US"],
        matched_locales=["en-us"], target_activations=9, matched_domain="Graphic Design",
    )
    ramp = SimpleNamespace(
        id="GMR-0029", project_id="6a5eb8b81c0db7c383c5a01b",
        project_name="RLI OTS Artifact Collection", summary="artifact collection",
        submitted_at="2026-08-24T20:41:51.010Z", linear_issue_id=None,
        cohorts=[cohort, cohort], off_platform=True,
    )
    rows = _ramp_to_rows(ramp)
    assert len(rows) == 2
    assert all(r["off_platform"] is True for r in rows)


def test_on_platform_ramp_rows_are_not_flagged():
    from main import _ramp_to_rows

    cohort = SimpleNamespace(
        id="c1", signup_flow_id="flow1", cohort_description="Cardiologists",
        selected_lp_url="", included_geos=["IN"], matched_locales=["en-in"],
        target_activations=5, matched_domain="Medical",
    )
    ramp = SimpleNamespace(
        id="GMR-0030", project_id="p1", project_name="P", summary="",
        submitted_at="", linear_issue_id=None, cohorts=[cohort], off_platform=False,
    )
    assert _ramp_to_rows(ramp)[0]["off_platform"] is False

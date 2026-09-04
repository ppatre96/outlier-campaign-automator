"""Stage A may only mine signals that relate to the requirement.

Stage A ranks whatever best predicts screening pass in the pool it is given.
That is the right question only when the pool is on-brief. GMR-0029 asked for
graphic designers, audio/music people, video producers and animators; the pool
was coder-heavy, so Stage A correctly reported `kubernetes + python`,
`objective-c + swift`, `graphql + java` — correct statistics, wrong audience.

`gate_features_to_jd` puts the job post + the Smart Ramp cohort's requirement
in charge of WHICH signals are admissible, and leaves Stage A in charge of
ranking them.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.jd_anchors import (  # noqa: E402
    build_jd_anchors,
    column_tokens,
    gate_features_to_jd,
    jd_relevant_columns,
)

# The real GMR-0029 mined columns, plus the design signals that should have won.
_GMR29_COLUMNS = [
    "skills__python",
    "skills__kubernetes",
    "skills__objective-c",
    "skills__swift",
    "skills__graphql",
    "skills__java",
    "job_titles_norm__senior_software_engineer",
    "job_titles_norm__software_engineer",
    "skills__graphic_design",
    "skills__2d_animation",
    "skills__3d_animation",
    "skills__video_editing",
    "fields_of_study__graphic_design",
    "job_titles_norm__motion_designer",
]

_GMR29_ICP = {
    "required_skills": ["graphic design", "illustration", "typography"],
    "derived_tg_label": "Professional graphic designers",
    "domain": "design",
}


def test_the_users_example_keeps_the_design_signal_and_drops_coding():
    """Requirement is graphic designer; activated CBs have coding AND a design
    internship in common. The design internship is the relevant one."""
    anchors = build_jd_anchors(matched_domain="Graphic Design")
    relevant, dropped = jd_relevant_columns(
        ["skills__python", "skills__graphic_design_internship"], anchors,
    )
    assert relevant == ["skills__graphic_design_internship"]
    assert dropped == ["skills__python"]


def test_gmr0029_feature_space_excludes_the_software_engineer_signals():
    kept = gate_features_to_jd(
        _GMR29_COLUMNS,
        derived_icp=_GMR29_ICP,
        matched_domain="Graphic Design",
        cohort_description="Professional graphic designers submitting portfolio artifacts",
        job_post_meta={"domain": "Design", "job_name": "[Experimental] Artifact Acquisition"},
    )
    for coder_col in [
        "skills__python", "skills__kubernetes", "skills__objective-c",
        "skills__swift", "skills__graphql", "skills__java",
        "job_titles_norm__senior_software_engineer", "job_titles_norm__software_engineer",
    ]:
        assert coder_col not in kept, f"{coder_col} survived the JD gate"
    for design_col in [
        "skills__graphic_design", "fields_of_study__graphic_design",
        "job_titles_norm__motion_designer",
    ]:
        assert design_col in kept, f"{design_col} was wrongly dropped"


def test_animation_cohort_keeps_animation_features():
    kept = gate_features_to_jd(
        _GMR29_COLUMNS,
        derived_icp={"required_skills": ["animation", "motion graphics"]},
        matched_domain="Design",
        cohort_description="Professional animation and motion graphics creatives",
    )
    assert "skills__2d_animation" in kept
    assert "skills__3d_animation" in kept
    assert "skills__python" not in kept


def test_a_coder_ramp_still_keeps_coder_features():
    """The gate is not anti-coding — it is pro-requirement."""
    kept = gate_features_to_jd(
        _GMR29_COLUMNS,
        derived_icp={"required_skills": ["python", "kubernetes"],
                     "derived_tg_label": "Software Engineers"},
        matched_domain="Coders",
        cohort_description="Senior backend software engineers",
    )
    assert "skills__python" in kept
    assert "skills__kubernetes" in kept
    assert "job_titles_norm__software_engineer" in kept
    assert "skills__graphic_design" not in kept


def test_morphological_variants_match():
    """designer/design, animations/animation must not fall through the gate."""
    anchors = build_jd_anchors(matched_domain="Design")
    relevant, _ = jd_relevant_columns(
        ["job_titles_norm__designer", "skills__designs", "skills__python"], anchors,
    )
    assert "job_titles_norm__designer" in relevant
    assert "skills__python" not in relevant


def test_icp_anchor_columns_are_never_dropped():
    """base_role_cols feed the ICP-fallback path — they must survive the gate."""
    kept = gate_features_to_jd(
        ["skills__python", "skills__graphic_design"],
        derived_icp={"required_skills": ["graphic design"]},
        matched_domain="Graphic Design",
        always_keep=["skills__python"],
    )
    assert kept == ["skills__python", "skills__graphic_design"]


def test_requirement_neutral_facets_survive():
    """Seniority and degree level are equally meaningful for any requirement —
    gating them on token overlap would drop every one of them."""
    kept = gate_features_to_jd(
        [
            "skills__python",
            "experience_band__5_10_years",
            "highest_degree_level__bachelors",
            "skills__graphic_design",
        ],
        derived_icp={"required_skills": ["graphic design"]},
        matched_domain="Graphic Design",
    )
    assert "experience_band__5_10_years" in kept
    assert "highest_degree_level__bachelors" in kept
    assert "skills__python" not in kept


def test_no_anchors_keeps_every_feature():
    """A requirement we can't read must not silently starve Stage A."""
    kept = gate_features_to_jd(_GMR29_COLUMNS, derived_icp={}, matched_domain="")
    assert kept == _GMR29_COLUMNS


def test_zero_matches_keeps_every_feature():
    """Nothing in the pool relates to the requirement — degrade loudly, don't
    hand Stage A an empty feature space."""
    kept = gate_features_to_jd(
        ["skills__python", "skills__kubernetes"],
        derived_icp={"required_skills": ["underwater basket weaving"]},
        matched_domain="Basketry",
    )
    assert kept == ["skills__python", "skills__kubernetes"]


def test_boilerplate_does_not_reopen_the_gate():
    """"Professional ... submitting ... collection" are in every brief; if they
    counted as anchors the gate would keep everything (the existing Stage 1
    brief filter takes that route and keeps ~73% of the pool)."""
    anchors = build_jd_anchors(
        cohort_description="Professional creatives submitting portfolio artifacts "
                           "for RLI OTS collection",
    )
    assert "professional" not in anchors
    assert "submitting" not in anchors
    assert "collection" not in anchors
    assert "portfolio" in anchors


@pytest.mark.parametrize(
    "col,expected",
    [
        ("skills__graphic_design", {"graphic", "design"}),
        ("job_titles_norm__senior_software_engineer", {"software", "engineer"}),
        ("fields_of_study__environmental_engineering", {"environmental", "engineering"}),
        # Seniority bands carry no lexical signal — they're exempted from the
        # gate by prefix instead (see test_requirement_neutral_facets_survive).
        ("experience_band__5_10_years", set()),
    ],
)
def test_column_tokens_strips_the_facet_prefix(col, expected):
    assert column_tokens(col) == expected

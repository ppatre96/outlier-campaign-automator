"""Regression tests for classify_tg and TG_* dicts — EXP-02."""
import pytest
from src.figma_creative import classify_tg, TG_PALETTES, TG_ILLUS_VARIANTS


@pytest.mark.parametrize("name, rules, expected", [
    # MATH — new bucket (EXP-02)
    ("statistician hiring pool", [], "MATH"),
    ("actuary talent pool", [], "MATH"),
    ("biostatistics research", [], "MATH"),
    ("mathematician cohort", [], "MATH"),
    ("probability theorist", [], "MATH"),
    ("econometrics phd", [], "MATH"),

    # MATH via rules field (verifies __ substitution path)
    ("cohort_42", [("skills__mathematics", 1)], "MATH"),
    ("cohort_7", [("fields_of_study__statistics", 1)], "MATH"),

    # ML_ENGINEER — unchanged
    ("ml engineer pool", [], "ML_ENGINEER"),
    ("pytorch researcher", [], "ML_ENGINEER"),

    # DATA_ANALYST — unchanged (note: 'data' wins over 'ml' by priority)
    ("data analyst", [], "DATA_ANALYST"),
    ("snowflake team", [], "DATA_ANALYST"),

    # MEDICAL — unchanged
    ("cardiology specialist", [], "MEDICAL"),
    ("clinical nurse", [], "MEDICAL"),

    # LANGUAGE — unchanged
    ("hindi translators", [], "LANGUAGE"),
    ("spanish linguist", [], "LANGUAGE"),

    # SOFTWARE_ENGINEER — unchanged
    ("backend developer", [], "SOFTWARE_ENGINEER"),
    ("devops engineer", [], "SOFTWARE_ENGINEER"),

    # GENERAL — fallback
    ("misc cohort", [], "GENERAL"),
    ("unmapped segment", [], "GENERAL"),
])
def test_classify_tg_bucket_returns(name, rules, expected):
    assert classify_tg(name, rules) == expected


def test_math_priority_beats_software_engineer():
    """When both math and python keywords are present, MATH must win (priority order)."""
    result = classify_tg("cohort", [("skills__mathematics", 1), ("skills__python", 1)])
    assert result == "MATH"


def test_math_priority_beats_software_engineer_via_name():
    """Even via cohort_name alone, math + python in the text should return MATH."""
    result = classify_tg("statistician who uses python", [])
    assert result == "MATH"


def test_tg_palettes_has_math():
    assert "MATH" in TG_PALETTES
    palette = TG_PALETTES["MATH"]
    assert isinstance(palette, list)
    assert len(palette) == 2
    for color in palette:
        assert set(color.keys()) == {"r", "g", "b"}
        for v in color.values():
            assert 0.0 <= v <= 1.0


def test_tg_illus_variants_has_math():
    assert "MATH" in TG_ILLUS_VARIANTS
    variants = TG_ILLUS_VARIANTS["MATH"]
    assert isinstance(variants, list)
    assert len(variants) == 3
    for v in variants:
        assert isinstance(v, str) and v


def test_all_classify_outputs_have_palette_and_illus():
    """Guardrail: every string returned by classify_tg must have a matching dict entry."""
    expected_buckets = {"DATA_ANALYST", "ML_ENGINEER", "MATH", "MEDICAL", "LANGUAGE", "SOFTWARE_ENGINEER", "GENERAL"}
    for bucket in expected_buckets:
        assert bucket in TG_PALETTES, f"{bucket} missing from TG_PALETTES"
        assert bucket in TG_ILLUS_VARIANTS, f"{bucket} missing from TG_ILLUS_VARIANTS"

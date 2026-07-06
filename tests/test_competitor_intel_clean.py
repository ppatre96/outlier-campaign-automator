"""Guards against the requester-ask leak that produced garbage experiment_ideas
(a raw cohort table showing up as an "Embed high-intent SEO terms" idea)."""

import pytest

from src.competitor_intel import _clean_tg_label, _is_clean_term


GARBAGE_SEED = (
    "I need 50 of each of the following user cohorts:\n"
    "- Cardiologists ($150) control - 4685750005\n"
    "- Cardiologists ($200) treatment - 4692313005\n"
    "- Cardiologists ($250) treatment - 4692314005"
)


def test_clean_tg_label_extracts_role_from_requester_ask():
    assert _clean_tg_label(GARBAGE_SEED) == "Cardiologists"


@pytest.mark.parametrize("label,expected", [
    ("Data Analysts", "Data Analysts"),
    ("clinical nurses Philippines", "clinical nurses Philippines"),
    ("", "general"),
    (None, "general"),
])
def test_clean_tg_label_passes_clean_labels(label, expected):
    assert _clean_tg_label(label) == expected


def test_clean_tg_label_caps_length():
    assert len(_clean_tg_label("alpha beta gamma delta epsilon zeta eta theta").split()) <= 6


@pytest.mark.parametrize("term", [
    "Cardiologists AI training jobs",
    "outlier AI review",
])
def test_is_clean_term_accepts_real_terms(term):
    assert _is_clean_term(term)


@pytest.mark.parametrize("term", [
    "I need 50 of each of the following user cohorts",
    "Cardiologists ($200) treatment - 4692313005",
    "$150 control",
    "x" * 61,
])
def test_is_clean_term_rejects_junk(term):
    assert not _is_clean_term(term)

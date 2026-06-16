"""
Bug 2 — generalist/i18n ramps target by locale instead of Stage A beam.

Covers detection, locale-cohort construction, the LinkedIn geo-only targeting
JSON, and the Meta/Google per-channel locale targeting. Network-free: the
resolvers make no API calls for a generalist cohort (no résumé-facet rules to
look up; Meta geo is local; Google geo passed empty).
"""
import json

import pytest

import main
from src.locales import get_locale, LOCALES


# ── Detection ────────────────────────────────────────────────────────────────

def test_detects_generalist_locale_cohorts():
    assert main.is_generalist_cohort(
        {"cohort_description": "Bengali generalist contributors", "matched_locales": ["bn-in"]}
    ) == "bn-in"
    assert main.is_generalist_cohort(
        {"cohort_description": "Simplified Chinese generalist contributors", "matched_locales": ["zh-cn"]}
    ) == "zh-cn"


def test_specialist_cohort_not_hijacked():
    # No "generalist"/"i18n" signal → None even with a known locale.
    assert main.is_generalist_cohort(
        {"cohort_description": "Senior Java Engineers", "matched_locales": ["de-de"]}
    ) is None
    assert main.is_generalist_cohort(
        {"cohort_description": "Pediatric Cardiologists", "matched_locales": ["en-us"]}
    ) is None


def test_generalist_word_but_unknown_locale_is_none():
    assert main.is_generalist_cohort(
        {"cohort_description": "generalist contributors", "matched_locales": ["xx-yy"]}
    ) is None


def test_empty_row():
    assert main.is_generalist_cohort({}) is None
    assert main.is_generalist_cohort(None) is None


# ── Cohort construction ──────────────────────────────────────────────────────

def test_build_locale_cohort_marker_and_rules():
    row = {"cohort_description": "Bengali generalist contributors", "matched_locales": ["bn-in"]}
    c = main._build_locale_cohort(row, "bn-in")
    assert c.name == "Bengali generalist contributors"
    assert c.rules == [("interface_locale", "bn-in")]
    assert c.facet_strength == {"generalist_locale": "bn-in"}


def test_targeting_json_is_geo_only_for_generalist():
    row = {"cohort_description": "German generalist contributors", "matched_locales": ["de-de"]}
    c = main._build_locale_cohort(row, "de-de")
    facet, criteria = main._cohort_to_targeting_json(c)
    assert facet == "generalist_locale"
    assert json.loads(criteria) == []  # no résumé facet URNs → LinkedIn geo-only


# ── Reference data ───────────────────────────────────────────────────────────

def test_all_locales_have_meta_and_google_ids():
    # The locale set grows as i18n ramps are added (13 → 17 and counting), so we
    # don't pin an exact count — the invariant is that EVERY registered locale
    # carries the per-channel ids + keywords the resolvers need.
    assert len(LOCALES) >= 13
    for code, lt in LOCALES.items():
        assert lt.meta_locale_id is not None, code
        assert lt.google_language_const is not None, code
        assert lt.generic_keywords, code


# ── Per-channel targeting ────────────────────────────────────────────────────

def test_meta_targeting_uses_locale_no_interests():
    from src.meta_targeting import MetaInterestResolver
    row = {"cohort_description": "Korean generalist contributors", "matched_locales": ["ko-kr"]}
    c = main._build_locale_cohort(row, "ko-kr")
    out = MetaInterestResolver().resolve_cohort(c, geos=["KR"])
    assert out["locales"] == [get_locale("ko-kr").meta_locale_id]  # 12
    assert out["geo_locations"]["countries"] == ["KR"]
    assert "flexible_spec" not in out  # no occupational interests for generalist


def test_google_targeting_uses_language_and_keywords():
    from src.google_targeting import GoogleSegmentResolver
    row = {"cohort_description": "Vietnamese generalist contributors", "matched_locales": ["vi-vn"]}
    c = main._build_locale_cohort(row, "vi-vn")
    out = GoogleSegmentResolver().resolve_cohort(c, geos=[])  # empty geos → no geo API
    assert out["language_constant"] == "languageConstants/1040"
    assert out["keyword_ideas"] == get_locale("vi-vn").generic_keywords

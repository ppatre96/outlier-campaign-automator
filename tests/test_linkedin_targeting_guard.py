"""Unit tests for the LinkedIn geo-only collapse guard.

Cold-start cohorts bypass Stage C, so a cohort whose LLM-coined skill/title
facets resolve to no LinkedIn URN would otherwise ship a country-wide
(geo-only) campaign. `linkedin_targeting_collapsed` detects that so the arm can
skip + route to a human. Generalist-locale cohorts (geo-targeted by design) and
rule-less cohorts must NOT be flagged.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.linkedin_targeting_guard import (
    linkedin_targeting_collapsed,
    has_facet_targeting_rules,
    is_generalist_locale,
)


class _Cohort:
    def __init__(self, rules, facet_strength=None):
        self.rules = rules
        self.facet_strength = facet_strength or {}


def test_collapsed_when_only_geo_resolves():
    """Cohort meant to target skills/titles but only profileLocations survived."""
    c = _Cohort([("skills__talkback_screen_reader", "talkback screen reader")])
    assert linkedin_targeting_collapsed(c, {"profileLocations": ["urn:li:geo:103644278"]}) is True


def test_not_collapsed_when_a_facet_resolves():
    c = _Cohort([("job_titles_norm__accessibility_specialist", "accessibility specialist")])
    facets = {"titles": ["urn:li:title:31415"], "profileLocations": ["urn:li:geo:103644278"]}
    assert linkedin_targeting_collapsed(c, facets) is False


def test_generalist_locale_is_exempt():
    """Generalist cohorts are geo(+language-skill) by design — geo-only is fine."""
    c = _Cohort([("interface_locale", "ko-kr")], {"generalist_locale": "ko-kr"})
    assert linkedin_targeting_collapsed(c, {"profileLocations": ["urn:li:geo:KR"]}) is False


def test_no_targeting_rules_is_not_a_collapse():
    """A cohort that never carried facet rules isn't 'collapsing' — nothing to lose."""
    c = _Cohort([("interface_locale", "en-us")])
    assert linkedin_targeting_collapsed(c, {"profileLocations": ["urn:li:geo:103644278"]}) is False


def test_empty_facets_with_targeting_rules_collapses():
    c = _Cohort([("skills__foo", "foo")])
    assert linkedin_targeting_collapsed(c, {}) is True


def test_helpers():
    assert has_facet_targeting_rules(_Cohort([("skills__x", "x")])) is True
    assert has_facet_targeting_rules(_Cohort([("interface_locale", "x")])) is False
    assert is_generalist_locale(_Cohort([], {"generalist_locale": "bn-in"})) is True
    assert is_generalist_locale(_Cohort([])) is False

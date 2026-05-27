"""Unit tests for the platform drop-rule functions extracted from main.py.

The de-narrow loop itself (denarrow_for_platform) is exercised indirectly via
the pipeline integration tests — these tests cover just the per-platform drop
rules in isolation, so changes to the drop order don't silently regress.
"""
from src.audience_check import drop_rule_for_google, drop_rule_for_meta


# ── Google ──────────────────────────────────────────────────────────────────


def test_google_drop_rule_pops_last_audience_segment():
    targeting = {
        "audience_segments": ["customers/1/userLists/A", "customers/1/userLists/B"],
        "geo_targets": ["geoTargetConstants/2840"],
        "keyword_ideas": ["python", "django"],
    }
    relaxed = drop_rule_for_google(targeting)
    assert relaxed is not None
    assert relaxed["audience_segments"] == ["customers/1/userLists/A"]
    # Other fields untouched
    assert relaxed["geo_targets"] == ["geoTargetConstants/2840"]
    assert relaxed["keyword_ideas"] == ["python", "django"]


def test_google_drop_rule_pops_last_geo_after_segments_exhausted():
    targeting = {
        "audience_segments": [],
        "geo_targets": ["geoTargetConstants/2840", "geoTargetConstants/2826"],
        "keyword_ideas": ["python"],
    }
    relaxed = drop_rule_for_google(targeting)
    assert relaxed is not None
    assert relaxed["geo_targets"] == ["geoTargetConstants/2840"]
    assert relaxed["audience_segments"] == []
    # Keywords intentionally NOT touched
    assert relaxed["keyword_ideas"] == ["python"]


def test_google_drop_rule_keeps_single_geo():
    """Single-geo cohorts should NOT be broadened to global — too aggressive."""
    targeting = {
        "audience_segments": [],
        "geo_targets": ["geoTargetConstants/2840"],
        "keyword_ideas": ["python"],
    }
    assert drop_rule_for_google(targeting) is None


def test_google_drop_rule_exhausted_returns_none():
    targeting = {
        "audience_segments": [],
        "geo_targets": [],
        "keyword_ideas": ["python"],
    }
    assert drop_rule_for_google(targeting) is None


def test_google_drop_rule_never_touches_keywords():
    """Removing keywords reduces Search reach — opposite of de-narrowing intent.

    Even after audience_segments and geo_targets are fully exhausted, the rule
    must return None rather than drop a keyword.
    """
    targeting = {
        "audience_segments": [],
        "geo_targets": [],
        "keyword_ideas": ["python", "django", "flask"],
        "keyword_volume_estimate": 5000,
    }
    assert drop_rule_for_google(targeting) is None


def test_google_drop_rule_does_not_mutate_input():
    """Drop rules are pure functions — callers reuse the original targeting."""
    targeting = {
        "audience_segments": ["A", "B"],
        "geo_targets": ["G1", "G2"],
    }
    original = {k: list(v) for k, v in targeting.items()}
    drop_rule_for_google(targeting)
    assert targeting == original


# ── Meta ────────────────────────────────────────────────────────────────────


def test_meta_drop_rule_pops_last_interest():
    targeting = {
        "flexible_spec": [{"interests": [{"id": "1"}, {"id": "2"}]}],
        "age_min": 25,
        "age_max": 65,
    }
    relaxed = drop_rule_for_meta(targeting)
    assert relaxed is not None
    assert relaxed["flexible_spec"] == [{"interests": [{"id": "1"}]}]
    # Age constraint untouched until interests fully exhausted
    assert relaxed["age_min"] == 25


def test_meta_drop_rule_removes_empty_flex_block():
    """When the last interest is dropped, the flex_spec entry itself drops out."""
    targeting = {
        "flexible_spec": [{"interests": [{"id": "1"}]}],
        "age_min": 25,
        "age_max": 65,
    }
    relaxed = drop_rule_for_meta(targeting)
    assert relaxed is not None
    assert relaxed["flexible_spec"] == []
    # Age still intact — flex emptying is separate from age relaxation
    assert relaxed["age_min"] == 25


def test_meta_drop_rule_falls_back_to_age_after_flex_exhausted():
    targeting = {
        "flexible_spec": [],
        "age_min": 25,
        "age_max": 65,
        "geo_locations": ["US"],
    }
    relaxed = drop_rule_for_meta(targeting)
    assert relaxed is not None
    assert "age_min" not in relaxed
    assert "age_max" not in relaxed
    # Geo intentionally never touched
    assert relaxed["geo_locations"] == ["US"]


def test_meta_drop_rule_exhausted_returns_none():
    targeting = {"flexible_spec": [], "geo_locations": ["US"]}
    assert drop_rule_for_meta(targeting) is None


def test_meta_drop_rule_does_not_mutate_input():
    targeting = {
        "flexible_spec": [{"interests": [{"id": "1"}, {"id": "2"}]}],
        "age_min": 25,
        "age_max": 65,
    }
    import copy
    original = copy.deepcopy(targeting)
    drop_rule_for_meta(targeting)
    assert targeting == original

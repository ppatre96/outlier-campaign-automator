"""Unit tests for MetaInterestResolver — covers cache + signal extraction."""
import json
import os
import sys
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.meta_targeting import MetaInterestResolver


class _FakeCohort:
    name = "test_cohort"

    def __init__(self, rules):
        self.rules = rules


def test_resolve_cohort_basic_geo_only(tmp_path):
    cache = tmp_path / "cache.json"
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    out = r.resolve_cohort(_FakeCohort(rules=[]), geos=["US", "CA"])
    assert out["geo_locations"]["countries"] == ["US", "CA"]


def test_resolve_cohort_extracts_skill_terms_from_cache(tmp_path):
    cache = tmp_path / "cache.json"
    cache.write_text(json.dumps({
        "python":         [{"id": "111", "name": "Python (programming language)"}],
        "data scientist": [{"id": "222", "name": "Data Scientist"}],
    }))
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    cohort = _FakeCohort(rules=[
        ("skills__python", "python"),
        ("job_titles_norm__data_scientist", "data scientist"),
    ])
    out = r.resolve_cohort(cohort, geos=["US"])
    interests = out["flexible_spec"][0]["interests"]
    ids = sorted(i["id"] for i in interests)
    assert ids == ["111", "222"]


def test_education_statuses_emitted_for_degree_features(tmp_path):
    cache = tmp_path / "cache.json"
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    cohort = _FakeCohort(rules=[
        ("highest_degree_level__bachelors", "bachelors"),
        ("highest_degree_level__masters",   "masters"),
    ])
    out = r.resolve_cohort(cohort, geos=["US"])
    assert out["education_statuses"] == [4, 5]


def test_employment_special_category_drops_age(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "SPECIAL_AD_CATEGORY", "EMPLOYMENT")
    cache = tmp_path / "cache.json"
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    out = r.resolve_cohort(_FakeCohort(rules=[]), geos=["US"])
    assert "age_min" not in out and "age_max" not in out


def test_no_special_category_keeps_age(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "SPECIAL_AD_CATEGORY", "NONE")
    cache = tmp_path / "cache.json"
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    out = r.resolve_cohort(_FakeCohort(rules=[]), geos=["US"])
    assert out["age_min"] == 21
    assert out["age_max"] == 65


def test_human_value_normalisation():
    assert MetaInterestResolver._human_value("skills__python", "python") == "python"
    assert MetaInterestResolver._human_value(
        "job_titles_norm__data_scientist", "raw"
    ) == "data scientist"
    assert MetaInterestResolver._human_value("rawcol", "fallback") == "fallback"


def test_lookup_misses_get_cached(tmp_path):
    """Repeated lookups of an unknown term must hit the API only once.

    We count actual SDK calls by patching the SDK boundary itself
    (`TargetingSearch.search`) — the first lookup hits it, the second is
    served from the on-disk cache.
    """
    cache = tmp_path / "cache.json"
    r = MetaInterestResolver(access_token="dummy", cache_path=cache)
    r._initialized = True   # skip SDK init

    with patch("facebook_business.adobjects.targetingsearch.TargetingSearch.search",
               return_value=[]) as mock_search:
        r._lookup_interests("never-heard-of-it")
        r._lookup_interests("never-heard-of-it")
    # SDK called at most once even though we asked twice.
    assert mock_search.call_count <= 1
    cached = json.loads(cache.read_text())
    assert "never-heard-of-it" in cached
    assert cached["never-heard-of-it"] == []

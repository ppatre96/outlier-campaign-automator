"""Per-cohort audience probe behind GEO_AUDIENCE_FLOOR.

Geo clustering is cohort-independent — it only sees the country list — but
whether a cluster clears the 50,000 floor depends entirely on the cohort's
targeting. "How many LinkedIn members live in these 7 countries" is always in
the millions and would never trip a floor, so the probe resolves the COHORT's
facets and pins profileLocations to the cluster.

The unknown-vs-zero distinction is the important part: merge_small_geo_groups
only merges a cluster it has a real number for, so a transient LinkedIn failure
must not read as "audience = 0" and collapse every cluster into one.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from main import _cohort_geo_audience_fn  # noqa: E402


def _cohort():
    return SimpleNamespace(name="designers", rules=[("skills__graphic_design", 1)],
                           exclude_add=[])


class _Urn:
    def resolve(self, facet, code):
        return f"urn:li:geo:{code}"

    def resolve_cohort_rules(self, rules):
        return {"skills": ["urn:li:skill:1"]}

    def resolve_facet_pairs(self, pairs):
        return {"titles": ["urn:li:title:9"]}


class _Li:
    def __init__(self, value):
        self.value = value
        self.calls = []

    def get_audience_count(self, facets, excludes=None):
        self.calls.append((facets, excludes))
        return self.value


def test_no_client_means_no_probe():
    """Without LinkedIn the floor is skipped, not guessed."""
    assert _cohort_geo_audience_fn(_cohort(), li_client=None, urn_res=None) is None
    assert _cohort_geo_audience_fn(_cohort(), li_client=_Li(1), urn_res=None) is None


def test_measures_cohort_facets_pinned_to_the_cluster():
    li = _Li(120_000)
    fn = _cohort_geo_audience_fn(_cohort(), li_client=li, urn_res=_Urn())
    assert fn(["US", "GB"]) == 120_000
    facets, excludes = li.calls[0]
    assert facets["skills"] == ["urn:li:skill:1"], "cohort targeting must be applied"
    assert facets["profileLocations"] == ["urn:li:geo:US", "urn:li:geo:GB"]


def test_results_are_cached_regardless_of_geo_order():
    li = _Li(120_000)
    fn = _cohort_geo_audience_fn(_cohort(), li_client=li, urn_res=_Urn())
    fn(["US", "GB"])
    fn(["GB", "US"])
    fn(["US", "GB"])
    assert len(li.calls) == 1, "one audience call per distinct cluster"


def test_cohort_excludes_are_applied():
    li = _Li(50_000)
    c = _cohort()
    c.exclude_add = [("titles", "Sales Engineer")]
    _cohort_geo_audience_fn(c, li_client=li, urn_res=_Urn())(["US"])
    assert li.calls[0][1] == {"titles": ["urn:li:title:9"]}


@pytest.mark.parametrize("value", [0, None])
def test_zero_or_missing_count_is_unknown_not_zero(value):
    """get_audience_count returns 0 on API error as well as for an empty
    audience. Reading that as 0 would merge every cluster on a transient blip."""
    fn = _cohort_geo_audience_fn(_cohort(), li_client=_Li(value), urn_res=_Urn())
    assert fn(["US"]) is None


def test_exception_is_unknown_not_zero():
    class Boom:
        def get_audience_count(self, facets, excludes=None):
            raise RuntimeError("LinkedIn 500")

    fn = _cohort_geo_audience_fn(_cohort(), li_client=Boom(), urn_res=_Urn())
    assert fn(["US"]) is None


def test_unresolvable_geos_are_unknown():
    class NoGeo(_Urn):
        def resolve(self, facet, code):
            return None

    li = _Li(999)
    fn = _cohort_geo_audience_fn(_cohort(), li_client=li, urn_res=NoGeo())
    assert fn(["ZZ"]) is None
    assert li.calls == [], "no point calling LinkedIn with zero geo urns"

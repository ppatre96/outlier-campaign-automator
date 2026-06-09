"""LinkedIn live geo audience estimate in measure_audience_for_cohort.

Generalist locale cohorts skip the Stage A/B/C beam, so cohort.audience_size
stays at its default 0 and the console showed a misleading "0 (below floor)".
When a LinkedIn client + URN resolver are supplied, the LinkedIn branch now
queries audienceCounts on the geo facet and reports the true reach.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.prep_audience import measure_audience_for_cohort


class _Cohort:
    def __init__(self, name, rules, facet_strength=None):
        self.name = name
        self.rules = rules
        self.facet_strength = facet_strength or {}
        self.audience_size = 0  # dataclass default — beam never ran


class _FakeUrn:
    def resolve(self, facet, country):
        return f"urn:li:geo:{country}"


class _FakeLI:
    def __init__(self, count):
        self._count = count
        self.calls = []

    def get_audience_count(self, facet_urns, exclude_facet_urns=None):
        self.calls.append(facet_urns)
        return self._count


def _li(rows):
    return next(r for r in rows if r.platform == "linkedin")


def test_generalist_gets_live_geo_estimate():
    c = _Cohort("Bengali generalist contributors", [("interface_locale", "bn-in")],
                {"generalist_locale": "bn-in"})
    li = _FakeLI(21_000_000)
    rows = measure_audience_for_cohort(
        c, included_geos=["BD", "IN"], enabled_platforms=["linkedin"],
        li_client=li, urn_resolver=_FakeUrn(),
    )
    r = _li(rows)
    assert r.audience_size == 21_000_000
    assert r.status == "measured"
    # Sized on the geo facet (profileLocations), not the synthetic locale rule.
    assert li.calls and "profileLocations" in li.calls[0]


def test_empty_geo_generalist_falls_back_to_locale_region():
    """ko-KR with empty included_geos sizes on the locale region (KR)."""
    c = _Cohort("Korean generalist contributors", [("interface_locale", "ko-kr")],
                {"generalist_locale": "ko-kr"})
    li = _FakeLI(5_000_000)
    rows = measure_audience_for_cohort(
        c, included_geos=[], enabled_platforms=["linkedin"],
        li_client=li, urn_resolver=_FakeUrn(),
    )
    r = _li(rows)
    assert r.audience_size == 5_000_000
    assert r.geos_used == ["KR"]
    assert li.calls[0]["profileLocations"] == ["urn:li:geo:KR"]


def test_no_client_keeps_legacy_behavior():
    """Without a client the generalist cohort still reports the unset default
    (0 → below_floor) — no crash, no behavior change for callers that don't
    pass a client."""
    c = _Cohort("Bengali generalist contributors", [("interface_locale", "bn-in")],
                {"generalist_locale": "bn-in"})
    rows = measure_audience_for_cohort(
        c, included_geos=["BD", "IN"], enabled_platforms=["linkedin"],
    )
    r = _li(rows)
    assert r.audience_size == 0
    assert r.status == "below_floor"


def test_stage_c_number_wins_over_live_call():
    """Specialist cohorts pass li_audience_size (the facet-based Stage C
    estimate); the geo-only live call must NOT override it."""
    c = _Cohort("data scientists", [("skills__python", "python")])
    li = _FakeLI(99_999_999)
    rows = measure_audience_for_cohort(
        c, included_geos=["US"], enabled_platforms=["linkedin"],
        li_audience_size=120_000, li_client=li, urn_resolver=_FakeUrn(),
    )
    r = _li(rows)
    assert r.audience_size == 120_000
    assert li.calls == []  # live call skipped — Stage C number used


# ── Cold-start (no Stage C number) sizes on the cohort's OWN facets ───────────

class _FakeUrnResolving(_FakeUrn):
    """Cold-start cohort whose facets DO resolve to a real LinkedIn URN."""
    def resolve_cohort_rules(self, rules):
        return {"titles": ["urn:li:title:31415"]}


class _FakeUrnCollapsing(_FakeUrn):
    """Cold-start cohort whose LLM-coined facets resolve to nothing."""
    def resolve_cohort_rules(self, rules):
        return {}


def test_cold_start_collapsed_targeting_flags_needs_human():
    """A specialist cold-start cohort whose facets evaporate must NOT report a
    geo-only size as 'measured' — it's flagged needs_human and never queried."""
    c = _Cohort("BLV accessibility contributors",
                [("skills__talkback_screen_reader", "talkback screen reader")])
    li = _FakeLI(290_000_000)
    rows = measure_audience_for_cohort(
        c, included_geos=["US"], enabled_platforms=["linkedin"],
        li_client=li, urn_resolver=_FakeUrnCollapsing(),
    )
    r = _li(rows)
    assert r.status == "needs_human"
    assert r.audience_size is None
    assert li.calls == []                      # geo-only count never fired
    assert r.facets.get("collapsed") is True


def test_cold_start_resolved_targeting_sizes_on_facets():
    """When the cohort's facets resolve, size on facets + geo (not geo-only)."""
    c = _Cohort("accessibility specialists",
                [("job_titles_norm__accessibility_specialist", "accessibility specialist")])
    li = _FakeLI(17_000)
    rows = measure_audience_for_cohort(
        c, included_geos=["US"], enabled_platforms=["linkedin"],
        li_client=li, urn_resolver=_FakeUrnResolving(),
    )
    r = _li(rows)
    assert r.audience_size == 17_000
    assert r.status == "below_floor"           # 17k < AUDIENCE_SIZE_MIN (50k)
    assert "titles" in li.calls[0] and "profileLocations" in li.calls[0]

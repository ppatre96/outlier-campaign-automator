"""LinkedIn language-skill + ISO-2 geo helpers (Diego 2026-06-04: target the
language as a skill; Smart Ramp feeds ISO-2 geos that LinkedIn matches by name).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.locales import country_name_for, linkedin_skill_urn
from src.prep_audience import measure_audience_for_cohort


def test_country_name_for():
    assert country_name_for("BD") == "Bangladesh"
    assert country_name_for("KR") == "South Korea"   # not "Korea, Republic of"
    assert country_name_for("VN") == "Vietnam"
    assert country_name_for("GB") == "United Kingdom"
    assert country_name_for("India") == "India"      # already a name → passthrough
    assert country_name_for("") in ("", None)


def test_linkedin_skill_urn_case_insensitive():
    assert linkedin_skill_urn("bn-in") == "urn:li:skill:12523"
    assert linkedin_skill_urn("ZH_CN") == "urn:li:skill:2473"
    assert linkedin_skill_urn("xx-yy") is None


class _Cohort:
    def __init__(self, name, locale):
        self.name = name
        self.rules = [("interface_locale", locale)]
        self.facet_strength = {"generalist_locale": locale}
        self.audience_size = 0


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


def test_generalist_audience_includes_language_skill():
    """The LinkedIn estimate for a generalist cohort must target language-skill
    + geo (Diego), so the audienceCounts call carries the skills facet."""
    li = _FakeLI(790_000)
    rows = measure_audience_for_cohort(
        _Cohort("Bengali generalist contributors", "bn-in"),
        included_geos=["BD", "IN"], enabled_platforms=["linkedin"],
        li_client=li, urn_resolver=_FakeUrn(),
    )
    r = next(x for x in rows if x.platform == "linkedin")
    assert r.audience_size == 790_000
    assert li.calls and li.calls[0].get("skills") == ["urn:li:skill:12523"]
    assert "profileLocations" in li.calls[0]
    assert r.facets["language_skill_urn"] == "urn:li:skill:12523"
    assert r.facets["geo_only"] is False

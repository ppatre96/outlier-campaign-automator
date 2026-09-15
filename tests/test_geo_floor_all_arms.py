"""GEO_AUDIENCE_FLOOR must apply on every arm that picks geo clusters.

The floor landed on the LinkedIn static arm only (41d29d0). Meta/Google/Reddit/
TikTok inherit static's campaign_specs so they came along for free, but the
InMail arm and the prep/brief path each did their own ramp-level clustering with
no floor at all.

That is not theoretical. On GMR-0029 (poller run 33971842229, 2026-09-05) the
InMail arm measured its own reach per cluster — 3,700 for Global, 8,200 for
Eastern European — logged both, wrote them to the registry, and launched anyway.
These tests pin the shared helper so a future arm can't reintroduce that.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402
import main  # noqa: E402
from main import _geo_groups_for_cohort, _warn_if_below_geo_floor  # noqa: E402
from src.geo_tiers import GeoCampaignGroup  # noqa: E402


def _cohort(name="designers"):
    return SimpleNamespace(name=name, rules=[("skills__graphic_design", 1)], exclude_add=[])


class _Urn:
    def resolve(self, facet, code):
        return f"urn:li:geo:{code}"

    def resolve_cohort_rules(self, rules):
        return {"skills": ["urn:li:skill:1"]}

    def resolve_facet_pairs(self, pairs):
        return {}


class _Li:
    """Audience is small for everything except a US+GB-bearing cluster.

    Mirrors the GMR-0029 shape: the long-tail clusters are tiny and the
    anglo markets carry the reach.
    """

    def __init__(self):
        self.calls = []

    def get_audience_count(self, facets, excludes=None):
        geos = {u.rsplit(":", 1)[-1] for u in facets.get("profileLocations", [])}
        self.calls.append(sorted(geos))
        return 400_000 if {"US", "GB"} & geos else 3_700


def _ramp_groups():
    """Two ramp-level clusters, one of which is far below the floor."""
    return [
        GeoCampaignGroup(cluster="anglo", cluster_label="English-speaking",
                         geos=["US", "GB"], median_multiplier=1.0,
                         advertised_rate="$50/hr", campaign_suffix="anglo"),
        GeoCampaignGroup(cluster="global_mix", cluster_label="Global",
                         geos=["TV", "WS"], median_multiplier=1.0,
                         advertised_rate="$50/hr", campaign_suffix="global"),
    ]


def _call(**overrides):
    kwargs = dict(
        geo_groups=_ramp_groups(),
        raw_geos=["US", "GB", "TV", "WS"],
        base_rate_usd=50.0,
        rate_geo_specific=False,
        li_client=_Li(),
        urn_res=_Urn(),
        arm="test-arm",
    )
    kwargs.update(overrides)
    return _geo_groups_for_cohort(_cohort(), **kwargs)


def test_undersized_cluster_is_merged_away(monkeypatch):
    """The sub-floor cluster must not survive as its own campaign."""
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    groups = _call()
    assert len(groups) == 1, "the 3,700-member cluster should have been merged"
    # No geo is lost in the merge — that was the other half of f9118b0.
    assert set(sum((g.geos for g in groups), [])) == {"US", "GB", "TV", "WS"}


def test_floor_disabled_leaves_ramp_groups_untouched(monkeypatch):
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 0)
    assert [g.cluster for g in _call()] == ["anglo", "global_mix"]


def test_no_linkedin_client_falls_back_rather_than_guessing(monkeypatch):
    """No probe means no measurement. Leave the clusters alone — never assume
    an unmeasured cluster is small and merge on a guess."""
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    assert [g.cluster for g in _call(li_client=None, urn_res=None)] == ["anglo", "global_mix"]


def test_empty_geo_list_falls_back(monkeypatch):
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    assert [g.cluster for g in _call(raw_geos=[])] == ["anglo", "global_mix"]


def test_reclustering_failure_is_not_fatal(monkeypatch):
    """A blowup inside the grouper must degrade to the ramp-level groups, not
    abort the arm — one cohort's probe failing can't cost the whole launch."""
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    import src.geo_tiers as _gt

    def boom(*a, **kw):
        raise RuntimeError("LinkedIn 500")

    monkeypatch.setattr(_gt, "group_geos_for_campaigns", boom)
    assert [g.cluster for g in _call()] == ["anglo", "global_mix"]


def test_empty_regrouping_result_falls_back(monkeypatch):
    """An empty result would otherwise launch nothing for this cohort."""
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    import src.geo_tiers as _gt
    monkeypatch.setattr(_gt, "group_geos_for_campaigns", lambda *a, **kw: [])
    assert [g.cluster for g in _call()] == ["anglo", "global_mix"]


# ── The terminal case: merging bottoms out and the survivor is still small ───

def test_warns_when_surviving_cluster_is_still_below_floor(monkeypatch, caplog):
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    with caplog.at_level(logging.WARNING, logger=main.log.name):
        _warn_if_below_geo_floor("designers", "Global", 3_700, "inmail")
    assert "3,700" in caplog.text and "below GEO_AUDIENCE_FLOOR" in caplog.text


@pytest.mark.parametrize("audience", [50_000, 400_000])
def test_no_warning_at_or_above_the_floor(monkeypatch, caplog, audience):
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    with caplog.at_level(logging.WARNING, logger=main.log.name):
        _warn_if_below_geo_floor("designers", "English-speaking", audience, "static")
    assert caplog.text == ""


@pytest.mark.parametrize("audience", [None, 0])
def test_unknown_audience_does_not_warn(monkeypatch, caplog, audience):
    """None/0 mean the count failed, not that the audience is empty — same
    unknown-vs-zero rule the probe follows."""
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 50_000)
    with caplog.at_level(logging.WARNING, logger=main.log.name):
        _warn_if_below_geo_floor("designers", "Global", audience, "static")
    assert caplog.text == ""


def test_warning_is_silent_when_floor_disabled(monkeypatch, caplog):
    monkeypatch.setattr(config, "GEO_AUDIENCE_FLOOR", 0)
    with caplog.at_level(logging.WARNING, logger=main.log.name):
        _warn_if_below_geo_floor("designers", "Global", 3_700, "static")
    assert caplog.text == ""

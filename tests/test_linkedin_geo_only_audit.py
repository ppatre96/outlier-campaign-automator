"""Tests for the LinkedIn geo-only targeting auditor (workstream A).

Catches the GMR-0024 class: a LinkedIn campaign whose live targeting is
geo-only (no narrowing skill/title facet) — the whole-country ~290M ship a
cold-start facet collapse can produce. Generalist-locale campaigns carry a
language skill facet and must NOT be flagged.
"""
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.linkedin_geo_only_audit import _is_geo_only, audit_linkedin_geo_only

_GEO = {"or": {"urn:li:adTargetingFacet:profileLocations": ["urn:li:geo:103644278"]}}
_TITLES = {"or": {"urn:li:adTargetingFacet:titles": ["urn:li:title:31415"]}}
_SKILLS = {"or": {"urn:li:adTargetingFacet:skills": ["urn:li:skill:12060"]}}
_LOCALE = {"or": {"urn:li:adTargetingFacet:interfaceLocales": ["urn:li:locale:en_US"]}}


def _crit(*clauses):
    return {"include": {"and": list(clauses)}}


# ── _is_geo_only ──────────────────────────────────────────────────────────────

def test_geo_only_true_for_location_only():
    assert _is_geo_only(_crit(_GEO)) is True


def test_geo_only_true_for_geo_plus_locale():
    assert _is_geo_only(_crit(_GEO, _LOCALE)) is True


def test_geo_only_false_with_titles():
    assert _is_geo_only(_crit(_GEO, _TITLES)) is False


def test_geo_only_false_for_generalist_language_skill():
    """Generalist-locale campaign = geo + language SKILL → exempt."""
    assert _is_geo_only(_crit(_GEO, _SKILLS)) is False


def test_geo_only_false_on_empty_targeting():
    assert _is_geo_only({}) is False
    assert _is_geo_only({"include": {"and": []}}) is False


# ── audit_linkedin_geo_only ───────────────────────────────────────────────────

def _row(**kw):
    base = {"platform": "linkedin", "platform_campaign_id": "c1",
            "smart_ramp_id": "GMR-0024", "status": "active"}
    base.update(kw)
    return base


def test_flags_and_pauses_geo_only_campaign():
    rows = [_row(platform_campaign_id="urn:li:sponsoredCampaign:1")]
    paused = []
    with patch("src.ui_decisions.log_event", lambda *a, **k: None):
        out = audit_linkedin_geo_only(
            rows, autofix=True,
            reader=lambda cid: _crit(_GEO),
            pauser=lambda cid: paused.append(cid) or True,
        )
    assert len(out["violations"]) == 1
    assert out["handled"] == ["urn:li:sponsoredCampaign:1"]
    assert paused == ["urn:li:sponsoredCampaign:1"]


def test_does_not_flag_properly_targeted_campaign():
    rows = [_row()]
    out = audit_linkedin_geo_only(
        rows, autofix=True,
        reader=lambda cid: _crit(_GEO, _TITLES),
        pauser=lambda cid: True,
    )
    assert out["violations"] == []
    assert out["handled"] == []


def test_ignores_non_linkedin_rows():
    rows = [_row(platform="meta")]
    out = audit_linkedin_geo_only(rows, autofix=True, reader=lambda cid: _crit(_GEO),
                                  pauser=lambda cid: True)
    assert out["violations"] == []


def test_skips_already_paused_rows():
    rows = [_row(status="paused")]
    out = audit_linkedin_geo_only(rows, autofix=True, reader=lambda cid: _crit(_GEO),
                                  pauser=lambda cid: True)
    assert out["violations"] == []


def test_detect_only_when_autofix_off():
    rows = [_row()]
    paused = []
    out = audit_linkedin_geo_only(
        rows, autofix=False,
        reader=lambda cid: _crit(_GEO),
        pauser=lambda cid: paused.append(cid) or True,
    )
    assert len(out["violations"]) == 1     # still flagged
    assert out["handled"] == []            # but not fixed
    assert paused == []                    # pauser never called


def test_excluded_container_skipped():
    rows = [_row(platform_campaign_id="c9")]
    out = audit_linkedin_geo_only(
        rows, autofix=True, exclude_containers={"c9"},
        reader=lambda cid: _crit(_GEO), pauser=lambda cid: True,
    )
    assert out["violations"] == []

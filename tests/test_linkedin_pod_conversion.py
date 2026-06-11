"""_linkedin_pod_conversion_id — Smart Ramp pod → per-pod WS Grant rule id, +
create_campaign forwarding it as the SOLE attached conversion (replacing the
default OCP) so LinkedIn optimizes on worker_skill_grant only. Rule ids verified
live by name 2026-06-11 (see reference_outlier_value_based_conversions).
"""
import os, sys
from types import SimpleNamespace
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as M


def test_each_pod_maps_to_its_rule():
    assert M._linkedin_pod_conversion_id("coders") == 26557044
    assert M._linkedin_pod_conversion_id("specialist") == 26557052
    assert M._linkedin_pod_conversion_id("languages") == 26557060
    assert M._linkedin_pod_conversion_id("generalist") == 26557068


def test_case_and_whitespace_insensitive():
    assert M._linkedin_pod_conversion_id("  Specialist ") == 26557052
    assert M._linkedin_pod_conversion_id("CODERS") == 26557044


def test_unknown_or_missing_pod_returns_none():
    assert M._linkedin_pod_conversion_id("") is None
    assert M._linkedin_pod_conversion_id(None) is None
    assert M._linkedin_pod_conversion_id("data-science") is None


def _fake_req_factory(seen):
    def fake_req(method, url, **kw):
        if "/conversions/" in url:
            seen.append(url)
            return SimpleNamespace(ok=True, status_code=200, headers={}, text="", json=lambda: {"campaigns": []})
        # create POST
        return SimpleNamespace(ok=True, status_code=201, headers={"x-linkedin-id": "999"}, text="", json=lambda: {})
    return fake_req


def test_create_campaign_attaches_passed_conversion_not_default(monkeypatch):
    """create_campaign(conversion_id=<per-pod>) attaches that rule, REPLACING
    the default LINKEDIN_CONVERSION_ID."""
    from src.linkedin_api import LinkedInClient
    c = LinkedInClient(token="dummy")
    seen: list[str] = []
    monkeypatch.setattr(c, "_req", _fake_req_factory(seen))
    c.create_campaign(
        "n", "urn:li:sponsoredCampaignGroup:1", {"skills": ["urn:li:skill:1"]},
        conversion_id=26557044,
    )
    assert any("/conversions/26557044" in u for u in seen)
    assert not any("/conversions/19801700" in u for u in seen)  # default NOT attached

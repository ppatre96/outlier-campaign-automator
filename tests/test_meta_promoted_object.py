"""Regression guard for the Meta ad-set conversion-optimization payload.

2026-06-09: the Meta arm optimized on a custom_conversion_id (986478843749388)
that was ARCHIVED in Meta. Archived custom conversions track NOTHING, so all 14
GMR-0023 language ad sets logged 0 conversions despite live traffic. Tuan's fix:
optimize on the pixel event directly via promoted_object
{pixel_id, custom_event_type: OTHER, custom_event_str}.

These tests pin that shape so we can't regress to a custom_conversion_id.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.meta_api import MetaClient


def _capture_adset_params(monkeypatch) -> dict:
    """Run create_campaign with the Meta SDK boundary mocked; return the params
    dict the arm would have sent to account.create_ad_set."""
    captured: dict = {}

    import facebook_business.adobjects.adaccount as adacct_mod

    class _FakeAccount:
        def __init__(self, _id):
            pass

        def create_ad_set(self, params):
            captured.update(params)
            return {"id": "123456"}

    monkeypatch.setattr(adacct_mod, "AdAccount", _FakeAccount)

    c = MetaClient(access_token="x", ad_account_id="act_1", api_version="v21.0", page_id="p")
    monkeypatch.setattr(c, "_ensure_init", lambda: None)
    c.create_campaign(name="t", campaign_group_id="cg", targeting={})
    return captured


def test_promoted_object_is_pixel_event_not_custom_conversion(monkeypatch):
    monkeypatch.setattr(config, "META_PIXEL_ID", "637714478283926")
    monkeypatch.setattr(config, "META_CUSTOM_EVENT_STR", "worker_skill_all")

    params = _capture_adset_params(monkeypatch)

    assert params["optimization_goal"] == "OFFSITE_CONVERSIONS"
    po = params["promoted_object"]
    assert po == {
        "pixel_id": "637714478283926",
        "custom_event_type": "OTHER",
        "custom_event_str": "worker_skill_all",
    }
    # The archived-conversion trap must never come back.
    assert "custom_conversion_id" not in po


def test_falls_back_to_link_clicks_when_event_unset(monkeypatch):
    monkeypatch.setattr(config, "META_PIXEL_ID", "637714478283926")
    monkeypatch.setattr(config, "META_CUSTOM_EVENT_STR", "")

    params = _capture_adset_params(monkeypatch)

    assert params["optimization_goal"] == "LINK_CLICKS"
    assert "promoted_object" not in params

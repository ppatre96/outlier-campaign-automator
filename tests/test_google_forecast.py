"""Google Search keyword forecast (GenerateKeywordForecastMetrics) — guard the
early-return paths so a malformed targeting dict never hits the API or raises.

The happy-path API shape (two-call merge of clicks + conversions) is verified
live against the account; here we only pin the cheap pre-flight guards.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_ads_api import GoogleAdsClient


def _client(monkeypatch):
    gc = GoogleAdsClient(channel="search")
    reached = []
    # Records whether the guards let execution reach client init. Raising here
    # is caught inside get_keyword_forecast (→ returns None), so we assert on
    # the flag, not on a propagated exception.
    def _stub():
        reached.append(True)
        raise RuntimeError("no creds in test")
    monkeypatch.setattr(gc, "_ensure_client", _stub)
    return gc, reached


def test_no_keywords_returns_none_without_api(monkeypatch):
    gc, reached = _client(monkeypatch)
    assert gc.get_keyword_forecast({"geo_targets": {"US": ["geoTargetConstants/2840"]}}) is None
    assert reached == []   # early-returned before touching the client


def test_no_geo_returns_none_without_api(monkeypatch):
    gc, reached = _client(monkeypatch)
    assert gc.get_keyword_forecast({"keyword_ideas": ["remote data tasks"]}) is None
    assert reached == []   # no geo → early return, no client


def test_geo_targets_dict_and_list_both_flatten(monkeypatch):
    # With keywords + geo present (either shape), the guards pass and execution
    # reaches client init (which our stub records).
    for gt in ({"US": ["geoTargetConstants/2840"]}, ["geoTargetConstants/2840"]):
        gc, reached = _client(monkeypatch)
        assert gc.get_keyword_forecast({"keyword_ideas": ["x"], "geo_targets": gt}) is None
        assert reached == [True], f"geo_targets={gt!r} should flatten and reach client"

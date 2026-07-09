"""Unit tests for the TikTok Ads client (src/tiktok_api.py).

Covers: gating (creative-only fallback when TIKTOK_API_ENABLED off), CTA
normalization, and the programmatic Phase-2 path (campaign/adgroup/image/ad
create + reporting) with a mocked HTTP layer. No network."""
import hashlib
import json
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import config
import src.tiktok_api as tt
from src.tiktok_api import TikTokClient, _normalize_cta, exchange_auth_code


# ── Fake HTTP layer ──────────────────────────────────────────────────────────
class FakeResp:
    def __init__(self, body: dict, status: int = 200):
        self._body = body
        self.status_code = status
        self.text = json.dumps(body)
        self.ok = 200 <= status < 300

    def json(self):
        return self._body


class FakeSession:
    """Records requests and replays a queue of canned TikTok envelopes."""
    def __init__(self, responses):
        self.headers = {}
        self._responses = list(responses)
        self.calls = []

    def request(self, method, url, json=None, params=None, files=None, data=None, timeout=None):
        self.calls.append({"method": method, "url": url, "json": json,
                           "params": params, "files": files, "data": data})
        return self._responses.pop(0)


def _client(monkeypatch, responses, *, enabled=True):
    monkeypatch.setattr(config, "TIKTOK_API_ENABLED", enabled)
    monkeypatch.setattr(config, "TIKTOK_ACCESS_TOKEN", "tok")
    monkeypatch.setattr(config, "TIKTOK_ADVERTISER_ID", "adv123")
    c = TikTokClient()
    c._session = FakeSession(responses)
    return c


def _ok(data):
    return FakeResp({"code": 0, "message": "OK", "data": data})


# ── Gating ─────────────────────────────────────────────────────────────────
class TestGating:
    def test_disabled_create_methods_raise(self, monkeypatch):
        monkeypatch.setattr(config, "TIKTOK_API_ENABLED", False)
        c = TikTokClient()
        with pytest.raises(RuntimeError):
            c.create_campaign_group("x")
        with pytest.raises(RuntimeError):
            c.upload_image("/tmp/x.png")

    def test_disabled_create_image_ad_returns_local_fallback(self, monkeypatch):
        monkeypatch.setattr(config, "TIKTOK_API_ENABLED", False)
        c = TikTokClient()
        r = c.create_image_ad("adg", "img", "Head", "Desc")
        assert r.status == "local_fallback"

    def test_enabled_but_missing_creds_raises(self, monkeypatch):
        monkeypatch.setattr(config, "TIKTOK_API_ENABLED", True)
        monkeypatch.setattr(config, "TIKTOK_ACCESS_TOKEN", "")
        monkeypatch.setattr(config, "TIKTOK_ADVERTISER_ID", "")
        c = TikTokClient()
        with pytest.raises(RuntimeError, match="not set"):
            c.create_campaign_group("x")


# ── CTA normalization ────────────────────────────────────────────────────────
class TestCTA:
    @pytest.mark.parametrize("raw,expected", [
        ("GET_STARTED", "SIGN_UP"),
        ("sign up", "SIGN_UP"),
        ("LEARN_MORE", "LEARN_MORE"),
        ("APPLY_NOW", "APPLY_NOW"),
        (None, "SIGN_UP"),
        ("not-a-cta", "SIGN_UP"),
    ])
    def test_remap(self, raw, expected):
        assert _normalize_cta(raw) == expected


# ── Programmatic path ────────────────────────────────────────────────────────
class TestProgrammatic:
    def test_create_campaign_group_paused(self, monkeypatch):
        c = _client(monkeypatch, [_ok({"campaign_id": "camp1"})])
        cid = c.create_campaign_group("GMR bn", geos=["US"])
        assert cid == "camp1"
        body = c._session.calls[0]["json"]
        assert body["operation_status"] == "DISABLE"     # PAUSED default
        assert body["objective_type"] == config.TIKTOK_OBJECTIVE
        assert body["advertiser_id"] == "adv123"
        assert c._session.calls[0]["url"].endswith("/open_api/v1.3/campaign/create/")

    def test_create_adgroup_sets_optimization_and_budget(self, monkeypatch):
        monkeypatch.setattr(config, "TIKTOK_PIXEL_ID", "pix1")
        c = _client(monkeypatch, [_ok({"adgroup_id": "ag1"})])
        ag = c.create_campaign("bn A", "camp1",
                               {"location_ids": [6252001], "age_groups": ["AGE_25_34"]},
                               daily_budget_cents=7500)
        assert ag == "ag1"
        body = c._session.calls[0]["json"]
        assert body["operation_status"] == "DISABLE"
        assert body["optimization_goal"] == config.TIKTOK_OPTIMIZATION_GOAL
        assert body["billing_event"] == config.TIKTOK_BILLING_EVENT
        assert body["budget"] == 75.0                    # cents → USD
        assert body["pixel_id"] == "pix1"
        assert body["location_ids"] == ["6252001"]

    def test_upload_image_sends_md5_signature(self, monkeypatch, tmp_path):
        png = tmp_path / "c.png"
        raw = b"\x89PNG\r\n_fake_bytes_"
        png.write_bytes(raw)
        c = _client(monkeypatch, [_ok({"image_id": "img99"})])
        img = c.upload_image(png)
        assert img == "img99"
        sent = c._session.calls[0]["data"]
        assert sent["image_signature"] == hashlib.md5(raw).hexdigest()
        assert sent["upload_type"] == "UPLOAD_BY_FILE"
        assert "image_file" in c._session.calls[0]["files"]

    def test_create_image_ad_single_image(self, monkeypatch):
        monkeypatch.setattr(config, "TIKTOK_IDENTITY_ID", "ident1")
        c = _client(monkeypatch, [_ok({"ad_ids": ["ad1"]})])
        r = c.create_image_ad("ag1", "img1", "Head", "Short caption",
                              cta="GET_STARTED", destination_url="https://o.co?utm_campaign=x")
        assert r.status == "ok" and r.creative_id == "ad1"
        creative = c._session.calls[0]["json"]["creatives"][0]
        assert creative["ad_format"] == config.TIKTOK_IMAGE_AD_FORMAT
        assert creative["image_ids"] == ["img1"]
        assert creative["call_to_action"] == "SIGN_UP"   # remapped
        assert creative["identity_id"] == "ident1"

    def test_error_code_returns_error_result(self, monkeypatch):
        c = _client(monkeypatch, [FakeResp({"code": 40002, "message": "bad param", "data": {}})])
        r = c.create_image_ad("ag1", "img1", "Head", "Desc")
        assert r.status == "error" and "40002" in (r.error_message or "")

    def test_pause_ad(self, monkeypatch):
        c = _client(monkeypatch, [_ok({})])
        assert c.pause_ad("ad1", "PAUSED") is True
        body = c._session.calls[0]["json"]
        assert body["operation_status"] == "DISABLE" and body["ad_ids"] == ["ad1"]


# ── Reporting ────────────────────────────────────────────────────────────────
class TestReporting:
    def test_daily_metrics_parse(self, monkeypatch):
        rows = {
            "list": [
                {"dimensions": {"campaign_id": "c1", "stat_time_day": "2026-07-08 00:00:00"},
                 "metrics": {"spend": "12.5", "impressions": "1000", "clicks": "30", "conversion": "4"}},
            ],
            "page_info": {"total_page": 1},
        }
        c = _client(monkeypatch, [_ok(rows)])
        out = c.fetch_campaign_metrics_daily(window_days=30)
        assert len(out) == 1
        r = out[0]
        assert r["campaign_id"] == "c1" and r["metric_date"] == "2026-07-08"
        assert r["spend_usd"] == 12.5 and r["impressions"] == 1000 and r["conversions"] == 4
        # daily dimension present in the request
        assert json.loads(c._session.calls[0]["params"]["dimensions"]) == ["campaign_id", "stat_time_day"]


# ── auth exchange ─────────────────────────────────────────────────────────────
def test_exchange_auth_code(monkeypatch):
    captured = {}

    def fake_post(url, json=None, timeout=None):
        captured["url"] = url
        captured["json"] = json
        return _ok({"access_token": "long_lived_tok", "advertiser_ids": ["adv1", "adv2"]})

    monkeypatch.setattr(tt.requests, "post", fake_post)
    monkeypatch.setattr(config, "TIKTOK_APP_ID", "app1")
    monkeypatch.setattr(config, "TIKTOK_APP_SECRET", "sec1")
    data = exchange_auth_code("the_auth_code")
    assert data["access_token"] == "long_lived_tok"
    assert data["advertiser_ids"] == ["adv1", "adv2"]
    assert captured["json"]["auth_code"] == "the_auth_code"
    assert captured["url"].endswith("/oauth2/access_token/")

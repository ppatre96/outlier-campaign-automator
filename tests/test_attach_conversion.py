"""attach_conversion_to_campaign robustness — verify-after-attach + 5xx retry.

The shared conversion's `campaigns` array is large (600+); `$set`-ing the whole
array makes LinkedIn return an intermittent 500 even though the write usually
LANDS. The bare 500 was a false-negative. These tests pin the new behavior:
verify after a failed PATCH, retry on 5xx, give up on 4xx, return ground truth.
"""
import sys, os
import requests
import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.linkedin_api import LinkedInClient

TARGET = "urn:li:sponsoredCampaign:1"


class FakeResp:
    def __init__(self, status, campaigns=None, text=""):
        self.status_code = status
        self.ok = 200 <= status < 300
        self.text = text
        self._campaigns = campaigns or []

    def json(self):
        return {"campaigns": list(self._campaigns)}

    def raise_for_status(self):
        if not self.ok:
            raise requests.HTTPError(f"{self.status_code}")


class Conv:
    """Scripted LinkedIn conversion. `post_results` per POST attempt:
    'ok' (200, lands), 'land500' (500 but lands), 500 (no land), 400 (no land)."""

    def __init__(self, post_results, initial=None):
        self.campaigns = list(initial or [])
        self.post_results = list(post_results)
        self.post_calls = 0
        self.get_calls = 0

    def req(self, method, url, **kw):
        if method == "GET":
            self.get_calls += 1
            return FakeResp(200, campaigns=self.campaigns)
        # POST (the PATCH)
        res = self.post_results[min(self.post_calls, len(self.post_results) - 1)]
        self.post_calls += 1
        if res == "ok":
            self.campaigns.append(TARGET)
            return FakeResp(200, campaigns=self.campaigns)
        if res == "land500":
            self.campaigns.append(TARGET)   # write lands despite the 500
            return FakeResp(500, text="ISE")
        return FakeResp(int(res), text="err")


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    monkeypatch.setattr("src.linkedin_api.time.sleep", lambda *a, **k: None)


def _client(monkeypatch, conv):
    c = LinkedInClient(token="dummy")
    monkeypatch.setattr(c, "_req", conv.req)
    return c


def test_happy_path(monkeypatch):
    conv = Conv(["ok"])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999) is True
    assert conv.post_calls == 1


def test_already_linked_skips_patch(monkeypatch):
    conv = Conv([], initial=[TARGET])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999) is True
    assert conv.post_calls == 0


def test_500_but_write_landed_is_verified_true(monkeypatch):
    """The exact GMR-0024 case: PATCH 500 but the campaign IS linked."""
    conv = Conv(["land500"])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999) is True
    assert conv.post_calls == 1


def test_retries_on_5xx_then_succeeds(monkeypatch):
    conv = Conv([500, "ok"])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999, max_attempts=3) is True
    assert conv.post_calls == 2


def test_persistent_5xx_returns_false(monkeypatch):
    conv = Conv([500, 500, 500])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999, max_attempts=3) is False
    assert conv.post_calls == 3


def test_4xx_no_retry(monkeypatch):
    conv = Conv([400, "ok"])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 999, max_attempts=3) is False
    assert conv.post_calls == 1  # gave up immediately, never retried


def test_disabled_when_id_zero(monkeypatch):
    conv = Conv(["ok"])
    assert _client(monkeypatch, conv).attach_conversion_to_campaign(TARGET, 0) is False
    assert conv.get_calls == 0 and conv.post_calls == 0

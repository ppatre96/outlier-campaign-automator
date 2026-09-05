"""InMail creative creation must survive LinkedIn's content propagation delay.

`POST /rest/inMailContents` returns a content URN before that URN is resolvable
by `POST /rest/adAccounts/{id}/creatives`. A creative posted immediately comes
back `404 NOT_FOUND — "Could not find entity"`, which looks like a malformed
payload but is purely a timing race.

Measured against the live ad account on 2026-09-05, with a freshly created
content urn:

    t+0.4s  404 Could not find entity
    t+2.8s  404 Could not find entity
    t+6.5s  201 urn:li:sponsoredCreative:1596881616

The pipeline posted ~150ms after creating the content, so it lost that race
every time: all 76 InMail ads on GMR-0029 failed and the ramp shipped 15
Message Ads campaigns containing zero ads.

These tests pin the retry, and pin that it is scoped to 404 — a genuinely bad
payload must still fail on the first attempt rather than burn ~30s first.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import src.linkedin_api as li  # noqa: E402


def _resp(status: int, *, creative_id: str = "", body: str = ""):
    r = MagicMock()
    r.status_code = status
    r.ok = 200 <= status < 300
    r.text = body or ("" if r.ok else '{"code":"NOT_FOUND","message":"Could not find entity"}')
    r.headers = {"x-restli-id": creative_id} if creative_id else {}
    r.json.return_value = {}
    # A bare MagicMock().raise_for_status() is a no-op, which would let a
    # "failing" response sail through _raise_for_status and make these tests
    # pass for the wrong reason.
    if not r.ok:
        import requests as _rq
        r.raise_for_status.side_effect = _rq.exceptions.HTTPError(
            f"{status} Client Error", response=r
        )
    return r


@pytest.fixture
def client(monkeypatch):
    """A LinkedInClient with the content POST stubbed and sleep neutralised."""
    monkeypatch.setattr(li.time, "sleep", lambda s: None)

    c = li.LinkedInClient.__new__(li.LinkedInClient)
    c._token = "t"
    c._account_reference_urn = "urn:li:organization:1234"

    # create_inmail_ad does `import requests as _req_lib` INSIDE the function
    # for the content POST, so patching li.requests doesn't intercept it —
    # patch the source module or the test hits the network for real.
    import requests as _real_requests
    content_resp = _resp(201, creative_id="urn:li:adInMailContent:999")
    monkeypatch.setattr(_real_requests, "post", lambda *a, **k: content_resp)
    monkeypatch.setattr(c, "_default_headers", lambda: {}, raising=False)
    monkeypatch.setattr(c, "_url", lambda p: f"https://api.linkedin.com/rest/{p}", raising=False)
    monkeypatch.setattr(c, "get_account_reference_urn",
                        lambda: "urn:li:organization:1234", raising=False)
    return c


def _call(c):
    return c.create_inmail_ad(
        campaign_urn="urn:li:sponsoredCampaign:1",
        sender_urn="urn:li:person:vYrY4QMQH0",
        subject="s",
        body="b",
        cta_label="Learn more",
        ad_name="n",
        destination_url="https://outlier.ai/experts/creators",
    )


def test_retries_past_the_propagation_window(client, monkeypatch):
    """404, 404, then 201 — the exact shape observed against the live account."""
    calls = []

    def fake_req(method, url, **kw):
        calls.append(url)
        if "/creatives" not in url:
            return _resp(201, creative_id="urn:li:adInMailContent:999")
        return _resp(404) if len(calls) < 3 else _resp(201, creative_id="urn:li:sponsoredCreative:77")

    monkeypatch.setattr(client, "_req", fake_req, raising=False)
    urn = _call(client)
    assert urn == "urn:li:sponsoredCreative:77"
    assert sum(1 for u in calls if "/creatives" in u) == 3


def test_first_attempt_success_does_not_sleep(client, monkeypatch):
    slept = []
    monkeypatch.setattr(li.time, "sleep", lambda s: slept.append(s))
    monkeypatch.setattr(
        client, "_req",
        lambda m, u, **k: _resp(201, creative_id="urn:li:sponsoredCreative:1"),
        raising=False,
    )
    assert _call(client) == "urn:li:sponsoredCreative:1"
    assert slept == []


def test_non_404_errors_fail_fast(client, monkeypatch):
    """A real payload error must not burn the whole backoff budget first."""
    attempts = []

    def fake_req(method, url, **kw):
        if "/creatives" in url:
            attempts.append(url)
            return _resp(400, body='{"message":"/Creative/type cannot be set"}')
        return _resp(201, creative_id="urn:li:adInMailContent:999")

    monkeypatch.setattr(client, "_req", fake_req, raising=False)
    with pytest.raises(Exception):
        _call(client)
    assert len(attempts) == 1, "a 400 must not be retried"


def test_gives_up_after_the_full_budget(client, monkeypatch):
    """Propagation slower than the budget still raises rather than hanging."""
    attempts = []

    def fake_req(method, url, **kw):
        if "/creatives" in url:
            attempts.append(url)
            return _resp(404)
        return _resp(201, creative_id="urn:li:adInMailContent:999")

    monkeypatch.setattr(client, "_req", fake_req, raising=False)
    with pytest.raises(Exception):
        _call(client)
    assert len(attempts) == li._INMAIL_CREATIVE_ATTEMPTS


def test_backoff_budget_covers_the_observed_delay():
    """Observed propagation was ~6s; the budget needs real headroom over that."""
    assert sum(li._INMAIL_CREATIVE_BACKOFF) >= 25
    assert li._INMAIL_CREATIVE_ATTEMPTS == len(li._INMAIL_CREATIVE_BACKOFF) + 1

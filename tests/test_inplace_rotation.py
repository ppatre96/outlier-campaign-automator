"""In-place creative rotation (FEEDBACK_INPLACE_ROTATION): pause the losing
creative + attach the challenger to the SAME campaign, instead of archiving/
cloning. Keeps the campaign + its utm_campaign stable."""
import os, sys, types
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.campaign_feedback_agent as cfa


# ── pause primitives ────────────────────────────────────────────────────────

def test_linkedin_set_creative_status(monkeypatch):
    from src.linkedin_api import LinkedInClient
    li = LinkedInClient.__new__(LinkedInClient)
    calls = {}
    monkeypatch.setattr(li, "_url", lambda p: p, raising=False)
    def fake_req(method, url, json=None, headers=None):
        calls.update(method=method, url=url, json=json, headers=headers)
        return types.SimpleNamespace(status_code=200, text="")
    monkeypatch.setattr(li, "_req", fake_req, raising=False)
    assert li.set_creative_status("urn:li:sponsoredCreative:123", "PAUSED") is True
    assert "creatives/" in calls["url"]
    assert calls["json"]["patch"]["$set"]["intendedStatus"] == "PAUSED"
    assert calls["headers"]["X-RestLi-Method"] == "PARTIAL_UPDATE"


def test_linkedin_set_creative_status_returns_false_on_error(monkeypatch):
    from src.linkedin_api import LinkedInClient
    li = LinkedInClient.__new__(LinkedInClient)
    monkeypatch.setattr(li, "_url", lambda p: p, raising=False)
    monkeypatch.setattr(li, "_req", lambda *a, **k: types.SimpleNamespace(status_code=400, text="bad"), raising=False)
    assert li.set_creative_status("urn:li:sponsoredCreative:123") is False


def test_reddit_pause_ad(monkeypatch):
    from src.reddit_api import RedditClient
    rc = RedditClient.__new__(RedditClient)
    calls = {}
    monkeypatch.setattr(rc, "_ensure_init", lambda: None, raising=False)
    monkeypatch.setattr(rc, "_api", lambda m, p, payload=None: calls.update(m=m, p=p, payload=payload) or {}, raising=False)
    assert rc.pause_ad("ad_1") is True
    assert calls["m"] == "PATCH" and calls["p"] == "/ads/ad_1"
    assert calls["payload"]["configured_status"] == "PAUSED"


# ── in-place rotation core ──────────────────────────────────────────────────

class _FakeLI:
    def __init__(self):
        self.calls = []
    def set_creative_status(self, c, s="PAUSED"):
        self.calls.append(("pause", c, s)); return True
    def upload_image(self, p):
        return "urn:li:image:1"
    def create_image_ad(self, **kw):
        self.calls.append(("attach", kw))
        return types.SimpleNamespace(status="ok", creative_urn="urn:li:sponsoredCreative:new")


_ENTRY = {
    "linkedin_campaign_urn": "urn:li:sponsoredCampaign:C1",
    "creative_urn": "urn:li:sponsoredCreative:LOSER",
    "campaign_name": "Scale-GMR-0001 | LinkedIn | en-US | 06/03/2026",
    "utm_campaign": "Scale-GMR-0001 | LinkedIn | en-US | 06/03/2026",
    "smart_ramp_id": "GMR-0001", "cohort_id": "c", "geos": "US",
}


def test_rotate_pauses_loser_and_attaches_to_same_campaign(monkeypatch, tmp_path):
    logged = {}
    monkeypatch.setattr("src.campaign_registry.log_campaign", lambda **kw: logged.update(kw))
    png = tmp_path / "c.png"; png.write_bytes(b"x")
    li = _FakeLI()
    res = cfa._rotate_creative_in_place(
        _ENTRY, li, variant={"headline": "H", "subheadline": "S"},
        png_path=png, next_label="C", pause_loser=True)
    assert res == "urn:li:sponsoredCampaign:C1"
    # the LOSING creative is paused
    assert ("pause", "urn:li:sponsoredCreative:LOSER", "PAUSED") in li.calls
    # the challenger attaches to the SAME campaign
    attach = next(c[1] for c in li.calls if c[0] == "attach")
    assert attach["campaign_urn"] == "urn:li:sponsoredCampaign:C1"
    # utm_campaign is unchanged → continuous attribution; new creative recorded
    assert logged["utm_campaign"] == _ENTRY["utm_campaign"]
    assert logged["linkedin_campaign_urn"] == "urn:li:sponsoredCampaign:C1"
    assert logged["creative_urn"] == "urn:li:sponsoredCreative:new"
    assert logged["angle"] == "C"


def test_rotate_winner_does_not_pause_anything(monkeypatch, tmp_path):
    logged = {}
    monkeypatch.setattr("src.campaign_registry.log_campaign", lambda **kw: logged.update(kw))
    png = tmp_path / "c.png"; png.write_bytes(b"x")
    li = _FakeLI()
    cfa._rotate_creative_in_place(
        _ENTRY, li, variant={"headline": "H", "subheadline": "S"},
        png_path=png, next_label="D", pause_loser=False)
    assert not any(c[0] == "pause" for c in li.calls)   # winner keeps running
    assert any(c[0] == "attach" for c in li.calls)


def test_rotate_no_campaign_urn_is_noop(monkeypatch):
    monkeypatch.setattr("src.campaign_registry.log_campaign", lambda **kw: None)
    assert cfa._rotate_creative_in_place({}, _FakeLI(), variant={"headline": "H"}, png_path=None, next_label="C", pause_loser=True) is None

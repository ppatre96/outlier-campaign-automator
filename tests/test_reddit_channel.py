"""Unit tests for the Reddit channel (v1 creative-only).

Covers the copy adapter (both ad formats), the subreddit targeting resolver,
and the RedditClient gating behaviour (create methods degrade to local_fallback
/ raise while REDDIT_API_ENABLED is off)."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

import config
from src.copy_adapter import adapt_copy_for_platform
from src.reddit_targeting import RedditSubredditResolver
from src.reddit_api import RedditClient, reddit_pod_conversion_event


CANONICAL = {
    "angle":          "A",
    "angleLabel":     "Income",
    "headline":       "Earn extra income with Outlier — your expertise is in demand worldwide",
    "subheadline":    "Help train AI on your schedule and earn payment in dollars per task.",
    "intro_text":     "Looking for flexible remote tasks? Outlier matches you to tasks that fit your skills.",
    "ad_headline":    "Get matched to remote AI tasks",
    "ad_description": "Earn $50/hr. Fully remote.",
    "photo_subject":  "female South Asian cardiologist seated at a home desk",
    "tgLabel":        "Cardiologists",
}


class FakeCohort:
    def __init__(self, pod="coders"):
        self.name = "Senior Python engineers"
        self.rules = [("skills__python", "python")]
        self.job_post_pod = pod


class TestRedditCopyAdapter:
    def test_uses_llm_both_formats(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"title": "AI labs pay $50/hr for your coding expertise", '
                '"cta": "Apply Now", '
                '"freeform_title": "Devs: get paid to make AI better at code", '
                '"freeform_body": "We pay engineers per task to review AI code. Remote, flexible, weekly payment. Reply or click to start."}'
            )
            out = adapt_copy_for_platform(CANONICAL, "reddit")
        assert out["title"] and len(out["title"]) <= 300
        assert out["cta"] in {"Sign Up", "Apply Now", "Learn More", "Get Started"}
        assert out["freeform_title"] and out["freeform_body"]
        assert out["photo_subject"] == CANONICAL["photo_subject"]  # carried through

    def test_falls_back_on_llm_failure(self):
        with patch("src.copy_adapter.call_claude", side_effect=RuntimeError("API down")):
            out = adapt_copy_for_platform(CANONICAL, "reddit")
        # Deterministic fallback still yields both formats + a valid CTA.
        assert out["title"] and out["freeform_title"] and out["freeform_body"]
        assert out["cta"] in {"Sign Up", "Apply Now", "Learn More", "Get Started"}

    def test_invalid_cta_falls_back_to_sign_up(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"title": "T", "cta": "Buy Now", "freeform_title": "FT", "freeform_body": "FB"}'
            )
            out = adapt_copy_for_platform(CANONICAL, "reddit")
        assert out["cta"] == "Sign Up"


class TestRedditTargetingResolver:
    def test_pod_to_subreddits_and_geos(self):
        t = RedditSubredditResolver().resolve_cohort(FakeCohort("coders"), geos=["US", "ca"])
        assert t["pod"] == "coders"
        assert "cscareerquestions" in t["subreddits"]
        assert t["geo_locations"] == ["US", "CA"]  # uppercased ISO codes

    def test_unknown_pod_falls_back_to_generalist_subs(self):
        # A cohort whose pod can't be derived → generalist default list.
        class Bare:
            name = ""
            rules = []
        t = RedditSubredditResolver().resolve_cohort(Bare(), geos=[])
        assert t["subreddits"] == config.REDDIT_POD_SUBREDDITS["generalist"]


class TestRedditClientGating:
    def test_image_ad_local_fallback_when_disabled(self, monkeypatch):
        monkeypatch.setattr(config, "REDDIT_API_ENABLED", False)
        r = RedditClient().create_image_ad(
            campaign_id="c", image_id="i", headline="h", description="d",
        )
        assert r.status == "local_fallback"
        assert r.error_class == "RedditApiDisabled"

    def test_campaign_group_raises_when_disabled(self, monkeypatch):
        monkeypatch.setattr(config, "REDDIT_API_ENABLED", False)
        try:
            RedditClient().create_campaign_group("x")
            assert False, "expected RuntimeError"
        except RuntimeError as exc:
            assert "REDDIT_API_ENABLED" in str(exc)


class TestRedditClientPhase2:
    """Phase-2 programmatic create payloads (HTTP layer mocked). Mirrors the
    live-validated contract: CONVERSIONS/PAUSED campaign, micros budget +
    community/geo targeting on the ad group, and the post→ad image-ad sequence
    with CTA-enum remapping."""

    def _client(self, monkeypatch):
        monkeypatch.setattr(config, "REDDIT_API_ENABLED", True)
        c = RedditClient()
        c._token = "tok"
        c._account_id = "a2_test"
        return c

    def test_campaign_group_payload(self, monkeypatch):
        c = self._client(monkeypatch)
        captured = {}
        def fake_api(method, path, payload=None):
            captured.update(method=method, path=path, payload=payload)
            return {"id": "111"}
        monkeypatch.setattr(c, "_api", fake_api)
        cid = c.create_campaign_group("My Campaign")
        assert cid == "111"
        assert captured["method"] == "POST" and captured["path"].endswith("/campaigns")
        assert captured["payload"]["objective"] == "CONVERSIONS"
        assert captured["payload"]["configured_status"] == "PAUSED"
        assert captured["payload"]["funding_instrument_id"] == config.REDDIT_FUNDING_INSTRUMENT_ID

    def test_ad_group_budget_and_targeting(self, monkeypatch):
        c = self._client(monkeypatch)
        captured = {}
        monkeypatch.setattr(c, "_api", lambda m, p, payload=None: (captured.update(p=payload) or {"id": "222"}))
        targeting = {"geo_locations": ["us", "CA"], "subreddits": ["programming"],
                     "interests": ["software_v3"], "keywords": [], "pod": "coders"}
        agid = c.create_campaign("AG", "111", targeting, daily_budget_cents=5000)
        assert agid == "222"
        pl = captured["p"]
        assert pl["goal_value"] == 5000 * 10_000          # cents → micros
        assert pl["goal_type"] == "DAILY_SPEND"
        assert pl["configured_status"] == "PAUSED"
        assert pl["bid_value"] == int(config.REDDIT_DEFAULT_BID_USD * 1_000_000)
        assert pl["conversion_pixel_id"] == "a2_test"      # falls back to account id
        assert pl["targeting"]["geolocations"] == ["US", "CA"]
        assert pl["targeting"]["communities"] == ["programming"]
        assert pl["targeting"]["excluded_communities"] == config.REDDIT_EXCLUDED_SUBREDDITS

    def test_ad_group_default_budget_when_unset(self, monkeypatch):
        c = self._client(monkeypatch)
        captured = {}
        monkeypatch.setattr(c, "_api", lambda m, p, payload=None: (captured.update(p=payload) or {"id": "x"}))
        c.create_campaign("AG", "111", {"geo_locations": [], "subreddits": []})
        assert captured["p"]["goal_value"] == config.REDDIT_DEFAULT_DAILY_USD * 1_000_000

    def test_image_ad_post_then_ad_with_cta_remap(self, monkeypatch):
        c = self._client(monkeypatch)
        calls = []
        def fake_api(method, path, payload=None):
            calls.append((path, payload))
            return {"id": "post_t3"} if path.endswith("/posts") else {"id": "ad_999"}
        monkeypatch.setattr(c, "_api", fake_api)
        res = c.create_image_ad(
            campaign_id="222", image_id="https://i.redd.it/x.png",
            headline="H", description="free-form body",
            cta="Get Started",  # invalid Reddit CTA → remap to Sign Up
            destination_url="https://outlier.ai/x", ad_name="My Ad",
        )
        assert res.status == "ok" and res.creative_id == "ad_999"
        post_path, post_pl = calls[0]
        ad_path, ad_pl = calls[1]
        assert post_path.endswith("/posts") and post_pl["type"] == "IMAGE"
        assert "body" not in post_pl                       # IMAGE posts carry no body
        assert post_pl["content"][0]["call_to_action"] == "Sign Up"
        assert ad_path.endswith("/ads")
        assert ad_pl["post_id"] == "post_t3" and ad_pl["ad_group_id"] == "222"
        assert ad_pl["configured_status"] == "PAUSED"

    def test_image_ad_returns_error_result_on_api_failure(self, monkeypatch):
        c = self._client(monkeypatch)
        def boom(*a, **k): raise RuntimeError("400 bad")
        monkeypatch.setattr(c, "_api", boom)
        res = c.create_image_ad(campaign_id="222", image_id="u", headline="h", description="d")
        assert res.status == "error" and "400 bad" in res.error_message

    def test_api_refreshes_ads_token_on_401_and_retries(self, monkeypatch):
        c = self._client(monkeypatch)
        monkeypatch.setattr(config, "REDDIT_REFRESH_TOKEN", "rt")

        class Resp:
            def __init__(self, status):
                self.status_code = status
                self.ok = status < 400
                self.text = '{"data":{"id":"ok"}}' if self.ok else "unauthorized"
            def json(self): return {"data": {"id": "ok"}}

        calls = {"n": 0}
        class FakeSession:
            headers = {}
            def request(self, *a, **k):
                calls["n"] += 1
                return Resp(401) if calls["n"] == 1 else Resp(200)
        c._session = FakeSession()
        refreshed = {"n": 0}
        monkeypatch.setattr("src.reddit_api.refresh_reddit_token",
                            lambda which="ads": refreshed.update(n=refreshed["n"] + 1) or "newtok")
        data = c._api("POST", "/x", {"a": 1})
        assert data == {"id": "ok"}
        assert calls["n"] == 2 and refreshed["n"] == 1   # retried once after refresh

    def test_refresh_reddit_token_returns_new_access(self, monkeypatch):
        import src.reddit_api as r
        monkeypatch.setattr(config, "REDDIT_CLIENT_ID", "cid")
        monkeypatch.setattr(config, "REDDIT_CLIENT_SECRET", "sec")
        monkeypatch.setattr(config, "REDDIT_REFRESH_TOKEN", "rt")
        monkeypatch.setattr(r, "_update_env_tokens", lambda *a: None)

        class Resp:
            ok = True
            def json(self): return {"access_token": "fresh_access"}
        monkeypatch.setattr(r.requests, "post", lambda *a, **k: Resp())
        assert r.refresh_reddit_token("ads") == "fresh_access"

    def test_upload_image_uploads_to_gcs_and_signs(self, monkeypatch, tmp_path):
        c = self._client(monkeypatch)
        monkeypatch.setattr(config, "GCS_CREATIVE_BUCKET", "outlier-reddit-creatives")
        monkeypatch.setattr("src.image_adapter.assert_min_dimensions", lambda *a, **k: None)
        img = tmp_path / "A.png"; img.write_bytes(b"\x89PNG")
        captured = {}

        class FakeBlob:
            def __init__(self, name): self.name = name
            def upload_from_filename(self, path, content_type=None):
                captured.update(path=path, content_type=content_type)
            def generate_signed_url(self, version=None, expiration=None, method=None):
                captured.update(version=version, method=method)
                return f"https://storage.googleapis.com/{self.name}?X-Goog-Signature=abc"
        class FakeBucket:
            def blob(self, name): captured["blob"] = name; return FakeBlob(name)
        class FakeClient:
            def bucket(self, b): captured["bucket"] = b; return FakeBucket()
        monkeypatch.setattr("google.cloud.storage.Client", lambda **k: FakeClient())
        monkeypatch.setattr(
            "google.oauth2.service_account.Credentials.from_service_account_file",
            staticmethod(lambda *a, **k: object()),
        )
        out = c.upload_image(str(img))
        assert out.startswith("https://storage.googleapis.com/") and "X-Goog-Signature" in out
        assert captured["bucket"] == "outlier-reddit-creatives"
        assert captured["blob"].startswith("reddit-creatives/") and captured["blob"].endswith("-A.png")
        assert captured["content_type"] == "image/png"
        assert captured["version"] == "v4" and captured["method"] == "GET"

    def test_upload_image_raises_when_bucket_unset(self, monkeypatch, tmp_path):
        c = self._client(monkeypatch)
        monkeypatch.setattr(config, "GCS_CREATIVE_BUCKET", "")
        monkeypatch.setattr("src.image_adapter.assert_min_dimensions", lambda *a, **k: None)
        img = tmp_path / "A.png"; img.write_bytes(b"\x89PNG")
        try:
            c.upload_image(str(img))
            assert False, "expected RuntimeError"
        except RuntimeError as e:
            assert "GCS_CREATIVE_BUCKET not set" in str(e)


class TestRedditPodConversion:
    def test_unknown_pod_falls_back_to_all(self, monkeypatch):
        monkeypatch.setattr(
            config, "REDDIT_POD_CONVERSION_EVENTS",
            {"all": "WS_all", "coders": "WS_Coders"},
        )
        assert reddit_pod_conversion_event("coders") == "WS_Coders"
        assert reddit_pod_conversion_event("unknown_pod") == "WS_all"
        assert reddit_pod_conversion_event("") == "WS_all"

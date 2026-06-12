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


class TestRedditPodConversion:
    def test_unknown_pod_falls_back_to_all(self, monkeypatch):
        monkeypatch.setattr(
            config, "REDDIT_POD_CONVERSION_EVENTS",
            {"all": "WS_all", "coders": "WS_Coders"},
        )
        assert reddit_pod_conversion_event("coders") == "WS_Coders"
        assert reddit_pod_conversion_event("unknown_pod") == "WS_all"
        assert reddit_pod_conversion_event("") == "WS_all"

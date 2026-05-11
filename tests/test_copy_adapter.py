"""Unit tests for src.copy_adapter — platform-shape rewrite of canonical copy."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

from src.copy_adapter import _truncate, adapt_copy_for_platform


CANONICAL = {
    "angle":          "A",
    "angleLabel":     "Income",
    "headline":       "Earn extra income with Outlier — your expertise is in demand worldwide",
    "subheadline":    "Help train AI on your schedule and earn payment in dollars per task.",
    "intro_text":     "Looking for flexible remote work? Outlier matches you to tasks that fit your skills.",
    "ad_headline":    "Get matched to remote AI tasks",
    "ad_description": "Earn $50/hr. Fully remote. No commute.",
    "photo_subject":  "female South Asian cardiologist seated at a home desk",
    "tgLabel":        "Cardiologists",
    "cta_button":     "APPLY",
}


class TestTruncate:
    def test_under_limit(self):
        assert _truncate("hello world", 30) == "hello world"

    def test_word_boundary(self):
        # "Hello there friend" trimmed to 12 chars hits the space after "there".
        assert _truncate("Hello there friend", 12) == "Hello there"

    def test_hard_cut_when_no_word_boundary(self):
        assert _truncate("supercalifragilisticexpialidocious", 10) == "supercalif"


class TestLinkedInPassthrough:
    def test_passthrough_no_llm(self):
        out = adapt_copy_for_platform(CANONICAL, "linkedin")
        # LinkedIn pass-through MUST not call the LLM.
        assert out is CANONICAL


class TestMetaAdapt:
    def test_meta_uses_llm_then_truncates(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"headline": "Earn $50/hr remote AI tasks", '
                '"primary_text": "Get matched to AI tasks that fit your expertise. Set your own schedule.", '
                '"description": "Apply now", "cta": "APPLY_NOW"}'
            )
            out = adapt_copy_for_platform(CANONICAL, "meta")
        assert out["headline"] == "Earn $50/hr remote AI tasks"
        assert len(out["headline"]) <= 40
        assert out["cta"] == "APPLY_NOW"
        assert out["photo_subject"] == CANONICAL["photo_subject"]   # carried through

    def test_meta_falls_back_on_llm_failure(self):
        with patch("src.copy_adapter.call_claude", side_effect=RuntimeError("API down")):
            out = adapt_copy_for_platform(CANONICAL, "meta")
        # Fallback truncation MUST still produce valid platform-shaped fields.
        assert "headline" in out and "primary_text" in out and "description" in out
        assert len(out["headline"]) <= 40
        assert len(out["description"]) <= 30
        assert out["cta"] in {"APPLY_NOW", "LEARN_MORE", "SIGN_UP", "GET_STARTED"}

    def test_meta_invalid_cta_falls_back(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"headline": "X", "primary_text": "Y", '
                '"description": "Z", "cta": "INVALID_CTA_VALUE"}'
            )
            out = adapt_copy_for_platform(CANONICAL, "meta")
        assert out["cta"] == "LEARN_MORE"


class TestGoogleAdapt:
    def test_google_produces_3_headlines_3_descriptions(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"headlines": ["Earn $50/hr remote", "AI tasks for experts", "Flexible AI work"], '
                '"long_headline": "Earn payment training AI on your schedule and expertise", '
                '"descriptions": ["Get matched.", "No commute.", "Set your own pace."]}'
            )
            out = adapt_copy_for_platform(CANONICAL, "google")
        assert len(out["headlines"]) == 3
        assert len(out["descriptions"]) == 3
        for h in out["headlines"]:
            assert len(h) <= 30
        for d in out["descriptions"]:
            assert len(d) <= 90
        assert len(out["long_headline"]) <= 90

    def test_google_pads_short_lists(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            # LLM only returned 1 headline + 1 description — adapter must pad.
            mock_claude.return_value = (
                '{"headlines": ["Only one"], "long_headline": "longer", '
                '"descriptions": ["one"]}'
            )
            out = adapt_copy_for_platform(CANONICAL, "google")
        assert len(out["headlines"]) == 3
        assert len(out["descriptions"]) == 3

    def test_google_failure_fallback(self):
        with patch("src.copy_adapter.call_claude", side_effect=RuntimeError("API down")):
            out = adapt_copy_for_platform(CANONICAL, "google")
        assert len(out["headlines"]) == 3
        assert len(out["descriptions"]) == 3

"""Unit tests for src.copy_adapter — platform-shape rewrite of canonical copy."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

import config
from src.copy_adapter import _truncate, adapt_copy_for_platform, localize_variant
from src.locales import get_locale

HI = get_locale("hi-in")  # Hindi LocaleTargeting


def _prompt_of(mock_claude) -> str:
    """The user-message prompt string passed to call_claude."""
    return mock_claude.call_args.kwargs["messages"][0]["content"]


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


class TestNewcomerContext:
    """Newcomer-context instruction is unconditional (not gated on locale)."""

    def test_meta_prompt_has_newcomer_block(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = '{"headline":"X","primary_text":"Y","description":"Z","cta":"APPLY_NOW"}'
            adapt_copy_for_platform(CANONICAL, "meta")
        p = _prompt_of(mock_claude)
        assert "NEWCOMER CONTEXT" in p and "heard of Outlier" in p

    def test_google_and_reddit_prompts_have_newcomer_block(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = '{"headlines":["a"],"long_headline":"b","descriptions":["c"]}'
            adapt_copy_for_platform(CANONICAL, "google")
            assert "NEWCOMER CONTEXT" in _prompt_of(mock_claude)
            mock_claude.return_value = '{"title":"a","cta":"Sign Up","freeform_title":"b","freeform_body":"c"}'
            adapt_copy_for_platform(CANONICAL, "reddit")
            assert "NEWCOMER CONTEXT" in _prompt_of(mock_claude)


class TestLocalization:
    """Language block fires for non-LinkedIn channels + a resolved locale only,
    and is gated by LOCALIZE_PLATFORM_COPY."""

    def test_meta_with_locale_injects_language_block(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = '{"headline":"X","primary_text":"Y","description":"Z","cta":"APPLY_NOW"}'
            adapt_copy_for_platform(CANONICAL, "meta", locale=HI)
        p = _prompt_of(mock_claude)
        assert "LANGUAGE REQUIREMENT" in p and "Hindi" in p
        assert "KEEP IN ENGLISH" in p and "$50/hr" in p  # keep-$/USD rule present

    def test_no_language_block_without_locale(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = '{"headline":"X","primary_text":"Y","description":"Z","cta":"APPLY_NOW"}'
            adapt_copy_for_platform(CANONICAL, "meta", locale=None)
        assert "LANGUAGE REQUIREMENT" not in _prompt_of(mock_claude)

    def test_language_block_gated_off_by_config(self):
        with patch("config.LOCALIZE_PLATFORM_COPY", False), \
             patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = '{"headline":"X","primary_text":"Y","description":"Z","cta":"APPLY_NOW"}'
            adapt_copy_for_platform(CANONICAL, "meta", locale=HI)
        assert "LANGUAGE REQUIREMENT" not in _prompt_of(mock_claude)

    def test_linkedin_passthrough_ignores_locale(self):
        # LinkedIn stays English — no LLM, returns the input object unchanged.
        out = adapt_copy_for_platform(CANONICAL, "linkedin", locale=HI)
        assert out is CANONICAL


class TestLocalizeVariant:
    def test_translates_fields(self):
        with patch("src.copy_adapter.call_claude") as mock_claude:
            mock_claude.return_value = (
                '{"headline":"हिंदी विशेषज्ञ चाहिए","subheadline":"घर से काम करें",'
                '"intro_text":"लचीला रिमोट काम","ad_headline":"अभी आवेदन करें",'
                '"ad_description":"$50/hr, पूरी तरह रिमोट"}'
            )
            out = localize_variant(CANONICAL, HI)
        assert out["headline"] == "हिंदी विशेषज्ञ चाहिए"
        assert "$50/hr" in out["ad_description"]
        # photo_subject is an image-gen prompt → must stay English / untouched.
        assert out["photo_subject"] == CANONICAL["photo_subject"]
        # keep-$/USD rule present in the translation prompt.
        assert "KEEP IN ENGLISH" in _prompt_of(mock_claude)

    def test_returns_input_on_llm_failure(self):
        with patch("src.copy_adapter.call_claude", side_effect=RuntimeError("API down")):
            out = localize_variant(CANONICAL, HI)
        assert out is CANONICAL

    def test_noop_without_locale(self):
        out = localize_variant(CANONICAL, None)
        assert out is CANONICAL

    def test_noop_when_config_off(self):
        with patch("config.LOCALIZE_PLATFORM_COPY", False):
            out = localize_variant(CANONICAL, HI)
        assert out is CANONICAL


# ── InMail localization (localize_inmail) ──────────────────────────────────────
from src.copy_adapter import localize_inmail  # noqa: E402


def test_localize_inmail_english_and_none_are_noop():
    # English locale → no translation, no LLM call.
    with patch("src.copy_adapter.call_claude") as m:
        class _EN:
            display_language = "English"
        assert localize_inmail("Subj", "Body text", _EN()) == ("Subj", "Body text")
        assert localize_inmail("Subj", "Body text", None) == ("Subj", "Body text")
        m.assert_not_called()


def test_localize_inmail_translates_non_english():
    with patch("src.copy_adapter.call_claude") as m:
        m.return_value = '{"subject": "Asunto ES", "body": "Cuerpo ES $5.50/hr"}'
        subj, body = localize_inmail("English subject", "English body $5.50/hr", get_locale("es-mx"))
        assert subj == "Asunto ES" and body == "Cuerpo ES $5.50/hr"
        # prompt names the language + keeps $/USD rule
        prompt = _prompt_of(m)
        assert "Spanish" in prompt and "$" in prompt


def test_localize_inmail_llm_failure_keeps_english():
    with patch("src.copy_adapter.call_claude", side_effect=RuntimeError("boom")):
        assert localize_inmail("Subj", "Body", get_locale("es-mx")) == ("Subj", "Body")

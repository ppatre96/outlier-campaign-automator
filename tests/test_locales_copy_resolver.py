"""Unit tests for the copy-localization helpers in src.locales:
resolve_copy_locale + locale_brand_voice_notes."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.locales import resolve_copy_locale, locale_brand_voice_notes


class _Cohort:
    def __init__(self, facet_strength=None):
        self.facet_strength = facet_strength or {}


class TestResolveCopyLocale:
    def test_generalist_locale_facet(self):
        c = _Cohort({"generalist_locale": "hi-in"})
        lt = resolve_copy_locale(c, None)
        assert lt is not None and lt.display_language == "Hindi"

    def test_icp_language_pref_bcp47(self):
        lt = resolve_copy_locale(None, {"language_pref": "hi-IN"})
        assert lt is not None and lt.display_language == "Hindi"

    def test_icp_language_pref_dataclass_like(self):
        class ICP:
            language_pref = "bn-IN"
        lt = resolve_copy_locale(None, ICP())
        assert lt is not None and lt.display_language == "Bengali"

    def test_facet_takes_precedence_over_icp(self):
        c = _Cohort({"generalist_locale": "de-de"})
        lt = resolve_copy_locale(c, {"language_pref": "hi-IN"})
        assert lt.display_language == "German"

    def test_english_returns_none(self):
        for code in ("en-US", "en-GB", "en-IN", "en", "", None):
            assert resolve_copy_locale(None, {"language_pref": code}) is None

    def test_unknown_locale_returns_none(self):
        assert resolve_copy_locale(_Cohort({"generalist_locale": "xx-yy"}), None) is None

    def test_no_signal_returns_none(self):
        assert resolve_copy_locale(_Cohort(), None) is None
        assert resolve_copy_locale(None, None) is None


class TestLocaleBrandVoiceNotes:
    def test_known_language(self):
        note = locale_brand_voice_notes("Hindi")
        assert "हिन्दी" in note and "Outlier" in note

    def test_unknown_language_generic_fallback(self):
        note = locale_brand_voice_notes("Klingon")
        assert note == "Write all copy in Klingon. 'Outlier' stays in English."

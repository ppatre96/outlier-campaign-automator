"""Every GMR-0023 i18n locale must be recognized by get_locale.

If a locale is missing from src/locales.LOCALES, is_generalist_cohort() fails
its "known locale" check and the cohort falls through to Stage-A beam discovery —
mining noise cohorts (english_certificate_b2, phd_student, …) instead of
targeting the locale. That's exactly what happened to es-MX (fixed 2026-06-09).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from src.locales import get_locale

# The 14 locale cohorts GMR-0023 (Multimango i18n) defines in Smart Ramp.
GMR0023_LOCALES = [
    "bn-in", "de-de", "fr-fr", "hi-in", "id-id", "it-it", "ko-kr",
    "pt-br", "th-th", "tl-ph", "vi-vn", "zh-cn", "ar-eg", "es-mx",
]


@pytest.mark.parametrize("loc", GMR0023_LOCALES)
def test_locale_recognized(loc):
    lt = get_locale(loc)
    assert lt is not None, f"{loc} missing from LOCALES → cohort would beam noise"
    assert lt.locale == loc
    assert lt.meta_locale_id is not None
    assert lt.google_language_const is not None


def test_generalist_cohort_takes_locale_path_for_es_mx():
    import main
    row = {
        "cohort_description": "Mexican Spanish generalist contributors",
        "matched_locales": ["es-mx"],
        "included_geos": ["MX"],
    }
    assert main.is_generalist_cohort(row) == "es-mx"

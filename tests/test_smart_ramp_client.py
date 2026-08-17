"""Unit tests for src.smart_ramp_client cohort parsing (locale backfill)."""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _parse(raw: dict):
    """Call _parse_cohort without touching __init__ (no env / HTTP needed)."""
    from src.smart_ramp_client import SmartRampClient
    return SmartRampClient.__new__(SmartRampClient)._parse_cohort(raw)


def test_matched_locales_backfilled_from_language_code():
    """GMR-0027 shape: matched_locales empty, language code present.

    Smart Ramp's own campaign naming resolves this cohort's locale to "en", so
    the client has to as well — otherwise the console has no locale to lock and
    main.py's ONLY_LOCALES filter drops the cohort, launching nothing.
    """
    spec = _parse({
        "id": "7e128f8f", "cohort_description": "Personal finance CBs",
        "matched_locales": [], "job_post_language_code": "en",
    })
    assert spec.matched_locales == ["en"]


def test_existing_matched_locales_win_over_language_code():
    spec = _parse({
        "id": "bn", "matched_locales": ["bn-IN"], "job_post_language_code": "en",
    })
    assert spec.matched_locales == ["bn-IN"]


def test_no_locale_and_no_language_code_stays_empty():
    """Nothing to invent — the notifier reports this as a blocker instead."""
    spec = _parse({"id": "x", "matched_locales": [], "job_post_language_code": ""})
    assert not spec.matched_locales

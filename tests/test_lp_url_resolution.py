"""Tests for LP URL resolution via marketing-maintained inventory sheet.

`config.LP_URL_BY_DOMAIN` now stores `{matched_domain: slug}` instead of
`{matched_domain: full_url}`. `resolve_base_lp_url` looks up the slug in
`sheets_client.read_lp_url_map()` (a flattened `{slug: full_url}` view of
the Outlier Landing Pages sheet) to get the live URL. Back-compat: a value
that already starts with `http` is used as-is and never queries the sheet.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utm_builder import resolve_base_lp_url


# ── Path 1: Smart Ramp campaign_state takes precedence over everything ────────

def test_campaign_state_url_wins_over_sheet():
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(return_value={"qfinance": "https://from-sheet.example/"})

    with patch.dict("config.LP_URL_BY_DOMAIN", {"Finance": "qfinance"}, clear=False):
        url = resolve_base_lp_url(
            campaign_state={"utm_linkedin": {"base_url": "https://from-campaign-state.example/"}},
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance",
            sheets_client=sheets,
        )
    assert url == "https://from-campaign-state.example/"
    sheets.read_lp_url_map.assert_not_called()


def test_google_uses_utm_joveo_block():
    """Smart Ramp's Google-channel base URL lives under `utm_joveo`, not
    `utm_google`. resolve_base_lp_url must map platform="google" to that key."""
    url = resolve_base_lp_url(
        campaign_state={"utm_joveo": {"base_url": "https://joveo.example/"}},
        platform="google",
        fallback="https://fallback.example/",
    )
    assert url == "https://joveo.example/"


# ── Path 2a: full URL in LP_URL_BY_DOMAIN still works (back-compat) ───────────

def test_back_compat_full_url_in_domain_map():
    """A value in LP_URL_BY_DOMAIN that already starts with http is used as-is.
    The sheet is NOT queried — useful for one-off LPs that aren't in the
    inventory yet."""
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock()

    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Custom Vertical": "https://outlier.ai/custom/lp"},
        clear=False,
    ):
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Custom Vertical",
            sheets_client=sheets,
        )
    assert url == "https://outlier.ai/custom/lp"
    sheets.read_lp_url_map.assert_not_called()


# ── Path 2b: slug in LP_URL_BY_DOMAIN → sheet lookup ──────────────────────────

def test_slug_resolved_via_sheet():
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(return_value={
        "qfinance": "https://outlier.ai/experts/qfinance",
        "experts/qfinance": "https://outlier.ai/experts/qfinance",
    })

    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Finance & Quantitative Analysis": "qfinance"},
        clear=False,
    ):
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance & Quantitative Analysis",
            sheets_client=sheets,
        )
    assert url == "https://outlier.ai/experts/qfinance"


def test_slug_with_leading_path_segment_resolves():
    """A slug like "/experts/qfinance" should also resolve — the lookup
    tries the trailing leaf first ("qfinance") and falls back to the full
    slug stripped of its leading slash."""
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(return_value={
        "qfinance": "https://outlier.ai/experts/qfinance",
    })

    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Finance": "/experts/qfinance"},
        clear=False,
    ):
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance",
            sheets_client=sheets,
        )
    assert url == "https://outlier.ai/experts/qfinance"


# ── Path 3: fallback paths ────────────────────────────────────────────────────

def test_slug_not_in_sheet_falls_back():
    """If the slug from LP_URL_BY_DOMAIN isn't in the sheet (e.g. the LP was
    moved to Draft), resolve_base_lp_url must fall through to the fallback
    URL rather than emit a broken link."""
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(return_value={})  # empty sheet

    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Finance": "qfinance"},
        clear=False,
    ):
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance",
            sheets_client=sheets,
        )
    assert url == "https://fallback.example/"


def test_no_sheets_client_skips_sheet_leg():
    """Replay scripts and unit tests can call resolve_base_lp_url without a
    SheetsClient; the sheet leg of path 2 is silently skipped, and a slug-
    style value falls through to the fallback (a full URL still resolves)."""
    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Finance": "qfinance", "OldDomain": "https://old.example/lp"},
        clear=False,
    ):
        # Slug path → fallback (no sheets_client, no resolution possible)
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance",
            sheets_client=None,
        )
        assert url == "https://fallback.example/"

        # Full-URL path → resolves even without sheets_client
        url2 = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="OldDomain",
            sheets_client=None,
        )
        assert url2 == "https://old.example/lp"


def test_unknown_matched_domain_returns_fallback():
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(return_value={"qfinance": "https://x.example/"})

    url = resolve_base_lp_url(
        campaign_state=None,
        platform="linkedin",
        fallback="https://fallback.example/",
        matched_domain="Definitely Not A Real Domain",
        sheets_client=sheets,
    )
    assert url == "https://fallback.example/"


def test_sheet_read_failure_falls_back_silently():
    """If read_lp_url_map raises (sheet permissions, transient API error),
    we must not crash the ad-creation flow — fall through to the fallback URL."""
    sheets = MagicMock()
    sheets.read_lp_url_map = MagicMock(side_effect=RuntimeError("Drive API 500"))

    with patch.dict(
        "config.LP_URL_BY_DOMAIN",
        {"Finance": "qfinance"},
        clear=False,
    ):
        url = resolve_base_lp_url(
            campaign_state=None,
            platform="linkedin",
            fallback="https://fallback.example/",
            matched_domain="Finance",
            sheets_client=sheets,
        )
    assert url == "https://fallback.example/"

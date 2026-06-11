"""Regression tests for the Smart Ramp v2 LinkedIn naming convention +
InMail body formatting. Locks the format the reviewer required on GMR-0024
(2026-06-11): channel-manager facet/lang/format segments from
campaign_state.linkedin, run date LAST, "Message ads" (not "Inmail")."""
from __future__ import annotations

import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from src.campaign_name import build_campaign_name  # noqa: E402
from src.linkedin_api import _inmail_html_body      # noqa: E402

# GMR-0024 campaign_state.linkedin block (verified via Smart Ramp get_ramp).
_GMR0024_STATE = {
    "linkedin": {
        "liTargetingFacet": "BLV",
        "liAdLanguage": "EN",
        "liAdFormat": "Message ads",
        "groupingType": "none",
    }
}

_COMMON = dict(
    ramp_id="GMR-0024",
    pod="specialist",
    domain="Media & Communications",
    locale="en-US",
    included_geos=["US"],
    campaign_state=_GMR0024_STATE,
)


def _strip_date(name: str) -> str:
    """Drop the trailing ' | MM/DD/YYYY' (today, non-deterministic)."""
    return re.sub(r" \| \d{2}/\d{2}/\d{4}$", "", name)


def test_inmail_name_matches_smart_ramp_format():
    name = build_campaign_name(platform="linkedin", campaign_type="inmail", **_COMMON)
    assert _strip_date(name) == (
        "Scale-GMR-0024 | LinkedIn | specialist | Media & Communications "
        "| en-US | US | ALL | BLV | EN | Message ads"
    )
    # Date is LAST and present.
    assert re.search(r" \| \d{2}/\d{2}/\d{4}$", name)
    # Legacy hardcoded "Inmail" label must be gone.
    assert "Inmail" not in name


def test_inmail_group_name_uses_override_format():
    grp = build_campaign_name(
        platform="linkedin", campaign_type="inmail",
        format_override="InMail Group", **_COMMON,
    )
    assert "| BLV | EN | InMail Group | " in grp


def test_language_falls_back_to_locale_when_blank():
    state = {"linkedin": {"liTargetingFacet": "", "liAdLanguage": "",
                          "liAdFormat": "", "groupingType": "none"}}
    name = build_campaign_name(
        ramp_id="GMR-0099", platform="linkedin", campaign_type="inmail",
        pod="coders", domain="General", locale="fr-CA",
        included_geos=["CA"], campaign_state=state,
    )
    # No facet segment when blank; language derived from locale (fr-CA → FR).
    assert "| BLV |" not in name
    assert "| FR | Message ads | " in name


def test_meta_name_unchanged_legacy_order():
    """Meta/Google keep the legacy 8-segment order (date before geo-tier);
    they must NOT pick up the LinkedIn facet/lang/format segments."""
    name = build_campaign_name(platform="meta", campaign_type="static", **_COMMON)
    assert _strip_date(re.sub(r" \| ALL$", "", name)) == (
        "Scale-GMR-0024 | Meta | specialist | Media & Communications | en-US | US"
    )
    assert "BLV" not in name


def test_inmail_html_body_strips_br_and_wraps_paragraphs():
    body = "First para.<br><br>Second para.\n\nThird para."
    html = _inmail_html_body(body)
    assert "<br>" not in html
    assert html == "<p>First para.</p><p>Second para.</p><p>Third para.</p>"


def test_inmail_html_body_escapes_and_caps_length():
    # HTML-special chars escaped; output never exceeds the 1000-char cap.
    out = _inmail_html_body("A & B <test>")
    assert "&amp;" in out and "&lt;test&gt;" in out
    assert len(_inmail_html_body("para. \n\n" * 400)) <= 1000

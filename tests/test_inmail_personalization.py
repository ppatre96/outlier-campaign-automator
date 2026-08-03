"""InMail personalization, the in-body CTA, and the HTML footer.

Reviewer feedback 2026-08-03: add "Hi firstname," to Message Ads, put a CTA in
the body as well as on the button, and fix the privacy notice that "appears with
typos and mistakes".

The footer was never misspelled. `customFooter` is rendered as HTML and the
pipeline was sending the policy as plain text with 27 newlines, so every break
collapsed into one run-on wall with stray "- " hyphens mid-sentence. Confirmed by
reading a live hand-built Message Ad back (adInMailContent:146413153), whose
footer is stored as `<p>…</p><p><br></p><ul><li>…</li></ul>` and reads correctly.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.inmail_copy_writer import (  # noqa: E402
    INMAIL_FIRSTNAME_MACRO, INMAIL_GREETING, _fallback_body, _fallback_subject,
    ensure_closing_cta, ensure_greeting,
)
from src.linkedin_api import _inmail_html_body, _inmail_html_footer  # noqa: E402


# ── Greeting ──────────────────────────────────────────────────────────────────

def test_greeting_is_the_upper_case_macro_on_its_own_line():
    """LinkedIn only expands %FIRSTNAME% in capitals, and it renders as "LinkedIn
    Member" when personalization is off — so it stands alone and no following
    sentence leans on the name."""
    assert INMAIL_GREETING == "Hi %FIRSTNAME%,"
    out = ensure_greeting("Your background in cardiology is what AI needs.")
    assert out.startswith("Hi %FIRSTNAME%,\n\n")
    assert out.replace("Hi %FIRSTNAME%,", "").strip().startswith("Your background")


def test_greeting_is_left_alone_when_already_correct():
    body = "Hi %FIRSTNAME%,\n\nYour background.\n\nClick Apply now to start."
    assert ensure_greeting(body) == body


@pytest.mark.parametrize("bad_open", [
    "Hi Sarah,", "Hello Dr. Chen,", "Hey there,", "Dear Colleague:", "Hi [Name],",
])
def test_a_concrete_or_placeholder_name_is_replaced_with_the_macro(bad_open):
    """A real name would be wrong for everyone but one recipient, and "[Name]"
    ships a literal placeholder."""
    out = ensure_greeting(f"{bad_open}\n\nYour background in tax is relevant.")
    assert out.startswith(INMAIL_GREETING)
    assert bad_open not in out
    assert out.count("%FIRSTNAME%") == 1


def test_lower_case_macro_is_corrected():
    """%firstname% does not expand — it would render literally in the inbox."""
    for variant in ("%firstname%", "%FirstName%", "%first name%"):
        out = ensure_greeting(f"Hi {variant},\n\nYour background.")
        assert INMAIL_FIRSTNAME_MACRO in out
        assert variant not in out


def test_greeting_survives_an_empty_body():
    assert ensure_greeting("") == INMAIL_GREETING
    assert ensure_greeting(None) == INMAIL_GREETING


# ── In-body CTA ───────────────────────────────────────────────────────────────

def test_a_body_with_no_ask_gets_a_closing_cta():
    out = ensure_closing_cta("Hi %FIRSTNAME%,\n\nYour background is relevant.")
    assert out.strip().split("\n")[-1].startswith("Click Apply now")


def test_an_existing_cta_is_not_duplicated():
    for closing in ("Click Apply now to see the tasks.",
                    "Apply now and get started this week.",
                    "Sign up to see what is open."):
        body = f"Hi %FIRSTNAME%,\n\nYour background.\n\n{closing}"
        assert ensure_closing_cta(body) == body


def test_the_closing_cta_never_promises_a_timeline():
    """"get started in less than 7 days" is a claim about screening speed that
    nothing in this pipeline can substantiate (Pranav's example, 2026-08-03)."""
    out = ensure_closing_cta("Hi %FIRSTNAME%,\n\nBody.")
    for claim in ("7 day", "24 hour", "48 hour", "instantly", "immediately", "same day"):
        assert claim not in out.lower(), out


# ── Fallbacks (they ship without the LLM, so they must satisfy the rules) ─────

@pytest.mark.parametrize("angle", ["A", "B", "C"])
def test_every_fallback_body_has_both_elements_and_clean_vocabulary(angle):
    from src.copy_design_qc import scan_brand_voice

    body = _fallback_body("cardiology", angle)
    assert body.startswith(INMAIL_GREETING)
    assert "apply now" in body.lower().split("\n")[-1]
    assert not scan_brand_voice(body, "body"), body


@pytest.mark.parametrize("angle", ["A", "B", "C"])
def test_every_fallback_subject_is_clean(angle):
    """Angle C used to read "Earn on your schedule — no fixed shifts", breaking
    two of this module's own rules: "schedule" is banned and rule 6 bans em
    dashes."""
    from src.copy_design_qc import scan_brand_voice

    subj = _fallback_subject("cardiology", angle)
    assert "—" not in subj
    assert len(subj) <= 60
    assert not scan_brand_voice(subj, "subject"), subj


# ── HTML rendering ────────────────────────────────────────────────────────────

def test_footer_plain_text_becomes_structured_html():
    """This is the actual bug: newlines collapse when rendered as HTML."""
    plain = (
        "Personal Information We Collect\n\n"
        "When we talk about personal information we mean a broad range.\n\n"
        "- Registration information: your name and country.\n"
        "- Survey information: responses to surveys.\n\n"
        "Privacy Policy: https://tryoutlier.com/legal-pages/privacy-policy"
    )
    out = _inmail_html_footer(plain)
    assert "\n" not in out, "a newline in HTML renders as a space — nothing may rely on it"
    assert out.startswith("<p>Personal Information We Collect</p>")
    assert out.count("<li>") == 2, "bullets must be list items, not stray hyphens"
    assert "- Registration" not in out
    assert "<p><br></p>" in out, "blank lines need an explicit spacer paragraph"
    assert out.endswith("legal-pages/privacy-policy</p>")


def test_footer_passes_through_markup_untouched():
    """A Doppler override can carry legal's own HTML."""
    html_footer = "<p>Terms</p><ul><li>One</li></ul>"
    assert _inmail_html_footer(html_footer) == html_footer


def test_footer_empty_stays_empty():
    assert _inmail_html_footer("") == ""
    assert _inmail_html_footer(None) == ""


def test_the_real_configured_footer_renders_with_structure():
    import config

    out = _inmail_html_footer(config.LINKEDIN_INMAIL_FOOTER)
    assert out.count("<li>") >= 10, "the policy's bullet lists must survive"
    assert "\n" not in out
    assert len(out) <= 20000, "LinkedIn's customFooter limit"
    # All four policy links get their own line rather than running together.
    for slug in ("community-guidelines", "terms-of-use", "cookies-policy", "privacy-policy"):
        assert f"{slug}</p>" in out


def test_body_truncation_keeps_the_greeting_and_the_cta():
    """Truncating from the end would delete the ask, which is the one paragraph
    that must survive."""
    filler = "x" * 300
    body = "\n\n".join([INMAIL_GREETING, filler, filler, filler, filler, filler,
                        "Click Apply now to see the current tasks."])
    out = _inmail_html_body(body)
    assert len(out) <= 1500 + len("<p></p>") + 60
    assert out.startswith(f"<p>{INMAIL_GREETING}</p>")
    assert out.endswith("<p>Click Apply now to see the current tasks.</p>")


def test_body_within_the_limit_keeps_every_paragraph():
    body = f"{INMAIL_GREETING}\n\nOne.\n\nTwo.\n\nClick Apply now to start."
    out = _inmail_html_body(body)
    assert out.count("<p>") == 4


def test_the_macro_survives_html_escaping():
    """html.escape must not mangle the percent signs."""
    out = _inmail_html_body(f"{INMAIL_GREETING}\n\nBody.")
    assert "%FIRSTNAME%" in out


# ── Validator ─────────────────────────────────────────────────────────────────

def test_validator_flags_a_missing_greeting_and_a_missing_cta():
    from src.copy_design_qc import validate_inmail_copy

    v = validate_inmail_copy(
        "Cardiologists earn $50/hr shaping AI",
        "Your expertise is relevant.\n\nIt would be nice if you took a look sometime.",
        "Apply now",
    )
    assert any("%FIRSTNAME%" in x for x in v)
    assert any("call to action" in x for x in v)


def test_validator_flags_a_lower_case_macro_specifically():
    from src.copy_design_qc import validate_inmail_copy

    v = validate_inmail_copy(
        "Subject", "Hi %firstname%,\n\nBody.\n\nClick Apply now to start.", "Apply now",
    )
    assert any("lower-case" in x for x in v)


def test_validator_is_quiet_on_a_correct_body():
    from src.copy_design_qc import validate_inmail_copy

    body = (
        f"{INMAIL_GREETING}\n\n"
        "Your ability to interpret ECG waveforms is exactly what AI developers cannot "
        "replicate on their own, and it is in short supply right now.\n\n"
        "Outlier matches domain experts with AI tasks: reviewing model outputs, rating "
        "responses, and generating examples in your own field. Everything is remote and "
        "async, and you choose your own hours. Payment is made weekly in USD.\n\n"
        "A typical task is evaluating AI-generated ECG interpretations for accuracy, "
        "flagging what a clinician would catch and a model would miss. You take on as "
        "many or as few as you like, with no minimum commitment and nothing to hand back "
        "on a deadline.\n\n"
        "Click Apply now to see the current tasks and get started."
    )
    v = validate_inmail_copy("Cardiologists earn $50/hr shaping AI", body, "Apply now")
    assert v == [], v

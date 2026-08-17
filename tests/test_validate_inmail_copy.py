"""
Tests for validate_inmail_copy in src/copy_design_qc.py.

Structural QC for InMail (Message Ad) variants — mirrors validate_copy_lengths
for static ads but scoped to InMail fields: subject char limits, body char/word
limits, LinkedIn hard limits, format violations, brand voice.
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.copy_design_qc import validate_inmail_copy  # noqa: E402


def _body(words: int = 115) -> str:
    """Generate a clean body of exactly `words` words.

    Includes the %FIRSTNAME% greeting and a closing CTA, both required since
    2026-08-03 — validate_inmail_copy emits a SOFT violation without them, so a
    fixture missing either is no longer "clean".
    """
    from src.inmail_copy_writer import INMAIL_GREETING

    closing = "Click Apply now to see the current tasks."
    filler = words - len(INMAIL_GREETING.split()) - len(closing.split())
    body = " ".join(f"word{i}" for i in range(max(filler, 1)))
    return f"{INMAIL_GREETING}\n\n{body}\n\n{closing}"


# ── Subject line ─────────────────────────────────────────────────────────────

def test_subject_clean_passes():
    # "schedule" is a banned token — use approved phrasing
    assert validate_inmail_copy("Cardiologists: earn $50/hr, set your own hours", _body()) == []


def test_subject_over_soft_limit_is_soft():
    long_subj = "A" * 61
    viols = validate_inmail_copy(long_subj, _body())
    assert any("[SOFT]" in v and "Subject" in v for v in viols)
    assert not any("[HARD]" in v and "Subject" in v for v in viols)


def test_subject_over_hard_limit_is_hard():
    long_subj = "A" * 201
    viols = validate_inmail_copy(long_subj, _body())
    assert any("[HARD]" in v and "Subject" in v for v in viols)


def test_subject_exactly_at_soft_limit_passes():
    subj = "A" * 60
    viols = validate_inmail_copy(subj, _body())
    assert not any("Subject" in v for v in viols)


# ── Body word count ────────────────────────────────────────────────────────────

def test_body_too_short_is_soft():
    viols = validate_inmail_copy("Good subject", _body(50))
    assert any("[SOFT]" in v and "too short" in v for v in viols)


def test_body_too_long_is_soft():
    viols = validate_inmail_copy("Good subject", _body(200))
    assert any("[SOFT]" in v and "words" in v and "SOFT" in v for v in viols)


def test_body_word_count_in_range_passes():
    for w in (100, 115, 130):
        viols = validate_inmail_copy("Good subject", _body(w))
        assert not any("words" in v and ("too short" in v or "underperform" in v) for v in viols), \
            f"False positive at {w} words: {viols}"


# ── LinkedIn hard limit on body ────────────────────────────────────────────────

def test_body_over_linkedin_hard_limit_is_hard():
    huge_body = "word " * 400  # ~2000 chars
    viols = validate_inmail_copy("Subject", huge_body)
    assert any("[HARD]" in v and "1900" in v for v in viols)


def test_body_just_under_hard_limit_no_hard_viol():
    ok_body = "a " * 940  # ~1880 chars, under 1900
    viols = validate_inmail_copy("Subject", ok_body)
    assert not any("[HARD]" in v and "1900" in v for v in viols)


# ── Format violations ─────────────────────────────────────────────────────────

def test_bullet_points_flagged():
    body_with_bullets = "First paragraph.\n- bullet one\n- bullet two\nEnd."
    viols = validate_inmail_copy("Subject", body_with_bullets)
    assert any("bullet" in v.lower() for v in viols)


def test_markdown_headers_flagged():
    body_with_header = "Intro.\n## Why Outlier\nMore text here."
    viols = validate_inmail_copy("Subject", body_with_header)
    assert any("header" in v.lower() for v in viols)


def test_plain_paragraphs_no_format_violations():
    clean_body = "First paragraph here.\n\nSecond paragraph here with no special formatting."
    viols = validate_inmail_copy("Subject", clean_body * 5)
    assert not any("bullet" in v.lower() or "header" in v.lower() for v in viols)


# ── CTA label ─────────────────────────────────────────────────────────────────

def test_cta_over_limit_is_hard():
    viols = validate_inmail_copy("Subject", _body(), cta_label="A" * 21)
    assert any("[HARD]" in v and "CTA" in v for v in viols)


def test_cta_at_limit_passes():
    viols = validate_inmail_copy("Subject", _body(), cta_label="A" * 20)
    assert not any("CTA" in v for v in viols)


def test_cta_empty_no_violation():
    viols = validate_inmail_copy("Subject", _body(), cta_label="")
    assert not any("CTA" in v for v in viols)


# ── Brand voice scan ──────────────────────────────────────────────────────────

def test_em_dash_in_subject_flagged():
    viols = validate_inmail_copy("Cardiologists — earn $50/hr", _body())
    assert any("em dash" in v.lower() and "subject" in v.lower() for v in viols)


def test_em_dash_in_body_flagged():
    body = _body(50) + " — this is banned " + _body(50)
    viols = validate_inmail_copy("Good subject", body)
    assert any("em dash" in v.lower() and "body" in v.lower() for v in viols)


def test_banned_vocab_in_body_flagged():
    body = _body(50) + " this is a job " + _body(50)
    viols = validate_inmail_copy("Good subject", body)
    assert any("job" in v.lower() for v in viols)


def test_clean_copy_all_pass():
    # Uses approved Outlier vocabulary (no "schedule", no em dashes, no banned tokens)
    subject = "Cardiologists: earn $50/hr, no fixed hours"
    body = (
        # Greeting + closing CTA are required since 2026-08-03.
        "Hi %FIRSTNAME%,\n\n"
        "Your ability to read ECG waveforms is exactly what AI developers need right now. "
        "Outlier pays cardiologists to review AI-generated clinical content from home. "
        "No fixed hours, no minimum commitment. We send payment every week. "
        "Typical tasks include rating AI responses to cardiology case questions, "
        "flagging clinical inaccuracies, and drafting expert answers to cardiac scenarios. "
        "You log in when it suits you and complete as much or as little as you want. "
        "Getting started takes under ten minutes: complete a brief screening, "
        "become familiar with project guidelines, then pick up tasks at your own pace. "
        "Outlier has paid over $500M to contributors worldwide. This opportunity is open now.\n\n"
        "Click Apply now to see the current tasks and get started."
    )
    cta = "See Open Tasks"
    viols = validate_inmail_copy(subject, body, cta)
    assert viols == [], f"Expected no violations, got: {viols}"


# ─────────────────────────────────────────────────────────────────────────────
# Fallback copy must read as English — no raw classify_tg() codes, no truncation
# ─────────────────────────────────────────────────────────────────────────────


def _cohort(rules):
    from types import SimpleNamespace
    return SimpleNamespace(name="c", rules=rules, lift_pp=0)


def test_fallback_never_ships_raw_category_code():
    """GMR-0027 shipped "Your GENERAL expertise is in demand" to LinkedIn when the
    LLM was down: the fallback interpolated classify_tg()'s raw code."""
    from src.inmail_copy_writer import _fallback_subject, _fallback_body
    cohort = _cohort([("skills__trading", "1"), ("highest_degree_level__Masters", "1")])
    for angle in ("F", "A", "B", "C"):
        subject = _fallback_subject("GENERAL", angle, cohort)
        body = _fallback_body("GENERAL", angle, cohort)
        assert "GENERAL" not in subject and "GENERAL" not in body
        assert len(subject) <= 60, f"{angle} subject over LinkedIn's cap: {subject!r}"


def test_fallback_derives_field_from_cohort_signals():
    from src.inmail_copy_writer import _fallback_subject, _fallback_body
    cohort = _cohort([("experience_band__10plus", "1"), ("skills__internal_controls", "1")])
    assert _fallback_subject("GENERAL", "A", cohort) == "Your internal controls expertise is in demand"
    assert "background in internal controls" in _fallback_body("GENERAL", "A", cohort)


def test_known_category_uses_written_out_label():
    from src.inmail_copy_writer import _fallback_subject
    assert _fallback_subject("ML_ENGINEER", "A", _cohort([])) == "Your machine learning expertise is in demand"


def test_unusable_signal_falls_back_to_generic_not_a_stutter():
    """Degree/accreditation-only cohorts have no field phrase that reads well.
    "domain expert expertise" and a mid-word truncation are both regressions."""
    from src.inmail_copy_writer import _fallback_subject
    for rules in ([("accreditations_norm__certified_public_accountant", "1")],
                  [("highest_degree_level__Phd", "1")],
                  []):
        a = _fallback_subject("GENERAL", "A", _cohort(rules))
        b = _fallback_subject("GENERAL", "B", _cohort(rules))
        assert a == "Your expertise is in demand", a
        assert b == "Domain experts are tasking on Outlier", b
        assert "expert expertise" not in a


def test_no_cohort_at_all_still_renders():
    from src.inmail_copy_writer import _fallback_subject, _fallback_body
    assert _fallback_subject("GENERAL", "A") == "Your expertise is in demand"
    assert "your field" in _fallback_body("GENERAL", "A")

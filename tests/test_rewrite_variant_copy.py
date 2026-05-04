"""
Tests for the polished rewrite_variant_copy in src/figma_creative.py.

Replays the actual violations observed in the GMR-0005 dry run (2026-04-28)
to assert the rewriter produces copy that passes the copy_design_qc gate
WITHOUT depending on a live LLM (mocks the LiteLLM client).

The deterministic scrub layer is the safety net: even if the LLM produces
copy that still contains em dashes or banned tokens, the post-process must
catch them before the variant is returned. These tests pin that contract.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture
def base_variant():
    """A representative ad variant with all 5 copy fields populated."""
    return {
        "angle": "A",
        "angleLabel": "Expertise",
        "photo_subject": "female South Asian compliance researcher",
        "headline": "AI needs your legal eye.",
        "subheadline": "Compliance skills earn real payment.",
        "intro_text": "Reviewing regulatory language all day?",
        "ad_headline": "Get paid for your legal expertise.",
        "ad_description": "Earn $25-50 USD per hour. Fully remote.",
    }


def _mock_llm_returning(rewritten_fields: dict):
    """Return a mock call_claude function that returns the given JSON object as the rewrite."""
    import json as _json
    return lambda messages, **kw: _json.dumps(rewritten_fields)


# ─────────────────────────────────────────────────────────────────────────────
# Field detection — the bug surfaced because the rewriter only knew about
# headline + subheadline. After the polish, every violated field must be detected.
# ─────────────────────────────────────────────────────────────────────────────


def test_violated_fields_detected_for_intro_text():
    from src.figma_creative import _violated_fields_from_messages
    violations = ['intro_text contains em dash (banned in contributor copy): "..."']
    assert _violated_fields_from_messages(violations) == {"intro_text"}


def test_violated_fields_detected_for_ad_headline():
    from src.figma_creative import _violated_fields_from_messages
    violations = ["ad_headline contains banned token 'training': 'AI training jobs need you'"]
    assert _violated_fields_from_messages(violations) == {"ad_headline"}


def test_violated_fields_detected_for_capitalized_headline():
    """validate_copy_lengths emits 'Headline has X words' (capitalized) — must still match."""
    from src.figma_creative import _violated_fields_from_messages
    violations = ["Headline wraps to 3 lines (max 2): 'AI needs compliance expertise.'"]
    assert _violated_fields_from_messages(violations) == {"headline"}


def test_field_detector_does_not_match_substring():
    """`headline` inside `subheadline` or `ad_headline` must not falsely match."""
    from src.figma_creative import _violated_fields_from_messages
    violations = ["subheadline contains banned token 'work': 'Compliance work, real payment.'"]
    fields = _violated_fields_from_messages(violations)
    assert fields == {"subheadline"}, f"expected only subheadline, got {fields}"


def test_violated_fields_detects_multiple():
    from src.figma_creative import _violated_fields_from_messages
    violations = [
        "intro_text contains em dash (banned)",
        "ad_headline contains banned token 'training'",
    ]
    assert _violated_fields_from_messages(violations) == {"intro_text", "ad_headline"}


# ─────────────────────────────────────────────────────────────────────────────
# Deterministic scrub — the safety net for LLM drift.
# These tests do NOT need a mock LLM; they exercise the regex layer directly.
# ─────────────────────────────────────────────────────────────────────────────


def test_scrub_strips_bare_em_dash():
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    s = "AI models need exactly that—someone who spots issues fast."
    out = _scrub_banned_tokens_and_dashes(s)
    assert "—" not in out


def test_scrub_strips_spaced_em_dash():
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    s = "Reviewing regulatory language — someone who spots issues."
    out = _scrub_banned_tokens_and_dashes(s)
    assert "—" not in out
    # Expect a single space, not double (the " — " pattern matches before " " replacement)
    assert "  " not in out


def test_scrub_replaces_banned_tokens_case_insensitive():
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    s = "AI Training projects — get paid bonus per Performance review."
    out = _scrub_banned_tokens_and_dashes(s)
    assert "training" not in out.lower()
    assert "bonus" not in out.lower()
    assert "performance" not in out.lower()
    assert "—" not in out
    # Replacements landed
    assert "project guidelines" in out.lower()
    assert "reward" in out.lower()
    assert "progress" in out.lower()


def test_scrub_does_not_break_partial_matches():
    """`compensate` shouldn't be touched by the `compensation` rule (whole-word boundary)."""
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    s = "We compensate top performers." # 'performers' — neither banned token is a whole word
    out = _scrub_banned_tokens_and_dashes(s)
    # 'compensate' has 'compensation' as prefix — should NOT be replaced
    assert "compensate" in out


def test_scrub_idempotent():
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    s = "Some text — with bonus and training."
    once = _scrub_banned_tokens_and_dashes(s)
    twice = _scrub_banned_tokens_and_dashes(once)
    assert once == twice


def test_scrub_handles_empty_and_none():
    from src.figma_creative import _scrub_banned_tokens_and_dashes
    assert _scrub_banned_tokens_and_dashes("") == ""
    assert _scrub_banned_tokens_and_dashes(None) is None


# ── 2026-05-04 GMR-0006 fix: ban 'Outlier' in headline/subheadline only ──

def test_outlier_scrubbed_from_headline(monkeypatch):
    """Rewriter strips 'Outlier' from headline post-LLM (the wordmark is
    composited in the bottom strip, repeating it triggers duplicate-logo QC)."""
    from src.figma_creative import rewrite_variant_copy
    import src.figma_creative as fc
    import json as _json
    monkeypatch.setattr(fc, "call_claude",
        lambda messages, **kw: _json.dumps({"headline": "Cardiologists Earn with Outlier"}))
    variant = {"angle": "B", "headline": "OLD", "subheadline": "x"}
    out = rewrite_variant_copy(variant, ["headline contains banned token 'Outlier': 'Cardiologists Earn with Outlier'"])
    assert "Outlier" not in out["headline"]
    # Should be cleaned to "Cardiologists Earn with" (Outlier dropped, trailing space stripped)
    assert "Cardiologists" in out["headline"]


def test_outlier_kept_in_intro_text(monkeypatch):
    """Rewriter does NOT strip 'Outlier' from intro_text (legitimate use)."""
    from src.figma_creative import rewrite_variant_copy
    import src.figma_creative as fc
    import json as _json
    monkeypatch.setattr(fc, "call_claude",
        lambda messages, **kw: _json.dumps({"intro_text": "Cardiologists across Europe earn with Outlier."}))
    variant = {"angle": "B", "intro_text": "OLD"}
    out = rewrite_variant_copy(variant, ["intro_text contains em dash"])
    assert "Outlier" in out["intro_text"]


def test_scan_brand_voice_flags_outlier_in_headline_only():
    """scan_brand_voice flags 'Outlier' for headline+subheadline, not other fields."""
    from src.copy_design_qc import scan_brand_voice
    text = "Cardiologists Earn with Outlier"
    assert any("Outlier" in v for v in scan_brand_voice(text, "headline"))
    assert any("Outlier" in v for v in scan_brand_voice(text, "subheadline"))
    assert not any("Outlier" in v for v in scan_brand_voice(text, "intro_text"))
    assert not any("Outlier" in v for v in scan_brand_voice(text, "ad_headline"))
    assert not any("Outlier" in v for v in scan_brand_voice(text, "ad_description"))


# ─────────────────────────────────────────────────────────────────────────────
# End-to-end rewriter — the big bug. Replays GMR-0005's actual em-dash-in-intro_text
# violation and asserts the returned variant has NO em dashes anywhere.
# ─────────────────────────────────────────────────────────────────────────────


def test_rewriter_strips_em_dash_from_intro_text_even_if_llm_returns_it(base_variant):
    """The exact GMR-0005 failure mode — LLM returns a rewrite that STILL has an em dash.
    The deterministic scrub layer must catch it before the variant is returned.
    """
    from src import figma_creative as F

    # LLM "rewrite" still contains an em dash (modeling the LLM-drift case)
    bad_rewrite = {
        "intro_text": "Reviewing regulatory language all day? AI models need exactly that — someone fast."
    }
    base_variant["intro_text"] = "Reviewing regulatory language all day? AI models need exactly that — someone who spots what's wrong fast."
    violations = ['intro_text contains em dash (banned in contributor copy): "..."']

    with patch("src.figma_creative.call_claude", side_effect=_mock_llm_returning(bad_rewrite)):
        result = F.rewrite_variant_copy(base_variant, violations)

    assert "—" not in result["intro_text"], \
        f"em dash leaked through despite scrub: {result['intro_text']!r}"


def test_rewriter_handles_ad_headline_banned_token(base_variant):
    """ad_headline previously was completely ignored by the rewriter."""
    from src import figma_creative as F

    base_variant["ad_headline"] = "Your legal eye is exactly what AI training projects need"
    violations = ["ad_headline contains banned token 'training': '...'"]

    # LLM "rewrite" honors the request
    good_rewrite = {"ad_headline": "Your legal eye is exactly what AI projects need"}

    with patch("src.figma_creative.call_claude", side_effect=_mock_llm_returning(good_rewrite)):
        result = F.rewrite_variant_copy(base_variant, violations)

    # Even if the LLM had failed to remove 'training', the scrub layer would replace it.
    assert "training" not in result["ad_headline"].lower()


def test_rewriter_scrubs_all_fields_not_just_violated_one(base_variant):
    """Even fields the LLM didn't touch must be scrubbed — em dashes can land in any field."""
    from src import figma_creative as F

    # subheadline has a banned token, but the violation only flags intro_text.
    base_variant["subheadline"] = "Get paid for compensation expertise."
    base_variant["intro_text"]   = "Some — text."
    violations = ['intro_text contains em dash (banned)']

    # LLM only rewrites intro_text (per the prompt's targeted ask).
    targeted_rewrite = {"intro_text": "Some text."}

    with patch("src.figma_creative.call_claude", side_effect=_mock_llm_returning(targeted_rewrite)):
        result = F.rewrite_variant_copy(base_variant, violations)

    # subheadline was NOT in fields_to_rewrite, but the scrub layer must still catch
    # 'compensation' there.
    assert "compensation" not in result["subheadline"].lower(), \
        f"scrub missed compensation in subheadline: {result['subheadline']!r}"
    assert "payment" in result["subheadline"].lower()


def test_rewriter_hard_truncates_when_llm_overshoots(base_variant):
    """If the LLM returns a rewrite that's STILL too long, last-resort truncation kicks in."""
    from src import figma_creative as F

    base_variant["headline"] = "AI needs your compliance expertise badly please help."  # too long
    violations = ["Angle A headline has 9 words (max 6): '...'"]

    # LLM rewrites but still overshoots the char limit
    bad_rewrite = {"headline": "AI Needs Your Legal Compliance Eye And Expertise Right Now Please."}  # 67 chars

    with patch("src.figma_creative.call_claude", side_effect=_mock_llm_returning(bad_rewrite)):
        result = F.rewrite_variant_copy(base_variant, violations)

    # 40-char hard limit must be enforced
    assert len(result["headline"]) <= 40, \
        f"headline exceeded 40 chars after truncation: {len(result['headline'])} ({result['headline']!r})"


def test_rewriter_falls_back_when_llm_fails(base_variant):
    """If the LLM call raises, the scrub layer must still run on the original copy."""
    from src import figma_creative as F

    base_variant["intro_text"] = "Some — text with bonus."
    violations = ['intro_text contains em dash']

    def _raise(**kw):
        raise RuntimeError("simulated Claude 5xx")

    with patch("src.figma_creative.call_claude", side_effect=_raise):
        result = F.rewrite_variant_copy(base_variant, violations)

    # Even without LLM, the scrub layer catches the em dash + banned token
    assert "—" not in result["intro_text"]
    assert "bonus" not in result["intro_text"].lower()


# ─────────────────────────────────────────────────────────────────────────────
# Integration check — assert the output of the rewriter passes
# copy_design_qc.validate_copy_lengths (the same gate that fails in production).
# ─────────────────────────────────────────────────────────────────────────────


def test_rewriter_output_passes_validate_copy_lengths(base_variant):
    """The end-to-end contract: rewriter output must pass the QC gate."""
    from src import figma_creative as F
    from src.copy_design_qc import validate_copy_lengths

    # Reconstruct the actual GMR-0005 failure: em dash on intro_text + banned token on ad_headline
    base_variant["intro_text"]  = "AI models need someone who spots issues fast — that's a rare skill."
    base_variant["ad_headline"] = "Your legal eye is exactly what AI training projects need"
    violations = [
        'intro_text contains em dash (banned)',
        "ad_headline contains banned token 'training': '...'",
    ]

    # LLM produces a clean rewrite
    good_rewrite = {
        "intro_text": "AI models need someone who spots issues fast.",
        "ad_headline": "Your legal eye is what AI projects need",
    }

    with patch("src.figma_creative.call_claude", side_effect=_mock_llm_returning(good_rewrite)):
        result = F.rewrite_variant_copy(base_variant, violations)

    # The QC gate must accept the rewritten variant
    qc_violations = validate_copy_lengths(
        headline=result["headline"],
        subheadline=result["subheadline"],
        intro_text=result.get("intro_text", ""),
        ad_headline=result.get("ad_headline", ""),
        ad_description=result.get("ad_description", ""),
        cta_button="LEARN_MORE",  # known-good CTA
    )
    assert qc_violations == [], \
        f"rewritten variant still fails QC: {qc_violations}"

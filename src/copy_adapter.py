"""
Platform copy adapter — converts a canonical (LinkedIn-shaped) copy variant
into the shape each ad platform needs.

The pipeline produces copy in the LinkedIn shape today (headline up to ~70
chars, subheadline ~200, intro_text ~140, ad_headline ~70, ad_description
~100). Meta and Google have tighter, structurally different formats:

  - Meta:    headline ≤40, primary_text ≤125, description ≤30 (single feed ad).
  - Google:  3 short headlines ≤30 each, 1 long_headline ≤90, 3 descriptions
             ≤90 each (Responsive Display Ads).

`adapt_copy_for_platform()` is the single entry point. For LinkedIn it's a
pass-through. For Meta/Google it asks Claude to rewrite the canonical copy
into the platform-shaped fields, with the platform's banned-word + tone
rules baked in. Output is always a dict — keys differ per platform.

Failures are non-fatal: if the LLM call fails or returns malformed JSON,
the function returns a deterministic truncation of the input copy so the
pipeline can still create ads (degraded but functional).
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from src.ad_platform import PlatformConstraints, get_constraints
from src.claude_client import call_claude

log = logging.getLogger(__name__)


# Vocabulary rules from CLAUDE.md (Outlier brand voice). Mirrors the rules
# already encoded in figma_creative.rewrite_variant_copy(); duplicated here
# rather than imported so the adapter is self-contained when copy_adapter is
# the entry point (e.g., Meta arm doesn't go through figma_creative at all).
_BANNED_VOCAB_BLOCK = """\
RESTRICTED VOCABULARY (BANNED — never use):
- "compensation" -> use "payment"
- "interview" -> use "screening"
- "bonus" -> use "reward"
- "performance" -> use "progress"
- "training" -> use "project guidelines"
- "promote" -> use "eligible to work on review-level tasks"
- "Discourse" -> use "Outlier Community"
- "assign" -> use "match"
- "instructions" -> use "project guidelines"
- "work", "schedule", "job", "role", "required" -> use NEUTRAL synonyms (task, availability, opportunity)
- NO em dashes (—) or en dashes (–). Use commas, periods, or colons.
- NEVER include the word "Outlier" in headlines/short fields — the brand
  wordmark is composited separately on image creatives, and Meta/Google
  surface a clearly labelled brand line.
"""


# ── Public API ───────────────────────────────────────────────────────────────


def _icp_block(icp) -> str:
    """
    Build a short ICP context block to inject into platform-specific copy
    rewrites. Returns "" when icp is falsy. Mirrors the longer block in
    figma_creative.build_copy_variants but trimmed because the adapter LLM
    is doing a constrained rewrite, not generating from scratch.
    """
    if not icp:
        return ""

    def _g(key: str, default=""):
        if isinstance(icp, dict):
            return icp.get(key, default)
        return getattr(icp, key, default)

    cohort_desc = _g("cohort_description", "")
    motivations = _g("top_motivations", []) or []
    liberty = _g("creative_liberty", "medium") or "medium"
    lang = _g("language_pref", "") or ""
    drivers = _g("decision_drivers", []) or []
    if not any([cohort_desc, motivations, drivers]):
        return ""
    return (
        "\nCOHORT ICP — preserve this when rewriting:\n"
        f"- Audience: {cohort_desc or '(unknown)'}\n"
        f"- Top motivations: {', '.join(motivations) or '(none)'}\n"
        f"- Decision drivers: {', '.join(drivers) or '(none)'}\n"
        f"- Creative liberty: {liberty} "
        "(high=bold/witty allowed; medium=clear+credible; low=corporate-safe, no humor)\n"
        f"- Language: {lang or '(default en-US)'}\n"
        "Keep the rewrite faithful to the canonical copy's angle but recalibrate "
        "tone + emphasis against the ICP above.\n"
    )


def adapt_copy_for_platform(
    variant: dict,
    platform: str,
    *,
    icp = None,
) -> dict:
    """Return a platform-shaped copy dict ready for that platform's
    `create_image_ad()` call.

    For "linkedin" this is the input variant unchanged (LinkedIn is the
    canonical shape). For "meta" / "google" this calls Claude to rewrite the
    variant into the platform-specific fields with the right char limits.

    Output schemas:

      linkedin: {
        headline, subheadline, photo_subject,
        intro_text, ad_headline, ad_description, cta_button, ...
      }   # unchanged

      meta: {
        headline:       str,  # ≤40 chars
        primary_text:   str,  # ≤125 chars
        description:    str,  # ≤30 chars
        cta:            str,  # Meta CTA enum, e.g. "LEARN_MORE"
        photo_subject:  str,  # carried through for image gen
        # plus passthrough fields used by image_adapter
      }

      google: {
        headlines:       list[str],  # 3 strings, each ≤30 chars
        long_headline:   str,        # ≤90 chars
        descriptions:    list[str],  # 3 strings, each ≤90 chars
        photo_subject:   str,
        # plus passthrough
      }
    """
    if platform == "linkedin":
        return variant

    constraints = get_constraints(platform)
    if platform == "meta":
        return _adapt_for_meta(variant, constraints, icp=icp)
    if platform in ("google", "google_search"):
        # RSA (Search) uses the same headline/description copy shape as Display
        # RDA; the constraints differ (per-field limits) but the adapter logic
        # is shared. Routing google_search here fixes the arm aborting with
        # "unknown platform 'google_search'" before any Search campaign was made.
        return _adapt_for_google(variant, constraints, icp=icp)
    if platform == "reddit":
        return _adapt_for_reddit(variant, constraints, icp=icp)
    raise ValueError(f"adapt_copy_for_platform: unknown platform {platform!r}")


# ── Meta ─────────────────────────────────────────────────────────────────────


def _adapt_for_meta(variant: dict, c: PlatformConstraints, *, icp = None) -> dict:
    """LLM-rewrite the canonical variant into Meta's headline + primary_text +
    description fields. Falls back to deterministic truncation on LLM failure."""
    headline_in    = (variant.get("headline") or "").strip()
    subhead_in     = (variant.get("subheadline") or "").strip()
    intro_in       = (variant.get("intro_text") or "").strip()
    ad_headline_in = (variant.get("ad_headline") or "").strip()
    ad_desc_in     = (variant.get("ad_description") or "").strip()

    prompt = f"""\
Rewrite the following Outlier ad copy for a Meta (Facebook/Instagram) feed image ad.
Output STRICT JSON only — no markdown fences, no commentary.

HARD CHARACTER LIMITS (inclusive — Meta truncates anything longer):
- headline:     MAX {c.headline_max_chars} characters. Big bold text under the image. 1 line.
- primary_text: MAX {c.primary_text_max_chars} characters. The body copy above the image. 1-2 sentences.
- description:  MAX {c.description_max_chars} characters. Small text under the headline.
- cta:          One of: APPLY_NOW, LEARN_MORE, SIGN_UP, GET_STARTED. Pick the most natural fit.

{_BANNED_VOCAB_BLOCK}

CANONICAL COPY (rewrite preserving angle + specificity):
- headline:       {headline_in!r}
- subheadline:    {subhead_in!r}
- intro_text:     {intro_in!r}
- ad_headline:    {ad_headline_in!r}
- ad_description: {ad_desc_in!r}
{_icp_block(icp)}
Return ONLY this JSON shape:
{{"headline": "...", "primary_text": "...", "description": "...", "cta": "..."}}
"""
    out: dict[str, Any] = {}
    try:
        raw = call_claude(messages=[{"role": "user", "content": prompt}], max_tokens=400)
        out = _extract_json(raw) or {}
    except Exception as exc:
        log.warning("Meta copy adapter LLM failed (%s) — using deterministic truncation", exc)

    headline    = _truncate(out.get("headline")    or headline_in,    c.headline_max_chars)
    primary     = _truncate(out.get("primary_text") or intro_in or subhead_in, c.primary_text_max_chars or 125)
    description = _truncate(out.get("description") or subhead_in or ad_desc_in, c.description_max_chars)
    cta         = (out.get("cta") or "LEARN_MORE").upper()
    if cta not in {"APPLY_NOW", "LEARN_MORE", "SIGN_UP", "GET_STARTED"}:
        cta = "LEARN_MORE"

    return {
        # Carry forward fields the image adapter / pipeline still needs.
        "angle":         variant.get("angle"),
        "angleLabel":    variant.get("angleLabel"),
        "photo_subject": variant.get("photo_subject"),
        "tgLabel":       variant.get("tgLabel"),
        # Platform-shaped output:
        "headline":      headline,
        "primary_text":  primary,
        "description":   description,
        "cta":           cta,
    }


# ── Google ───────────────────────────────────────────────────────────────────


def _adapt_for_google(variant: dict, c: PlatformConstraints, *, icp = None) -> dict:
    """LLM-rewrite the canonical variant into Google RDA (Responsive Display
    Ads) fields: 3 short headlines, 1 long headline, 3 descriptions."""
    headline_in    = (variant.get("headline") or "").strip()
    subhead_in     = (variant.get("subheadline") or "").strip()
    intro_in       = (variant.get("intro_text") or "").strip()
    ad_headline_in = (variant.get("ad_headline") or "").strip()
    ad_desc_in     = (variant.get("ad_description") or "").strip()

    n_h = c.headline_count
    n_d = c.description_count
    long_max = c.long_headline_max_chars or 90

    prompt = f"""\
Rewrite the following Outlier ad copy for a Google Responsive Display Ad (RDA).
RDAs auto-mix headlines + descriptions + image, so we need MULTIPLE distinct
short headlines and descriptions for Google's optimizer to test.
Output STRICT JSON only — no markdown fences, no commentary.

HARD CHARACTER LIMITS:
- headlines:      EXACTLY {n_h} short headlines. Each MAX {c.headline_max_chars} characters. Distinct angles.
- long_headline:  ONE headline. MAX {long_max} characters. The expanded version of the headline.
- descriptions:   EXACTLY {n_d} descriptions. Each MAX {c.description_max_chars} characters. Distinct value props.

{_BANNED_VOCAB_BLOCK}

CANONICAL COPY (rewrite preserving angle + specificity):
- headline:       {headline_in!r}
- subheadline:    {subhead_in!r}
- intro_text:     {intro_in!r}
- ad_headline:    {ad_headline_in!r}
- ad_description: {ad_desc_in!r}
{_icp_block(icp)}
Return ONLY this JSON shape:
{{"headlines": ["...", "...", "..."], "long_headline": "...", "descriptions": ["...", "...", "..."]}}
"""
    out: dict[str, Any] = {}
    try:
        raw = call_claude(messages=[{"role": "user", "content": prompt}], max_tokens=600)
        out = _extract_json(raw) or {}
    except Exception as exc:
        log.warning("Google copy adapter LLM failed (%s) — using deterministic truncation", exc)

    # Headlines: pad/trim to exactly n_h items, each within char limit.
    raw_headlines = out.get("headlines") or [headline_in, ad_headline_in or headline_in, subhead_in or headline_in]
    headlines = [_truncate((h or "").strip(), c.headline_max_chars) for h in raw_headlines][:n_h]
    while len(headlines) < n_h:
        headlines.append(_truncate(headline_in, c.headline_max_chars))

    long_headline = _truncate(
        out.get("long_headline") or subhead_in or headline_in,
        long_max,
    )

    raw_descs = out.get("descriptions") or [subhead_in, intro_in, ad_desc_in]
    descriptions = [_truncate((d or "").strip(), c.description_max_chars) for d in raw_descs][:n_d]
    while len(descriptions) < n_d:
        descriptions.append(_truncate(subhead_in or intro_in, c.description_max_chars))

    return {
        "angle":         variant.get("angle"),
        "angleLabel":    variant.get("angleLabel"),
        "photo_subject": variant.get("photo_subject"),
        "tgLabel":       variant.get("tgLabel"),
        # Platform-shaped output:
        "headlines":     headlines,
        "long_headline": long_headline,
        "descriptions":  descriptions,
    }


# ── Reddit ───────────────────────────────────────────────────────────────────

# Reddit promoted-ad CTA buttons we allow (human-readable; the programmatic
# Phase-2 enum value is verified against the Reddit Ads API v3 ref at build
# time). Recruitment-appropriate subset.
_REDDIT_CTAS = {"Sign Up", "Apply Now", "Learn More", "Get Started"}
# Soft cap for the free-form/native post body. Reddit allows ~40k chars, but a
# native, high-performing recruitment post is short — keep it tight.
_REDDIT_FREEFORM_MAX = 1200


def _adapt_for_reddit(variant: dict, c: PlatformConstraints, *, icp=None) -> dict:
    """LLM-rewrite the canonical variant into BOTH Reddit ad formats:

      - image ad:  a promoted-post `title` (the headline over/with the image)
                   + a `cta` button label.
      - free-form: a native, Reddit-voiced text post (`freeform_title` +
                   `freeform_body`) — conversational, peer-to-peer, value-first,
                   NOT corporate (Redditors distrust ad-speak).

    Returns one dict carrying both forms. Falls back to deterministic
    truncation of the canonical copy on LLM failure, and runs a brand-voice
    banned-term pass on the free-form body (which bypasses the image QC path)."""
    headline_in    = (variant.get("headline") or "").strip()
    subhead_in     = (variant.get("subheadline") or "").strip()
    intro_in       = (variant.get("intro_text") or "").strip()
    ad_headline_in = (variant.get("ad_headline") or "").strip()
    ad_desc_in     = (variant.get("ad_description") or "").strip()

    prompt = f"""\
Rewrite the following Outlier ad copy for Reddit. Produce TWO formats in one JSON object.
Output STRICT JSON only — no markdown fences, no commentary.

Reddit culture: users distrust corporate/marketing tone. Write like a knowledgeable
peer posting in a niche community, not a recruiter. Lead with a concrete outcome or
an honest, specific value prop. No hype, no buzzwords, no exclamation spam.

FIELDS + HARD LIMITS:
- title:          MAX {c.headline_max_chars} characters. The promoted IMAGE-ad post title. Specific + concrete; name the work + the payment.
- cta:            One of: Sign Up, Apply Now, Learn More, Get Started. Pick the most natural.
- freeform_title: MAX {c.headline_max_chars} characters. The native TEXT-post title — can be slightly more conversational/curious than `title`.
- freeform_body:  MAX {_REDDIT_FREEFORM_MAX} characters. A short native Reddit post body: 2-4 short paragraphs, peer voice, concrete task + payment + flexibility, one clear closing line pointing to the link. Plain text, no markdown headers, no bullet spam.

{_BANNED_VOCAB_BLOCK}

CANONICAL COPY (rewrite preserving angle + specificity):
- headline:       {headline_in!r}
- subheadline:    {subhead_in!r}
- intro_text:     {intro_in!r}
- ad_headline:    {ad_headline_in!r}
- ad_description: {ad_desc_in!r}
{_icp_block(icp)}
Return ONLY this JSON shape:
{{"title": "...", "cta": "...", "freeform_title": "...", "freeform_body": "..."}}
"""
    out: dict[str, Any] = {}
    try:
        raw = call_claude(messages=[{"role": "user", "content": prompt}], max_tokens=800)
        out = _extract_json(raw) or {}
    except Exception as exc:
        log.warning("Reddit copy adapter LLM failed (%s) — using deterministic truncation", exc)

    title          = _truncate(out.get("title") or headline_in or ad_headline_in, c.headline_max_chars)
    freeform_title = _truncate(out.get("freeform_title") or title, c.headline_max_chars)
    freeform_body  = _truncate(
        out.get("freeform_body") or " ".join(p for p in [intro_in, subhead_in, ad_desc_in] if p),
        _REDDIT_FREEFORM_MAX,
    )
    cta = (out.get("cta") or "Sign Up").strip().title()
    if cta not in _REDDIT_CTAS:
        cta = "Sign Up"

    # Brand-voice safety net on the free-form body + titles (they skip the
    # image-creative QC path). Best-effort — never blocks ad generation.
    try:
        from src.brand_voice_validator import BrandVoiceValidator
        _bv = BrandVoiceValidator()
        title, _ = _bv.rewrite_banned_terms(title)
        freeform_title, _ = _bv.rewrite_banned_terms(freeform_title)
        freeform_body, _ = _bv.rewrite_banned_terms(freeform_body)
    except Exception as exc:  # pragma: no cover - defensive
        log.debug("Reddit banned-term rewrite skipped (non-fatal): %s", exc)

    return {
        "angle":          variant.get("angle"),
        "angleLabel":     variant.get("angleLabel"),
        "photo_subject":  variant.get("photo_subject"),
        "tgLabel":        variant.get("tgLabel"),
        # Image-ad form:
        "title":          title,
        "cta":            cta,
        # Free-form / native text-post form:
        "freeform_title": freeform_title,
        "freeform_body":  freeform_body,
    }


# ── Helpers ──────────────────────────────────────────────────────────────────


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    # Truncate at last whitespace before the limit when possible — avoids
    # ugly mid-word cuts. Fallback to hard cut if no whitespace exists.
    cut = s[:n]
    idx = cut.rfind(" ")
    if idx > n * 0.6:  # only use word-boundary if it doesn't lose too much
        return cut[:idx].rstrip()
    return cut.rstrip()


def _extract_json(text: str) -> dict | None:
    """Pull the first JSON object out of a possibly-fenced LLM response."""
    if not text:
        return None
    text = text.strip()
    # Strip ```json fences if present
    if text.startswith("```"):
        text = re.sub(r"^```(?:json)?\s*", "", text)
        text = re.sub(r"\s*```$", "", text)
    # Find the first { ... } block
    m = re.search(r"\{.*\}", text, flags=re.DOTALL)
    if not m:
        return None
    try:
        return json.loads(m.group(0))
    except json.JSONDecodeError:
        return None

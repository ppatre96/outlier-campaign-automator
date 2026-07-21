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

import config
from src.ad_platform import PlatformConstraints, get_constraints
from src.claude_client import call_claude
from src.locales import LocaleTargeting, locale_brand_voice_notes

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


# Keep-in-English rule shared by the localization prompt + the per-platform
# language block. "$"/currency/numerals carry trust for international audiences
# (and a translated rate figure is a correctness risk), so they stay verbatim.
_KEEP_ENGLISH_RULE = (
    'KEEP IN ENGLISH / UNTRANSLATED: the brand name "Outlier"; the "$" sign, all '
    "currency symbols, USD, and every numeral / rate figure (e.g. \"$50/hr\" stays "
    '"$50/hr" exactly — never translate, localize, or convert the number or symbol). '
    "Translate everything else."
)


def _newcomer_context_block(carrier_field: str) -> str:
    """Instruction (channel-agnostic) telling the model to make `carrier_field`
    legible to someone who has never heard of Outlier. Applied unconditionally —
    a separate concern from localization."""
    return f"""
NEWCOMER CONTEXT — assume the reader has NEVER heard of Outlier. The {carrier_field}
must make these clear without jargon: (1) Outlier is a platform where domain experts
earn payment doing flexible, remote AI training tasks; (2) the concrete task THIS
reader would do, tied to their expertise — name the actual task (e.g. "review
AI-generated ECG readings for accuracy"), never a vague "help improve AI"; (3)
payment is remote, flexible, and paid hourly in USD. Specificity beats hype. A bare
line like "Hindi experts paid in USD" is a FAILURE — it tells a newcomer nothing
about what they'd actually do.
"""


def _language_block(locale, fields_desc: str) -> str:
    """Per-platform 'write in {language}' block, gated on config + a resolved
    locale. The input copy is ALREADY localized by `localize_variant`, so this
    instructs the reshaping LLM to PRESERVE the language (not revert to English)
    and restates the keep-$/USD rule. Returns "" when localization is off."""
    if not locale or not config.LOCALIZE_PLATFORM_COPY:
        return ""
    lang = getattr(locale, "display_language", "") or str(locale)
    return f"""
## LANGUAGE REQUIREMENT — MANDATORY
The copy below is already written in {lang}. Keep ALL output fields ({fields_desc})
in {lang} — do NOT translate back to English. English-language ads land at a
fraction of native CTR for this audience.
{_KEEP_ENGLISH_RULE}
Locale brand-voice rules for {lang}:
{locale_brand_voice_notes(lang)}
"""


# Variant text fields that carry ad copy (translated by localize_variant).
# photo_subject is an image-generation prompt, not ad copy → stays English.
_LOCALIZABLE_FIELDS = ("headline", "subheadline", "intro_text", "ad_headline", "ad_description")


def localize_variant(variant: dict, locale) -> dict:
    """Translate a canonical copy variant's text fields into the target locale's
    language, in ONE LLM call, keeping "$"/USD/numerals + "Outlier" in English.

    This is what localizes the IMAGE OVERLAY (the per-channel PNG is composed
    from headline/subheadline of the returned variant) as well as the canonical
    text the platform adapter then reshapes. Scoped by the caller to non-LinkedIn
    channels + locale-defined cohorts.

    Returns a NEW dict (shallow copy with localized fields). On any LLM/parse
    failure returns the input unchanged — degraded (English) but functional.
    """
    if not locale or not config.LOCALIZE_PLATFORM_COPY:
        return variant
    lang = getattr(locale, "display_language", "") or str(locale)
    present = {f: (variant.get(f) or "").strip() for f in _LOCALIZABLE_FIELDS}
    present = {f: v for f, v in present.items() if v}
    # Bottom-band descriptive line: derive the English banner from the (still-
    # English) subheadline + rate via the same rate-safe helper the compositor uses
    # (never invents a rate), then translate it alongside the copy in this ONE call.
    # The $X/hr pay-rate token stays English (_KEEP_ENGLISH_RULE). Compositor reads
    # variant['banner_line']; without this the band stayed hardcoded English while
    # the rest of the creative was localized.
    try:
        from src.gemini_creative import derive_bottom_text
        _en_banner = derive_bottom_text(
            variant.get("subheadline", ""), variant.get("advertised_rate", "")
        ).strip()
    except Exception:  # noqa: BLE001 — never block localization on the banner
        _en_banner = ""
    if _en_banner:
        present["banner_line"] = _en_banner
    if not present:
        return variant

    prompt = f"""\
Translate the following Outlier ad copy fields into {lang}. Preserve each field's
meaning, angle, and approximate length. Output STRICT JSON only — same keys, no
markdown fences, no commentary.

{_KEEP_ENGLISH_RULE}

Locale brand-voice rules for {lang}:
{locale_brand_voice_notes(lang)}

FIELDS (JSON):
{json.dumps(present, ensure_ascii=False, indent=2)}

Return ONLY a JSON object with the SAME keys, values translated into {lang}."""
    try:
        raw = call_claude(messages=[{"role": "user", "content": prompt}], max_tokens=700)
        out = _extract_json(raw) or {}
    except Exception as exc:
        log.warning("localize_variant LLM failed (%s) — keeping English copy", exc)
        return variant
    if not isinstance(out, dict) or not out:
        log.warning("localize_variant returned no JSON — keeping English copy")
        return variant

    localized = dict(variant)
    changed = 0
    for f in present:
        val = out.get(f)
        if isinstance(val, str) and val.strip():
            localized[f] = val.strip()
            changed += 1
    log.info("localize_variant → %s: localized %d/%d fields", lang, changed, len(present))
    return localized


def localize_inmail(subject: str, body: str, locale) -> tuple[str, str]:
    """Translate a LinkedIn InMail subject + body into the target locale's
    language in ONE LLM call, keeping "$"/USD/numerals + "Outlier" in English and
    preserving the identity-first hook, angle, and ~length.

    Unlike `localize_variant` this is NOT gated on config — it's driven by the
    per-ramp `localize_inmail` decision flag (console Review tab). No-op (returns
    the inputs unchanged) for English locales or on any LLM/parse failure, so a
    failure degrades to the English InMail rather than breaking the send.
    """
    lang = (getattr(locale, "display_language", "") or "").strip() if locale else ""
    if not lang or lang.lower() == "english":
        return subject, body
    # Only localize into a language LinkedIn Ads can actually target — otherwise
    # a translated InMail can't be delivered to that audience. (EU geo/consent
    # is a separate concern handled at targeting time, not here.)
    from src.locales import linkedin_supports_language
    if not linkedin_supports_language(lang):
        log.info("localize_inmail: %s not a LinkedIn-supported ad language — keeping English", lang)
        return subject, body
    present = {"subject": (subject or "").strip(), "body": (body or "").strip()}
    if not present["body"]:
        return subject, body

    prompt = f"""\
Translate the following Outlier LinkedIn InMail into {lang}. Preserve the meaning,
the identity-first opening hook, the angle, and the approximate length (subject
≤60 chars, body 100–130 words). Output STRICT JSON only — keys "subject" and
"body", no markdown fences, no commentary.

{_KEEP_ENGLISH_RULE}

Locale brand-voice rules for {lang}:
{locale_brand_voice_notes(lang)}

INMAIL (JSON):
{json.dumps(present, ensure_ascii=False, indent=2)}

Return ONLY a JSON object with keys "subject" and "body", translated into {lang}."""
    try:
        raw = call_claude(messages=[{"role": "user", "content": prompt}], max_tokens=900)
        out = _extract_json(raw) or {}
    except Exception as exc:  # noqa: BLE001
        log.warning("localize_inmail LLM failed (%s) — keeping English InMail", exc)
        return subject, body
    if not isinstance(out, dict):
        return subject, body
    s, b = out.get("subject"), out.get("body")
    new_subject = s.strip() if isinstance(s, str) and s.strip() else subject
    new_body = b.strip() if isinstance(b, str) and b.strip() else body
    log.info("localize_inmail → %s: localized subject+body", lang)
    return new_subject, new_body


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
    locale = None,
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
        # LinkedIn stays English (2026-06-17 decision) — pass-through, locale ignored.
        return variant

    constraints = get_constraints(platform)
    if platform == "meta":
        return _adapt_for_meta(variant, constraints, icp=icp, locale=locale)
    if platform in ("google", "google_search"):
        # RSA (Search) uses the same headline/description copy shape as Display
        # RDA; the constraints differ (per-field limits) but the adapter logic
        # is shared. Routing google_search here fixes the arm aborting with
        # "unknown platform 'google_search'" before any Search campaign was made.
        return _adapt_for_google(variant, constraints, icp=icp, locale=locale)
    if platform == "reddit":
        return _adapt_for_reddit(variant, constraints, icp=icp, locale=locale)
    raise ValueError(f"adapt_copy_for_platform: unknown platform {platform!r}")


# ── Meta ─────────────────────────────────────────────────────────────────────


def _adapt_for_meta(variant: dict, c: PlatformConstraints, *, icp = None, locale = None) -> dict:
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
{_language_block(locale, "headline, primary_text, description")}
{_newcomer_context_block("primary_text")}
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


def _adapt_for_google(variant: dict, c: PlatformConstraints, *, icp = None, locale = None) -> dict:
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
{_language_block(locale, "all headlines, long_headline, all descriptions")}
{_newcomer_context_block("long_headline and at least one description")}
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

# Reddit promoted-ad CTA buttons we allow — a recruitment-appropriate subset of
# the v3 API's call_to_action enum (verified live 2026-06-13). NB: "Get Started"
# is NOT a valid Reddit CTA, so it's excluded here (and remapped to "Sign Up" in
# reddit_api as a belt-and-suspenders guard).
_REDDIT_CTAS = {"Sign Up", "Apply Now", "Learn More"}
# Soft cap for the free-form/native post body. Reddit allows ~40k chars, but a
# native, high-performing recruitment post is short — keep it tight.
_REDDIT_FREEFORM_MAX = 1200


def _adapt_for_reddit(variant: dict, c: PlatformConstraints, *, icp=None, locale=None) -> dict:
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
- cta:            One of: Sign Up, Apply Now, Learn More. Pick the most natural.
- freeform_title: MAX {c.headline_max_chars} characters. The native TEXT-post title — can be slightly more conversational/curious than `title`.
- freeform_body:  MAX {_REDDIT_FREEFORM_MAX} characters. A short native Reddit post body: 2-4 short paragraphs, peer voice, concrete task + payment + flexibility, one clear closing line pointing to the link. Plain text, no markdown headers, no bullet spam.

{_BANNED_VOCAB_BLOCK}
{_language_block(locale, "title, freeform_title, freeform_body")}
{_newcomer_context_block("freeform_body")}
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

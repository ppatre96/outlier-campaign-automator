"""Two-phase creative generation: brief → review → copy.

Phase 1 — `build_briefs()`: produces 3 structured creative briefs (one per
A/B/C angle) for a (cohort × geo_cluster). Each brief captures the *intent*
of an angle — angle_hook, headline_direction, photo_direction, tone,
proof_points, must_include, must_avoid — without writing the final copy.

Phase 2 — `build_copy_from_brief()`: takes ONE brief + an optional reviewer
comment and produces ONE variant dict in the schema today's pipeline already
consumes (headline, subheadline, intro_text, ad_headline, ad_description,
cta_button, photo_subject, rationale, competitor_signal, layerUpdates).

Why split: the brief-review gate (config.BRIEF_REVIEW_AUTO_CONFIRM_HOURS)
pauses the pipeline between Phase 1 and Phase 2 so the reviewer (Diego/Bryan)
can drop a free-text comment per (cohort × angle). The comment is then
prepended to the Phase-2 prompt as hard reviewer guidance.

Back-compat: `src/figma_creative.build_copy_variants()` is a thin wrapper
around `build_briefs() + build_copy_from_brief()` (no reviewer_comment, no
pause). Legacy CLI paths continue to work unchanged.
"""
from __future__ import annotations

import json
import logging
import pathlib
import time
from typing import Any

from src.claude_client import call_claude

log = logging.getLogger(__name__)


# Transient API errors worth retrying. Auth / bad-request / permission errors
# are NOT in this set — those are fatal misconfigurations, retrying just delays
# the inevitable. anthropic's modern SDK exposes these as top-level classes;
# fall back to bare-Exception detection on older SDKs so we never crash trying
# to import the names.
try:
    from anthropic import (
        APIConnectionError,
        APITimeoutError,
        InternalServerError,
        RateLimitError,
    )
    _RETRIABLE_API_EXCEPTIONS: tuple[type[BaseException], ...] = (
        RateLimitError,
        APIConnectionError,
        APITimeoutError,
        InternalServerError,
    )
except ImportError:                                                # pragma: no cover
    _RETRIABLE_API_EXCEPTIONS = ()


# Exponential backoff for transient errors: 2s, 4s, 8s before the next attempt.
# Three retries (4 total attempts) gives Anthropic ~14s to recover from a
# transient burst rate-limit before we give up and let the caller's
# per-(cohort × geo) try/except swallow the failure.
_BRIEF_GEN_MAX_ATTEMPTS = 4
_BRIEF_GEN_BACKOFF_SECONDS = (2, 4, 8)


def _call_claude_with_retry(messages: list[dict], **kwargs) -> str:
    """call_claude wrapper that retries on transient Anthropic errors.

    Used by both Phase 1 (build_briefs) and Phase 2 (build_copy_from_brief)
    so a rate-limit burst during prep doesn't cascade into 0-brief output.

    Re-raises non-transient errors immediately so the caller's try/except
    sees auth / bad-request failures without 14s of dead wait."""
    last_exc: BaseException | None = None
    for attempt in range(_BRIEF_GEN_MAX_ATTEMPTS):
        try:
            return call_claude(messages, **kwargs)
        except _RETRIABLE_API_EXCEPTIONS as exc:
            last_exc = exc
            if attempt == _BRIEF_GEN_MAX_ATTEMPTS - 1:
                # Final attempt failed — re-raise so the per-(cohort × geo)
                # try/except in _prep_ramp catches it and skips this combo.
                log.warning(
                    "brief gen: transient Anthropic error after %d attempts "
                    "(%s) — giving up on this call",
                    _BRIEF_GEN_MAX_ATTEMPTS, type(exc).__name__,
                )
                raise
            delay = _BRIEF_GEN_BACKOFF_SECONDS[
                min(attempt, len(_BRIEF_GEN_BACKOFF_SECONDS) - 1)
            ]
            log.info(
                "brief gen: transient Anthropic error %s on attempt %d/%d — "
                "sleeping %ds before retry",
                type(exc).__name__, attempt + 1, _BRIEF_GEN_MAX_ATTEMPTS, delay,
            )
            time.sleep(delay)
    # Defensive — the loop should always either return or raise; this only
    # fires if _RETRIABLE_API_EXCEPTIONS is empty (anthropic import failed).
    if last_exc is not None:
        raise last_exc
    raise RuntimeError("brief gen retry loop exited unexpectedly")  # pragma: no cover


# ── Phase 1: build_briefs ────────────────────────────────────────────────────


_CHANNELS_SUPPORTED = ("linkedin", "meta", "google")


def build_briefs(
    cohort,
    *,
    geos: list[str] | None = None,
    description_hint: str = "",
    hourly_rate: str = "",
    geo_icp_hint: str = "",
    icp=None,
    channel: str = "linkedin",
    competitor_intel_path: str | pathlib.Path | None = None,
) -> list[dict]:
    """Phase 1 — produce 3 structured briefs (A/B/C) for this cohort × geo.

    One Claude call, returns a list of 3 brief dicts. Brief shape matches
    the `cohort_briefs.brief` JSONB column in scripts/sql/006_cohort_briefs.sql:

        {
          "angle":             "A" | "B" | "C",
          "angle_hook":        str,  # one-line direction
          "headline_direction": str, # what the overlay headline should land
          "subheadline_direction": str,
          "photo_direction":   str,  # subject + setting + ethnicity + activity
          "tone":              str,  # calibrated against icp.creative_liberty
          "proof_points":      [str, ...],
          "language_hint":     str,
          "competitor_signal": str,
          "must_include":      [str, ...],
          "must_avoid":        [str, ...]
        }

    No copy length limits or banned-vocab scrubbing here — those are Phase 2's
    job. This phase is intent-only.
    """
    from src.linkedin_urn import _col_to_human
    from src.analysis import _feature_to_facet

    signals = []
    for feat, _ in cohort.rules:
        human = _col_to_human(feat)
        facet = _feature_to_facet(feat)
        signals.append(f"{facet}: {human}")

    competitor_context = _load_competitor_context(competitor_intel_path)
    icp_block = _icp_brief_block(icp if icp is not None else getattr(cohort, "_icp", None))
    pay_rate_block = _pay_rate_brief_block(hourly_rate)
    description_block = (
        "\n\nSMART RAMP REQUESTER'S AUDIENCE BRIEF (verbatim) — treat this as the "
        "AUTHORITATIVE description of who the audience actually is:\n"
        f"{description_hint}\n"
        if description_hint else ""
    )
    geo_block = _format_geos_for_brief(geos)
    channel_norm = (channel or "linkedin").lower()
    if channel_norm not in _CHANNELS_SUPPORTED:
        log.warning("build_briefs: unknown channel %r — falling back to linkedin", channel)
        channel_norm = "linkedin"
    channel_block = _CHANNEL_GUIDANCE_BRIEF.get(channel_norm, _CHANNEL_GUIDANCE_BRIEF["linkedin"])

    prompt = _BRIEF_PROMPT_TEMPLATE.format(
        cohort_label=cohort.name.replace("__", " ").replace("_", " "),
        signals_str="\n".join(f"  - {s}" for s in signals) or "  (none)",
        geo_block=geo_block,
        description_block=description_block,
        pay_rate_block=pay_rate_block,
        icp_block=icp_block,
        geo_icp_hint=geo_icp_hint or "",
        competitor_context=competitor_context,
        channel_block=channel_block,
    )

    log.info("Phase 1 brief gen — cohort=%s channel=%s signals=%d geo=%s",
             cohort.name[:40], channel_norm, len(signals), ",".join(geos or []) or "—")

    raw = _call_claude_with_retry(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2048,
        cache_system=True,
    ).strip()

    try:
        parsed = _extract_json(raw)
    except Exception as exc:
        log.error("Failed to parse Phase 1 brief JSON: %s\n%s", exc, raw[:500])
        return []

    briefs = parsed.get("briefs") or parsed.get("variants") or []
    if not isinstance(briefs, list) or len(briefs) == 0:
        log.warning("Phase 1 returned no briefs — falling back to empty list")
        return []

    # Normalize: ensure each brief has the canonical keys + an angle label.
    normalized: list[dict] = []
    for i, b in enumerate(briefs[:3]):
        if not isinstance(b, dict):
            continue
        angle = (b.get("angle") or ["A", "B", "C"][i]).strip().upper()
        normalized.append({
            "angle":                 angle,
            "angle_hook":            b.get("angle_hook", "") or "",
            "headline_direction":    b.get("headline_direction", "") or "",
            "subheadline_direction": b.get("subheadline_direction", "") or "",
            "photo_direction":       b.get("photo_direction", "") or "",
            "tone":                  b.get("tone", "") or "",
            "proof_points":          _coerce_str_list(b.get("proof_points")),
            "language_hint":         b.get("language_hint", "") or "",
            "competitor_signal":     b.get("competitor_signal", "") or "",
            "must_include":          _coerce_str_list(b.get("must_include")),
            "must_avoid":            _coerce_str_list(b.get("must_avoid")),
        })

    log.info("Phase 1 produced %d brief(s) for '%s'", len(normalized), cohort.name)
    return normalized


# ── Phase 2: build_copy_from_brief ────────────────────────────────────────────


def build_copy_from_brief(
    brief: dict,
    *,
    layer_map: dict[str, str],
    cohort,
    geos: list[str] | None = None,
    hourly_rate: str = "",
    reviewer_comment: str = "",
    channel: str = "linkedin",
) -> dict:
    """Phase 2 — take ONE (possibly reviewer-edited) brief and produce ONE
    variant dict in today's pipeline schema.

    Returns a dict matching the existing build_copy_variants() per-entry shape:
        {
          "angle":         "A" | "B" | "C",
          "angleLabel":    str,
          "headline":      str,  # ≤6 words, ≤40 chars
          "subheadline":   str,  # ≤7 words, ≤48 chars
          "intro_text":    str,  # ≤140 chars
          "ad_headline":   str,  # ≤70 chars
          "ad_description": str, # ≤100 chars (optional)
          "cta_button":    "APPLY",
          "photo_subject": str,
          "rationale":     str,
          "competitor_signal": str,
          "layerUpdates":  {node_id: text}
        }

    The `reviewer_comment` (if non-empty) is prepended to the prompt as a hard
    constraint — "Reviewer guidance for this angle: <comment>. Treat as hard
    constraint, not a suggestion." LLM is told to obey it OVER the brief
    when in conflict.
    """
    # Import lazily to avoid circular imports — figma_creative imports us.
    from src.figma_creative import (
        _validate_copy_limits,
        _scrub_banned_tokens_and_dashes,
        _BANNED_TOKEN_REPLACEMENTS,  # noqa: F401  (sanity check the import path)
    )

    layers_summary = json.dumps(layer_map, indent=2)
    cohort_label = cohort.name.replace("__", " ").replace("_", " ")

    reviewer_block = ""
    if reviewer_comment.strip():
        reviewer_block = (
            "\n\n## REVIEWER GUIDANCE — HARD CONSTRAINT (wins over the brief on conflict)\n"
            f"{reviewer_comment.strip()}\n"
            "Treat this as a non-negotiable instruction from the campaign reviewer. "
            "If the brief says one thing and this guidance contradicts it, the guidance wins.\n"
        )
        log.info("Phase 2 honoring reviewer comment (%d chars) for angle %s",
                 len(reviewer_comment), brief.get("angle", "?"))

    pay_rate_block = _pay_rate_brief_block(hourly_rate)
    geo_block = _format_geos_for_brief(geos)
    channel_norm = (channel or "linkedin").lower()
    if channel_norm not in _CHANNELS_SUPPORTED:
        channel_norm = "linkedin"
    channel_block = _CHANNEL_GUIDANCE_PHASE2.get(channel_norm, _CHANNEL_GUIDANCE_PHASE2["linkedin"])

    prompt = _PHASE2_PROMPT_TEMPLATE.format(
        cohort_label=cohort_label,
        brief_json=json.dumps(brief, indent=2),
        reviewer_block=reviewer_block,
        layers_summary=layers_summary,
        pay_rate_block=pay_rate_block,
        geo_block=geo_block,
        channel_block=channel_block,
    )

    # Retry loop matches build_copy_variants — LLM sometimes overshoots length
    # limits. Give it 3 attempts with the violation list fed back in.
    variant: dict = {}
    last_violations: list[str] = []
    for attempt in range(3):
        user_content = prompt
        if attempt > 0 and last_violations:
            user_content = prompt + (
                "\n\nRETRY — your previous output violated the hard limits:\n"
                + "\n".join(f"- {v}" for v in last_violations)
                + "\nREWRITE so headline ≤6 words AND ≤40 chars; subheadline "
                "≤7 words AND ≤48 chars. No exceptions."
            )
        raw = _call_claude_with_retry(
            messages=[{"role": "user", "content": user_content}],
            max_tokens=1024,
            cache_system=True,
        ).strip()
        try:
            parsed = _extract_json(raw)
        except Exception as exc:
            log.error("Phase 2 JSON parse failed (attempt %d): %s\n%s", attempt + 1, exc, raw[:400])
            variant = {}
            continue
        variant = parsed if isinstance(parsed, dict) and "headline" in parsed else (parsed.get("variant") or {})
        if not isinstance(variant, dict):
            variant = {}
            continue
        last_violations = _validate_copy_limits([variant])
        if not last_violations:
            break
        log.warning("Phase 2 length limits violated (attempt %d): %s", attempt + 1, last_violations)

    if not variant:
        log.error("Phase 2 returned no variant after 3 attempts for angle %s", brief.get("angle"))
        return {}

    # Ensure angle stays consistent with the brief — LLM occasionally relabels.
    variant.setdefault("angle", brief.get("angle", "A"))
    variant.setdefault("angleLabel", _DEFAULT_ANGLE_LABELS.get(variant.get("angle", "A"), "Expertise Hook"))
    variant.setdefault("cta_button", "APPLY")
    variant.setdefault("layerUpdates", {})
    # Carry the competitor_signal through from the brief if Phase 2 didn't echo it.
    if not variant.get("competitor_signal"):
        variant["competitor_signal"] = brief.get("competitor_signal", "")

    # Banned-vocab post-process. Identical to build_copy_variants's pass.
    try:
        from src.brand_voice_validator import BrandVoiceValidator
        bv = BrandVoiceValidator()
        copy_fields = ("headline", "subheadline", "cta", "photo_subject",
                       "intro_text", "ad_headline", "ad_description")
        replacements: list[tuple[str, str]] = []
        for fld in copy_fields:
            if fld in variant and isinstance(variant[fld], str) and variant[fld]:
                new_v, repls = bv.rewrite_banned_terms(variant[fld])
                if repls:
                    variant[fld] = new_v
                    replacements.extend(repls)
        if replacements:
            log.info("Phase 2 sanitised %d banned term(s) for angle %s: %s",
                     len(replacements), brief.get("angle"),
                     [f"{a}→{b}" for a, b in replacements])
    except Exception as exc:
        log.warning("Phase 2 banned-term rewriter failed (non-fatal): %s", exc)

    # Always also run the deterministic dash + token scrubber on every text field
    for fld in ("headline", "subheadline", "intro_text", "ad_headline", "ad_description"):
        if fld in variant and isinstance(variant[fld], str):
            variant[fld] = _scrub_banned_tokens_and_dashes(variant[fld])

    return variant


# ── Helpers ───────────────────────────────────────────────────────────────────


_DEFAULT_ANGLE_LABELS = {
    "A": "Expertise Hook",
    "B": "Earnings Hook",
    "C": "Flexibility Hook",
}


def _coerce_str_list(v: Any) -> list[str]:
    """JSONB can store anything — coerce list-likes to clean str list."""
    if not v:
        return []
    if isinstance(v, list):
        return [str(x) for x in v if x]
    if isinstance(v, str):
        return [v] if v.strip() else []
    return []


def _load_competitor_context(path: str | pathlib.Path | None) -> str:
    """Pull experiment_ideas from data/competitor_intel/latest.json — same
    behavior as build_copy_variants. Best-effort; empty string on any error."""
    p = pathlib.Path(path) if path else pathlib.Path("data/competitor_intel/latest.json")
    if not p.exists():
        return ""
    try:
        data = json.loads(p.read_text())
        ideas = data.get("experiment_ideas", [])
        if not ideas:
            return ""
        # Accept both string-list and {idea, priority} dict-list shapes
        clean = []
        for it in ideas[:5]:
            if isinstance(it, str):
                clean.append(it)
            elif isinstance(it, dict):
                clean.append(it.get("idea") or "")
        clean = [c for c in clean if c]
        if not clean:
            return ""
        return "\n\nCompetitor experiment ideas to consider (one signal max per angle):\n" + "\n".join(
            f"- {c}" for c in clean
        )
    except Exception as exc:
        log.warning("competitor intel load failed: %s", exc)
        return ""


def _icp_brief_block(icp_obj: Any) -> str:
    """Format the ICP into a Phase-1 prompt block. Tolerant of both
    CohortIcp dataclasses and dict shape (JSONB readback)."""
    if icp_obj is None:
        return ""

    def _g(key: str, default=""):
        if isinstance(icp_obj, dict):
            return icp_obj.get(key, default)
        return getattr(icp_obj, key, default)

    motivations = _g("top_motivations", []) or []
    content_prefs = _g("content_prefs", []) or []
    drivers = _g("decision_drivers", []) or []
    skills = _g("skill_priorities", []) or []
    if not any([motivations, content_prefs, drivers, skills]):
        return ""

    return (
        "\n\n## COHORT ICP (calibrate every brief against this)\n"
        f"- Who they are: {_g('cohort_description') or '(not provided)'}\n"
        f"- Top motivations: {', '.join(motivations) or '(none)'}\n"
        f"- Content preferences: {', '.join(content_prefs) or '(none)'}\n"
        f"- Decision drivers: {', '.join(drivers) or '(none)'}\n"
        f"- Skill priorities: {', '.join(skills) or '(none)'}\n"
        f"- Creative liberty: {_g('creative_liberty', 'medium') or 'medium'}\n"
        f"- Language preference: {_g('language_pref', 'en-US') or 'en-US'}\n"
    )


def _pay_rate_brief_block(hourly_rate: str) -> str:
    """Phase-1 block for hourly rate. Brief gen needs to know whether $$ is
    available to mention in the brief or not (so it can choose proof_points
    that don't fabricate numbers)."""
    if hourly_rate:
        return (
            f"\n\nPAY RATE FOR THIS GEO: {hourly_rate} (use this exact figure if you "
            f"reference earnings — do NOT round, invent, or extrapolate).\n"
        )
    return (
        "\n\nPAY RATE: UNRESOLVED — no $/hr figure available for this ramp. "
        "Briefs MUST NOT propose earnings claims with specific numbers. Lean on "
        "non-monetary value props: flexibility, remote freedom, AI-experience claim, "
        "peer social proof without a dollar figure.\n"
    )


def _format_geos_for_brief(geos: list[str] | None) -> str:
    """Concise geo block for Phase 1 + Phase 2 — just the country list."""
    if not geos:
        return ""
    return f"\nGEO CLUSTER (ISO countries): {', '.join(geos)}\n"


def _extract_json(raw: str) -> dict:
    """Tolerant JSON extraction. Mirrors src/figma_creative._extract_json so the
    two phases don't drift on parsing rules."""
    import re as _re
    match = _re.search(r"```(?:json)?\s*([\s\S]+?)```", raw)
    if match:
        return json.loads(match.group(1).strip())
    # Raw JSON fallback
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        return json.loads(raw[start:end + 1])
    raise ValueError(f"no JSON found in response: {raw[:200]}")


# ── Prompt templates ──────────────────────────────────────────────────────────


# Channel-specific creative guidance. Inserted into both Phase-1 (brief) and
# Phase-2 (copy) prompts. Same brief shape across all 3 channels — the
# *content* of each field shifts based on the channel's creative norms.
_CHANNEL_GUIDANCE_BRIEF = {
    "linkedin": """
## CHANNEL — LinkedIn (B2B sponsored static ad)
- Audience mindset: scrolling between colleagues' posts. Credibility-led, not entertainment-led.
- Hook style: expertise validation, peer-benchmarked claims, "people like you do this" framing.
- Tone target: confident-professional. Calibrate against ICP creative_liberty but bias TOWARDS clear/credible over playful.
- Photo direction: clean, professional, well-lit. Business-casual or in-context. Indoor or outdoor — natural over staged. Never stock-photo cheese.
- Headline direction: lead with the person's identity or skill. Earnings claims OK if grounded in the pay_rate_block.
""",
    "meta": """
## CHANNEL — Meta (Facebook + Instagram Feed, 4:5 vertical)
- Audience mindset: scrolling for entertainment + social. First 1.5 seconds decide it. Attention battle is brutal.
- Hook style: pattern-interrupt, curiosity gap, specific stat, or bold declaration. Earnings + lifestyle frames over expertise frames.
- Tone target: punchier, more conversational than LinkedIn. Witty if ICP creative_liberty allows. Avoid B2B vocabulary ("professionals", "experts") — treat as B2C-shaped messaging.
- Photo direction: high-contrast subject + setting. Real-feeling, not corporate-staged. Single hero subject, vertical-crop-safe composition.
- Headline direction: SHARPER than LinkedIn (≤6 words always; aim for ≤5). Earnings claim, pain-point, or curiosity gap all work. Don't soften with caveats.
""",
    "google": """
## CHANNEL — Google Display (responsive display ads, intent-adjacent placements)
- Audience mindset: not actively scrolling — ad appears alongside content they're consuming. Lower attention than Feed.
- Hook style: direct-response, low ambiguity. "What you get + what you do." NO clever copy — Display has 1–2 seconds of engagement at best.
- Tone target: clear and functional. Avoid metaphors, riddles, ambiguity. Be the value prop in plain words.
- Photo direction: clean subject focus on simple background. Google's responsive display rules penalize busy backgrounds. NO text in the photo — Google's image policy rejects it.
- Headline direction: action-oriented. Mirror likely search keywords: "Earn paid AI tasks", "Remote AI work for [profession]". Keyword-adjacent phrasing helps quality score.
""",
}

_CHANNEL_GUIDANCE_PHASE2 = {
    "linkedin": """
## CHANNEL — LinkedIn (sponsored static + InMail)
- Headlines + subheadlines can lean on professional credibility ("Built this skill", "Used by peers like you").
- ad_description has room — use it to add credibility detail.
- cta_button is "APPLY".
""",
    "meta": """
## CHANNEL — Meta (Facebook + Instagram Feed)
- Headlines must be punchier than LinkedIn — assume scroll mode, not reading mode.
- intro_text first line is the WHOLE battle (Feed truncates at ~125 chars on mobile). Open with the hook, not setup.
- ad_description rarely renders — keep it short or skip.
- Avoid corporate vocabulary ("professionals", "experts"). Use 2nd-person ("you").
- cta_button is "APPLY".
""",
    "google": """
## CHANNEL — Google Display (responsive display)
- Headlines must work standalone — Google may show ANY headline + ANY description combination. Each line should make sense in isolation.
- intro_text is reused as the long_headline — keep under 90 chars for safety.
- No clever copy. State the offer in plain language: who it's for + what they earn + what they do.
- cta_button is "APPLY".
""",
}


_BRIEF_PROMPT_TEMPLATE = """You are creating 3 A/B/C creative briefs for **Outlier** — a platform where domain experts earn payment doing flexible, remote AI training tasks (reviewing, rating, and improving AI outputs in their field).

This is the BRIEF stage. You produce only the INTENT — the angle hook, headline direction, photo direction, tone, proof points, and non-negotiables. A second LLM pass turns each brief into the actual headline/subheadline/intro_text/ad_headline. Do NOT write the final copy here.
{channel_block}
## WHO THIS PERSON IS
Cohort name (raw): {cohort_label}
Signals from statistical analysis (features that predict this person passes Outlier's screening):
{signals_str}
{geo_block}{description_block}{pay_rate_block}{icp_block}{geo_icp_hint}{competitor_context}

From these inputs, identify their specific professional title (not a category), primary daily activity, schedule constraint, geography/language context, and emotional state. Use that identity as the anchor for all 3 briefs.

## THE 3 ANGLES (one brief each)

### Variant A — Expertise / AI Experience Hook
Insight: This person has niche expertise and wants meaningful use of it.
Pattern: Name their specific professional moment → reveal their exact skill has AI value.

### Variant B — Earnings / Social Proof Hook
Insight: Proof that people like them are already earning real money.
Pattern: Bold social proof stat or earnings claim → aspirational pull.

### Variant C — Flexibility / Lifestyle Hook
Insight: Their schedule is controlled by something external. Freedom from that is aspirational.
Pattern: Bold declaration naming their specific constraint → low-friction income claim.

## BRIEF FIELDS YOU MUST FILL (each angle, JSON shape exact)

- **angle_hook**: one-line creative direction the Phase 2 writer should follow. e.g. "open with their lab-bench frustration; pivot to AI-review work that uses the same observational skill."
- **headline_direction**: what the image-overlay headline should communicate (NOT the final wording — the *direction*). e.g. "Name the niche skill (metagenomics) + flexible-earning frame."
- **subheadline_direction**: what the subheadline should reinforce.
- **photo_direction**: subject + setting + ethnicity + activity, no screen-bound activities. e.g. "male South Asian environmental engineer, examining a rolled-up site blueprint on a sunlit table." This becomes Phase 2's photo_subject.
- **tone**: calibrated against the ICP's creative_liberty (high=bold/witty, medium=clear+credible, low=corporate-safe).
- **proof_points**: 2-4 concrete things the writer can pull from (peer counts, payment cadence, screening signals — NOT invented numbers).
- **language_hint**: from ICP language_pref. "en-US" / "es-419" / etc.
- **competitor_signal**: the specific competitor insight that informs this angle. Empty string if none.
- **must_include**: non-negotiable phrases the final copy must contain (e.g. ["paid hourly", "remote"] OR ["$45/hr"] when the pay rate is known).
- **must_avoid**: phrases the final copy MUST NOT contain (e.g. ["training", "job", "team", "interview"] — Outlier banned vocabulary).

## RESPONSE FORMAT
Return ONLY valid JSON, no other text. Exactly 3 briefs:

```json
{{
  "briefs": [
    {{
      "angle": "A",
      "angle_hook": "...",
      "headline_direction": "...",
      "subheadline_direction": "...",
      "photo_direction": "...",
      "tone": "...",
      "proof_points": ["..."],
      "language_hint": "...",
      "competitor_signal": "...",
      "must_include": ["..."],
      "must_avoid": ["..."]
    }},
    {{ "angle": "B", "...": "..." }},
    {{ "angle": "C", "...": "..." }}
  ]
}}
```
"""


_PHASE2_PROMPT_TEMPLATE = """You are writing ONE Outlier ad creative variant from a structured brief.
{channel_block}
## THE BRIEF (your hard inputs — follow these)
Cohort: {cohort_label}{geo_block}{pay_rate_block}

```json
{brief_json}
```
{reviewer_block}

## OUTPUT — produce ONE variant matching this schema

You produce TWO TEXT SETS:

### (I) IMAGE OVERLAY TEXT (baked into 1200×1200 PNG)
- **headline**: MAX 6 words AND MAX 40 characters. Bold white over photo. NEVER include "Outlier" (brand wordmark composited separately).
- **subheadline**: MAX 7 words AND MAX 48 characters. NEVER include "Outlier".

### (II) LINKEDIN AD COPY (around the image in the feed)
- **intro_text**: MAX 140 characters. Hook in first line — feed previews cut at ~140 chars. Question, bold claim, or specific pain point.
- **ad_headline**: MAX 70 characters. Reinforces or extends the overlay headline.
- **ad_description**: MAX 100 characters. Optional (empty string allowed). Recommended for stronger conversion.
- **cta_button**: ALWAYS "APPLY".

### NEWCOMER CONTEXT — the reader may have never heard of Outlier
The overlay headline can be short and punchy, but the AD COPY (intro_text + ad_description) must give a first-time reader enough to act on:
- **intro_text** frames what Outlier is and how payment works: a platform where domain experts earn payment doing flexible, remote AI training tasks, paid hourly in USD.
- **ad_description** names the CONCRETE task tied to this audience's expertise (e.g. "review AI-generated ECG readings for accuracy"), never a vague "help improve AI".
A bare line like "Hindi experts paid in USD" is a FAILURE — it tells a newcomer nothing about what they'd actually do. Specificity beats hype.

### PHOTO SUBJECT
- **photo_subject**: Use the brief's `photo_direction` directly OR refine it. Format: "[gender] [ethnicity] [specific profession], [specific activity off-screen]". NEVER an activity that puts the subject staring at a laptop/phone/tablet screen — Vision QC rejects rendered text in photos. Use paper notes, printouts, notebooks, closed laptop, etc.

## TEXT LAYERS IN THE BASE CREATIVE
{layers_summary}

The `layerUpdates` field maps layer node_id → new text. Match headline + subheadline to the right layers.

## OUTLIER BRAND VOICE — MANDATORY

### Banned vocabulary (use alternatives)
| Banned | Use Instead |
|---|---|
| work, job | contribute, tasks, projects, opportunity |
| training, learning, growth | session, walk through, get familiar with project guidelines |
| performance | progress |
| assign | match |
| bonus | reward |
| role, position | opportunity |
| compensation, salary | payment |
| interview | screening |
| instructions | project guidelines |
| team | part of this project |

### Banned filler + AI vocabulary
ZERO tolerance: genuinely, honestly, truly, actually, really, very, so, just, delve, landscape, leverage, foster, robust, holistic, dive into, unpack, game-changer, cutting-edge, revolutionary, seamless, transformative, tapestry, realm, journey, testament.

### Formatting
- Sentence case (proper nouns + first word only)
- NO em dashes ("—") or en dashes ("–"). Use commas, periods, colons, parentheses.
- NO hashtags. NO ALL CAPS. NO exclamation chains.
- Oxford commas. Contractions throughout.

### Honor the brief's must_include / must_avoid
- Every phrase in `must_include` MUST appear in at least one of the 5 text fields.
- Every phrase in `must_avoid` MUST NOT appear in any field.

## RESPONSE FORMAT
Return ONLY valid JSON, no other text. ONE variant:

```json
{{
  "angle": "A|B|C (from brief)",
  "angleLabel": "Expertise Hook|Earnings Hook|Flexibility Hook",
  "headline": "...",
  "subheadline": "...",
  "intro_text": "...",
  "ad_headline": "...",
  "ad_description": "...",
  "cta_button": "APPLY",
  "photo_subject": "...",
  "rationale": "1-2 sentences. Why this angle for this audience now. Reference the brief's competitor_signal if non-empty.",
  "competitor_signal": "(echo from brief)",
  "layerUpdates": {{"<node_id>": "new text"}}
}}
```
"""

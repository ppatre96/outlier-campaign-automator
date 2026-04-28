"""
Copy + Design QC — combined quality control for Outlier creatives.

Runs after outlier-copy-writer + outlier-creative-generator produce the brief + PNG,
and before any LinkedIn upload. Catches failures in two dimensions:

1. COPY (structural — runs without Gemini):
   - Length limits (words / chars / rendered lines) on every text field:
     headline, subheadline, intro_text, ad_headline, ad_description
   - CTA button enum validation
   - Outlier brand voice scan: banned vocabulary, banned phrases, em dashes,
     hashtags, ALL CAPS — scanned across ALL text fields

2. DESIGN (vision-based — uses Gemini vision):
   - Text/logos rendered INTO the photo by Gemini
   - Duplicate Outlier logos
   - Text overlapping the subject (hair counts)
   - Headroom gap appropriate (not too big, not too close)
   - Gradient in correct quadrant (pink top-left, teal bottom-left)
   - Edge border artefact
   - Photo fills frame
   - Logo renders correctly (real SVG, not Inter Bold fallback)
   - Subject authenticity (not AI-looking)
   - Subject differs from reference image person
   - Text-zone contrast
   - Professional quality

Each call returns a structured verdict with PASS/FAIL, violation list, and a specific
retry instruction routed to either outlier-copy-writer or outlier-creative-generator
depending on what failed.
"""
import base64
import json
import logging
import os
import re
from dataclasses import dataclass, field
from pathlib import Path

import requests

import config

log = logging.getLogger(__name__)

# Copy length hard limits — enforced structurally before any vision call.
# (I) Image overlay text (baked into the PNG)
HEADLINE_MAX_WORDS = 6
HEADLINE_MAX_CHARS = 40
SUBHEAD_MAX_WORDS  = 7
SUBHEAD_MAX_CHARS  = 48
HEADLINE_MAX_LINES = 2
SUBHEAD_MAX_LINES  = 2
# (II) LinkedIn ad copy (feed text around the image)
INTRO_TEXT_MAX_CHARS    = 140  # feed preview cuts off ~140
AD_HEADLINE_MAX_CHARS   = 70
AD_DESCRIPTION_MAX_CHARS = 100
ALLOWED_CTA_BUTTONS = {
    "APPLY", "SIGN_UP", "LEARN_MORE", "REGISTER", "GET_STARTED",
    "DOWNLOAD", "JOIN", "SUBSCRIBE", "REQUEST_DEMO", "VISIT_WEBSITE",
}

# ── Outlier brand voice — banned tokens ───────────────────────────────────────
# Scanned case-insensitively, whole-word (\bword\b) to avoid false positives
# (e.g. "so" matched inside "sound"). Sourced from the outlier-copy-writer agent
# definition + brand voice guide.
_BANNED_VOCABULARY = [
    # Employment-implying / platform-restricted
    "work", "job", "performance", "assign", "bonus", "role", "position",
    "training", "employee", "worker", "schedule", "shift",
    "Mango", "MultiMango", "Discourse", "sprint",
    "compensation", "salary", "required", "interview", "instructions", "promote",
    # Filler
    "genuinely", "honestly", "truly", "actually", "really",
    # AI vocabulary
    "delve", "landscape", "leverage", "foster", "robust", "holistic",
    "unpack", "tapestry", "realm", "journey", "testament",
    "transformative", "seamless", "revolutionary",
    # Specific model names — should never appear in contributor-facing copy
    "ChatGPT", "Claude", "Gemini", "Grok", "DeepSeek",
]

# Phrases (multi-word) scanned as substrings, case-insensitive
_BANNED_PHRASES = [
    "dive into", "game-changer", "cutting-edge", "at the end of the day",
    "we're excited to", "we'd love to", "we wanted to reach out",
    "set the right expectations", "as you may be aware",
    "great news:", "good news:", "we're thrilled", "we can't wait",
    "i'm excited to announce",
    "here's the thing", "quick pitch:", "here's why this matters",
    "let me be clear", "excited to share",
    "don't miss this", "now's your chance", "this is your moment",
]


def scan_brand_voice(text: str, field_name: str = "field") -> list[str]:
    """
    Scan a copy field for banned vocabulary and phrases. Returns a list of
    human-readable violations. Empty list = clean.
    """
    if not text:
        return []
    violations: list[str] = []
    low = text.lower()

    # Word-boundary scan for single tokens
    for banned in _BANNED_VOCABULARY:
        if re.search(rf"\b{re.escape(banned.lower())}\b", low):
            violations.append(f"{field_name} contains banned token {banned!r}: {text!r}")

    # Substring scan for multi-word phrases
    for phrase in _BANNED_PHRASES:
        if phrase in low:
            violations.append(f"{field_name} contains banned phrase {phrase!r}: {text!r}")

    # Formatting checks
    if "—" in text:
        violations.append(f"{field_name} contains em dash (banned in contributor copy): {text!r}")
    if re.search(r"#\w+", text):
        violations.append(f"{field_name} contains hashtag (banned on all platforms): {text!r}")
    # ALL CAPS header check — 3+ consecutive words of all caps (excluding acronyms like AI, USD, PhD)
    if re.search(r"\b[A-Z]{4,}\s+[A-Z]{4,}\s+[A-Z]{4,}", text):
        violations.append(f"{field_name} contains ALL CAPS phrasing (banned): {text!r}")

    return violations

# Gemini vision model (multimodal input: image + text)
_GEMINI_VISION_MODEL = "gemini-2.5-flash"
_GEMINI_VISION_URL_TMPL = (
    "https://generativelanguage.googleapis.com/v1beta/models/"
    "{model}:generateContent?key={api_key}"
)


@dataclass
class QCReport:
    """Structured QC verdict for a single creative."""
    verdict: str  # "PASS" | "FAIL"
    checks: dict[str, bool] = field(default_factory=dict)  # check_name -> pass/fail
    violations: list[str] = field(default_factory=list)
    retry_instruction: str = ""
    retry_target: str = "none"  # "copywriter" | "gemini" | "none"
    copy_violations: list[str] = field(default_factory=list)  # subset for copywriter retry

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "checks": self.checks,
            "violations": self.violations,
            "retry_instruction": self.retry_instruction,
            "retry_target": self.retry_target,
            "copy_violations": self.copy_violations,
        }


def _count_rendered_lines(text: str, font_size_frac: float, canvas_size: int = 1200, bold: bool = False) -> int:
    """
    Return how many lines the text would occupy when rendered at the ad canvas settings.
    Uses the same font loader, font size, and max-width rules that compose_ad() uses so
    the line count here matches what the user actually sees in the final creative.
    """
    from PIL import Image, ImageDraw
    # Lazy import so importing image_qc doesn't require cairosvg on headless systems
    from src.gemini_creative import _load_font, _wrap_text

    border = int(canvas_size * 0.033)
    photo_w = canvas_size - 2 * border
    max_text_w = int(photo_w * (0.88 if bold else 0.82))
    font = _load_font(int(canvas_size * font_size_frac), bold=bold)

    # Account for explicit \n in the string (caller-inserted breaks)
    parts = text.split("\n")
    total_lines = 0
    for part in parts:
        lines = _wrap_text(part, font, max_text_w)
        total_lines += max(len(lines), 1)
    return total_lines


def validate_copy_lengths(
    headline: str,
    subheadline: str,
    intro_text: str = "",
    ad_headline: str = "",
    ad_description: str = "",
    cta_button: str = "",
) -> list[str]:
    """
    Return a list of copy-length violations. Checks:
    - Image overlay: headline/subheadline word+char+line counts
    - LinkedIn ad copy: intro_text, ad_headline, ad_description char limits
    - CTA button is in the allowed LinkedIn enum set

    Fields with default "" are skipped — only image-overlay checks run if the ad
    copy fields aren't provided (backward compatibility for callers that haven't
    migrated yet).
    """
    violations: list[str] = []

    # Strip explicit newlines when measuring word/char counts — those are layout hints
    h_plain = headline.replace("\n", " ").strip()
    s_plain = subheadline.replace("\n", " ").strip()

    h_words = len(h_plain.split())
    s_words = len(s_plain.split())

    if h_words > HEADLINE_MAX_WORDS:
        violations.append(f"Headline has {h_words} words (max {HEADLINE_MAX_WORDS}): {headline!r}")
    if len(h_plain) > HEADLINE_MAX_CHARS:
        violations.append(f"Headline has {len(h_plain)} chars (max {HEADLINE_MAX_CHARS}): {headline!r}")
    if s_words > SUBHEAD_MAX_WORDS:
        violations.append(f"Subheadline has {s_words} words (max {SUBHEAD_MAX_WORDS}): {subheadline!r}")
    if len(s_plain) > SUBHEAD_MAX_CHARS:
        violations.append(f"Subheadline has {len(s_plain)} chars (max {SUBHEAD_MAX_CHARS}): {subheadline!r}")

    # Line-count check — render to check actual wrapping at canvas width
    try:
        h_lines = _count_rendered_lines(headline, 0.085, bold=True)
        s_lines = _count_rendered_lines(subheadline, 0.044, bold=False)
        if h_lines > HEADLINE_MAX_LINES:
            violations.append(f"Headline wraps to {h_lines} lines (max {HEADLINE_MAX_LINES}): {headline!r}")
        if s_lines > SUBHEAD_MAX_LINES:
            violations.append(f"Subheadline wraps to {s_lines} lines (max {SUBHEAD_MAX_LINES}): {subheadline!r}")
    except Exception as exc:
        log.warning("Line-count check failed (non-fatal): %s", exc)

    # LinkedIn ad copy checks — only enforced when the caller supplies these fields
    if intro_text:
        if len(intro_text.strip()) > INTRO_TEXT_MAX_CHARS:
            violations.append(
                f"intro_text has {len(intro_text.strip())} chars (max {INTRO_TEXT_MAX_CHARS}): {intro_text!r}"
            )
    if ad_headline:
        if len(ad_headline.strip()) > AD_HEADLINE_MAX_CHARS:
            violations.append(
                f"ad_headline has {len(ad_headline.strip())} chars (max {AD_HEADLINE_MAX_CHARS}): {ad_headline!r}"
            )
    if ad_description:
        if len(ad_description.strip()) > AD_DESCRIPTION_MAX_CHARS:
            violations.append(
                f"ad_description has {len(ad_description.strip())} chars (max {AD_DESCRIPTION_MAX_CHARS}): {ad_description!r}"
            )
    if cta_button and cta_button.upper() not in ALLOWED_CTA_BUTTONS:
        violations.append(
            f"cta_button {cta_button!r} not in allowed set {sorted(ALLOWED_CTA_BUTTONS)}"
        )

    # Brand voice scan — applied to every text field
    violations.extend(scan_brand_voice(headline, "headline"))
    violations.extend(scan_brand_voice(subheadline, "subheadline"))
    if intro_text:
        violations.extend(scan_brand_voice(intro_text, "intro_text"))
    if ad_headline:
        violations.extend(scan_brand_voice(ad_headline, "ad_headline"))
    if ad_description:
        violations.extend(scan_brand_voice(ad_description, "ad_description"))

    return violations


# ── QC prompt for Gemini vision ───────────────────────────────────────────────

_QC_PROMPT = """\
You are the Copy + Design QC reviewer for Outlier LinkedIn ad creatives. Be brutally strict. \
Your job is to catch Gemini's hallucinations and layout defects before they ship.

You are given TWO images:
1. The generated creative (1200x1200 PNG).
2. The reference image that guided composition.

EXPECTED STRUCTURE OF THE CREATIVE (anything outside this is a failure):
- A single photograph of a person in a plant-filled home office, filling the upper ~85% of the canvas.
- The photo is inset with a ~40px WHITE border on the left, right, and top (rounded corners).
- One crisp white headline overlay, near the top of the photo area (≤2 lines).
- One crisp white subheadline overlay, near the bottom of the photo area (≤2 lines).
- At the very bottom: a white strip containing (left) brown earnings text ("Earn $X USD per hour. Fully remote.") and (right) exactly ONE brown "Outlier" wordmark.

EXPECTED HEADLINE: {headline}
EXPECTED SUBHEADLINE: {subheadline}

You MUST answer every question below mechanically. Do not hedge. Do not be polite. \
If you are unsure, count it as a FAIL. Borderline = FAIL.

SCAN PROTOCOL (execute in this order before scoring):
(1) Count every occurrence of the word "Outlier" (or stylized equivalent) anywhere in the image. Expected: exactly 1.
(2) Read the photo area like a page. Transcribe EVERY piece of text you see inside the photo that is NOT the crisp headline/subheadline overlay or the bottom-strip earnings line. Expected: none.
(3) Measure the visible gap between the BOTTOM edge of the headline text (letter descenders) and the TOP of the subject's hair. The gap should be small but clearly visible — roughly 3-8% of the frame height. Report it as: TOO CLOSE (text touching hair), JUST RIGHT (small clean gap), or TOO FAR (large empty band between text and head).
(4) Look at where the COLORED GRADIENT WASHES sit in the image, using the quadrant grid: top-left / top-right / bottom-left / bottom-right.
     - Expected: PINK/CORAL wash in TOP-LEFT quadrant only. TEAL/BLUE wash in BOTTOM-LEFT quadrant only. Right half should be neutral.
     - Also check intensity: the washes should be subtle and painterly, not saturated blocks of color.
(5) Look at the LEFT EDGE and RIGHT EDGE of the photo (just inside the white border). Are there visible colored stripes, bands, or gradient "lines" along either edge that look like separate graphical elements (not natural photo content)?
(6) Look at the bottom-right Outlier logo. Is it the actual Outlier wordmark (clean, proportional brown letterforms "O-u-t-l-i-e-r") or does it look warped, cut off, tiled, or like generic bold text?

Respond with a single JSON object (JSON only, no prose, no markdown fences):

{{
  "outlier_logo_count": {{
    "count": <integer from scan step 1>,
    "locations": "<describe every place you see it>"
  }},
  "visible_text_inside_photo": {{
    "found": <true if scan step 2 found any stray text; false otherwise>,
    "detail": "<verbatim transcription of any stray text found>"
  }},
  "rendered_text_in_photo": {{
    "pass": <true iff visible_text_inside_photo.found is false>,
    "detail": "<echo visible_text_inside_photo.detail>"
  }},
  "duplicate_logo": {{
    "pass": <true iff outlier_logo_count.count == 1>,
    "detail": "<echo outlier_logo_count.locations>"
  }},
  "text_overlaps_subject": {{
    "pass": <true iff scan step 3 classified the gap as JUST RIGHT — text is not touching subject AND gap is not excessively large>,
    "gap_classification": "<TOO CLOSE | JUST RIGHT | TOO FAR>",
    "detail": "FAIL if text is touching the subject OR if there is an unusually large empty band between the text and the hairline (indicates Gemini placed the subject too low, creating an awkward gap). JUST RIGHT is roughly 3-8% of the frame height between bottom of text descenders and top of hair."
  }},
  "headroom_gap_appropriate": {{
    "pass": <true iff scan step 3 gap_classification is JUST RIGHT>,
    "detail": "FAIL if gap is TOO FAR (>10% of frame height) — this wastes vertical space and looks amateur. FAIL if gap is TOO CLOSE (touching) — covered by text_overlaps_subject."
  }},
  "gradient_position_correct": {{
    "pass": <true iff PINK is only in TOP-LEFT quadrant AND TEAL is only in BOTTOM-LEFT quadrant AND the right half is largely neutral>,
    "pink_location": "<top-left | top-right | bottom-left | bottom-right | not visible | spread-across>",
    "teal_location": "<top-left | top-right | bottom-left | bottom-right | not visible | spread-across>",
    "detail": "FAIL if pink appears anywhere besides top-left, or teal appears anywhere besides bottom-left. FAIL if washes are spread across the whole frame or on the right half. Describe where you see the colored washes."
  }},
  "edge_border_artefact": {{
    "pass": <true iff scan step 4 found NO colored stripes/bands along left or right edges inside the white border>,
    "detail": "FAIL if a thin colored line, gradient band, or graphical strip is visible along the left or right edge of the photo (just inside the white frame). Describe any artefact seen. This often happens when Gemini paints its own gradient all the way to the frame edge — the result looks like an inner border."
  }},
  "photo_fills_frame": {{
    "pass": <true iff the photo fills the entire photo-area rectangle with NO white gaps, no transparent strips, and no obvious horizontal/vertical bands where the photo failed to extend>,
    "detail": "describe any white gap or photo-edge visible inside the intended photo rectangle"
  }},
  "logo_correct_shape": {{
    "pass": <true iff the bottom-right Outlier wordmark looks like the real Outlier brand mark — clean proportional letterforms, not warped, not cropped, not generic bold text>,
    "detail": "describe the logo shape/quality you see"
  }},
  "subject_looks_ai": {{
    "pass": true | false,
    "detail": "FAIL for uncanny skin, warped features, glitched hands, overly stock-photo perfect faces, plastic skin"
  }},
  "matches_reference_person": {{
    "pass": <set true iff the generated subject is CLEARLY a DIFFERENT person from the reference image person — different gender OR different ethnicity OR visibly different appearance. Set false ONLY if they look like the same person was regenerated>,
    "detail": "FAIL ONLY if the generated subject appears to be the same person as the reference (same gender + same ethnicity + similar pose/accessories). PASS if subject is a genuinely different person — even if both are professional-looking. The creative team intentionally uses diverse subjects — a different gender or ethnicity is expected and correct."
  }},
  "text_zone_contrast": {{
    "pass": true | false,
    "detail": "FAIL if the white headline or subheadline sits on a near-white background where it becomes hard to read"
  }},
  "professional_quality": {{
    "pass": true | false,
    "detail": "FAIL if the image looks amateur: weird color grading, muddy shadows, bad composition, obviously AI-generated background patterns, or anything that wouldn't ship in a real LinkedIn brand campaign"
  }},
  "summary": "one sentence overall take"
}}

Be harsh. Return JSON only.
"""


def _image_to_b64(path: str | Path) -> str:
    return base64.b64encode(Path(path).read_bytes()).decode("utf-8")


def _call_gemini_vision(prompt: str, creative_path: str, reference_path: str | None) -> dict:
    """
    Call Gemini vision with the creative + reference image. Returns parsed JSON.
    """
    api_key = config.GEMINI_API_KEY
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY not set — cannot run visual QC")

    parts = [{"text": prompt}]
    parts.append({
        "inline_data": {
            "mime_type": "image/png",
            "data": _image_to_b64(creative_path),
        }
    })
    if reference_path and Path(reference_path).exists():
        parts.append({
            "inline_data": {
                "mime_type": "image/png",
                "data": _image_to_b64(reference_path),
            }
        })

    url = _GEMINI_VISION_URL_TMPL.format(model=_GEMINI_VISION_MODEL, api_key=api_key)
    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "temperature": 0.0,
        },
    }
    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini vision QC error {resp.status_code}: {resp.text[:400]}")

    text_parts = (
        resp.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    raw = "".join(p.get("text", "") for p in text_parts).strip()
    # Strip markdown fences if present
    raw = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw).strip()
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        log.error("Gemini QC returned non-JSON: %s\n%s", e, raw[:500])
        raise


def qc_creative(
    creative_path: str | Path,
    reference_path: str | Path | None,
    headline: str,
    subheadline: str,
    intro_text: str = "",
    ad_headline: str = "",
    ad_description: str = "",
    cta_button: str = "",
) -> QCReport:
    """
    Run full QC on a single creative. Structural copy-length checks run FIRST — if copy
    fails, we return a copywriter-retry verdict without ever calling the vision API
    (no point asking Gemini to fix text that's too long; the text needs to be rewritten).

    When the LinkedIn ad copy fields (intro_text, ad_headline, ad_description, cta_button)
    are provided, they're validated alongside the image overlay text.
    """
    # ── Step 1: structural copy length validation (word/char/line count) ──
    copy_violations = validate_copy_lengths(
        headline, subheadline,
        intro_text=intro_text,
        ad_headline=ad_headline,
        ad_description=ad_description,
        cta_button=cta_button,
    )
    if copy_violations:
        log.info("Copy length violations found: %s", copy_violations)
        return QCReport(
            verdict="FAIL",
            checks={
                "Copy within limits (words/chars/lines)": False,
            },
            violations=copy_violations,
            retry_target="copywriter",
            copy_violations=copy_violations,
            retry_instruction=(
                "Copy exceeds hard limits. Send back to outlier-copy-writer:\n"
                + "\n".join(f"- {v}" for v in copy_violations)
                + f"\nHard limits: headline ≤{HEADLINE_MAX_WORDS} words, ≤{HEADLINE_MAX_CHARS} chars, "
                  f"≤{HEADLINE_MAX_LINES} lines when rendered. Subheadline ≤{SUBHEAD_MAX_WORDS} words, "
                  f"≤{SUBHEAD_MAX_CHARS} chars, ≤{SUBHEAD_MAX_LINES} lines when rendered."
            ),
        )

    # ── Step 2: vision-based image checks ──
    prompt = _QC_PROMPT.format(headline=headline, subheadline=subheadline)
    try:
        result = _call_gemini_vision(prompt, str(creative_path), str(reference_path) if reference_path else None)
    except Exception as exc:
        log.error("QC vision call failed: %s", exc)
        return QCReport(
            verdict="FAIL",
            checks={"qc_infrastructure": False},
            violations=[f"QC vision call failed: {exc}"],
            retry_target="none",
            retry_instruction="QC infrastructure error — do not ship, investigate before retrying",
        )

    checks = {"Copy within limits (words/chars/lines)": True}
    violations = []
    retry_hints: list[str] = []

    # When no reference image is available, mimicry check cannot be evaluated —
    # auto-pass it (we can't mimic a reference we never sent to Gemini).
    reference_unavailable = not reference_path or not Path(str(reference_path)).exists()

    check_map = {
        "rendered_text_in_photo":      ("No rendered text in photo",     "Gemini rendered text/letters/logos INTO the photo. Add explicit 'ZERO TEXT in image. No words, letters, logos, wordmarks, earnings figures, or caption-like shapes anywhere'."),
        "duplicate_logo":              ("No duplicate logos",            "A second Outlier logo is visible inside the photograph. Tell Gemini 'Do NOT render any Outlier logo, brand mark, or wordmark — the logo is composited post-hoc.'"),
        "text_overlaps_subject":       ("Text doesn't overlap subject",  "Headline is touching the subject's hair. Tell Gemini 'the TOP of the subject\\'s hair must sit at approximately 28% from the top of the frame, giving a small 3-5% visible gap between the bottom of the headline and the top of the hair. Text must NEVER touch hair, flyaways, or any subject pixels'."),
        "headroom_gap_appropriate":    ("Headroom gap not too big",      "The empty gap between the headline and the top of the subject's hair is TOO LARGE — looks like wasted space. Tell Gemini 'place the subject HIGHER in the frame: top of hair should sit at ~28% from the top, not lower. We want a small clean 3-5% gap between the headline bottom and the hairline — NOT a huge empty band'. Reduce the head-clearance requirement."),
        "gradient_position_correct":   ("Gradient matches reference",    "The gradient washes are in the WRONG position — pink/coral must be in TOP-LEFT corner only, teal/blue in BOTTOM-LEFT corner only. Right half must be neutral. Tell Gemini 'Pink/coral goes ONLY in the top-left quadrant anchored at (x=15%%, y=15%%). Teal/blue goes ONLY in the bottom-left quadrant anchored at (x=15%%, y=85%%). Do NOT put pink in top-right, do NOT put teal anywhere besides bottom-left, do NOT spread the washes across the whole frame, do NOT paint the right half. Match the reference image exactly.'"),
        "edge_border_artefact":        ("No gradient border artefact",   "A thin colored gradient line is visible on the left/right edges of the photo, looking like an inner border. Tell Gemini 'The gradient washes must fade to neutral BEFORE reaching the outer 5% of any edge. No colored stripe along any frame edge.'"),
        "photo_fills_frame":           ("Photo fills frame",             "Photo has white gaps or unfilled strips inside the intended photo rectangle. Verify bg_image sizing and cropping in compose_ad()."),
        "logo_correct_shape":          ("Logo renders correctly",        "The bottom-right Outlier logo is warped or not rendering from the SVG. Check that _rasterize_outlier_logo() resolved the SVG file and that DYLD_FALLBACK_LIBRARY_PATH is set."),
        "subject_looks_ai":            ("Subject looks authentic",       "Subject reads as AI-generated. Add 'authentic candid photo, real human skin texture with natural asymmetry, not AI-generated, not uncanny, not stock-photo perfect'."),
        "matches_reference_person":    ("Subject differs from reference","Subject mimics the reference-image person. Omit the reference image on retry, OR explicitly specify a different gender/ethnicity than the reference."),
        "text_zone_contrast":          ("Contrast adequate",             "Text zones are too bright. Tell Gemini 'Top 30%% must be mid-tone to dark background. No white walls, no blown-out windows in text zones.'"),
        "professional_quality":        ("Professional quality",          "Image lacks professional polish. Tell Gemini 'magazine-quality editorial photography, professional color grading, balanced composition, appropriate for a Fortune-500 LinkedIn campaign'."),
    }

    for key, (label, retry_hint) in check_map.items():
        # Auto-pass mimicry check when there is no reference image
        if key == "matches_reference_person" and reference_unavailable:
            checks[label] = True
            log.debug("matches_reference_person: auto-pass (no reference image provided)")
            continue
        block = result.get(key, {}) if isinstance(result, dict) else {}
        passed = bool(block.get("pass", False)) if isinstance(block, dict) else False
        checks[label] = passed
        if not passed:
            detail = (block.get("detail", "") if isinstance(block, dict) else "") or "check failed"
            violations.append(f"{label}: {detail}")
            retry_hints.append(retry_hint)

    verdict = "PASS" if all(checks.values()) else "FAIL"
    retry_instruction = ""
    retry_target = "none"
    if retry_hints:
        retry_target = "gemini"
        retry_instruction = (
            "Append the following to the Gemini prompt for this variant:\n"
            + "\n".join(f"- {h}" for h in retry_hints)
        )

    return QCReport(
        verdict=verdict,
        checks=checks,
        violations=violations,
        retry_instruction=retry_instruction,
        retry_target=retry_target,
    )


def qc_batch(
    creatives: list[dict],
    reference_path: str | Path | None,
) -> dict[str, QCReport]:
    """
    Run QC on all variants in a batch. Returns dict mapping angle -> QCReport.

    creatives: list of dicts with keys: angle, path, headline, subheadline
    """
    reports: dict[str, QCReport] = {}
    for c in creatives:
        angle = c["angle"]
        log.info("QC running for variant %s: %s", angle, c["path"])
        reports[angle] = qc_creative(
            creative_path=c["path"],
            reference_path=reference_path,
            headline=c["headline"],
            subheadline=c["subheadline"],
        )
        log.info("QC variant %s: %s (%d violations)", angle, reports[angle].verdict, len(reports[angle].violations))
    return reports


if __name__ == "__main__":
    # CLI smoke test: python -m src.image_qc <creative_path> <reference_path> "<headline>" "<subheadline>"
    import sys
    if len(sys.argv) < 5:
        print("Usage: python -m src.image_qc <creative> <reference> <headline> <subheadline>")
        sys.exit(1)
    logging.basicConfig(level=logging.INFO)
    report = qc_creative(sys.argv[1], sys.argv[2], sys.argv[3], sys.argv[4])
    print(json.dumps(report.to_dict(), indent=2))

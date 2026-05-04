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

# ── InMail (Message Ad) structural limits ─────────────────────────────────────
# Hard limits are LinkedIn API requirements — exceeding them causes 400 on upload.
# Soft limits are quality targets from 12-month performance analysis (2026-05-04).
INMAIL_SUBJECT_SOFT_MAX_CHARS = 60     # quality target; 28-char subjects scored 28% CTR
INMAIL_SUBJECT_HARD_MAX_CHARS = 200    # LinkedIn API hard limit
INMAIL_BODY_HARD_MAX_CHARS    = 1900   # LinkedIn API hard limit
INMAIL_BODY_SOFT_MIN_WORDS    = 100    # quality target lower bound (500 chars ≈ 5.80% CTR bucket)
INMAIL_BODY_SOFT_MAX_WORDS    = 130    # quality target upper bound (650 chars)
INMAIL_CTA_MAX_CHARS          = 20     # internal limit for readability

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

    # Field-scoped ban: 'Outlier' inside headline/subheadline conflicts with the
    # composited wordmark in the bottom strip — vision QC reads both as logos
    # and flags 'duplicate_logo'. Surfaced 2026-05-04 (GMR-0006 cohort 1).
    # Other fields (intro_text, ad_headline, ad_description, body) freely use
    # "Outlier" because they don't render against the wordmark.
    if field_name in ("headline", "subheadline"):
        if re.search(r"\boutlier\b", low):
            violations.append(
                f"{field_name} contains banned token 'Outlier': {text!r} "
                "(image-overlay text must not include the brand name; the wordmark is composited in the bottom strip)"
            )

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
    # Paths to PNG crops illustrating specific defects (currently: edge-bleed
    # bands). Caller attaches these as additional inline images on the next
    # Gemini call so the model SEES the failure instead of just reading the
    # textual hint.
    feedback_crop_paths: list[Path] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "verdict": self.verdict,
            "checks": self.checks,
            "violations": self.violations,
            "retry_instruction": self.retry_instruction,
            "retry_target": self.retry_target,
            "copy_violations": self.copy_violations,
            "feedback_crop_paths": [str(p) for p in self.feedback_crop_paths],
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


# ── Deterministic edge-bleed detector ─────────────────────────────────────────
# Catches the colored-gradient stripe Gemini paints at L/R photo edges. We look
# at the outermost photo column and count how many rows have noticeable color
# spread (max(RGB) - min(RGB)). Plain photo content (walls, shadows) clusters
# near zero spread; a saturated stripe (pink/teal bleed) clusters at ~0.4+.
# Thresholds tuned against the 14-image GMR-0005 dry-run set: 12 visibly bleed,
# 2 are clean — separation at frac>=0.10 with spread>0.20 perfectly matches.
EDGE_SPREAD_PIXEL_THRESHOLD = 0.20
EDGE_BLEED_ROW_FRAC_THRESHOLD = 0.10


def _detect_photo_bounds(arr) -> tuple[int, int] | None:
    """
    Locate the inner edges of the white frame on the L/R sides. Returns
    (left_x, right_x) where left_x is the first photo column and right_x is
    one past the last photo column. Returns None if no photo region detected.
    """
    import numpy as np

    h, w, _ = arr.shape
    is_color = ~np.all(arr > 240, axis=2)  # h x w bool
    y_lo, y_hi = int(h * 0.07), int(h * 0.80)
    col_color_frac = is_color[y_lo:y_hi].mean(axis=0)
    is_photo_col = col_color_frac > 0.30
    if not is_photo_col.any():
        return None
    left = int(np.argmax(is_photo_col))
    right = int(w - np.argmax(is_photo_col[::-1]))
    return left, right


def _detect_photo_bounds_4side(arr) -> tuple[int, int, int, int] | None:
    """
    Locate the inner edges of the white frame on ALL 4 sides. Returns
    (left, top, right, bottom) — left/top inclusive, right/bottom exclusive.
    Returns None if no photo region detected.

    Algorithm: same non-white-fraction detection as L/R, applied row-wise
    with a horizontal x-band that excludes the rounded corners. The bottom
    earnings strip (always white + Outlier wordmark in brown) is excluded
    by clamping the search range to [0, 0.85h] — strip is ~14% of canvas.
    """
    import numpy as np

    lr = _detect_photo_bounds(arr)
    if lr is None:
        return None
    left, right = lr
    h, w, _ = arr.shape
    is_color = ~np.all(arr > 240, axis=2)
    # Use the inner 60% of width to avoid rounded corners which can dilute
    # row-fraction. y_search_hi = 0.86h matches the canonical photo_bottom in
    # compose_ad (strip_h = 0.14h, so photo ends exactly at h - 0.14h = 0.86h).
    # Going higher would scan into the earnings strip's brown text/wordmark.
    x_lo = left + (right - left) // 5
    x_hi = right - (right - left) // 5
    y_search_hi = int(h * 0.86)
    row_color_frac = is_color[:y_search_hi, x_lo:x_hi].mean(axis=1)
    is_photo_row = row_color_frac > 0.30
    if not is_photo_row.any():
        return None
    top = int(np.argmax(is_photo_row))
    # bottom = last True in is_photo_row
    bottom = int(y_search_hi - np.argmax(is_photo_row[::-1]))
    return left, top, right, bottom


def crop_failure_edge(
    creative_path: str | Path,
    side: str,
    band_w: int = 80,
) -> Path | None:
    """
    Crop a vertical band from the L or R photo edge of `creative_path` and save
    to a temp PNG. Used to attach a visual failure example to the next Gemini
    call so the model SEES the defect instead of just reading about it.

    Returns the Path to the cropped PNG, or None if the source image can't be
    opened or the photo bounds can't be detected.
    """
    if side not in ("left", "right", "top", "bottom"):
        return None
    try:
        from PIL import Image
        import numpy as np
        import tempfile
    except Exception:
        return None
    try:
        img = Image.open(creative_path).convert("RGB")
    except Exception:
        return None
    arr = np.asarray(img)
    bounds = _detect_photo_bounds_4side(arr)
    if bounds is None:
        return None
    left, top, right, bottom = bounds
    if side == "left":
        x0, x1 = left, min(left + band_w, right)
        y0, y1 = top, bottom
    elif side == "right":
        x0, x1 = max(right - band_w, left), right
        y0, y1 = top, bottom
    elif side == "top":
        x0, x1 = left, right
        y0, y1 = top, min(top + band_w, bottom)
    else:  # bottom
        x0, x1 = left, right
        y0, y1 = max(bottom - band_w, top), bottom
    crop = img.crop((x0, y0, x1, y1))
    tmp = tempfile.NamedTemporaryFile(prefix=f"qc_edge_{side}_", suffix=".png", delete=False)
    crop.save(tmp.name)
    return Path(tmp.name)


def detect_edge_bleed(
    creative_path: str | Path,
    spread_threshold: float = EDGE_SPREAD_PIXEL_THRESHOLD,
    row_frac_threshold: float = EDGE_BLEED_ROW_FRAC_THRESHOLD,
) -> dict:
    """
    Structured edge-bleed result. Returns:
      {
        "passed": bool,
        "left_frac": float,    # fraction of L-edge column rows that are colored
        "right_frac": float,
        "top_frac": float,     # fraction of T-edge row columns that are colored
        "bottom_frac": float,
        "failed_sides": [...]  # subset of {"left","right","top","bottom"}
        "detail": str,         # human-readable summary
        "skipped": bool,       # True if PIL/numpy missing or file unreadable
      }
    Used by validate_edges_neutral (legacy 2-tuple wrapper) and by qc_creative
    when it needs the failed_sides to crop the bleeding region for retry feedback.

    All 4 edges checked because Gemini occasionally paints the gradient washes
    along ANY frame edge (GMR-0016 surfaced bottom-edge stripes that the
    previous L/R-only check missed).
    """
    try:
        from PIL import Image
        import numpy as np
    except Exception as exc:
        return {"passed": True, "left_frac": 0.0, "right_frac": 0.0,
                "top_frac": 0.0, "bottom_frac": 0.0,
                "failed_sides": [], "skipped": True,
                "detail": f"skipped: PIL/numpy unavailable ({exc})"}
    try:
        img = Image.open(creative_path).convert("RGB")
    except Exception as exc:
        return {"passed": True, "left_frac": 0.0, "right_frac": 0.0,
                "top_frac": 0.0, "bottom_frac": 0.0,
                "failed_sides": [], "skipped": True,
                "detail": f"skipped: could not open {creative_path} ({exc})"}

    arr = np.asarray(img)
    bounds4 = _detect_photo_bounds_4side(arr)
    if bounds4 is None:
        return {"passed": True, "left_frac": 0.0, "right_frac": 0.0,
                "top_frac": 0.0, "bottom_frac": 0.0,
                "failed_sides": [], "skipped": True,
                "detail": "skipped: photo region not detected"}
    left, top, right, bottom = bounds4

    # Crop margin to skip rounded corners on each axis. ~7% of photo dim is a
    # safe-but-stingy margin that excludes the corner curves while keeping
    # plenty of edge sample.
    h, w, _ = arr.shape
    photo_w = right - left
    photo_h = bottom - top
    y_lo = top + max(int(photo_h * 0.07), 1)
    y_hi = bottom - max(int(photo_h * 0.10), 1)  # extra bottom margin to dodge subject torso/desk
    x_lo = left + max(int(photo_w * 0.07), 1)
    x_hi = right - max(int(photo_w * 0.07), 1)

    def _col_frac(x: int) -> float:
        col = arr[y_lo:y_hi, x].astype(np.float32)
        spread = (col.max(axis=1) - col.min(axis=1)) / 255.0
        return float((spread > spread_threshold).mean())

    def _row_frac(y: int) -> float:
        row = arr[y, x_lo:x_hi].astype(np.float32)
        spread = (row.max(axis=1) - row.min(axis=1)) / 255.0
        return float((spread > spread_threshold).mean())

    left_frac = _col_frac(left)
    right_frac = _col_frac(right - 1)
    top_frac = _row_frac(top)
    bottom_frac = _row_frac(bottom - 1)

    failed_sides: list[str] = []
    if left_frac > row_frac_threshold:
        failed_sides.append("left")
    if right_frac > row_frac_threshold:
        failed_sides.append("right")
    if top_frac > row_frac_threshold:
        failed_sides.append("top")
    if bottom_frac > row_frac_threshold:
        failed_sides.append("bottom")

    if failed_sides:
        labels = {
            "left":   ("left edge",   left_frac),
            "right":  ("right edge",  right_frac),
            "top":    ("top edge",    top_frac),
            "bottom": ("bottom edge", bottom_frac),
        }
        parts = [f"{labels[s][0]} {labels[s][1]*100:.0f}% colored (>{row_frac_threshold*100:.0f}%)"
                 for s in failed_sides]
        detail = "Edge gradient bleed: " + "; ".join(parts)
    else:
        detail = (f"edges neutral: L={left_frac*100:.0f}% R={right_frac*100:.0f}% "
                  f"T={top_frac*100:.0f}% B={bottom_frac*100:.0f}%")

    return {
        "passed": not failed_sides,
        "left_frac": left_frac,
        "right_frac": right_frac,
        "top_frac": top_frac,
        "bottom_frac": bottom_frac,
        "failed_sides": failed_sides,
        "skipped": False,
        "detail": detail,
    }


def validate_edges_neutral(
    creative_path: str | Path,
    spread_threshold: float = EDGE_SPREAD_PIXEL_THRESHOLD,
    row_frac_threshold: float = EDGE_BLEED_ROW_FRAC_THRESHOLD,
) -> tuple[bool, str]:
    """
    Detect the colored-gradient edge bleed Gemini sometimes paints along the
    L/R sides of the photo. Sample the outermost photo column on each side
    and count rows whose RGB color spread (max-min over channels) exceeds
    `spread_threshold`. If that fraction is above `row_frac_threshold` on
    either side, FAIL — a substantial portion of the edge column is uniformly
    saturated, which is the signature of a bleed.

    Returns (passed, detail_message). Never raises — on PIL/numpy import or
    decoding failure returns (True, "skipped: <reason>") so the QC pipeline
    never hard-fails on a missing optional dep.

    Thin 2-tuple wrapper around detect_edge_bleed() for legacy callers + tests.
    """
    res = detect_edge_bleed(
        creative_path,
        spread_threshold=spread_threshold,
        row_frac_threshold=row_frac_threshold,
    )
    return res["passed"], res["detail"]


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
(3) Measure the visible gap between the BOTTOM edge of the headline text (letter descenders, including the lowest descender on letters like 'g', 'p', 'y') and the TOP of the subject's hair (including any flyaway strands above the main mass). Be STRICT: if a single hair pixel appears within OR right against the headline's bounding box, that is TOO CLOSE — answer TOO CLOSE. Report it as: TOO CLOSE (text touching, overlapping, or directly abutting hair — even by 1-2 pixels), JUST RIGHT (clearly visible empty band between text and head, ~3-8% of frame height), or TOO FAR (large wasted band, >10% of frame height). When in doubt between TOO CLOSE and JUST RIGHT, answer TOO CLOSE — borderline is FAIL.
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
    "pass": <true iff scan step 3 classified the gap as JUST RIGHT — text is not touching subject AND gap is not excessively large. Pass=false if gap_classification is TOO CLOSE; the borderline case (descenders within ~1% of frame height of the hairline) ALWAYS counts as TOO CLOSE>,
    "gap_classification": "<TOO CLOSE | JUST RIGHT | TOO FAR>",
    "detail": "FAIL if ANY pixel of hair (including flyaways) is within or right against the headline's text bounding box, even by 1-2 pixels. FAIL if there is an unusually large empty band between the text and the hairline. JUST RIGHT is a clearly visible 3-8% empty band between descenders and hairline."
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


# Alternate phrasings for retry-suffix hints. When the same QC check fails on
# consecutive retries, Gemini sometimes "learns past" a fixed phrasing — the
# same instruction stops moving the output. Rotating the wording on each
# subsequent attempt forces a new attentional path. The rotation only kicks in
# for keys present here; checks not listed reuse the static text from check_map.
# Pranav rule (2026-04-29): rotate every attempt 2+, not just every 2-3.
_RETRY_HINT_VARIANTS: dict[str, list[str]] = {
    "edge_border_artefact": [
        # attempt 1 — original, descriptive
        "A thin colored gradient line is visible on the left/right edges of the photo, looking like an inner border. Tell Gemini 'The gradient washes must fade to neutral BEFORE reaching the outer 5% of any edge. No colored stripe along any frame edge.'",
        # attempt 2 — concrete texture-anchor
        "STILL FAILING: gradient bleed at edges. Tell Gemini 'The outermost 6 pixels of every edge MUST be plain natural photo texture — wood, plaster, leaf, brick, fabric — NOT pink or teal or purple of any saturation. Edges must look like the photo continues; they must NOT look like a colored frame.'",
        # attempt 3 — negative-example phrasing
        "EDGES STILL COLORED. Tell Gemini 'Imagine the photo cropped slightly larger, with the outer 5% sliced off and discarded. The remaining edges must show ordinary photo content — bookshelf, plant leaves, wall, window. NEVER a saturated stripe of color. Reset the gradient washes to be CENTERED at (15%, 15%) and (15%, 85%) with rapid fade-to-neutral by 25% radius — they must NOT touch the frame.'",
    ],
    "subject_looks_ai": [
        "Subject reads as AI-generated. Add 'authentic candid photo, real human skin texture with natural asymmetry, not AI-generated, not uncanny, not stock-photo perfect'.",
        "PERSISTENT AI LOOK. Tell Gemini 'this must look like a 35mm film snapshot taken on a casual Tuesday — visible skin pores, slight color cast from window light, asymmetric features, NO retouched magazine quality. Documentary photography aesthetic.'",
        "STILL UNCANNY. Tell Gemini 'pretend this is a frame from a Vimeo travel vlog — handheld, slightly imperfect focus, natural variation in lighting across the face, real human texture. NOT a portrait studio session.'",
    ],
    "duplicate_logo": [
        "A second Outlier logo is visible inside the photograph. Tell Gemini 'Do NOT render any Outlier logo, brand mark, or wordmark — the logo is composited post-hoc.'",
        "DUPLICATE LOGO STILL APPEARING. Tell Gemini 'There must be ZERO branding, ZERO text shapes that look like words, ZERO letterforms in the photo region. The bottom-right Outlier wordmark is added in post-processing — your image must have BLANK space where it goes. Anywhere your image contains a letter-like shape, REPLACE it with plain background texture.'",
    ],
    "rendered_text_in_photo": [
        "Gemini rendered text/letters/logos INTO the photo. Add explicit 'ZERO TEXT in image. No words, letters, logos, wordmarks, earnings figures, or caption-like shapes anywhere'.",
        "TEXT STILL APPEARING. Tell Gemini 'I will manually delete any letterform you draw. The headline, subheadline, earnings strip, and Outlier wordmark are ALL composited in post-processing. Generate ONLY the unbranded photograph — clean of any pixel that resembles a glyph.'",
    ],
}


# ── InMail structural QC ──────────────────────────────────────────────────────

def validate_inmail_copy(
    subject: str,
    body: str,
    cta_label: str = "",
) -> list[str]:
    """
    Structural QC for a single InMail (Message Ad) variant. Mirrors
    validate_copy_lengths for static ads but scoped to InMail fields.

    Returns a list of human-readable violations, empty = clean. Violations
    are tagged as HARD or SOFT so callers can decide severity:
      - HARD: LinkedIn API will reject the campaign on upload (fix before sending)
      - SOFT: Quality target miss (log as warning, do not block)

    Checks:
      subject:   HARD >200 chars (LinkedIn limit), SOFT >60 chars (quality target)
      body:      HARD >1900 chars (LinkedIn limit),
                 SOFT word count outside 100–130 (quality target from CTR analysis),
                 SOFT contains bullet points or markdown headers (format violations)
      cta_label: HARD >20 chars (internal limit, LinkedIn has no enforced cap)
      all fields: Outlier brand voice scan (banned tokens, phrases, em dashes)
    """
    violations: list[str] = []
    subj = (subject or "").strip()
    body_text = (body or "").strip()
    cta = (cta_label or "").strip()

    # ── Subject ──
    if len(subj) > INMAIL_SUBJECT_HARD_MAX_CHARS:
        violations.append(
            f"[HARD] Subject is {len(subj)} chars (LinkedIn hard limit {INMAIL_SUBJECT_HARD_MAX_CHARS}): {subj!r}"
        )
    elif len(subj) > INMAIL_SUBJECT_SOFT_MAX_CHARS:
        violations.append(
            f"[SOFT] Subject is {len(subj)} chars (quality target ≤{INMAIL_SUBJECT_SOFT_MAX_CHARS}): {subj!r}"
        )

    # ── Body ──
    if len(body_text) > INMAIL_BODY_HARD_MAX_CHARS:
        violations.append(
            f"[HARD] Body is {len(body_text)} chars (LinkedIn hard limit {INMAIL_BODY_HARD_MAX_CHARS})"
        )

    word_count = len(body_text.split())
    if word_count < INMAIL_BODY_SOFT_MIN_WORDS:
        violations.append(
            f"[SOFT] Body is {word_count} words (quality target ≥{INMAIL_BODY_SOFT_MIN_WORDS}): too short, may feel abrupt"
        )
    elif word_count > INMAIL_BODY_SOFT_MAX_WORDS:
        violations.append(
            f"[SOFT] Body is {word_count} words (quality target ≤{INMAIL_BODY_SOFT_MAX_WORDS}): "
            f"longer bodies underperform — data shows 300-599 chars gets 5.80% CTR vs 4.33% for 900+ chars"
        )

    # Bullet points or markdown headers — format violations that look bad in LinkedIn
    if re.search(r"^\s*[-•*]\s+\S", body_text, re.MULTILINE):
        violations.append("[SOFT] Body contains bullet points — InMail must use plain paragraphs only")
    if re.search(r"^\s*#{1,3}\s+\S", body_text, re.MULTILINE):
        violations.append("[SOFT] Body contains markdown headers — InMail must use plain paragraphs only")

    # ── CTA label ──
    if len(cta) > INMAIL_CTA_MAX_CHARS:
        violations.append(
            f"[HARD] CTA label is {len(cta)} chars (limit {INMAIL_CTA_MAX_CHARS}): {cta!r}"
        )

    # ── Brand voice scan on all fields ──
    violations.extend(scan_brand_voice(subj, "subject"))
    violations.extend(scan_brand_voice(body_text, "body"))
    if cta:
        violations.extend(scan_brand_voice(cta, "cta_label"))

    return violations


def qc_creative(
    creative_path: str | Path,
    reference_path: str | Path | None,
    headline: str,
    subheadline: str,
    intro_text: str = "",
    ad_headline: str = "",
    ad_description: str = "",
    cta_button: str = "",
    attempt_index: int = 0,
) -> QCReport:
    """
    Run full QC on a single creative. Structural copy-length checks run FIRST — if copy
    fails, we return a copywriter-retry verdict without ever calling the vision API
    (no point asking Gemini to fix text that's too long; the text needs to be rewritten).

    When the LinkedIn ad copy fields (intro_text, ad_headline, ad_description, cta_button)
    are provided, they're validated alongside the image overlay text.

    `attempt_index` (0-based) lets the caller rotate the retry-suffix phrasing
    when the same QC check fails on consecutive attempts — Gemini sometimes
    learns past a fixed phrasing. See `_RETRY_HINT_VARIANTS`.
    """
    # ── Step 1: structural copy length validation (word/char/line count) ──
    # No early-return on copy failure: we ALSO run the vision + pixel checks so
    # the next retry sees image-side feedback alongside the copy fix. Pre-fix
    # behavior (early-return on copy) caused combined-defect creatives to take
    # 2-3 extra retries — Gemini was blind to image issues until copy passed.
    # Surfaced by GMR-0016 dry-run 2026-04-29.
    copy_violations = validate_copy_lengths(
        headline, subheadline,
        intro_text=intro_text,
        ad_headline=ad_headline,
        ad_description=ad_description,
        cta_button=cta_button,
    )
    if copy_violations:
        log.info("Copy length violations found (will continue to vision + pixel checks): %s", copy_violations)

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

    def _pick_hint(key: str, default_hint: str) -> str:
        variants = _RETRY_HINT_VARIANTS.get(key)
        if not variants:
            return default_hint
        idx = max(0, min(attempt_index, len(variants) - 1))
        return variants[idx]

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
            retry_hints.append(_pick_hint(key, retry_hint))

    # Deterministic pixel-level edge-bleed check — runs independently of the
    # vision model. The vision check `edge_border_artefact` is too permissive on
    # subtle bleeds, so we sample the photo's outermost columns directly. A FAIL
    # here routes through the same gemini-retry path as any vision failure.
    _PIXEL_EDGE_HINTS = [
        "PIXEL CHECK FOUND COLORED EDGE STRIPE. The outermost photo column on the L or R "
        "is a uniformly saturated stripe (pink/teal). Tell Gemini 'The photo MUST extend "
        "with natural neutral content all the way to the frame edge. The outer 4 pixels "
        "of every edge MUST be plain photo content (wall, shadow, neutral tones), NOT "
        "colored gradient banding. Fade the corner washes to FULLY NEUTRAL well before "
        "reaching the edge — no colored stripe of any width along any frame edge.'",
        "EDGE STRIPE STILL DETECTED BY PIXEL CHECK. Tell Gemini 'I am sampling the outer 4 "
        "pixels of each edge programmatically and counting any saturated rows. Currently "
        "you are still painting a stripe there. Move the corner washes INWARD: the gradient "
        "must be CENTERED at (15%%, 15%%) and (15%%, 85%%) and decay completely to RGB-neutral "
        "by (40%%, 40%%) and (40%%, 60%%) respectively. From 50%% inward there must be ZERO color cast.'",
        "PIXEL CHECK STILL FAILING. Gemini, this is mechanical: a script samples your output. "
        "Make the photo edges EQUAL to the photo center in saturation level — no peripheral "
        "color gradient anywhere. If you must include the pink/teal washes, paint them as a "
        "small soft circle in the inner-left region only. Do NOT extend any colored gradient "
        "past 30%% of the frame width on any axis.",
    ]
    edge_res = detect_edge_bleed(creative_path)
    edges_ok = edge_res["passed"]
    checks["No gradient bleed at photo edges (pixel)"] = edges_ok
    feedback_crop_paths: list[Path] = []
    if not edges_ok:
        violations.append(f"No gradient bleed at photo edges (pixel): {edge_res['detail']}")
        idx = max(0, min(attempt_index, len(_PIXEL_EDGE_HINTS) - 1))
        retry_hints.append(_PIXEL_EDGE_HINTS[idx])
        # Crop the bleeding edge from this attempt's PNG so the next Gemini call
        # can SEE the defect (multi-image input). Filenames are temp; the caller
        # owns cleanup. Best-effort — skip silently on any error.
        for side in edge_res.get("failed_sides", []):
            try:
                p = crop_failure_edge(creative_path, side)
                if p is not None:
                    feedback_crop_paths.append(p)
            except Exception as exc:
                log.warning("crop_failure_edge(%s) failed: %s", side, exc)

    # Merge copy + image findings. Copy violations fold into checks + violations
    # so the QCReport reflects EVERYTHING that's wrong, not just the image side.
    if copy_violations:
        checks["Copy within limits (words/chars/lines)"] = False
        violations = copy_violations + violations

    verdict = "PASS" if all(checks.values()) else "FAIL"

    # retry_target: copywriter wins if copy is broken (the rewriter must run).
    # The retry_instruction still carries any image-side hints so the next
    # attempt's prompt_suffix can include both the rewritten copy AND Gemini
    # feedback. Caller (generate_imagen_creative_with_qc) is responsible for
    # using BOTH on the next iteration.
    retry_target = "none"
    retry_instruction = ""
    if copy_violations:
        retry_target = "copywriter"
    elif retry_hints:
        retry_target = "gemini"
    if retry_hints:
        retry_instruction = (
            "Append the following to the Gemini prompt for this variant:\n"
            + "\n".join(f"- {h}" for h in retry_hints)
        )
    elif copy_violations:
        retry_instruction = (
            "Copy exceeds hard limits. Send back to outlier-copy-writer:\n"
            + "\n".join(f"- {v}" for v in copy_violations)
            + f"\nHard limits: headline ≤{HEADLINE_MAX_WORDS} words, ≤{HEADLINE_MAX_CHARS} chars, "
              f"≤{HEADLINE_MAX_LINES} lines when rendered. Subheadline ≤{SUBHEAD_MAX_WORDS} words, "
              f"≤{SUBHEAD_MAX_CHARS} chars, ≤{SUBHEAD_MAX_LINES} lines when rendered."
        )

    return QCReport(
        verdict=verdict,
        checks=checks,
        violations=violations,
        copy_violations=copy_violations,
        feedback_crop_paths=feedback_crop_paths,
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

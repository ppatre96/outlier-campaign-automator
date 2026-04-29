"""
Gemini creative generation for Outlier LinkedIn ads.

Pipeline per TG + copy variant:
  1. Build a TG-aware prompt replicating Outlier's lifestyle-photo aesthetic
  2. Call Gemini image model (via LiteLLM proxy) to generate the background photo
  3. Composite the ad: gradient overlay + white headline/subheadline + bottom strip
  4. Return the final PNG path (ready for LinkedIn upload)

Visual style — ground-truth from Outlier Static Ads v2 reference analysis:
  - CLOSE-UP PORTRAIT: subject's face and upper body fill 60-70% of frame
  - Text deliberately overlaps face area — LEFT-SIDE gradient provides contrast
  - Background: plant-dense home interior (this is Outlier's brand signature)
  - Gradient: soft LEFT-SIDE wash — pink/coral cloud top-left + teal cloud bottom-left
    Covers ~50% of image width, fading toward right. Right half mostly unaffected.
  - Headline: very large (~8.5% canvas), Inter Bold, white, centered, no shadow
  - Subheadline: smaller (~4.4% canvas), Inter Regular, white, centered
  - Bottom strip: white, earnings claim bold left + "Outlier" wordmark right
  - White border on all 4 sides, photo inset with rounded corners
  - Reference image + Outlier logo SVG embedded inline in Gemini prompt (not Drive URL)
"""
import base64
import logging
import os
import re
import tempfile
from io import BytesIO
from pathlib import Path

# cairosvg needs libcairo from Homebrew on macOS — set the search path before import
if Path("/opt/homebrew/lib").exists():
    os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

import requests
from PIL import Image, ImageDraw, ImageFont
import numpy as np

import config

log = logging.getLogger(__name__)

# ── Reference assets (inlined for Gemini — avoids Drive URL that Gemini cannot fetch) ──
# These are resolved once at import time from known local paths.
# Fall back gracefully if running outside the standard dev environment.

def _load_reference_image_b64() -> str:
    """Return base64-encoded Finance-Branded-BankerMale-Futureproof-1x1.png."""
    candidates = [
        Path("/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
        Path("/Users/pranavpatre/Desktop/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
        Path("/Users/pranavpatre/Downloads/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
    ]
    for p in candidates:
        if p.exists():
            data = base64.b64encode(p.read_bytes()).decode("utf-8")
            log.debug("Reference image loaded from %s (%d chars base64)", p, len(data))
            return data
    log.warning("Reference image Finance-Branded-BankerMale-Futureproof-1x1.png not found locally — prompt will omit inline image")
    return ""


def _load_outlier_logo_svg() -> str:
    """Return the Outlier logo SVG as a string."""
    candidates = [
        Path("/Users/pranavpatre/Downloads/outlier logo.svg"),
        Path("/Users/pranavpatre/Desktop/outlier logo.svg"),
    ]
    for p in candidates:
        if p.exists():
            return p.read_text()
    log.warning("outlier logo.svg not found locally — prompt will omit inline SVG")
    return ""


def _rasterize_outlier_logo(target_width: int = 800) -> Image.Image | None:
    """
    Rasterize the Outlier SVG logo to a PIL image, tinted to Outlier brown (#3D1A00).
    Returns None if cairosvg or the SVG file isn't available.
    """
    svg_path = None
    for p in (Path("/Users/pranavpatre/Downloads/outlier logo.svg"),
              Path("/Users/pranavpatre/Desktop/outlier logo.svg")):
        if p.exists():
            svg_path = p
            break
    if svg_path is None:
        return None
    try:
        import cairosvg
        png_bytes = cairosvg.svg2png(url=str(svg_path), output_width=target_width)
        logo = Image.open(BytesIO(png_bytes)).convert("RGBA")
        # Tint the black logo to Outlier brown while preserving alpha
        r, g, b, a = logo.split()
        brown = Image.new("RGB", logo.size, OUTLIER_BROWN)
        tinted = Image.merge("RGBA", (*brown.split(), a))
        return tinted
    except Exception as exc:
        log.warning("Could not rasterize Outlier logo SVG: %s", exc)
        return None


_REFERENCE_IMAGE_B64: str = _load_reference_image_b64()
_OUTLIER_LOGO_SVG: str = _load_outlier_logo_svg()

# ── Output size ───────────────────────────────────────────────────────────────
AD_SIZE = 1200   # 1200×1200 px for high-res LinkedIn 1:1

# ── Outlier brand colors ───────────────────────────────────────────────────────
OUTLIER_BROWN   = (61, 26, 0)
GRADIENT_PINK   = (255, 182, 193)  # top-right corner wash (Angle A/Expertise)
GRADIENT_BLUE   = (173, 216, 230)  # bottom-left corner wash
GRADIENT_ORANGE = (255, 200, 150)  # top-right variant for Earnings (B)
GRADIENT_GREEN  = (180, 230, 200)  # bottom-left variant for Flexibility (C)

# Per-angle gradient colors: (top_right_rgb, bottom_left_rgb)
ANGLE_GRADIENTS = {
    "A": (GRADIENT_PINK,   GRADIENT_BLUE),
    "B": (GRADIENT_ORANGE, GRADIENT_BLUE),
    "C": (GRADIENT_PINK,   GRADIENT_GREEN),
}

# ── Expression modifiers per copy angle ───────────────────────────────────────
# Derived from Outlier Static Ads v2 reference analysis:
# Angle A (Expertise): thoughtful, looking slightly off-camera — in their element
# Angle B (Earnings):  warm smile, relaxed — financially at ease
# Angle C (Flexibility): genuine laugh, looking at camera — free, unhurried
_ANGLE_EXPRESSIONS = {
    "A": "thoughtful expression, looking slightly off to the side, three-quarter angle",
    "B": "warm genuine smile, relaxed, looking off to the side",
    "C": "genuine relaxed smile, looking directly at camera",
}

# ── Gemini prompt template ────────────────────────────────────────────────────────
# Full spec document (used by outlier-creative-generator agent for context + design reference).
GEMINI_PROMPT_TEMPLATE = """\
PHOTO GENERATION PROMPT:

A close-up environmental portrait of a {photo_subject}. HEAD PLACEMENT (CRITICAL): the \
TOP of the subject's hair must sit at approximately 28% from the top of the frame — \
NOT higher (text would clip hair), NOT lower (creates excessive empty gap). The top \
~25% of the frame is a text-safe strip (no subject pixels, no hair, no flyaways); the \
top of the hair then appears at ~28%, with a small clean 3-5% visible gap between the \
bottom of the future headline overlay and the hairline. Subject's face centered \
vertically in the MIDDLE THIRD of the frame. Body/torso clearly visible below shoulders. \
EMPTY SPACE on the left and right sides of the subject — text will overlay beside/below \
subject, NOT on them. Lush plant-filled home interior: bookshelves, potted plants, wall \
art, warm natural window light. 85mm prime lens, shallow depth of field. {expression}. \
Shot on film, analog color grade, authentic lifestyle photo. NOT stock photo. NOT \
corporate headshot.

GRADIENT WASH — EXACTLY MATCH THE REFERENCE IMAGE (attached). Two specific corner washes only:

1. TOP-LEFT corner: soft pastel PINK/CORAL wash. Must originate in the TOP-LEFT quadrant \
   of the frame. Must NOT appear in the top-right, bottom-left, or bottom-right. Must be \
   CENTRED around roughly (x=15%, y=15%) and fade outward from there.

2. BOTTOM-LEFT corner: soft pastel TEAL/BLUE wash. Must originate in the BOTTOM-LEFT \
   quadrant of the frame. Must NOT appear in the top-left, top-right, or bottom-right. \
   Must be CENTRED around roughly (x=15%, y=85%) and fade outward from there.

EXPLICIT NEGATIVES (these are the most common failure modes):
- DO NOT place pink/coral in the top-right, bottom-left, or anywhere else besides the top-left quadrant.
- DO NOT place teal/blue in the top-left, top-right, or anywhere else besides the bottom-left quadrant.
- DO NOT spread the washes across the entire top or entire bottom — they stay in the LEFT half.
- DO NOT produce a symmetric or radial pattern — the washes are ONLY on the left side.
- The ENTIRE RIGHT HALF of the frame must remain neutral (natural photo tones, no colored washes).

EDGE RULE: The gradient washes must FADE COMPLETELY TO NEUTRAL before reaching the outer \
5% of any edge. The outermost 5% of every edge (top, bottom, left, right) must show \
natural photo content with NO colored line, NO gradient band, NO graphic stripe. \
Colored washes NEVER touch the frame edge.

These washes are PART OF THE PHOTO — baked into the lighting. Do NOT draw separate overlay \
graphics or border stripes.

TEXT-SAFE ZONE CONTRAST (CRITICAL for white text overlay readability):
- Top 30% of the frame MUST be a mid-tone to dark background (warm wood shelves, deeper plant \
  foliage, shadowed walls). Combined with the pink/coral wash, this gives white headline text \
  enough contrast to read. NO white walls, NO bright windows in the top band.
- Bottom third around subject's waist/desk area must also be mid-tone / darker — the teal wash \
  sits here. White subheadline text will overlay this zone.

CRITICAL OUTPUT CONSTRAINTS:
- DO NOT render any text, words, letters, logos, or wordmarks in the image
- DO NOT add the Outlier logo, earnings strip, or any branding — these are composited separately in post-processing
- OUTPUT ONLY THE PHOTOGRAPH — no overlays, no text, no graphics, no solid-color borders
- The headline + subheadline text will be limited to MAX 2 LINES each (≤6 words headline, \
  ≤7 words subheadline) — compose the photo assuming this text will fit in the top/bottom \
  safe zones without overlapping the subject's face.

The attached reference image shows the target COMPOSITION LAYOUT ONLY (subject framing, \
top-clear space, side margins, background color tones). Match that composition exactly. \
Ignore any text, logos, or branding visible in the reference — generate ONLY the clean \
background photograph.\
"""

# Reference image URL (Google Drive folder with reference PNG + logo SVG)
_REFERENCE_IMAGE_URL = "https://drive.google.com/drive/folders/1EYVpR40lXOiZFPBV-HkZ3Jx0CImjsHvV?usp=drive_link"


def _build_imagen_prompt(photo_subject: str, angle: str) -> tuple[str, str]:
    """
    Build Gemini image prompt matching Outlier Static Ads v2 aesthetic.

    Returns a (prompt_text, reference_image_b64) tuple:
      - prompt_text: full formatted prompt
      - reference_image_b64: base64-encoded Finance-Branded-BankerMale-Futureproof-1x1.png
        (attached as image part in direct Gemini API calls; empty string if not found)

    Args:
        photo_subject: Specific description of the subject (gender, ethnicity, profession, activity)
        angle: Copy variant angle ("A", "B", or "C") — determines expression in the prompt
    """
    expression = _ANGLE_EXPRESSIONS.get(angle, _ANGLE_EXPRESSIONS["A"])

    # Format the full template with actual values
    prompt_text = GEMINI_PROMPT_TEMPLATE.format(
        photo_subject=photo_subject,
        expression=expression,
    )

    return prompt_text, _REFERENCE_IMAGE_B64


# ── Reference image handling ────────────────────────────────────────────────

def _fetch_and_encode_image(image_url: str) -> str:
    """
    Fetch an image from a URL and encode it as base64.

    Args:
        image_url: HTTP(S) URL to the image

    Returns:
        Base64-encoded image data string (without data: URI prefix)

    Raises:
        RuntimeError: If image fetch or encoding fails
    """
    try:
        resp = requests.get(image_url, timeout=30)
        resp.raise_for_status()
        return base64.b64encode(resp.content).decode("utf-8")
    except Exception as e:
        raise RuntimeError(f"Failed to fetch/encode reference image from {image_url}: {e}")


# ── Gemini Imagen API call (via LiteLLM proxy) ────────────────────────────────

def _generate_imagen(
    prompt: str,
    gemini_api_key: str = "",
    reference_image_b64: str = "",
    feedback_image_b64s: list[str] | None = None,
) -> Image.Image:
    """
    Generate an image via the Scale LiteLLM proxy using Gemini image models.

    Uses the OpenAI-compatible /images/generations endpoint.  The model is
    configured via config.GEMINI_IMAGE_MODEL (default: gemini/gemini-2.5-flash-image).
    Other confirmed working options: gemini/imagen-4.0-generate-001,
    gemini/imagen-4.0-fast-generate-001, gemini/gemini-3.1-flash-image-preview.

    Falls back to the direct Google API (GEMINI_API_KEY) if LiteLLM is unavailable.

    Args:
        prompt: The image generation prompt (SVG already inlined in prompt text)
        gemini_api_key: Optional override for Gemini API key (falls back to config)
        reference_image_b64: Base64-encoded Finance-Branded-BankerMale-Futureproof-1x1.png
            (inline local file — not a URL). Sent as an image part in the Gemini
            multipart request so the model can directly see the reference composition.
            Empty string = no reference image attached (degrades gracefully).
        feedback_image_b64s: Optional list of additional base64 PNGs attached to
            the request. Used by the QC retry loop to show Gemini exactly what
            the previous attempt got wrong (e.g. a crop of the bleeding edge).
            Each is appended as an inline_data part AFTER the reference image.
            Only used by the direct-Google-API path; LiteLLM fallback ignores them.
    """
    # ── Primary: direct Google Gemini API (supports multipart with reference image) ──
    # Preferred when GEMINI_API_KEY is available because it can accept the reference
    # image as inline_data — which dramatically improves composition adherence.
    api_key = gemini_api_key or config.GEMINI_API_KEY
    if not api_key and not config.LITELLM_API_KEY:
        raise RuntimeError("No image generation available: LITELLM_API_KEY and GEMINI_API_KEY are both unset")

    # ── Fallback: LiteLLM proxy (text-only, no reference image attachment) ──
    # Only used when GEMINI_API_KEY is unavailable since it cannot attach images.
    if not api_key and config.LITELLM_API_KEY:
        url = f"{config.LITELLM_BASE_URL}/images/generations"
        headers = {
            "Authorization": f"Bearer {config.LITELLM_API_KEY}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": config.GEMINI_IMAGE_MODEL,
            "prompt": prompt,
            "n": 1,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=120)
        if resp.status_code == 200:
            imgs = resp.json().get("data", [])
            if imgs and "b64_json" in imgs[0]:
                return Image.open(BytesIO(base64.b64decode(imgs[0]["b64_json"]))).convert("RGBA")
            raise RuntimeError(f"LiteLLM image response missing b64_json: {resp.text[:300]}")
        raise RuntimeError(f"LiteLLM image generation failed ({resp.status_code}): {resp.text[:300]}")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models"
        f"/gemini-2.5-flash-image:generateContent?key={api_key}"
    )

    # Build request parts: text prompt first, then reference image inline,
    # then any QC-feedback failure-region crops.
    parts = [{"text": prompt}]
    if reference_image_b64:
        parts.append({
            "inline_data": {
                "mime_type": "image/png",
                "data": reference_image_b64,
            }
        })
        log.info("Reference image attached inline (%d chars base64)", len(reference_image_b64))
    else:
        log.warning("Reference image not available — Gemini will generate without composition reference")
    for i, fb_b64 in enumerate(feedback_image_b64s or []):
        if not fb_b64:
            continue
        parts.append({
            "inline_data": {
                "mime_type": "image/png",
                "data": fb_b64,
            }
        })
        log.info("QC feedback crop #%d attached inline (%d chars base64)", i + 1, len(fb_b64))

    payload = {
        "contents": [{"parts": parts}],
        "generationConfig": {"responseModalities": ["IMAGE", "TEXT"]},
    }
    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini direct API error {resp.status_code}: {resp.text[:400]}")

    resp_json = resp.json()
    candidates = resp_json.get("candidates", [])
    if not candidates:
        finish = resp_json.get("promptFeedback", {}).get("blockReason", "unknown")
        raise RuntimeError(f"Gemini direct API returned no candidates (blockReason={finish!r}): {resp.text[:400]}")

    candidate = candidates[0]
    finish_reason = candidate.get("finishReason", "")
    parts_resp = candidate.get("content", {}).get("parts", [])
    for part in parts_resp:
        if "inlineData" in part:
            return Image.open(BytesIO(base64.b64decode(part["inlineData"]["data"]))).convert("RGBA")

    # Log text parts to help diagnose safety blocks
    text_parts = [p.get("text", "") for p in parts_resp if "text" in p]
    log.warning("Gemini returned no image part. finishReason=%r text=%r", finish_reason, text_parts[:2])
    raise RuntimeError(
        f"Gemini direct API returned no image in response "
        f"(finishReason={finish_reason!r}, text={str(text_parts)[:200]})"
    )


# ── Image composition ─────────────────────────────────────────────────────────

def _make_gradient_overlay(width: int, height: int, angle: str) -> Image.Image:
    """
    Left-side soft wash gradient matching Outlier Static Ads v2 reference.

    Ground truth from 7 reference ads:
    - Pink/coral cloud radiating from TOP-LEFT, spreading ~50% across image width
    - Teal/blue cloud radiating from BOTTOM-LEFT, spreading ~50% across image width
    - Right half of image mostly unaffected (gradient fades to zero)
    - Both clouds are soft/diffuse — NOT tight corner dots
    - Opacity: ~40-45% at strongest point, fading outward

    This spread is what makes text readable when overlaid on the face area
    (left portion of frame where text sits).
    """
    # NOTE: Gemini now bakes the pink/coral + teal corner washes directly into the photo
    # (see GEMINI_PROMPT_TEMPLATE → "GRADIENT WASH" section). PIL should NOT add its own
    # colored clouds on top — that caused the "gradient border" artefact the user flagged.
    # We only add subtle DARK wash bands in the text zones so white text stays readable
    # regardless of what Gemini produces.
    overlay = np.zeros((height, width, 4), dtype=np.float32)
    y_grid, _ = np.meshgrid(
        np.linspace(0, 1, height),
        np.linspace(0, 1, width),
        indexing="ij",
    )

    # Dark wash bands behind text zones — gentle, no colored tint.
    top_band    = np.clip(1.0 - y_grid / 0.28, 0, 1) ** 1.5 * 0.32
    bottom_band = np.clip((y_grid - 0.72) / 0.20, 0, 1) ** 1.5 * 0.28
    dark_alpha  = np.maximum(top_band, bottom_band)
    overlay[:, :, 3] = np.clip(dark_alpha, 0, 1)
    return Image.fromarray((np.clip(overlay, 0, 1) * 255).astype(np.uint8), "RGBA")


def _load_font(size: int, bold: bool = False):
    """
    Load Inter font (Outlier brand standard).
    Bold for headlines, Regular for subheadlines.
    Falls back to system fonts if Inter is unavailable.
    """
    inter_paths = (
        [
            "/System/Library/Fonts/Inter.ttf",
            "/Library/Fonts/Inter.ttf",
            "/usr/share/fonts/opentype/inter/Inter-Bold.ttf",
            "/usr/share/fonts/truetype/inter/Inter-Bold.ttf",
        ] if bold else [
            "/System/Library/Fonts/Inter.ttf",
            "/Library/Fonts/Inter.ttf",
            "/usr/share/fonts/opentype/inter/Inter-Regular.ttf",
            "/usr/share/fonts/truetype/inter/Inter-Regular.ttf",
        ]
    )
    # Try Inter first
    for path in inter_paths:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue
    # Fallback chain (Avenir Next → Arial → Liberation)
    fallback_candidates = (
        [
            "/System/Library/Fonts/Avenir Next.ttc",
            "/Library/Fonts/Arial Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ] if bold else [
            "/System/Library/Fonts/Avenir Next.ttc",
            "/Library/Fonts/Arial.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
    )
    for path in fallback_candidates:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _wrap_text(text: str, font, max_width: int) -> list[str]:
    words = text.replace("\n", " \n ").split(" ")
    lines, cur = [], ""
    dummy = Image.new("RGB", (1, 1))
    draw  = ImageDraw.Draw(dummy)
    for word in words:
        if word == "\n":
            lines.append(cur.strip()); cur = ""; continue
        test = (cur + " " + word).strip()
        if draw.textlength(test, font=font) <= max_width:
            cur = test
        else:
            if cur: lines.append(cur.strip())
            cur = word
    if cur.strip(): lines.append(cur.strip())
    return lines


def _draw_text_left(draw, lines, font, y_top, x_left, color, line_spacing=10, canvas_width=None):
    """
    Draws text lines. When canvas_width is provided, each line is centered on the canvas.
    NO drop shadow — reference images have clean text on gradient.
    """
    y = y_top
    for line in lines:
        if canvas_width is not None:
            x = (canvas_width - draw.textlength(line, font=font)) / 2
        else:
            x = x_left
        draw.text((x, y), line, font=font, fill=color)
        bbox = draw.textbbox((x, y), line, font=font)
        y += (bbox[3] - bbox[1]) + line_spacing
    return y


def _add_bottom_strip(canvas: Image.Image, bottom_text: str, strip_height_frac: float = 0.14) -> Image.Image:
    """
    White strip at bottom matching Outlier Static Ads v2 reference:
    - Left: two lines — earnings claim line 1, "Fully remote." line 2
    - Right: actual Outlier SVG logo (rasterized and tinted to #3D1A00 brown)
      Falls back to Inter Bold "Outlier" text only if SVG rasterization is unavailable.
    """
    size    = canvas.size[0]
    strip_h = int(size * strip_height_frac)
    strip_y = size - strip_h
    draw    = ImageDraw.Draw(canvas)
    draw.rectangle([0, strip_y, size, size], fill=(255, 255, 255, 255))

    pad = int(size * 0.05)

    # Split bottom_text into two lines at ". " or mid-point
    if ". " in bottom_text:
        line1, line2 = bottom_text.split(". ", 1)
        line1 = line1 + "."
    else:
        line1, line2 = bottom_text, ""

    body_size = int(size * 0.027)
    body_font = _load_font(body_size)
    bold_font = _load_font(body_size, bold=True)

    # Vertically center the two lines in the strip
    line_h    = body_size + 4
    total_h   = line_h * (2 if line2 else 1)
    text_y    = strip_y + (strip_h - total_h) // 2

    draw.text((pad, text_y), line1, font=bold_font, fill=OUTLIER_BROWN)
    if line2:
        draw.text((pad, text_y + line_h), line2, font=body_font, fill=OUTLIER_BROWN)

    # Outlier wordmark — paste the actual SVG logo (tinted brown), right-aligned
    logo_target_h = int(strip_h * 0.48)  # ~48% of strip height
    logo_img = _rasterize_outlier_logo(target_width=800)
    if logo_img is not None:
        # Scale to the target height
        scale = logo_target_h / logo_img.height
        new_w = int(logo_img.width * scale)
        logo_img = logo_img.resize((new_w, logo_target_h), Image.LANCZOS)
        logo_x = size - pad - new_w
        logo_y = strip_y + (strip_h - logo_target_h) // 2
        canvas.paste(logo_img, (logo_x, logo_y), logo_img)
    else:
        # Fallback: text wordmark in Inter Bold
        logo_font = _load_font(int(size * 0.055), bold=True)
        logo_text = "Outlier"
        logo_w    = draw.textlength(logo_text, font=logo_font)
        logo_h    = int(size * 0.055)
        logo_y    = strip_y + (strip_h - logo_h) // 2
        draw.text((size - pad - logo_w, logo_y), logo_text, font=logo_font, fill=OUTLIER_BROWN)

    return canvas


def compose_ad(
    bg_image: Image.Image,
    headline: str,
    subheadline: str,
    angle: str = "A",
    bottom_text: str = "",
    with_bottom_strip: bool = True,
) -> Image.Image:
    """
    Composite the final Outlier ad creative from a background photo.

    Layout (matches reference Finance-Branded-BankerMale style):
      - White canvas (1200×1200)
      - Photo inset with white border on all 4 sides (~3.3% each)
      - Gradient overlay on the photo (corner washes)
      - White bold headline centered over the photo (top area)
      - White regular subheadline centered over the photo (near bottom)
      - White bottom strip: earnings + Outlier wordmark
    """
    size    = AD_SIZE
    border  = int(size * 0.033)          # ~40px white border on L / R / T

    strip_h = int(size * 0.14) if with_bottom_strip else border
    photo_x = border
    photo_y = border
    photo_w = size - 2 * border          # 1120px
    photo_h = size - strip_h - border   # 992px  (top border + bottom strip consumed)

    # ── 1. Crop generated image to exact photo aspect ratio, then resize ───────
    # Gemini outputs a square (1:1). The photo area is photo_w × photo_h which
    # is slightly wider than tall (~1.13:1). Resizing square→non-square directly
    # causes vertical stretching. Instead: center-crop the square to the target
    # ratio first, then resize — preserves correct proportions with no distortion.
    target_ratio = photo_w / photo_h
    src_w, src_h = bg_image.size
    src_ratio = src_w / src_h

    if src_ratio > target_ratio:
        # Generated image wider than needed — crop the sides
        new_w = int(src_h * target_ratio)
        x_off = (src_w - new_w) // 2
        bg_image = bg_image.crop((x_off, 0, x_off + new_w, src_h))
    elif src_ratio < target_ratio:
        # Generated image taller than needed — crop top and bottom equally
        new_h = int(src_w / target_ratio)
        y_off = (src_h - new_h) // 2
        bg_image = bg_image.crop((0, y_off, src_w, y_off + new_h))

    bg_resized = bg_image.convert("RGBA").resize((photo_w, photo_h), Image.LANCZOS)

    # ── 2. Gradient overlay at photo dimensions ────────────────────────────────
    overlay    = _make_gradient_overlay(photo_w, photo_h, angle)
    photo_comp = Image.alpha_composite(bg_resized, overlay)

    # ── 3. Rounded corners clipped to the photo ────────────────────────────────
    photo_mask = Image.new("L", (photo_w, photo_h), 0)
    ImageDraw.Draw(photo_mask).rounded_rectangle(
        [0, 0, photo_w - 1, photo_h - 1],
        radius=int(size * 0.025),
        fill=255,
    )
    photo_comp.putalpha(photo_mask)

    # ── 5. White canvas + paste inset photo ────────────────────────────────────
    canvas = Image.new("RGBA", (size, size), (255, 255, 255, 255))
    canvas.paste(photo_comp, (photo_x, photo_y), photo_comp)

    # ── 6. Text (centered on full canvas width, y relative to photo area) ──────
    draw       = ImageDraw.Draw(canvas)
    max_text_w = int(photo_w * 0.88)

    hl_size = int(size * 0.085)   # ~102px — matches large bold headline in reference
    hl_min  = int(size * 0.060)   # floor: still legible at 72px
    hl_font  = _load_font(hl_size, bold=True)
    hl_lines = _wrap_text(headline, hl_font, max_text_w)
    while len(hl_lines) > 2 and hl_size > hl_min:
        hl_size -= int(size * 0.004)
        hl_font  = _load_font(hl_size, bold=True)
        hl_lines = _wrap_text(headline, hl_font, max_text_w)
    _draw_text_left(
        draw, hl_lines, hl_font,
        photo_y + int(photo_h * 0.06),
        0, (255, 255, 255, 255),
        line_spacing=14, canvas_width=size,
    )

    sh_font  = _load_font(int(size * 0.044))
    sh_lines = _wrap_text(subheadline, sh_font, int(photo_w * 0.82))
    _draw_text_left(
        draw, sh_lines, sh_font,
        photo_y + int(photo_h * 0.84),
        0, (255, 255, 255, 255),
        line_spacing=10, canvas_width=size,
    )

    # ── 7. Bottom strip ────────────────────────────────────────────────────────
    canvas = canvas.convert("RGB")
    if with_bottom_strip and bottom_text:
        canvas = _add_bottom_strip(canvas, bottom_text, strip_h / size)

    return canvas


# ── photo_subject validation ──────────────────────────────────────────────────

# Patterns that indicate a generic, cohort-agnostic description.
# The LiteLLM prompt explicitly forbids these, but this guard catches
# cases where the LLM truncates or falls back to a stock phrase.
_GENERIC_SUBJECT_PATTERNS = [
    r"professional\s+(person|individual|at\s+a?\s*(?:laptop|computer|desk))",
    r"scientist\s+at\s+a\s+computer",
    r"person\s+working\s+at\s+a\s+laptop",
    r"^(male|female)\s+(professional|scientist|person)\s*$",
    r"domain\s+expert",
    r"remote\s+worker",
    r"knowledge\s+worker",
]


def validate_photo_subject(photo_subject: str) -> None:
    """
    Raise ValueError if photo_subject matches a known generic pattern.

    The photo_subject must be specific to the cohort — describing a concrete
    profession, attire, setting, and optionally geography. Generic descriptions
    cause Gemini to generate stock-photo-style images unrelated to the audience.

    Called by generate_imagen_creative() before the Gemini API call.

    Args:
        photo_subject: The subject description produced by build_copy_variants().

    Raises:
        ValueError: If the subject matches a forbidden generic pattern.

    Examples of valid subjects:
        "female South Asian cardiologist, reviewing ECG data on a laptop at home"
        "male Northern European DNA sequencing researcher, reviewing sequencing data"

    Examples of invalid (raises ValueError):
        "professional person at a laptop"
        "scientist at a computer"
        "domain expert"
    """
    subject_lower = photo_subject.strip().lower()
    for pattern in _GENERIC_SUBJECT_PATTERNS:
        if re.search(pattern, subject_lower):
            raise ValueError(
                f"photo_subject is too generic: '{photo_subject}'. "
                "It must describe the cohort's actual profession, attire, setting, "
                "and optionally geography — e.g. 'female South Asian cardiologist, "
                "reviewing ECG data on a laptop at home'. "
                "Check build_copy_variants() output and re-run."
            )


# ── Public entry point ────────────────────────────────────────────────────────

def generate_imagen_creative(
    variant: dict,
    photo_subject: str | None = None,
    gemini_api_key: str | None = None,
    tg_category: str | None = None,  # kept for backward compat, unused
    prompt_suffix: str = "",
    attach_reference_image: bool = True,
    feedback_image_paths: list | None = None,
    **_kwargs,
) -> Path:
    """
    Full pipeline: Gemini background photo → composed ad PNG.

    Args:
        variant:       copy variant dict — keys: angle, headline, subheadline, cta,
                       and optionally photo_subject (overrides the photo_subject arg)
        photo_subject: specific scene description derived from cohort data by
                       ad-creative-brief-generator. Must describe the actual profession,
                       attire, setting, and geography — NOT a generic category label.
                       Falls back to variant["photo_subject"] if present.
        gemini_api_key: Gemini API key (falls back to config.GEMINI_API_KEY)
        prompt_suffix: Extra instructions appended to the Gemini prompt. Used by the QC
                       retry loop to pass specific failure-mode feedback back to the model.
        attach_reference_image: Whether to attach the Finance-Branded-BankerMale reference
                       image. QC may set this to False on retry when the reference is
                       causing the model to mimic the reference person or render its text.

    Returns:
        Path to the composed PNG (temp file — caller owns cleanup).
    """
    angle       = variant.get("angle", "A")
    headline    = variant.get("headline", "")
    subheadline = variant.get("subheadline", "")

    # photo_subject: prefer explicit arg, then variant field
    subject = photo_subject or variant.get("photo_subject", "")
    if not subject:
        raise RuntimeError(
            "photo_subject is required — pass the cohort-derived subject description "
            "from ad-creative-brief-generator, not a TG category label."
        )

    validate_photo_subject(subject)  # raises ValueError if generic

    prompt, ref_img_b64 = _build_imagen_prompt(subject, angle)
    if prompt_suffix:
        prompt = prompt + "\n\nADDITIONAL QC FEEDBACK (apply strictly):\n" + prompt_suffix
    if not attach_reference_image:
        ref_img_b64 = ""

    log.info("Gemini call — angle=%s photo_subject=%r", angle, subject[:80])
    log.info("Imagen prompt (first 200 chars): %s", prompt[:200])
    log.info("Reference image inline: %s (%d chars b64)",
             "YES" if ref_img_b64 else "NO (disabled or file not found)", len(ref_img_b64))
    if prompt_suffix:
        log.info("QC prompt suffix applied: %s", prompt_suffix[:200])

    feedback_b64s: list[str] = []
    for p in (feedback_image_paths or []):
        try:
            with open(str(p), "rb") as fh:
                feedback_b64s.append(base64.b64encode(fh.read()).decode("ascii"))
        except Exception as exc:
            log.warning("Could not load feedback crop %s: %s", p, exc)

    bg_image = _generate_imagen(
        prompt,
        gemini_api_key or config.GEMINI_API_KEY,
        reference_image_b64=ref_img_b64,
        feedback_image_b64s=feedback_b64s,
    )
    log.info("Imagen photo received (%dx%d)", bg_image.width, bg_image.height)

    earnings_match = re.search(
        r'\$[\d,]+(?:\.\d+)?(?:\s*USD)?(?:\s*(?:/hr|per hour|weekly|hourly))?',
        subheadline,
    )
    if earnings_match:
        bottom_text = f"Earn {earnings_match.group()} or more. Fully remote."
    else:
        bottom_text = "Earn $25–$50 USD per hour. Fully remote."

    ad_image = compose_ad(
        bg_image=bg_image,
        headline=headline,
        subheadline=subheadline,
        angle=angle,
        bottom_text=bottom_text,
        with_bottom_strip=True,
    )

    safe_label = re.sub(r"[^a-z0-9]", "_", subject[:30].lower())
    out_path = Path(tempfile.mktemp(suffix=f"_gemini_{safe_label}_{angle}.png"))
    ad_image.save(out_path, "PNG", optimize=True)
    log.info("Gemini creative saved: %s (%d bytes)", out_path, out_path.stat().st_size)
    return out_path


_QC_MAX_RETRIES_DEFAULT = int(os.getenv("QC_MAX_RETRIES", "9"))


def generate_imagen_creative_with_qc(
    variant: dict,
    photo_subject: str | None = None,
    reference_image_path: str | Path | None = None,
    max_retries: int = _QC_MAX_RETRIES_DEFAULT,
    copy_rewriter: "callable | None" = None,
    initial_prompt_suffix: str = "",
    no_reference_image: bool = False,
    **kwargs,
) -> tuple[Path, dict]:
    """
    Generate a creative with automated QC + retry loop.

    Pranav rule (2026-04-29): we always ship a creative — keep retrying until QC
    PASSes or `max_retries` is exhausted, and on exhaust ship the BEST attempt
    (fewest violations) rather than the last attempt. Default cap is 9 (= 10
    attempts ≈ 5 min worst case per variant); override via QC_MAX_RETRIES env.

    Handles two distinct failure classes:
      1. **Copy failure** (headline/subheadline exceeds word/char/line limits) — the image
         would just get cut off, so we send the variant back to the copy rewriter
         (if one is provided) BEFORE even calling Gemini. No image generation wasted.
      2. **Image failure** (Gemini rendered text, duplicate logo, mimicry, etc.) —
         regenerate the image with the QC-suggested prompt suffix. On the second image
         retry we drop the reference image if mimicry was flagged.

    Args:
        copy_rewriter: optional callable taking (variant_dict, copy_violations: list[str])
                       and returning an updated variant dict with rewritten headline/
                       subheadline. Called if QC detects copy-length violations. If None,
                       copy failures cause immediate FAIL without regeneration.
        initial_prompt_suffix: Extra constraints injected into the FIRST generation
                       attempt — applied before QC runs. Use this to front-load known
                       failure modes for a specific angle (e.g., Angle A earnings banner)
                       so the first generation is already hardened rather than relying
                       on a QC failure to surface the instruction.

    Returns (final_path, qc_report_dict). On PASS, the report's verdict is "PASS".
    On cap-exhaust, returns the lowest-violation attempt with verdict="FAIL" so the
    caller can decide whether to ship the best-so-far image or skip the creative.
    """
    from src.copy_design_qc import qc_creative, validate_copy_lengths  # local import

    if no_reference_image:
        # Caller explicitly opts out of the reference image for both generation AND QC.
        reference_image_path = None
        attach_ref_initial = False
    else:
        attach_ref_initial = True
        if reference_image_path is None:
            for p in (
                Path("/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
                Path("/Users/pranavpatre/Desktop/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
            ):
                if p.exists():
                    reference_image_path = p
                    break

    # ── Pre-flight copy validation ──
    # If copy is already too long, rewrite before wasting a Gemini call.
    copy_retry_budget = 2
    while copy_retry_budget > 0:
        copy_violations = validate_copy_lengths(
            variant.get("headline", ""),
            variant.get("subheadline", ""),
        )
        if not copy_violations:
            break
        log.warning("Pre-flight copy violations for angle %s: %s", variant.get("angle"), copy_violations)
        if copy_rewriter is None:
            log.error("No copy_rewriter provided — cannot fix copy violations. Skipping generation.")
            return Path("/dev/null"), {
                "verdict": "FAIL",
                "retry_target": "copywriter",
                "copy_violations": copy_violations,
                "violations": copy_violations,
            }
        variant = copy_rewriter(variant, copy_violations)
        copy_retry_budget -= 1

    prompt_suffix = initial_prompt_suffix  # seed from caller; QC failures append to it
    attach_ref   = attach_ref_initial
    last_report: dict = {"verdict": "FAIL", "violations": ["not attempted"]}
    path = Path("/dev/null")
    # Track best-so-far across the loop. If we exhaust the cap without a PASS,
    # we ship the lowest-violation attempt rather than whatever the last one was
    # (Gemini sometimes regresses on later retries).
    best_violations = float("inf")
    best_path = path
    best_report = last_report
    # QC-feedback failure-region crops attached on the NEXT call (Pranav rule
    # 2026-04-29: Gemini ignores text hints, so we also attach a visual
    # example of the defect from the previous attempt's PNG).
    feedback_image_paths: list = []

    for attempt in range(max_retries + 1):
        log.info("Creative generation attempt %d/%d (angle=%s)",
                 attempt + 1, max_retries + 1, variant.get("angle", "?"))
        path = generate_imagen_creative(
            variant=variant,
            photo_subject=photo_subject,
            prompt_suffix=prompt_suffix,
            attach_reference_image=attach_ref,
            feedback_image_paths=feedback_image_paths,
            **kwargs,
        )

        try:
            report = qc_creative(
                creative_path=path,
                reference_path=reference_image_path,
                headline=variant.get("headline", ""),
                subheadline=variant.get("subheadline", ""),
                intro_text=variant.get("intro_text", ""),
                ad_headline=variant.get("ad_headline", ""),
                ad_description=variant.get("ad_description", ""),
                cta_button=variant.get("cta_button", ""),
                attempt_index=attempt,
            )
        except Exception as exc:
            log.warning("QC could not run on attempt %d: %s — accepting creative without QC", attempt + 1, exc)
            return path, {"verdict": "UNKNOWN", "error": str(exc)}

        last_report = report.to_dict()
        log.info("QC attempt %d: %s (target=%s, %d violations)",
                 attempt + 1, report.verdict, report.retry_target, len(report.violations))

        if report.verdict == "PASS":
            return path, last_report

        # Track best-so-far for cap-exhaust fallback
        n_viol = len(report.violations)
        if n_viol < best_violations:
            best_violations = n_viol
            best_path = path
            best_report = last_report

        # Route retry based on failure type
        if report.retry_target == "copywriter":
            if copy_rewriter is None:
                log.error("Copy QC failed and no copy_rewriter available — returning FAIL")
                return path, last_report
            log.info("Copy QC failed — invoking copy_rewriter")
            variant = copy_rewriter(variant, report.copy_violations)
            prompt_suffix = ""  # fresh image attempt, not a gemini-feedback suffix
            continue

        # Gemini-side image failure — append QC feedback to existing suffix (preserve initial constraints)
        qc_suffix = report.retry_instruction
        if initial_prompt_suffix and initial_prompt_suffix not in (prompt_suffix or ""):
            prompt_suffix = initial_prompt_suffix + "\n\n" + qc_suffix
        else:
            prompt_suffix = qc_suffix
        # Capture failure-region crops from THIS attempt to attach to the NEXT
        # call. We only keep the latest set (older crops are temp files; OS will
        # GC eventually). Augmenting the suffix with a marker that tells Gemini
        # the extra images are defect examples, not composition references.
        feedback_image_paths = list(report.feedback_crop_paths or [])
        if feedback_image_paths:
            crop_note = (
                "\n\nVISUAL FAILURE EXAMPLES ATTACHED: the additional image(s) below "
                "are CROPS OF THE DEFECT from your previous attempt — they show the exact "
                "edge bleed pattern you must NOT reproduce. Treat them as negative examples "
                "(do NOT match these), NOT as composition references."
            )
            prompt_suffix = (prompt_suffix or "") + crop_note
            log.info("Attaching %d failure-region crop(s) to next attempt", len(feedback_image_paths))
        if attempt >= 1 and (
            "reference" in report.retry_instruction.lower()
            or any("mimic" in v.lower() or "matches_reference" in v.lower() for v in report.violations)
        ):
            attach_ref = False
            log.info("QC flagged reference-image mimicry — dropping reference image for next retry")

    log.warning(
        "Creative still failing QC after %d attempts — returning best-so-far attempt with %d violations (last had %d)",
        max_retries + 1, best_violations, len(last_report.get("violations", [])),
    )
    return best_path, best_report

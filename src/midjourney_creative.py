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
  - Headline: very large (~8.5% canvas), Avenir Next Bold, white, centered, no shadow
  - Subheadline: smaller (~4.4% canvas), Avenir Next Regular, white, centered
  - Bottom strip: white, earnings claim bold left + "Outlier" wordmark right
  - White border on all 4 sides, photo inset with rounded corners
"""
import base64
import logging
import re
import tempfile
from io import BytesIO
from pathlib import Path

import requests
from PIL import Image, ImageDraw, ImageFont
import numpy as np

import config

log = logging.getLogger(__name__)

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

# Style suffix: authentic UGC/film aesthetic — NOT corporate stock photo
_IMAGEN_STYLE_SUFFIX = (
    "shot on film, 85mm prime lens, shallow depth of field, "
    "warm natural window light, analog color grade, "
    "authentic lifestyle photo, NOT stock photo, NOT corporate headshot, NOT AI generated"
)

# ── Imagen prompt template ────────────────────────────────────────────────────
# GROUND TRUTH from Outlier Static Ads v2 reference images (7 ads analyzed):
#
# Photo style: CLOSE-UP PORTRAIT. Subject's face and upper body fill 60-70%
# of the frame. This is NOT a wide/medium shot — it's a tight environmental
# portrait where the person is the dominant element.
#
# Background: PLANT-DENSE home interior visible around and behind subject
# (bookshelves, wall art, potted plants, warm décor). This is Outlier's brand
# signature — every single reference ad has this.
#
# Text overlaps face area by design — the left-side gradient provides contrast.
# Do NOT try to push the face lower or shoot from far away.
#
# photo_subject format: "[gender] [ethnicity] [profession], [activity]"
# e.g. "male South Asian software developer, working at a laptop"
# SHORT — the template adds all room/light/style context.


def _build_imagen_prompt(photo_subject: str, angle: str) -> str:
    """
    Build Gemini image prompt matching Outlier Static Ads v2 aesthetic.

    photo_subject must be SHORT:
      "[gender] [ethnicity] [profession], [activity]"
    e.g. "male South Asian software developer, working at a laptop"

    The template wraps it with the correct room/light/style — do NOT add long
    scene descriptions. The result should match the close-up, plant-background,
    analog-film look of the reference ads.
    """
    expression = _ANGLE_EXPRESSIONS.get(angle, _ANGLE_EXPRESSIONS["A"])
    return (
        f"a close-up environmental portrait of a {photo_subject}, "
        "face and upper body filling most of the frame, "
        "lush plant-filled home interior visible in background around subject, "
        "bookshelves, wall art, and potted plants behind them, "
        "warm natural window light, "
        f"85mm prime lens, {expression}, "
        f"{_IMAGEN_STYLE_SUFFIX}"
    )


# ── Gemini Imagen API call (via LiteLLM proxy) ────────────────────────────────

def _generate_imagen(prompt: str, gemini_api_key: str = "") -> Image.Image:
    """
    Generate an image via the Scale LiteLLM proxy using Gemini image models.

    Uses the OpenAI-compatible /images/generations endpoint.  The model is
    configured via config.GEMINI_IMAGE_MODEL (default: gemini/gemini-2.5-flash-image).
    Other confirmed working options: gemini/imagen-4.0-generate-001,
    gemini/imagen-4.0-fast-generate-001, gemini/gemini-3.1-flash-image-preview.

    Falls back to the direct Google API (GEMINI_API_KEY) if LiteLLM is unavailable.
    """
    # ── Primary: LiteLLM proxy ────────────────────────────────────────────────
    if config.LITELLM_API_KEY:
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
        log.warning("LiteLLM image generation failed (%d), falling back to direct API", resp.status_code)

    # ── Fallback: direct Google Gemini API ────────────────────────────────────
    api_key = gemini_api_key or config.GEMINI_API_KEY
    if not api_key:
        raise RuntimeError("No image generation available: LITELLM_API_KEY and GEMINI_API_KEY are both unset")

    url = (
        "https://generativelanguage.googleapis.com/v1beta/models"
        f"/gemini-2.5-flash-image:generateContent?key={api_key}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseModalities": ["IMAGE", "TEXT"]},
    }
    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(f"Gemini direct API error {resp.status_code}: {resp.text[:400]}")

    parts = (
        resp.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    for part in parts:
        if "inlineData" in part:
            return Image.open(BytesIO(base64.b64decode(part["inlineData"]["data"]))).convert("RGBA")

    raise RuntimeError("Gemini direct API returned no image in response")


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
    tl_color, bl_color = ANGLE_GRADIENTS.get(angle, ANGLE_GRADIENTS["A"])
    overlay = np.zeros((height, width, 4), dtype=np.float32)
    y_grid, x_grid = np.meshgrid(
        np.linspace(0, 1, height),
        np.linspace(0, 1, width),
        indexing="ij",
    )
    # Top-left cloud — spread ~50% across width (multiplier 1.2 vs 1.8 = wider)
    tl_dist  = np.sqrt(x_grid ** 2 + y_grid ** 2)
    tl_alpha = np.clip(1.0 - tl_dist * 1.2, 0, 1) ** 1.4 * 0.45
    # Bottom-left cloud — spread ~50% across width
    bl_dist  = np.sqrt(x_grid ** 2 + (1 - y_grid) ** 2)
    bl_alpha = np.clip(1.0 - bl_dist * 1.2, 0, 1) ** 1.4 * 0.40

    for c_idx, c_val in enumerate(tl_color):
        overlay[:, :, c_idx] += tl_alpha * (c_val / 255.0)
    for c_idx, c_val in enumerate(bl_color):
        overlay[:, :, c_idx] += bl_alpha * (c_val / 255.0)
    overlay[:, :, 3] = np.clip(np.maximum(tl_alpha, bl_alpha), 0, 1)
    return Image.fromarray((np.clip(overlay, 0, 1) * 255).astype(np.uint8), "RGBA")


def _load_font(size: int, bold: bool = False):
    """
    Load Avenir Next (matches Outlier brand style — rounded, clean sans-serif).
    Bold (index 0) for headlines, Regular (index 7) for subheadlines.
    Falls back to system fonts if Avenir is unavailable.
    """
    avenir = "/System/Library/Fonts/Avenir Next.ttc"
    try:
        idx = 0 if bold else 7  # 0=Bold, 7=Regular
        return ImageFont.truetype(avenir, size, index=idx)
    except (IOError, OSError):
        pass
    # Fallback chain
    candidates = (
        [
            "/Library/Fonts/Arial Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ] if bold else [
            "/Library/Fonts/Arial.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]
    )
    for path in candidates:
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
    - Right: "Outlier" wordmark in Avenir Next Bold, brown
    No shadow, clean Avenir type throughout.
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

    # "Outlier" wordmark — right-aligned, vertically centered
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


# ── Public entry point ────────────────────────────────────────────────────────

def generate_midjourney_creative(
    variant: dict,
    photo_subject: str | None = None,
    gemini_api_key: str | None = None,
    tg_category: str | None = None,  # kept for backward compat, unused
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

    prompt = _build_imagen_prompt(subject, angle)
    log.info("Generating Gemini creative | angle=%s", angle)
    log.debug("Imagen prompt: %s", prompt)

    bg_image = _generate_imagen(prompt, gemini_api_key or config.GEMINI_API_KEY)
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

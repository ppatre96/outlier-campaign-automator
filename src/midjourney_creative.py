"""
Gemini creative generation for Outlier LinkedIn ads.

Pipeline per TG + copy variant:
  1. Build a TG-aware prompt replicating Outlier's lifestyle-photo aesthetic
  2. Call Gemini 2.5 Flash Image (gemini-2.5-flash-image) to generate the background photo
  3. Composite the ad: gradient overlay + white headline/subheadline + optional bottom strip
  4. Return the final PNG path (ready for LinkedIn upload)

The visual style is derived from analysis of real Outlier Static Ads v2:
  - Full-bleed lifestyle photo (real person, cozy indoor, plants, natural light)
  - Soft gradient wash: pink/coral top-right corner, teal/blue bottom-left corner (~30% opacity)
  - Large white bold headline at top (≤8 words, 2 lines)
  - Smaller white regular subheadline lower on photo
  - Optional white strip at bottom: earnings claim (left) + Outlier wordmark (right)
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
# Angle A (Expertise): focused/professional — person is in their element
# Angle B (Earnings):  warm smile — happy, at ease, socially validated
# Angle C (Flexibility): genuine laugh — free, relaxed, unhurried
_ANGLE_EXPRESSIONS = {
    "A": "focused expression, slight furrowed brow, looking down at work, side profile",
    "B": "mouth slightly open mid-sentence, warm smile, looking off to the side, three-quarter angle",
    "C": "genuine laugh, eyes crinkled, looking directly at camera, frontal",
}

_IMAGEN_STYLE_SUFFIX = (
    "editorial lifestyle photography, Canon EOS 5D Mark IV, 85mm f/1.8, "
    "shallow depth of field, warm film grain, natural colors, "
    "photorealistic, NOT stock photo, NOT corporate headshot"
)


def _build_imagen_prompt(photo_subject: str, angle: str) -> str:
    """
    Build Gemini image prompt from the photo_subject derived by the brief generator.
    photo_subject is specific to the actual cohort TG — not a generic category.
    """
    expression = _ANGLE_EXPRESSIONS.get(angle, _ANGLE_EXPRESSIONS["A"])
    return (
        f"a simple environmental portrait of a {photo_subject}, "
        f"{expression}, "
        "the room is decorated with plants and nature details, "
        f"natural light, {_IMAGEN_STYLE_SUFFIX}"
    )


# ── Gemini Imagen API call ────────────────────────────────────────────────────

def _generate_imagen(prompt: str, gemini_api_key: str) -> Image.Image:
    """
    Call Gemini 2.5 Flash Image (gemini-2.5-flash-image) and return a PIL Image.
    Uses the generateContent endpoint with IMAGE response modality.
    """
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models"
        f"/gemini-2.5-flash-image:generateContent?key={gemini_api_key}"
    )
    payload = {
        "contents": [{"parts": [{"text": prompt}]}],
        "generationConfig": {"responseModalities": ["IMAGE", "TEXT"]},
    }

    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Gemini image API error {resp.status_code}: {resp.text[:400]}"
        )

    parts = (
        resp.json()
        .get("candidates", [{}])[0]
        .get("content", {})
        .get("parts", [])
    )
    for part in parts:
        if "inlineData" in part:
            b64 = part["inlineData"]["data"]
            return Image.open(BytesIO(base64.b64decode(b64))).convert("RGBA")

    raise RuntimeError("Gemini returned no image in response")


# ── Image composition ─────────────────────────────────────────────────────────

def _make_gradient_overlay(size: int, angle: str) -> Image.Image:
    tr_color, bl_color = ANGLE_GRADIENTS.get(angle, ANGLE_GRADIENTS["A"])
    overlay = np.zeros((size, size, 4), dtype=np.float32)
    y_grid, x_grid = np.meshgrid(
        np.linspace(0, 1, size),
        np.linspace(0, 1, size),
        indexing="ij",
    )
    tr_dist  = np.sqrt((1 - x_grid) ** 2 + y_grid ** 2)
    tr_alpha = np.clip(1.0 - tr_dist * 1.4, 0, 1) ** 1.5 * 0.55
    bl_dist  = np.sqrt(x_grid ** 2 + (1 - y_grid) ** 2)
    bl_alpha = np.clip(1.0 - bl_dist * 1.4, 0, 1) ** 1.5 * 0.55
    for c_idx, c_val in enumerate(tr_color):
        overlay[:, :, c_idx] += tr_alpha * (c_val / 255.0)
    for c_idx, c_val in enumerate(bl_color):
        overlay[:, :, c_idx] += bl_alpha * (c_val / 255.0)
    overlay[:, :, 3] = np.clip(np.maximum(tr_alpha, bl_alpha), 0, 1)
    return Image.fromarray((np.clip(overlay, 0, 1) * 255).astype(np.uint8), "RGBA")


def _load_font(size: int, bold: bool = False):
    candidates = (
        [
            "/System/Library/Fonts/HelveticaNeue.ttc",
            "/Library/Fonts/Arial Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ] if bold else [
            "/System/Library/Fonts/HelveticaNeue.ttc",
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


def _draw_text_centered(draw, lines, font, y_top, canvas_width, color, line_spacing=8, shadow=True):
    y = y_top
    for line in lines:
        w = draw.textlength(line, font=font)
        x = (canvas_width - w) // 2
        if shadow:
            draw.text((x + 3, y + 3), line, font=font, fill=(0, 0, 0, 90))
        draw.text((x, y), line, font=font, fill=color)
        bbox = draw.textbbox((x, y), line, font=font)
        y += (bbox[3] - bbox[1]) + line_spacing
    return y


def _add_bottom_strip(canvas: Image.Image, bottom_text: str, strip_height_frac: float = 0.13) -> Image.Image:
    size    = canvas.size[0]
    strip_h = int(size * strip_height_frac)
    strip_y = size - strip_h
    draw    = ImageDraw.Draw(canvas)
    draw.rectangle([0, strip_y, size, size], fill=(255, 255, 255, 255))
    pad = int(size * 0.04)
    body_font = _load_font(int(size * 0.026))
    draw.text((pad, strip_y + int(strip_h * 0.2)), bottom_text, font=body_font, fill=OUTLIER_BROWN)
    logo_font = _load_font(int(size * 0.052), bold=True)
    logo_text = "Outlier"
    logo_w    = draw.textlength(logo_text, font=logo_font)
    draw.text(
        (size - pad - logo_w, strip_y + int(strip_h * 0.15)),
        logo_text, font=logo_font, fill=OUTLIER_BROWN,
    )
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

    Layout:
      - Full-bleed photo background
      - Gradient overlay (corner washes)
      - White bold headline: top 10% of photo area, centered
      - White regular subheadline: 60% down the photo area, centered
      - White bottom strip (if with_bottom_strip): earnings + Outlier wordmark
    """
    size    = AD_SIZE
    canvas  = bg_image.convert("RGBA").resize((size, size), Image.LANCZOS)
    overlay = _make_gradient_overlay(size, angle)
    canvas  = Image.alpha_composite(canvas, overlay)

    strip_h    = int(size * 0.13) if with_bottom_strip else 0
    photo_h    = size - strip_h
    draw       = ImageDraw.Draw(canvas)
    max_text_w = int(size * 0.82)

    hl_font  = _load_font(int(size * 0.062), bold=True)
    hl_lines = _wrap_text(headline, hl_font, max_text_w)
    _draw_text_centered(draw, hl_lines, hl_font, int(photo_h * 0.10), size, (255, 255, 255, 255))

    sh_font  = _load_font(int(size * 0.038))
    sh_lines = _wrap_text(subheadline, sh_font, max_text_w)
    _draw_text_centered(draw, sh_lines, sh_font, int(photo_h * 0.60), size, (255, 255, 255, 230))

    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, size, size], radius=int(size * 0.025), fill=255)
    canvas.putalpha(mask)

    if with_bottom_strip and bottom_text:
        bg     = Image.new("RGBA", (size, size), (255, 255, 255, 255))
        canvas = Image.alpha_composite(bg, canvas)
        canvas = _add_bottom_strip(canvas, bottom_text)

    return canvas.convert("RGB")


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
    api_key     = gemini_api_key or config.GEMINI_API_KEY
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

    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set — add it to .env")

    prompt = _build_imagen_prompt(subject, angle)
    log.info("Generating Gemini creative | angle=%s", angle)
    log.debug("Imagen prompt: %s", prompt)

    bg_image = _generate_imagen(prompt, api_key)
    log.info("Imagen photo received (%dx%d)", bg_image.width, bg_image.height)

    earnings_match = re.search(
        r'\$[\d,]+(?:\.\d+)?(?:\s*USD)?(?:\s*(?:/hr|per hour|weekly|hourly))?',
        subheadline,
    )
    bottom_text = (
        f"Earn {earnings_match.group()} or more. Work from home."
        if earnings_match else ""
    )

    ad_image = compose_ad(
        bg_image=bg_image,
        headline=headline,
        subheadline=subheadline,
        angle=angle,
        bottom_text=bottom_text,
        with_bottom_strip=bool(bottom_text),
    )

    safe_label = re.sub(r"[^a-z0-9]", "_", subject[:30].lower())
    out_path = Path(tempfile.mktemp(suffix=f"_gemini_{safe_label}_{angle}.png"))
    ad_image.save(out_path, "PNG", optimize=True)
    log.info("Gemini creative saved: %s (%d bytes)", out_path, out_path.stat().st_size)
    return out_path

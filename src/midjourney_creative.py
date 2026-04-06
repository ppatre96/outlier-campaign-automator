"""
Gemini Imagen creative generation for Outlier LinkedIn ads.

Pipeline per TG + copy variant:
  1. Build a TG-aware prompt replicating Outlier's lifestyle-photo aesthetic
  2. Call Gemini Imagen (imagen-3.0-generate-002) to generate the background photo
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

# ── Imagen prompt templates per TG ────────────────────────────────────────────
_IMAGEN_SUBJECTS = {
    "ML_ENGINEER": (
        "young male software engineer in casual black t-shirt sitting at wooden desk "
        "with laptop, surrounded by lush green houseplants and filled bookshelves, "
        "cozy home office, diffused natural window light from left"
    ),
    "SOFTWARE_ENGINEER": (
        "young female developer in casual hoodie working on laptop, "
        "cozy bedroom desk setup with plants and framed art on walls, "
        "warm afternoon sunlight, relaxed smile"
    ),
    "MEDICAL": (
        "confident female doctor in light blue scrubs sitting in a sunlit clinic corner "
        "with potted plants and framed prints on wall, wearing stethoscope, "
        "relaxed warm expression, looking at camera"
    ),
    "DATA_ANALYST": (
        "young professional woman at desk with open laptop, colorful data visualizations "
        "on screen, plants and shelves visible behind, bright natural light, "
        "casual shirt, calm focused expression"
    ),
    "LANGUAGE": (
        "smiling young woman of South Asian descent wearing headphones around neck, "
        "cozy indoor setting with plants and warm artwork on walls, "
        "casual clothes, natural soft window light"
    ),
    "LEGAL": (
        "confident professional in smart-casual blazer sitting at a home library desk "
        "with legal books and laptop, warm bookshelves, natural window light, "
        "calm and focused expression"
    ),
    "GENERAL": (
        "young professional in casual clothes sitting in a cozy sunlit room with "
        "houseplants, wooden furniture, and art on walls, "
        "warm relaxed smile, looking at camera"
    ),
}

_ANGLE_MODIFIERS = {
    "A": "looking directly at camera with quiet confidence, professional energy",
    "B": "smiling warmly, genuinely happy, comfortable and at ease",
    "C": "relaxed, leaning slightly back, free and unhurried vibe",
}

_IMAGEN_STYLE_SUFFIX = (
    "editorial lifestyle photography, Canon EOS 5D Mark IV, 85mm f/1.8, "
    "shallow depth of field, warm film grain, natural colors, "
    "photorealistic, NOT stock photo, NOT corporate headshot"
)


def _build_imagen_prompt(tg_category: str, angle: str) -> str:
    subject  = _IMAGEN_SUBJECTS.get(tg_category, _IMAGEN_SUBJECTS["GENERAL"])
    modifier = _ANGLE_MODIFIERS.get(angle, _ANGLE_MODIFIERS["A"])
    return f"{subject}, {modifier}, {_IMAGEN_STYLE_SUFFIX}"


# ── Gemini Imagen API call ────────────────────────────────────────────────────

def _generate_imagen(prompt: str, gemini_api_key: str) -> Image.Image:
    """
    Call Gemini Imagen 3 (imagen-3.0-generate-002) and return a PIL Image.
    Uses the REST predict endpoint — no extra SDK needed beyond requests.
    """
    url = (
        "https://generativelanguage.googleapis.com/v1beta/models"
        f"/imagen-3.0-generate-002:predict?key={gemini_api_key}"
    )
    payload = {
        "instances": [{"prompt": prompt}],
        "parameters": {
            "sampleCount": 1,
            "aspectRatio": "1:1",
            "personGeneration": "allow_adult",
            "safetySetting": "block_only_high",
        },
    }

    resp = requests.post(url, json=payload, timeout=120)
    if resp.status_code != 200:
        raise RuntimeError(
            f"Imagen API error {resp.status_code}: {resp.text[:400]}"
        )

    data = resp.json()
    predictions = data.get("predictions", [])
    if not predictions:
        raise RuntimeError("Imagen returned no predictions")

    b64 = predictions[0].get("bytesBase64Encoded")
    if not b64:
        raise RuntimeError("Imagen prediction missing bytesBase64Encoded field")

    return Image.open(BytesIO(base64.b64decode(b64))).convert("RGBA")


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
    tg_category: str,
    variant: dict,
    gemini_api_key: str | None = None,
    **_kwargs,  # absorb legacy mj_token / claude_key / mcp_url args silently
) -> Path:
    """
    Full pipeline: Gemini Imagen background photo → composed ad PNG.

    Args:
        tg_category:    ML_ENGINEER / SOFTWARE_ENGINEER / MEDICAL /
                        DATA_ANALYST / LANGUAGE / LEGAL / GENERAL
        variant:        copy variant dict — keys: angle, headline, subheadline, cta
        gemini_api_key: Gemini API key (falls back to config.GEMINI_API_KEY)

    Returns:
        Path to the composed PNG (temp file — caller owns cleanup).
    """
    api_key     = gemini_api_key or config.GEMINI_API_KEY
    angle       = variant.get("angle", "A")
    headline    = variant.get("headline", "")
    subheadline = variant.get("subheadline", "")

    if not api_key:
        raise RuntimeError("GEMINI_API_KEY is not set — add it to .env")

    prompt = _build_imagen_prompt(tg_category, angle)
    log.info("Generating Imagen creative | TG=%s angle=%s", tg_category, angle)
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

    out_path = Path(tempfile.mktemp(suffix=f"_gemini_{tg_category.lower()}_{angle}.png"))
    ad_image.save(out_path, "PNG", optimize=True)
    log.info("Gemini creative saved: %s (%d bytes)", out_path, out_path.stat().st_size)
    return out_path

"""
Midjourney creative generation for Outlier LinkedIn ads.

Pipeline per TG + copy variant:
  1. Build a TG-aware Midjourney prompt that replicates Outlier's lifestyle-photo aesthetic
  2. Call Midjourney via MCP (Claude API + betas mcp-client) to generate the photo
  3. Download the generated image
  4. Composite the ad: gradient overlay + white headline/subheadline + optional bottom strip
  5. Return the final PNG path (ready for LinkedIn upload)

The visual style is derived from analysis of real Outlier Static Ads v2:
  - Full-bleed lifestyle photo (real person, cozy indoor, plants, natural light)
  - Soft gradient wash: pink/coral top-right corner, teal/blue bottom-left corner (~30% opacity)
  - Large white bold headline at top (≤8 words, 2 lines)
  - Smaller white regular subheadline lower on photo
  - Optional white strip at bottom: earnings claim (left) + Outlier wordmark (right)
"""
import json
import logging
import re
import tempfile
from pathlib import Path
from io import BytesIO

import anthropic
import requests
from PIL import Image, ImageDraw, ImageFont
import numpy as np

import config

log = logging.getLogger(__name__)

# ── Output size ───────────────────────────────────────────────────────────────
AD_SIZE = 1200   # 1200×1200 px for high-res LinkedIn 1:1

# ── Outlier brand colors ───────────────────────────────────────────────────────
# Observed from real ads:
OUTLIER_BROWN   = (61, 26, 0)      # Outlier wordmark
GRADIENT_PINK   = (255, 182, 193)  # top-right corner wash (Angle A/Expertise)
GRADIENT_BLUE   = (173, 216, 230)  # bottom-left corner wash
GRADIENT_ORANGE = (255, 200, 150)  # top-right variant for Earnings (B)
GRADIENT_GREEN  = (180, 230, 200)  # bottom-left variant for Flexibility (C)

# Per-angle gradient colors: (top_right_rgb, bottom_left_rgb)
ANGLE_GRADIENTS = {
    "A": (GRADIENT_PINK,   GRADIENT_BLUE),    # Expertise — pink/blue
    "B": (GRADIENT_ORANGE, GRADIENT_BLUE),    # Earnings  — warm orange/blue
    "C": (GRADIENT_PINK,   GRADIENT_GREEN),   # Flexibility — pink/green
}

# ── Midjourney prompt templates per TG ────────────────────────────────────────
# Based on Outlier Static Ads v2 visual analysis:
# - Real people (not illustrations) in lifestyle settings
# - Indoor environments: cozy home office, living room, clinical setting
# - Plants, bookshelves, art on walls, warm natural light
# - Film-like color grading, NOT corporate stock photo
# - Person faces camera, relaxed confident expression, casual-professional

_MJ_SUBJECTS = {
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
    "GENERAL": (
        "young professional in casual clothes sitting in a cozy sunlit room with "
        "houseplants, wooden furniture, and art on walls, "
        "warm relaxed smile, looking at camera"
    ),
}

# Per-angle subject modifier (subtle mood shift)
_ANGLE_MODIFIERS = {
    "A": "looking directly at camera with quiet confidence, professional energy",
    "B": "smiling warmly, genuinely happy, comfortable and at ease",
    "C": "relaxed, leaning slightly back, free and unhurried vibe",
}

_MJ_STYLE_SUFFIX = (
    "editorial lifestyle photography, Canon EOS 5D Mark IV, 85mm f/1.8, "
    "shallow depth of field, warm film grain, natural colors, "
    "NOT stock photo, NOT corporate, NOT AI generated looking "
    "--ar 1:1 --style raw --v 6.1 --q 2"
)


def _build_mj_prompt(tg_category: str, angle: str) -> str:
    """Build a Midjourney prompt for the given TG category and copy angle."""
    subject  = _MJ_SUBJECTS.get(tg_category, _MJ_SUBJECTS["GENERAL"])
    modifier = _ANGLE_MODIFIERS.get(angle, _ANGLE_MODIFIERS["A"])
    return f"{subject}, {modifier}, {_MJ_STYLE_SUFFIX}"


# ── MCP call ──────────────────────────────────────────────────────────────────

def _call_midjourney_mcp(
    prompt: str,
    mj_token: str,
    claude_key: str,
    mcp_url: str | None = None,
) -> str:
    """
    Call the Midjourney MCP via Claude API betas mcp-client.
    Returns the generated image URL.
    """
    mcp_url = mcp_url or config.MIDJOURNEY_MCP_URL

    instruction = (
        "Use the Midjourney tool to generate an image with the following prompt. "
        "Return ONLY the resulting image URL in your response, nothing else.\n\n"
        f"Prompt: {prompt}"
    )

    client = anthropic.Anthropic(api_key=claude_key)
    resp = client.beta.messages.create(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        betas=["mcp-client-2025-04-04"],
        mcp_servers=[{
            "type": "url",
            "url": mcp_url,
            "name": "midjourney",
            "headers": {"Authorization": f"Bearer {mj_token}"},
        }],
        messages=[{"role": "user", "content": instruction}],
    )

    image_url = _parse_image_url(resp)
    if not image_url:
        raise RuntimeError("Midjourney MCP returned no image URL")
    log.info("Midjourney generated: %s", image_url[:80])
    return image_url


def _parse_image_url(resp) -> str | None:
    """Extract image URL from Claude MCP response."""
    for block in resp.content:
        block_dict = block if isinstance(block, dict) else (
            block.model_dump() if hasattr(block, "model_dump") else {}
        )
        text = ""
        if block_dict.get("type") == "text":
            text = block_dict.get("text", "")
        elif block_dict.get("type") == "tool_result":
            content = block_dict.get("content", "")
            text = content if isinstance(content, str) else json.dumps(content)

        if not text:
            continue

        # Try JSON with url/imageUrl/image_url
        try:
            data = json.loads(re.search(r'\{[\s\S]+\}', text).group())
            for key in ("url", "imageUrl", "image_url", "imageURL", "uri"):
                if data.get(key):
                    return data[key]
        except Exception:
            pass

        # Fallback: find a bare HTTPS URL to an image
        match = re.search(r'https?://\S+\.(?:png|jpg|jpeg|webp)(?:\?\S*)?', text, re.IGNORECASE)
        if match:
            return match.group()

        # Fallback: any https URL (CDN might not have extension)
        match = re.search(r'https?://\S{20,}', text)
        if match:
            return match.group().rstrip(".,)")

    return None


# ── Image composition ─────────────────────────────────────────────────────────

def _download_image(url: str) -> Image.Image:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    img = Image.open(BytesIO(resp.content)).convert("RGBA")
    return img.resize((AD_SIZE, AD_SIZE), Image.LANCZOS)


def _make_gradient_overlay(size: int, angle: str) -> Image.Image:
    """
    Create the soft corner gradient overlay matching Outlier's visual style.
    Top-right: warm color wash. Bottom-left: cool color wash.
    Center is transparent so the photo shows through.
    """
    tr_color, bl_color = ANGLE_GRADIENTS.get(angle, ANGLE_GRADIENTS["A"])

    overlay = np.zeros((size, size, 4), dtype=np.float32)

    # Normalized coordinate grids
    y_grid, x_grid = np.meshgrid(
        np.linspace(0, 1, size),
        np.linspace(0, 1, size),
        indexing="ij",
    )

    # Top-right influence: distance from (0, 1) corner in normalized coords
    # Strong near top-right, fades toward center/bottom-left
    tr_dist = np.sqrt((1 - x_grid) ** 2 + y_grid ** 2)  # dist from top-right
    tr_alpha = np.clip(1.0 - tr_dist * 1.4, 0, 1) ** 1.5 * 0.55  # max ~55% alpha

    # Bottom-left influence: distance from (1, 0) corner
    bl_dist = np.sqrt(x_grid ** 2 + (1 - y_grid) ** 2)  # dist from bottom-left
    bl_alpha = np.clip(1.0 - bl_dist * 1.4, 0, 1) ** 1.5 * 0.55

    for c_idx, c_val in enumerate(tr_color):
        overlay[:, :, c_idx] += tr_alpha * (c_val / 255.0)
    for c_idx, c_val in enumerate(bl_color):
        overlay[:, :, c_idx] += bl_alpha * (c_val / 255.0)

    # Alpha channel = max of both influences (no double-counting)
    overlay[:, :, 3] = np.clip(np.maximum(tr_alpha, bl_alpha), 0, 1)

    overlay_uint8 = (np.clip(overlay, 0, 1) * 255).astype(np.uint8)
    return Image.fromarray(overlay_uint8, "RGBA")


def _load_font(size: int, bold: bool = False) -> ImageFont.FreeTypeFont | ImageFont.ImageFont:
    """Try to load a clean sans-serif font; fall back to PIL default."""
    candidates = []
    if bold:
        candidates = [
            "/System/Library/Fonts/HelveticaNeue.ttc",          # macOS
            "/Library/Fonts/Arial Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
        ]
    else:
        candidates = [
            "/System/Library/Fonts/HelveticaNeue.ttc",
            "/Library/Fonts/Arial.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
        ]

    for path in candidates:
        try:
            return ImageFont.truetype(path, size)
        except (IOError, OSError):
            continue

    # Last resort: PIL built-in bitmap font (no size control)
    try:
        return ImageFont.load_default(size=size)
    except TypeError:
        return ImageFont.load_default()


def _wrap_text(text: str, font: ImageFont.FreeTypeFont, max_width: int) -> list[str]:
    """Word-wrap text to fit within max_width pixels."""
    words  = text.replace("\n", " \n ").split(" ")
    lines  = []
    cur    = ""
    dummy  = Image.new("RGB", (1, 1))
    draw   = ImageDraw.Draw(dummy)

    for word in words:
        if word == "\n":
            lines.append(cur.strip())
            cur = ""
            continue
        test = (cur + " " + word).strip()
        w    = draw.textlength(test, font=font)
        if w <= max_width:
            cur = test
        else:
            if cur:
                lines.append(cur.strip())
            cur = word

    if cur.strip():
        lines.append(cur.strip())
    return lines


def _draw_text_centered(
    draw: ImageDraw.ImageDraw,
    lines: list[str],
    font,
    y_top: int,
    canvas_width: int,
    color: tuple,
    line_spacing: int = 8,
    shadow: bool = True,
) -> int:
    """Draw centered multi-line text with optional drop shadow. Returns bottom y."""
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


def _add_bottom_strip(
    canvas: Image.Image,
    bottom_text: str,
    strip_height_frac: float = 0.13,
) -> Image.Image:
    """
    Add the white bottom strip with left-aligned body text and right-aligned
    Outlier wordmark — matching the real Outlier ad layout.
    """
    size        = canvas.size[0]  # square
    strip_h     = int(size * strip_height_frac)
    strip_y     = size - strip_h

    # White strip
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([0, strip_y, size, size], fill=(255, 255, 255, 255))

    pad = int(size * 0.04)

    # Body text (left-aligned, small)
    body_font = _load_font(int(size * 0.026))
    draw.text((pad, strip_y + int(strip_h * 0.2)), bottom_text, font=body_font, fill=OUTLIER_BROWN)

    # "Outlier" wordmark (right-aligned, larger, bold)
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
    Compose the final Outlier ad creative from a background photo.

    Layout (matching real Outlier Static Ads v2):
      - Full-bleed photo background
      - Gradient overlay (corner washes)
      - White bold headline: top 12% from top, centered
      - White regular subheadline: ~58% from top, centered
      - White bottom strip (if with_bottom_strip): earnings + Outlier logo
    """
    size   = AD_SIZE
    canvas = bg_image.convert("RGBA").resize((size, size), Image.LANCZOS)

    # Gradient overlay
    overlay = _make_gradient_overlay(size, angle)
    canvas  = Image.alpha_composite(canvas, overlay)

    # Photo area height (above bottom strip)
    strip_h     = int(size * 0.13) if with_bottom_strip else 0
    photo_h     = size - strip_h
    draw        = ImageDraw.Draw(canvas)

    # ── Headline ──
    hl_font_size = int(size * 0.062)   # ~74px at 1200px
    hl_font      = _load_font(hl_font_size, bold=True)
    max_text_w   = int(size * 0.82)
    hl_lines     = _wrap_text(headline, hl_font, max_text_w)
    hl_y         = int(photo_h * 0.10)  # 10% from top of photo area
    _draw_text_centered(draw, hl_lines, hl_font, hl_y, size, (255, 255, 255, 255))

    # ── Subheadline ──
    sh_font_size = int(size * 0.038)   # ~46px at 1200px
    sh_font      = _load_font(sh_font_size, bold=False)
    sh_lines     = _wrap_text(subheadline, sh_font, max_text_w)
    sh_y         = int(photo_h * 0.60)  # 60% down the photo area
    _draw_text_centered(draw, sh_lines, sh_font, sh_y, size, (255, 255, 255, 230))

    # ── Rounded corner clip ──
    mask = Image.new("L", (size, size), 0)
    ImageDraw.Draw(mask).rounded_rectangle([0, 0, size, size], radius=int(size * 0.025), fill=255)
    canvas.putalpha(mask)

    # ── Bottom strip ──
    if with_bottom_strip and bottom_text:
        # Re-flatten to RGB for the strip (strip is fully opaque)
        bg = Image.new("RGBA", (size, size), (255, 255, 255, 255))
        canvas = Image.alpha_composite(bg, canvas)
        canvas = _add_bottom_strip(canvas, bottom_text)

    return canvas.convert("RGB")


# ── Public entry point ─────────────────────────────────────────────────────────

def generate_midjourney_creative(
    tg_category: str,
    variant: dict,
    mj_token: str,
    claude_key: str,
    mcp_url: str | None = None,
) -> Path:
    """
    Full pipeline: Midjourney photo → composed ad PNG.

    Args:
        tg_category: one of DATA_ANALYST / ML_ENGINEER / MEDICAL / LANGUAGE /
                     SOFTWARE_ENGINEER / GENERAL
        variant:     copy variant dict from build_copy_variants()
                     (keys: angle, headline, subheadline, cta)
        mj_token:    Midjourney API token (MIDJOURNEY_API_TOKEN env var)
        claude_key:  Anthropic API key
        mcp_url:     override MIDJOURNEY_MCP_URL from config

    Returns:
        Path to the composed PNG file (temp file, caller owns cleanup).
    """
    angle       = variant.get("angle", "A")
    headline    = variant.get("headline", "")
    subheadline = variant.get("subheadline", "")

    # Build and call Midjourney
    mj_prompt = _build_mj_prompt(tg_category, angle)
    log.info("Generating MJ creative | TG=%s angle=%s", tg_category, angle)
    log.debug("MJ prompt: %s", mj_prompt)

    image_url = _call_midjourney_mcp(mj_prompt, mj_token, claude_key, mcp_url)

    # Download
    bg_image = _download_image(image_url)
    log.info("Downloaded MJ image (%dx%d)", bg_image.width, bg_image.height)

    # Build bottom strip text using variant info (no invented numbers)
    # If subheadline contains an earnings figure, use it; else leave strip empty
    earnings_match = re.search(r'\$[\d,]+(?:\.\d+)?(?:\s*USD)?(?:\s*(?:/hr|per hour|weekly|hourly))?', subheadline)
    bottom_text    = f"Earn {earnings_match.group()} or more. Work from home." if earnings_match else ""

    # Compose
    ad_image = compose_ad(
        bg_image=bg_image,
        headline=headline,
        subheadline=subheadline,
        angle=angle,
        bottom_text=bottom_text,
        with_bottom_strip=bool(bottom_text),
    )

    # Save to temp PNG
    out_path = Path(tempfile.mktemp(
        suffix=f"_mj_{tg_category.lower()}_{angle}.png"
    ))
    ad_image.save(out_path, "PNG", optimize=True)
    log.info("MJ creative saved: %s (%d bytes)", out_path, out_path.stat().st_size)
    return out_path

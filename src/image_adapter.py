"""
Platform image adapter — produces ad creative images at platform-correct
aspect ratios from a single source photo.

LinkedIn ads are 1:1 (1200×1200). Meta feed ads are 1:1 (1200×1200). Google
Display Banner ads are 1.91:1 (1200×628). Generating fresh Gemini photos for
every aspect would triple our image-gen cost; instead we generate ONE square
photo per (cohort × geo × angle) and crop/composite it into each platform's
required aspect inside this module.

For v1 (single big-bang PR) each platform gets its dominant aspect:
  - linkedin → 1:1
  - meta     → 1:1   (most common for feed ads; v2 adds 4:5 + 1.91:1 stories)
  - google   → 1.91:1 (Display Banner; v2 adds 1:1 square)

`compose_ad_for_platform()` is the single entry point. It:
  1. Center-crops the source photo to the target aspect.
  2. Reuses `gemini_creative.detect_subject_bbox()` to find the hairline.
  3. Composites the headline above the hairline + brand mark.
  4. Returns a tempfile Path the caller hands to `client.upload_image()`.

The LinkedIn arm continues using the existing rich `compose_ad()` (with
gradient, subheadline, bottom strip, earnings text). Meta/Google use a
simpler version since those platforms surface primary text + descriptions
as separate fields under the image — duplicating them on the image is
visually noisy.
"""
from __future__ import annotations

import logging
import tempfile
from pathlib import Path
from typing import Optional

from PIL import Image, ImageDraw

from src.ad_platform import get_constraints
from src.gemini_creative import (
    AD_SIZE,
    _draw_text_left,
    _load_font,
    _wrap_text,
    compose_ad as _compose_linkedin_ad,
    detect_subject_bbox,
)

log = logging.getLogger(__name__)


# Default aspect per platform (the one we actually render in v1). Stored as
# (width_units, height_units) integer ratios — float-safe.
_PRIMARY_ASPECT: dict[str, tuple[int, int]] = {
    "linkedin": (1, 1),
    "meta":     (1, 1),
    "google":   (191, 100),  # 1.91:1
}

# Pixel dimensions for each aspect — sized for sharpness on the platform's
# largest surface but small enough to keep upload time reasonable.
_PIXEL_DIMS: dict[tuple[int, int], tuple[int, int]] = {
    (1, 1):     (1200, 1200),
    (4, 5):     (1080, 1350),
    (191, 100): (1200, 628),
}


def primary_aspect(platform: str) -> tuple[int, int]:
    """Return the (w_ratio, h_ratio) v1 default aspect for the platform."""
    return _PRIMARY_ASPECT.get(platform, (1, 1))


def compose_ad_for_platform(
    bg_image: Image.Image,
    copy_variant: dict,
    platform: str,
    angle: str = "A",
    bottom_text: str = "",
    save_to: Optional[Path] = None,
) -> Path:
    """Render a final ad creative for the given platform from a Gemini photo.

    Args:
        bg_image: Source photo (typically 1024×1024 from Gemini).
        copy_variant: Platform-shaped copy dict from `copy_adapter`.
            For linkedin: must have headline + subheadline.
            For meta: must have headline (+ optional primary_text/description).
            For google: must have headlines (list) — the first short headline
                is rendered on the image; the rest are surfaced as separate
                ad fields by Google's RDA optimizer.
        platform: "linkedin" | "meta" | "google".
        angle: A/B/C — used for gradient direction in linkedin's compose_ad.
        bottom_text: Earnings strip text (linkedin only — ignored for others).
        save_to: Optional explicit output path; if None, a NamedTemporaryFile
            is allocated.

    Returns:
        Path to the rendered PNG on disk.
    """
    if platform == "linkedin":
        # Use the rich existing compositor — full headline + subhead + bottom
        # strip + gradient. Zero behavior change for the LinkedIn arm.
        out = _compose_linkedin_ad(
            bg_image=bg_image,
            headline=copy_variant.get("headline", ""),
            subheadline=copy_variant.get("subheadline", ""),
            angle=angle,
            bottom_text=bottom_text,
            with_bottom_strip=True,
        )
        return _save(out, save_to, suffix=f"_linkedin_{angle}")

    aspect = primary_aspect(platform)
    headline = _platform_headline(copy_variant, platform)
    out = _compose_simple_image_ad(bg_image, headline, aspect)
    return _save(out, save_to, suffix=f"_{platform}_{angle}")


def _platform_headline(copy_variant: dict, platform: str) -> str:
    """Pull the right headline field out of the platform-shaped copy dict."""
    if platform == "google":
        # Google RDA gives us a list — render the first short headline on the
        # image; the optimizer surfaces the rest as text fields.
        headlines = copy_variant.get("headlines") or []
        if headlines:
            return headlines[0]
        return copy_variant.get("long_headline", "") or copy_variant.get("headline", "")
    return copy_variant.get("headline", "")


def _compose_simple_image_ad(
    bg_image: Image.Image,
    headline: str,
    aspect: tuple[int, int],
) -> Image.Image:
    """Render a minimal image ad: cropped photo with the headline rendered on
    top and a small white border for legibility. Used for Meta + Google (which
    surface body copy as separate fields, so the image stays clean).

    Layout:
      - Outer canvas matches `_PIXEL_DIMS[aspect]`.
      - Photo fills the canvas.
      - Headline is rendered in white above the detected hairline (or at
        12% from the top if detection fails) — same algorithm as
        `compose_ad()` but with no subheadline / earnings strip.
    """
    canvas_w, canvas_h = _PIXEL_DIMS.get(aspect, (1200, 1200))

    # Center-crop bg_image to the target aspect, then resize.
    target_ratio = canvas_w / canvas_h
    src_w, src_h = bg_image.size
    src_ratio = src_w / src_h
    if src_ratio > target_ratio:
        new_w = int(src_h * target_ratio)
        x_off = (src_w - new_w) // 2
        bg_image = bg_image.crop((x_off, 0, x_off + new_w, src_h))
    elif src_ratio < target_ratio:
        new_h = int(src_w / target_ratio)
        y_off = (src_h - new_h) // 2
        bg_image = bg_image.crop((0, y_off, src_w, y_off + new_h))

    photo = bg_image.convert("RGBA").resize((canvas_w, canvas_h), Image.LANCZOS)

    # Subject-bbox detection for vertical placement (always at the top).
    bbox = detect_subject_bbox(photo)
    if bbox:
        hair_top = int(bbox["hair_top_y"])
        hl_bottom = max(int(canvas_h * 0.05), hair_top - int(canvas_h * 0.04))
    else:
        hl_bottom = int(canvas_h * 0.18)

    draw       = ImageDraw.Draw(photo)
    max_text_w = int(canvas_w * 0.88)

    # Font: scale to canvas height (smaller dimension if landscape).
    base = min(canvas_w, canvas_h)
    hl_size = int(base * 0.075)
    hl_min  = int(base * 0.050)
    hl_font  = _load_font(hl_size, bold=True)
    hl_lines = _wrap_text(headline, hl_font, max_text_w)
    while len(hl_lines) > 2 and hl_size > hl_min:
        hl_size -= max(2, int(base * 0.004))
        hl_font  = _load_font(hl_size, bold=True)
        hl_lines = _wrap_text(headline, hl_font, max_text_w)

    LINE_SPACING = 10
    hl_height    = hl_size * len(hl_lines) + LINE_SPACING * (len(hl_lines) - 1)
    hl_top       = max(int(canvas_h * 0.04), hl_bottom - hl_height)

    # Subtle dark scrim under the headline so white text always reads.
    scrim_pad_y = int(canvas_h * 0.025)
    scrim = Image.new("RGBA", (canvas_w, hl_height + 2 * scrim_pad_y), (0, 0, 0, 90))
    photo.alpha_composite(scrim, (0, max(0, hl_top - scrim_pad_y)))

    _draw_text_left(
        draw, hl_lines, hl_font, hl_top,
        0, (255, 255, 255, 255),
        line_spacing=LINE_SPACING, canvas_width=canvas_w,
    )

    return photo.convert("RGB")


def _save(img: Image.Image, save_to: Optional[Path], suffix: str) -> Path:
    if save_to is None:
        f = tempfile.NamedTemporaryFile(suffix=f"{suffix}.png", delete=False)
        save_to = Path(f.name)
        f.close()
    save_to = Path(save_to)
    save_to.parent.mkdir(parents=True, exist_ok=True)
    img.save(save_to, "PNG")
    log.info("image_adapter saved %s (%dx%d, %d bytes)",
             save_to.name, img.width, img.height, save_to.stat().st_size)
    return save_to

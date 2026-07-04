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
    OUTLIER_BROWN,
    _draw_text_left,
    _load_font,
    _rasterize_outlier_logo,
    _wrap_text,
    compose_ad as _compose_linkedin_ad,
    derive_bottom_text as _derive_bottom_text,
    detect_subject_bbox,
)

log = logging.getLogger(__name__)


# Default aspect per platform (the one we actually render in v1). Stored as
# (width_units, height_units) integer ratios — float-safe.
#
# 2026-05-20: meta default flipped from 1:1 → 4:5. Per Meta Help Center +
# 2025/2026 ad-performance benchmarks, 4:5 is the highest-converting static
# Feed ratio on both FB and IG (~33% more vertical screen than 1:1). The live
# pipeline now uploads 1080×1350 creatives to Meta for every ramp. fb/ig
# aliases stay defined so the secondary-creative script can write to those
# slugs in Drive when a separate folder split is desired; the LIVE Meta arm
# uses the canonical "meta" key.
#
# tiktok was added 2026-05-20 for GMR-0021 secondary-creative gen (Drive-only,
# no API integration — see scripts/generate_secondary_creatives.py).
_PRIMARY_ASPECT: dict[str, tuple[int, int]] = {
    "linkedin": (1, 1),
    "meta":     (4, 5),      # FB + IG Feed 1080×1350 — see header comment
    "google":   (191, 100),  # 1.91:1
    "fb":       (4, 5),      # FB Feed 1080×1350 (Drive-folder alias of meta)
    "ig":       (4, 5),      # IG Feed 1080×1350 (Drive-folder alias of meta)
    "tiktok":   (9, 16),     # TikTok in-feed / Carousel slide 1080×1920
    "reddit":   (1, 1),      # Reddit feed promoted image 1200×1200 (new channel 2026-06-11)
}

# Pixel dimensions for each aspect — sized for sharpness on the platform's
# largest surface but small enough to keep upload time reasonable.
_PIXEL_DIMS: dict[tuple[int, int], tuple[int, int]] = {
    (1, 1):     (1200, 1200),
    (4, 5):     (1080, 1350),
    (9, 16):    (1080, 1920),  # TikTok / IG Reels / FB Reels / Stories
    (191, 100): (1200, 628),
}


def primary_aspect(platform: str) -> tuple[int, int]:
    """Return the (w_ratio, h_ratio) v1 default aspect for the platform."""
    return _PRIMARY_ASPECT.get(platform, (1, 1))


def pixel_dims_for_aspect(aspect: tuple[int, int]) -> tuple[int, int]:
    """Return (canvas_w, canvas_h) for the given aspect ratio."""
    return _PIXEL_DIMS.get(aspect, (1200, 1200))


def assert_min_dimensions(image_path, min_px: int, *, platform: str = "") -> None:
    """Raise ValueError if the image's short side is below ``min_px``.

    Guards every platform's ``upload_image`` against shipping a thumbnail-
    resolution creative (GMR-0023 2026-06-09: native-language B/C variants
    uploaded at 64×64 and rendered pixelated). Real pipeline creatives are
    ≥1080 on every side, so a sub-``min_px`` image is a thumbnail. The raise is
    caught by the launch arm's verify-and-heal so the reason surfaces on the
    console + Slack instead of a pixelated ad going live.

    Best-effort dimension read: if PIL can't open the file we return quietly and
    let the platform API surface the real error (don't block on a read quirk).
    """
    try:
        with Image.open(image_path) as im:
            w, h = im.size
    except Exception as exc:
        log.warning("assert_min_dimensions: could not read %s (%s) — skipping check", image_path, exc)
        return
    if min(w, h) < min_px:
        raise ValueError(
            f"Creative {Path(str(image_path)).name} is {w}x{h}px — below the {min_px}px "
            f"minimum{f' for {platform}' if platform else ''}; refusing to upload a "
            f"thumbnail-resolution image (would render pixelated)."
        )


def compose_ad_for_platform(
    bg_image: Image.Image,
    copy_variant: dict,
    platform: str,
    angle: str = "A",
    bottom_text: str = "",
    save_to: Optional[Path] = None,
    aspect: Optional[tuple[int, int]] = None,
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
        aspect: Optional override of the platform's primary aspect. Lets a caller
            request meta at 4:5 (or any platform at a non-default ratio) without
            changing the platform's registered default. Falls back to
            `primary_aspect(platform)` when None.

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

    if aspect is None:
        aspect = primary_aspect(platform)
    # All non-LinkedIn channels now use the SAME bordered template as LinkedIn
    # (Pranav 2026-06-18) — border + inset photo + headline + subheadline + a
    # white bottom band whose descriptive line wraps before the logo, so the
    # logo never overlaps copy. Rendered at the platform's aspect. The bottom
    # line is derived the same way the LinkedIn arm derives it.
    headline = _platform_headline(copy_variant, platform)
    subheadline = _platform_subheadline(copy_variant, platform)
    bt = bottom_text or _derive_bottom_text(subheadline, copy_variant.get("advertised_rate", ""))
    out = _compose_linkedin_ad(
        bg_image=bg_image,
        headline=headline,
        subheadline=subheadline,
        angle=angle,
        bottom_text=bt,
        with_bottom_strip=True,
        aspect=aspect,
    )
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


def _platform_subheadline(copy_variant: dict, platform: str) -> str:
    """Pull the supporting line to render UNDER the headline on the image.

    The simple compositor previously dropped this — fine for Meta/Google where
    body copy is a separate ad field, but the secondary creatives (esp. TikTok)
    go to Drive for manual upload where the PNG is the whole ad, so a lone
    headline reads as a vague one-liner. We now render a subheadline on-image
    for every simple-compositor channel. Field name varies by copy source:
    figma_creative.build_copy_variants → 'subheadline'; the platform copy
    adapters → 'description' / 'primary_text'. First non-empty wins."""
    if platform == "google":
        headlines = copy_variant.get("headlines") or []
        return (
            copy_variant.get("description", "")
            or (headlines[1] if len(headlines) > 1 else "")
            or copy_variant.get("long_headline", "")
        )
    return (
        copy_variant.get("subheadline", "")
        or copy_variant.get("description", "")
        or copy_variant.get("primary_text", "")
    )


# FIXED Outlier watermark placement per aspect ratio. Explicit pixels (NOT
# percentages, NOT platform-keyed) so the wordmark lands at the SAME bottom-right
# spot on every render of a given aspect — copy and photo never move it.
#   width_px      = rendered wordmark width
#   pad_right_px  = gap from the canvas RIGHT edge
#   pad_bottom_px = gap from the canvas BOTTOM edge
# 9:16 uses a larger bottom pad so the logo clears TikTok's caption / CTA UI
# strip (which overlays the lower portion of an in-feed post). Adjust these
# numbers to move the logo; nothing else in the compositor affects placement.
_WATERMARK_SPEC: dict[tuple[int, int], dict[str, int]] = {
    (9, 16):    {"width_px": 200, "pad_right_px": 48, "pad_bottom_px": 290},
    (1, 1):     {"width_px": 200, "pad_right_px": 44, "pad_bottom_px": 44},
    (4, 5):     {"width_px": 200, "pad_right_px": 44, "pad_bottom_px": 44},
    (191, 100): {"width_px": 150, "pad_right_px": 32, "pad_bottom_px": 24},
}
_WATERMARK_DEFAULT = {"width_px": 200, "pad_right_px": 44, "pad_bottom_px": 44}


def _watermark_placement(canvas_w: int, canvas_h: int, aspect: tuple[int, int]):
    """Return (logo_img, x, y) for the fixed Outlier watermark, or (None, 0, 0)
    if the logo can't be rasterized. Placement is a pure function of `aspect`
    (via _WATERMARK_SPEC) — identical for every render regardless of copy/photo.
    The subheadline compositor reads this so it can sit above the logo box."""
    spec = _WATERMARK_SPEC.get(aspect, _WATERMARK_DEFAULT)
    # White wordmark — photo bottom-right corners are typically mid/dark
    # (warm interiors, window light), so white reads more legibly than brown.
    logo_img = _rasterize_outlier_logo(target_width=spec["width_px"], color=(255, 255, 255))
    if logo_img is None:
        return None, 0, 0
    # Fade to ~70% opacity so it reads as a watermark, not a hard logo.
    if logo_img.mode != "RGBA":
        logo_img = logo_img.convert("RGBA")
    r, g, b, a = logo_img.split()
    a = a.point(lambda p: int(p * 0.70))
    logo_img = Image.merge("RGBA", (r, g, b, a))
    x = canvas_w - logo_img.width - spec["pad_right_px"]
    y = canvas_h - logo_img.height - spec["pad_bottom_px"]
    return logo_img, x, y


def _add_outlier_watermark(canvas: Image.Image, aspect: tuple[int, int]) -> Image.Image:
    """Paste the Outlier wordmark at its FIXED per-aspect bottom-right position
    (see _WATERMARK_SPEC). Bottom-right keeps it clear of the upper-middle photo
    subject; the placement does not depend on copy, photo, or platform."""
    logo_img, x, y = _watermark_placement(canvas.size[0], canvas.size[1], aspect)
    if logo_img is None:
        return canvas
    canvas.paste(logo_img, (x, y), logo_img)
    return canvas


def _compose_simple_image_ad(
    bg_image: Image.Image,
    headline: str,
    aspect: tuple[int, int],
    platform: str = "",
    subheadline: str = "",
) -> Image.Image:
    """Render an Outlier-branded image ad: full-canvas photo + headline overlay
    + a small Outlier watermark in the top-right corner. Used for Meta + Google
    + FB + IG + TikTok (platforms that surface body copy as separate fields,
    so we don't duplicate the description on the image but DO show subtle
    brand identity).

    Layout:
      - Outer canvas matches `_PIXEL_DIMS[aspect]`. Photo fills the entire
        canvas (no cropped-off bottom band).
      - Headline is rendered in white above the detected hairline (or at
        12% from the top if detection fails). Respects the top safe-zone
        for tiktok (~140px reserved for the username pill).
      - Outlier watermark (small, low-opacity, top-right corner) is overlaid
        last, below the headline scrim band — see `_add_outlier_watermark`
        for placement rationale.
    """
    canvas_w, canvas_h = _PIXEL_DIMS.get(aspect, (1200, 1200))

    # Center-crop bg_image to the canvas aspect, then resize. Photo fills the
    # full canvas — the watermark sits on top of the photo (not in a strip).
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
    photo_only = photo  # alias kept for the bbox detection block below

    # Per-platform top-UI safe zone (pixels reserved at the top of the canvas
    # for native platform overlays — username bar, progress indicator, etc.).
    # Headline must sit BELOW this band so it isn't occluded in the live ad.
    # Source: web research 2026-05-20 (TikTok Triple Whale, Meta Help Center).
    _SAFE_TOP_PX = {
        "tiktok": 140,  # ~130px username bar + a few px of breathing room
        "fb":      0,   # Feed has no fixed top overlay
        "ig":      0,   # Feed has no fixed top overlay
        "meta":    0,
        "google":  0,
    }
    safe_top = _SAFE_TOP_PX.get(platform, 0)

    # Subject-bbox detection for vertical placement (always at the top).
    # Detect on `photo_only` (the actual photo crop) since the full `photo`
    # canvas now includes white space below the photo for the brand strip,
    # which would skew vision-model coordinates.
    bbox = detect_subject_bbox(photo_only)
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
    # Pass `text=headline` so _load_font detects non-Latin scripts (Devanagari,
    # Bengali, Thai, Arabic, CJK, Hangul, …) and loads a script-aware font +
    # RAQM shaping. Without it, localized overlays render as tofu/"?" — the
    # exact regression LinkedIn's compose_ad was fixed for in d7accfb; this
    # path (Meta/Google/Reddit) was still loading the Latin-only brand font.
    hl_font  = _load_font(hl_size, bold=True, text=headline)
    hl_lines = _wrap_text(headline, hl_font, max_text_w)
    while len(hl_lines) > 2 and hl_size > hl_min:
        hl_size -= max(2, int(base * 0.004))
        hl_font  = _load_font(hl_size, bold=True, text=headline)
        hl_lines = _wrap_text(headline, hl_font, max_text_w)

    LINE_SPACING = 10
    hl_height    = hl_size * len(hl_lines) + LINE_SPACING * (len(hl_lines) - 1)
    # Headline top respects: (1) platform safe-top zone, (2) 4% canvas margin,
    # (3) anchoring to the photo subject's hairline. The MAX of these three
    # is the effective top — guarantees no occlusion by platform UI overlays.
    hl_top       = max(int(canvas_h * 0.04), safe_top, hl_bottom - hl_height)

    # Subtle dark scrim under the headline so white text always reads.
    scrim_pad_y = int(canvas_h * 0.025)
    scrim = Image.new("RGBA", (canvas_w, hl_height + 2 * scrim_pad_y), (0, 0, 0, 90))
    photo.alpha_composite(scrim, (0, max(0, hl_top - scrim_pad_y)))

    _draw_text_left(
        draw, hl_lines, hl_font, hl_top,
        0, (255, 255, 255, 255),
        line_spacing=LINE_SPACING, canvas_width=canvas_w,
    )

    # Supporting subheadline in the lower third — gives the ad context beyond
    # the headline (critical for TikTok/Drive hand-off where the PNG is the
    # whole ad). White, centered, smaller than the headline, with its own scrim,
    # sitting ABOVE the platform bottom safe-zone + the watermark.
    if subheadline:
        # Anchor the subheadline just ABOVE the FIXED watermark box so the two
        # never collide — the logo position is pinned per aspect, so the
        # subheadline floats up from there rather than the logo moving for copy.
        _wm_logo, _wm_x, _wm_top = _watermark_placement(canvas_w, canvas_h, aspect)
        sub_floor = (_wm_top - int(canvas_h * 0.02)) if _wm_logo is not None \
            else (canvas_h - int(canvas_h * 0.04))

        sub_size = int(base * 0.046)
        sub_min = int(base * 0.032)
        sub_font = _load_font(sub_size, bold=False, text=subheadline)
        sub_lines = _wrap_text(subheadline, sub_font, max_text_w)
        while len(sub_lines) > 3 and sub_size > sub_min:
            sub_size -= max(2, int(base * 0.003))
            sub_font = _load_font(sub_size, bold=False, text=subheadline)
            sub_lines = _wrap_text(subheadline, sub_font, max_text_w)

        SUB_SPACING = 8
        sub_height = sub_size * len(sub_lines) + SUB_SPACING * (len(sub_lines) - 1)
        sub_top = sub_floor - sub_height
        # Never collide with the headline block above.
        sub_top = max(sub_top, hl_top + hl_height + int(canvas_h * 0.05))

        sub_scrim = Image.new("RGBA", (canvas_w, sub_height + 2 * scrim_pad_y), (0, 0, 0, 90))
        photo.alpha_composite(sub_scrim, (0, max(0, sub_top - scrim_pad_y)))
        _draw_text_left(
            draw, sub_lines, sub_font, sub_top,
            0, (255, 255, 255, 255),
            line_spacing=SUB_SPACING, canvas_width=canvas_w,
        )

    # Outlier watermark — FIXED bottom-right position per aspect (copy/photo
    # never move it). See _WATERMARK_SPEC.
    _add_outlier_watermark(photo, aspect)

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

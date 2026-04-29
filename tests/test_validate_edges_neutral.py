"""
Tests for the deterministic edge-bleed detector in src/copy_design_qc.py.

The bug it guards against: Gemini paints the corner gradient washes all the
way to the photo edge instead of fading to neutral, leaving a thin saturated
stripe along the L/R sides of the creative. The vision-model QC misses subtle
cases, so we sample the outer photo column directly.

Tests build synthetic 1200x1200 PNGs that mirror compose_ad's layout (white
border + bottom strip + photo region) so the frame detection logic exercises
the same path it sees in production.
"""
from __future__ import annotations

import sys
from pathlib import Path

from PIL import Image, ImageDraw

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.copy_design_qc import validate_edges_neutral  # noqa: E402


CANVAS = 1200
BORDER = int(CANVAS * 0.033)        # 39
STRIP_H = int(CANVAS * 0.14)        # 168
PHOTO_RIGHT = CANVAS - BORDER       # 1161
PHOTO_BOTTOM = CANVAS - STRIP_H     # 1032


def _build_canvas(photo_fill: tuple[int, int, int]) -> Image.Image:
    """Mirror compose_ad's layout: white frame, white bottom strip, photo region in between."""
    canvas = Image.new("RGB", (CANVAS, CANVAS), (255, 255, 255))
    draw = ImageDraw.Draw(canvas)
    draw.rectangle([BORDER, BORDER, PHOTO_RIGHT - 1, PHOTO_BOTTOM - 1], fill=photo_fill)
    return canvas


def test_clean_neutral_photo_passes(tmp_path):
    """A photo region of plain neutral gray must pass — no edge stripes."""
    img = _build_canvas(photo_fill=(120, 120, 120))
    p = tmp_path / "clean.png"
    img.save(p)
    ok, msg = validate_edges_neutral(p)
    assert ok, f"clean photo should pass; got: {msg}"


def test_left_edge_teal_stripe_fails(tmp_path):
    """A vertical teal stripe at the L photo edge must FAIL."""
    img = _build_canvas(photo_fill=(120, 120, 120))
    draw = ImageDraw.Draw(img)
    # 6-pixel-wide saturated teal stripe at the inner left edge of the photo
    draw.rectangle([BORDER, BORDER + 60, BORDER + 6, PHOTO_BOTTOM - 60], fill=(40, 200, 220))
    p = tmp_path / "left_bleed.png"
    img.save(p)
    ok, msg = validate_edges_neutral(p)
    assert not ok, f"left-edge teal stripe should fail; got pass: {msg}"
    assert "left edge" in msg


def test_right_edge_pink_stripe_fails(tmp_path):
    """A vertical pink stripe at the R photo edge must FAIL."""
    img = _build_canvas(photo_fill=(120, 120, 120))
    draw = ImageDraw.Draw(img)
    draw.rectangle([PHOTO_RIGHT - 6, BORDER + 60, PHOTO_RIGHT - 1, PHOTO_BOTTOM - 60], fill=(245, 180, 175))
    p = tmp_path / "right_bleed.png"
    img.save(p)
    ok, msg = validate_edges_neutral(p)
    assert not ok, f"right-edge pink stripe should fail; got pass: {msg}"
    assert "right edge" in msg


def test_short_stripe_below_threshold_passes(tmp_path):
    """A small (<10%) stripe should not trigger — avoids false positives on natural color blobs."""
    img = _build_canvas(photo_fill=(120, 120, 120))
    draw = ImageDraw.Draw(img)
    # ~5% of photo height — well below the 10% row-frac threshold
    photo_h = PHOTO_BOTTOM - BORDER
    short_h = int(photo_h * 0.05)
    draw.rectangle([BORDER, BORDER + 100, BORDER + 6, BORDER + 100 + short_h], fill=(40, 200, 220))
    p = tmp_path / "small_blob.png"
    img.save(p)
    ok, msg = validate_edges_neutral(p)
    assert ok, f"5% short stripe should pass; got: {msg}"


def test_missing_file_skips_gracefully(tmp_path):
    """A path that doesn't exist returns (True, skip) — never raises in QC pipeline."""
    ok, msg = validate_edges_neutral(tmp_path / "does_not_exist.png")
    assert ok is True
    assert "skipped" in msg.lower()

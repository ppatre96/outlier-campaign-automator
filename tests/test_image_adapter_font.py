"""Regression guard for the tofu/question-marks bug: the Meta/Google/Reddit
compositor (_compose_simple_image_ad) MUST pass `text=headline` into _load_font
so non-Latin scripts pick a script-aware font instead of the Latin-only brand
font (which renders Hindi/Bengali/etc. as □□□ / ?)."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch, MagicMock

from PIL import Image

import src.image_adapter as ia

HINDI_HEADLINE = "हिंदी विशेषज्ञ चाहिए"


def test_load_font_receives_headline_text():
    bg = Image.new("RGB", (400, 400), (10, 20, 30))
    with patch.object(ia, "_load_font", return_value=MagicMock()) as mock_load, \
         patch.object(ia, "_wrap_text", return_value=["line"]), \
         patch.object(ia, "_draw_text_left"), \
         patch.object(ia, "detect_subject_bbox", return_value=None), \
         patch.object(ia, "_add_outlier_watermark"):
        ia._compose_simple_image_ad(bg, HINDI_HEADLINE, aspect=(1, 1), platform="meta")

    assert mock_load.call_count >= 1
    # Every _load_font call in this path must carry the headline text so script
    # detection + RAQM shaping engage.
    for call in mock_load.call_args_list:
        assert call.kwargs.get("text") == HINDI_HEADLINE


def test_platform_subheadline_field_precedence():
    """The simple compositor now renders a subheadline; it's pulled from
    'subheadline' (figma copy) → 'description' → 'primary_text' for non-Google,
    and 'description' → headlines[1] for Google."""
    assert ia._platform_subheadline({"subheadline": "S", "description": "D"}, "meta") == "S"
    assert ia._platform_subheadline({"description": "D", "primary_text": "P"}, "tiktok") == "D"
    assert ia._platform_subheadline({"primary_text": "P"}, "fb") == "P"
    assert ia._platform_subheadline({"headlines": ["H1", "H2"]}, "google") == "H2"
    assert ia._platform_subheadline({"headline": "only"}, "meta") == ""


def test_watermark_placement_is_fixed_per_aspect():
    """The Outlier watermark position is a pure function of aspect — identical
    coords regardless of copy/photo. 9:16 clears TikTok's caption zone."""
    l1, x1, y1 = ia._watermark_placement(1080, 1920, (9, 16))
    l2, x2, y2 = ia._watermark_placement(1080, 1920, (9, 16))
    assert (x1, y1) == (x2, y2)                      # deterministic
    assert 1080 - x1 - l1.width == 48                # 48px from right
    assert 1920 - y1 - l1.height == 290              # 290px from bottom (TikTok-safe)
    _, xs, ys = ia._watermark_placement(1080, 1080, (1, 1))
    assert 1080 - ys - l1.height == 44               # 1:1 → 44px from bottom (separate)

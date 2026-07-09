"""Tofu guard: never ship a creative whose localized overlay would render as
missing-glyph boxes. Deterministic (no vision call), so it protects the
Meta/Google/Reddit path that runs no vision QC."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.gemini_creative as gc
import src.copy_design_qc as qc


def test_latin_is_always_ok():
    assert gc.overlay_font_ok("Earn on your schedule") is True
    assert gc.overlay_font_ok("") is True


def test_bengali_ok_when_font_resolves(monkeypatch):
    # Pretend a Bengali font resolves (any truetype load succeeds).
    monkeypatch.setattr(gc, "_truetype", lambda path, size: object())
    assert gc.overlay_font_ok("বাংলা বিশেষজ্ঞ") is True


def test_bengali_tofu_when_no_font_resolves(monkeypatch):
    # No script font, no broad font, and fontconfig finds nothing → tofu → False.
    monkeypatch.setattr(gc, "_SCRIPT_FONTS", {"bengali": []})
    monkeypatch.setattr(gc, "_BROAD_UNICODE_FONTS", [])
    def _boom(path, size):
        raise OSError("no such font")
    monkeypatch.setattr(gc, "_truetype", _boom)
    monkeypatch.setattr(gc, "_fc_list_font", lambda script: None)  # fontconfig has no covering font
    monkeypatch.setattr(gc.time, "sleep", lambda *a: None)          # don't actually wait in the test
    assert gc.overlay_font_ok("বাংলা বিশেষজ্ঞ") is False


def test_check_overlay_renderable_flags_tofu(monkeypatch):
    # headline renders (True), subheadline would tofu (False) → one violation.
    monkeypatch.setattr(gc, "overlay_font_ok", lambda t: "বাংলা" not in t)
    ok, viol = qc.check_overlay_renderable(
        {"headline": "Earn with Outlier", "subheadline": "বাংলা বিশেষজ্ঞদের জন্য"})
    assert ok is False
    assert len(viol) == 1 and "subheadline" in viol[0] and "tofu" in viol[0].lower()


def test_check_overlay_renderable_clean_passes(monkeypatch):
    monkeypatch.setattr(gc, "overlay_font_ok", lambda t: True)
    ok, viol = qc.check_overlay_renderable({"headline": "H", "subheadline": "S"})
    assert ok is True and viol == []


def test_strip_overlay_symbols_removes_emoji_keeps_script():
    s = gc.strip_overlay_symbols
    # emoji the LLM slipped in (the live Bengali creative's box glyphs) → gone,
    # Bengali letters + $/% + digits kept.
    assert s("📊 আউটপুট 💰 উপার্জন") == "আউটপুট উপার্জন"
    assert s("Earn $5.50/hr — 85% remote ✅") == "Earn $5.50/hr — 85% remote"
    assert s("no symbols here") == "no symbols here"

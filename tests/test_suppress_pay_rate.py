"""Regression tests for config.SUPPRESS_PAY_RATE.

When the flag is on, generated creatives must carry NO pay-rate figure — in the
copy (brief block) or on the bottom band. When off, behaviour is unchanged.
Added for GMR-0019 (the rate is the experiment variable and lives on the LP,
not the creative).
"""
import config
from src.gemini_creative import derive_bottom_text
from src.brief_generator import _pay_rate_brief_block


def _set(flag: bool):
    config.SUPPRESS_PAY_RATE = flag


def test_band_keeps_rate_when_off(monkeypatch):
    _set(False)
    assert "$150" in derive_bottom_text("Earn $150/hr training AI")


def test_band_is_rate_free_when_on(monkeypatch):
    _set(True)
    try:
        assert derive_bottom_text("Earn $150/hr training AI") == "Fully remote. Paid hourly in USD."
        # Falls to the rate-free line even when the subheadline had no figure.
        assert derive_bottom_text("no figure here") == "Fully remote. Paid hourly in USD."
    finally:
        _set(False)


def test_brief_block_injects_rate_when_off(monkeypatch):
    _set(False)
    assert "$150/hr" in _pay_rate_brief_block("$150/hr")


def test_brief_block_is_rate_free_when_on(monkeypatch):
    _set(True)
    try:
        block = _pay_rate_brief_block("$150/hr")
        # The exact figure must NOT appear; the rate-free (UNRESOLVED) path is taken.
        assert "$150" not in block
        assert "UNRESOLVED" in block
    finally:
        _set(False)

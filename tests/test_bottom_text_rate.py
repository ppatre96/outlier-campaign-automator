"""derive_bottom_text must never emit a hardcoded dollar range.

Regression for the GMR-0023 bug (2026-07-03): angles whose subheadline led with
a non-rate hook fell back to a literal "Earn $25-$50 USD per hour", advertising a
false rate for low-rate cohorts (Bengali $5.50/hr shown as $25-$50 on LinkedIn +
Meta creatives). The fallback must use the cohort's real resolved rate when known,
and rate-free phrasing otherwise — never a fabricated range.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.gemini_creative import derive_bottom_text


def test_rate_in_subheadline_wins(monkeypatch):
    monkeypatch.setattr(config, "SUPPRESS_PAY_RATE", False, raising=False)
    out = derive_bottom_text("Get paid $5.50/hr to review AI", "$31/hr")
    assert out == "Earn $5.50/hr or more. Fully remote."


def test_falls_back_to_real_rate_when_subheadline_has_none(monkeypatch):
    monkeypatch.setattr(config, "SUPPRESS_PAY_RATE", False, raising=False)
    out = derive_bottom_text("Rate AI responses, get paid hourly", "$5.50/hr")
    assert out == "Earn $5.50/hr or more. Fully remote."
    assert "$25" not in out


def test_rate_free_when_no_rate_anywhere(monkeypatch):
    monkeypatch.setattr(config, "SUPPRESS_PAY_RATE", False, raising=False)
    out = derive_bottom_text("Review AI in Bengali, after hours", "")
    assert out == "Fully remote. Paid hourly in USD."


def test_never_emits_hardcoded_range(monkeypatch):
    monkeypatch.setattr(config, "SUPPRESS_PAY_RATE", False, raising=False)
    for sub, rate in [("no rate here", ""), ("flexible work", None), ("", "")]:
        assert "$25" not in derive_bottom_text(sub, rate or "")


def test_suppress_pay_rate_overrides(monkeypatch):
    monkeypatch.setattr(config, "SUPPRESS_PAY_RATE", True, raising=False)
    assert derive_bottom_text("Get paid $5.50/hr", "$5.50/hr") == "Fully remote. Paid hourly in USD."

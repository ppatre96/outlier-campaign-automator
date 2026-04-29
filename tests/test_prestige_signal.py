"""
Tests for compute_prestige_signal in src/profile_tiering.py.

The signal gates the conditional graft Pranav described 2026-04-29: fold
prestige cues into copy/targeting only if ≥50% of positives are top-tier.
These tests pin the threshold + missing-data behavior so future changes
to row_tier_labels can't silently flip the gate.
"""
from __future__ import annotations

import json
import sys
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.profile_tiering import compute_prestige_signal  # noqa: E402


def _row(school: str | None = None, company: str | None = None) -> dict:
    """Build a row matching the fetch_prestige_columns schema."""
    edu = (
        json.dumps([{"school": school}]) if school is not None else None
    )
    return {
        "linkedin_education": edu,
        "resume_job_company": company,
    }


def test_all_top_tier_applies():
    """Every positive at IIT/Stanford/Google → applies=True."""
    rows = [
        _row(school="IIT Bombay", company="Google"),
        _row(school="Stanford University", company="Microsoft"),
        _row(school="MIT", company="Meta"),
    ] * 5  # 15 rows — above MIN_FOR_SIGNAL=10
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df, country_hint="india")
    assert sig["applies"] is True
    assert sig["top_tier_pct"] == 1.0
    assert sig["n_with_data"] == 15


def test_mixed_above_threshold_applies():
    """60% top-tier, 40% regular → applies=True at the 50% threshold."""
    rows = [_row(school="IIT Delhi") for _ in range(6)]
    rows += [_row(school="Some Regional College") for _ in range(4)]
    rows += [_row(school="IIT Madras") for _ in range(2)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df, country_hint="india", threshold=0.50)
    assert sig["applies"] is True
    assert sig["top_tier_pct"] >= 0.50


def test_below_threshold_does_not_apply():
    """30% top-tier → applies=False."""
    rows = [_row(school="IIT Bombay") for _ in range(3)]
    rows += [_row(school="Generic State University") for _ in range(7)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df, country_hint="india")
    assert sig["applies"] is False
    assert sig["top_tier_pct"] < 0.50


def test_no_top_tier_does_not_apply():
    """Zero top-tier → applies=False, top_tier_pct=0."""
    rows = [_row(school="Generic Community College") for _ in range(15)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df)
    assert sig["applies"] is False
    assert sig["top_tier_pct"] == 0.0


def test_below_min_for_signal_does_not_apply():
    """Even 100% top-tier with only 5 rows shouldn't trip — too noisy."""
    rows = [_row(school="MIT") for _ in range(5)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df)
    assert sig["applies"] is False
    assert sig["top_tier_pct"] == 1.0  # ratio is 100%, but n is below MIN_FOR_SIGNAL


def test_no_prestige_data_returns_unavailable():
    """Rows where both prestige columns are null → n_with_data=0, applies=False."""
    rows = [{"linkedin_education": None, "resume_job_company": None} for _ in range(20)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df)
    assert sig["applies"] is False
    assert sig["n_with_data"] == 0
    assert "signal unavailable" in sig["summary"]


def test_company_only_signal():
    """Top-tier company without school data still counts as top_tier."""
    rows = [_row(company="Google") for _ in range(12)]
    df = pd.DataFrame(rows)
    sig = compute_prestige_signal(df)
    assert sig["applies"] is True
    assert sig["top_global_company_pct"] == 1.0


def test_empty_df():
    """Empty input → graceful return."""
    sig = compute_prestige_signal(pd.DataFrame())
    assert sig["applies"] is False
    assert sig["n_total"] == 0
    assert sig["summary"] == "no positives"

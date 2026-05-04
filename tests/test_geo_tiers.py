"""
Tests for src/geo_tiers.py — per-geo campaign grouping and rate computation.

Key behaviors validated:
  - G4 blocked geos strictly filtered
  - Geos grouped by ethnic creative cluster
  - Rate computed as base_rate × median(cluster_multipliers), rounded to $5
  - Single-geo input → single group, no splitting
  - All-G4 input → empty groups (no campaigns)
  - Mixed tier input → multiple groups with correct rates
"""
from __future__ import annotations

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.geo_tiers import (  # noqa: E402
    filter_blocked_geos,
    group_geos_for_campaigns,
    compute_geo_rate,
    COUNTRY_PAY_MULTIPLIER,
    GEO_G4_BLOCKED,
    GEO_ETHNIC_CLUSTER,
)


# ── filter_blocked_geos ────────────────────────────────────────────────────────

def test_g4_strictly_skipped():
    """G4 blocked countries must never appear in the allowed list."""
    blocked_sample = list(GEO_G4_BLOCKED)[:3]
    allowed, skipped = filter_blocked_geos(blocked_sample)
    assert allowed == []
    assert set(skipped) == set(b.upper() for b in blocked_sample)


def test_allowed_geos_pass_through():
    allowed, skipped = filter_blocked_geos(["US", "IN", "GB"])
    assert set(allowed) == {"US", "IN", "GB"}
    assert skipped == []


def test_mixed_allowed_and_blocked():
    allowed, skipped = filter_blocked_geos(["US", "SY", "GB", "KP"])
    assert set(allowed) == {"US", "GB"}
    assert set(skipped) == {"SY", "KP"}


def test_empty_input():
    allowed, skipped = filter_blocked_geos([])
    assert allowed == []
    assert skipped == []


def test_lowercase_input_normalised():
    """Country codes should be normalised to uppercase."""
    allowed, skipped = filter_blocked_geos(["us", "sy", "gb"])
    assert "US" in allowed
    assert "GB" in allowed
    assert "SY" in skipped


# ── compute_geo_rate ──────────────────────────────────────────────────────────

def test_us_rate_equals_base():
    assert compute_geo_rate(50.0, "US") == "$50/hr"


def test_sg_rate_equals_base():
    """Singapore multiplier is 1.0 — should equal US rate."""
    assert compute_geo_rate(50.0, "SG") == "$50/hr"


def test_ch_rate_slightly_above_base():
    """Switzerland multiplier is 1.05 → $50 × 1.05 = $52.5 → rounds to nearest $5 = $50."""
    result = compute_geo_rate(50.0, "CH")
    assert result == "$50/hr"  # $52.5 rounds to $50 (nearest $5)


def test_india_rate_scaled():
    """IN multiplier = 0.55 → $50 × 0.55 = $27.5 → rounds to $30."""
    result = compute_geo_rate(50.0, "IN")
    assert result == "$30/hr"


def test_philippines_rate_scaled():
    """PH multiplier = 0.47 → $50 × 0.47 = $23.5 → rounds to $25."""
    result = compute_geo_rate(50.0, "PH")
    assert result == "$25/hr"


def test_minimum_rate_five():
    """Even very low multipliers should produce at least $5."""
    result = compute_geo_rate(50.0, "NG")  # NG = 0.06
    val = int(result.replace("$", "").replace("/hr", ""))
    assert val >= 5


def test_unknown_country_defaults():
    """Unknown country code should not raise — uses a default multiplier."""
    result = compute_geo_rate(50.0, "XX")
    assert result.startswith("$")


# ── group_geos_for_campaigns ─────────────────────────────────────────────────

def test_single_geo_no_split():
    groups = group_geos_for_campaigns(["US"], base_rate_usd=50.0)
    assert len(groups) == 1
    assert groups[0].geos == ["US"]
    assert groups[0].advertised_rate == "$50/hr"


def test_same_cluster_geos_merged():
    """US + CA + GB + AU are all 'anglo' → one group."""
    groups = group_geos_for_campaigns(["US", "CA", "GB", "AU"], base_rate_usd=50.0)
    assert len(groups) == 1
    assert groups[0].cluster == "anglo"
    assert set(groups[0].geos) == {"US", "CA", "GB", "AU"}


def test_different_cluster_geos_split():
    """US (anglo) + IN (south_asian) → two groups."""
    groups = group_geos_for_campaigns(["US", "IN"], base_rate_usd=50.0)
    clusters = {g.cluster for g in groups}
    assert "anglo" in clusters
    assert "south_asian" in clusters
    assert len(groups) == 2


def test_per_geo_rates_differ():
    """Groups for different multiplier regions should have different advertised rates."""
    groups = group_geos_for_campaigns(["US", "CA", "IN", "PH"], base_rate_usd=50.0)
    rates = {g.cluster: g.advertised_rate for g in groups}
    assert rates["anglo"] != rates["south_asian"]
    # Anglo (US=1.0, CA=0.91) median ~0.955 → $50 → "$50/hr"
    # South Asian (IN=0.55, PH=0.47) median ~0.51 → $25 or $30
    assert int(rates["anglo"].replace("$", "").replace("/hr", "")) > \
           int(rates["south_asian"].replace("$", "").replace("/hr", ""))


def test_all_g4_geos_returns_empty():
    """When every geo is G4 blocked, group_geos_for_campaigns returns empty list."""
    all_blocked = ["SY", "KP", "AF"]
    groups = group_geos_for_campaigns(all_blocked, base_rate_usd=50.0)
    assert groups == []


def test_mixed_g4_and_allowed():
    """G4 geos are filtered; allowed geos still form groups."""
    groups = group_geos_for_campaigns(["US", "SY", "GB", "KP"], base_rate_usd=50.0)
    all_geos = [g for grp in groups for g in grp.geos]
    assert "SY" not in all_geos
    assert "KP" not in all_geos
    assert "US" in all_geos or "GB" in all_geos


def test_latam_geos_grouped():
    """LATAM countries cluster together."""
    groups = group_geos_for_campaigns(["MX", "CO", "AR", "PE"], base_rate_usd=50.0)
    assert len(groups) == 1
    assert groups[0].cluster == "latin_american"


def test_campaign_suffix_in_name():
    """Non-global clusters should have a descriptive campaign_suffix."""
    groups = group_geos_for_campaigns(["IN", "PK"], base_rate_usd=50.0)
    assert groups[0].campaign_suffix == "south_asian"
    assert groups[0].cluster_label == "South Asian"


def test_empty_geos_returns_empty():
    groups = group_geos_for_campaigns([], base_rate_usd=50.0)
    assert groups == []


# ── data integrity ────────────────────────────────────────────────────────────

def test_no_g4_country_has_multiplier_zero():
    """Sanity: every country in the multiplier table has multiplier > 0."""
    for cc, mult in COUNTRY_PAY_MULTIPLIER.items():
        assert mult > 0, f"{cc} has zero multiplier"


def test_multiplier_table_covers_common_geos():
    """Core geos used by campaigns must be in the multiplier table."""
    core = ["US", "CA", "GB", "AU", "IN", "SG", "DE", "FR", "JP", "BR", "MX"]
    missing = [c for c in core if c not in COUNTRY_PAY_MULTIPLIER]
    assert missing == [], f"Missing from multiplier table: {missing}"

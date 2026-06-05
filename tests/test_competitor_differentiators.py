"""Competitor differentiators: per-TG relevance ranking (A) + live Trustpilot
grounding (B). Previously a static list re-ordered only by competitor keyword
overlap, so every ramp surfaced the same 3 bullets.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.competitor_intel import (
    score_differentiators, _finalize_differentiators, CompetitorIntel, AdCreative,
)


def _ad():
    return AdCreative(
        competitor="X", hook="earn money fast flexible", body="weekly pay work from home",
        cta="Apply", format="image", profession_mention=None, source="meta",
        earnings_claim="$30/hr", angle="B",
    )


def test_top3_varies_by_tg_label():
    ads = [_ad() for _ in range(3)]
    coder = [d["claim"] for d in score_differentiators(ads, "T3 backend software engineers python coders technical")[:3]]
    gig = [d["claim"] for d in score_differentiators(ads, "flexible remote gig earn extra income from home")[:3]]
    assert coder != gig, "differentiators must differ by TG, not be a static list"
    # coder ramp leans on expertise/frontier claims
    assert any("frontier model" in c or "Domain-specific" in c for c in coder)
    # gig ramp leans on earnings claims
    assert any("$500M" in c or "Weekly payment" in c for c in gig)


def test_trustpilot_claim_live_grounded():
    intel = CompetitorIntel()
    intel.trustpilot_ratings = {"Outlier": {"rating": 4.3, "review_count": 12044}}
    scored = score_differentiators([], "trust legit reviews safe")  # boosts the trustpilot theme
    finals = _finalize_differentiators(scored, intel)
    assert any("4.3/5 Trustpilot rating (12,044 reviews)" in c for c in finals)
    # no static "4.1/5" leaks through anywhere
    assert not any("4.1/5" in c for c in finals)


def test_trustpilot_claim_drops_number_when_no_live_data():
    intel = CompetitorIntel()  # no scraped rating
    # isolate the trustpilot claim
    tp_only = [d for d in score_differentiators([], "trust") if d.get("live") == "trustpilot"]
    finals = _finalize_differentiators(tp_only, intel)
    assert finals == ["Independent Trustpilot reviews — earned, not manufactured"]
    assert not any(ch.isdigit() for ch in finals[0])  # never invents a rating

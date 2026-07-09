"""Unit tests for the TikTok targeting resolver (src/tiktok_targeting.py)."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.tiktok_targeting import TikTokTargetingResolver


class TestTikTokTargeting:
    def test_iso_to_location_ids(self):
        r = TikTokTargetingResolver()
        out = r.resolve_cohort(None, geos=["US", "GB"])
        assert out["location_ids"] == sorted([6252001, 2635167])
        assert out["genders"] == "GENDER_UNLIMITED"
        assert "AGE_18_24" in out["age_groups"] and "AGE_13_17" not in out["age_groups"]

    def test_case_insensitive_and_dedup(self):
        r = TikTokTargetingResolver()
        out = r.resolve_cohort(None, geos=["us", "US", "gb"])
        assert out["location_ids"] == sorted([6252001, 2635167])

    def test_unmapped_geo_dropped_and_surfaced(self):
        r = TikTokTargetingResolver()
        out = r.resolve_cohort(None, geos=["US", "XX", "ZZ"])
        assert out["location_ids"] == [6252001]
        assert out["unmapped_geos"] == ["XX", "ZZ"]

    def test_no_geos_omits_location_ids(self):
        r = TikTokTargetingResolver()
        out = r.resolve_cohort(None, geos=[])
        assert "location_ids" not in out            # broad (all locations)
        assert out["age_groups"] and out["genders"] == "GENDER_UNLIMITED"

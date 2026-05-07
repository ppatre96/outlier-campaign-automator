"""Unit tests for the AdPlatformClient ABC + PlatformConstraints + CreateAdResult."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    LINKEDIN_CONSTRAINTS,
    META_CONSTRAINTS,
    GOOGLE_CONSTRAINTS,
    enabled_platforms,
    get_constraints,
)


class TestPlatformConstraints:
    def test_linkedin_supports_inmail(self):
        assert LINKEDIN_CONSTRAINTS.supports_inmail is True

    def test_meta_does_not_support_inmail(self):
        assert META_CONSTRAINTS.supports_inmail is False

    def test_google_rda_has_multiple_headlines(self):
        assert GOOGLE_CONSTRAINTS.headline_count == 3
        assert GOOGLE_CONSTRAINTS.long_headline_max_chars == 90

    def test_meta_has_tightest_headline(self):
        assert META_CONSTRAINTS.headline_max_chars == 40
        assert LINKEDIN_CONSTRAINTS.headline_max_chars == 70
        assert GOOGLE_CONSTRAINTS.headline_max_chars == 30

    def test_get_constraints_lookup(self):
        assert get_constraints("linkedin") is LINKEDIN_CONSTRAINTS
        assert get_constraints("meta") is META_CONSTRAINTS
        assert get_constraints("google") is GOOGLE_CONSTRAINTS

    def test_unknown_platform_raises(self):
        with pytest.raises(KeyError):
            get_constraints("snapchat")


class TestEnabledPlatforms:
    def test_default_includes_all_three(self, monkeypatch):
        # The ENABLED_PLATFORMS env value is captured at config import time;
        # to test runtime override we mutate config directly.
        import config
        monkeypatch.setattr(config, "ENABLED_PLATFORMS", "linkedin,meta,google")
        assert set(enabled_platforms()) == {"linkedin", "meta", "google"}

    def test_unknown_platforms_dropped(self, monkeypatch):
        import config
        monkeypatch.setattr(config, "ENABLED_PLATFORMS", "linkedin,tiktok,meta")
        assert enabled_platforms() == ["linkedin", "meta"]

    def test_empty_falls_back_to_linkedin(self, monkeypatch):
        import config
        monkeypatch.setattr(config, "ENABLED_PLATFORMS", "")
        assert enabled_platforms() == ["linkedin"]


class TestCreateAdResult:
    def test_creative_urn_alias_round_trip(self):
        r = CreateAdResult(creative_urn="urn:li:sponsoredCreative:1")
        assert r.creative_id == "urn:li:sponsoredCreative:1"
        assert r.creative_urn == "urn:li:sponsoredCreative:1"

    def test_creative_id_round_trip(self):
        r = CreateAdResult(creative_id="meta_ad_42", status="ok")
        assert r.creative_urn == "meta_ad_42"

    def test_status_local_fallback_default_creative_id_none(self):
        r = CreateAdResult(status="local_fallback", error_class="X", error_message="m")
        assert r.creative_id is None
        assert r.status == "local_fallback"

    def test_setter_via_creative_urn(self):
        r = CreateAdResult()
        r.creative_urn = "x"
        assert r.creative_id == "x"


class TestAdPlatformClientABC:
    def test_cannot_instantiate_abc_directly(self):
        with pytest.raises(TypeError):
            AdPlatformClient()  # type: ignore[abstract]

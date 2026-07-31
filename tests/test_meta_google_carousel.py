"""Meta + Google Demand Gen carousel — specs, validation, and payload shape.

Cards can't be edited after an ad is saved on either platform, so validation is
fail-closed and these tests pin every published limit it enforces. Meta specs
verified 2026-07-30 from the Marketing API carousel reference; Google field names
read off the installed protos (google-ads 30.1.0, default v24) — see the module
docstrings for the two places the protos disagree with the documentation.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.meta_carousel import (  # noqa: E402
    CARD_NAME_MAX, MAX_CARDS, MIN_CARDS, MetaCarouselCard, MetaCarouselSpecError,
    build_link_data,
)
from src.meta_carousel import validate_cards as validate_meta  # noqa: E402


def _png(tmp_path: Path, name: str, size=(1080, 1350)) -> Path:
    from PIL import Image

    p = tmp_path / name
    Image.new("RGB", size, "white").save(p)
    return p


def _meta_card(tmp_path, name="c.png", size=(1080, 1350), headline="A real headline",
               landing="https://outlier.ai/experts/x") -> MetaCarouselCard:
    return MetaCarouselCard(png_path=_png(tmp_path, name, size), headline=headline,
                            landing_page=landing, image_hash="abc123")


def _meta_cards(tmp_path, n=4, **kw) -> list:
    return [_meta_card(tmp_path, name=f"c{i}.png", **kw) for i in range(n)]


# ── Meta: card count and assets ───────────────────────────────────────────────

def test_meta_accepts_two_to_ten_cards(tmp_path):
    validate_meta(_meta_cards(tmp_path, MIN_CARDS))
    validate_meta(_meta_cards(tmp_path, MAX_CARDS))


def test_meta_rejects_one_card(tmp_path):
    with pytest.raises(MetaCarouselSpecError, match="2-10 cards"):
        validate_meta(_meta_cards(tmp_path, 1))


def test_meta_rejects_mixed_ratios_because_facebook_crops_silently(tmp_path):
    """Facebook takes the FIRST card's ratio and crops the rest, so a mixed set
    is never rejected by Meta — it just quietly mangles every later card."""
    cards = [_meta_card(tmp_path, "a.png", (1080, 1080)),
             _meta_card(tmp_path, "b.png", (1080, 1350))]
    with pytest.raises(MetaCarouselSpecError, match="mix aspect ratios"):
        validate_meta(cards)


def test_meta_accepts_both_published_ratios(tmp_path):
    validate_meta(_meta_cards(tmp_path, 2, size=(1080, 1080)))
    validate_meta(_meta_cards(tmp_path, 2, size=(1080, 1350)))


def test_meta_rejects_an_unsupported_ratio(tmp_path):
    with pytest.raises(MetaCarouselSpecError, match="wants one of"):
        validate_meta(_meta_cards(tmp_path, 2, size=(1910, 1000)))


def test_meta_rejects_overlong_headline_and_missing_landing_page(tmp_path):
    cards = _meta_cards(tmp_path, 2)
    cards[0].headline = "x" * (CARD_NAME_MAX + 1)
    cards[1].landing_page = ""
    with pytest.raises(MetaCarouselSpecError) as exc:
        validate_meta(cards)
    assert "over the 40-char limit" in str(exc.value)
    assert "no landing page" in str(exc.value)


# ── Meta: the payload, and the reordering trap ────────────────────────────────

def test_link_data_keeps_our_card_order(tmp_path):
    """multi_share_optimized defaults to TRUE, which lets Meta reorder the cards.
    The four cards are one argument read in order, so it must be False."""
    cards = _meta_cards(tmp_path, 4)
    for i, c in enumerate(cards, 1):
        c.headline = f"card {i}"
    ld = build_link_data(cards, primary_text="hello",
                         default_link="https://outlier.ai/experts/x",
                         cta_type="APPLY_NOW")
    assert ld["multi_share_optimized"] is False
    assert ld["multi_share_end_card"] is False, "an end card would bury our CTA card"
    assert [c["name"] for c in ld["child_attachments"]] == [f"card {i}" for i in range(1, 5)]


def test_child_attachment_has_the_required_fields(tmp_path):
    card = _meta_card(tmp_path, landing="https://outlier.ai/experts/x?utm_content=card1")
    att = card.as_child_attachment(default_link="https://fallback", cta_type="APPLY_NOW")
    assert att["link"] == "https://outlier.ai/experts/x?utm_content=card1"
    assert att["image_hash"] == "abc123"
    assert att["call_to_action"] == {
        "type": "APPLY_NOW",
        "value": {"link": "https://outlier.ai/experts/x?utm_content=card1"},
    }


def test_link_data_truncates_primary_text(tmp_path):
    ld = build_link_data(_meta_cards(tmp_path, 2), primary_text="x" * 400,
                         default_link="https://o.ai", cta_type="LEARN_MORE")
    assert len(ld["message"]) == 125


def test_meta_carousel_ad_refuses_cards_without_an_image_hash(tmp_path, monkeypatch):
    """A card with no image_hash would post an empty carousel."""
    import config
    from src.meta_api import MetaClient

    monkeypatch.setattr(config, "META_PAGE_ID", "12345", raising=False)
    client = MetaClient()
    monkeypatch.setattr(client, "_page_id", "12345", raising=False)
    cards = _meta_cards(tmp_path, 2)
    cards[1].image_hash = ""
    result = client.create_carousel_ad("adset_1", cards)
    assert result.status == "error"
    assert "no image_hash" in (result.error_message or "")


def test_meta_carousel_ad_reports_spec_failure_without_calling_meta(tmp_path, monkeypatch):
    import config
    from src.meta_api import MetaClient

    monkeypatch.setattr(config, "META_PAGE_ID", "12345", raising=False)
    client = MetaClient()
    monkeypatch.setattr(client, "_page_id", "12345", raising=False)
    called = []
    monkeypatch.setattr(client, "_ensure_init", lambda: called.append(1))
    result = client.create_carousel_ad("adset_1", _meta_cards(tmp_path, 1))
    assert result.status == "error"
    assert "2-10 cards" in (result.error_message or "")
    assert not called, "must not initialise the SDK when the cards fail spec"


def test_meta_carousel_ad_degrades_when_no_page_configured(tmp_path, monkeypatch):
    """Same graceful degradation as the single-image arm: a human can still build
    it by hand from the PNGs."""
    from src.meta_api import MetaClient

    client = MetaClient()
    monkeypatch.setattr(client, "_page_id", "", raising=False)
    result = client.create_carousel_ad("adset_1", _meta_cards(tmp_path, 4))
    assert result.status == "local_fallback"
    assert "META_PAGE_ID" in (result.error_message or "")


# ── Google Demand Gen ─────────────────────────────────────────────────────────

def test_google_validates_cards_and_ad_text(tmp_path):
    from src.google_carousel import (
        AD_DESCRIPTION_MAX, BUSINESS_NAME_MAX, GoogleCarouselCard,
        GoogleCarouselSpecError, validate_ad_text, validate_cards,
    )

    cards = [GoogleCarouselCard(png_path=_png(tmp_path, f"g{i}.png", (1200, 1200)),
                                headline=f"card {i}") for i in range(4)]
    validate_cards(cards)          # 1:1 is a published Demand Gen ratio

    cards[0].headline = "x" * 41
    with pytest.raises(GoogleCarouselSpecError, match="over the 40-char limit"):
        validate_cards(cards)

    validate_ad_text(business_name="Outlier", headline="Tax pros, AI needs you",
                     description="Paid per task, hours that suit you.")
    with pytest.raises(GoogleCarouselSpecError, match="business_name"):
        validate_ad_text(business_name="x" * (BUSINESS_NAME_MAX + 1),
                         headline="h", description="d")
    with pytest.raises(GoogleCarouselSpecError, match="ad description"):
        validate_ad_text(business_name="Outlier", headline="h",
                         description="x" * (AD_DESCRIPTION_MAX + 1))


def test_google_rejects_mixed_ratios(tmp_path):
    from src.google_carousel import (
        GoogleCarouselCard, GoogleCarouselSpecError, validate_cards,
    )

    cards = [GoogleCarouselCard(png_path=_png(tmp_path, "a.png", (1200, 1200)), headline="a"),
             GoogleCarouselCard(png_path=_png(tmp_path, "b.png", (1200, 628)), headline="b")]
    with pytest.raises(GoogleCarouselSpecError, match="mix aspect ratios"):
        validate_cards(cards)


def test_google_carousel_ad_refuses_before_creating_assets(tmp_path, monkeypatch):
    """Google's asset layers mean a rejected ad leaves orphaned image and card
    assets behind, so validation must run before any mutate call."""
    from src.google_ads_api import GoogleAdsClient
    from src.google_carousel import GoogleCarouselCard

    client = GoogleAdsClient()
    called = []
    monkeypatch.setattr(client, "_ensure_client", lambda: called.append(1))
    cards = [GoogleCarouselCard(png_path=_png(tmp_path, "g.png", (1200, 1200)),
                                headline="only one card")]
    result = client.create_carousel_ad(
        "customers/1/adGroups/2", cards, headline="h", description="d",
    )
    assert result.status == "error"
    assert "2-10 cards" in (result.error_message or "")
    assert not called


def test_google_carousel_ad_requires_card_assets(tmp_path, monkeypatch):
    from src.google_ads_api import GoogleAdsClient
    from src.google_carousel import GoogleCarouselCard

    client = GoogleAdsClient()
    monkeypatch.setattr(client, "_ensure_client", lambda: pytest.fail("must not call Google"))
    cards = [GoogleCarouselCard(png_path=_png(tmp_path, f"g{i}.png", (1200, 1200)),
                                headline=f"card {i}", image_asset="customers/1/assets/9")
             for i in range(2)]
    result = client.create_carousel_ad(
        "customers/1/adGroups/2", cards, headline="h", description="d",
    )
    assert result.status == "error"
    assert "create_carousel_card_assets first" in (result.error_message or "")


def test_google_card_assets_need_uploaded_images(tmp_path, monkeypatch):
    from src.google_ads_api import GoogleAdsClient
    from src.google_carousel import GoogleCarouselCard, GoogleCarouselSpecError

    client = GoogleAdsClient()
    monkeypatch.setattr(client, "_ensure_client", lambda: pytest.fail("must not call Google"))
    cards = [GoogleCarouselCard(png_path=_png(tmp_path, "g.png", (1200, 1200)), headline="a")]
    with pytest.raises(GoogleCarouselSpecError, match="upload_image first"):
        client.create_carousel_card_assets(cards)


# ── Registry ──────────────────────────────────────────────────────────────────

def test_both_carousel_platforms_are_registered():
    """A platform missing from PLATFORM_CONSTRAINTS is silently dropped by
    enabled_platforms(), which is how the carousel channel was almost shipped
    dead the first time."""
    from src.ad_platform import PLATFORM_CONSTRAINTS
    from src.image_adapter import primary_aspect

    for name, aspect in (("meta_carousel", (4, 5)), ("google_carousel", (1, 1))):
        assert name in PLATFORM_CONSTRAINTS
        assert PLATFORM_CONSTRAINTS[name].headline_max_chars == 40
        assert primary_aspect(name) == aspect


# ── The dispatch arm ──────────────────────────────────────────────────────────

class _StubGeo:
    cluster = "anglo"
    cluster_label = "US"
    campaign_suffix = "us"
    advertised_rate = "$40/hr"


class _StubResolver:
    def resolve_cohort(self, cohort, geos=None):
        return {"geo_locations": {"countries": list(geos or [])}}


class _StubMetaClient:
    """Records the calls the arm makes so the wiring is checked without Meta."""
    def __init__(self):
        self.calls = []
        self.uploaded = []

    def create_campaign_group(self, name, geos=None):
        self.calls.append(("group", name)); return "group_1"

    def create_campaign(self, *, name, campaign_group_id, targeting, **kw):
        self.calls.append(("adset", name, campaign_group_id)); return "adset_1"

    def upload_image(self, path):
        self.uploaded.append(str(path)); return f"hash_{len(self.uploaded)}"

    def create_carousel_ad(self, ad_set_id, cards, **kw):
        from src.ad_platform import CreateAdResult
        self.calls.append(("ad", ad_set_id, len(cards), kw))
        return CreateAdResult(creative_id="ad_1", status="ok")


def _spec(tmp_path):
    class _Cohort:
        name = "US personal-tax professionals"
        rules = []
        _stg_id = "T1"
        _stg_name = "tax pros"
        id = "T1"

    return {
        "cohort": _Cohort(), "geo_group": _StubGeo(), "group_geos": ["US"],
        "angle_idx": 0, "angle_label": "A",
        "variants": [{
            "headline": "Tax pros, AI needs you now",
            "subheadline": "Review AI answers on returns",
            "intro_text": "Outlier matches tax professionals with AI projects.",
            "photo_subject": "a certified public accountant in a small office",
            "advertised_rate": "$40/hr", "angle": "A", "cta_button": "APPLY_NOW",
        }],
        "png_path": None,
    }


def test_meta_carousel_arm_builds_group_adset_and_ad(tmp_path, monkeypatch):
    """The arm reuses the ordinary Meta container (an ad set holds any format) and
    only the AD is carousel-shaped."""
    import main

    monkeypatch.setattr("src.campaign_registry.log_campaign", lambda **kw: None)
    monkeypatch.setattr(main, "_render_carousel_card",
                        lambda cv, angle, aspect, **kw: _png(tmp_path, f"card{kw['slot']}.png",
                                                             (1080, 1350)))
    monkeypatch.setattr("src.utm_builder.resolve_base_lp_url",
                        lambda **kw: "https://outlier.ai/experts/qfinance")
    monkeypatch.setattr("src.utm_builder.build_utm_url",
                        lambda **kw: "https://outlier.ai/experts/qfinance?utm_source=Meta")
    monkeypatch.setattr("src.carousel.build_card_copy",
                        lambda v, **kw: [__import__("src.carousel", fromlist=["CardCopy"]).CardCopy(
                            overlay=f"overlay {i}", caption=f"caption {i}") for i in range(1, 5)])

    client = _StubMetaClient()
    out = main._process_platform_carousel_arm(
        platform="meta_carousel", client=client, resolver=_StubResolver(),
        campaign_specs=[_spec(tmp_path)], ramp_id="GMR-TEST-ARM",
        cohort_id_override=None, destination_url_override=None,
    )
    kinds = [c[0] for c in client.calls]
    assert kinds == ["group", "adset", "ad"], client.calls
    assert client.calls[2][2] == 4, "all four cards must reach the ad"
    assert len(client.uploaded) == 4
    assert out["campaigns"] == ["adset_1"]
    assert out["creative_paths"] == {"T1_A": "ad_1"}


def test_carousel_arm_rejects_an_unknown_platform():
    import main

    with pytest.raises(ValueError, match="unsupported carousel platform"):
        main._process_platform_carousel_arm(
            platform="tiktok_carousel", client=None, resolver=None,
            campaign_specs=[{"cohort": None, "geo_group": _StubGeo()}],
            ramp_id=None, cohort_id_override=None, destination_url_override=None,
        )

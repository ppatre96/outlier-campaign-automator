"""LinkedIn Carousel Ads — spec validation, card copy, and the API payload.

Carousel cards CANNOT be edited once an ad is saved, so validate_cards is
fail-closed and these tests pin every published limit it enforces. Specs
verified 2026-07-30 against LinkedIn's Carousel Ads API docs + Marketing
Solutions Help a427022.
"""
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.linkedin_carousel import (  # noqa: E402
    CARD_HEADLINE_MAX, INTRO_TEXT_SAFE, MAX_CARDS, MIN_CARDS,
    CarouselCard, CarouselSpecError, build_card_copy, card_photo_variant,
    clamp_intro_text, validate_cards,
)


def _png(tmp_path: Path, name: str, size=(1080, 1080)) -> Path:
    from PIL import Image

    p = tmp_path / name
    Image.new("RGB", size, "white").save(p)
    return p


def _card(tmp_path, name="c.png", size=(1080, 1080), headline="A real headline",
          landing="https://outlier.ai/experts/x") -> CarouselCard:
    return CarouselCard(png_path=_png(tmp_path, name, size), headline=headline,
                        landing_page=landing)


def _cards(tmp_path, n=4, **kw) -> list:
    return [_card(tmp_path, name=f"c{i}.png", **kw) for i in range(n)]


# ── Card count ───────────────────────────────────────────────────────────────

def test_accepts_two_to_ten_cards(tmp_path):
    validate_cards(_cards(tmp_path, MIN_CARDS))
    validate_cards(_cards(tmp_path, MAX_CARDS))


def test_rejects_one_card(tmp_path):
    with pytest.raises(CarouselSpecError, match="2-10 cards"):
        validate_cards(_cards(tmp_path, 1))


def test_rejects_eleven_cards(tmp_path):
    with pytest.raises(CarouselSpecError, match="2-10 cards"):
        validate_cards(_cards(tmp_path, 11))


# ── Image specs ──────────────────────────────────────────────────────────────

def test_rejects_mixed_aspect_ratios(tmp_path):
    """LinkedIn requires ONE ratio across a carousel; mixing pads or crops and
    visibly breaks the scroll."""
    cards = [
        _card(tmp_path, "sq.png", (1080, 1080)),
        _card(tmp_path, "wide.png", (1920, 1080)),
    ]
    with pytest.raises(CarouselSpecError, match="mix aspect ratios"):
        validate_cards(cards)


def test_rejects_non_square_even_when_consistent(tmp_path):
    cards = _cards(tmp_path, 2, size=(1920, 1080))
    with pytest.raises(CarouselSpecError, match=r"carousel wants \(1, 1\)"):
        validate_cards(cards)


def test_rejects_images_below_the_recommended_1080(tmp_path):
    cards = _cards(tmp_path, 2, size=(600, 600))
    with pytest.raises(CarouselSpecError, match="below the 1080px"):
        validate_cards(cards)


def test_rejects_images_over_4320(tmp_path):
    cards = _cards(tmp_path, 2, size=(5000, 5000))
    with pytest.raises(CarouselSpecError, match="exceeds 4320px"):
        validate_cards(cards)


def test_rejects_unsupported_file_type(tmp_path):
    cards = _cards(tmp_path, 2)
    bad = tmp_path / "card.bmp"
    bad.write_bytes(cards[0].png_path.read_bytes())
    cards[0].png_path = bad
    with pytest.raises(CarouselSpecError, match="not one of"):
        validate_cards(cards)


def test_rejects_missing_image(tmp_path):
    cards = _cards(tmp_path, 2)
    cards[0].png_path = tmp_path / "gone.png"
    with pytest.raises(CarouselSpecError, match="image missing"):
        validate_cards(cards)


# ── Copy specs ───────────────────────────────────────────────────────────────

def test_rejects_headline_over_45_chars(tmp_path):
    cards = _cards(tmp_path, 2)
    cards[1].headline = "x" * (CARD_HEADLINE_MAX + 1)
    with pytest.raises(CarouselSpecError, match="over the 45-char limit"):
        validate_cards(cards)


def test_rejects_empty_headline_and_missing_landing_page(tmp_path):
    cards = _cards(tmp_path, 2)
    cards[0].headline = "  "
    cards[1].landing_page = ""
    with pytest.raises(CarouselSpecError) as exc:
        validate_cards(cards)
    assert "empty headline" in str(exc.value)
    assert "no landing page" in str(exc.value)


def test_clamp_intro_text_trims_on_a_word_boundary():
    long = "word " * 60
    out = clamp_intro_text(long)
    assert len(out) <= INTRO_TEXT_SAFE
    assert not out.endswith("wor"), "must not cut mid-word"
    assert clamp_intro_text("  short   text ") == "short text"


# ── API payload ──────────────────────────────────────────────────────────────

def test_as_api_card_matches_the_posts_api_shape(tmp_path):
    card = _card(tmp_path)
    card.image_urn = "urn:li:image:ABC123"
    card.alt_text = "alt"
    payload = card.as_api_card()
    assert payload == {
        "media": {"id": "urn:li:image:ABC123", "title": "A real headline", "altText": "alt"},
        "landingPage": "https://outlier.ai/experts/x",
    }


def test_as_api_card_omits_alt_text_when_absent(tmp_path):
    card = _card(tmp_path)
    card.image_urn = "urn:li:image:ABC123"
    assert "altText" not in card.as_api_card()["media"]


def test_carousel_ad_refuses_cards_without_an_uploaded_image(tmp_path, monkeypatch):
    """A card with no image URN would post an empty carousel."""
    import config
    from src.linkedin_api import LinkedInClient

    monkeypatch.setattr(config, "LINKEDIN_ORG_ID", "12345", raising=False)
    client = LinkedInClient("fake-token")
    result = client.create_carousel_ad(
        campaign_urn="urn:li:sponsoredCampaign:1",
        cards=_cards(tmp_path, 2),   # image_urn never set
        intro_text="hello",
    )
    assert result.status == "error"
    assert "no image URN" in (result.error_message or "")


def test_carousel_ad_reports_spec_failure_without_calling_linkedin(tmp_path, monkeypatch):
    import config
    from src.linkedin_api import LinkedInClient

    monkeypatch.setattr(config, "LINKEDIN_ORG_ID", "12345", raising=False)
    called = []
    monkeypatch.setattr("requests.post", lambda *a, **k: called.append(a) or None)
    client = LinkedInClient("fake-token")
    result = client.create_carousel_ad(
        campaign_urn="urn:li:sponsoredCampaign:1",
        cards=_cards(tmp_path, 1),   # too few
        intro_text="hello",
    )
    assert result.status == "error"
    assert "2-10 cards" in (result.error_message or "")
    assert not called, "must not touch the API when the cards fail spec"


# ── Card copy ────────────────────────────────────────────────────────────────

def test_card_copy_falls_back_to_a_narrative_within_the_limit(monkeypatch):
    """LLM unavailable — the deterministic sequence must still be usable, since
    an over-length card can't be fixed after the ad is saved."""
    monkeypatch.setattr("src.claude_client.call_claude",
                        lambda **kw: (_ for _ in ()).throw(RuntimeError("no key")))
    cards = build_card_copy(
        {"headline": "Your tax expertise, applied to AI models today and tomorrow",
         "subheadline": "Review model answers on individual returns",
         "advertised_rate": "$40/hr"},
        n_cards=4,
    )
    assert len(cards) == 4
    assert all(0 < len(c) <= CARD_HEADLINE_MAX for c in cards), cards
    assert all(not c.endswith(("-", ",", ";")) for c in cards)
    assert "$40/hr" in cards[2]


def test_card_copy_parses_llm_json(monkeypatch):
    monkeypatch.setattr(
        "src.claude_client.call_claude",
        lambda **kw: '```json\n{"cards": ["One", "Two", "Three", "Four"]}\n```',
    )
    assert build_card_copy({}, n_cards=4) == ["One", "Two", "Three", "Four"]


def test_card_copy_trims_an_overlong_llm_headline(monkeypatch):
    monkeypatch.setattr(
        "src.claude_client.call_claude",
        lambda **kw: '{"cards": ["' + "long words " * 10 + '", "b", "c", "d"]}',
    )
    cards = build_card_copy({}, n_cards=4)
    assert len(cards[0]) <= CARD_HEADLINE_MAX


def test_trimmed_headline_never_ends_on_a_dangling_word(monkeypatch):
    """"Review AI answers to 1040 questions and" reads as a bug, not a headline."""
    monkeypatch.setattr(
        "src.claude_client.call_claude",
        lambda **kw: '{"cards": ["Review AI answers to 1040 questions and filings today",'
                     ' "Built for tax pros and their clients everywhere now", "c", "d"]}',
    )
    cards = build_card_copy({}, n_cards=4)
    for c in cards:
        last = c.split()[-1].lower()
        assert last not in {"and", "or", "to", "for", "the", "your", "their"}, c


def test_card_photo_variant_keeps_the_profession_and_varies_the_frame():
    """photo_subject carries the ICP profession — creative rules require the
    subject to read as the cohort — while each slot gets its own framing."""
    base = {"photo_subject": "a certified public accountant", "headline": "orig",
            "subheadline": "keep out"}
    v1 = card_photo_variant(base, 1, "Card one")
    v2 = card_photo_variant(base, 2, "Card two")
    assert "certified public accountant" in v1["photo_subject"]
    assert "certified public accountant" in v2["photo_subject"]
    assert v1["photo_subject"] != v2["photo_subject"]
    assert v1["headline"] == "Card one"
    assert v1["subheadline"] == "", "card images carry only their own headline"
    assert base["headline"] == "orig", "must not mutate the caller's variant"


# ── Registry leak guard ──────────────────────────────────────────────────────

def test_registry_refuses_obvious_test_ramp_ids():
    """Regression: a local carousel harness wrote 3 "GMR-TEST" rows into the
    production registry AND the team's Sheet. `sheets=None` was no defence —
    log_campaign holds its own module-global SheetsClient."""
    from src.campaign_registry import log_campaign

    for rid in ("GMR-TEST", "gmr-test", "my-dummy-ramp", "SAMPLE-1", "flow"):
        with pytest.raises(ValueError, match="test fixture|test-fixture"):
            log_campaign(
                smart_ramp_id=rid, cohort_id="c", cohort_signature="s",
                geo_cluster="anglo", geo_cluster_label="US", geos=["US"],
                angle="A", campaign_type="carousel", advertised_rate="$40/hr",
            )

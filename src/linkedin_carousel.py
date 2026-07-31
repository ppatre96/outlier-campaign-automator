"""LinkedIn Carousel Ads — asset specs, validation, and card copy.

Carousel is a Sponsored Content format: 2-10 cards shown in succession, each
with its OWN image, headline and landing page. We run one carousel per angle
(A/B/C) per cohort×geo, each a 4-card narrative:

    hook → what the tasks are → pay and flexibility → how to start

Everything here is pinned to LinkedIn's published specs (verified 2026-07-30):

  Assets
    - 2 cards minimum, 10 maximum
    - 1:1 square, 1080×1080 recommended, 4320×4320 maximum, displays at 312×312
    - EVERY card must share the same aspect ratio, else LinkedIn pads or crops
      and the carousel visibly breaks mid-scroll
    - 10 MB maximum per card; JPG, PNG, or non-animated GIF
  Copy
    - card headline (the API's `media.title`): 45 characters. The API itself
      accepts <400, but the feed truncates a card headline after two lines and
      LinkedIn's own spec sheet states 45 for carousel, so 45 is the real limit
    - intro text (the post `commentary`): 255 maximum, ≤150 to avoid truncation
      on some devices
    - alt text: <4086 characters
    - destination URL: ≤2000 characters
  Campaign
    - type=SPONSORED_UPDATES with format=CAROUSEL, set AT CREATION (LinkedIn
      cannot change a campaign's format afterwards)
    - objectiveType WEBSITE_CONVERSION is allowed and REQUIRES conversion
      tracking, which create_campaign already attaches. WEBSITE_VISIT and
      LEAD_GENERATION are also valid; LEAD_GENERATION disallows the Audience
      Network
  Operational
    - once an ad is saved its carousel cards CANNOT be edited. A wrong card
      means deleting the ad and rebuilding it, which is why validate_cards is
      fail-closed and runs before anything is uploaded

Sources: LinkedIn Carousel Ads API (learn.microsoft.com, li-lms-2026-07),
LinkedIn Marketing Solutions Help a427022, and Create-and-Manage-Campaigns
(objective × format validation table).
"""
from __future__ import annotations

import logging
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

# ── Published spec constants ──────────────────────────────────────────────────
CARD_ASPECT = (1, 1)
RECOMMENDED_PX = (1080, 1080)
MAX_PX = 4320
MIN_PX = 1080                           # below this LinkedIn upscales at 312×312 display
MAX_CARD_BYTES = 10 * 1024 * 1024
ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".gif"}
INTRO_TEXT_MAX = 255
ALT_TEXT_MAX = 4086
DESTINATION_URL_MAX = 2000
CAMPAIGN_FORMAT = "CAROUSEL"

# The card-copy engine is platform-agnostic and lives in src/carousel.py, which
# Meta and Google's carousel arms share. Re-exported here so `linkedin_carousel`
# stays the one import a LinkedIn caller needs.
from src.carousel import (  # noqa: F401,E402  (re-export)
    CARD_HEADLINE_MAX,
    CARD_HEADLINE_TARGET,
    CARD_PLAN,
    DEFAULT_CARDS,
    INTRO_TEXT_SAFE,
    MAX_CARDS,
    MIN_CARDS,
    CardCopy,
    build_card_copy,
    card_photo_variant,
    clamp_intro_text,
    safe_intro_text,
)

@dataclass
class CarouselCard:
    """One card: an image on disk, its headline, and where it clicks through."""
    png_path: Path
    headline: str
    landing_page: str
    alt_text: str = ""
    image_urn: str = ""          # filled after upload

    def as_api_card(self) -> dict:
        """The Posts API `content.carousel.cards[]` shape."""
        media: dict = {"id": self.image_urn, "title": self.headline[:CARD_HEADLINE_MAX]}
        if self.alt_text:
            media["altText"] = self.alt_text[:ALT_TEXT_MAX]
        return {"media": media, "landingPage": self.landing_page[:DESTINATION_URL_MAX]}


class CarouselSpecError(ValueError):
    """Raised when cards violate LinkedIn's published carousel specs."""


def validate_cards(cards: list[CarouselCard]) -> None:
    """Fail closed BEFORE uploading anything.

    Carousel cards can't be edited once the ad is saved, so a spec violation
    that slips through costs a delete-and-rebuild rather than an edit. Checks
    card count, per-card file type / size / dimensions, the shared aspect ratio,
    headline length, and that a landing page exists.
    """
    from PIL import Image

    if len(cards) < MIN_CARDS or len(cards) > MAX_CARDS:
        raise CarouselSpecError(
            f"carousel needs {MIN_CARDS}-{MAX_CARDS} cards, got {len(cards)}"
        )

    problems: list[str] = []
    ratios: set[tuple[int, int]] = set()
    for i, card in enumerate(cards, 1):
        p = Path(card.png_path)
        if not p.exists():
            problems.append(f"card {i}: image missing at {p}")
            continue
        if p.suffix.lower() not in ALLOWED_SUFFIXES:
            problems.append(f"card {i}: {p.suffix} not one of {sorted(ALLOWED_SUFFIXES)}")
        size = p.stat().st_size
        if size > MAX_CARD_BYTES:
            problems.append(f"card {i}: {size / 1e6:.1f} MB exceeds the 10 MB limit")
        try:
            with Image.open(p) as im:
                w, h = im.size
        except Exception as exc:  # noqa: BLE001
            problems.append(f"card {i}: unreadable image ({type(exc).__name__})")
            continue
        if max(w, h) > MAX_PX:
            problems.append(f"card {i}: {w}×{h} exceeds {MAX_PX}px")
        if min(w, h) < MIN_PX:
            problems.append(f"card {i}: {w}×{h} is below the {MIN_PX}px recommendation")
        ratios.add(_reduce_ratio(w, h))
        if not card.headline.strip():
            problems.append(f"card {i}: empty headline")
        elif len(card.headline) > CARD_HEADLINE_MAX:
            problems.append(
                f"card {i}: headline is {len(card.headline)} chars, over the "
                f"{CARD_HEADLINE_MAX}-char limit ({card.headline!r})"
            )
        if not card.landing_page.strip():
            problems.append(f"card {i}: no landing page")

    if len(ratios) > 1:
        problems.append(
            f"cards mix aspect ratios {sorted(ratios)} — LinkedIn requires one "
            "ratio across every card in a carousel"
        )
    elif ratios and next(iter(ratios)) != CARD_ASPECT:
        problems.append(f"cards are {next(iter(ratios))}, carousel wants {CARD_ASPECT}")

    if problems:
        raise CarouselSpecError("; ".join(problems))


def _reduce_ratio(w: int, h: int) -> tuple[int, int]:
    from math import gcd

    g = gcd(w, h) or 1
    return (w // g, h // g)



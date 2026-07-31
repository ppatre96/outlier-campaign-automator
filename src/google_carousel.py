"""Google Demand Gen carousel — asset specs, validation, and the ad payload.

Google has no carousel in Display. Carousel is a **Demand Gen** format
(`advertising_channel_type = DEMAND_GEN`), which this codebase has never created —
`google_ads_api` has only ever built SEARCH and DISPLAY. So unlike Meta, this is a
new campaign type, not a new creative shape.

Building one carousel ad takes three asset layers, in this order:

  1. an IMAGE asset per card (`AssetService`, `image_asset`)
  2. a CARD asset per card (`asset.demand_gen_carousel_card_asset`), which wraps
     the image plus that card's headline
  3. the AD (`ad_group_ad.ad.demand_gen_carousel_ad`), which references the card
     assets IN ORDER and carries the ad-level business name, logo, headline and
     description

Field names here were read off the installed protos (google-ads 30.1.0, default
API v24) rather than taken from documentation, because two of them differ from
what the docs implied:

  - `DemandGenCarouselCardAsset` has NO description field. It is exactly
    {marketing_image_asset, square_marketing_image_asset,
     portrait_marketing_image_asset, headline, call_to_action_text} — so a card
    carries ONE line of text, and our `caption` is it.
  - `carousel_cards` entries are `AdDemandGenCarouselCardAsset`, whose only field
    is `asset` (a resource name). The cards are not inlined in the ad.

Specs (Google Ads help 13695771 + the proto):
  - 2-10 cards; ALL one ratio; 1:1 (1200×1200) or 1.91:1 (1200×628)
  - per-card headline 40 characters
  - ad-level business_name <=25, headline 40, description 90
  - logo must be 1:1, 144×144 minimum, 1200×1200 recommended
  - no Merchant Center feed required (that's Demand Gen product ads, not carousel)
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

# ── Published spec constants ──────────────────────────────────────────────────
MIN_CARDS = 2
MAX_CARDS = 10
CARD_ASPECTS = ((1, 1), (191, 100))
RECOMMENDED_PX = {(1, 1): (1200, 1200), (191, 100): (1200, 628)}
MIN_PX = 600
MAX_CARD_BYTES = 5 * 1024 * 1024
ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png"}
CARD_HEADLINE_MAX = 40
AD_HEADLINE_MAX = 40
AD_DESCRIPTION_MAX = 90
BUSINESS_NAME_MAX = 25
LOGO_ASPECT = (1, 1)
LOGO_MIN_PX = 144
CHANNEL_TYPE = "DEMAND_GEN"


@dataclass
class GoogleCarouselCard:
    """One card: an image on disk and the single line of text Google renders."""
    png_path: Path
    headline: str
    image_asset: str = ""     # resource name, filled after upload
    card_asset: str = ""      # resource name of the card asset built from it


class GoogleCarouselSpecError(ValueError):
    """Raised when cards violate Google's published Demand Gen carousel specs."""


def validate_cards(cards: list[GoogleCarouselCard]) -> None:
    """Fail closed BEFORE creating any asset.

    Google's asset layers mean a bad card leaves orphaned image and card assets
    on the account, so validating up front is cheaper than cleaning up after.
    """
    from PIL import Image

    if len(cards) < MIN_CARDS or len(cards) > MAX_CARDS:
        raise GoogleCarouselSpecError(
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
            problems.append(f"card {i}: {size / 1e6:.1f} MB exceeds the 5 MB limit")
        try:
            with Image.open(p) as im:
                w, h = im.size
        except Exception as exc:  # noqa: BLE001
            problems.append(f"card {i}: unreadable image ({type(exc).__name__})")
            continue
        if min(w, h) < MIN_PX:
            problems.append(f"card {i}: {w}×{h} is below the {MIN_PX}px floor")
        ratios.add(_reduce_ratio(w, h))
        if not card.headline.strip():
            problems.append(f"card {i}: empty headline")
        elif len(card.headline) > CARD_HEADLINE_MAX:
            problems.append(
                f"card {i}: headline is {len(card.headline)} chars, over the "
                f"{CARD_HEADLINE_MAX}-char limit ({card.headline!r})"
            )

    if len(ratios) > 1:
        problems.append(
            f"cards mix aspect ratios {sorted(ratios)} — Demand Gen requires one "
            "ratio across every card"
        )
    elif ratios and next(iter(ratios)) not in CARD_ASPECTS:
        problems.append(
            f"cards are {next(iter(ratios))}, Demand Gen carousel wants one of "
            f"{sorted(CARD_ASPECTS)}"
        )

    if problems:
        raise GoogleCarouselSpecError("; ".join(problems))


def _reduce_ratio(w: int, h: int) -> tuple[int, int]:
    from math import gcd

    g = gcd(w, h) or 1
    return (w // g, h // g)


def validate_ad_text(*, business_name: str, headline: str, description: str) -> None:
    """The ad-level fields Google requires alongside the cards."""
    problems = []
    if not business_name.strip():
        problems.append("business_name is required")
    elif len(business_name) > BUSINESS_NAME_MAX:
        problems.append(
            f"business_name is {len(business_name)} chars, over {BUSINESS_NAME_MAX}"
        )
    if not headline.strip():
        problems.append("ad headline is required")
    elif len(headline) > AD_HEADLINE_MAX:
        problems.append(f"ad headline is {len(headline)} chars, over {AD_HEADLINE_MAX}")
    if not description.strip():
        problems.append("ad description is required")
    elif len(description) > AD_DESCRIPTION_MAX:
        problems.append(
            f"ad description is {len(description)} chars, over {AD_DESCRIPTION_MAX}"
        )
    if problems:
        raise GoogleCarouselSpecError("; ".join(problems))

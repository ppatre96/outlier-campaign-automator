"""Meta Carousel Ads — asset specs, validation, and the child_attachments payload.

A Meta carousel is the SAME `object_story_spec.link_data` the single-image arm
already builds, with a `child_attachments[]` list added. Card copy comes from
`src/carousel.py`, shared with LinkedIn and Google.

Specs pinned to Meta's published limits (verified 2026-07-30):

  Assets
    - 2 cards minimum, 10 maximum (Meta recommends 3+)
    - 1:1 (1080×1080) or 4:5 (1080×1350). EVERY card must share one ratio:
      Facebook takes the FIRST card's ratio and crops the rest to match, so a
      mixed set silently loses part of every later card
    - 30 MB maximum per card; JPG or PNG
  Copy
    - primary text (`message`): ~125 characters, ~80 visible before "See more"
    - card headline (`name`): 40 characters
    - card description: ~18 characters, Facebook-only and rarely rendered — we
      leave it off rather than ship copy nobody sees
  Behaviour, and the trap
    - `multi_share_optimized` defaults to TRUE, which lets Meta REORDER the cards
      by predicted performance. Our cards are a narrative read in order (hook →
      tasks → payment → how to start), so reordering turns it into nonsense. It
      MUST be set False.
    - `multi_share_end_card` also defaults to True, appending a page-profile end
      card after the last one. Our card 4 IS the call to action, so we turn it
      off — otherwise the ask is buried one swipe deeper.

Sources: Meta Marketing API "Carousel Ads" + Ad Creative reference (v21.0),
Meta Business Help 1442840814389736 (asset specs).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path

log = logging.getLogger(__name__)

# ── Published spec constants ──────────────────────────────────────────────────
MIN_CARDS = 2
MAX_CARDS = 10
CARD_ASPECTS = ((1, 1), (4, 5))
RECOMMENDED_PX = {(1, 1): (1080, 1080), (4, 5): (1080, 1350)}
MIN_PX = 600                              # Meta's floor for feed placements
MAX_CARD_BYTES = 30 * 1024 * 1024
ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png"}
CARD_NAME_MAX = 40                        # card headline
PRIMARY_TEXT_MAX = 125
DESTINATION_URL_MAX = 2000


@dataclass
class MetaCarouselCard:
    """One card: an image on disk, its headline, and where it clicks through."""
    png_path: Path
    headline: str
    landing_page: str
    image_hash: str = ""          # filled after upload

    def as_child_attachment(self, *, default_link: str, cta_type: str) -> dict:
        """The `link_data.child_attachments[]` shape.

        `link` and one of image_hash/picture are REQUIRED per card. The per-card
        `call_to_action` repeats the ad-level CTA so every card is clickable to
        its own URL rather than falling back to the parent link.
        """
        link = self.landing_page[:DESTINATION_URL_MAX] or default_link
        return {
            "link": link,
            "image_hash": self.image_hash,
            "name": self.headline[:CARD_NAME_MAX],
            "call_to_action": {"type": cta_type, "value": {"link": link}},
        }


class MetaCarouselSpecError(ValueError):
    """Raised when cards violate Meta's published carousel specs."""


def validate_cards(cards: list[MetaCarouselCard]) -> None:
    """Fail closed BEFORE uploading anything.

    Same reasoning as the LinkedIn arm: a saved carousel's cards can't be edited,
    so a spec violation costs a delete-and-rebuild. Checks card count, per-card
    file type / size / dimensions, and the shared aspect ratio — mixed ratios are
    the dangerous one because Meta crops rather than rejects, so nothing surfaces
    until a human looks at the ad.
    """
    from PIL import Image

    if len(cards) < MIN_CARDS or len(cards) > MAX_CARDS:
        raise MetaCarouselSpecError(
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
            problems.append(f"card {i}: {size / 1e6:.1f} MB exceeds the 30 MB limit")
        try:
            with Image.open(p) as im:
                w, h = im.size
        except Exception as exc:  # noqa: BLE001
            problems.append(f"card {i}: unreadable image ({type(exc).__name__})")
            continue
        if min(w, h) < MIN_PX:
            problems.append(f"card {i}: {w}×{h} is below Meta's {MIN_PX}px floor")
        ratios.add(_reduce_ratio(w, h))
        if not card.headline.strip():
            problems.append(f"card {i}: empty headline")
        elif len(card.headline) > CARD_NAME_MAX:
            problems.append(
                f"card {i}: headline is {len(card.headline)} chars, over the "
                f"{CARD_NAME_MAX}-char limit ({card.headline!r})"
            )
        if not card.landing_page.strip():
            problems.append(f"card {i}: no landing page")

    if len(ratios) > 1:
        problems.append(
            f"cards mix aspect ratios {sorted(ratios)} — Facebook crops every card "
            "to the FIRST card's ratio, so this silently mangles the set"
        )
    elif ratios and next(iter(ratios)) not in CARD_ASPECTS:
        problems.append(
            f"cards are {next(iter(ratios))}, Meta carousel wants one of "
            f"{sorted(CARD_ASPECTS)}"
        )

    if problems:
        raise MetaCarouselSpecError("; ".join(problems))


def _reduce_ratio(w: int, h: int) -> tuple[int, int]:
    from math import gcd

    g = gcd(w, h) or 1
    return (w // g, h // g)


def build_link_data(
    cards: list[MetaCarouselCard],
    *,
    primary_text: str,
    default_link: str,
    cta_type: str,
) -> dict:
    """The `link_data` for a carousel creative, cards in OUR order.

    `multi_share_optimized=False` is the load-bearing line: left at its default
    Meta reorders the cards by predicted performance and the narrative breaks.
    """
    return {
        "link": default_link,
        "message": (primary_text or "")[:PRIMARY_TEXT_MAX],
        "child_attachments": [
            c.as_child_attachment(default_link=default_link, cta_type=cta_type)
            for c in cards
        ],
        # Keep OUR order: the cards are one argument read start to finish.
        "multi_share_optimized": False,
        # Card 4 is the ask; an appended page-profile card would bury it.
        "multi_share_end_card": False,
    }

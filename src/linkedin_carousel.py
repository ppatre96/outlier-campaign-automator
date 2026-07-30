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
MIN_CARDS = 2
MAX_CARDS = 10
DEFAULT_CARDS = 4                       # our narrative length
CARD_ASPECT = (1, 1)
RECOMMENDED_PX = (1080, 1080)
MAX_PX = 4320
MIN_PX = 1080                           # below this LinkedIn upscales at 312×312 display
MAX_CARD_BYTES = 10 * 1024 * 1024
ALLOWED_SUFFIXES = {".jpg", ".jpeg", ".png", ".gif"}
CARD_HEADLINE_MAX = 45
# What we actually write to. LinkedIn's cap is 45, but our overlay template
# wraps a headline past ~40 chars onto a third line and a card is displayed at
# 312×312, where three lines is unreadable. So aim at 40 and keep 45 as the
# hard spec ceiling validate_cards enforces.
CARD_HEADLINE_TARGET = 40
INTRO_TEXT_SAFE = 150
INTRO_TEXT_MAX = 255
ALT_TEXT_MAX = 4086
DESTINATION_URL_MAX = 2000
CAMPAIGN_FORMAT = "CAROUSEL"

# The 4-card narrative. `key` selects which copy field seeds the fallback, and
# `intent` is what the card has to accomplish — both are handed to the LLM and
# used by the deterministic fallback.
# Fallback captions — the line LinkedIn renders under each card. Deliberately
# complementary to the overlay, never a restatement, and free of banned
# vocabulary ("work", "schedule") and task counts.
_FALLBACK_CAPTIONS = {
    1: "Your field, applied to AI models",
    2: "Judge accuracy in your own domain",
    3: "Paid per task, hours that suit you",
    4: "Screening takes a few minutes",
}

CARD_PLAN: list[dict] = [
    {"slot": 1, "intent": "hook — name who they are and the shift to AI work",
     "fallback_key": "headline"},
    {"slot": 2, "intent": "what the tasks actually are, concretely",
     "fallback_key": "subheadline"},
    {"slot": 3, "intent": "payment and flexibility, with the real rate if known",
     "fallback_key": "advertised_rate"},
    {"slot": 4, "intent": "how to start — a plain next step",
     "fallback_key": "cta"},
]


@dataclass
class CardCopy:
    """The two strings one card needs: the line rendered ON the image, and the
    caption LinkedIn renders UNDER it (the API's media.title). They display
    together, so they must not say the same thing twice."""
    overlay: str
    caption: str


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


def clamp_intro_text(text: str) -> str:
    """Trim the post commentary to the safe length on a word boundary.

    255 is the hard cap but some devices truncate at ~150, and a headline cut
    mid-word is worse than a shorter one.
    """
    t = re.sub(r"\s+", " ", (text or "")).strip()
    if len(t) <= INTRO_TEXT_SAFE:
        return t
    cut = t[:INTRO_TEXT_SAFE]
    if " " in cut:
        cut = cut[: cut.rindex(" ")]
    return cut.rstrip(" ,;:—-")


# ── Card copy ────────────────────────────────────────────────────────────────

_CARD_COPY_SYSTEM = """\
You write LinkedIn carousel ad cards for Outlier, which pays professionals to
help train AI models in their own field of expertise.

A carousel is read in order by swiping, so the cards must form ONE sequence,
not four restatements of the same hook. You will be given the angle's existing
ad copy and the intent of each card.

Each card needs TWO different strings:
  "overlay" — the line rendered ON the card image
  "caption" — the line LinkedIn shows UNDERNEATH that same card

They appear together, so a reader sees both at once. The caption must NEVER
repeat or paraphrase the overlay; it carries the NEXT piece of information.

Return a single JSON object:

{"cards": [{"overlay": "...", "caption": "..."}, ...]}

HARD rules:
- Each "overlay" is AT MOST 40 characters and 6 words. Longer text wraps onto a
  third line and a card is shown at 312x312, where that is unreadable.
- Each "caption" is AT MOST 45 characters, and must differ from its overlay.
- COUNT THE CHARACTERS before answering. An overlay over 40 characters or 6
  words, or a caption over 45 characters, is DISCARDED and replaced with generic
  copy — so a shorter complete line always beats a longer one.
- NEVER state a number of tasks anywhere. A contributor doesn't care that a
  project has N tasks and won't be doing them all. "paid per task" is fine.
- One idea per card, and each card must advance the story from the previous.
- Plain sentence case. No em dashes, no hashtags, no ALL CAPS, no emoji.
- At most 6 words per headline. The overlay renders 2 lines; 7 words wraps to 3
  and collides with the subject.
- NEVER use any of these words (Outlier's banned vocabulary, checked in code and
  rejected): {banned}. Say instead: opportunity, task, project guidelines,
  strongly encouraged, reward, payment, screening, match, flexible hours.
- Use the real rate when one is supplied. Never invent a number or a claim.
- Card 4 is the ask. Keep it a concrete next step, not hype.
"""


def build_card_copy(
    copy_variant: dict,
    *,
    n_cards: int = DEFAULT_CARDS,
    advertised_rate: str = "",
    cohort_label: str = "",
) -> list[CardCopy]:
    """Return `n_cards` CardCopy pairs forming one narrative for this angle.

    Tries the LLM, falls back to a deterministic build from the angle's existing
    copy fields. Never raises and never returns an over-length headline — the
    fallback matters because an unusable card can't be fixed after the ad is
    saved.
    """
    plan = CARD_PLAN[:n_cards]
    fallback = _fallback_cards(copy_variant, plan, advertised_rate)
    try:
        from src.claude_client import call_claude
        from src.copy_design_qc import _BANNED_VOCABULARY

        rate = advertised_rate or copy_variant.get("advertised_rate", "") or ""
        user_msg = (
            f"Cohort: {cohort_label or '(unspecified)'}\n"
            f"Angle headline: {copy_variant.get('headline', '')}\n"
            f"Angle subheadline: {copy_variant.get('subheadline', '')}\n"
            f"Ad intro text: {copy_variant.get('intro_text', '')}\n"
            f"Ad headline: {copy_variant.get('ad_headline', '')}\n"
            f"Ad description: {copy_variant.get('ad_description', '')}\n"
            f"Real tasking rate: {rate or '(none given — do not invent one)'}\n\n"
            f"Write exactly {len(plan)} cards with these intents:\n"
            + "\n".join(f"  {c['slot']}. {c['intent']}" for c in plan)
            + "\n\nReturn the JSON now."
        )
        raw = call_claude(
            messages=[{"role": "user", "content": user_msg}],
            # replace, NOT .format — this prompt contains literal JSON braces
            # ({"cards": [...]}) and .format() raises KeyError on them, which
            # silently forced every card set onto the fallback.
            system=_CARD_COPY_SYSTEM.replace("{banned}", ", ".join(_BANNED_VOCABULARY[:24])),
            cache_system=True,
            max_tokens=600,
        )
        cards = _parse_cards(raw, len(plan))
        if cards:
            out: list[CardCopy] = []
            for i, pair in enumerate(cards):
                fb = fallback[i] if i < len(fallback) else CardCopy("", "")
                raw_overlay = (pair.get("overlay") or "").strip()
                raw_caption = (pair.get("caption") or "").strip()
                overlay = _fit_headline(raw_overlay)
                caption = _fit_caption(raw_caption)
                # Trimming an over-length line leaves a fragment ("...and get
                # paid per"), which reads worse than a shorter complete line. If
                # the model blew the limit, take the fallback instead of the
                # fragment.
                if len(raw_caption) > CARD_HEADLINE_MAX:
                    log.warning("build_card_copy: card %d caption was %d chars — using the "
                                "fallback rather than a truncated fragment (%r)",
                                i + 1, len(raw_caption), raw_caption[:60])
                    caption = fb.caption
                if len(raw_overlay) > CARD_HEADLINE_TARGET or len(raw_overlay.split()) > 6:
                    log.warning("build_card_copy: card %d overlay was %d chars / %d words — "
                                "using the fallback rather than a truncated fragment (%r)",
                                i + 1, len(raw_overlay), len(raw_overlay.split()), raw_overlay[:60])
                    overlay = fb.overlay
                # Substitute rather than ship: the copy checks downstream do not
                # gate the carousel (regen cannot fix copy), so anything banned
                # here would reach a live ad.
                # The overlay is scanned AS a headline so the "no Outlier
                # wordmark in image text" rule applies to it; the caption isn't
                # on the image, so it's scanned as a caption and may name the
                # brand. Getting this backwards silently drops either the brand
                # protection or every good caption.
                for label, field, val, fbv in (
                    ("overlay", "headline", overlay, fb.overlay),
                    ("caption", "caption", caption, fb.caption),
                ):
                    bad = _banned_violations(val, field=field)
                    if bad or not val:
                        log.warning("build_card_copy: card %d %s rejected (%s) — using fallback",
                                    i + 1, label, "; ".join(bad)[:160] or "empty")
                        if label == "overlay":
                            overlay = fbv
                        else:
                            caption = fbv
                # The caption renders directly under the overlay, so a duplicate
                # reads as a rendering bug.
                if _same_text(overlay, caption):
                    log.warning("build_card_copy: card %d caption duplicates the overlay "
                                "(%r) — using fallback caption", i + 1, caption)
                    caption = fb.caption if not _same_text(overlay, fb.caption) else ""
                if overlay:
                    out.append(CardCopy(overlay=overlay, caption=caption))
            if out:
                return out
        log.warning("build_card_copy: could not parse LLM cards — using fallback. Raw: %s",
                    (raw or "")[:200])
    except Exception as exc:  # noqa: BLE001
        log.warning("build_card_copy: LLM call failed (%s) — using fallback", exc)

    return fallback


def _parse_cards(raw: str, want: int) -> list[dict]:
    import json

    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", (raw or "").strip()).strip()
    for candidate in (cleaned, _first_json_object(cleaned)):
        if not candidate:
            continue
        try:
            obj = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        cards = obj.get("cards") if isinstance(obj, dict) else obj
        if isinstance(cards, list):
            out: list[dict] = []
            for c in cards:
                if isinstance(c, dict):
                    ov, cap = str(c.get("overlay", "")).strip(), str(c.get("caption", "")).strip()
                elif str(c).strip():
                    ov, cap = str(c).strip(), ""   # tolerate the older flat shape
                else:
                    continue
                if ov:
                    out.append({"overlay": ov, "caption": cap})
            if len(out) >= want:
                return out[:want]
    return []


def _first_json_object(s: str) -> str:
    m = re.search(r"\{.*\}", s or "", re.S)
    return m.group(0) if m else ""


def _fit_headline(text: str) -> str:
    """Trim to the overlay's budget: 40 chars AND 6 words.

    Chars alone weren't enough — "Apply and get matched to a project" is 34 chars
    but 7 words, and the compositor wrapped it to 3 lines, which then collided
    with the subject and burned three QC regens on a live run.
    """
    from src.copy_design_qc import HEADLINE_MAX_WORDS

    t = re.sub(r"\s+", " ", (text or "")).strip().rstrip(".")
    words = t.split()
    if len(words) > HEADLINE_MAX_WORDS:
        t = " ".join(words[:HEADLINE_MAX_WORDS])
    if len(t) > CARD_HEADLINE_TARGET:
        t = t[:CARD_HEADLINE_TARGET]
        if " " in t:
            t = t[: t.rindex(" ")]
    return _drop_dangling_word(t.rstrip(" ,;:—-"))


def _fit_caption(text: str) -> str:
    """Trim a caption to LinkedIn's 45-char media.title limit on a word boundary."""
    t = re.sub(r"\s+", " ", (text or "")).strip().rstrip(".")
    if len(t) <= CARD_HEADLINE_MAX:
        return t
    cut = t[:CARD_HEADLINE_MAX]
    if " " in cut:
        cut = cut[: cut.rindex(" ")]
    return _drop_dangling_word(cut.rstrip(" ,;:—-"))


def _same_text(a: str, b: str) -> bool:
    """Loose equality — case, punctuation and spacing don't rescue a duplicate."""
    norm = lambda s: re.sub(r"[^a-z0-9 ]", "", (s or "").lower()).strip()
    return bool(norm(a)) and norm(a) == norm(b)


def _banned_violations(text: str, field: str = "headline") -> list[str]:
    """Outlier's banned-vocabulary scan, from the ONE canonical list in
    copy_design_qc. `field` matters: scan_brand_voice additionally bans the word
    "Outlier" in headline/subheadline because the wordmark is composited on the
    image — a card CAPTION isn't on the image, so it passes "caption" and may
    name the brand — a hand-copied subset in the prompt above is what let
    "Earn $40/hr, work when you want" reach a live campaign."""
    try:
        from src.copy_design_qc import scan_brand_voice

        return scan_brand_voice(text, field)
    except Exception:  # noqa: BLE001 — never block card copy on the scanner
        return []


# Words that read as broken when truncation leaves them at the end
# ("Review AI answers to 1040 questions and").
_DANGLING = {
    "and", "or", "but", "so", "to", "for", "with", "from", "into", "onto",
    "of", "in", "on", "at", "by", "as", "than", "that", "the", "a", "an",
    "your", "their", "our", "its", "this", "these", "plus", "while", "when",
}


def _drop_dangling_word(text: str) -> str:
    words = text.split()
    while len(words) > 1 and words[-1].lower().strip(",;:") in _DANGLING:
        words.pop()
    return " ".join(words).rstrip(" ,;:—-")


def _fallback_cards(copy_variant: dict, plan: list[dict], advertised_rate: str) -> list[CardCopy]:
    """Deterministic narrative from the copy fields we already have."""
    rate = advertised_rate or copy_variant.get("advertised_rate", "") or ""
    seeds = {
        "headline":        copy_variant.get("headline", "") or "Your expertise, applied to AI",
        "subheadline":     copy_variant.get("subheadline", "") or copy_variant.get("ad_description", "")
                           or "Review and improve AI answers in your field",
        # NB: "schedule" and "work" are both banned vocabulary — keep this
        # fallback clean, it is what ships when the LLM is unavailable.
        "advertised_rate": (f"Earn {rate}, flexible hours" if rate
                            else "Flexible hours, paid per task"),
        "cta":             "See if you qualify",
    }
    return [
        CardCopy(overlay=_fit_headline(seeds.get(c["fallback_key"], "")),
                 caption=_fit_caption(_FALLBACK_CAPTIONS.get(c["slot"], "")))
        for c in plan
    ]


# ── Card photo prompts ───────────────────────────────────────────────────────

# Per-slot photo direction, so four cards in one carousel don't look like the
# same frame four times. Appended to the angle's own photo_subject.
# Rules learned from a live run where card 2 failed QC five times and the whole
# carousel was (correctly) skipped:
#   - never describe screen CONTENT ("reviewing text on screen") — Gemini renders
#     literal text, and QC bans any text baked into the photo. It also invented
#     lettering on a lapel pin when pushed toward detail.
#   - keep the subject centred with clear headroom; the headline overlays the top
#     of the card, and QC fails when hair touches the text box.
_CARD_PHOTO_HINT = {
    1: "a confident head-and-shoulders portrait at their workplace, centred, looking at camera, clear space above the head",
    2: "seated at a desk with a closed notebook and pen, hands resting on the desk, centred, clear space above the head",
    3: "relaxed at a home desk in natural window light, centred, clear space above the head",
    4: "standing by a window with a calm, satisfied expression, wider framing, centred, clear space above the head",
}


def card_photo_variant(copy_variant: dict, slot: int, headline: str) -> dict:
    """A per-card copy of the variant dict, so each card gets its own photo.

    Keeps the angle's `photo_subject` (which carries the ICP profession — the
    subject must read as the cohort, see the creative rules) and appends the
    slot's framing direction. The headline is swapped in so the compositor
    renders THIS card's text.
    """
    v = dict(copy_variant)
    subject = (v.get("photo_subject") or "").strip()
    hint = _CARD_PHOTO_HINT.get(slot, "")
    v["photo_subject"] = f"{subject}, {hint}".strip(", ") if subject else hint
    v["headline"] = headline
    # The card image carries only its headline. A subheadline on a 312×312
    # display card is unreadable, and the narrative already lives across cards.
    v["subheadline"] = ""
    return v

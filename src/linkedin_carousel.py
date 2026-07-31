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
    3: "Hours that suit you, fully remote",
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


def safe_intro_text(intro: str, *, cohort_name: str = "", rate: str = "") -> str:
    """The post commentary, guaranteed to clear the brand-voice scan.

    The intro sits above the cards in the feed, so it's the most-read text on
    the ad — but unlike the card copy it arrives verbatim from the static
    variant, and nothing in this arm used to check it. Cards get scanned in
    build_card_copy; this closes the same hole one level up.
    """
    text = clamp_intro_text(intro)
    if text:
        bad = _banned_violations(text, "intro_text")
        if not bad:
            return text
        log.warning("safe_intro_text: intro rejected (%s) — using fallback",
                    "; ".join(bad)[:160])
    who = (cohort_name or "").strip() or "professionals"
    out = f"Outlier matches {who} with AI projects that need their expertise."
    if rate:
        out += f" Current tasking rate: {rate}."
    return clamp_intro_text(out)


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
- Each "caption" is AT MOST 40 characters, and must differ from its overlay.
- COUNT THE CHARACTERS before answering. An over-length line is DISCARDED and
  replaced with generic copy, so a shorter complete line always beats a longer
  one. Aim several characters under the cap: the usual failure is a caption that
  misses by two or three and loses copy that was otherwise good.
- An "overlay" contains NO digits at all. The pay rate is already painted into a
  band at the bottom of every card, so an overlay that names it prints the same
  sentence twice on one image. Card 3 is about payment and flexibility, so talk
  about being paid per task and choosing your own hours, NOT the number.
- A "caption" may name the rate, since it sits off the image. Nothing else
  numeric: no task counts, and no form or document numbers either — "Review AI
  answers on 1040s" scans as a quantity ("1040 answers"), not as a tax form. Say
  "individual returns" instead. A contributor doesn't care how many tasks exist
  and won't be doing them all.
- One idea per card, and each card must advance the story from the previous.
- The overlay must NOT contain the word "Outlier". The wordmark is composited
  onto the card, so naming the brand in the overlay prints it twice. The caption
  is not on the image and MAY name the brand — card 4 is the natural place.
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
        system = _CARD_COPY_SYSTEM.replace("{banned}", ", ".join(_BANNED_VOCABULARY[:24]))
        messages = [{"role": "user", "content": user_msg}]
        best: list[CardCopy] = []
        best_problems: list[str] = []
        # Two attempts. The model routinely lands a caption 2-3 chars over the
        # cap, and swapping that for a generic fallback throws away good copy for
        # nothing — naming the offending strings back to it fixes most of them in
        # one pass. One extra Claude call is trivial next to four Gemini images.
        for attempt in (1, 2):
            raw = call_claude(
                messages=messages,
                # replace, NOT .format — this prompt contains literal JSON braces
                # ({"cards": [...]}) and .format() raises KeyError on them, which
                # silently forced every card set onto the fallback.
                system=system,
                cache_system=True,
                max_tokens=600,
            )
            pairs = _parse_cards(raw, len(plan))
            if not pairs:
                log.warning("build_card_copy: could not parse LLM cards (attempt %d). Raw: %s",
                            attempt, (raw or "")[:200])
                break
            cards, problems = _vet_cards(pairs, fallback)
            if not best or len(problems) < len(best_problems):
                best, best_problems = cards, problems
            if not problems or attempt == 2:
                break
            log.info("build_card_copy: %d card problem(s) on attempt 1 — asking for a repair",
                     len(problems))
            messages = messages + [
                {"role": "assistant", "content": raw},
                {"role": "user", "content":
                    "These are not shippable:\n" + "\n".join(f"  - {p}" for p in problems)
                    + "\n\nReturn the SAME JSON shape with every card rewritten to fix its "
                      "problem. Keep the cards that were fine as they are. Count the "
                      "characters this time."},
            ]
        if best:
            if best_problems:
                log.warning("build_card_copy: %d card(s) still fell back after the repair pass: %s",
                            len(best_problems), "; ".join(best_problems)[:300])
            return best
    except Exception as exc:  # noqa: BLE001
        log.warning("build_card_copy: LLM call failed (%s) — using fallback", exc)

    return fallback


def _vet_cards(
    pairs: list[dict], fallback: list[CardCopy]
) -> tuple[list[CardCopy], list[str]]:
    """Validate one LLM card set, substituting the fallback where it fails.

    Returns the shippable cards plus a problem line per substitution, phrased for
    the model so a repair attempt knows exactly what to change.
    """
    out: list[CardCopy] = []
    problems: list[str] = []
    for i, pair in enumerate(pairs):
        fb = fallback[i] if i < len(fallback) else CardCopy("", "")
        raw_overlay = (pair.get("overlay") or "").strip()
        raw_caption = (pair.get("caption") or "").strip()
        overlay = _fit_headline(raw_overlay)
        caption = _fit_caption(raw_caption)
        # Trimming an over-length line leaves a fragment ("...and get paid per"),
        # which reads worse than a shorter complete line. If the model blew the
        # limit, take the fallback instead of the fragment.
        if len(raw_caption) > CARD_HEADLINE_MAX:
            problems.append(f"card {i + 1} caption is {len(raw_caption)} chars, "
                            f"max {CARD_HEADLINE_MAX}: {raw_caption!r}")
            caption = fb.caption
        if len(raw_overlay) > CARD_HEADLINE_TARGET or len(raw_overlay.split()) > 6:
            problems.append(f"card {i + 1} overlay is {len(raw_overlay)} chars / "
                            f"{len(raw_overlay.split())} words, max "
                            f"{CARD_HEADLINE_TARGET} chars and 6 words: {raw_overlay!r}")
            overlay = fb.overlay
        # Substitute rather than ship: the copy checks downstream do not gate the
        # carousel (regen cannot fix copy), so anything banned here would reach a
        # live ad.
        # The overlay is scanned AS a headline so the "no Outlier wordmark in
        # image text" rule applies to it; the caption isn't on the image, so it's
        # scanned as a caption and may name the brand. Getting this backwards
        # silently drops either the brand protection or every good caption.
        for label, field, val, fbv in (
            ("overlay", "headline", overlay, fb.overlay),
            ("caption", "caption", caption, fb.caption),
        ):
            bad = _banned_violations(val, field=field)
            if not bad and label == "overlay" and re.search(r"\d", val or ""):
                # The rate is composited into a band on every card, so an overlay
                # naming it duplicates the band; any other number reads as a
                # quantity. Either way an overlay has no use for a digit.
                bad = [f"contains a number, but the card already shows the rate in "
                       f"its bottom band: {val!r}"]
            elif not bad and _has_bare_number(val):
                bad = [f"contains a number that isn't the pay rate, which reads as a "
                       f"quantity: {val!r}"]
            if bad or not val:
                problems.append(f"card {i + 1} {label} {'; '.join(bad)[:200] or 'is empty'}")
                if label == "overlay":
                    overlay = fbv
                else:
                    caption = fbv
        # The caption renders directly under the overlay, so a duplicate reads as
        # a rendering bug.
        if _same_text(overlay, caption):
            problems.append(f"card {i + 1} caption repeats its overlay ({caption!r}); the "
                            "caption must carry the next piece of information")
            caption = fb.caption if not _same_text(overlay, fb.caption) else ""
        if overlay:
            out.append(CardCopy(overlay=overlay, caption=caption))
    return out, problems


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


# A rate is the ONLY number that belongs on a card.
_CURRENCY_RE = re.compile(r"[$£€]\s?\d[\d,.]*(?:\s*/\s*(?:hr|hour|day|week|mo|month))?", re.I)


def _has_bare_number(text: str) -> bool:
    """True if the text carries a number that isn't the pay rate.

    A digit next to a noun reads as a QUANTITY, whatever we meant by it:
    "Review AI answers on 1040s" scans as "1040 answers", not "the 1040 tax
    form" (Pranav, 2026-07-31). Cards have no legitimate use for a number other
    than the rate, so anything else is rejected and the fallback ships.
    """
    return bool(re.search(r"\d", _CURRENCY_RE.sub("", text or "")))


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
    def _clean(text: str, generic: str) -> str:
        """Seeds come from the angle's own copy, which may itself carry a number
        ("Review how models answer individual 1040 questions"). The fallback is
        what ships when everything else is rejected, so it has to be clean."""
        return generic if (not text or _has_bare_number(text)) else text

    seeds = {
        "headline":        _clean(copy_variant.get("headline", ""), "Your expertise, applied to AI"),
        "subheadline":     _clean(copy_variant.get("subheadline", "") or copy_variant.get("ad_description", ""),
                                  "Review and improve AI answers in your field"),
        # No rate here even when we know it: compose_ad_for_platform paints the
        # rate into a band at the bottom of EVERY card, so an overlay that
        # repeats it renders the same sentence twice on one image.
        # NB: "schedule" and "work" are both banned vocabulary — keep this
        # fallback clean, it is what ships when the LLM is unavailable.
        "advertised_rate": "Paid for every task you finish",
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

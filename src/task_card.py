"""
Task grounding for ad copy (2026-06-17).

The copy generators historically worked from the cohort NAME + statistical
signals only — they never saw what the contributor actually does. That produced
vague copy ("evaluate and rate AI-generated content") and, worse, confidently
WRONG copy: for the GMR-0024 BLV ramp (Android/TalkBack screen-recording task)
the model invented "JAWS / NVDA" — desktop screen readers that have nothing to
do with the actual mobile task — because it had to guess.

This module extracts a small, structured **TaskCard** ONCE per cohort from the
real task sources (the landing page + the Smart Ramp job-post / summary /
cohort description) and renders a prompt block the generators inject. The block
carries a hard no-invention rule: only reference tools/devices/workflows present
in the grounding. When grounding is too thin (cold-start ramps), the card is
marked `is_thin` and the block tells the model to stay general rather than
fabricate specifics.

Build once per (cohort × ramp); thread into build_inmail_variants + build_briefs
so InMail, LinkedIn static, Meta, Google, and Reddit all ground on the same facts
(the non-LinkedIn channels reshape the brief-generated canonical copy).
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass

import requests

from src.claude_client import call_claude

log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
        "(KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

# Cap the scraped LP text fed to the extractor — enough for the task section,
# not the whole marketing site.
_LP_TEXT_MAX = 4000
# Minimum grounding length below which we don't even call the extractor — there
# isn't enough to extract a concrete task from, so stay general (is_thin).
_MIN_GROUNDING_CHARS = 40


@dataclass
class TaskCard:
    """Concrete, grounded facts about what a contributor actually does.

    Every field is the empty string when the grounding didn't state it (the
    extractor is told NOT to invent). `is_thin` is True when we couldn't ground
    a concrete task → generators stay general instead of fabricating specifics.
    """
    what_you_do: str = ""
    device_or_tool: str = ""
    output_artifact: str = ""
    time_per_task: str = ""
    who_its_for: str = ""
    is_thin: bool = True
    source: str = "none"   # "lp+smart_ramp" | "smart_ramp" | "none"


def scrape_lp_text(url: str | None, *, timeout: int = 15) -> str:
    """Best-effort plain-text extraction of a landing page. Returns "" on any
    failure (never raises — task-card build degrades to Smart-Ramp fields)."""
    if not url or not str(url).strip().lower().startswith("http"):
        return ""
    try:
        from bs4 import BeautifulSoup
        resp = requests.get(url.strip(), headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        soup = BeautifulSoup(resp.text, "html.parser")
        for tag in soup(["script", "style", "noscript", "svg", "header", "footer", "nav"]):
            tag.decompose()
        text = soup.get_text(separator=" ")
        text = re.sub(r"\s+", " ", text).strip()
        return text[:_LP_TEXT_MAX]
    except Exception as exc:
        log.warning("scrape_lp_text(%s) failed: %s", url, exc)
        return ""


_EXTRACT_PROMPT = """\
You are extracting a factual task card for an Outlier recruitment ad. Below is the
real context for a contributor opportunity (landing page text + the internal
request). Extract ONLY facts that are explicitly stated in the context.

HARD RULE: Do NOT infer, guess, or invent. If the context does not state a field,
return an empty string for it. Never name a tool, device, app, or task step that
is not in the context. (Example failure to avoid: writing "JAWS/NVDA" when the
context says the task uses Android TalkBack.)

Extract these fields:
- what_you_do:     the concrete action the contributor performs (one clause)
- device_or_tool:  the specific device/app/tool used, only if stated (else "")
- output_artifact: what they produce/submit, only if stated (else "")
- time_per_task:   time per task or session, only if stated (else "")
- who_its_for:     who the audience is, in plain words

CONTEXT:
{context}

Return ONLY this JSON (no markdown, no commentary):
{{"what_you_do": "...", "device_or_tool": "...", "output_artifact": "...", "time_per_task": "...", "who_its_for": "..."}}"""


def build_task_card(
    *,
    lp_url: str | None = None,
    job_post_name: str = "",
    ramp_summary: str = "",
    cohort_description: str = "",
    lp_text: str | None = None,
) -> TaskCard:
    """Build a TaskCard from the LP + Smart Ramp fields. One LLM call (skipped
    when grounding is too thin). Never raises — returns an is_thin card on any
    failure so copy generation continues (general, not fabricated).

    `lp_text` may be passed pre-scraped (tests / caching); otherwise lp_url is
    scraped here.
    """
    if lp_text is None:
        lp_text = scrape_lp_text(lp_url)

    parts = [
        ("Job post title", (job_post_name or "").strip()),
        ("Internal request summary", (ramp_summary or "").strip()),
        ("Cohort description", (cohort_description or "").strip()),
        ("Landing page", (lp_text or "").strip()),
    ]
    context = "\n\n".join(f"{label}: {val}" for label, val in parts if val)
    source = "lp+smart_ramp" if (lp_text or "").strip() else ("smart_ramp" if context else "none")

    if len(context) < _MIN_GROUNDING_CHARS:
        log.info("build_task_card: grounding too thin (%d chars) — staying general", len(context))
        return TaskCard(is_thin=True, source=source)

    try:
        raw = call_claude(
            messages=[{"role": "user", "content": _EXTRACT_PROMPT.format(context=context[:6000])}],
            max_tokens=400,
        )
        out = _extract_json(raw) or {}
    except Exception as exc:
        log.warning("build_task_card LLM failed (%s) — staying general", exc)
        return TaskCard(is_thin=True, source=source)

    card = TaskCard(
        what_you_do=str(out.get("what_you_do") or "").strip(),
        device_or_tool=str(out.get("device_or_tool") or "").strip(),
        output_artifact=str(out.get("output_artifact") or "").strip(),
        time_per_task=str(out.get("time_per_task") or "").strip(),
        who_its_for=str(out.get("who_its_for") or "").strip(),
        source=source,
    )
    # Thin unless we got a concrete action AND at least one supporting specific.
    card.is_thin = not (card.what_you_do and (card.device_or_tool or card.output_artifact))
    log.info(
        "build_task_card: source=%s is_thin=%s what_you_do=%r device=%r artifact=%r",
        card.source, card.is_thin, card.what_you_do[:60], card.device_or_tool[:40],
        card.output_artifact[:40],
    )
    return card


def task_card_prompt_block(card: TaskCard | None) -> str:
    """Render a prompt block grounding copy in the task card, with a hard
    no-invention rule. Returns "" when card is None (callers degrade to prior
    behavior). When the card is thin, instructs the model to stay general."""
    if card is None:
        return ""
    if card.is_thin:
        return (
            "\n\n## TASK GROUNDING — stay general (no concrete task facts available)\n"
            "We do not have specific task details for this audience. Describe the work "
            "generally and honestly: flexible, remote AI training / evaluation tasks paid "
            "hourly. DO NOT invent specific tools, devices, apps, software, or task steps "
            "(e.g. never name a screen reader, platform, or workflow that wasn't given to you).\n"
        )
    lines = ["\n\n## TASK GROUNDING — use THESE facts, invent nothing else"]
    if card.what_you_do:
        lines.append(f"- What they actually do: {card.what_you_do}")
    if card.device_or_tool:
        lines.append(f"- Device / tool (use this exact one): {card.device_or_tool}")
    if card.output_artifact:
        lines.append(f"- What they produce: {card.output_artifact}")
    if card.time_per_task:
        lines.append(f"- Time per task: {card.time_per_task}")
    if card.who_its_for:
        lines.append(f"- Who it's for: {card.who_its_for}")
    lines.append(
        "Ground the concrete task line in these facts. Do NOT introduce any tool, "
        "device, app, or task step not listed above — if it's not here, leave it out."
    )
    return "\n".join(lines)


# ── Per-process cache ──────────────────────────────────────────────────────────
# The task is defined at the (ramp × Smart-Ramp-cohort) level, but the generators
# fire per mined-cohort × geo × angle × channel. Build the card ONCE per row and
# let every downstream generator read it by (ramp_id, cohort_id) — one LLM call +
# one LP fetch per row instead of per ad. Warmed in _process_row_both_modes;
# read at the InMail + Phase-2 copy call sites. Per-process (launch and its copy
# generators share one process).
_CARD_CACHE: dict[tuple, TaskCard] = {}


def warm_task_card(
    ramp_id: str | None,
    cohort_id: str | None,
    *,
    lp_url: str | None = None,
    job_post_name: str = "",
    ramp_summary: str = "",
    cohort_description: str = "",
) -> TaskCard:
    """Build (once) + cache the task card for a row. Returns the cached card on
    repeat calls with the same key. Key is (ramp_id, cohort_id)."""
    key = (ramp_id or "", cohort_id or "")
    if key in _CARD_CACHE:
        return _CARD_CACHE[key]
    card = build_task_card(
        lp_url=lp_url, job_post_name=job_post_name,
        ramp_summary=ramp_summary, cohort_description=cohort_description,
    )
    _CARD_CACHE[key] = card
    return card


def cached_card(ramp_id: str | None, cohort_id: str | None) -> TaskCard | None:
    """Read the cached card for (ramp_id, cohort_id). None on miss → callers
    degrade to prior (ungrounded) behavior."""
    return _CARD_CACHE.get((ramp_id or "", cohort_id or ""))


def _extract_json(raw: str) -> dict:
    """Lenient JSON extraction (mirrors copy_adapter._extract_json)."""
    if not raw:
        return {}
    s = raw.strip()
    if s.startswith("```"):
        s = re.sub(r"^```(?:json)?", "", s).rsplit("```", 1)[0].strip()
    try:
        return json.loads(s)
    except Exception:
        m = re.search(r"\{.*\}", s, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except Exception:
                return {}
    return {}

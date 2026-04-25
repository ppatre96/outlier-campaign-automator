"""
Sentiment Miner - Phase 2.5 V2 (FEED-17, FEED-18, FEED-19)

Weekly pull of contributor feedback from public review sources (Reddit, Trustpilot,
Glassdoor, Outlier Community) and internal support channels (Zendesk, Intercom).
Extracts themed issues via LiteLLM gateway and writes data/sentiment_callouts.json
as a scored directive feed for ad-creative-brief-generator.

NOTE: Apple App Store and Google Play Store sources are NOT implemented because
Outlier operates entirely in-browser - no native mobile app exists (verified
2026-04-24). If a mobile app launches, add fetchers here.
"""
from __future__ import annotations

import json
import logging
import re
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests
from bs4 import BeautifulSoup
from openai import OpenAI

import config

log = logging.getLogger(__name__)

_HEADERS = {
    "User-Agent": "outlier-campaign-agent/0.2 (sentiment_miner; +https://outlier.ai)",
    "Accept-Language": "en-US,en;q=0.9",
}

_CALLOUTS_PATH = Path("data/sentiment_callouts.json")
_RAW_DIR       = Path("data/sentiment_raw")

# Banned -> approved vocabulary (from CLAUDE.md). Applied as regex scrub in LLM output.
_VOCAB_SCRUB: list[tuple[re.Pattern, str]] = [
    (re.compile(r"\brequired\b",          re.IGNORECASE), "strongly encouraged"),
    (re.compile(r"\bjob(s)?\b",           re.IGNORECASE), "task"),
    (re.compile(r"\brole(s)?\b",          re.IGNORECASE), "opportunity"),
    (re.compile(r"\bposition(s)?\b",      re.IGNORECASE), "opportunity"),
    (re.compile(r"\btraining\b",          re.IGNORECASE), "project guidelines"),
    (re.compile(r"\bproject rate\b",      re.IGNORECASE), "current tasking rate"),
    (re.compile(r"\bbonus(es)?\b",        re.IGNORECASE), "reward"),
    (re.compile(r"\bassign(ed|ment)?\b",  re.IGNORECASE), "match"),
    (re.compile(r"\bcompensation\b",      re.IGNORECASE), "payment"),
    (re.compile(r"\bperformance\b",       re.IGNORECASE), "progress"),
    (re.compile(r"\binterview(s|ing)?\b", re.IGNORECASE), "screening"),
    (re.compile(r"\bpromote(d|s)?\b",     re.IGNORECASE), "eligible to work on review-level tasks"),
]


def _scrub_vocab(text: str) -> str:
    """Defense-in-depth: replace banned tokens with approved substitutions."""
    if not isinstance(text, str):
        return text
    for pattern, replacement in _VOCAB_SCRUB:
        text = pattern.sub(replacement, text)
    return text


# ── HTTP helper ───────────────────────────────────────────────────────────────

def _get(url: str, timeout: int = 15, auth=None,
         extra_headers: dict | None = None) -> Optional[requests.Response]:
    """Cloned + extended from src/competitor_intel.py:320-327.
    Returns None on any error after logging a warning. Never raises.
    """
    headers = {**_HEADERS, **(extra_headers or {})}
    try:
        resp = requests.get(url, headers=headers, timeout=timeout, auth=auth)
        resp.raise_for_status()
        return resp
    except Exception as e:
        log.warning("GET %s failed: %s", url, e)
        return None


# ── Public source fetchers ────────────────────────────────────────────────────

def fetch_reddit(subs: list[str] | None = None, limit: int = 100) -> list[dict]:
    """Fetch recent posts from configured subreddits via the public JSON endpoint.

    One sub failing does NOT block the others. Returns a flat list of normalized
    snippet dicts: {source, url, title, body, ts}.
    """
    if subs is None:
        subs = config.SENTIMENT_REDDIT_SUBS
    out: list[dict] = []
    for sub in subs:
        url = f"https://www.reddit.com/r/{sub}/new.json?limit={limit}"
        resp = _get(url)
        if resp is None:
            log.info("reddit r/%s: skipped (HTTP error)", sub)
            continue
        try:
            children = resp.json().get("data", {}).get("children", [])
        except Exception as e:
            log.warning("reddit r/%s: JSON parse failed: %s", sub, e)
            continue
        count = 0
        for child in children:
            data = child.get("data", {})
            permalink = data.get("permalink", "")
            out.append({
                "source": f"reddit_{sub.lower()}",
                "url":    f"https://reddit.com{permalink}",
                "title":  data.get("title", "") or "",
                "body":   data.get("selftext", "") or "",
                "ts":     data.get("created_utc"),
            })
            count += 1
        log.info("reddit r/%s: fetched %d posts", sub, count)
    return out


def fetch_trustpilot(url: str = "https://www.trustpilot.com/review/outlier.ai") -> list[dict]:
    """Scrape recent reviews from Trustpilot. Returns [] on error or block."""
    resp = _get(url)
    if resp is None:
        return []
    try:
        soup = BeautifulSoup(resp.text, "html.parser")
    except Exception as e:
        log.warning("trustpilot: BS4 parse failed: %s", e)
        return []

    out: list[dict] = []
    # Try modern review card selector first; fall back to legacy text-typography.
    cards = soup.select("article[data-service-review-card-paper]")
    if not cards:
        cards = soup.find_all("article")

    for card in cards[:50]:
        try:
            title_el = card.find(["h2", "h3"]) or card.find("a", attrs={"data-review-title-typography": True})
            body_el  = card.find("p", attrs={"data-service-review-text-typography": True})
            if not body_el:
                body_el = card.find("p")
            time_el  = card.find("time")
            link_el  = card.find("a", href=re.compile(r"/reviews/"))

            title_text = title_el.get_text(strip=True) if title_el else ""
            body_text  = body_el.get_text(" ", strip=True) if body_el else ""
            ts         = time_el.get("datetime") if time_el and time_el.has_attr("datetime") else None
            href       = link_el.get("href") if link_el else None
            review_url = (
                f"https://www.trustpilot.com{href}" if href and href.startswith("/")
                else (href or url)
            )
            if not (title_text or body_text):
                continue
            out.append({
                "source": "trustpilot",
                "url":    review_url,
                "title":  title_text,
                "body":   body_text,
                "ts":     ts,
            })
        except Exception as e:
            log.warning("trustpilot: per-review parse failed (skipped): %s", e)
            continue

    log.info("trustpilot: fetched %d reviews", len(out))
    return out


def fetch_glassdoor(url: str = "https://www.glassdoor.com/Reviews/Outlier-Reviews-E8756349.htm") -> list[dict]:
    """Scrape Glassdoor reviews; gracefully degrade on 403 / login wall."""
    resp = _get(url)
    if resp is None:
        log.info("glassdoor: blocked (HTTP error or 403/login wall)")
        return []
    # Detect login wall heuristically (Glassdoor anti-scrape often returns a sign-in HTML on 200)
    text = resp.text or ""
    if resp.status_code == 403 or "Sign In" in text[:5000] and "review" not in text[:5000].lower():
        log.info("glassdoor: blocked (403/login wall)")
        return []

    try:
        soup = BeautifulSoup(text, "html.parser")
    except Exception as e:
        log.warning("glassdoor: BS4 parse failed: %s", e)
        return []

    out: list[dict] = []
    review_blocks = soup.select("[data-test='employer-review']") or soup.find_all("li", class_=re.compile(r"empReview", re.IGNORECASE))
    for block in review_blocks[:50]:
        try:
            title_el = block.find(["h2", "h3"]) or block.find("a", class_=re.compile(r"reviewLink", re.IGNORECASE))
            body_el  = block.find("p")
            time_el  = block.find("time")
            link_el  = block.find("a", href=re.compile(r"/Reviews/"))
            title_text = title_el.get_text(strip=True) if title_el else ""
            body_text  = body_el.get_text(" ", strip=True) if body_el else ""
            ts         = time_el.get("datetime") if time_el and time_el.has_attr("datetime") else None
            href       = link_el.get("href") if link_el else None
            review_url = (
                f"https://www.glassdoor.com{href}" if href and href.startswith("/")
                else (href or url)
            )
            if not (title_text or body_text):
                continue
            out.append({
                "source": "glassdoor",
                "url":    review_url,
                "title":  title_text,
                "body":   body_text,
                "ts":     ts,
            })
        except Exception as e:
            log.warning("glassdoor: per-review parse failed (skipped): %s", e)
            continue

    log.info("glassdoor: fetched %d reviews", len(out))
    return out


def fetch_discourse(url: str = "https://community.outlier.ai/latest.json") -> list[dict]:
    """Outlier Community forum (Discourse). Skip silently if host unreachable."""
    resp = _get(url)
    if resp is None:
        log.info("discourse: host unreachable - skip")
        return []
    try:
        payload = resp.json()
    except Exception as e:
        log.warning("discourse: JSON parse failed: %s", e)
        return []

    topics = (payload.get("topic_list", {}) or {}).get("topics", []) or []
    out: list[dict] = []
    for topic in topics[:100]:
        slug   = topic.get("slug")
        tid    = topic.get("id")
        topic_url = f"https://community.outlier.ai/t/{slug}/{tid}" if slug and tid else url
        out.append({
            "source": "discourse",
            "url":    topic_url,
            "title":  topic.get("title", "") or "",
            "body":   topic.get("excerpt", "") or "",
            "ts":     topic.get("created_at"),
        })
    log.info("discourse: fetched %d topics", len(out))
    return out


# ── Internal source fetchers (credential-gated) ───────────────────────────────

def fetch_zendesk() -> list[dict]:
    """Zendesk Search API (HTTP Basic). Returns [] when creds empty.

    PII rule: persists only id, subject, truncated description, created_at -
    NEVER requester name/email.
    """
    if (
        config.ZENDESK_SUBDOMAIN == ""
        or config.ZENDESK_EMAIL == ""
        or config.ZENDESK_API_TOKEN == ""
    ):
        log.warning("zendesk: credentials missing - skip")
        return []

    cutoff = (datetime.now(timezone.utc)
              - timedelta(days=config.SENTIMENT_LOOKBACK_DAYS)).strftime("%Y-%m-%d")
    query = f"type:ticket tags:contributor created>{cutoff}"
    url = (
        f"https://{config.ZENDESK_SUBDOMAIN}.zendesk.com/api/v2/search.json"
        f"?query={requests.utils.quote(query)}"
    )
    auth = (f"{config.ZENDESK_EMAIL}/token", config.ZENDESK_API_TOKEN)

    resp = _get(url, auth=auth, timeout=30)
    if resp is None:
        return []
    try:
        results = resp.json().get("results", []) or []
    except Exception as e:
        log.warning("zendesk: JSON parse failed: %s", e)
        return []

    out: list[dict] = []
    for t in results[:100]:
        tid = t.get("id")
        if tid is None:
            continue
        out.append({
            "source": "zendesk",
            "url":    f"https://{config.ZENDESK_SUBDOMAIN}.zendesk.com/agent/tickets/{tid}",
            "title":  t.get("subject", "") or "",
            "body":   (t.get("description", "") or "")[:800],
            "ts":     t.get("created_at"),
        })
    log.info("zendesk: fetched %d tickets", len(out))
    return out


def fetch_intercom() -> list[dict]:
    """Intercom Conversations search (Bearer auth). Returns [] when token empty.

    PII rule: persists only conversation id, subject, truncated body, created_at -
    NEVER requester name/email.
    """
    if config.INTERCOM_ACCESS_TOKEN == "":
        log.warning("intercom: access token missing - skip")
        return []

    cutoff_epoch = int((datetime.now(timezone.utc)
                        - timedelta(days=config.SENTIMENT_LOOKBACK_DAYS)).timestamp())
    body = {
        "query": {"field": "created_at", "operator": ">", "value": cutoff_epoch},
        "pagination": {"per_page": 50},
    }
    headers = {
        "Authorization": f"Bearer {config.INTERCOM_ACCESS_TOKEN}",
        "Accept":        "application/json",
        "Content-Type":  "application/json",
        **_HEADERS,
    }
    url = "https://api.intercom.io/conversations/search"
    try:
        resp = requests.post(url, headers=headers, json=body, timeout=15)
        resp.raise_for_status()
    except Exception as e:
        log.warning("intercom: POST %s failed: %s", url, e)
        return []

    try:
        conversations = resp.json().get("conversations", []) or []
    except Exception as e:
        log.warning("intercom: JSON parse failed: %s", e)
        return []

    out: list[dict] = []
    for c in conversations:
        cid = c.get("id")
        if cid is None:
            continue
        src = c.get("source", {}) or {}
        out.append({
            "source": "intercom",
            "url":    f"https://app.intercom.com/a/apps/_/conversations/{cid}",
            "title":  src.get("subject", "") or "",
            "body":   ((src.get("body", "") or ""))[:800],
            "ts":     c.get("created_at"),
        })
    log.info("intercom: fetched %d conversations", len(out))
    return out


# ── Theme extraction (LiteLLM) ────────────────────────────────────────────────

# IMPORTANT: This system prompt embeds the CLAUDE.md vocabulary table verbatim
# so the LLM rewrites contributor quotes into approved Outlier vocabulary when
# generating theme labels. Raw evidence_quotes stay verbatim from the input.
# The "NEVER X" rule lines deliberately mention banned tokens; that is OK
# because the production code paths apply _scrub_vocab() before writing JSON.
_THEME_SYSTEM_PROMPT = """\
You extract issue themes from contributor reviews and support tickets for Outlier,
an AI-training platform. You receive a JSON array of text snippets (each with
`i` index + `text`). Return ONLY a JSON array of theme objects - no prose,
no code fences, no markdown.

Each theme object has keys:
  theme             - short label (<=6 words). MUST use Outlier approved vocabulary:
                        - "payment" (NEVER "compensation")
                        - "task" or "opportunity" (NEVER "job", "role", "position")
                        - "screening" (NEVER "interview")
                        - "Outlier Community" (NEVER "Discourse" in user-facing label)
                        - "progress" (NEVER "performance")
                        - "current tasking rate" (NEVER "project rate")
                        - "reward" (NEVER "bonus")
                        - "match" (NEVER "assign")
                        - "strongly encouraged" (NEVER "required")
                        - "project guidelines" (NEVER "instructions" / "training")
                        - "eligible to work on review-level tasks" (NEVER "promote")
  sentiment         - "positive" | "negative" | "neutral"
  evidence_quotes   - list of 1-3 verbatim substrings from inputs supporting the theme
  source_indices    - list of input snippet indices (0-based) contributing to this theme

Rules:
  - Each input snippet should map to at most 2 themes.
  - Omit themes backed by fewer than 2 evidence_quotes.
  - Do NOT generate themes about PII (contributor names, emails, payment account numbers).
  - Evidence quotes are verbatim from the input - do not paraphrase.
  - Theme labels must be paraphrased to approved vocabulary.
"""


def _is_model_not_found(exc: Exception) -> bool:
    msg = str(exc).lower()
    return "model" in msg and (
        "not found" in msg or "invalid" in msg or "unknown" in msg
    )


def extract_themes(snippets: list[dict],
                   model: str = "anthropic/claude-haiku-4-5") -> list[dict]:
    """Send snippets to LiteLLM for theme extraction.

    On any failure (network, JSON parse, empty response) returns []. On
    "model not found" exception, retries once with config.LITELLM_MODEL.
    """
    if not snippets:
        return []
    user_payload = json.dumps([
        {"i": i, "text": ((s.get("title", "") or "") + " "
                           + (s.get("body", "") or "")).strip()[:1500]}
        for i, s in enumerate(snippets)
    ])

    client = OpenAI(base_url=config.LITELLM_BASE_URL, api_key=config.LITELLM_API_KEY)

    def _call(_model: str) -> str:
        resp = client.chat.completions.create(
            model=_model,
            max_tokens=2048,
            messages=[
                {"role": "system", "content": _THEME_SYSTEM_PROMPT},
                {"role": "user",   "content": user_payload},
            ],
        )
        return (resp.choices[0].message.content or "").strip()

    raw: str = ""
    try:
        raw = _call(model)
    except Exception as e:
        if _is_model_not_found(e):
            log.warning("extract_themes: model %s not found, retrying with %s",
                        model, config.LITELLM_MODEL)
            try:
                raw = _call(config.LITELLM_MODEL)
            except Exception as e2:
                log.warning("extract_themes: fallback model failed: %s", e2)
                return []
        else:
            log.warning("extract_themes: LLM call failed: %s", e)
            return []

    if not raw:
        return []
    if raw.startswith("```"):
        try:
            raw = "\n".join(raw.split("\n")[1:-1])
        except Exception:
            pass

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as e:
        log.warning("extract_themes: JSON decode failed: %s; raw=%r", e, raw[:200])
        return []
    if not isinstance(parsed, list):
        log.warning("extract_themes: expected list, got %s", type(parsed).__name__)
        return []

    # Defense-in-depth vocabulary scrub on every theme label.
    out: list[dict] = []
    for item in parsed:
        if not isinstance(item, dict):
            continue
        scrubbed = dict(item)
        if "theme" in scrubbed and isinstance(scrubbed["theme"], str):
            scrubbed["theme"] = _scrub_vocab(scrubbed["theme"])
        out.append(scrubbed)
    return out


# ── Directive shaping + classifier ────────────────────────────────────────────

_AVOID_KEYWORDS = ("confus", "unclear", "broken", "removed", "deprecated")


def _directive_for_theme(theme: dict) -> str:
    """Map (sentiment, theme label) -> 'Address: ...' / 'Lean into: ...' / 'Avoid: ...'."""
    label = theme.get("theme", "") or ""
    sentiment = (theme.get("sentiment") or "").lower()
    label_low = label.lower()

    # 'Avoid' wins when the theme talks about a removed/confusing/broken angle.
    if any(k in label_low for k in _AVOID_KEYWORDS):
        return _scrub_vocab(f"Avoid: {label}")
    if sentiment == "positive":
        return _scrub_vocab(f"Lean into: {label}")
    # Default for negative / neutral sentiment is to address the issue head-on.
    return _scrub_vocab(f"Address: {label}")


def _truncate_record_for_raw(record: dict, body_chars: int = 400) -> dict:
    """Trim body to body_chars for the raw on-disk dump (extra PII safety)."""
    return {
        "source": record.get("source"),
        "url":    record.get("url"),
        "title":  record.get("title", ""),
        "body":   (record.get("body", "") or "")[:body_chars],
        "ts":     record.get("ts"),
    }


# ── Public entrypoint ─────────────────────────────────────────────────────────

def run(sources: list[str] | None = None) -> dict:
    """Run the weekly sentiment fetch + theme extraction.

    Writes data/sentiment_callouts.json (themes >= SENTIMENT_THEME_MIN_EVIDENCE)
    and data/sentiment_raw/<yyyy-mm-dd>.json (full normalized records + sub-
    threshold themes for postmortem). Returns a small status dict.
    """
    if sources is None:
        sources = ["reddit", "trustpilot", "glassdoor", "discourse",
                   "zendesk", "intercom"]

    fetcher_map = {
        "reddit":     fetch_reddit,
        "trustpilot": fetch_trustpilot,
        "glassdoor":  fetch_glassdoor,
        "discourse":  fetch_discourse,
        "zendesk":    fetch_zendesk,
        "intercom":   fetch_intercom,
    }

    sources_queried: list[str] = []
    sources_skipped: list[dict] = []
    all_snippets: list[dict] = []

    for src in sources:
        fetcher = fetcher_map.get(src)
        if fetcher is None:
            sources_skipped.append({"source": src, "reason": "unknown_source"})
            continue
        try:
            records = fetcher() or []
        except Exception as e:
            log.warning("source %s failed: %s", src, e)
            sources_skipped.append({"source": src, "reason": f"exception: {e!s}"[:200]})
            continue
        sources_queried.append(src)
        log.info("source %s: %d records", src, len(records))
        all_snippets.extend(records)

    # Theme extraction (handles empty input gracefully).
    themes = extract_themes(all_snippets) if all_snippets else []

    # Enrich every theme with evidence_count, source_urls, directive_for_brief.
    enriched: list[dict] = []
    for theme in themes:
        evidence_quotes = list(theme.get("evidence_quotes", []) or [])
        evidence_count = len(evidence_quotes)
        source_indices = [i for i in (theme.get("source_indices", []) or [])
                          if isinstance(i, int) and 0 <= i < len(all_snippets)]
        source_urls = sorted({all_snippets[i].get("url", "") for i in source_indices
                              if all_snippets[i].get("url")})
        directive = _directive_for_theme(theme)
        enriched.append({
            "theme":               _scrub_vocab(theme.get("theme", "") or ""),
            "sentiment":           theme.get("sentiment", "neutral"),
            "evidence_count":      evidence_count,
            "evidence_quotes":     evidence_quotes,
            "source_urls":         source_urls,
            "directive_for_brief": directive,
        })

    threshold = config.SENTIMENT_THEME_MIN_EVIDENCE
    surfaced = [t for t in enriched if t["evidence_count"] >= threshold]
    raw_only = [t for t in enriched if t["evidence_count"] < threshold]

    generated_at = datetime.now(timezone.utc).isoformat()
    callouts_payload = {
        "generated_at":    generated_at,
        "sources_queried": sources_queried,
        "sources_skipped": sources_skipped,
        "themes":          surfaced,
    }

    # Ensure output dirs exist.
    Path("data").mkdir(parents=True, exist_ok=True)
    _CALLOUTS_PATH.parent.mkdir(parents=True, exist_ok=True)
    _RAW_DIR.mkdir(parents=True, exist_ok=True)

    _CALLOUTS_PATH.write_text(json.dumps(callouts_payload, indent=2, ensure_ascii=False))
    log.info("wrote %s with %d surfaced themes", _CALLOUTS_PATH, len(surfaced))

    raw_path = _RAW_DIR / f"{datetime.now(timezone.utc).strftime('%Y-%m-%d')}.json"
    raw_payload = {
        "generated_at":     generated_at,
        "sources_queried":  sources_queried,
        "sources_skipped":  sources_skipped,
        "snippets":         [_truncate_record_for_raw(s) for s in all_snippets],
        "themes_below_threshold": raw_only,
    }
    raw_path.write_text(json.dumps(raw_payload, indent=2, ensure_ascii=False))
    log.info("wrote %s with %d snippets, %d sub-threshold themes",
             raw_path, len(all_snippets), len(raw_only))

    return {
        "callouts_path":    str(_CALLOUTS_PATH),
        "raw_path":         str(raw_path),
        "themes_surfaced":  len(surfaced),
        "sources_queried":  sources_queried,
        "sources_skipped":  sources_skipped,
    }


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    result = run()
    print(json.dumps(result, indent=2, ensure_ascii=False))

"""
Competitor intelligence module for the Outlier campaign pipeline.

Researches:
  1. Competitor ad creatives — Meta Ads Library, LinkedIn
  2. Site traffic signals — SimilarWeb/Semrush public data
  3. User reviews — Reddit, Trustpilot, YouTube, App stores
  4. SEO search intent — high-intent queries, autocomplete patterns

Outputs a structured CompetitorIntel object that is passed to the brief
generator to sharpen angle selection, hooks, and proof elements.
"""

import logging
import re
import time
from dataclasses import dataclass, field
from typing import Optional

import requests
from bs4 import BeautifulSoup

import config

log = logging.getLogger(__name__)

# ── Competitors ───────────────────────────────────────────────────────────────

COMPETITORS = {
    "dataannotation": {
        "name": "DataAnnotation",
        "domain": "dataannotation.tech",
        "meta_search": "DataAnnotation",
        "trustpilot_slug": "dataannotation.tech",
        "reddit_terms": ["dataannotation", "data annotation tech"],
    },
    "mercor": {
        "name": "Mercor",
        "domain": "mercor.com",
        "meta_search": "Mercor AI",
        "trustpilot_slug": "mercor.com",
        "reddit_terms": ["mercor", "mercor.com"],
    },
    "alignerr": {
        "name": "Alignerr",
        "domain": "alignerr.com",
        "meta_search": "Alignerr",
        "trustpilot_slug": "alignerr.com",
        "reddit_terms": ["alignerr"],
    },
    "micro1": {
        "name": "Micro1",
        "domain": "micro1.ai",
        "meta_search": "Micro1 AI",
        "trustpilot_slug": "micro1.ai",
        "reddit_terms": ["micro1", "micro1.ai"],
    },
    "appen": {
        "name": "Appen",
        "domain": "appen.com",
        "meta_search": "Appen",
        "trustpilot_slug": "appen.com",
        "reddit_terms": ["appen", "appen.com"],
    },
}

OUTLIER_INTEL = {
    "name": "Outlier",
    "domain": "outlier.ai",
    "trustpilot_slug": "outlier.ai",
    "reddit_terms": ["outlier.ai", "outlier ai", "outlierapp"],
}

# ── Data classes ──────────────────────────────────────────────────────────────

@dataclass
class AdCreative:
    competitor: str
    hook: str                        # first ~10 words
    body: str                        # full ad text
    cta: str
    format: str                      # static / video / carousel
    earnings_claim: Optional[str]    # exact figure if present
    profession_mention: Optional[str]
    angle: str                       # A / B / C (inferred)
    source: str                      # meta / linkedin
    url: Optional[str] = None


@dataclass
class ReviewSignal:
    source: str          # reddit / trustpilot / youtube / appstore
    platform: str        # competitor name or "outlier"
    sentiment: str       # positive / negative / neutral
    theme: str           # payment / availability / community / trust / earnings / ux
    quote: str           # exact text
    url: Optional[str] = None


@dataclass
class CompetitorIntel:
    competitor_ads: list[AdCreative] = field(default_factory=list)
    review_signals: list[ReviewSignal] = field(default_factory=list)
    trustpilot_ratings: dict = field(default_factory=dict)   # {competitor: {"rating": 4.1, "count": 1200}}
    search_terms: list[str] = field(default_factory=list)
    dominant_competitor_angle: Optional[str] = None          # A/B/C most used by competitors
    whitespace_angle: Optional[str] = None                   # A/B/C least used = opportunity
    top_user_pain_points: list[str] = field(default_factory=list)
    outlier_praise_themes: list[str] = field(default_factory=list)
    outlier_complaint_themes: list[str] = field(default_factory=list)
    differentiators: list[str] = field(default_factory=list)
    copy_recommendations: list[str] = field(default_factory=list)
    design_recommendations: list[str] = field(default_factory=list)


# ── Helpers ───────────────────────────────────────────────────────────────────

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
}

def _get(url: str, timeout: int = 15) -> Optional[requests.Response]:
    try:
        resp = requests.get(url, headers=_HEADERS, timeout=timeout)
        resp.raise_for_status()
        return resp
    except Exception as e:
        log.warning("GET %s failed: %s", url, e)
        return None


def _infer_angle(text: str) -> str:
    """Infer copy angle A/B/C from ad text."""
    t = text.lower()
    earnings_signals = [r"\$\d", "paid out", "earn ", "payment", "income", "salary",
                        "per hour", "per week", "how much", "money"]
    flexibility_signals = ["from home", "anywhere", "schedule", "flexible", "your time",
                           "9-5", "remote", "whenever", "on your terms"]
    expertise_signals = ["expertise", "experience", "skills", "domain", "background",
                         "between projects", "ai training", "put your"]

    scores = {"B": 0, "C": 0, "A": 0}
    for sig in earnings_signals:
        if re.search(sig, t): scores["B"] += 1
    for sig in flexibility_signals:
        if re.search(sig, t): scores["C"] += 1
    for sig in expertise_signals:
        if re.search(sig, t): scores["A"] += 1

    return max(scores, key=scores.get)


def _extract_earnings_claim(text: str) -> Optional[str]:
    match = re.search(
        r'\$[\d,]+(?:\.\d+)?(?:\s*[Kk])?\s*(?:/hr|per hour|per week|hourly|weekly|paid|USD)?',
        text
    )
    return match.group().strip() if match else None


# ── Meta Ads Library ──────────────────────────────────────────────────────────

def fetch_meta_ads(competitor_key: str) -> list[AdCreative]:
    """
    Fetch active ads from Meta Ads Library for a competitor.
    Uses the public search URL — no auth required for public ads.
    """
    comp = COMPETITORS.get(competitor_key, {})
    if not comp:
        return []

    search_term = comp["meta_search"]
    url = (
        f"https://www.facebook.com/ads/library/?"
        f"active_status=active&ad_type=all&country=ALL"
        f"&q={requests.utils.quote(search_term)}&search_type=keyword_unordered"
    )

    log.info("Fetching Meta ads for %s: %s", comp["name"], url)

    # Meta Ads Library requires JS rendering — return URL for Claude agent to browse
    # When run via the Claude agent (competitor-bot), it will use WebFetch on this URL
    return [AdCreative(
        competitor=comp["name"],
        hook=f"[Fetch via browser: {url}]",
        body="",
        cta="",
        format="unknown",
        earnings_claim=None,
        profession_mention=None,
        angle="?",
        source="meta",
        url=url,
    )]


# ── Trustpilot ────────────────────────────────────────────────────────────────

def fetch_trustpilot(slug: str, platform_name: str) -> dict:
    """
    Scrape Trustpilot public page for rating + recent review themes.
    Returns: {rating, review_count, positive_themes, negative_themes, sample_quotes}
    """
    url = f"https://www.trustpilot.com/review/{slug}"
    resp = _get(url)
    if not resp:
        return {}

    soup = BeautifulSoup(resp.text, "html.parser")
    result = {"platform": platform_name, "url": url}

    # Rating
    rating_el = soup.find("span", {"data-rating-typography": True})
    if not rating_el:
        rating_el = soup.find("p", class_=re.compile(r"typography_heading"))
    if rating_el:
        try:
            result["rating"] = float(rating_el.get_text(strip=True))
        except ValueError:
            pass

    # Review count
    count_el = soup.find("span", text=re.compile(r"\d[\d,]+ total"))
    if not count_el:
        count_el = soup.find("p", text=re.compile(r"\d[\d,]+ reviews?"))
    if count_el:
        numbers = re.findall(r"[\d,]+", count_el.get_text())
        if numbers:
            result["review_count"] = int(numbers[0].replace(",", ""))

    # Recent review quotes
    review_cards = soup.find_all("p", {"data-service-review-text-typography": True})
    if not review_cards:
        review_cards = soup.find_all("p", class_=re.compile(r"typography_body"))

    quotes = [card.get_text(strip=True) for card in review_cards[:8] if card.get_text(strip=True)]
    result["sample_quotes"] = quotes

    log.info("Trustpilot %s: rating=%s reviews=%s quotes=%d",
             platform_name, result.get("rating"), result.get("review_count"), len(quotes))
    return result


# ── Reddit ────────────────────────────────────────────────────────────────────

def fetch_reddit_signals(search_term: str, platform_name: str) -> list[ReviewSignal]:
    """
    Search Reddit for discussions about a platform.
    Uses the public Reddit search JSON endpoint — no auth required.
    """
    signals = []
    url = (
        f"https://www.reddit.com/search.json?"
        f"q={requests.utils.quote(search_term)}&sort=relevance&limit=25&t=year"
    )
    resp = _get(url)
    if not resp:
        return signals

    try:
        posts = resp.json().get("data", {}).get("children", [])
    except Exception:
        return signals

    for post in posts:
        data = post.get("data", {})
        title = data.get("title", "")
        selftext = data.get("selftext", "")
        text = f"{title} {selftext}".strip()
        if not text or len(text) < 20:
            continue

        # Classify sentiment and theme
        neg_words = ["scam", "fraud", "bad", "terrible", "awful", "hate",
                     "payment issue", "didn't pay", "no work", "waste", "avoid"]
        pos_words = ["great", "love", "good", "excellent", "paid", "recommend",
                     "worth it", "legit", "worked well", "happy"]
        sentiment = "neutral"
        if any(w in text.lower() for w in neg_words): sentiment = "negative"
        elif any(w in text.lower() for w in pos_words): sentiment = "positive"

        theme_map = {
            "payment": ["pay", "paid", "payment", "money", "earn", "income"],
            "availability": ["no work", "tasks", "projects", "available", "waitlist"],
            "trust": ["legit", "scam", "real", "fake", "legitimate", "trustworthy"],
            "community": ["community", "discord", "forum", "support", "help"],
            "earnings": ["how much", "rate", "per hour", "$/hr", "weekly"],
            "ux": ["app", "platform", "interface", "ux", "difficult", "easy"],
        }
        theme = "general"
        for t, keywords in theme_map.items():
            if any(k in text.lower() for k in keywords):
                theme = t
                break

        signals.append(ReviewSignal(
            source="reddit",
            platform=platform_name,
            sentiment=sentiment,
            theme=theme,
            quote=title[:200],
            url=f"https://reddit.com{data.get('permalink', '')}",
        ))

    log.info("Reddit %s: %d signals for '%s'", platform_name, len(signals), search_term)
    return signals


# ── SEO search terms ──────────────────────────────────────────────────────────

def fetch_search_intent_terms(tg_label: str) -> list[str]:
    """
    Derive high-intent search terms for a given TG.
    Returns terms that should be embedded in copy for SEO/retargeting alignment.
    """
    base_terms = [
        f"{tg_label} AI training jobs",
        f"{tg_label} work from home",
        f"{tg_label} side income",
        f"outlier AI {tg_label}",
        f"data annotation {tg_label}",
        "is outlier AI legit",
        "outlier AI review",
        "AI training side hustle",
        "earn money reviewing AI",
        "remote AI work for experts",
        f"how much does outlier pay {tg_label}",
    ]

    # Try to pull Google autocomplete suggestions
    autocomplete_terms = []
    for seed in [f"outlier ai {tg_label}", f"AI training jobs {tg_label}"]:
        url = (
            f"https://suggestqueries.google.com/complete/search?"
            f"client=firefox&q={requests.utils.quote(seed)}"
        )
        resp = _get(url)
        if resp:
            try:
                suggestions = resp.json()[1]
                autocomplete_terms.extend(suggestions[:5])
            except Exception:
                pass
        time.sleep(0.5)

    return list(dict.fromkeys(base_terms + autocomplete_terms))  # dedup, preserve order


# ── Angle analysis ────────────────────────────────────────────────────────────

def analyze_angle_distribution(ads: list[AdCreative]) -> dict:
    """
    Given a list of competitor ads, return angle distribution and identify whitespace.
    """
    counts = {"A": 0, "B": 0, "C": 0, "?": 0}
    for ad in ads:
        counts[ad.angle] = counts.get(ad.angle, 0) + 1

    total = sum(v for k, v in counts.items() if k != "?")
    if total == 0:
        return {"dominant": None, "whitespace": None, "distribution": counts}

    dominant = max(["A", "B", "C"], key=lambda x: counts[x])
    whitespace = min(["A", "B", "C"], key=lambda x: counts[x])
    return {
        "dominant": dominant,
        "whitespace": whitespace,
        "distribution": {k: f"{counts[k]/total*100:.0f}%" for k in ["A", "B", "C"]},
    }


# ── Differentiator scoring ────────────────────────────────────────────────────

OUTLIER_DIFFERENTIATORS = [
    {"claim": "$500M+ paid out to contributors", "angle": "B", "uniqueness": "high"},
    {"claim": "Scale AI enterprise backing — the same infrastructure powering GPT-4 labeling", "angle": "A", "uniqueness": "high"},
    {"claim": "4.1/5 Trustpilot rating — earned, not manufactured", "angle": "B", "uniqueness": "medium"},
    {"claim": "Access to frontier model training tasks (GPT-5 class, Claude Sonnet)", "angle": "A", "uniqueness": "high"},
    {"claim": "Outlier Community — active Discord, no competitor has this at scale", "angle": "C", "uniqueness": "high"},
    {"claim": "Weekly payment — not monthly, not upon request", "angle": "B", "uniqueness": "medium"},
    {"claim": "Domain-specific tasks — your exact expertise, not generic tagging", "angle": "A", "uniqueness": "high"},
]


def score_differentiators(competitor_ads: list[AdCreative]) -> list[dict]:
    """
    Score each Outlier differentiator by how much competitors are claiming the same thing.
    Higher score = more differentiated = more valuable to lead with.
    """
    competitor_copy = " ".join(
        f"{ad.hook} {ad.body}".lower() for ad in competitor_ads
    )

    scored = []
    for diff in OUTLIER_DIFFERENTIATORS:
        claim_lower = diff["claim"].lower()
        # Check if competitors are saying anything similar
        key_terms = claim_lower.split()[:4]
        competitor_coverage = sum(1 for t in key_terms if t in competitor_copy) / len(key_terms)
        # Lower competitor coverage = more whitespace for Outlier
        whitespace_score = 1.0 - competitor_coverage
        scored.append({**diff, "whitespace_score": round(whitespace_score, 2)})

    return sorted(scored, key=lambda x: x["whitespace_score"], reverse=True)


# ── Main orchestration ────────────────────────────────────────────────────────

def run_competitor_intel(
    tg_label: str = "general",
    target_competitors: list[str] | None = None,
    include_reddit: bool = True,
    include_trustpilot: bool = True,
    include_seo: bool = True,
) -> CompetitorIntel:
    """
    Run full competitive intelligence sweep for a given TG.

    Args:
        tg_label:             The TG label from the cohort (e.g. "clinical nurses Philippines")
        target_competitors:   List of competitor keys to research. Defaults to top 4.
        include_reddit:       Whether to pull Reddit signals
        include_trustpilot:   Whether to scrape Trustpilot
        include_seo:          Whether to pull search intent terms

    Returns:
        CompetitorIntel — structured output ready to pass to brief generator
    """
    competitors = target_competitors or ["dataannotation", "mercor", "alignerr", "micro1"]
    intel = CompetitorIntel()

    # 1. Meta ads (URLs for agent to browse)
    for comp_key in competitors:
        ads = fetch_meta_ads(comp_key)
        intel.competitor_ads.extend(ads)
        log.info("Meta ads queued for %s", comp_key)

    # 2. Trustpilot ratings
    if include_trustpilot:
        # Competitors
        for comp_key in competitors:
            comp = COMPETITORS[comp_key]
            rating_data = fetch_trustpilot(comp["trustpilot_slug"], comp["name"])
            if rating_data:
                intel.trustpilot_ratings[comp["name"]] = rating_data
            time.sleep(1)

        # Outlier itself
        outlier_tp = fetch_trustpilot(OUTLIER_INTEL["trustpilot_slug"], "Outlier")
        if outlier_tp:
            intel.trustpilot_ratings["Outlier"] = outlier_tp

    # 3. Reddit signals
    if include_reddit:
        for comp_key in competitors:
            comp = COMPETITORS[comp_key]
            for term in comp["reddit_terms"][:2]:  # limit to 2 terms per competitor
                signals = fetch_reddit_signals(term, comp["name"])
                intel.review_signals.extend(signals)
                time.sleep(1)

        # Outlier signals
        for term in OUTLIER_INTEL["reddit_terms"][:2]:
            signals = fetch_reddit_signals(term, "Outlier")
            intel.review_signals.extend(signals)
            time.sleep(1)

    # 4. SEO terms
    if include_seo:
        intel.search_terms = fetch_search_intent_terms(tg_label)

    # 5. Angle analysis
    real_ads = [ad for ad in intel.competitor_ads if ad.angle != "?"]
    if real_ads:
        angle_data = analyze_angle_distribution(real_ads)
        intel.dominant_competitor_angle = angle_data["dominant"]
        intel.whitespace_angle = angle_data["whitespace"]

    # 6. Synthesize review signals
    outlier_signals = [s for s in intel.review_signals if s.platform == "Outlier"]
    competitor_signals = [s for s in intel.review_signals if s.platform != "Outlier"]

    intel.outlier_praise_themes = list({
        s.theme for s in outlier_signals if s.sentiment == "positive"
    })
    intel.outlier_complaint_themes = list({
        s.theme for s in outlier_signals if s.sentiment == "negative"
    })
    intel.top_user_pain_points = list({
        s.theme for s in competitor_signals if s.sentiment == "negative"
    })

    # 7. Score differentiators
    scored = score_differentiators(intel.competitor_ads)
    intel.differentiators = [d["claim"] for d in scored[:3]]  # top 3 with most whitespace

    # 8. Generate copy recommendations
    intel.copy_recommendations = _generate_copy_recommendations(intel, tg_label)
    intel.design_recommendations = _generate_design_recommendations(intel)

    return intel


def _generate_copy_recommendations(intel: CompetitorIntel, tg_label: str) -> list[str]:
    recs = []

    if intel.whitespace_angle:
        angle_name = {"A": "Expertise Hook", "B": "Earnings Hook", "C": "Flexibility Hook"}.get(
            intel.whitespace_angle, intel.whitespace_angle
        )
        recs.append(
            f"Lead with Angle {intel.whitespace_angle} ({angle_name}) — "
            f"competitors are underusing it (whitespace opportunity)"
        )

    if intel.dominant_competitor_angle:
        recs.append(
            f"Avoid leading with Angle {intel.dominant_competitor_angle} — "
            f"most competitor ads are already running this angle (saturation)"
        )

    if "trust" in intel.top_user_pain_points:
        recs.append(
            "Embed social proof early — 'trust' is the top pain point on competitor platforms. "
            "Lead Angle B variants with $500M+ stat or Trustpilot rating."
        )

    if "availability" in intel.top_user_pain_points:
        recs.append(
            "Avoid implying guaranteed work volume — 'project availability' is a common complaint "
            "across platforms. Frame as 'tasks when you want them' not 'unlimited tasks'."
        )

    if intel.differentiators:
        recs.append(
            f"Strongest differentiator to lead with: \"{intel.differentiators[0]}\""
        )

    if intel.search_terms:
        recs.append(
            f"Embed high-intent SEO terms: {', '.join(intel.search_terms[:3])}"
        )

    return recs


def _generate_design_recommendations(intel: CompetitorIntel) -> list[str]:
    recs = []

    # Check what visual formats competitors are using
    formats = [ad.format for ad in intel.competitor_ads if ad.format != "unknown"]
    if formats:
        from collections import Counter
        most_common = Counter(formats).most_common(1)[0][0]
        if most_common == "static":
            recs.append("Competitors are mostly running static images — test video or carousel for differentiation")
        elif most_common == "video":
            recs.append("Competitors are running video — high-quality static may stand out in a video-heavy feed")

    # Check if competitors are using person photos or illustrations
    person_ads = sum(1 for ad in intel.competitor_ads
                     if any(w in ad.body.lower() for w in ["photo", "person", "real"]))
    if person_ads == 0:
        recs.append("No competitor ads appear to use lifestyle person photography — our editorial photo style is a differentiator")

    recs.append(
        "Show Outlier Community proof — Discord member count, active task feed screenshot, "
        "or contributor quote. Competitors lack this signal."
    )

    return recs


def to_brief_context(intel: CompetitorIntel) -> dict:
    """
    Convert CompetitorIntel to the brief_context format consumed by
    ad-creative-brief-generator and outlier-copy-writer.
    """
    return {
        "dominant_competitor_angle": intel.dominant_competitor_angle,
        "whitespace_angle": intel.whitespace_angle,
        "top_user_pain_points": intel.top_user_pain_points,
        "outlier_praise_themes": intel.outlier_praise_themes,
        "outlier_complaint_themes": intel.outlier_complaint_themes,
        "top_differentiators": intel.differentiators,
        "high_intent_search_terms": intel.search_terms[:5],
        "copy_recommendations": intel.copy_recommendations,
        "design_recommendations": intel.design_recommendations,
        "trustpilot_snapshot": {
            k: {"rating": v.get("rating"), "count": v.get("review_count")}
            for k, v in intel.trustpilot_ratings.items()
        },
    }

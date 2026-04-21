"""
Campaign Feedback Agent

Weekly analysis loop for all active LinkedIn image ad campaigns.

Pipeline per run:
  1. Pull full-funnel metrics from Snowflake via Redash (60-day window)
  2. Fetch creative metadata via LinkedIn Marketing API v2
  3. Download creative images and run Claude Vision analysis
  4. Score each creative across 6 dimensions (CTR, LP intent, CPC,
     volume, visual quality, copy angle)
  5. Classify each creative: SCALE / MAINTAIN / HOLD / EXPERIMENT / PAUSE
  6. Generate concrete experiment briefs for EXPERIMENT/PAUSE creatives
  7. Check data/experiment_queue.json for previously recommended experiments
     and report on their progress if they are now live
  8. Return the full report text (caller posts to Slack via MCP plugin)
  9. Persist updated experiment_queue.json

Run manually:
    python -m src.campaign_feedback_agent

The experiment queue is the feedback loop:
  Cycle 1 → PAUSE deepanshu, generate brief for replacement
  Human implements new creative
  Cycle 2 → agent detects new creative in same campaign, links to experiment,
             begins tracking
  Cycle 3+ → agent compares challenger vs control, declares winner or extends
"""
import base64
import json
import logging
import re
import time
import uuid
from dataclasses import dataclass, asdict, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import requests
from dotenv import load_dotenv
from openai import OpenAI

# Load .env so the module works standalone (CLI) and when imported
load_dotenv(Path(__file__).parent.parent / ".env")

import config
from src.redash_db import RedashClient

log = logging.getLogger(__name__)

# ── Paths ──────────────────────────────────────────────────────────────────────

_DATA_DIR        = Path(__file__).parent.parent / "data"
_EXPERIMENT_FILE = _DATA_DIR / "experiment_queue.json"
_VISION_CACHE    = _DATA_DIR / "creative_vision_cache.json"

# ── SQL ────────────────────────────────────────────────────────────────────────

_METRICS_SQL = """
WITH ch AS (
    SELECT
        cr.ID        AS creative_id,
        cr.CAMPAIGN_ID,
        ROW_NUMBER() OVER (PARTITION BY cr.ID ORDER BY cr.LAST_MODIFIED_AT DESC) AS rn
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CREATIVE_HISTORY cr
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp ON cr.CAMPAIGN_ID = camp.ID
    WHERE cr.ACCOUNT_ID = {account_id}
      AND camp.FORMAT IN ('STANDARD_UPDATE', 'SINGLE_VIDEO', 'CAROUSEL', 'TEXT_AD')
),
creatives AS (SELECT creative_id, CAMPAIGN_ID FROM ch WHERE rn = 1),
metrics AS (
    SELECT
        c.creative_id,
        camp.ID              AS campaign_id,
        camp.NAME            AS campaign_name,
        camp.FORMAT          AS ad_format,
        camp.LOCALE_COUNTRY  AS geo_country,
        camp.LOCALE_LANGUAGE AS geo_language,
        SUM(aa.IMPRESSIONS)          AS impressions,
        SUM(aa.CLICKS)               AS total_clicks,
        SUM(aa.LANDING_PAGE_CLICKS)  AS lp_clicks,
        SUM(aa.COST_IN_USD)          AS cost_usd
    FROM creatives c
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE aa
         ON c.creative_id = aa.CREATIVE_ID
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp
         ON c.CAMPAIGN_ID = camp.ID
    WHERE aa.DAY >= CURRENT_DATE - INTERVAL '{window} days'
    GROUP BY 1,2,3,4,5,6
    HAVING SUM(aa.IMPRESSIONS) >= 50000
),
apps AS (
    SELECT
        TRY_TO_NUMBER(AD_ID)                                           AS creative_id,
        COUNT(DISTINCT EMAIL)                                          AS applications,
        COUNT(DISTINCT CASE WHEN ACTIVATION_DAY IS NOT NULL
                            THEN EMAIL END)                            AS activations
    FROM VIEW.APPLICATION_CONVERSION
    WHERE UTM_SOURCE ILIKE '%linkedin%'
      AND UTM_MEDIUM  = 'paid'
      AND APPLICATION_DAY >= CURRENT_DATE - INTERVAL '{window} days'
      AND TRY_TO_NUMBER(AD_ID) IS NOT NULL
    GROUP BY 1
)
SELECT
    m.creative_id,
    m.campaign_id,
    m.campaign_name,
    m.ad_format,
    m.geo_country,
    m.geo_language,
    m.impressions,
    m.total_clicks,
    m.lp_clicks,
    m.cost_usd,
    COALESCE(a.applications, 0)  AS applications,
    COALESCE(a.activations,  0)  AS activations,
    ROUND(m.lp_clicks::FLOAT    / NULLIF(m.impressions,   0) * 100, 3) AS lp_ctr,
    ROUND(m.total_clicks::FLOAT / NULLIF(m.impressions,   0) * 100, 3) AS total_ctr,
    ROUND(m.lp_clicks::FLOAT    / NULLIF(m.total_clicks,  0) * 100, 1) AS lp_intent_pct,
    ROUND(m.cost_usd            / NULLIF(m.lp_clicks,     0), 2)       AS cpc_lp,
    ROUND(m.cost_usd            / NULLIF(a.applications,  0), 2)       AS cpa
FROM metrics m
LEFT JOIN apps a ON m.creative_id = a.creative_id
ORDER BY m.impressions DESC
"""

# ── Data classes ───────────────────────────────────────────────────────────────

@dataclass
class CreativeMetrics:
    creative_id:   int
    campaign_id:   int
    campaign_name: str
    ad_format:     str
    geo:           str
    impressions:   int
    total_clicks:  int
    lp_clicks:     int
    cost_usd:      float
    applications:  int
    activations:   int
    lp_ctr:        float   # LP clicks / impressions %
    total_ctr:     float   # all clicks / impressions %
    lp_intent_pct: float   # LP clicks / total clicks %
    cpc_lp:        float   # cost per LP click
    cpa:           Optional[float]


@dataclass
class VisionAnalysis:
    """Output of Claude Vision analysis on a creative image."""
    image_type:        str   # workspace_dual_monitor | workspace_single | outdoor_lifestyle
                             # graduation_formal | studio_professional | other
    code_visibility:   str   # none | partial | prominent
    professional_signal: int # 1-10
    identity_trigger:  str   # engineering | travel_freedom | academic | community | unknown
    aspect_class:      str   # landscape | square | portrait | tall_portrait
    raw_summary:       str   # 1-2 sentence description


@dataclass
class CopyAnalysis:
    """Classification of the post copy."""
    opening_hook:   str   # expert_identity | community_identity | legacy_impact
                          # learning_growth | financial | story_driven | unknown
    rate_framing:   str   # none | vague_competitive | hourly_ceiling | hourly_range
                          # weekly_range | weekly_ceiling
    tone:           str   # professional | casual | academic | motivational
    key_hook:       str   # the single most important sentence from the post


@dataclass
class CreativeScore:
    creative_id:    int
    campaign_name:  str
    geo:            str

    # Raw metrics (pass-through)
    metrics: CreativeMetrics

    # Content analysis
    vision:  Optional[VisionAnalysis]
    copy:    Optional[CopyAnalysis]

    # Dimension scores (0-10 each)
    s_ctr:        float = 0.0
    s_lp_intent:  float = 0.0
    s_cpc:        float = 0.0
    s_volume:     float = 0.0
    s_visual:     float = 0.0
    s_copy:       float = 0.0

    total:        float = 0.0

    # Decision
    recommendation:    str = "HOLD"   # SCALE | MAINTAIN | HOLD | EXPERIMENT | PAUSE
    experiment_brief:  Optional[dict] = None


# ── LinkedIn API helpers ───────────────────────────────────────────────────────

_LI_HEADERS = {
    "Authorization": f"Bearer {config.LINKEDIN_TOKEN}",
    "X-Restli-Protocol-Version": "2.0.0",
}

_UA = ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
       "AppleWebKit/537.36 Chrome/120 Safari/537.36")


def _fetch_creative_api(creative_id: int) -> dict:
    """Return adCreativesV2 payload for a given creative id."""
    r = requests.get(
        f"https://api.linkedin.com/v2/adCreativesV2/{creative_id}",
        headers=_LI_HEADERS,
        timeout=15,
    )
    if r.status_code == 200:
        return r.json()
    log.warning("adCreativesV2 %s → %s", creative_id, r.status_code)
    return {}


def _fetch_og_image_url(activity_urn: str) -> Optional[str]:
    """Scrape the OG image URL from a LinkedIn activity post page."""
    activity_id = activity_urn.split(":")[-1]
    url = f"https://www.linkedin.com/feed/update/urn:li:activity:{activity_id}/"
    try:
        resp = requests.get(url, headers={"User-Agent": _UA, "Accept-Language": "en-US"},
                            timeout=15)
        # Match the specific og:image meta tag — must start with https://
        m = re.search(
            r'<meta[^>]+property=["\']og:image["\'][^>]+content=["\']([^"\']+)["\']',
            resp.text,
        )
        if not m:
            # Try reversed attribute order: content= before property=
            m = re.search(
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+property=["\']og:image["\']',
                resp.text,
            )
        if m:
            url_val = m.group(1).replace("&amp;", "&")
            if url_val.startswith("https://"):
                return url_val
    except Exception as e:
        log.warning("og:image scrape failed for %s: %s", activity_urn, e)
    return None


def _fetch_post_copy(activity_urn: str) -> Optional[str]:
    """Scrape the post text from the LinkedIn activity embed."""
    activity_id = activity_urn.split(":")[-1]
    embed_url = f"https://www.linkedin.com/embed/feed/update/urn:li:activity:{activity_id}"
    try:
        resp = requests.get(embed_url, headers={"User-Agent": _UA}, timeout=15)
        # Very rough extraction — grab text between description meta tags
        text = resp.text
        for tag in ['og:description', 'description']:
            marker = f'name="{tag}"'
            if marker in text:
                chunk = text.split(marker)[1]
                if 'content="' in chunk:
                    return chunk.split('content="')[1].split('"')[0][:1000]
    except Exception as e:
        log.warning("post copy scrape failed for %s: %s", activity_urn, e)
    return None


def _download_image(url: str, path: Path) -> bool:
    """Download image to path. Returns True on success."""
    try:
        r = requests.get(url, headers={"User-Agent": _UA}, timeout=20)
        if r.status_code == 200 and len(r.content) > 5000:
            path.write_bytes(r.content)
            return True
    except Exception as e:
        log.warning("Image download failed: %s", e)
    return False


# ── Vision analysis ────────────────────────────────────────────────────────────

_VISION_PROMPT = """Analyze this LinkedIn ad creative image and classify it precisely.

Return a JSON object with these exact keys:
{
  "image_type": one of: workspace_dual_monitor | workspace_single_monitor | outdoor_lifestyle | graduation_formal | studio_professional | other,
  "code_visibility": one of: none | partial | prominent,
  "professional_signal": integer 1-10 (10 = immediately reads as a skilled professional at work),
  "identity_trigger": one of: engineering | travel_freedom | academic | community | unknown,
  "raw_summary": 1-2 sentence plain description of what is literally shown
}

Definitions:
- workspace_dual_monitor: person at desk with 2+ monitors visible showing IDE/code
- workspace_single_monitor: person at desk with 1 monitor, code may be partially visible
- outdoor_lifestyle: person in outdoor setting — travel, nature, streets (not at desk)
- graduation_formal: person in graduation gown/cap or other formal academic/ceremony attire
- studio_professional: plain background, professional headshot or studio-style photo
- code_visibility: prominent = IDE code clearly legible on screen, partial = small/blurry, none = no code visible
- professional_signal: for an ad targeting senior software engineers (3+ yrs), how strongly does this image say "this person is a skilled engineer doing real work"

Return only the JSON, no other text."""


def _analyze_image_vision(image_path: Path) -> Optional[VisionAnalysis]:
    """Call Claude Vision via LiteLLM proxy to classify the creative image."""
    try:
        img_b64 = base64.b64encode(image_path.read_bytes()).decode()
        client = OpenAI(base_url=config.LITELLM_BASE_URL, api_key=config.LITELLM_API_KEY)
        resp = client.chat.completions.create(
            model=config.LITELLM_MODEL,
            messages=[{
                "role": "user",
                "content": [
                    {"type": "image_url",
                     "image_url": {"url": f"data:image/jpeg;base64,{img_b64}"}},
                    {"type": "text", "text": _VISION_PROMPT},
                ],
            }],
            max_tokens=400,
        )
        raw = resp.choices[0].message.content.strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)

        # Derive aspect class from image dimensions via Pillow
        aspect_class = _classify_aspect(image_path)

        return VisionAnalysis(
            image_type=data.get("image_type", "other"),
            code_visibility=data.get("code_visibility", "none"),
            professional_signal=int(data.get("professional_signal", 5)),
            identity_trigger=data.get("identity_trigger", "unknown"),
            aspect_class=aspect_class,
            raw_summary=data.get("raw_summary", ""),
        )
    except Exception as e:
        log.warning("Vision analysis failed: %s", e)
        return None


def _classify_aspect(image_path: Path) -> str:
    try:
        from PIL import Image
        with Image.open(image_path) as img:
            w, h = img.size
            ratio = w / h
        if ratio > 1.2:
            return "landscape"
        if ratio > 0.9:
            return "square"
        if ratio > 0.72:
            return "portrait"
        return "tall_portrait"
    except Exception:
        return "unknown"


# ── Copy analysis ──────────────────────────────────────────────────────────────

_COPY_PROMPT = """Classify this LinkedIn ad post copy.

Post text:
---
{copy}
---

Return a JSON object with these exact keys:
{{
  "opening_hook": one of: expert_identity | community_identity | legacy_impact | learning_growth | financial | story_driven | unknown,
  "rate_framing": one of: none | vague_competitive | hourly_ceiling | hourly_range | weekly_range | weekly_ceiling,
  "tone": one of: professional | casual | academic | motivational,
  "key_hook": the single most important sentence or phrase (max 120 chars)
}}

Definitions:
- expert_identity: opens by validating the reader's existing expertise ("real engineer", "you already know")
- community_identity: opens by addressing a specific community ("Ukrainian speakers:", "Python engineers:")
- legacy_impact: opens with impact on others ("students I teach will use AI I helped train")
- learning_growth: frames the opportunity as a way to grow/learn ("I didn't realise how much Outlier taught me")
- financial: leads with a specific rate or earnings claim
- hourly_ceiling: "$X/hr" phrased as "up to $X" or "earn up to"
- hourly_range: "$X-Y/hr" range
- weekly_range: "$X-Y per week" or "USD $X-Y weekly"
- weekly_ceiling: "up to $X per week"

Return only the JSON, no other text."""


def _analyze_copy(copy_text: str) -> Optional[CopyAnalysis]:
    if not copy_text or len(copy_text) < 50:
        return None
    try:
        client = OpenAI(base_url=config.LITELLM_BASE_URL, api_key=config.LITELLM_API_KEY)
        prompt = _COPY_PROMPT.format(copy=copy_text[:1500])
        resp = client.chat.completions.create(
            model=config.LITELLM_MODEL,
            messages=[{"role": "user", "content": prompt}],
            max_tokens=300,
        )
        raw = resp.choices[0].message.content.strip()
        if raw.startswith("```"):
            raw = raw.split("```")[1]
            if raw.startswith("json"):
                raw = raw[4:]
        data = json.loads(raw)
        return CopyAnalysis(
            opening_hook=data.get("opening_hook", "unknown"),
            rate_framing=data.get("rate_framing", "none"),
            tone=data.get("tone", "professional"),
            key_hook=data.get("key_hook", "")[:120],
        )
    except Exception as e:
        log.warning("Copy analysis failed: %s", e)
        return None


# ── Scoring ────────────────────────────────────────────────────────────────────

def _score_ctr(lp_ctr: float) -> float:
    """Score LP CTR (0-10). Benchmarks are geo-agnostic for now."""
    if lp_ctr >= 0.6:  return 10.0
    if lp_ctr >= 0.4:  return 9.0
    if lp_ctr >= 0.3:  return 7.5
    if lp_ctr >= 0.2:  return 6.0
    if lp_ctr >= 0.1:  return 4.0
    if lp_ctr >= 0.05: return 2.0
    return 0.0


def _score_lp_intent(pct: float) -> float:
    """Score LP-click / total-click ratio (0-10). High = intentional traffic."""
    if pct >= 75: return 10.0
    if pct >= 65: return 8.5
    if pct >= 55: return 7.0
    if pct >= 45: return 5.5
    if pct >= 35: return 3.5
    return 2.0


def _score_cpc(cpc: float) -> float:
    """Score cost per LP click (0-10). Lower is better."""
    if cpc <= 0:    return 0.0   # no data / div-by-zero
    if cpc < 0.5:   return 10.0
    if cpc < 1.0:   return 8.5
    if cpc < 2.0:   return 7.0
    if cpc < 5.0:   return 4.5
    if cpc < 10.0:  return 2.0
    return 0.0


def _score_volume(lp_clicks: int) -> float:
    """Score LP click volume (statistical significance proxy)."""
    if lp_clicks >= 10000: return 10.0
    if lp_clicks >= 5000:  return 9.0
    if lp_clicks >= 2000:  return 7.0
    if lp_clicks >= 500:   return 5.0
    if lp_clicks >= 200:   return 2.0
    return 0.0


def _score_visual(v: Optional[VisionAnalysis]) -> float:
    """Score visual element quality (0-10)."""
    if v is None:
        return 5.0  # unknown — neutral
    base = {
        "workspace_dual_monitor":  9.5,
        "workspace_single_monitor": 7.0,
        "studio_professional":      6.0,
        "outdoor_lifestyle":        5.5,
        "graduation_formal":        0.0,
        "other":                    4.0,
    }.get(v.image_type, 4.0)
    # Bonus for visible code
    code_bonus = {"prominent": 1.0, "partial": 0.3, "none": 0.0}.get(v.code_visibility, 0)
    return min(10.0, base + code_bonus)


def _score_copy(c: Optional[CopyAnalysis]) -> float:
    """Score copy angle quality (0-10)."""
    if c is None:
        return 5.0
    hook_scores = {
        "expert_identity":    8.5,
        "community_identity": 8.0,
        "financial":          7.5,
        "legacy_impact":      7.0,
        "story_driven":       5.0,
        "unknown":            4.0,
        "learning_growth":    1.0,   # proven to underperform for senior TGs
    }
    base = hook_scores.get(c.opening_hook, 4.0)
    # Rate framing modifiers
    rate_mod = {
        "vague_competitive": +0.5,
        "weekly_range":      +0.5,
        "hourly_range":      +0.0,
        "hourly_ceiling":    -1.0,
        "weekly_ceiling":    -0.5,
        "none":              +0.0,
    }.get(c.rate_framing, 0.0)
    return min(10.0, max(0.0, base + rate_mod))


def _recommend(total: float) -> str:
    if total >= 52: return "SCALE"
    if total >= 44: return "MAINTAIN"
    if total >= 36: return "HOLD"
    if total >= 24: return "EXPERIMENT"
    return "PAUSE"


def _compute_score(m: CreativeMetrics,
                   vision: Optional[VisionAnalysis],
                   copy: Optional[CopyAnalysis]) -> CreativeScore:
    s_ctr       = _score_ctr(m.lp_ctr)
    s_lp_intent = _score_lp_intent(m.lp_intent_pct)
    s_cpc       = _score_cpc(m.cpc_lp)
    s_volume    = _score_volume(m.lp_clicks)
    s_visual    = _score_visual(vision)
    s_copy      = _score_copy(copy)
    total       = s_ctr + s_lp_intent + s_cpc + s_volume + s_visual + s_copy

    score = CreativeScore(
        creative_id=m.creative_id,
        campaign_name=m.campaign_name,
        geo=m.geo,
        metrics=m,
        vision=vision,
        copy=copy,
        s_ctr=s_ctr,
        s_lp_intent=s_lp_intent,
        s_cpc=s_cpc,
        s_volume=s_volume,
        s_visual=s_visual,
        s_copy=s_copy,
        total=total,
        recommendation=_recommend(total),
    )
    score.experiment_brief = _build_brief(score) if score.recommendation in ("PAUSE", "EXPERIMENT") else None
    return score


# ── Experiment brief generator ─────────────────────────────────────────────────

def _build_brief(score: CreativeScore) -> dict:
    """Generate a concrete experiment brief for a creative that needs improvement."""
    m = score.metrics
    v = score.vision
    c = score.copy

    # Diagnose the top 2 problems
    dim_scores = {
        "CTR (LP)":     score.s_ctr,
        "LP Intent":    score.s_lp_intent,
        "CPC":          score.s_cpc,
        "Volume":       score.s_volume,
        "Visual":       score.s_visual,
        "Copy angle":   score.s_copy,
    }
    worst = sorted(dim_scores.items(), key=lambda x: x[1])[:2]

    # Build problem statement
    problems = []
    if v and v.image_type == "graduation_formal":
        problems.append("Graduation photo signals 'student' to a senior-engineer TG")
    elif v and v.code_visibility == "none" and "workspace" not in (v.image_type or ""):
        problems.append("No desk/workspace/code visible — identity signal is weak")
    if c and c.opening_hook == "learning_growth":
        problems.append("Learning-growth hook underperforms for 3+ yr experienced TGs")
    if c and c.rate_framing == "hourly_ceiling":
        problems.append("'Up to $X/hr' ceiling framing suppresses CTR vs range framing")
    if m.lp_intent_pct < 45:
        problems.append(f"Only {m.lp_intent_pct:.0f}% of clicks reach LP — lifestyle image drives curiosity, not intent")
    if not problems:
        problems.append(f"Weakest dimensions: {worst[0][0]} ({worst[0][1]:.1f}/10), {worst[1][0]} ({worst[1][1]:.1f}/10)")

    # Prescribe the fix
    visual_fix = (
        "Creator at their coding desk, dual monitors with IDE/code visible and colourful"
        if m.lp_ctr < 0.2 or (v and v.image_type in ("graduation_formal", "outdoor_lifestyle"))
        else "Keep current visual but ensure code/IDE is prominent in frame"
    )
    copy_fix = (
        "Open with expert identity — e.g. 'You already know how to read bad code. Outlier pays you to apply that to AI.'"
        if not c or c.opening_hook in ("learning_growth", "story_driven", "unknown")
        else "Keep current hook; test weekly range rate framing instead of hourly ceiling"
    )
    rate_fix = (
        "Use weekly range e.g. '$700-1100/week' instead of 'up to $X/hr'"
        if c and c.rate_framing in ("hourly_ceiling", "hourly_range")
        else "Add explicit rate range if not present"
    )

    return {
        "id":              f"exp_{datetime.now(timezone.utc).strftime('%Y%m%d')}_{m.creative_id}",
        "type":            "creative_replacement",
        "campaign_id":     m.campaign_id,
        "current_creative_id": m.creative_id,
        "current_score":   round(score.total, 1),
        "recommendation":  score.recommendation,
        "problems":        problems,
        "action":          "PAUSE_CURRENT" if score.recommendation == "PAUSE" else "ADD_CHALLENGER",
        "hypothesis": (
            "Workspace photo with visible IDE code + expert identity hook will "
            "achieve LP CTR >0.3% and LP intent >60%"
        ),
        "brief": {
            "visual":         visual_fix,
            "copy_angle":     copy_fix,
            "rate_framing":   rate_fix,
            "aspect":         "1:1.5 near-square or 4:3 landscape (avoid tall 3:4)",
            "creator_brief":  (
                "Self-portrait at your coding setup. Two monitors behind you with IDE open "
                "and code clearly visible. Warm natural expression. Casual shirt. "
                "Photo taken from desk level looking up slightly."
            ),
        },
        "target_metrics": {
            "lp_ctr":        0.30,
            "lp_intent_pct": 60.0,
            "cpc_lp":        2.0,
        },
        "status":              "pending",
        "created_at":          datetime.now(timezone.utc).date().isoformat(),
        "implemented_at":      None,
        "challenger_creative": None,
        "outcome":             None,
    }


# ── Experiment queue management ────────────────────────────────────────────────

def _load_queue() -> list[dict]:
    if _EXPERIMENT_FILE.exists():
        return json.loads(_EXPERIMENT_FILE.read_text())
    return []


def _save_queue(queue: list[dict]) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _EXPERIMENT_FILE.write_text(json.dumps(queue, indent=2))


def _update_queue(queue: list[dict],
                  current_scores: list[CreativeScore],
                  active_ids: set[int]) -> list[dict]:
    """
    For each pending experiment:
     - If a new creative appeared in the same campaign: link it and set status=running
     - If status=running: report current metrics vs baseline
    """
    scored_by_id = {s.creative_id: s for s in current_scores}

    for exp in queue:
        if exp["status"] not in ("pending", "running"):
            continue

        campaign_id = exp.get("campaign_id")
        current_cid = exp.get("current_creative_id")

        # Find any new creative in the same campaign that's not the original
        if exp["status"] == "pending":
            new_creatives = [
                s for s in current_scores
                if s.metrics.campaign_id == campaign_id
                and s.creative_id != current_cid
                and s.creative_id not in [e.get("current_creative_id") for e in queue]
            ]
            if new_creatives:
                exp["status"] = "running"
                exp["challenger_creative"] = new_creatives[0].creative_id
                exp["implemented_at"] = datetime.now(timezone.utc).date().isoformat()
                log.info("Experiment %s now running — challenger %s",
                         exp["id"], exp["challenger_creative"])

        if exp["status"] == "running" and exp.get("challenger_creative"):
            challenger = scored_by_id.get(exp["challenger_creative"])
            baseline   = scored_by_id.get(exp["current_creative_id"])
            if challenger:
                exp["_latest_challenger_score"] = round(challenger.total, 1)
                exp["_latest_challenger_lp_ctr"] = challenger.metrics.lp_ctr
                exp["_latest_challenger_cpc"]    = challenger.metrics.cpc_lp
            if baseline:
                exp["_latest_baseline_score"] = round(baseline.total, 1)

            # Declare winner if challenger has enough data (>2000 LP clicks) and is ahead
            ch = scored_by_id.get(exp.get("challenger_creative", 0))
            bl = scored_by_id.get(exp.get("current_creative_id", 0))
            if ch and ch.metrics.lp_clicks >= 2000:
                if ch.metrics.lp_ctr > (bl.metrics.lp_ctr if bl else 0) * 1.1:
                    exp["status"] = "concluded"
                    exp["outcome"] = "CHALLENGER_WINS"
                elif ch.metrics.lp_ctr < (bl.metrics.lp_ctr if bl else 0) * 0.9:
                    exp["status"] = "concluded"
                    exp["outcome"] = "CONTROL_WINS"

    return queue


# ── Vision cache ───────────────────────────────────────────────────────────────

def _load_vision_cache() -> dict:
    if _VISION_CACHE.exists():
        return json.loads(_VISION_CACHE.read_text())
    return {}


def _save_vision_cache(cache: dict) -> None:
    _DATA_DIR.mkdir(parents=True, exist_ok=True)
    _VISION_CACHE.write_text(json.dumps(cache, indent=2))


# ── Content fetch orchestrator ─────────────────────────────────────────────────

def _fetch_content(creative_id: int,
                   vision_cache: dict) -> tuple[Optional[VisionAnalysis],
                                                Optional[CopyAnalysis]]:
    """
    Fetch creative content for a single creative_id.
    Uses vision_cache to avoid re-downloading/re-analyzing images.
    """
    cache_key = str(creative_id)

    # ── Vision ─────────────────────────────────────────────────────────────────
    vision: Optional[VisionAnalysis] = None
    if cache_key in vision_cache:
        d = vision_cache[cache_key]
        vision = VisionAnalysis(**d) if d else None
    else:
        api_data = _fetch_creative_api(creative_id)
        vars_ = (api_data.get("variables", {})
                         .get("data", {})
                         .get("com.linkedin.ads.SponsoredUpdateCreativeVariables", {}))
        activity_urn = vars_.get("activity", "")

        if activity_urn:
            img_url = _fetch_og_image_url(activity_urn)
            if img_url:
                img_path = Path(f"/tmp/li_creative_{creative_id}.jpg")
                if _download_image(img_url, img_path):
                    vision = _analyze_image_vision(img_path)
                    # Derive aspect from API aspect ratio if vision failed
                    if vision is None:
                        aspect = vars_.get("mediaAspectRatio", {})
                        w = aspect.get("widthAspect", 1)
                        h = aspect.get("heightAspect", 1)
                        ratio = w / h if h else 1
                        if ratio > 1.2:   a_class = "landscape"
                        elif ratio > 0.9: a_class = "square"
                        elif ratio > 0.72: a_class = "portrait"
                        else:             a_class = "tall_portrait"
                        vision = VisionAnalysis(
                            image_type="other", code_visibility="none",
                            professional_signal=5, identity_trigger="unknown",
                            aspect_class=a_class, raw_summary="(image analysis unavailable)",
                        )
        vision_cache[cache_key] = asdict(vision) if vision else None

    # ── Copy ────────────────────────────────────────────────────────────────────
    copy: Optional[CopyAnalysis] = None
    api_data = _fetch_creative_api(creative_id)
    vars_ = (api_data.get("variables", {})
                     .get("data", {})
                     .get("com.linkedin.ads.SponsoredUpdateCreativeVariables", {}))
    activity_urn = vars_.get("activity", "")
    if activity_urn:
        raw_copy = _fetch_post_copy(activity_urn)
        if raw_copy:
            copy = _analyze_copy(raw_copy)

    return vision, copy


# ── Report builder ─────────────────────────────────────────────────────────────

def _recommendation_emoji(rec: str) -> str:
    return {"SCALE": "🟢", "MAINTAIN": "🔵", "HOLD": "🟡",
            "EXPERIMENT": "🟠", "PAUSE": "🔴"}.get(rec, "⚪")


def _build_report(scores: list[CreativeScore],
                  queue: list[dict],
                  window: int) -> str:
    date = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines = [
        f"*LinkedIn Campaign Feedback Report — {date}*",
        f"_{len(scores)} active image ad creatives | {window}-day window_",
        "",
        "*CREATIVE SCORECARD*",
        f"{'Creative':<38} {'Geo':<8} {'LP CTR':>7} {'LP→LP%':>7} {'CPC':>6} "
        f"{'LPclks':>7} {'Score':>6} {'Rec':>12}",
        "─" * 95,
    ]

    for s in sorted(scores, key=lambda x: x.total, reverse=True):
        name = (s.campaign_name.split("|")[-1].strip())[:35]
        geo  = s.geo[:7]
        em   = _recommendation_emoji(s.recommendation)
        cpc  = f"${s.metrics.cpc_lp:.2f}" if s.metrics.cpc_lp else "N/A"
        lines.append(
            f"{name:<38} {geo:<8} {s.metrics.lp_ctr:>6.3f}% "
            f"{s.metrics.lp_intent_pct:>6.1f}% {cpc:>6} "
            f"{s.metrics.lp_clicks:>7,} {s.total:>6.1f} "
            f"{em} {s.recommendation}"
        )
        if s.vision:
            v_line = (f"  Visual: {s.vision.image_type} | code={s.vision.code_visibility} "
                      f"| aspect={s.vision.aspect_class} | signal={s.vision.professional_signal}/10")
            lines.append(v_line)
        if s.copy:
            c_line = f"  Copy: hook={s.copy.opening_hook} | rate={s.copy.rate_framing}"
            lines.append(c_line)

    lines += ["", "─" * 95, "", "*DIMENSION SCORES (max 10 each)*",
              f"{'Creative':<38} {'CTR':>5} {'Intent':>7} {'CPC':>5} {'Vol':>5} {'Visual':>7} {'Copy':>5}"]
    for s in sorted(scores, key=lambda x: x.total, reverse=True):
        name = (s.campaign_name.split("|")[-1].strip())[:35]
        lines.append(
            f"{name:<38} {s.s_ctr:>5.1f} {s.s_lp_intent:>7.1f} {s.s_cpc:>5.1f} "
            f"{s.s_volume:>5.1f} {s.s_visual:>7.1f} {s.s_copy:>5.1f}"
        )

    # Experiment queue section
    pending = [e for e in queue if e["status"] == "pending"]
    running = [e for e in queue if e["status"] == "running"]
    concluded = [e for e in queue if e["status"] == "concluded"]

    lines += ["", "*EXPERIMENT QUEUE*"]

    if pending:
        lines.append(f"_Pending ({len(pending)} — awaiting implementation):_")
        for e in pending:
            lines.append(f"  🟠 {e['id']}")
            lines.append(f"     Campaign: {e['campaign_id']} | Creative: {e['current_creative_id']}")
            lines.append(f"     Action: {e['action']}")
            lines.append(f"     Problems: {'; '.join(e['problems'][:2])}")
            lines.append(f"     Test: {e['brief']['visual'][:80]}")
            lines.append(f"     Copy: {e['brief']['copy_angle'][:80]}")
            lines.append(f"     Target: LP CTR >={e['target_metrics']['lp_ctr']*100:.1f}bps, "
                         f"LP Intent >={e['target_metrics']['lp_intent_pct']:.0f}%, "
                         f"CPC <=${e['target_metrics']['cpc_lp']:.2f}")
            lines.append("")

    if running:
        lines.append(f"_Running ({len(running)} — in-flight experiments):_")
        for e in running:
            challenger_score = e.get("_latest_challenger_score", "N/A")
            baseline_score   = e.get("_latest_baseline_score", "N/A")
            lines.append(f"  🔄 {e['id']}  challenger={e['challenger_creative']} "
                         f"score: {challenger_score} vs baseline {baseline_score}")

    if concluded:
        lines.append(f"_Concluded ({len(concluded)}):_")
        for e in concluded:
            lines.append(f"  ✅ {e['id']}  outcome={e['outcome']}")

    if not pending and not running and not concluded:
        lines.append("  No experiments in queue.")

    # Attribution gap warning if any creative has 0 apps but high LP clicks
    no_attr = [s for s in scores if s.metrics.applications == 0 and s.metrics.lp_clicks > 1000]
    if no_attr:
        lines += [
            "",
            "*⚠️ ATTRIBUTION GAP*",
            f"{len(no_attr)} creatives with >1K LP clicks but 0 attributed applications.",
            "Cause: UGC boosted posts use lnkd.in URLs that don't pass {ad.id} to APPLICATION_CONVERSION.",
            "Fix: In Campaign Manager → campaign Tracking settings → add URL parameter:",
            "  utm_source=linkedin&utm_medium=paid&ad_id={creative.id}",
            "Until fixed, CPA for image ads is unknown despite real spend.",
        ]

    return "\n".join(lines)


# ── Main entry point ───────────────────────────────────────────────────────────

def run(window: int = 60) -> str:
    """
    Run the full campaign feedback loop.
    Returns the report text. Caller is responsible for posting to Slack.
    """
    log.info("Campaign Feedback Agent starting (window=%d days)", window)
    _DATA_DIR.mkdir(parents=True, exist_ok=True)

    # 1. Pull metrics
    db  = RedashClient()
    sql = _METRICS_SQL.format(account_id=config.LINKEDIN_AD_ACCOUNT_ID, window=window)
    df  = db._run_query(sql, label="feedback-metrics")
    if df.empty:
        return f"Campaign Feedback Agent: no data for last {window} days."

    # 2. Parse metrics
    metrics_list: list[CreativeMetrics] = []
    for _, r in df.iterrows():
        def _fi(v): return int(v) if v is not None and v == v else 0
        def _ff(v): return float(v) if v is not None and v == v else 0.0
        country  = r.get("geo_country") or ""
        language = r.get("geo_language") or ""
        metrics_list.append(CreativeMetrics(
            creative_id=_fi(r["creative_id"]),
            campaign_id=_fi(r["campaign_id"]),
            campaign_name=r.get("campaign_name") or "",
            ad_format=r.get("ad_format") or "",
            geo=f"{country}-{language}".strip("-"),
            impressions=_fi(r["impressions"]),
            total_clicks=_fi(r["total_clicks"]),
            lp_clicks=_fi(r["lp_clicks"]),
            cost_usd=_ff(r["cost_usd"]),
            applications=_fi(r["applications"]),
            activations=_fi(r["activations"]),
            lp_ctr=_ff(r["lp_ctr"]),
            total_ctr=_ff(r["total_ctr"]),
            lp_intent_pct=_ff(r["lp_intent_pct"]),
            cpc_lp=_ff(r["cpc_lp"]),
            cpa=_ff(r["cpa"]) if r.get("cpa") and r["cpa"] == r["cpa"] else None,
        ))

    # Deduplicate by creative_id (keep best LP clicks row)
    seen: dict[int, CreativeMetrics] = {}
    for m in metrics_list:
        if m.creative_id not in seen or m.lp_clicks > seen[m.creative_id].lp_clicks:
            seen[m.creative_id] = m
    metrics_list = list(seen.values())

    log.info("Fetched %d unique creatives", len(metrics_list))

    # 3. Content analysis (vision + copy) with cache
    vision_cache = _load_vision_cache()
    scores: list[CreativeScore] = []

    for m in metrics_list:
        log.info("Analyzing creative %s — %s", m.creative_id,
                 m.campaign_name.split("|")[-1].strip()[:40])
        vision, copy = _fetch_content(m.creative_id, vision_cache)
        score = _compute_score(m, vision, copy)
        scores.append(score)

    _save_vision_cache(vision_cache)

    # 4. Load and update experiment queue
    queue = _load_queue()

    # Add new experiment briefs for PAUSE/EXPERIMENT creatives
    # (only if not already in queue)
    existing_ids = {e["current_creative_id"] for e in queue}
    for s in scores:
        if s.recommendation in ("PAUSE", "EXPERIMENT") and s.creative_id not in existing_ids:
            if s.experiment_brief:
                queue.append(s.experiment_brief)
                log.info("Added experiment to queue: %s", s.experiment_brief["id"])

    # Update running experiments
    active_ids = {m.creative_id for m in metrics_list}
    queue = _update_queue(queue, scores, active_ids)
    _save_queue(queue)

    # 5. Build report
    return _build_report(scores, queue, window)


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import argparse
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    parser = argparse.ArgumentParser(description="Campaign Feedback Agent")
    parser.add_argument("--window", type=int, default=60,
                        help="Lookback window in days (default: 60)")
    args = parser.parse_args()

    print(run(window=args.window))

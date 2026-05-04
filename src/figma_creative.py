"""
Figma creative automation.
  1. Classify TG category from cohort rules (port of ui.html classifyTG)
  2. Fetch base node text layer map via Figma REST API
  3. Generate 3 copy variants via Claude API (Expertise / Earnings / Flexibility hooks)
  4. Execute plugin logic in Figma via Claude API + use_figma MCP:
       clone × 3, apply text updates, customizeDesign per variantIndex
  5. Export clone frames as PNG via Figma REST API
"""
import json
import logging
import re
import tempfile
from pathlib import Path

import anthropic
import requests
from openai import OpenAI

import config


def _llm_client() -> OpenAI:
    """Return an OpenAI-compatible client pointed at the LiteLLM proxy."""
    return OpenAI(
        base_url=config.LITELLM_BASE_URL,
        api_key=config.LITELLM_API_KEY,
    )

log = logging.getLogger(__name__)

FIGMA_API = "https://api.figma.com/v1"

# ── TG design data (mirrors code.js exactly) ──────────────────────────────────

TG_PALETTES = {
    "DATA_ANALYST":      [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}],
    "ML_ENGINEER":       [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}],
    "MATH":              [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}],
    "SOFTWARE_ENGINEER": [{"r": 0.67, "g": 0.85, "b": 1.00}, {"r": 0.85, "g": 0.95, "b": 1.00}],
    "MEDICAL":           [{"r": 0.87, "g": 0.85, "b": 1.00}, {"r": 0.93, "g": 0.91, "b": 1.00}],
    "LANGUAGE":          [{"r": 0.78, "g": 0.94, "b": 0.86}, {"r": 0.88, "g": 0.98, "b": 0.94}],
    "GENERAL":           [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}],
}

TG_ILLUS_VARIANTS = {
    "DATA_ANALYST":      ["chart",  "neural", "code"],
    "ML_ENGINEER":       ["neural", "chart",  "code"],
    "MATH":              ["chart",  "neural", "chart"],
    "SOFTWARE_ENGINEER": ["code",   "chart",  "neural"],
    "MEDICAL":           ["brain",  "speech", "brain"],
    "LANGUAGE":          ["speech", "brain",  "speech"],
    "GENERAL":           ["chart",  "code",   "neural"],
}


# ── TG classifier ─────────────────────────────────────────────────────────────

def classify_tg(cohort_name: str, rules: list) -> str:
    """
    Port of ui.html classifyTG() — keyword regex against cohort name + feature columns.
    Returns one of: DATA_ANALYST, ML_ENGINEER, MATH, MEDICAL, LANGUAGE, SOFTWARE_ENGINEER, GENERAL
    """
    # Replace __ separators with spaces so \b word-boundary regexes match correctly
    # e.g. "skills__diagnosis" → "skills  diagnosis" → \bdiagnosis\b matches
    text = (cohort_name.lower() + " " + " ".join(feat.lower() for feat, _ in rules)).replace("__", " ")

    if re.search(r'\b(data|sql|analyst|analytics|tableau|snowflake|bigquery|looker|power.?bi|excel|dashboard|spreadsheet)\b', text):
        return "DATA_ANALYST"
    if re.search(r'\b(ml|machine.?learning|deep.?learning|pytorch|tensorflow|llm|nlp|neural|ai.?model|research.?scientist)\b', text):
        return "ML_ENGINEER"
    if re.search(r'\b(math|mathematics|statistics|statistician|actuary|actuarial|quantitative|physicist|physics|algebra|calculus|probability|stochastic|mathematician|econometrics|biostatistics)\b', text):
        return "MATH"
    if re.search(r'\b(doctor|physician|clinical|nurse|dentist|surgeon|orthopedic|diagnosis|medicine|anatomy|physiology|surgery|emergency|pharmacol|therapeut|patient|hospital|healthcare)\b', text) or \
       re.search(r'(radiolog|cardiolog|oncolog|patholog|neurolog|psychiatr|pediatr|dermatol|urolog|nephrolog|gastroenterol|endocrinol|immunolog|pulmonol|ophthal|anesthesiol|internal.?med|medical|health|pharma|biotech|med.?grad)', text):
        return "MEDICAL"
    if re.search(r'\b(hindi|urdu|lingui|translat|spanish|french|german|arabic|japanese|korean|chinese|portuguese|italian|language)\b', text):
        return "LANGUAGE"
    if re.search(r'\b(software|engineer|developer|backend|frontend|fullstack|swe|devops|cloud|aws|python|java|react|node)\b', text):
        return "SOFTWARE_ENGINEER"
    return "GENERAL"


# ── Figma REST helpers ─────────────────────────────────────────────────────────

class FigmaCreativeClient:
    def __init__(self, token: str | None = None):
        self._token = token or config.FIGMA_TOKEN
        self._session = requests.Session()
        self._session.headers.update({"X-Figma-Token": self._token})

    def _get(self, path: str, **kwargs):
        resp = self._session.get(f"{FIGMA_API}/{path.lstrip('/')}", **kwargs)
        resp.raise_for_status()
        return resp.json()

    def get_text_layer_map(self, file_key: str, node_id: str) -> dict[str, str]:
        """
        Fetch the base node document tree from Figma REST API.
        Returns {node_id: current_text} for every TEXT node in the tree.
        """
        # Normalize node_id separator
        api_id = node_id.replace("-", ":")
        data = self._get(f"files/{file_key}/nodes", params={"ids": api_id})
        nodes = data.get("nodes", {})
        key = api_id if api_id in nodes else (node_id if node_id in nodes else None)
        if not key and nodes:
            key = next(iter(nodes))
        if not key:
            raise ValueError(f"Node {node_id} not found in file {file_key}")
        doc = nodes[key].get("document", {})
        result: dict[str, str] = {}
        _walk_text_layers(doc, result)
        log.info("Found %d text layers in base node", len(result))
        return result

    def export_clone_pngs(self, file_key: str, clone_ids: list[str], scale: float = 2.0) -> list[Path]:
        """
        Export a list of node IDs as PNGs from Figma.
        Returns list of Path objects (one per ID).
        """
        if not clone_ids:
            return []

        ids_param = ",".join(id.replace("-", ":") for id in clone_ids)
        data = self._get(
            f"images/{file_key}",
            params={"ids": ids_param, "format": "png", "scale": scale},
        )
        if data.get("err"):
            raise RuntimeError(f"Figma export error: {data['err']}")

        images = data.get("images", {})
        paths = []
        for orig_id in clone_ids:
            norm_id = orig_id.replace("-", ":") if orig_id.replace("-", ":") in images else orig_id
            if norm_id not in images:
                norm_id = next((k for k in images if k.replace(":", "-") == orig_id.replace(":", "-")), None)
            url = images.get(norm_id) if norm_id else None
            if not url:
                log.warning("No export URL for node %s", orig_id)
                paths.append(None)
                continue
            img_resp = requests.get(url)
            img_resp.raise_for_status()
            tmp = Path(tempfile.mktemp(suffix=f"_{orig_id.replace(':', '-')}.png"))
            tmp.write_bytes(img_resp.content)
            log.info("Exported %s → %s (%d bytes)", orig_id, tmp, len(img_resp.content))
            paths.append(tmp)

        return paths


def _walk_text_layers(node: dict, out: dict) -> None:
    if node.get("type") == "TEXT":
        node_id = node.get("id", "")
        chars   = node.get("characters", "")
        name    = node.get("name", "")
        if node_id and chars:
            out[node_id] = chars
    for child in node.get("children", []):
        _walk_text_layers(child, out)


# ── LLM Context Flow Documentation ───────────────────────────────────────────
#
# Stage 1 — Copy generation (this function): LiteLLM → claude-sonnet-4-6
#   Input context passed to the LLM:
#     - cohort.name        : raw feature label, e.g. "skills__diagnosis__healthcare"
#     - cohort.rules       : list of (feature, value) tuples from Stage A/B analysis
#     - human-readable signals derived from _col_to_human() + _feature_to_facet()
#     - Figma text layer map (empty dict {} when Figma is not configured)
#
#   Output consumed downstream:
#     - headline, subheadline, cta  → text overlay in compose_ad()
#     - photo_subject               → THE ONLY SIGNAL passed to Gemini (Stage 2)
#     - layerUpdates                → Figma MCP path (currently out of scope)
#
# Stage 2 — Image generation: LiteLLM → Gemini /images/generations
#   Defined in: src/midjourney_creative.py  generate_imagen_creative()
#   Input: photo_subject + hard-coded template (portrait framing, plant background,
#          warm window light, 85mm lens, angle-specific expression)
#   Output: background PNG, composited by compose_ad() with gradient + text overlay
#
# Why photo_subject is critical:
#   It is the ONLY cohort-specific signal Gemini receives. If the LLM produces a
#   generic description (e.g. "professional person"), Gemini generates a stock-photo
#   image with no connection to the audience segment.
#   validate_photo_subject() in midjourney_creative.py enforces specificity before
#   the Gemini call is made.
#
# Approved Outlier vocabulary: all copy produced here must avoid the words in
# CLAUDE.md "Don't Say" column. The ## STRICT RULES section of _build_copy_prompt()
# enforces this at the prompt level.
# ──────────────────────────────────────────────────────────────────────────────

# ── Copy generation ────────────────────────────────────────────────────────────

# Geo → photo-subject ethnicity hints. The LLM uses these to choose subjects that
# look plausible for someone living and working in the targeted country. Without
# this, the LLM defaults to a global ethnic mix that breaks audience relatability
# (GMR-0005 surfaced this — Singapore-targeted creatives shipped with South Asian
# and Black male subjects, when the realistic Singapore-lawyer pool is Chinese
# Singaporean, Malay Singaporean, Indian Singaporean, or expat).
_GEO_ETHNICITY_HINTS: dict[str, str] = {
    "SG": "Chinese Singaporean (most common in legal/finance), Malay Singaporean, Indian Singaporean, or East Asian / South Asian expat working in Singapore",
    "IN": "Indian (regionally varied — North, South, East, or Northeast Indian)",
    "US": "Representative of the US professional mix — white, Black, Hispanic Latino, East Asian American, or South Asian American",
    "UK": "British (mixed European, South Asian British, Black British, East Asian British)",
    "CA": "Canadian (representative — white, East Asian Canadian, South Asian Canadian, Black Canadian, Indigenous)",
    "AU": "Australian (representative — white, East Asian Australian, South Asian Australian, Indigenous)",
    "DE": "German or other Continental European",
    "FR": "French (representative — white, North African French, sub-Saharan African French, mixed)",
    "ES": "Spanish or Iberian",
    "IT": "Italian",
    "BR": "Brazilian (representative — white, mixed-race, Afro-Brazilian, Indigenous)",
    "MX": "Mexican (representative — Mestizo, Indigenous, white)",
    "JP": "Japanese",
    "KR": "Korean",
    "ID": "Indonesian",
    "MY": "Malaysian (Chinese Malaysian, Malay, Indian Malaysian)",
    "PH": "Filipino",
    "VN": "Vietnamese",
    "TH": "Thai",
}


def _format_geo_hint(geos: list[str] | None) -> str:
    """Build the geo-aware photo-subject guidance block for the LLM prompt.

    Returns an empty string when no geo signal is available — the LLM falls
    back to global mix, which is correct when targeting is genuinely global.
    """
    if not geos:
        return ""
    geos_clean = [g for g in geos if g and isinstance(g, str)]
    if not geos_clean:
        return ""

    # Multi-geo (3+) → no single ethnicity fits; use a "match the audience"
    # instruction without locking to a country.
    if len(geos_clean) >= 3:
        return (
            "\n## GEO CONTEXT — photo subject ethnicity\n"
            f"This campaign targets multiple countries: {', '.join(geos_clean)}. "
            "Choose ethnicities representative of the COMBINED audience. "
            "Vary the ethnicity across the 3 angles (don't repeat the same look) — "
            "e.g., one angle Chinese Singaporean, one angle Indian Singaporean, etc.\n"
        )

    # Single or dual geo → look up specific hints.
    parts: list[str] = []
    for g in geos_clean:
        hint = _GEO_ETHNICITY_HINTS.get(g.upper())
        if hint:
            parts.append(f"- {g.upper()}: {hint}")
        else:
            parts.append(
                f"- {g.upper()}: choose an ethnicity plausible for someone living "
                f"and working in this country. Research the country's demographic "
                f"composition; do NOT default to a global mix."
            )
    return (
        "\n## GEO CONTEXT — photo subject ethnicity (CRITICAL for relatability)\n"
        "This campaign targets contributors in:\n"
        + "\n".join(parts)
        + "\n\nThe `photo_subject` ethnicity for EVERY variant MUST be plausible "
          "for the targeted geo. Audience relatability hinges on this — a Singapore "
          "lawyer ad with an African-American subject feels off-brand to the actual "
          "Singapore audience seeing it in their LinkedIn feed.\n"
    )


def build_copy_variants(
    cohort,
    layer_map: dict[str, str],
    *,
    geos: list[str] | None = None,
    claude_key: str = "",
    description_hint: str = "",
) -> list[dict]:
    """
    Generate 3 A/B/C copy variants fully derived from cohort signals — no fixed TG categories.
    The LLM infers the professional identity from the cohort name + rules and writes copy
    and photo_subject specific to that exact audience.

    `geos` (optional, ISO country codes from Smart Ramp `included_geos`) drives the
    photo_subject ethnicity choice. When set, the LLM is told which ethnicities are
    plausible for the targeted country, preventing global-mix defaults that break
    audience relatability. See `_GEO_ETHNICITY_HINTS` for the lookup.

    `description_hint` (optional, free-form text from the Smart Ramp cohort form,
    e.g., "We want cardiologists with 5+ yrs experience"). Surfaced 2026-04-29
    (GMR-0016 drift): without this hint the LLM only sees the cohort signature
    (e.g. `highest_degree_level__Phd`) and invents a generic profession ("biomedical
    researcher", "postdoc"). Pass the description to anchor the LLM on the
    requester's actual specialty so photo_subject + copy reflect it.

    Returns: [{angle, angleLabel, headline, subheadline, cta, photo_subject, tgLabel, layerUpdates}, ...]
    """
    from src.linkedin_urn import _col_to_human
    from src.analysis import _feature_to_facet

    signals = []
    for feat, _ in cohort.rules:
        human = _col_to_human(feat)
        facet = _feature_to_facet(feat)
        signals.append(f"{facet}: {human}")

    # Load competitor intel if available (from weekly competitor-bot run)
    competitor_context = ""
    import pathlib
    _intel_path = pathlib.Path("data/competitor_intel/latest.json")
    if _intel_path.exists():
        try:
            intel_data = json.loads(_intel_path.read_text())
            ideas = intel_data.get("experiment_ideas", [])
            if ideas:
                competitor_context = "\n\nCompetitor experiment ideas to consider:\n" + "\n".join(f"- {i}" for i in ideas[:3])
                log.info("Loaded %d competitor experiment ideas for copy gen", len(ideas))
        except Exception as exc:
            log.warning("Failed to load competitor intel: %s", exc)

    prompt = _build_copy_prompt(cohort.name, signals, layer_map)
    geo_hint = _format_geo_hint(geos)
    if geo_hint:
        prompt += geo_hint
    if description_hint:
        prompt += (
            "\n\nSMART RAMP REQUESTER'S AUDIENCE BRIEF (the verbatim ask from the requester for this cohort) — "
            "treat this as the AUTHORITATIVE description of who the audience actually is. The cohort signature "
            "above (skills, fields_of_study, degree_level) was mined from RESUME data and MAY be broader or less "
            "specific than the brief. When the brief names a specialty (e.g., 'cardiologists', 'tax attorneys', "
            "'civil engineers'), that specialty MUST appear in the photo_subject and headlines — do NOT default "
            "to the generic role implied by the signals.\n"
            f"Brief:\n{description_hint}\n"
        )
        log.info("Copy gen using Smart Ramp description hint: %r", description_hint[:120])
    if competitor_context:
        prompt += competitor_context

    # Validate context fields are populated before LiteLLM call
    _required_signals = ["skills", "job_titles_norm", "fields_of_study", "highest_degree_level",
                         "accreditations_norm", "experience_band"]
    populated = [s for s in signals if any(s.startswith(r.replace("_", " ")) for r in _required_signals)]
    log.info("LiteLLM copy gen — cohort=%s signals=%d populated=%d",
             cohort.name[:40], len(signals), len(populated))
    if not signals:
        log.warning("No signals for copy gen — LLM will use generic copy")

    client = _llm_client()

    # Retry loop: LLM sometimes overshoots hard limits. Retry with explicit violation list.
    variants: list[dict] = []
    last_violations: list[str] = []
    for attempt in range(3):
        if attempt > 0 and last_violations:
            retry_note = (
                "\n\nRETRY — your previous output violated the hard limits:\n"
                + "\n".join(f"- {v}" for v in last_violations)
                + "\nREWRITE so every headline is ≤6 words AND ≤40 chars, every subheadline is "
                  "≤7 words AND ≤48 chars. No exceptions."
            )
            messages = [{"role": "user", "content": prompt + retry_note}]
        else:
            messages = [{"role": "user", "content": prompt}]

        resp = client.chat.completions.create(
            model=config.LITELLM_MODEL,
            max_tokens=2048,
            messages=messages,
        )
        raw = resp.choices[0].message.content.strip()
        try:
            parsed = _extract_json(raw)
            variants = parsed.get("variants", [])
        except Exception as exc:
            log.error("Failed to parse copy variants JSON (attempt %d): %s\n%s", attempt + 1, exc, raw[:500])
            variants = []
            continue

        last_violations = _validate_copy_limits(variants)
        if not last_violations:
            break
        log.warning("Copy limits violated (attempt %d): %s", attempt + 1, last_violations)

    if last_violations:
        log.warning("Proceeding with copy that still has violations after 3 attempts: %s", last_violations)

    log.info("Generated %d copy variants for '%s'", len(variants), cohort.name)
    return variants


# ── Copy length validation ────────────────────────────────────────────────────
_HEADLINE_MAX_WORDS = 6
_HEADLINE_MAX_CHARS = 40
_SUBHEAD_MAX_WORDS  = 7
_SUBHEAD_MAX_CHARS  = 48


# Banned-token replacements (CLAUDE.md vocabulary table). Applied as deterministic
# post-processing after every LLM rewrite — LLMs reliably ignore vocab rules even when
# the prompt says not to (verified GMR-0005 dry run 2026-04-28: 'training' shipped 4
# times in a row despite explicit "BANNED" instruction). Only context-safe substitutions
# go here; `work` / `schedule` / `job` / `role` are context-sensitive and stay LLM-only.
_BANNED_TOKEN_REPLACEMENTS = {
    "compensation": "payment",
    "interview":    "screening",
    "interviews":   "screenings",
    "interviewing": "screening",
    "bonus":        "reward",
    "bonuses":      "rewards",
    "performance":  "progress",
    "training":     "project guidelines",
    "instructions": "project guidelines",
    "discourse":    "Outlier Community",
    "assign":       "match",
    "assigned":     "matched",
    "promote":      "eligible to work on review-level tasks",
}

# Em + en dashes — banned in contributor copy per CLAUDE.md. LLMs love these and will
# slip them in even with explicit "NO EM DASHES" rules in the prompt. Spaced versions
# matched first so " — " doesn't leave a stranded comma.
_DASH_REPLACEMENTS = [
    (" — ", " "),    # em dash with surrounding spaces
    (" – ", " "),    # en dash with surrounding spaces
    ("—", ","),      # bare em dash
    ("–", ","),      # bare en dash
]

# Copy fields the rewriter knows how to operate on. Order matches downstream usage.
_REWRITABLE_COPY_FIELDS = (
    "headline", "subheadline", "intro_text", "ad_headline", "ad_description",
)

# Per-field hard char limits — used for last-resort truncation if LLM still overshoots.
# Match copy_design_qc.py validate_copy_lengths constants.
_FIELD_MAX_CHARS = {
    "headline":       _HEADLINE_MAX_CHARS,
    "subheadline":    _SUBHEAD_MAX_CHARS,
    "intro_text":     140,
    "ad_headline":    70,
    "ad_description": 100,
}

# Detect which copy fields a violation message names. \b ensures `headline` doesn't
# match inside `subheadline` or `ad_headline` (underscore is a word char, so \b before
# `headline` requires a non-word char immediately to the left).
_FIELD_DETECTOR = re.compile(
    r"\b(?:Headline|Subheadline|headline|subheadline|intro_text|ad_headline|ad_description|cta_button)\b",
)


def _scrub_banned_tokens_and_dashes(text: str) -> str:
    """Strip em/en dashes and replace context-safe banned tokens.

    Deterministic, idempotent. Run after every LLM rewrite to catch the cases the
    LLM ignored. Banned tokens are matched as whole words (case-insensitive) so we
    don't break legitimate substrings (e.g. 'compensate' won't be scrubbed by the
    'compensation' rule because of \b boundaries).
    """
    if not text:
        return text
    for old, new in _DASH_REPLACEMENTS:
        text = text.replace(old, new)
    for banned, replacement in _BANNED_TOKEN_REPLACEMENTS.items():
        text = re.sub(r"\b" + re.escape(banned) + r"\b", replacement, text, flags=re.IGNORECASE)
    # Collapse the double-space that can result from " — " → " " replacement
    text = re.sub(r"  +", " ", text).strip()
    return text


def _violated_fields_from_messages(violations: list[str]) -> set[str]:
    """Parse violation strings to identify which copy fields had violations.

    QC validator emits violations in two shapes:
      - validate_copy_lengths: 'Headline has 7 words ...' or 'Headline wraps to 3 lines ...'
      - banned-token / em-dash checks: "intro_text contains em dash ..." (lowercase)
    Both shapes are covered by _FIELD_DETECTOR.
    """
    fields: set[str] = set()
    for v in violations or []:
        for match in _FIELD_DETECTOR.findall(v):
            # Normalize "Headline" → "headline" etc.
            fields.add(match.lower())
    return fields


def rewrite_variant_copy(variant: dict, violations: list[str]) -> dict:
    """
    Rewrite the violated copy fields when QC flags issues. Handles all 5 image+ad
    copy fields (headline, subheadline, intro_text, ad_headline, ad_description),
    not just headline+subheadline (the GMR-0005 dry run 2026-04-28 demonstrated
    this is required — em-dash violations on intro_text loop forever otherwise).

    Two-stage fix:
      1. LLM rewrite of every field that has a flagged violation.
      2. Deterministic post-process — strip em/en dashes and swap banned tokens —
         on EVERY field, including ones the LLM didn't touch (LLMs ignore vocab
         rules even with explicit "BANNED" instructions in the prompt).
      3. Last-resort hard truncation if rewrite still overshoots character limits.
    """
    angle = variant.get("angle", "?")
    angle_label = variant.get("angleLabel", "")
    photo_subject = variant.get("photo_subject", "")

    flagged = _violated_fields_from_messages(violations)
    # Back-compat: if violations don't name a field (or violations list is empty), default
    # to the legacy behavior of rewriting headline + subheadline.
    if not flagged:
        flagged = {"headline", "subheadline"}

    fields_to_rewrite = [f for f in _REWRITABLE_COPY_FIELDS if f in flagged]
    # If nothing rewritable was flagged (e.g., only `cta_button`), fall through with no LLM call.

    current_values = {f: (variant.get(f) or "").strip() for f in _REWRITABLE_COPY_FIELDS}
    violation_bullets = "\n".join(f"- {v}" for v in violations) or "(no details)"
    field_block = "\n".join(
        f"- {f}: {current_values[f]!r}"
        for f in fields_to_rewrite if current_values[f]
    ) or "(no current values for the fields to rewrite)"
    return_schema = "{" + ", ".join(f'"{f}": "<rewritten>"' for f in fields_to_rewrite) + "}"

    parsed: dict = {}
    if fields_to_rewrite:
        prompt = f"""\
Rewrite the violating copy fields for an Outlier LinkedIn ad variant. Fix all violations.
Return ONLY the fields you were asked to rewrite.

VIOLATIONS:
{violation_bullets}

HARD LIMITS:
- headline: MAX {_HEADLINE_MAX_WORDS} words AND MAX {_HEADLINE_MAX_CHARS} characters. MUST fit in 1-2 lines. NEVER include the word "Outlier" — the brand wordmark is composited on the image separately, repeating it in the headline causes a duplicate-logo failure.
- subheadline: MAX {_SUBHEAD_MAX_WORDS} words AND MAX {_SUBHEAD_MAX_CHARS} characters. MUST fit in 1-2 lines. NEVER include the word "Outlier" (same reason as headline).
- intro_text: MAX 140 characters. Feed text above the image. Mentioning "Outlier" is OK here.
- ad_headline: MAX 70 characters. Bold text below the image. Mentioning "Outlier" is OK here.
- ad_description: MAX 100 characters. Small text under ad_headline. Mentioning "Outlier" is OK here.

RESTRICTED VOCABULARY (BANNED — never use, in ANY field):
- "compensation" -> use "payment"
- "interview" -> use "screening"
- "bonus" -> use "reward"
- "performance" -> use "progress"
- "training" -> use "project guidelines"
- "promote" -> use "eligible to work on review-level tasks"
- "Discourse" -> use "Outlier Community"
- "assign" -> use "match"
- "instructions" -> use "project guidelines"
- "work", "schedule", "job", "role", "required" -> use NEUTRAL synonyms (task, availability, opportunity)
- NO em dashes (-) or en dashes. Use commas, periods, or colons.

CONTEXT (DO NOT CHANGE, tone alignment only):
- Angle: {angle} - {angle_label}
- Photo subject: {photo_subject!r}

CURRENT FIELD VALUES (rewrite to fix violations, preserve angle/specificity):
{field_block}

Return ONLY valid JSON, no markdown fences:
{return_schema}
"""
        try:
            client = _llm_client()
            resp = client.chat.completions.create(
                model=config.LITELLM_MODEL,
                max_tokens=600,
                messages=[{"role": "user", "content": prompt}],
            )
            raw = resp.choices[0].message.content.strip()
            parsed = _extract_json(raw) or {}
            if not isinstance(parsed, dict):
                parsed = {}
        except Exception as exc:
            log.warning("rewrite_variant_copy LLM call failed (%s) - falling back to deterministic scrub-only", exc)

    # Apply LLM rewrites (where present) on top of original variant.
    updated = dict(variant)
    for field in fields_to_rewrite:
        if field in parsed and isinstance(parsed[field], str):
            updated[field] = parsed[field].strip()

    # Deterministic post-process on EVERY rewritable field — LLM ignored vocab rules
    # in the GMR-0005 run despite explicit "BANNED" instructions; we cannot rely on it.
    for field in _REWRITABLE_COPY_FIELDS:
        val = updated.get(field)
        if val:
            updated[field] = _scrub_banned_tokens_and_dashes(val)
            # Field-scoped scrub: 'Outlier' is forbidden in headline/subheadline
            # because those get baked onto the photo where the wordmark already
            # appears in the bottom strip (vision QC reads both as logos →
            # duplicate-logo false positive). Surfaced 2026-05-04 GMR-0006.
            if field in ("headline", "subheadline"):
                # Word-boundary case-insensitive replace, collapse double spaces.
                updated[field] = re.sub(r"\s*\bOutlier\b\s*", " ", updated[field], flags=re.IGNORECASE)
                updated[field] = re.sub(r"\s+", " ", updated[field]).strip(" ,.:;-")

    # Last-resort hard truncation. Drops final word, strips trailing punctuation.
    for field, max_c in _FIELD_MAX_CHARS.items():
        val = updated.get(field) or ""
        if len(val) > max_c:
            truncated = val[:max_c].rsplit(" ", 1)[0].rstrip(",.:;")
            log.warning("rewrite_variant_copy: hard-truncated %s from %d to %d chars: %r",
                        field, len(val), len(truncated), truncated)
            updated[field] = truncated

    log.info(
        "Copy rewritten (angle %s, fields=%s): %s",
        angle, fields_to_rewrite,
        {f: updated.get(f) for f in fields_to_rewrite},
    )
    return updated


def _validate_copy_limits(variants: list[dict]) -> list[str]:
    """
    Return a list of human-readable violations for headline/subheadline length limits.
    Empty list = all variants pass.
    """
    violations: list[str] = []
    for v in variants:
        angle = v.get("angle", "?")
        h = (v.get("headline") or "").strip()
        s = (v.get("subheadline") or "").strip()
        h_words = len(h.split())
        s_words = len(s.split())
        if h_words > _HEADLINE_MAX_WORDS:
            violations.append(f"Angle {angle} headline has {h_words} words (max {_HEADLINE_MAX_WORDS}): {h!r}")
        if len(h) > _HEADLINE_MAX_CHARS:
            violations.append(f"Angle {angle} headline has {len(h)} chars (max {_HEADLINE_MAX_CHARS}): {h!r}")
        if s_words > _SUBHEAD_MAX_WORDS:
            violations.append(f"Angle {angle} subheadline has {s_words} words (max {_SUBHEAD_MAX_WORDS}): {s!r}")
        if len(s) > _SUBHEAD_MAX_CHARS:
            violations.append(f"Angle {angle} subheadline has {len(s)} chars (max {_SUBHEAD_MAX_CHARS}): {s!r}")
    return violations


def _build_copy_prompt(cohort_name: str, signals: list[str], layer_map: dict) -> str:
    layers_summary = json.dumps(layer_map, indent=2)
    signals_str    = "\n".join(f"  - {s}" for s in signals)
    cohort_label   = cohort_name.replace("__", " ").replace("_", " ")

    return f"""You are writing 3 A/B test ad creatives for **Outlier** — a platform where domain experts earn payment doing flexible, remote AI training tasks (reviewing, rating, and improving AI outputs in their field).

## YOUR FIRST JOB: IDENTIFY WHO THIS PERSON IS
Do not use any pre-defined audience categories. Derive the person's professional identity entirely from the signals below.

Cohort name (raw feature label): {cohort_label}
Signals from statistical analysis — these are the features that predict this person passes Outlier's screening:
{signals_str}

From these signals, identify:
1. Their **specific professional title** — not a broad category. E.g. "DNA sequencing researcher" not "scientist". "Research associate at a European university" not "academic". "Environmental sanitary engineer" not "engineer".
2. Their **primary daily activity** — what do they actually do at work? (sequencing DNA samples, reviewing wastewater treatment plans, teaching maths at a European university)
3. Their **schedule constraint** — what controls their time? (lab schedules, academic calendar, project contracts, clinical shifts)
4. Their **geography / language context** — any geo signals in the cohort name or accreditations? (Italian titles → EU academic, ICAO/EASA → aviation, European context)
5. Their **emotional state** — are they likely between projects? Seeking side income? Wanting flexible work?

Write all 3 copy variants for the specific person you've identified — not a generic category.

## TEXT LAYERS IN THE BASE CREATIVE
{layers_summary}

## AD FORMAT CONSTRAINTS (HARD LIMITS — text is rejected if violated)

You are producing TWO TEXT SETS per variant:

### (I) IMAGE OVERLAY TEXT (baked into the 1200×1200 creative PNG)
- **headline**: MAX 6 words AND MAX 40 characters. 1–2 lines. Bold white text over the photo. NEVER include the word "Outlier" — the brand wordmark is composited on the image separately, repeating it in the headline causes a duplicate-logo QC failure.
- **subheadline**: MAX 7 words AND MAX 48 characters. 1–2 lines. Regular white text over the photo. NEVER include the word "Outlier" (same reason as headline).
- Every word must earn its place. Short, punchy, specific. Cut filler.
- No logo or CTA text in the image itself.

### (II) LINKEDIN AD COPY (shown around the image in the LinkedIn feed)
- **intro_text**: MAX 140 characters. The text that appears ABOVE the image in the feed. Must land the hook in the first line — feed preview cuts off after ~140 chars. Start with a question, bold claim, or specific pain point. NOT a restating of the headline.
- **ad_headline**: MAX 70 characters. Bold text that appears BELOW the image in the feed. Should complement the overlay headline (don't duplicate it verbatim — reinforce or extend the angle).
- **ad_description**: MAX 100 characters. Small text under ad_headline. OPTIONAL (can be empty string) but recommended for stronger conversion — use to reinforce earnings/flexibility or add a specific proof point.
- **cta_button**: ALWAYS "APPLY" (Outlier's funnel = screening application). Do not pick anything else.

No variant ships if ANY of the 5 text fields above violates its length limit. The copy rewriter will kick the variant back to you with a list of violations.

## 3 COPY ANGLES

### Variant A — Expertise / AI Experience Hook
**Insight:** This person has niche expertise and wants meaningful use of it. They may be between tasks or looking for flexible extra income.
**Pattern:** Name their specific professional moment → reveal that their exact skill has AI value.
**Examples of good openers:** "In between DNA sequencing projects?", "Between lab rotations?", "AI needs your metagenomics expertise.", "Get AI experience as a research associate."
**Never:** "Your expertise is in demand." (too generic) — always name the specific expertise.

### Variant B — Earnings / Social Proof Hook
**Insight:** Proof that people like them are already earning real money.
**Pattern:** Bold social proof stat or earnings claim → aspirational pull.
**Examples:** "Over 500 environmental engineers paid.", "We've paid $500M+ to domain experts like you.", "Researchers across Europe earn with Outlier."
**Never:** Generic "thousands of professionals paid." — name the exact peer group.

### Variant C — Flexibility / Lifestyle Hook
**Insight:** Their schedule is controlled by something external (lab hours, academic calendar, project deadlines, shifts). Freedom from that is aspirational.
**Pattern:** Bold declaration naming their specific constraint → low-friction income claim.
**Examples:** "Lab hours don't have to own your income.", "Academic schedules leave room to earn.", "Between contracts? Work on AI, from home."
**Never:** A question. Always a statement. **Never** "Work on your terms." — name the specific constraint.

## STRICT RULES
1. **NEVER start a headline with the audience name as a label** — "DNA Researchers:" is wrong. The profession can appear naturally mid-sentence.
2. Each variant must have a COMPLETELY DIFFERENT opening structure — question vs. statement vs. bold claim.
3. Do NOT invent earnings figures — only use numbers if the layer text already contains them.
4. Headline and subheadline are separate fields — never merge them. Same for intro_text and ad_headline (they carry different angles on the same value prop — never duplicate).
5. Human and specific, not corporate. Write how a sharp, friendly colleague would put it.

## OUTLIER BRAND VOICE — MANDATORY (copy is rejected if violated)

### Restricted Vocabulary (banned — use alternative)
The following words are BANNED in every field (headline, subheadline, intro_text, ad_headline, ad_description). Scan every field before returning.

| Banned | Use Instead |
|---|---|
| work, job | contribute, contributing, tasks, projects, opportunity |
| performance | progress (or rephrase) |
| assign | match, matching |
| bonus | reward, extra |
| role, position | opportunity |
| training, learning, growth | session, walk through, get familiar with project guidelines |
| employee, worker | contributor |
| schedule, shift | rephrase completely — these words imply employment |
| team (as org membership) | rephrase — "part of this project" if needed |
| Mango, MultiMango | Aether |
| Discourse | Outlier Community |
| sprint | window, push |
| compensation, salary | payment |
| required | strongly encouraged |
| interview | screening |
| instructions | project guidelines |
| remove from project | release from project |
| promote | eligible to work on review-level tasks |

### Banned Filler + AI Vocabulary
ZERO tolerance for any of these tokens anywhere in any field:
- Filler: genuinely, honestly, truly, actually, really, very, so, just
- AI vocabulary: delve, landscape, leverage, foster, robust, holistic, dive into, unpack, game-changer, cutting-edge, revolutionary, seamless, transformative, tapestry, realm, journey, testament, at the end of the day
- Corporate openers: "we're excited to," "we'd love to," "we wanted to reach out," "as you may be aware," "Great news:", "Good news:", "We're thrilled," "We can't wait!", "I'm excited to announce"

### Structure Rules
- No throat-clearing ("here's the thing," "quick pitch:", "let me be clear", "excited to share")
- No fake discovery arcs ("But here's what surprised me," "That's when it hit me")
- No bait questions at end ("What do you think?")
- No weird urgency ("this is your moment", "don't miss this", "now's your chance")
- Lead with the concrete thing — the artifact, number, story, or example — then explain

### Formatting Rules
- Sentence case everywhere. First word + proper nouns only. NO ALL CAPS anywhere.
- "Outlier" always capitalized
- Spell out numbers under ten in prose (one, five, seven). Numerals for money ($25), percentages (5%), thresholds.
- NO em dashes in contributor content. Use commas, periods, colons, or parentheses.
- Oxford commas
- Contractions throughout (we're, it's, they're) — no formal expansions
- NO hashtags
- Start some sentences with "And," "But," "So," "Because" — sounds human

### Calibrated Warmth
- Don't overstate emotional stakes
- Exclamation points: 0–1 per variant. NEVER on deadlines or facts.
- Stress test: would this feel strange to say out loud to a friend? If yes, it's wrong.

### AI/LLM References
- NEVER name specific models (ChatGPT, Claude, Gemini, Grok, DeepSeek)
- Use instead: "AI tools", "AI models", "frontier models"

### Read-Aloud Test
Before returning, mentally read each field out loud. Does it sound like a ChatGPT response, a keynote speech, or a LinkedIn announcement? → rewrite. Does it sound like a sharp friend in this profession talking to peers? → keep it.

## PHOTO SUBJECT (one per variant)
Each variant needs a `photo_subject` — a SHORT, specific scene description for the background photo generator.

Format: "[gender] [ethnicity] [specific profession title], [specific activity at home]"

Derive this from the professional identity you identified above. Never use generic descriptions.

Good examples:
  - "male Northern European DNA sequencing researcher, reviewing sequencing data on a laptop at home"
  - "female Italian research associate, reading academic papers on a laptop at a home desk"
  - "male South Asian environmental engineer, reviewing technical drawings on a laptop at home"

Bad examples (never do these):
  - "professional person working at a laptop at home" — too generic
  - "scientist at a computer" — too vague, no ethnicity, no activity

The photo generator will add: close-up portrait framing, plant-filled home interior, natural window light, warm film aesthetic.

## RESPONSE FORMAT
Return ONLY valid JSON, no other text. Every variant MUST contain all 8 fields below:
```json
{{
  "tg_label": "<your derived human-readable label for this TG, e.g. 'European DNA sequencing researcher'>",
  "variants": [
    {{
      "angle": "A",
      "angleLabel": "Expertise Hook",
      "headline": "<≤6 words, ≤40 chars — image overlay>",
      "subheadline": "<≤7 words, ≤48 chars — image overlay>",
      "intro_text": "<≤140 chars — feed text ABOVE image>",
      "ad_headline": "<≤70 chars — bold text BELOW image>",
      "ad_description": "<≤100 chars — small text, optional but recommended>",
      "cta_button": "APPLY",
      "photo_subject": "<[gender] [ethnicity] [profession], [activity]>",
      "layerUpdates": {{"<node_id_from_layers>": "new text"}}
    }},
    {{
      "angle": "B",
      "angleLabel": "Earnings Hook",
      "headline": "...",
      "subheadline": "...",
      "intro_text": "...",
      "ad_headline": "...",
      "ad_description": "...",
      "cta_button": "APPLY",
      "photo_subject": "...",
      "layerUpdates": {{"<node_id_from_layers>": "new text"}}
    }},
    {{
      "angle": "C",
      "angleLabel": "Flexibility Hook",
      "headline": "...",
      "subheadline": "...",
      "intro_text": "...",
      "ad_headline": "...",
      "ad_description": "...",
      "cta_button": "APPLY",
      "photo_subject": "...",
      "layerUpdates": {{"<node_id_from_layers>": "new text"}}
    }}
  ]
}}
```"""


def _extract_json(raw: str) -> dict:
    """Extract JSON from Claude response, handling ```json fences."""
    # Try to find JSON block
    match = re.search(r"```(?:json)?\s*([\s\S]+?)```", raw)
    if match:
        return json.loads(match.group(1).strip())
    # Try raw JSON
    start = raw.find("{")
    end   = raw.rfind("}") + 1
    if start != -1 and end > start:
        return json.loads(raw[start:end])
    raise ValueError("No JSON found in response")


# ── Plugin logic via use_figma MCP ─────────────────────────────────────────────

def apply_plugin_logic(
    file_key: str,
    base_node_id: str,
    variants: list[dict],
    tg_category: str,
    claude_key: str,
    mcp_url: str | None = None,
) -> list[str]:
    """
    Execute the plugin's apply handler in Figma via Claude API + use_figma MCP.
    Clones the base frame × len(variants), applies text + design customization.
    Returns list of created clone node IDs.
    """
    mcp_url = mcp_url or config.MCP_FIGMA_URL

    js_code = _build_apply_js(base_node_id, variants, tg_category)

    prompt = (
        "Execute the following JavaScript in Figma using the use_figma tool. "
        "Pass the code exactly as written with skillNames: 'figma-use'. "
        "Return the result from the tool call — specifically the clone node IDs.\n\n"
        f"```javascript\n{js_code}\n```"
    )

    client = anthropic.Anthropic(api_key=claude_key)

    try:
        resp = client.beta.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=4096,
            betas=["mcp-client-2025-04-04"],
            mcp_servers=[{
                "type": "url",
                "url": mcp_url,
                "name": "figma-mcp",
            }],
            messages=[{"role": "user", "content": prompt}],
        )
    except Exception as exc:
        log.error("apply_plugin_logic MCP call failed: %s", exc)
        raise

    # Parse clone IDs from response
    clone_ids = _parse_clone_ids(resp)
    log.info("Plugin logic created %d clones: %s", len(clone_ids), clone_ids)
    return clone_ids


def _parse_clone_ids(resp) -> list[str]:
    """Extract clone node IDs from the Claude/MCP response."""
    # Walk content blocks looking for tool result with clone IDs
    for block in resp.content:
        block_dict = block if isinstance(block, dict) else (block.model_dump() if hasattr(block, "model_dump") else {})
        text = ""
        if block_dict.get("type") == "text":
            text = block_dict.get("text", "")
        elif block_dict.get("type") == "tool_result":
            content = block_dict.get("content", "")
            text = content if isinstance(content, str) else json.dumps(content)

        if not text:
            continue

        # Try to extract JSON with cloneIds
        try:
            data = _extract_json(text)
            ids = data.get("cloneIds") or data.get("clone_ids") or data.get("createdNodeIds") or []
            if ids:
                return [str(i) for i in ids]
        except Exception:
            pass

        # Fallback: scan for node ID patterns (e.g. "123:456")
        found = re.findall(r'\b\d+:\d+\b', text)
        if found:
            return found

    return []


# ── JavaScript template ────────────────────────────────────────────────────────

def _build_apply_js(base_node_id: str, variants: list[dict], tg_category: str) -> str:
    """
    Build the JavaScript string that mirrors the plugin's apply handler.
    Embeds all helper functions + drawing functions from code.js verbatim.
    """
    variants_json   = json.dumps(variants)
    palettes_json   = json.dumps(TG_PALETTES)
    illus_var_json  = json.dumps(TG_ILLUS_VARIANTS)

    return f"""
// ── Injected data ──
const BASE_NODE_ID      = {json.dumps(base_node_id)};
const VARIANTS_PAYLOAD  = {variants_json};
const TG_PALETTES       = {palettes_json};
const TG_ILLUS_VARIANTS = {illus_var_json};
const TG_CATEGORY       = {json.dumps(tg_category)};

// ── Helpers (verbatim from figma-ad-modifier/code.js) ──

function safeVal(v) {{
  try {{ return v === figma.mixed ? null : v; }} catch (_) {{ return null; }}
}}

function parseBold(str) {{
  const boldRanges = [];
  let plain = "", i = 0;
  while (i < str.length) {{
    if (str[i] === "*" && str[i+1] === "*") {{
      const close = str.indexOf("**", i+2);
      if (close !== -1) {{
        const start = plain.length;
        plain += str.slice(i+2, close);
        boldRanges.push({{ start, end: plain.length }});
        i = close + 2;
      }} else {{ plain += str[i++]; }}
    }} else {{ plain += str[i++]; }}
  }}
  return {{ text: plain, boldRanges }};
}}

function getFontFamily(textNode) {{
  const fn = safeVal(textNode.fontName);
  if (fn) return fn.family;
  try {{
    const range = textNode.getRangeFontName(0,1);
    if (range && range !== figma.mixed) return range.family;
  }} catch (_) {{}}
  return "Inter";
}}

async function loadAllFonts(textNode) {{
  const seen = new Set();
  async function tryLoad(fn) {{
    if (!fn || fn === figma.mixed) return;
    const key = fn.family + "::" + fn.style;
    if (seen.has(key)) return;
    seen.add(key);
    try {{ await figma.loadFontAsync(fn); }} catch (_) {{}}
  }}
  const len = textNode.characters.length;
  for (let i = 0; i < len; i++) {{
    try {{ const fn = textNode.getRangeFontName(i,i+1); await tryLoad(fn); }} catch (_) {{}}
  }}
  await tryLoad(safeVal(textNode.fontName));
  if (!seen.size) await figma.loadFontAsync({{ family:"Inter", style:"Regular" }});
}}

function dfsNodes(root) {{
  const result = [];
  function walk(n) {{ result.push(n); if ("children" in n) n.children.forEach(walk); }}
  walk(root);
  return result;
}}

function applyBoldRanges(target, boldRanges, family, maxLen) {{
  for (const {{ start, end }} of boldRanges) {{
    if (start >= maxLen) continue;
    const clampedEnd = Math.min(end, maxLen);
    try {{ target.setRangeFontName(start, clampedEnd, {{ family, style:"Bold" }}); }}
    catch (_) {{ try {{ target.setRangeFontName(start, clampedEnd, {{ family:"Inter", style:"Bold" }}); }} catch (_) {{}} }}
  }}
}}

const MIN_FONT_SIZE = 11;

async function setTextWithFit(target, plainText, boldRanges, family) {{
  const origHeight = target.height, origWidth = target.width;
  const origResize = target.textAutoResize, origFontSize = safeVal(target.fontSize);
  target.characters = plainText;
  applyBoldRanges(target, boldRanges, family, plainText.length);
  if (origResize !== "NONE" || !origFontSize) return;
  target.textAutoResize = "HEIGHT";
  if (target.height > origHeight) {{
    let size = origFontSize;
    while (target.height > origHeight && size > MIN_FONT_SIZE) {{
      size = Math.max(size - 0.5, MIN_FONT_SIZE);
      try {{ target.fontSize = size; }} catch (_) {{ break; }}
    }}
  }}
  target.textAutoResize = "NONE";
  target.resize(origWidth, origHeight);
}}

function findColoredEllipses(root) {{
  var result = [];
  function walk(node) {{
    if ((node.type==="ELLIPSE"||node.type==="RECTANGLE") && node.width>40 && node.height>40) {{
      var fills; try {{ fills=node.fills; }} catch(_) {{ return; }}
      if (fills && fills!==figma.mixed && fills.length>0) {{
        var f=fills[0];
        if (f && f.type==="SOLID" && f.color) {{
          var c=f.color, isLight=c.r>0.55&&c.g>0.55&&c.b>0.55;
          var notWhite=!(c.r>0.95&&c.g>0.95&&c.b>0.95), notBlack=!(c.r<0.10&&c.g<0.10&&c.b<0.10);
          if (isLight&&notWhite&&notBlack) result.push(node);
        }}
      }}
    }}
    if ("children" in node) node.children.forEach(walk);
  }}
  if ("children" in root) root.children.forEach(walk);
  return result.slice(0,3);
}}

function findIllustrationFrame(root) {{
  var best=null, bestScore=0;
  var illustRe=/illust|icon|graphic|visual|art|symbol|character|svg/i;
  function walk(node,depth) {{
    if (depth>4) return;
    if ((node.type==="FRAME"||node.type==="GROUP")&&"children" in node) {{
      var score=illustRe.test(node.name)?15:0;
      score+=dfsNodes(node).filter(n=>n.type==="VECTOR"||n.type==="BOOLEAN_OPERATION").length;
      if (score>bestScore&&score>=4) {{ bestScore=score; best=node; }}
    }}
    if ("children" in node) node.children.forEach(c=>walk(c,depth+1));
  }}
  if ("children" in root) root.children.forEach(c=>walk(c,1));
  return best;
}}

function clearFrame(frame) {{
  if (!("children" in frame)) return;
  var kids=Array.from(frame.children);
  for (var i=0;i<kids.length;i++) {{ try {{ kids[i].remove(); }} catch(_) {{}} }}
}}

function drawBarChart(frame) {{
  var w=frame.width,h=frame.height;
  var blue={{r:0.05,g:0.60,b:1.00}},darkBlue={{r:0.02,g:0.38,b:0.78}},ltBlue={{r:0.75,g:0.88,b:1.00}};
  var cx=w*0.10,cy=h*0.08,cw=w*0.80,ch=h*0.68;
  var barHeights=[0.45,0.72,0.55,0.90,0.62],barSpacing=cw/5,barW=cw/9;
  for (var i=0;i<=2;i++) {{ var g=figma.createRectangle(); g.x=cx; g.y=cy+ch*(i/2); g.resize(cw,0.8); g.fills=[{{type:"SOLID",color:ltBlue,opacity:0.5}}]; frame.appendChild(g); }}
  for (var i=0;i<5;i++) {{ var bh=ch*barHeights[i]; var bar=figma.createRectangle(); bar.x=cx+i*barSpacing+barSpacing/2-barW/2; bar.y=cy+ch-bh; bar.resize(barW,bh); bar.cornerRadius=2; bar.fills=[{{type:"SOLID",color:blue,opacity:0.55+i*0.08}}]; frame.appendChild(bar); }}
  try {{ var pts=barHeights.map((bh,i)=>(cx+i*barSpacing+barSpacing/2).toFixed(1)+" "+(cy+ch-ch*bh).toFixed(1)); var vec=figma.createVector(); vec.vectorPaths=[{{windingRule:"NONE",data:"M "+pts.join(" L ")}}]; vec.strokes=[{{type:"SOLID",color:darkBlue}}]; vec.strokeWeight=1.5; vec.fills=[]; frame.appendChild(vec); }} catch(_) {{}}
  var axis=figma.createRectangle(); axis.x=cx; axis.y=cy+ch; axis.resize(cw,1); axis.fills=[{{type:"SOLID",color:ltBlue}}]; frame.appendChild(axis);
}}

function drawNeuralNet(frame) {{
  var w=frame.width,h=frame.height;
  var blue={{r:0.05,g:0.60,b:1.00}},ltBlue={{r:0.75,g:0.88,b:1.00}};
  var layers=[{{x:w*0.14,ys:[0.22,0.42,0.62,0.82]}},{{x:w*0.50,ys:[0.18,0.38,0.58,0.78]}},{{x:w*0.86,ys:[0.30,0.52,0.74]}}];
  var r=Math.min(w,h)*0.042;
  for (var li=0;li<layers.length-1;li++) {{ var cur=layers[li],nxt=layers[li+1]; for (var ai=0;ai<cur.ys.length;ai++) {{ for (var bi=0;bi<nxt.ys.length;bi++) {{ try {{ var conn=figma.createVector(); conn.vectorPaths=[{{windingRule:"NONE",data:"M "+cur.x.toFixed(1)+" "+(h*cur.ys[ai]).toFixed(1)+" L "+nxt.x.toFixed(1)+" "+(h*nxt.ys[bi]).toFixed(1)}}]; conn.strokes=[{{type:"SOLID",color:ltBlue,opacity:0.35}}]; conn.strokeWeight=0.7; conn.fills=[]; frame.appendChild(conn); }} catch(_) {{}} }} }} }}
  for (var li=0;li<layers.length;li++) {{ var layer=layers[li]; for (var ni=0;ni<layer.ys.length;ni++) {{ var circle=figma.createEllipse(); circle.x=layer.x-r; circle.y=h*layer.ys[ni]-r; circle.resize(r*2,r*2); if (li===1) {{ circle.fills=[{{type:"SOLID",color:blue,opacity:0.85}}]; circle.strokes=[]; }} else {{ circle.fills=[{{type:"SOLID",color:{{r:1,g:1,b:1}},opacity:0.9}}]; circle.strokes=[{{type:"SOLID",color:blue}}]; circle.strokeWeight=1.5; }} frame.appendChild(circle); }} }}
}}

function drawBrainOutline(frame) {{
  var w=frame.width,h=frame.height,purple={{r:0.50,g:0.35,b:0.90}},ltPurple={{r:0.85,g:0.80,b:1.00}};
  try {{ var brain=figma.createVector(); brain.vectorPaths=[{{windingRule:"NONZERO",data:"M "+(w*0.50)+" "+(h*0.82)+" C "+(w*0.18)+" "+(h*0.82)+" "+(w*0.08)+" "+(h*0.60)+" "+(w*0.12)+" "+(h*0.44)+" C "+(w*0.16)+" "+(h*0.28)+" "+(w*0.30)+" "+(h*0.18)+" "+(w*0.50)+" "+(h*0.18)+" C "+(w*0.70)+" "+(h*0.18)+" "+(w*0.84)+" "+(h*0.28)+" "+(w*0.88)+" "+(h*0.44)+" C "+(w*0.92)+" "+(h*0.60)+" "+(w*0.82)+" "+(h*0.82)+" "+(w*0.50)+" "+(h*0.82)+" Z"}}]; brain.strokes=[{{type:"SOLID",color:purple}}]; brain.strokeWeight=2; brain.fills=[{{type:"SOLID",color:ltPurple,opacity:0.30}}]; frame.appendChild(brain); var wave=figma.createVector(); wave.vectorPaths=[{{windingRule:"NONE",data:"M "+(w*0.16)+" "+(h*0.50)+" C "+(w*0.22)+" "+(h*0.43)+" "+(w*0.30)+" "+(h*0.57)+" "+(w*0.38)+" "+(h*0.50)+" C "+(w*0.44)+" "+(h*0.43)+" "+(w*0.48)+" "+(h*0.50)+" "+(w*0.49)+" "+(h*0.55)+" M "+(w*0.84)+" "+(h*0.50)+" C "+(w*0.78)+" "+(h*0.43)+" "+(w*0.70)+" "+(h*0.57)+" "+(w*0.62)+" "+(h*0.50)+" C "+(w*0.56)+" "+(h*0.43)+" "+(w*0.52)+" "+(h*0.50)+" "+(w*0.51)+" "+(h*0.55)}}]; wave.strokes=[{{type:"SOLID",color:purple,opacity:0.55}}]; wave.strokeWeight=1.5; wave.fills=[]; frame.appendChild(wave); }} catch(_) {{}}
}}

function drawSpeechBubble(frame) {{
  var w=frame.width,h=frame.height,green={{r:0.13,g:0.70,b:0.45}},ltGreen={{r:0.75,g:0.95,b:0.85}};
  try {{ var bubble=figma.createRectangle(); bubble.x=w*0.10; bubble.y=h*0.10; bubble.resize(w*0.80,h*0.55); bubble.cornerRadius=Math.min(w,h)*0.12; bubble.fills=[{{type:"SOLID",color:ltGreen,opacity:0.5}}]; bubble.strokes=[{{type:"SOLID",color:green}}]; bubble.strokeWeight=2; frame.appendChild(bubble); var tail=figma.createVector(); tail.vectorPaths=[{{windingRule:"NONZERO",data:"M "+(w*0.25)+" "+(h*0.65)+" L "+(w*0.18)+" "+(h*0.82)+" L "+(w*0.36)+" "+(h*0.65)+" Z"}}]; tail.fills=[{{type:"SOLID",color:ltGreen,opacity:0.5}}]; tail.strokes=[{{type:"SOLID",color:green}}]; tail.strokeWeight=1.5; frame.appendChild(tail); [0.27,0.38,0.49].forEach((yFrac,i)=>{{ var line=figma.createRectangle(); line.x=w*0.20; line.y=h*yFrac; line.resize(w*(i===1?0.35:0.50),h*0.04); line.cornerRadius=2; line.fills=[{{type:"SOLID",color:green,opacity:0.65}}]; frame.appendChild(line); }}); }} catch(_) {{}}
}}

function drawCodeBrackets(frame) {{
  var w=frame.width,h=frame.height,blue={{r:0.05,g:0.60,b:1.00}},ltBlue={{r:0.75,g:0.88,b:1.00}};
  try {{ var lb=figma.createVector(); lb.vectorPaths=[{{windingRule:"NONE",data:"M "+(w*0.40)+" "+(h*0.22)+" L "+(w*0.22)+" "+(h*0.50)+" L "+(w*0.40)+" "+(h*0.78)}}]; lb.strokes=[{{type:"SOLID",color:blue}}]; lb.strokeWeight=4; lb.fills=[]; frame.appendChild(lb); var rb=figma.createVector(); rb.vectorPaths=[{{windingRule:"NONE",data:"M "+(w*0.60)+" "+(h*0.22)+" L "+(w*0.78)+" "+(h*0.50)+" L "+(w*0.60)+" "+(h*0.78)}}]; rb.strokes=[{{type:"SOLID",color:blue}}]; rb.strokeWeight=4; rb.fills=[]; frame.appendChild(rb); var slash=figma.createVector(); slash.vectorPaths=[{{windingRule:"NONE",data:"M "+(w*0.56)+" "+(h*0.24)+" L "+(w*0.44)+" "+(h*0.76)}}]; slash.strokes=[{{type:"SOLID",color:ltBlue}}]; slash.strokeWeight=3; slash.fills=[]; frame.appendChild(slash); }} catch(_) {{}}
}}

function customizeDesign(frame, tgCategory, variantIndex) {{
  var palette=TG_PALETTES[tgCategory], illustTypes=TG_ILLUS_VARIANTS[tgCategory]||["chart"];
  var illustType=illustTypes[(variantIndex||0)%illustTypes.length];
  if (palette) {{
    var blobs=findColoredEllipses(frame);
    for (var i=0;i<blobs.length;i++) {{
      var color=palette[i%palette.length];
      try {{ var origFills=blobs[i].fills; var opacity=(origFills&&origFills!==figma.mixed&&origFills.length>0&&origFills[0].opacity!=null)?origFills[0].opacity:0.5; blobs[i].fills=[{{type:"SOLID",color:color,opacity:opacity}}]; }} catch(_) {{}}
    }}
  }}
  if (illustType) {{
    var illustFrame=findIllustrationFrame(frame);
    if (illustFrame) {{
      clearFrame(illustFrame);
      if (illustType==="chart") drawBarChart(illustFrame);
      else if (illustType==="neural") drawNeuralNet(illustFrame);
      else if (illustType==="brain") drawBrainOutline(illustFrame);
      else if (illustType==="speech") drawSpeechBubble(illustFrame);
      else if (illustType==="code") drawCodeBrackets(illustFrame);
    }}
  }}
}}

// ── Apply handler ──

const base = await figma.getNodeByIdAsync(BASE_NODE_ID);
if (!base) return {{ error: "Base frame not found: " + BASE_NODE_ID }};

var startX = base.x + base.width;
var parent = base.parent || figma.currentPage;
if ("children" in parent) {{
  for (var si=0;si<parent.children.length;si++) {{
    var sib=parent.children[si];
    if ("x" in sib && "width" in sib) {{ var sibRight=sib.x+sib.width; if (sibRight>startX) startX=sibRight; }}
  }}
}}

const baseNodes=dfsNodes(base);
const idToIndex={{}};
baseNodes.forEach((n,i)=>{{ idToIndex[n.id]=i; }});

const cloneIds=[];

for (let vi=0;vi<VARIANTS_PAYLOAD.length;vi++) {{
  const variant=VARIANTS_PAYLOAD[vi];
  const clone=base.clone();
  clone.x=startX+80+vi*(base.width+80);
  clone.y=base.y;
  (base.parent||figma.currentPage).appendChild(clone);

  const cloneNodes=dfsNodes(clone);

  for (const [originalId,newText] of Object.entries(variant.layerUpdates||{{}})) {{
    if (!newText) continue;
    const idx=idToIndex[originalId];
    if (idx===undefined) continue;
    const target=cloneNodes[idx];
    if (!target||target.type!=="TEXT") continue;
    try {{
      const {{text:plainText,boldRanges}}=parseBold(newText);
      const family=getFontFamily(target);
      await loadAllFonts(target);
      if (boldRanges.length) {{ try {{ await figma.loadFontAsync({{family,style:"Bold"}}); }} catch(_) {{ try {{ await figma.loadFontAsync({{family:"Inter",style:"Bold"}}); }} catch(_) {{}} }} }}
      await setTextWithFit(target,plainText,boldRanges,family);
    }} catch(e) {{}}
  }}

  const tgCat=variant.tgCategory||TG_CATEGORY;
  if (tgCat&&tgCat!=="GENERAL") {{
    try {{ customizeDesign(clone,tgCat,vi); }} catch(e) {{}}
  }}

  const label=variant.variantLabel||(variant.angle+" · "+variant.angleLabel)||"";
  clone.name=(label?" "+label+" — ":"")+base.name;
  cloneIds.push(clone.id);
}}

return {{ cloneIds, count: cloneIds.length }};
"""

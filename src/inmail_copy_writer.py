"""
InMail copy writer for LinkedIn Message Ad campaigns.

Generates angle variants for a given TG. The FINANCIAL angle is always the
control/baseline — data shows it consistently outperforms Expertise and
Flexibility angles on CTR and CPA across all geos and TGs. Experimental
angles (Expertise, Flexibility, Urgency, etc.) rotate as challengers.

New angles can be injected via hypothesis dicts from the competitor agent —
see build_inmail_variants() for the interface.

## Context notes for generation

GEO RESTRICTIONS: Campaigns do not always run in all geos by our own choice.
Client restrictions may limit which geos a campaign is eligible to run in.
Do not assume poor geo coverage = poor TG — it may be a client constraint.
When comparing performance across geos, flag this as a possible confounder.

TG EARNINGS BASELINE: Lower conversion rates for high-skill TGs (e.g. Math,
PhD-level STEM) do not necessarily mean the TG is weak. These professionals
may have higher average earnings and face less financial urgency, making them
harder to convert via financial hooks alone. Compare within-TG (angle vs.
angle) rather than across TGs when evaluating copy effectiveness.
"""
import logging
from dataclasses import dataclass

from openai import OpenAI

log = logging.getLogger(__name__)

# ── Angle configs ──────────────────────────────────────────────────────────────
# FINANCIAL is the proven control. All data (90-day Snowflake analysis) shows
# financial subject hooks beat expertise/flexibility hooks on CTR and CPA.
# Rule: rate must appear in the subject. Never say "up to $X" (ceiling framing
# kills CTR). Never say "Side Income" (lowers perceived value).
# Winning subject formats:
#   "[Role] | Flexible Hours & $X/hr"
#   "[Skill] + AI = Flexible $X/hr"
#   "Earn $X/hr with [Skill] + AI"

ANGLE_CONFIGS = {
    "F": {
        "label":         "Financial",
        "hook":          "specific hourly rate + role — lead with the number",
        "tone":          "direct, factual, outcome-first",
        "subject_style": (
            "Include the exact hourly rate. Use one of these proven formats: "
            "'[Role] | Flexible Hours & $X/hr'  OR  '[Skill] + AI = Flexible $X/hr'  OR  "
            "'Earn $X/hr with [Skill] + AI'. Never say 'up to', never say 'Side Income'."
        ),
        "body_structure": (
            "Open with the payment figure or rate → "
            "briefly explain what contributors do on Outlier (review, rate, generate AI content) → "
            "weekly payment + fully flexible schedule → "
            "no commitment, no minimum hours → "
            "what a typical task looks like for this specific TG → "
            "call to action"
        ),
        "is_control": True,
    },
    "A": {
        "label":         "Expertise",
        "hook":          "specific professional moment — recognition of their domain expertise",
        "tone":          "respectful, peer-to-peer, thoughtful",
        "subject_style": (
            "Role/skill recognition — make them feel seen. Still include a rate or "
            "earning signal (e.g. '$50/hr' or 'well-paid') to avoid pure vanity framing."
        ),
        "body_structure": (
            "Open by acknowledging their specific expertise → "
            "explain what Outlier is in one sentence → "
            "why their background specifically matters for AI tasks → "
            "what a typical task looks like → "
            "mention payment weekly as a supporting fact → "
            "close with a clear, low-friction call to action"
        ),
        "is_control": False,
    },
    "B": {
        "label":         "Earnings",
        "hook":          "peer group stat + payment figure",
        "tone":          "factual, motivating, social proof",
        "subject_style": "lead with a payment figure or '1,000+ [TG] professionals' stat",
        "body_structure": (
            "Open with the payment figure or peer count → "
            "briefly explain what contributors do on Outlier → "
            "weekly payment + flexible schedule → "
            "no commitment, work as much or as little as you want → "
            "call to action"
        ),
        "is_control": False,
    },
    "C": {
        "label":         "Flexibility",
        "hook":          "lifestyle / schedule declaration",
        "tone":          "relaxed, empowering, lifestyle-led",
        "subject_style": (
            "Lead with time freedom or 'no fixed schedule'. Include an earning "
            "signal in the subject — pure lifestyle framing without a rate underperforms."
        ),
        "body_structure": (
            "Open with the schedule freedom (no shifts, no deadlines) → "
            "what Outlier tasks actually look like → "
            "mention weekly payment as a supporting fact, not the main hook → "
            "Outlier's scale / credibility → "
            "call to action"
        ),
        "is_control": False,
    },
}

VOCAB_RULES = """
VOCABULARY RULES — strictly enforce, no exceptions:
- Never say "job"           → say "opportunity" or "task"
- Never say "role" or "position" → say "opportunity"
- Never say "training" or "learning" → say "become familiar with project guidelines"
- Never say "compensation"  → say "payment"
- Never say "bonus"         → say "reward"
- Never say "assign"        → say "match"
- Never say "instructions"  → say "project guidelines"
- Never say "interview"     → say "screening"
- Never say "performance"   → say "progress"
- Never say "team"          → say "part of this project" or "member of this group"
- Never say "required"      → say "strongly encouraged"
- Never say "promote"       → say "eligible to work on review-level tasks"
"""


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class InMailVariant:
    angle:       str   # "A" | "B" | "C"
    angle_label: str   # "Expertise" | "Earnings" | "Flexibility"
    subject:     str   # ≤60 chars — InMail subject line
    body:        str   # 150–300 words — plain text, no bullet points
    cta_label:   str   # ≤20 chars — button label
    cta_url:     str   # destination URL


# ── Public API ─────────────────────────────────────────────────────────────────

def build_inmail_variants(
    tg_category: str,
    cohort,
    claude_key: str,
    destination_url: str = "https://app.outlier.ai/en/contributors/projects",
    angle_keys: list[str] | None = None,
    extra_angles: dict | None = None,
    hourly_rate: str = "$50",
) -> list[InMailVariant]:
    """
    Generate InMail variants for the given TG/cohort.

    By default runs the Financial control (F) + challengers A, B, C.
    Pass angle_keys to override which angles to generate (e.g. ["F", "A"]).
    Pass extra_angles to inject competitor-hypothesis angles:
        extra_angles = {
            "D": {
                "label": "Urgency",
                "hook": "limited spots / high demand signal",
                "tone": "...", "subject_style": "...", "body_structure": "...",
                "is_control": False,
            }
        }
    hourly_rate: rate to inject into financial/subject prompts (e.g. "$50", "$80").

    Falls back to hardcoded defaults if LiteLLM call fails.
    Returns list of InMailVariant objects in the order of angle_keys.
    """
    import config
    client = OpenAI(base_url=config.LITELLM_BASE_URL, api_key=config.LITELLM_API_KEY)
    cohort_summary = _cohort_summary(cohort, tg_category)
    variants: list[InMailVariant] = []

    all_angles = {**ANGLE_CONFIGS, **(extra_angles or {})}
    keys_to_run = angle_keys or list(all_angles.keys())

    for angle_key in keys_to_run:
        angle_cfg = all_angles.get(angle_key)
        if not angle_cfg:
            log.warning("Unknown angle key '%s', skipping", angle_key)
            continue
        prompt = _build_prompt(cohort_summary, tg_category, angle_key, angle_cfg, hourly_rate=hourly_rate)
        try:
            response = client.chat.completions.create(
                model=config.LITELLM_MODEL,
                max_tokens=1024,
                messages=[{"role": "user", "content": prompt}],
            )
            parsed = _parse_response(response.choices[0].message.content.strip())
            subject = parsed.get("subject", _fallback_subject(tg_category, angle_key))
            cta_label = parsed.get("cta_label", "See Opportunities")
            if len(subject) > 60:
                log.warning("Subject over 60 chars (%d), rewording", len(subject))
                subject = _shorten_field(client, "subject line", subject, 60)
            if len(cta_label) > 20:
                log.warning("CTA label over 20 chars (%d), rewording", len(cta_label))
                cta_label = _shorten_field(client, "CTA button label", cta_label, 20)
            variant = InMailVariant(
                angle=angle_key,
                angle_label=angle_cfg["label"],
                subject=subject,
                body=parsed.get("body", _fallback_body(tg_category, angle_key)),
                cta_label=cta_label,
                cta_url=destination_url,
            )
            log.info("InMail angle %s generated for '%s'", angle_key, cohort.name)
        except Exception as exc:
            log.warning("InMail angle %s failed for '%s': %s", angle_key, cohort.name, exc)
            variant = InMailVariant(
                angle=angle_key,
                angle_label=angle_cfg["label"],
                subject=_fallback_subject(tg_category, angle_key),
                body=_fallback_body(tg_category, angle_key),
                cta_label="See Opportunities",
                cta_url=destination_url,
            )
        variants.append(variant)

    return variants


# ── Prompt helpers ─────────────────────────────────────────────────────────────

def _cohort_summary(cohort, tg_category: str) -> str:
    rules_str = (
        ", ".join(f"{r[0]}={r[1]}" for r in cohort.rules[:4])
        if cohort.rules else "n/a"
    )
    return (
        f"Target audience category: {tg_category}\n"
        f"Cohort name: {cohort.name}\n"
        f"Key signals: {rules_str}\n"
        f"Lift over baseline: {round(getattr(cohort, 'lift_pp', 0), 1)} percentage points"
    )


def _build_prompt(
    cohort_summary: str,
    tg_category: str,
    angle_key: str,
    angle_cfg: dict,
    hourly_rate: str = "$50",
) -> str:
    control_note = (
        "NOTE: The Financial angle (rate in subject line) is the proven control — "
        "data shows it consistently outperforms expertise/flexibility framing on CTR and CPA. "
        "This variant is a challenger. Make it genuinely distinctive, not just a rewording.\n\n"
        if not angle_cfg.get("is_control") else ""
    )
    return f"""You are writing a LinkedIn InMail (Message Ad) for Outlier — an AI data platform that pays domain experts to complete AI tasks remotely (reviewing, rating, and generating content to improve AI models). Outlier has paid over $500M to contributors globally.

Hourly rate for this TG: {hourly_rate}/hr (use this exact figure in subject and body — do not say "up to").

{control_note}

{cohort_summary}

Angle: {angle_key} — {angle_cfg['label']}
Hook: {angle_cfg['hook']}
Tone: {angle_cfg['tone']}
Subject line style: {angle_cfg['subject_style']}
Body structure: {angle_cfg['body_structure']}

{VOCAB_RULES}

Guidelines:
- Address the reader as a professional — this is a peer-to-peer InMail, not a mass marketing email
- Do NOT use bullet points or headers inside the body — plain paragraphs only
- Do NOT use the word "Hi" or "Hello" — open with a punchy first sentence
- Subject must be specific to this TG, not generic ("An opportunity for you" is not acceptable)
- Body: 150–250 words
- CTA label: action-oriented verb phrase (not "Click here") — MUST be 20 characters or fewer (count every character including spaces). Good examples: "See Open Tasks" (14), "Start Contributing" (18), "View Opportunities" (18), "Explore Tasks" (13). Count carefully before writing.
- Subject line MUST be 60 characters or fewer (count every character including spaces). Count carefully before writing.

Write your response EXACTLY in this format (no other text):
SUBJECT: <subject line, max 60 chars — count carefully>
BODY: <body text, 150–250 words, plain paragraphs>
CTA_LABEL: <button label, max 20 chars — count carefully>"""


def _shorten_field(client, field_name: str, value: str, max_chars: int) -> str:
    """
    Ask the model to reword a field that exceeds max_chars.
    Returns a shortened version, or the original truncated as a last resort.
    """
    prompt = (
        f"Rewrite the following {field_name} so it is {max_chars} characters or fewer "
        f"(count every character including spaces). Keep the meaning and tone intact. "
        f"Return ONLY the rewritten text, nothing else.\n\n"
        f"Original: {value}"
    )
    try:
        resp = client.chat.completions.create(
            model=config.LITELLM_MODEL,
            max_tokens=64,
            messages=[{"role": "user", "content": prompt}],
        )
        shortened = resp.choices[0].message.content.strip().strip('"')
        if len(shortened) <= max_chars:
            return shortened
        log.warning("Rewrite of %s still over limit (%d > %d), truncating", field_name, len(shortened), max_chars)
    except Exception as exc:
        log.warning("Could not rewrite %s: %s", field_name, exc)
    return value[:max_chars]


def _parse_response(raw: str) -> dict:
    """Extract SUBJECT, BODY, CTA_LABEL from Claude's structured response."""
    result: dict[str, str] = {}
    body_lines: list[str] = []
    current = None

    for line in raw.splitlines():
        if line.startswith("SUBJECT:"):
            current = "subject"
            result["subject"] = line[8:].strip()
        elif line.startswith("CTA_LABEL:"):
            if body_lines:
                result["body"] = "\n".join(body_lines).strip()
                body_lines = []
            current = "cta_label"
            result["cta_label"] = line[10:].strip()
        elif line.startswith("BODY:"):
            current = "body"
            first = line[5:].strip()
            if first:
                body_lines.append(first)
        elif current == "body":
            body_lines.append(line)

    if body_lines and "body" not in result:
        result["body"] = "\n".join(body_lines).strip()

    return result


# ── Fallbacks ──────────────────────────────────────────────────────────────────

def _fallback_subject(tg_category: str, angle_key: str) -> str:
    subjects = {
        "A": f"Your {tg_category} expertise is in demand",
        "B": f"1,000+ {tg_category} professionals work on Outlier",
        "C": "Earn on your schedule — no fixed shifts",
    }
    return subjects.get(angle_key, "AI tasks for domain experts")[:60]


def _fallback_body(tg_category: str, angle_key: str) -> str:
    if angle_key == "A":
        return (
            f"Your background in {tg_category} is exactly what AI companies are looking for right now.\n\n"
            f"Outlier is an AI data platform that matches domain experts with AI tasks — reviewing model "
            f"outputs, generating examples, and rating responses in your area of expertise. Everything is "
            f"remote and async; you set your own schedule.\n\n"
            f"Your specific background means you'd be working on tasks where your judgment actually matters "
            f"to the quality of the AI. Payment is made weekly, and you can take on as many or as few tasks "
            f"as you want.\n\n"
            f"Click below to see current opportunities and get started."
        )
    elif angle_key == "B":
        return (
            f"Over 1,000 {tg_category} professionals are already contributing to AI projects on Outlier — "
            f"and the platform has paid out more than $500M to contributors globally.\n\n"
            f"Outlier matches domain experts with AI tasks: reviewing model outputs, rating responses, and "
            f"generating content in their field. Tasks are remote, async, and flexible — no fixed schedule, "
            f"no minimum hours.\n\n"
            f"Payment is made weekly. You decide how much time you put in.\n\n"
            f"If you're looking for a way to put your expertise to work on your own terms, see what's "
            f"currently available on Outlier."
        )
    else:  # C
        return (
            f"No shifts. No deadlines. No minimum hours.\n\n"
            f"Outlier is an AI data platform where {tg_category} professionals complete AI tasks on their "
            f"own schedule — reviewing model outputs, generating examples, rating responses. Everything is "
            f"remote and async.\n\n"
            f"More than $500M has been paid to contributors on the platform. Payment is weekly. You choose "
            f"when you work and how much.\n\n"
            f"If you've been looking for a flexible way to use your expertise outside of your main work, "
            f"take a look at what's available."
        )

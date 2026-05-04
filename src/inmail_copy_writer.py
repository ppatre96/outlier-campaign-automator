"""
InMail copy writer for LinkedIn Message Ad campaigns.

Generates angle variants for a given TG. The FLEXIBILITY angle is the
data-validated winner — 12-month analysis (17.4M sends, 2026-05-04) shows
Flexibility subjects average 5.84% CTR vs 4.38% for Financial and 3.45% for
Aspirational. Financial is kept as a tested challenger because it still works
well for Coders ("$80/hr with Machine Learning + AI" = 23.9% CTR, $0.08/click).

Key findings folded into prompts:
  - Identity-first subjects win: "Romanian Speakers Needed" (28.5% CTR) vs
    "Top Coders Wanted" (0.65%). Name the exact skill/group, not a flattering label.
  - Rate format: always "$X/hr" in the main clause — never parenthetical "(Earn $X)"
    and never drop the $ sign. "40/h" scored 0.19% CTR vs "$40 USD/hr" at 27.5%.
  - Body length: 300-599 chars = 5.80% CTR; 900+ chars = 4.33%. Target 75-100 words.
  - Opening line: start with an observation about the reader, NOT a company intro.
    Good: "Your background in X is exactly what AI teams are missing right now."
    Bad:  "Outlier is an AI data platform that has paid over $500M..."
  - Aspirational angle ("Your X expertise is worth $Y") is the weakest by cost:
    $10.26/click vs $2.55/click for identity-functional subjects. Keep it but
    pair it with a concrete earning signal.
  - Medical/PhD/Math is the hardest segment (2.77% avg CTR) — extra specificity
    in subject + concrete task description needed to compensate.

New angles can be injected via hypothesis dicts from the competitor agent —
see build_inmail_variants() for the interface.
"""
import logging
from dataclasses import dataclass

from openai import OpenAI

log = logging.getLogger(__name__)

# ── Angle configs ──────────────────────────────────────────────────────────────
# Data-validated ranking by CTR (12-month, 17.4M sends, 2026-05-04 analysis):
#   C (Flexibility): 5.84% CTR — DATA WINNER
#   F (Financial):   4.38% CTR — strong for Coders specifically
#   B (Social Proof):~4.5% CTR — peer count + rate works well
#   A (Aspirational): 3.45% CTR — weakest; needs concrete rate to avoid $10+/click
#
# Subject-line rules that cut across all angles (from top-10 analysis):
#   1. Name the exact identity/skill group ("Romanian Speakers", "Machine Learning")
#      NOT a flattering label ("Top Coders", "Physics Experts").
#   2. Rate in the MAIN CLAUSE with "$" sign. Never parenthetical "(Earn $X)".
#   3. Never say "up to $X", "Side Income", or generic "Remote Opportunity" alone.

ANGLE_CONFIGS = {
    "F": {
        "label":         "Financial",
        "hook":          "specific hourly rate + identity — rate in main clause with $ sign",
        "tone":          "direct, factual, outcome-first",
        "subject_style": (
            "Name the exact skill/identity then the rate in the main clause. "
            "Proven formats: 'Earn $X/hr with [Skill] + AI'  OR  '[Skill] + AI = Flexible $X/hr'  OR  "
            "'[Role] | Flexible Hours & $X/hr'. "
            "Never parenthetical: NO '(Earn $X/hr)'. Never 'up to'. Never 'Side Income'. "
            "Always include the $ sign — '50/hr' without $ scored 0.19% CTR vs 27% with $."
        ),
        "body_structure": (
            "OPEN: observation about the reader's specific background, NOT a company introduction. "
            "E.g. 'Your background in [specific skill] is exactly what AI developers are missing.' "
            "Then: what contributors do (review, rate, generate AI content, 1 sentence) → "
            "concrete task example for this TG → "
            "weekly payment + no fixed schedule → "
            "no commitment, no minimum hours → "
            "call to action. Keep to 75-100 words total."
        ),
        "is_control": True,
    },
    "A": {
        "label":         "Expertise",
        "hook":          "recognition of domain expertise — MUST still include earning signal",
        "tone":          "respectful, peer-to-peer, thoughtful",
        "subject_style": (
            "Identity-first recognition + rate signal. Data shows pure aspirational "
            "('Your X is valuable') without a rate costs $10+/click. "
            "Good: 'Cardiologists earn $50/hr shaping AI' — names the identity AND the rate. "
            "Bad: 'Your medical expertise is in demand' — no rate, generic."
        ),
        "body_structure": (
            "OPEN: specific observation about the reader's expertise — name the actual skill "
            "or domain ('your ability to interpret ECGs / read code / parse legal arguments'). "
            "NOT 'Outlier is a platform...' — that is the worst-performing opener pattern. "
            "Then: why that specific expertise matters for AI (1-2 sentences) → "
            "concrete task example → payment weekly + flexible → call to action. "
            "Keep to 75-100 words."
        ),
        "is_control": False,
    },
    "B": {
        "label":         "Social Proof",
        "hook":          "peer group stat + payment figure",
        "tone":          "factual, motivating, social proof",
        "subject_style": (
            "Lead with a peer count stat or the rate: '1,000+ [exact role] earn $X/hr' OR "
            "'[exact role] earn $X/hr improving AI'. "
            "Be specific to the cohort — '1,000+ cardiologists' not '1,000+ professionals'."
        ),
        "body_structure": (
            "OPEN: peer count or 'others like you are already doing this' — "
            "name the specific peer group, not a generic label. "
            "Then: what they do on Outlier → why this TG's knowledge is valuable → "
            "flexible schedule + weekly payment → call to action. "
            "Keep to 75-100 words."
        ),
        "is_control": False,
    },
    "C": {
        "label":         "Flexibility",
        "hook":          "lifestyle / schedule freedom — DATA WINNER (5.84% avg CTR)",
        "tone":          "relaxed, empowering, lifestyle-led",
        "subject_style": (
            "Lead with schedule freedom AND earning signal together. "
            "Good: '[Role]: set your own hours, earn $X/hr' or 'Flexible, Remote — $X/hr for [skill]'. "
            "Pure lifestyle without a rate underperforms. Must include the $ amount. "
            "Never 'Remote Opportunity' alone — it is in the bottom-10 subject patterns."
        ),
        "body_structure": (
            "OPEN: call out the constraints they actually face (shifts, call schedules, "
            "hospital deadlines, fixed hours) — make it specific to this TG. "
            "E.g. 'No call schedule. No charting deadlines.' for medical. "
            "Then: what flexibility on Outlier actually looks like → "
            "what the tasks involve (specific to TG) → weekly payment → call to action. "
            "Keep to 75-100 words. This is the highest-CTR angle — write it with energy."
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
    body:        str   # 75–100 words — plain text, no bullet points
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
        "NOTE: Data (12-month, 17.4M sends) shows the FLEXIBILITY angle has the highest "
        "avg CTR (5.84%) — higher than Financial (4.38%). This is a Financial challenger. "
        "Make it genuinely distinctive — if it reads like a reworded Flexibility message "
        "without the rate front-and-center, it will underperform.\n\n"
        if not angle_cfg.get("is_control") else
        "NOTE: Financial is the best angle for Coders TG specifically. For Medical/PhD TGs "
        "it under-indexes — compensate by making the task description very concrete.\n\n"
    )
    return f"""You are writing a LinkedIn InMail (Message Ad) for Outlier — an AI data platform where domain experts earn payment doing flexible, remote AI tasks (reviewing, rating, generating content to improve AI models). Outlier has paid over $500M to contributors globally.

Hourly rate for this TG: {hourly_rate}/hr (use this exact figure — never say "up to $X").

{control_note}{cohort_summary}

Angle: {angle_key} — {angle_cfg['label']}
Hook: {angle_cfg['hook']}
Tone: {angle_cfg['tone']}
Subject line style: {angle_cfg['subject_style']}
Body structure: {angle_cfg['body_structure']}

{VOCAB_RULES}

CRITICAL WRITING RULES (based on 12-month performance data — these directly affect CTR):

1. OPENING LINE: Start with a specific observation about the reader's background.
   GOOD: "Your ability to interpret ECG waveforms is exactly what AI developers can't replicate."
   BAD:  "Outlier is an AI data platform that has paid over $500M to contributors worldwide."
   The company-intro opener is the most common failure pattern in low-CTR InMails.

2. BODY LENGTH: 75–100 words maximum. Data shows 300–599 char bodies get 5.80% CTR vs 4.33%
   for 900+ char bodies. Stop as soon as the CTA is clear. Do NOT pad with company history.

3. ACTIVE VOICE: Prefer active constructions. "We send payment weekly" beats "Payment is issued."
   One or two passive phrases are acceptable — don't make it the default register.

4. SUBJECT LINE: Name the exact skill/identity group — not a flattering label.
   GOOD: "Cardiologists: set your own hours, earn $50/hr"
   BAD:  "Top Medical Experts Wanted" or "Push the Limits of AI (Earn $50/hr)"
   Rate must appear with $ sign in the main clause — never in parentheses.

5. SPECIFICITY: Name a concrete task the reader will actually do.
   GOOD: "evaluating AI-generated ECG interpretations for accuracy"
   BAD:  "helping improve AI models" (too vague to motivate action)

6. NO em dashes (—). Use a period or comma instead.
7. No bullet points or headers — plain paragraphs only.
8. Do NOT start with "Hi" or "Hello".
9. CTA label: action verb, ≤20 characters. Examples: "See Open Tasks" (14), "View Opportunities" (18).
10. Subject line: ≤60 characters including spaces. Count carefully.

Write your response EXACTLY in this format (no other text):
SUBJECT: <subject line, max 60 chars>
BODY: <body text, 75–100 words, plain paragraphs>
CTA_LABEL: <button label, max 20 chars>"""


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

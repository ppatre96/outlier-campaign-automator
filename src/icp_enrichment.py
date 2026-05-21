"""
ICP enrichment — translates a finalized Cohort into a structured Ideal
Customer Profile.

Background: outlier-data-analyst's Stage A/B/C output is a Cohort dataclass
of statistical signals (rules, lift, pass_rate, audience_size). That tells
the pipeline WHO to target but not WHY they convert. The brief + copy
agents need preference + motivation context to produce angles that resonate
with the cohort — high-creative-liberty engineers respond to bold hooks;
risk-averse healthcare workers respond to brand-legitimacy + benefits.

This module calls Claude with the Cohort signals + a small Snowflake resume
sample and asks for a structured ICP:

  - cohort_description: one-line "who they are"
  - top_motivations:    what they care about (3-5 items)
  - content_prefs:      preferred message formats
  - creative_liberty:   "high" | "medium" | "low"
  - language_pref:      BCP-47 ish locale code
  - decision_drivers:   what tips them from interest to apply
  - skill_priorities:   their dominant skills (drives photo_subject)

Persisted to the `cohort_icp` Postgres table by ui_decisions.upsert_cohort_icp.
The console reads from there and renders IcpCard above the Angles card.
"""
from __future__ import annotations

import json
import logging
import re
from dataclasses import dataclass, field
from typing import Any, Optional

import config
from src.claude_client import call_claude

log = logging.getLogger(__name__)

# Number of resume rows we sample from Snowflake to ground the LLM call.
# Stays small to keep prompt size low and avoid leaking too many PII fields
# into the model context.
_RESUME_SAMPLE_SIZE = 10

_ICP_SYSTEM_PROMPT = """\
You are the Outlier campaign agent's ICP analyst. You translate statistical
cohort definitions (skills, titles, geos) into a structured Ideal Customer
Profile that the brief + copy agents use to generate per-channel angles.

You will receive:
  1. The cohort's statistical signature (rules, audience_size, lift_pp).
  2. A small sample (10 rows) of contributor resumes matching the cohort.

Return a single JSON object (no prose, no markdown fences) with EXACTLY
these keys:

{
  "cohort_description": "one-line description of who this cohort is (≤140 chars)",
  "top_motivations":    ["3-5 motivations they care about as Outlier contributors"],
  "content_prefs":      ["3-5 content formats / tones that resonate"],
  "creative_liberty":   "high" | "medium" | "low",
  "language_pref":      "BCP-47 locale code, e.g. en-US, en-IN, es-419, hi-IN",
  "decision_drivers":   ["3-5 things that tip them from interest to apply"],
  "skill_priorities":   ["3-5 dominant skills/specialties they bring"]
}

Voice rules (HARD):
- Never use "job", "role", "training", "required" — these are banned in
  contributor-facing copy. Substitute: "opportunity", "task", "guidelines",
  "strongly encouraged".
- Don't use em dashes, hashtags, or ALL CAPS.
- Keep entries terse — 2-6 words each, not full sentences.

`creative_liberty` calibration:
- HIGH: software engineers, ML researchers, designers, creators who expect
  bold, irreverent, or witty copy.
- MEDIUM: data scientists, consultants, mid-career professionals who
  expect clear value + a credible brand.
- LOW: healthcare workers, regulated professions, conservative regions
  where corporate-safe + benefits-forward copy lands best.

Be specific. Generic priors ("flexibility", "extra income") are fine ONLY
if backed by what you see in the resume sample.
"""


@dataclass
class CohortIcp:
    cohort_description: str = ""
    top_motivations:    list[str] = field(default_factory=list)
    content_prefs:      list[str] = field(default_factory=list)
    creative_liberty:   str = "medium"
    language_pref:      str = "en-US"
    decision_drivers:   list[str] = field(default_factory=list)
    skill_priorities:   list[str] = field(default_factory=list)
    sample_size_n:      int = 0
    model_version:      str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "cohort_description": self.cohort_description,
            "top_motivations":    self.top_motivations,
            "content_prefs":      self.content_prefs,
            "creative_liberty":   self.creative_liberty,
            "language_pref":      self.language_pref,
            "decision_drivers":   self.decision_drivers,
            "skill_priorities":   self.skill_priorities,
            "sample_size_n":      self.sample_size_n,
            "model_version":      self.model_version,
        }


def _summarize_resume(row: dict) -> str:
    """One-line summary of a resume row — keeps the prompt small."""
    parts = []
    if row.get("resume_job_title"):
        parts.append(f"title={row['resume_job_title']}")
    if row.get("resume_job_company"):
        parts.append(f"co={row['resume_job_company']}")
    if row.get("resume_degree") and row.get("resume_field"):
        parts.append(f"edu={row['resume_degree']}/{row['resume_field']}")
    skills = row.get("resume_job_skills") or ""
    if isinstance(skills, list):
        skills = ", ".join(skills[:6])
    if skills:
        parts.append(f"skills=[{str(skills)[:120]}]")
    return " · ".join(parts) or "(empty resume row)"


def enrich(
    cohort,
    *,
    resume_sample: Optional[list[dict]] = None,
    locale_hint: Optional[str] = None,
) -> CohortIcp:
    """
    Produce a structured ICP for a finalized Cohort.

    `cohort` is the dataclass from src/analysis.py — uses .name, .rules,
    .audience_size, .lift_pp, .pass_rate.

    `resume_sample` is an optional pre-fetched list of resume row dicts. When
    omitted, returns a heuristic ICP without an LLM call (used for cold-start
    paths where Snowflake isn't accessible).

    `locale_hint` (e.g. "en-IN") is passed to the LLM to steer language_pref
    when cohort geo is known but the resume sample doesn't make it obvious.

    NEVER raises — falls back to a heuristic ICP on any LLM/network failure
    so cohort selection isn't blocked by enrichment outages.
    """
    rules_summary = ", ".join(f"{r[0]}={r[1]}" for r in (getattr(cohort, "rules", []) or [])[:8])
    sample = (resume_sample or [])[:_RESUME_SAMPLE_SIZE]
    sample_lines = "\n".join(f"  - {_summarize_resume(r)}" for r in sample)

    user_msg = (
        f"Cohort signature: {getattr(cohort, 'name', '?')}\n"
        f"Rules: {rules_summary}\n"
        f"Audience size: {getattr(cohort, 'audience_size', None)}\n"
        f"Lift pp vs base: {getattr(cohort, 'lift_pp', None)}\n"
        f"Pass rate (%): {getattr(cohort, 'pass_rate', None)}\n"
        f"Locale hint: {locale_hint or '(none)'}\n"
        f"Resume sample ({len(sample)} rows):\n{sample_lines or '  (none)'}\n\n"
        "Return the JSON now."
    )

    icp = CohortIcp(sample_size_n=len(sample))
    icp.model_version = config.ANTHROPIC_MODEL

    if not sample:
        # No resume context — emit a defensive heuristic instead of calling
        # the LLM blind. Callers usually provide a sample; this branch is
        # for cold-start dry-runs.
        icp.cohort_description = getattr(cohort, "name", "")[:140]
        icp.top_motivations = ["fair payment", "flexibility", "interesting tasks"]
        icp.content_prefs = ["clear value", "credible brand"]
        icp.creative_liberty = "medium"
        icp.language_pref = locale_hint or "en-US"
        icp.decision_drivers = ["legitimate brand", "clear payment terms"]
        icp.skill_priorities = []
        return icp

    try:
        raw = call_claude(
            messages=[{"role": "user", "content": user_msg}],
            system=_ICP_SYSTEM_PROMPT,
            cache_system=True,
            max_tokens=1024,
        )
    except Exception as exc:
        log.warning(
            "icp_enrichment.enrich: LLM call failed for cohort=%s: %s — using heuristic",
            getattr(cohort, "name", "?"), exc,
        )
        icp.cohort_description = getattr(cohort, "name", "")[:140]
        return icp

    parsed = _parse_icp_json(raw)
    if parsed is None:
        log.warning(
            "icp_enrichment.enrich: could not parse LLM JSON for cohort=%s — using heuristic. Raw: %s",
            getattr(cohort, "name", "?"), raw[:200],
        )
        icp.cohort_description = getattr(cohort, "name", "")[:140]
        return icp

    icp.cohort_description = str(parsed.get("cohort_description", "") or "")[:280]
    icp.top_motivations   = _to_string_list(parsed.get("top_motivations"))
    icp.content_prefs     = _to_string_list(parsed.get("content_prefs"))
    icp.creative_liberty  = _norm_liberty(parsed.get("creative_liberty"))
    icp.language_pref     = str(parsed.get("language_pref", "") or "en-US")[:16]
    icp.decision_drivers  = _to_string_list(parsed.get("decision_drivers"))
    icp.skill_priorities  = _to_string_list(parsed.get("skill_priorities"))
    return icp


def _parse_icp_json(raw: str) -> Optional[dict]:
    if not raw:
        return None
    cleaned = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip()).strip()
    try:
        obj = json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to find the first {...} block in the response.
        match = re.search(r"\{.*\}", cleaned, re.S)
        if not match:
            return None
        try:
            obj = json.loads(match.group(0))
        except json.JSONDecodeError:
            return None
    return obj if isinstance(obj, dict) else None


def _to_string_list(v: Any, *, max_n: int = 8) -> list[str]:
    if not v:
        return []
    if isinstance(v, str):
        return [v]
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v[:max_n]:
        if isinstance(x, str) and x.strip():
            out.append(x.strip()[:80])
    return out


def _norm_liberty(v: Any) -> str:
    if not isinstance(v, str):
        return "medium"
    low = v.strip().lower()
    if low in ("high", "medium", "low"):
        return low
    return "medium"

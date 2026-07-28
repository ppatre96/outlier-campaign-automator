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
  - summary:            plain-language paragraph on who they are
  - location:           where we source them, in plain language
  - core_requirements:  the qualifying buckets (name + what the work is +
                        the titles that carry it) — "must have at least one"
  - strong_signals:     certifications / designations / tenure that mark a
                        strong candidate
  - exclusions:         adjacent profiles we should NOT source
  - evidence:           the frequency counts the spec was mined from
  - top_motivations:    what they care about (3-5 items)
  - content_prefs:      preferred message formats
  - creative_liberty:   "high" | "medium" | "low"
  - language_pref:      BCP-47 ish locale code
  - decision_drivers:   what tips them from interest to apply
  - skill_priorities:   their dominant skills (drives photo_subject)

The first block (summary → exclusions) is the *sourcing spec* — the readable
"who to source and who not to" a human recruiter would write. It exists so a
sizing analysis returns a decision-ready ICP rather than a bag of chips.

Persisted to the `cohort_icp` Postgres table by ui_decisions.upsert_cohort_icp.
The console reads from there and renders IcpCard above the Angles card.
"""
from __future__ import annotations

import json
import logging
import re
from collections import Counter
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
  2. Where we source (geos), when known.
  3. Frequency evidence: how many of the qualified contributors share each
     resume title / skill / field, when known.
  4. A small sample (10 rows) of contributor resumes matching the cohort.

Return a single JSON object (no prose, no markdown fences) with EXACTLY
these keys:

{
  "cohort_description": "one-line description of who this cohort is (≤140 chars)",
  "summary":            "2-4 sentences on who these people are and what work they actually do day to day (≤600 chars)",
  "location":           "where we source, in plain language, e.g. 'United States only' or 'India and the Philippines'",
  "core_requirements":  [
    {
      "name":        "short bucket name, e.g. Personal tax",
      "description": "one clause on the concrete work they must have done, naming the artefacts or filings (≤200 chars)",
      "titles":      ["2-5 real titles that carry this work"]
    }
  ],
  "strong_signals":     ["3-6 certifications, designations, or tenure markers of a strong candidate"],
  "exclusions":         ["3-6 adjacent profiles we should NOT source, and the one clause of why"],
  "exclude_titles":     ["3-8 SPECIFIC titles to exclude from ad targeting, drawn from your exclusions"],
  "top_motivations":    ["3-5 motivations they care about as Outlier contributors"],
  "content_prefs":      ["3-5 content formats / tones that resonate"],
  "creative_liberty":   "high" | "medium" | "low",
  "language_pref":      "BCP-47 locale code, e.g. en-US, en-IN, es-419, hi-IN",
  "decision_drivers":   ["3-5 things that tip them from interest to apply"],
  "skill_priorities":   ["3-5 dominant skills/specialties they bring"]
}

Sourcing-spec rules (the summary / core_requirements / strong_signals /
exclusions block — this is what a human decides from, so it carries the
detail):
- 2-5 `core_requirements`, treated as "must have at least one". Each is a
  distinct slice of the work, never a restatement of the cohort name.
- Be concrete about WHAT they produced, not just the domain: name the
  deliverables, filings, models, systems, or clients you can see in the
  evidence. "Prepared individual 1040 returns for retail clients" is right;
  "tax experience" is not.
- Ground every requirement, signal, and exclusion in the evidence or the
  resume sample. Do not invent a specialty no contributor shows.
- The evidence describes people who ALREADY contribute with us, but the spec is
  for sourcing people who have not joined yet. So never state a requirement or
  signal that is an artefact of that: prior annotation, data-labelling, model
  scoring, or rubric work, and tenure at Outlier, Alignerr, Mercor, or Scale.
  Those are circular. State the domain expertise they were sourced FOR.
- `titles` must be titles you can see in the evidence or resume sample, or the
  standard synonym for that same work. Never name a different licensed
  profession (attorney, physician, engineer) unless the evidence shows it. A
  profession that merely works nearby belongs in `exclusions`, not `titles`.
- Write each exclusion as one sentence: the profile, a colon, then why it
  fails the core requirement. No dashes.
- `exclude_titles` turns those exclusions into ad targeting. Each entry must be
  a real title someone would put on LinkedIn ("Tax Attorney", "Estate Planning
  Attorney", "Bookkeeper", "Account Executive"), NOT a description of a
  category ("people without licences"). Only list a title you would genuinely
  refuse, since it is subtracted from the reachable audience.
- Last pass before you return: read every title back against your exclusions.
  Never list a title you also exclude. When a requirement sits right next to
  an excluded profession, put the boundary in the bucket name itself, e.g.
  "Estate planning (non-attorney)".
- `exclusions` are the near-misses: profiles that match the domain keywords
  but lack the core requirement (wrong client type, wrong seniority, wrong
  licence, adjacent profession). These are the highest-value part of the
  spec, so make them specific enough to filter on.
- When the evidence is thin (few contributors, or no feature shared by
  many), say so inside `summary` rather than padding the spec.

Voice rules (HARD):
- Never use "job", "role", "training", "required" — these are banned in
  contributor-facing copy. Substitute: "opportunity", "task", "guidelines",
  "strongly encouraged". ("titles" is fine — it names resume titles.)
- Don't use em dashes, hashtags, or ALL CAPS.
- Keep the chip lists (motivations, content_prefs, decision_drivers,
  skill_priorities) terse — 2-6 words each, not full sentences. The
  sourcing-spec descriptions are full clauses.

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
    # Sourcing spec — the readable "who to source, who not to".
    summary:            str = ""
    location:           str = ""
    core_requirements:  list[dict] = field(default_factory=list)
    strong_signals:     list[str] = field(default_factory=list)
    exclusions:         list[str] = field(default_factory=list)
    exclude_titles:     list[str] = field(default_factory=list)
    evidence:           dict[str, Any] = field(default_factory=dict)

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
            "summary":            self.summary,
            "location":           self.location,
            "core_requirements":  self.core_requirements,
            "strong_signals":     self.strong_signals,
            "exclusions":         self.exclusions,
            "exclude_titles":     self.exclude_titles,
            "evidence":           self.evidence,
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
    evidence: Optional[dict] = None,
    geos: Optional[list[str]] = None,
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

    `evidence` is the frequency mine behind the cohort — how many of the
    qualified contributors share each title / skill / field (see
    sizing_analysis._mine_and_size). It is what lets the sourcing spec name
    concrete requirements instead of restating the cohort label, and it is
    persisted alongside the ICP so a reader can audit every claim.

    `geos` (ISO-2 codes) renders the plain-language `location` line.

    NEVER raises — falls back to a heuristic ICP on any LLM/network failure
    so cohort selection isn't blocked by enrichment outages.
    """
    rules_summary = ", ".join(f"{r[0]}={r[1]}" for r in (getattr(cohort, "rules", []) or [])[:8])
    sample = (resume_sample or [])[:_RESUME_SAMPLE_SIZE]
    sample_lines = "\n".join(f"  - {_summarize_resume(r)}" for r in sample)
    location = _location_line(geos)

    icp = CohortIcp(sample_size_n=len(sample))
    icp.model_version = config.ANTHROPIC_MODEL
    icp.evidence = dict(evidence or {})
    # Seed the sourcing spec from the cohort rules so every path — LLM
    # success, LLM failure, no-sample cold start — returns something a human
    # can read. The LLM overwrites what it can improve on.
    _fill_from_rules(icp, cohort, location, locale_hint)

    if not sample:
        # No resume context — emit a defensive heuristic instead of calling
        # the LLM blind. Callers usually provide a sample; this branch is
        # for cold-start dry-runs.
        icp.top_motivations = ["fair payment", "flexibility", "interesting tasks"]
        icp.content_prefs = ["clear value", "credible brand"]
        icp.creative_liberty = "medium"
        icp.decision_drivers = ["legitimate brand", "clear payment terms"]
        return icp

    user_msg = (
        f"Cohort signature: {getattr(cohort, 'name', '?')}\n"
        f"Rules: {rules_summary}\n"
        f"Audience size: {getattr(cohort, 'audience_size', None)}\n"
        f"Lift pp vs base: {getattr(cohort, 'lift_pp', None)}\n"
        f"Pass rate (%): {getattr(cohort, 'pass_rate', None)}\n"
        f"Sourcing geos: {location or '(broad / unspecified)'}\n"
        f"Locale hint: {locale_hint or '(none)'}\n"
        f"{_evidence_block(evidence)}"
        f"Resume sample ({len(sample)} rows):\n{sample_lines or '  (none)'}\n\n"
        "Return the JSON now."
    )

    try:
        raw = call_claude(
            messages=[{"role": "user", "content": user_msg}],
            system=_ICP_SYSTEM_PROMPT,
            cache_system=True,
            max_tokens=2048,
        )
    except Exception as exc:
        log.warning(
            "icp_enrichment.enrich: LLM call failed for cohort=%s: %s — using heuristic",
            getattr(cohort, "name", "?"), exc,
        )
        return icp

    parsed = _parse_icp_json(raw)
    if parsed is None:
        log.warning(
            "icp_enrichment.enrich: could not parse LLM JSON for cohort=%s — using heuristic. Raw: %s",
            getattr(cohort, "name", "?"), raw[:200],
        )
        return icp

    icp.cohort_description = str(parsed.get("cohort_description", "") or "")[:280]
    icp.top_motivations   = _to_string_list(parsed.get("top_motivations"))
    icp.content_prefs     = _to_string_list(parsed.get("content_prefs"))
    icp.creative_liberty  = _norm_liberty(parsed.get("creative_liberty"))
    icp.language_pref     = str(parsed.get("language_pref", "") or "en-US")[:16]
    icp.decision_drivers  = _to_string_list(parsed.get("decision_drivers"))
    icp.skill_priorities  = _to_string_list(parsed.get("skill_priorities"))
    icp.summary           = str(parsed.get("summary", "") or "")[:800]
    icp.strong_signals    = _to_string_list(parsed.get("strong_signals"), max_item_len=160)
    icp.exclusions        = _to_string_list(parsed.get("exclusions"), max_item_len=200)
    icp.exclude_titles    = _to_string_list(parsed.get("exclude_titles"), max_n=8, max_item_len=60)
    # Keep the rules-derived location/requirements when the model omits them —
    # a half-filled spec reads as broken, and geos are ours to know anyway.
    icp.location          = str(parsed.get("location", "") or "")[:160] or icp.location
    reqs = _to_requirements(parsed.get("core_requirements"))
    if reqs:
        icp.core_requirements = reqs
    return icp


def _location_line(geos: Optional[list[str]]) -> str:
    """ISO-2 geos → the plain-language sourcing location ("United States only",
    "India, Philippines and 2 more"). Empty when we're sourcing broad."""
    from src.locales import country_name_for

    names = [country_name_for(g) or g for g in (geos or []) if str(g or "").strip()]
    names = [n for n in dict.fromkeys(names)]  # dedupe, keep order
    if not names:
        return ""
    if len(names) == 1:
        return f"{names[0]} only"
    if len(names) <= 4:
        return f"{', '.join(names[:-1])} and {names[-1]}"
    return f"{', '.join(names[:4])} and {len(names) - 4} more"


def _deslug(v: str) -> str:
    """Rule slug → readable phrase ("personal_tax" → "personal tax")."""
    return re.sub(r"[_\s]+", " ", str(v or "")).strip()


def _fill_from_rules(icp: CohortIcp, cohort, location: str, locale_hint: Optional[str]) -> None:
    """Seed the sourcing spec straight from the cohort's targeting rules.

    Rules are (f"{feature}__{value}", 1) pairs — the hard common parameters the
    cohort was actually mined on, so they are the most defensible thing we can
    state without an LLM. Grouped into one requirement per feature family.
    """
    icp.cohort_description = getattr(cohort, "name", "")[:140]
    icp.location = location
    icp.language_pref = locale_hint or icp.language_pref

    groups: dict[str, list[str]] = {}
    for feat, val in (getattr(cohort, "rules", []) or []):
        if not val:
            continue
        prefix, _, slug = str(feat).partition("__")
        if slug:
            groups.setdefault(prefix, []).append(_deslug(slug))

    titles = groups.get("job_titles_norm", [])
    skills = groups.get("skills", [])
    fields = groups.get("fields_of_study", [])
    degrees = groups.get("highest_degree_level", [])

    reqs: list[dict] = []
    if skills:
        reqs.append({
            "name": "Hands-on experience",
            "description": f"Professional experience across {', '.join(skills)}",
            "titles": titles[:5],
        })
    elif titles:
        reqs.append({
            "name": "Relevant experience",
            "description": f"Currently or recently working as {', '.join(titles)}",
            "titles": titles[:5],
        })
    if fields:
        reqs.append({
            "name": "Academic background",
            "description": f"Studied {', '.join(fields)}",
            "titles": [],
        })
    icp.core_requirements = reqs
    icp.strong_signals = [f"{d} degree" for d in degrees[:2]]
    icp.skill_priorities = skills[:5]


_SKILL_SPLIT_RE = re.compile(r"[;,/|]")
# Titles / fields / companies are pipe-joined career histories in
# WORKER_RESUME_SUMMARY ("Investment Advisor Representative | Data Analyst |
# ..."), so they must be split before counting — a whole history is never
# "shared by k contributors". Split on the pipe only: commas and slashes occur
# INSIDE real titles ("Director, Investor Relations", "Financial
# Planner/Advisor") and splitting on those would shred them.
_MULTI_SPLIT_RE = re.compile(r"[|;]")


def resume_evidence(rows: list[dict], *, top_n: int = 12) -> dict[str, Any]:
    """Frequency mine over resume rows (as returned by
    RedashClient.fetch_signal_columns) → the `evidence` dict `enrich` takes.

    Values are [name, count] pairs sorted most-common first, so both the prompt
    and the console can say "shared by k of n contributors". Each value counts
    at most once per contributor, so a repeated title in one career history
    can't inflate its share.
    """
    counters: dict[str, Counter] = {
        "top_titles": Counter(), "top_skills": Counter(),
        "top_fields": Counter(), "top_companies": Counter(),
    }
    for r in rows or []:
        for key, bucket, splitter in (
            ("resume_job_skills",  "top_skills",    _SKILL_SPLIT_RE),
            ("resume_job_title",   "top_titles",    _MULTI_SPLIT_RE),
            ("resume_field",       "top_fields",    _MULTI_SPLIT_RE),
            ("resume_job_company", "top_companies", _MULTI_SPLIT_RE),
        ):
            seen = {v.strip() for v in splitter.split(str(r.get(key) or "")) if v.strip()}
            for v in seen:
                counters[bucket][v] += 1
    out: dict[str, Any] = {"n_contributors": len(rows or [])}
    for bucket, counter in counters.items():
        out[bucket] = [[name, count] for name, count in counter.most_common(top_n)]
    return out


def _evidence_block(evidence: Optional[dict]) -> str:
    """Render the frequency mine for the prompt: "title (k of n)" lines. Empty
    string when the caller has no evidence (job-post / cold-start paths)."""
    if not evidence:
        return ""
    n = evidence.get("n_contributors") or 0
    lines = [f"Frequency evidence across {n} qualified contributors:"]
    for key, label in (("top_titles", "Titles"), ("top_skills", "Skills"),
                       ("top_fields", "Fields of study"), ("top_companies", "Companies")):
        pairs = evidence.get(key) or []
        if not pairs:
            continue
        rendered = ", ".join(f"{name} ({count} of {n})" for name, count in pairs[:12])
        lines.append(f"  {label}: {rendered}")
    return "\n".join(lines) + "\n"


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


def _to_string_list(v: Any, *, max_n: int = 8, max_item_len: int = 80) -> list[str]:
    if not v:
        return []
    if isinstance(v, str):
        return [v[:max_item_len]]
    if not isinstance(v, list):
        return []
    out: list[str] = []
    for x in v[:max_n]:
        if isinstance(x, str) and x.strip():
            out.append(x.strip()[:max_item_len])
    return out


def _to_requirements(v: Any, *, max_n: int = 6) -> list[dict]:
    """Coerce the LLM's core_requirements into [{name, description, titles}].
    Skips entries with neither a name nor a description; a plain string entry
    becomes a description-only requirement."""
    if not isinstance(v, list):
        return []
    out: list[dict] = []
    for x in v[:max_n]:
        if isinstance(x, str):
            if x.strip():
                out.append({"name": "", "description": x.strip()[:240], "titles": []})
            continue
        if not isinstance(x, dict):
            continue
        name = str(x.get("name", "") or "").strip()[:80]
        desc = str(x.get("description", "") or "").strip()[:240]
        if not name and not desc:
            continue
        out.append({
            "name": name,
            "description": desc,
            "titles": _to_string_list(x.get("titles"), max_n=6),
        })
    return out


def _norm_liberty(v: Any) -> str:
    if not isinstance(v, str):
        return "medium"
    low = v.strip().lower()
    if low in ("high", "medium", "low"):
        return low
    return "medium"

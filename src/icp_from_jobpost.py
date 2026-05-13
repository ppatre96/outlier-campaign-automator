"""
Cold-start ICP extraction — derive a targeting spec from a project's job post HTML.

Used by main.py when `fetch_stage1_contributors(project_id)` returns 0 activators.
Because there's no statistical cohort to analyse, we bootstrap an ICP by reading
the project's job description and letting the LLM pull the structured signals that
the copy-writer + LinkedIn targeting layer need.

The returned dict is hand-shaped to match the TG spec that outlier-copy-writer
consumes — it is the ONLY input to the copy-writer in cold_start mode, so the
schema must stay stable:

    {
        "derived_tg_label":         str,        # e.g. "Pediatric Cardiologists (India)"
        "required_skills":          list[str],
        "preferred_skills":         list[str],
        "required_degrees":         list[str],  # e.g. ["MBBS", "DM Cardiology"]
        "required_fields":          list[str],  # e.g. ["medicine", "cardiology"]
        "required_experience_yrs":  int | None, # minimum, None if unspecified
        "domain":                   str,        # e.g. "medical", "software_engineering", "language"
        "geography":                str,        # e.g. "India", "Global", "US"
        "raw_excerpt":              str,        # first ~500 chars, PII-stripped
    }

If the LLM fails or the html is empty, returns an EMPTY_ICP sentinel with
`derived_tg_label == ""` — callers should treat that as "no signal, abort with
explicit message" rather than passing empty fields to the copy-writer.
"""
from __future__ import annotations

import html as _html
import json
import logging
import re
from typing import Any

import config
from src.claude_client import call_claude

log = logging.getLogger(__name__)

EMPTY_ICP: dict[str, Any] = {
    "derived_tg_label":        "",
    "required_skills":         [],
    "preferred_skills":        [],
    "required_degrees":        [],
    "required_fields":         [],
    "required_experience_yrs": None,
    "domain":                  "",
    "geography":               "",
    "raw_excerpt":             "",
}

_SYSTEM_PROMPT = (
    "You are an ICP extraction engine for Outlier, an AI-training platform that "
    "matches domain experts to paid remote tasks. You receive a project's public "
    "job post (often HTML) and return a single JSON object describing the ideal "
    "contributor profile — no prose, no code fences, no extra keys."
)

_EXTRACTION_INSTRUCTIONS = """\
Extract the ideal-contributor profile from the job post below and return ONLY a JSON object with EXACTLY these keys:

  derived_tg_label          — short human label (≤60 chars), e.g. "Pediatric Cardiologists (India)"
  required_skills           — list of strings (must-have hard skills, each ≤40 chars)
  preferred_skills          — list of strings (nice-to-have)
  required_degrees          — list of strings, e.g. ["MBBS", "MD", "PhD in Statistics"]
  required_fields           — list of lowercase single-word or short-phrase fields, e.g. ["cardiology", "medicine"]
  required_experience_yrs   — integer minimum years, or null if unspecified
  domain                    — one short tag: medical | software_engineering | data_science | language | math | legal | finance | general
  geography                 — country or region, or "Global" if unspecified
  raw_excerpt               — the first ≤500 characters of the cleaned job text (no HTML tags, no emails/phones)

RULES:
- Omit PII from raw_excerpt (names, emails, phone numbers, contact links).
- Use [] (empty list) where a field is unknown. Use null for required_experience_yrs if unknown.
- Do not invent data. If the job post is empty or too short to extract anything, return every field empty per the defaults ({} / [] / null).
- Output MUST parse as JSON. No markdown fencing.
"""


def _clean_html(raw: str) -> str:
    """Strip HTML tags + entities, normalize whitespace, remove obvious PII."""
    if not raw:
        return ""
    # Remove scripts/styles entirely
    cleaned = re.sub(r"(?is)<(script|style).*?</\1>", " ", raw)
    # Strip all remaining tags
    cleaned = re.sub(r"<[^>]+>", " ", cleaned)
    # Decode HTML entities
    cleaned = _html.unescape(cleaned)
    # Redact obvious PII tokens
    cleaned = re.sub(r"[\w.+-]+@[\w-]+\.[\w.-]+", "[email]", cleaned)
    cleaned = re.sub(r"\+?\d[\d\s().-]{7,}\d", "[phone]", cleaned)
    # Collapse whitespace
    cleaned = re.sub(r"\s+", " ", cleaned).strip()
    return cleaned




def _coerce(obj: Any) -> dict[str, Any]:
    """Merge LLM output into the canonical schema, coercing types and dropping garbage."""
    out: dict[str, Any] = dict(EMPTY_ICP)
    if not isinstance(obj, dict):
        return out

    def _as_str(v: Any, max_len: int) -> str:
        if v is None:
            return ""
        s = str(v).strip()
        return s[:max_len]

    def _as_str_list(v: Any, max_items: int = 20, max_item_len: int = 80) -> list[str]:
        if v is None:
            return []
        if isinstance(v, str):
            parts = [p.strip() for p in re.split(r"[,;|]\s*", v) if p.strip()]
        elif isinstance(v, (list, tuple, set)):
            parts = [str(p).strip() for p in v if p is not None and str(p).strip()]
        else:
            return []
        return [p[:max_item_len] for p in parts[:max_items]]

    out["derived_tg_label"]  = _as_str(obj.get("derived_tg_label"), 60)
    out["required_skills"]   = _as_str_list(obj.get("required_skills"))
    out["preferred_skills"]  = _as_str_list(obj.get("preferred_skills"))
    out["required_degrees"]  = _as_str_list(obj.get("required_degrees"), max_item_len=60)
    out["required_fields"]   = [f.lower() for f in _as_str_list(obj.get("required_fields"), max_item_len=40)]
    out["domain"]            = _as_str(obj.get("domain"), 40).lower().replace(" ", "_")
    out["geography"]         = _as_str(obj.get("geography"), 60) or "Global"
    out["raw_excerpt"]       = _as_str(obj.get("raw_excerpt"), 500)

    yrs = obj.get("required_experience_yrs")
    if yrs in (None, "", "null"):
        out["required_experience_yrs"] = None
    else:
        try:
            out["required_experience_yrs"] = int(float(yrs))
        except (TypeError, ValueError):
            out["required_experience_yrs"] = None
    return out


def derive_icp_from_job_post(html_text: str) -> dict[str, Any]:
    """
    Parse a project's job-post HTML and return an ICP spec. Returns EMPTY_ICP
    (derived_tg_label="") if the post is empty, too short, or extraction fails —
    callers should treat that as "no data → abort cold_start".

    This is the ONLY ICP input to the copy-writer in cold_start mode, so the
    returned schema must match what outlier-copy-writer and the TG classifier
    expect.
    """
    cleaned = _clean_html(html_text or "")
    if len(cleaned) < 40:
        log.info("Job post too short after cleaning (%d chars) — returning EMPTY_ICP", len(cleaned))
        icp = dict(EMPTY_ICP)
        icp["raw_excerpt"] = cleaned[:500]
        return icp

    prompt = (
        f"{_EXTRACTION_INSTRUCTIONS}\n"
        f"--- JOB POST (cleaned) ---\n{cleaned[:6000]}\n--- END JOB POST ---"
    )

    try:
        raw = call_claude(
            messages=[{"role": "user", "content": prompt}],
            system=_SYSTEM_PROMPT,
            cache_system=True,
            max_tokens=1024,
        )
    except Exception as exc:
        log.warning("Claude ICP extraction failed: %s (%s)", exc, type(exc).__name__, exc_info=True)
        return dict(EMPTY_ICP, raw_excerpt=cleaned[:500])

    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError:
        # Some models still emit code-fenced JSON — strip and retry once.
        stripped = re.sub(r"^```(?:json)?\s*|\s*```$", "", raw.strip(), flags=re.IGNORECASE)
        try:
            parsed = json.loads(stripped)
        except json.JSONDecodeError as exc:
            log.warning("LLM returned non-JSON for ICP extraction: %s (%s)", exc, raw[:200])
            return dict(EMPTY_ICP, raw_excerpt=cleaned[:500])

    icp = _coerce(parsed)
    # Always carry a cleaned excerpt even if the model omitted it — it's the
    # audit trail that shows up in the Slack summary.
    if not icp["raw_excerpt"]:
        icp["raw_excerpt"] = cleaned[:500]
    log.info(
        "Derived ICP from job post: label=%r domain=%r geography=%r (skills=%d, degrees=%d)",
        icp["derived_tg_label"], icp["domain"], icp["geography"],
        len(icp["required_skills"]), len(icp["required_degrees"]),
    )
    return icp


def resolve_job_post(
    redash_client,
    project_id: str,
    signup_flow_id: str | None = None,
    override_text: str | None = None,
) -> str:
    """
    Get the raw job-post HTML for a project.

    Priority:
      1. `override_text` — if the caller passed one (CLI flag / sheet cell), use it as-is.
      2. Snowflake lookup via `redash_client.fetch_job_post(signup_flow_id)` — requires
         a signup_flow_id (PUBLIC.JOBPOSTS is keyed only by SIGNUP_FLOW_ID).
      3. `redash_client.fetch_project_meta(project_id).description` — cold-start
         fallback for internal projects that never had a public signup flow
         (e.g. Valkyrie Internal). PUBLIC.PROJECTS.DESCRIPTION is typically
         Markdown/plaintext, not HTML, but the cleaner in derive_icp_from_job_post
         handles both.
      4. Empty string — caller's cold_start branch must handle this gracefully.
    """
    if override_text:
        return override_text

    if signup_flow_id:
        try:
            raw = redash_client.fetch_job_post(signup_flow_id) or ""
            if raw:
                return raw
        except Exception as exc:
            log.warning("resolve_job_post: fetch_job_post failed for flow=%s: %s", signup_flow_id, exc)

    if project_id:
        try:
            meta = redash_client.fetch_project_meta(project_id) or {}
        except Exception as exc:
            log.warning("resolve_job_post: fetch_project_meta failed for project=%s: %s", project_id, exc)
            meta = {}
        desc = (meta.get("description") or "").strip()
        if desc:
            # Prepend the project name when available so the LLM has a clear title line.
            name = (meta.get("project_name") or meta.get("tasker_name") or "").strip()
            return f"# {name}\n\n{desc}" if name else desc

    log.info(
        "resolve_job_post: nothing to return for project=%s flow=%s — cold-start has no signal.",
        project_id or "—", signup_flow_id or "—",
    )
    return ""


# ── Base-role extraction ──────────────────────────────────────────────────────

# These titles + family keywords drive the "base role" heuristic. When a
# project's job post says it's for "Data Analytics" or "Data Analyst", Stage A
# will anchor every shortlisted cohort on a Data Analyst / Data Scientist /
# Analytics-adjacent feature. The list is intentionally short — add as we see
# real projects that don't fit.
_BASE_ROLE_FAMILIES: dict[str, list[str]] = {
    "data_analyst":       ["Data Analyst", "Data Analytics", "Business Analyst", "Analytics"],
    "data_scientist":     ["Data Scientist", "Data Science", "Machine Learning Scientist", "ML Scientist", "Research Scientist"],
    # Extended with "Coding" / "Programming" keywords so a project whose WORKER_SKILLS
    # include "Coding" (OpenClaw does) triggers this family via WS alone — no LLM needed.
    "software_engineer":  ["Software Engineer", "Software Developer", "Backend Engineer", "Frontend Engineer", "Full Stack", "Coding", "Programming", "Programmer"],
    "ml_engineer":        ["ML Engineer", "Machine Learning Engineer", "AI Engineer", "LLM Engineer"],
    "mathematician":      ["Mathematician", "Mathematics", "Statistician", "Quantitative"],
    "physician":          ["Physician", "Doctor", "Medical Doctor", "Cardiologist", "Radiologist", "MBBS", "MD"],
    "nurse":              ["Nurse", "Registered Nurse", "RN", "Nurse Practitioner"],
    "lawyer":             ["Lawyer", "Attorney", "Counsel", "Paralegal"],
    "finance":            ["Financial Analyst", "Investment Analyst", "CFA", "Accountant"],
    "linguist":           ["Linguist", "Translator", "Interpreter", "Language Expert"],
    "teacher":            ["Teacher", "Professor", "Tutor", "Instructor", "Academic"],
    "biologist":          ["Biologist", "Biology", "Molecular Biology", "Biochemistry", "Geneticist"],
    "chemist":            ["Chemist", "Chemistry", "Organic Chemistry"],
    "physicist":          ["Physicist", "Physics"],
}


# Maps Outlier WORKER_SKILL_NAME (from PROJECT_QUALIFICATIONS_LONG) to candidate
# LinkedIn-skill terms. Worker skills are capability buckets ("Coding", "Data
# Science"), not LinkedIn's skill vocabulary — so we expand each bucket into
# the concrete skills a LinkedIn user would list if they can do that work.
# The `required_skill_feature_columns` path then intersects these against
# `skills__*` columns in the frame, so only skills that actually appear among
# our CBs become anchors.
_WORKER_SKILL_TO_LINKEDIN_SKILLS: dict[str, list[str]] = {
    "coding":       ["Python", "JavaScript", "Java", "TypeScript", "C++", "Go", "SQL",
                     "React", "Node.js", "Software Development", "Programming",
                     "Algorithms", "Data Structures"],
    "data science": ["Python", "R", "SQL", "Machine Learning", "Data Science",
                     "Pandas", "Statistics", "Statistical Analysis",
                     "Data Analysis", "Data Visualization"],
    "data analysis": ["Data Analysis", "SQL", "Excel", "Tableau", "Power BI",
                      "Python", "R", "Statistics"],
    "biology":      ["Biology", "Molecular Biology", "Biochemistry", "Genetics",
                     "Cell Biology", "Microbiology", "Life Sciences"],
    "chemistry":    ["Chemistry", "Organic Chemistry", "Biochemistry",
                     "Analytical Chemistry"],
    "physics":      ["Physics", "Applied Physics", "Theoretical Physics"],
    "mathematics":  ["Mathematics", "Applied Mathematics", "Statistics",
                     "Linear Algebra", "Calculus", "Probability"],
    "medicine":     ["Medicine", "Clinical Medicine", "Internal Medicine",
                     "Pharmacology", "Patient Care"],
    "translation":  ["Translation", "Localization", "Interpretation"],
    "writing":      ["Creative Writing", "Technical Writing", "Copywriting",
                     "Content Writing"],
    "law":          ["Legal Research", "Legal Writing", "Litigation",
                     "Corporate Law"],
    "finance":      ["Financial Analysis", "Financial Modeling", "Accounting",
                     "Investment Analysis"],
}


def worker_skill_to_linkedin_skills(worker_skills: list[str]) -> list[str]:
    """Expand a list of Outlier WORKER_SKILL_NAMEs into candidate LinkedIn
    skill terms via `_WORKER_SKILL_TO_LINKEDIN_SKILLS`. Unknown buckets are
    passed through unchanged (let the downstream matcher try)."""
    out: list[str] = []
    seen: set[str] = set()
    for ws in worker_skills or []:
        key = _norm(ws).strip()
        expansions = _WORKER_SKILL_TO_LINKEDIN_SKILLS.get(key, [ws])
        for s in expansions:
            if s and s not in seen:
                seen.add(s)
                out.append(s)
    return out

# Per-family negation facets — layered on top of config.DEFAULT_EXCLUDE_FACETS
# when a base role is detected. Each entry is a (facet, value) pair that gets
# fuzzy-resolved via the URN sheet, same path as the defaults. The goal is to
# drop LinkedIn users who share a title *keyword* with the base role but do
# fundamentally different work — e.g. "Sales Analyst" is an Analyst-family
# title, shares "Analyst" with Data Analyst, but targets a completely
# different job function.
#
# Keep each list conservative. Each addition further narrows the audience, so
# only add things you're confident about. Families without a clear adjacent-
# role trap just map to [].
_BASE_ROLE_EXCLUSIONS: dict[str, list[tuple[str, str]]] = {
    "data_analyst": [
        ("titles", "Sales Analyst"),
        ("titles", "Marketing Analyst"),
        ("titles", "Human Resources Analyst"),
        ("titles", "Operations Analyst"),
    ],
    "data_scientist": [
        ("titles", "Research Assistant"),   # academic RAs, not industry DS
        ("titles", "Market Research Analyst"),
    ],
    "software_engineer": [
        ("titles", "Sales Engineer"),
        ("titles", "Solutions Engineer"),   # often pre-sales, not builders
        ("titles", "Support Engineer"),
    ],
    "ml_engineer": [
        ("titles", "Research Assistant"),
    ],
    "mathematician": [
        ("titles", "Math Teacher"),         # K-12 teachers aren't our TG
    ],
    "physician": [
        ("titles", "Medical Sales Representative"),
        ("titles", "Pharmaceutical Sales Representative"),
        ("titles", "Medical Device Sales"),
    ],
    "nurse": [
        ("titles", "Medical Sales Representative"),
        ("titles", "Nurse Recruiter"),
    ],
    "lawyer": [
        ("titles", "Legal Secretary"),
        ("titles", "Legal Assistant"),      # distinct from Paralegal which is in includes
    ],
    "finance": [
        ("titles", "Financial Advisor"),    # retail advisors, not analysts
        ("titles", "Insurance Sales Agent"),
    ],
    "linguist": [
        ("titles", "Language Teacher"),
    ],
    "teacher": [
        ("titles", "Teaching Assistant"),   # grad TAs skew toward our TG actually — revisit
    ],
}


def _matched_family_keys(
    job_post_meta: dict | None = None,
    project_meta: dict | None = None,
    signup_flow_name: str | None = None,
    derived_tg_label: str | None = None,
    worker_skills: list[str] | None = None,
) -> list[str]:
    """Return the list of family KEYS (e.g. "data_analyst") that matched the
    input sources. Same matching logic as `extract_base_role_candidates`, just
    returns keys instead of flattened titles so the caller can look up the
    family's exclusion list in `_BASE_ROLE_EXCLUSIONS`.

    `worker_skills` is accepted for signature parity with the caller but is
    intentionally NOT fed into the haystack. WS are internal capability-bucket
    labels (e.g. OpenClaw lists "Biology" as a T3 gate even though its CBs are
    software engineers — the label verifies domain knowledge, not what those
    CBs would put on LinkedIn). Matching families on WS would trigger false-
    positive families (biologist/chemist) on projects where those aren't the
    actual targeting audience. So: family detection stays tied to the
    human-readable project text only; WS flows to the summary as a suggestion.
    """
    _ = worker_skills  # deliberately unused; see docstring

    haystack_parts: list[str] = []
    for src in (
        (job_post_meta or {}).get("job_name"),
        (job_post_meta or {}).get("job_post_name"),
        (job_post_meta or {}).get("domain"),
        signup_flow_name,
        (project_meta or {}).get("project_name"),
        (project_meta or {}).get("tasker_name"),
        derived_tg_label,
    ):
        if src:
            haystack_parts.append(_norm(str(src)))
    haystack = " | ".join(haystack_parts)
    if not haystack:
        return []

    matched: list[str] = []
    for key, titles in _BASE_ROLE_FAMILIES.items():
        if any(_word_hit(_norm(t), haystack) for t in titles):
            matched.append(key)
    return matched


def family_exclusions_for(
    job_post_meta: dict | None = None,
    project_meta: dict | None = None,
    signup_flow_name: str | None = None,
    derived_tg_label: str | None = None,
    worker_skills: list[str] | None = None,
) -> list[tuple[str, str]]:
    """Return the union of `(facet, value)` exclusion pairs for every matched
    `_BASE_ROLE_FAMILIES` family, deduped in input order.

    Example:
        haystack contains "Data Analyst"
        → matched keys = ["data_analyst"]
        → returns _BASE_ROLE_EXCLUSIONS["data_analyst"]
    """
    keys = _matched_family_keys(
        job_post_meta=job_post_meta, project_meta=project_meta,
        signup_flow_name=signup_flow_name, derived_tg_label=derived_tg_label,
        worker_skills=worker_skills,
    )
    seen: set[tuple[str, str]] = set()
    out: list[tuple[str, str]] = []
    for k in keys:
        for pair in _BASE_ROLE_EXCLUSIONS.get(k, []):
            if pair not in seen:
                seen.add(pair)
                out.append(pair)
    if out:
        log.info(
            "Family-specific exclusions for %s: %s",
            keys, out,
        )
    return out


def _norm(s: str) -> str:
    return (s or "").lower().replace("_", " ").replace("-", " ")


def _word_hit(needle: str, haystack: str) -> bool:
    """Word-boundary substring match with optional trailing plural 's'.

    Prevents false positives like 'RN' matching 'inte**rn**al' in project names
    (we saw this on Valkyrie Internal Human Performance, which triggered the
    Nurse family via 'rn' inside 'internal').

    Allows the plural form of the needle to count as a match: "Software
    Engineer" matches both "software engineer" and "software engineers", but
    still does NOT match "software engineering" (different root word). Without
    this, "Backend/AI Software Engineers" — the LLM's own derived_tg_label
    for OpenClaw — wouldn't hit the software_engineer family.
    """
    if not needle or not haystack:
        return False
    return re.search(rf"\b{re.escape(needle)}s?\b", haystack) is not None


def extract_base_role_candidates(
    job_post_meta: dict | None = None,
    project_meta: dict | None = None,
    signup_flow_name: str | None = None,
    derived_tg_label: str | None = None,
    worker_skills: list[str] | None = None,
) -> list[str]:
    """
    Return a deduped list of candidate base-role titles drawn from the
    structured sources available (most specific first):

      1. `jobposts.job_name` — e.g. "Analyst for AI Training (Data Analytics & Modeling)"
      2. `jobposts.domain`   — e.g. "Data Analytics"
      3. `signup_flow_name`  — e.g. "Data Analyst Screening T1"
      4. `projects.name`     — e.g. "Generalist Checkpoint Evals"
      5. `derived_tg_label`  — the LLM-extracted label when the above are missing

    Matches against `_BASE_ROLE_FAMILIES` and returns the union of matched
    family titles. If nothing matches, returns [] — caller should fall back to
    "no base role, anchor not enforced".

    Example:
        inputs: domain="Data Analytics"
        output: ["Data Analyst", "Data Analytics", "Business Analyst", "Analytics",
                 "Data Scientist", ...]  ← merged Data_Analyst + Data_Scientist families
    """
    _ = worker_skills  # accepted for signature parity, deliberately unused — see _matched_family_keys docstring.

    haystack_parts: list[str] = []
    for src in (
        (job_post_meta or {}).get("job_name"),
        (job_post_meta or {}).get("job_post_name"),
        (job_post_meta or {}).get("domain"),
        signup_flow_name,
        (project_meta or {}).get("project_name"),
        (project_meta or {}).get("tasker_name"),
        derived_tg_label,
    ):
        if src:
            haystack_parts.append(_norm(str(src)))
    haystack = " | ".join(haystack_parts)
    if not haystack:
        return []

    out: list[str] = []
    seen: set[str] = set()
    for family_titles in _BASE_ROLE_FAMILIES.values():
        hit = any(_word_hit(_norm(t), haystack) for t in family_titles)
        if hit:
            for t in family_titles:
                key = _norm(t)
                if key not in seen:
                    seen.add(key)
                    out.append(t)
    log.info(
        "Base-role candidates from [job_name=%r, domain=%r, flow_name=%r, proj_name=%r, worker_skills=%s]: %s",
        (job_post_meta or {}).get("job_name"),
        (job_post_meta or {}).get("domain"),
        signup_flow_name,
        (project_meta or {}).get("project_name"),
        worker_skills or [],
        out,
    )
    return out


def base_role_feature_columns(
    base_role_titles: list[str],
    df_columns: list[str],
) -> list[str]:
    """
    Map a list of base-role titles (e.g. ["Data Analyst", "Data Scientist"]) to
    the matching `job_titles_norm__*` dummy column names present in the current
    DataFrame. Case-insensitive, space→underscore match.

    Returns an ordered, deduped list of column names. Empty list → no base role
    found in the frame, caller should skip the anchor.
    """
    if not base_role_titles:
        return []
    wanted = {_norm(t).replace(" ", "_") for t in base_role_titles}
    out: list[str] = []
    seen: set[str] = set()
    for col in df_columns:
        if not col.startswith("job_titles_norm__"):
            continue
        tail = col[len("job_titles_norm__"):].lower()
        if tail in wanted and col not in seen:
            seen.add(col)
            out.append(col)
    return out


def required_skill_feature_columns(
    required_skills: list[str],
    df_columns: list[str],
) -> list[str]:
    """
    Same idea as `base_role_feature_columns` but for the LLM-derived ICP's
    `required_skills` field — map each skill string to its matching `skills__*`
    dummy column in the frame. These become additional base-role anchors so
    Stage A synthesises combos like `skills__Python × skills__Java` when the
    job post explicitly requires both.

    Matching is generous: we normalise both sides (lowercase, spaces →
    underscores, strip punctuation) and accept exact hits. Fuzzy matching
    stays cheap here because `skills__` columns already are the frame's most
    recognisable skill labels.
    """
    if not required_skills:
        return []
    wanted: set[str] = set()
    for s in required_skills:
        norm = _norm(s).replace(" ", "_")
        # Also strip common punctuation from skill names like "C/C++" or "Node.js"
        norm = re.sub(r"[^a-z0-9_]", "_", norm).strip("_")
        if norm:
            wanted.add(norm)
    out: list[str] = []
    seen: set[str] = set()
    for col in df_columns:
        if not col.startswith("skills__"):
            continue
        tail = col[len("skills__"):].lower()
        if tail in wanted and col not in seen:
            seen.add(col)
            out.append(col)
    if out:
        log.info(
            "LLM-required-skill anchors matched in frame: %s (from %d skills in ICP)",
            out, len(required_skills),
        )
    return out

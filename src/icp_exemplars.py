"""
ICP Exemplars — pick up to 5 representative ICP profiles for user-facing summaries.

Built AFTER pick_target_tier() has chosen the target column. Selects diverse positive
examples (different degree levels / countries / companies when possible) and returns
PII-stripped profile dicts — cb_id only, NEVER name/email/phone.

Exemplars flow to:
  - Slack DM summary (src/campaign_summary_slack.py)
  - Console / sheet run reports

They are NEVER passed to outlier-copy-writer or ad-creative-brief-generator — those
agents target a statistical cohort, not individuals.
"""
import logging
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)

DEFAULT_MAX_EXEMPLARS = 5

# Fields surfaced per exemplar. cb_id is the only identifier we emit.
_EXEMPLAR_RESUME_FIELDS = [
    "resume_job_title", "resume_job_company", "resume_degree", "resume_field",
    "resume_job_skills",
]
_EXEMPLAR_LINKEDIN_FIELDS = [
    "linkedin_url", "linkedin_education", "linkedin_certifications",
]
# Banned PII fields — never emit these even if present
_PII_BLOCKLIST = {"first_name", "last_name", "full_name", "name", "email", "phone", "phone_number"}


def _top_skills(skills_raw: Any, limit: int = 5) -> list[str]:
    """Parse the RESUME_JOB_SKILLS value (string or list) into a trimmed list."""
    if skills_raw is None or (isinstance(skills_raw, float) and pd.isna(skills_raw)):
        return []
    if isinstance(skills_raw, list):
        items = [str(s).strip() for s in skills_raw if s]
    else:
        raw = str(skills_raw).strip()
        # Split on commas, semicolons, or pipes (Scale's resume_job_skills uses pipes).
        seen: set[str] = set()
        items: list[str] = []
        for part in raw.replace(";", ",").replace("|", ",").split(","):
            p = part.strip()
            if p and p.lower() not in seen:
                seen.add(p.lower())
                items.append(p)
    return items[:limit]


def _shorten_pipe_list(raw: str, max_items: int = 3, max_chars: int = 100) -> str:
    """Scale resume fields pack multiple values with ' | ' separators. Return the first
    few items trimmed for display."""
    if not raw:
        return ""
    parts = [p.strip() for p in str(raw).split("|") if p.strip()]
    if not parts:
        return str(raw)[:max_chars]
    kept = parts[:max_items]
    out = ", ".join(kept)
    if len(parts) > max_items:
        out += f" (+{len(parts) - max_items} more)"
    if len(out) > max_chars:
        out = out[:max_chars].rstrip(",. ") + "…"
    return out


def _qualifying_signals(row: pd.Series, tier: str) -> list[str]:
    """
    Build 3 human-readable bullets describing why this CB is a strong ICP. Heuristic,
    not LLM-generated — keeps exemplars cheap. For richer signals, upgrade this
    function to call an LLM in a follow-up.
    """
    bullets: list[str] = []

    # Tier-specific lead — handles both new STAGE1_SQL columns and legacy RESUME_SQL columns.
    if tier == "T3":
        # New path: total_payout_attempts + last_task_date from CBPR.
        attempts = int(row.get("total_payout_attempts") or 0)
        last_task = row.get("last_task_date")
        # Legacy path: task_count_30d came from TASK_ATTEMPTS_W_PAYOUT with a 30-day window.
        legacy_30d = int(row.get("task_count_30d") or 0)
        if attempts > 0:
            lead = f"Activated on project: {attempts} paid task attempts"
            if last_task and not pd.isna(last_task):
                lead += f" (last {str(last_task)[:10]})"
            bullets.append(lead)
        elif legacy_30d > 0:
            bullets.append(f"Active on project: {legacy_30d} task attempts in the last 30 days")
        else:
            bullets.append("Activated on project (has attempted at least one paid task)")
    elif tier == "T2.5":
        started = row.get("ocp_started_at")
        bullets.append(
            f"Started project onboarding{f' ({str(started)[:10]})' if started and not pd.isna(started) else ''}"
        )
    elif tier == "T2":
        # New path: CESF funnel boolean — no numerator/denominator split available.
        n_done = int(row.get("courses_completed_count") or 0)
        n_req  = int(row.get("courses_total_required") or 0)
        if n_req > 0:
            bullets.append(f"Course-eligible: completed {n_done}/{n_req} required courses")
        else:
            bullets.append("Completed this project's onboarding course(s)")
    elif tier == "T1":
        bullets.append("Passed the resume screening for this project")

    # Experience / seniority
    years = row.get("total_years_experience")
    if years and not pd.isna(years):
        try:
            yrs = float(years)
            if yrs >= 10:
                bullets.append(f"{yrs:.0f}+ years professional experience")
            elif yrs >= 5:
                bullets.append(f"{yrs:.0f} years professional experience")
        except (TypeError, ValueError):
            pass

    # Degree + field
    deg = row.get("resume_degree") or row.get("highest_degree_level")
    field = row.get("resume_field")
    if deg and not pd.isna(deg):
        if field and not pd.isna(field):
            bullets.append(f"{deg} in {field}")
        else:
            bullets.append(str(deg))

    # Most recent job — pipe-separated strings get compressed to top 3
    title = row.get("resume_job_title")
    company = row.get("resume_job_company")
    if title and not pd.isna(title):
        title_short = _shorten_pipe_list(str(title), max_items=2, max_chars=80)
        role_line = title_short
        if company and not pd.isna(company):
            company_short = _shorten_pipe_list(str(company), max_items=2, max_chars=60)
            role_line += f" at {company_short}"
        bullets.append(role_line)

    # Accreditations
    accred = row.get("accreditations_str") or row.get("linkedin_certifications")
    if accred and not pd.isna(accred) and len(str(accred)) > 5:
        bullets.append(f"Accreditations: {str(accred)[:80]}")

    return bullets[:3]


def _safe(val: Any) -> Any:
    """Return val if non-null/non-nan, else None."""
    if val is None:
        return None
    try:
        if pd.isna(val):
            return None
    except (TypeError, ValueError):
        pass
    return val


def build_exemplars(
    df: pd.DataFrame,
    target_col: str,
    tier: str,
    max_count: int = DEFAULT_MAX_EXEMPLARS,
) -> list[dict]:
    """
    Return up to `max_count` PII-stripped exemplar profiles drawn from the positive rows.

    Diversification: prefer different combinations of (highest_degree_level, country,
    resume_job_company). Ties broken by preferring rows with both resume + LinkedIn data.

    Args:
        df: the screening DataFrame from Stage 1
        target_col: the positive-indicator column (e.g. "tasked_on_project")
        tier: human-readable tier label (T3/T2/T1) for the bullets
        max_count: max number of exemplars to return

    Returns:
        list of dicts. Each dict is safe to pass to user-facing channels — cb_id is the
        only identifier, zero PII columns.
    """
    if target_col not in df.columns:
        log.warning("build_exemplars: target_col %r missing", target_col)
        return []
    positives = df[df[target_col].fillna(False).astype(bool)].copy()
    if positives.empty:
        log.info("build_exemplars: no positives found for %s", target_col)
        return []

    # Diversity key — tuple of (degree, country, company). Rows with more data come first.
    positives["_has_linkedin"] = positives["linkedin_url"].notna() if "linkedin_url" in positives.columns else False
    positives["_has_resume"] = positives["resume_job_title"].notna() if "resume_job_title" in positives.columns else False
    positives["_completeness"] = positives["_has_linkedin"].astype(int) + positives["_has_resume"].astype(int)
    positives = positives.sort_values(["_completeness"], ascending=False)

    seen_keys: set[tuple] = set()
    chosen: list[pd.Series] = []
    for _, row in positives.iterrows():
        key = (
            str(row.get("highest_degree_level") or row.get("resume_degree") or ""),
            str(row.get("country") or ""),
            str(row.get("resume_job_company") or ""),
        )
        if key in seen_keys:
            continue
        seen_keys.add(key)
        chosen.append(row)
        if len(chosen) >= max_count:
            break

    # If we didn't get enough diverse rows, top up with any positive (may duplicate keys)
    if len(chosen) < max_count:
        for _, row in positives.iterrows():
            if any(c.name == row.name for c in chosen):
                continue
            chosen.append(row)
            if len(chosen) >= max_count:
                break

    out: list[dict] = []
    for row in chosen:
        # Explicit PII check — drop anything in the blocklist
        exemplar: dict = {
            "cb_id": _safe(row.get("user_id")),
            "tier": tier,
            "top_qualifying_signals": _qualifying_signals(row, tier),
            "top_skills": _top_skills(row.get("resume_job_skills")),
        }
        for fld in _EXEMPLAR_RESUME_FIELDS:
            if fld == "resume_job_skills":
                continue  # already summarised into top_skills
            exemplar[fld] = _safe(row.get(fld))
        for fld in _EXEMPLAR_LINKEDIN_FIELDS:
            exemplar[fld] = _safe(row.get(fld))

        # Safety net — strip any blocklisted field
        for bad in list(exemplar):
            if bad.lower() in _PII_BLOCKLIST:
                exemplar.pop(bad, None)
        out.append(exemplar)

    log.info("Built %d exemplars from %d positives (tier=%s)", len(out), len(positives), tier)
    return out


def format_exemplars_for_slack(exemplars: list[dict]) -> str:
    """Render exemplars as a Slack-friendly bullet list. Returns empty string if list is empty."""
    if not exemplars:
        return ""
    lines: list[str] = []
    lines.append("*👥 ICP Exemplars (PII-stripped)*")
    for i, ex in enumerate(exemplars, 1):
        cb_id = ex.get("cb_id") or "?"
        lines.append(f"")
        lines.append(f"_{i}. cb_id `{cb_id}` — tier {ex.get('tier', '?')}_")
        title = ex.get("resume_job_title")
        company = ex.get("resume_job_company")
        if title:
            title_short = _shorten_pipe_list(str(title), max_items=2, max_chars=80)
            role = title_short
            if company:
                company_short = _shorten_pipe_list(str(company), max_items=2, max_chars=60)
                role += f" @ {company_short}"
            lines.append(f"  • {role}")
        deg = ex.get("resume_degree")
        field = ex.get("resume_field")
        if deg:
            deg_short = _shorten_pipe_list(str(deg), max_items=2, max_chars=50)
            field_short = _shorten_pipe_list(str(field), max_items=2, max_chars=50) if field else None
            lines.append(f"  • {deg_short}" + (f" in {field_short}" if field_short else ""))
        if ex.get("top_skills"):
            lines.append(f"  • Skills: {', '.join(ex['top_skills'])}")
        if ex.get("linkedin_url"):
            lines.append(f"  • LinkedIn: {ex['linkedin_url']}")
        signals = ex.get("top_qualifying_signals") or []
        for s in signals:
            lines.append(f"    ‣ {s}")
    return "\n".join(lines)

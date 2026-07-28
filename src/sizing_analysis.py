"""Ad-hoc sizing analysis — per-channel audience sizing + ICP, no launch.

"Size up" a potential campaign without launching it. Three input modes, each
deriving the ICP the SAME way our ramp ICPs are derived (the common parameters
among the high-quality contributors), then measuring per-channel audience:

- project  → reuse main._resolve_cohorts (pick_target_tier → Stage A/B/C mine →
             ICP enrich → per-channel audience), keyed to a synthetic analysis
             id. This IS the ramp-ICP path; nothing new.
- job_post → derive cohort specs + ICP from JD text (src.icp_from_jobpost),
             build cohorts, size each.
- cb_ids   → treat the supplied qualified CBs as the high-quality set and mine
             their common parameters (modal skills/titles/fields from their
             resumes), build a cohort, size it.

Results are persisted to cohort_audience / cohort_icp / cohort_targeting keyed by
ramp_id = analysis_id, so the console renders them exactly like a ramp's sizing.
No briefs, no creatives, no launch.
"""
from __future__ import annotations

import logging
import os
import re
import statistics
from collections import Counter

import config
from src import ui_decisions

log = logging.getLogger(__name__)


def _parse_list(raw: str, *, sep_newline: bool = True) -> list[str]:
    s = raw or ""
    if sep_newline:
        s = s.replace("\n", ",")
    return [x.strip() for x in s.split(",") if x.strip()]


def _slug(v: str) -> str:
    return re.sub(r"[^a-z0-9]+", "_", (v or "").strip().lower()).strip("_")


def _build_clients():
    """Construct the shared clients _resolve_cohorts / sizing need, mirroring
    run_launch_for_ramp (main.py). Requires a LinkedIn token for Stage C + the
    LinkedIn/Meta/Google audience estimates."""
    from src.sheets import SheetsClient
    from src.redash_db import RedashClient
    from src.linkedin_api import LinkedInClient
    from src.linkedin_urn import UrnResolver

    sheets = SheetsClient()
    sheet_cfg = sheets.read_config()
    li_token = (
        sheet_cfg.get("LINKEDIN_TOKEN")
        or os.getenv("LINKEDIN_ACCESS_TOKEN")
        or os.getenv("LINKEDIN_TOKEN")
        or config.LINKEDIN_TOKEN
    )
    if not li_token:
        raise RuntimeError("LINKEDIN_TOKEN not set — sizing needs it for Stage C + audience")
    li_client = LinkedInClient(li_token)
    urn_res = UrnResolver(sheets, linkedin_client=li_client)
    snowflake = RedashClient()
    claude_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("CLAUDE_API_KEY", "")
    return sheets, snowflake, li_client, urn_res, claude_key


def build_cohorts_from_specs(specs: list[dict]) -> list:
    """Spec dict → Cohort (rules across skills / titles / fields / degrees).
    Mirrors main.py's cold-start spec→Cohort builder; used by the job_post and
    cb_ids paths. A spec is {label, required_skills, job_titles, fields_of_study,
    degrees, geos, skills_only?}."""
    from src.analysis import Cohort
    from src.icp_from_jobpost import extract_base_role_candidates, family_exclusions_for
    from src.sheets import make_stg_id

    cohorts: list = []
    seen_names: set[str] = set()
    for spec in specs:
        label = (spec.get("label") or "Sizing cohort").strip()
        rules: list[tuple[str, int]] = []
        seen_rules: set[str] = set()

        def _add(prefix: str, value: str):
            slug = _slug(value)
            key = f"{prefix}__{slug}"
            if slug and key not in seen_rules:
                seen_rules.add(key)
                rules.append((key, 1))

        for s in (spec.get("required_skills") or [])[:5]:
            _add("skills", s)
        titles = list(spec.get("job_titles") or [])
        if not spec.get("skills_only"):
            titles += extract_base_role_candidates(derived_tg_label=label)
        for t in titles:
            _add("job_titles_norm", t)
        for f in (spec.get("fields_of_study") or [])[:3]:
            _add("fields_of_study", f)
        for d in (spec.get("degrees") or [])[:2]:
            _add("highest_degree_level", d)

        if not rules:
            log.warning("build_cohorts_from_specs: spec %r produced no rules — skipping", label)
            continue

        name = label
        if name in seen_names:
            disc = (spec.get("required_skills") or spec.get("geos") or [""])[0]
            name = f"{label} ({disc})".strip()[:80] if disc else f"{label} #{len(seen_names) + 1}"
        seen_names.add(name)

        cohort = Cohort(name=name, rules=rules)
        cohort.exclude_add = family_exclusions_for(derived_tg_label=label)
        cohort._stg_id = make_stg_id()
        cohort._cold_start_geos = list(spec.get("geos") or [])
        cohorts.append(cohort)
    return cohorts


def _size_and_persist(analysis_id: str, cohorts: list, geos: list[str], *, li_client, urn_res) -> int:
    """Per-channel audience + heuristic ICP for each cohort, persisted under
    ramp_id=analysis_id (mirrors the audience/ICP block in _resolve_cohorts)."""
    from src.prep_audience import measure_audience_for_cohort
    from src.icp_enrichment import enrich as enrich_icp
    from src.ui_decisions import (
        upsert_cohort_audience, upsert_cohort_targeting, upsert_cohort_icp,
    )

    enabled = [p.strip().lower() for p in (config.ENABLED_PLATFORMS or "").split(",") if p.strip()]
    if not enabled:
        enabled = ["linkedin", "meta", "google"]

    for cohort in cohorts:
        rows = measure_audience_for_cohort(
            cohort,
            included_geos=geos,
            enabled_platforms=enabled,
            li_audience_size=None,
            li_client=li_client,
            urn_resolver=urn_res,
        )
        cid = getattr(cohort, "_stg_id", "") or ""
        for ca in rows:
            upsert_cohort_audience(
                ramp_id=analysis_id, cohort_id=cid, cohort_signature=cohort.name,
                platform=ca.platform, audience_size=ca.audience_size, status=ca.status,
                geos_used=ca.geos_used, rules_dropped=ca.rules_dropped, forecast=ca.forecast,
            )
            upsert_cohort_targeting(
                ramp_id=analysis_id, cohort_id=cid, cohort_signature=cohort.name,
                platform=ca.platform, facets=ca.facets,
            )
        try:
            icp = enrich_icp(cohort, resume_sample=None, locale_hint=None)
            upsert_cohort_icp(
                ramp_id=analysis_id, cohort_id=cid, cohort_signature=cohort.name,
                icp_dict=icp.to_dict(),
            )
        except Exception as exc:  # noqa: BLE001
            log.warning("_size_and_persist: ICP enrich failed for %s: %s", cohort.name, exc)
        log.info("sizing %s: cohort %s → %s", analysis_id, cohort.name,
                 " ".join(f"{r.platform}={r.audience_size}({r.status})" for r in rows))
    return len(cohorts)


def compute_sizing_analysis(analysis_id: str) -> dict:
    """Run one sizing analysis (reads its input from the sizing_analyses row),
    persist per-channel sizing + ICP under ramp_id=analysis_id, flip status."""
    rec = ui_decisions.get_sizing_analysis(analysis_id)
    if not rec:
        raise ValueError(f"sizing_analysis {analysis_id!r} not found")

    input_type = (rec.get("input_type") or "").strip()
    geos = _parse_list(rec.get("geos") or "")
    platforms = _parse_list(rec.get("platforms") or "", sep_newline=False)
    # Per-run platform override — config.ENABLED_PLATFORMS is what the audience
    # block reads. This process runs one analysis then exits, so setting it here
    # is safe and process-local.
    if platforms:
        config.ENABLED_PLATFORMS = ",".join(platforms)

    log.info("sizing analysis %s: input_type=%s geos=%s platforms=%s",
             analysis_id, input_type, geos or "(broad)", platforms or "(default)")

    try:
        sheets, snowflake, li_client, urn_res, claude_key = _build_clients()
        if input_type == "project":
            result = _run_project(analysis_id, (rec.get("project_id") or "").strip(), geos,
                                   sheets, snowflake, li_client, urn_res, claude_key)
        elif input_type == "job_post":
            result = _run_job_post(analysis_id, rec.get("input_text") or "", geos,
                                   li_client, urn_res)
        elif input_type == "cb_ids":
            result = _run_cb_ids(analysis_id, _parse_list(rec.get("cb_ids") or ""), geos,
                                 snowflake, li_client, urn_res)
        elif input_type == "quality_sheet":
            result = _run_quality_sheet(analysis_id, (rec.get("input_text") or "").strip(), geos,
                                        sheets, snowflake, li_client, urn_res)
        else:
            raise ValueError(f"unknown input_type {input_type!r}")
        ui_decisions.set_sizing_analysis_status(analysis_id, "done")
        log.info("sizing analysis %s done: %s", analysis_id, result)
        return {"ok": True, "analysis_id": analysis_id, **result}
    except Exception as exc:  # noqa: BLE001 — record failure, never crash the worker
        log.exception("sizing analysis %s failed", analysis_id)
        ui_decisions.set_sizing_analysis_status(analysis_id, "failed", error=str(exc)[:500])
        return {"ok": False, "analysis_id": analysis_id, "error": str(exc)}


def _run_project(analysis_id, project_id, geos, sheets, snowflake, li_client, urn_res, claude_key) -> dict:
    """Project path — the exact ramp-ICP method. _resolve_cohorts mines the
    high-quality tier's common parameters (Stage A/B/C), enriches the ICP, and
    measures + persists per-channel audience, all keyed to analysis_id."""
    if not project_id:
        raise ValueError("project sizing requires a project_id")
    import main  # lazy: main.py is heavy

    row = {
        "ramp_id": analysis_id,
        "project_id": project_id,
        "included_geos": geos,
        "cohort_description": "",
        "ramp_summary": "",
    }
    resolved = main._resolve_cohorts(
        row, sheets=sheets, snowflake=snowflake, li_client=li_client, urn_res=urn_res,
        claude_key=claude_key, project_id=project_id, location="", dry_run=False,
    )
    return {"mode": "project", "cohorts": len(getattr(resolved, "selected", []) or [])}


def _run_job_post(analysis_id, jd_text, geos, li_client, urn_res) -> dict:
    """Job-post path — derive cohort specs + build cohorts from the JD, size each."""
    from src.icp_from_jobpost import derive_cohorts_from_job_post

    if not (jd_text or "").strip():
        raise ValueError("job_post sizing requires job-post text")
    specs = derive_cohorts_from_job_post(jd_text)
    if not specs:
        raise ValueError("could not derive any cohort from the job post")
    cohorts = build_cohorts_from_specs(specs)
    if not cohorts:
        raise ValueError("job-post cohorts had no targetable rules")
    n = _size_and_persist(analysis_id, cohorts, geos, li_client=li_client, urn_res=urn_res)
    return {"mode": "job_post", "cohorts": n}


def _mine_and_size(analysis_id, user_ids, geos, label, *, snowflake, li_client, urn_res) -> dict:
    """Fetch resume features for a set of qualified contributors, mine the common
    parameters (modal skills/titles/fields shared by ≥30% of them) into one
    cohort, and size it. Shared by the cb_ids and quality_sheet paths — with an
    explicit "these are the good CBs" set, the whole set IS the positive class."""
    ids = [u for u in dict.fromkeys(user_ids) if u]  # dedupe, preserve order
    if not ids:
        raise ValueError("no contributor ids to analyze")
    df = snowflake.fetch_signal_columns(ids)
    n = len(df)
    if n == 0:
        raise ValueError("no resume features found for the supplied contributor ids")

    skill_c: Counter = Counter()
    title_c: Counter = Counter()
    field_c: Counter = Counter()
    for _, r in df.iterrows():
        for s in re.split(r"[;,/|]", str(r.get("resume_job_skills") or "")):
            s = s.strip()
            if s:
                skill_c[s] += 1
        t = str(r.get("resume_job_title") or "").strip()
        if t:
            title_c[t] += 1
        f = str(r.get("resume_field") or "").strip()
        if f:
            field_c[f] += 1

    # "Common parameters" = the most-shared resume features. Require a feature to
    # be shared by ≥2 contributors (drop singletons) and take the top by
    # frequency — robust across diverse sets where no single feature hits a large
    # fraction.
    thresh = 2
    spec = {
        "label": label,
        "required_skills": [s for s, c in skill_c.most_common(8) if c >= thresh][:5],
        "job_titles": [t for t, c in title_c.most_common(5) if c >= thresh][:3],
        "fields_of_study": [f for f, c in field_c.most_common(5) if c >= thresh][:3],
        "degrees": [],
        "geos": geos,
    }
    log.info("sizing %s: n=%d spec=%s", analysis_id, n, spec)
    cohorts = build_cohorts_from_specs([spec])
    if not cohorts:
        raise ValueError(
            f"no resume feature is shared by ≥2 of {n} contributors — the set is "
            "too heterogeneous to derive a common ICP"
        )
    written = _size_and_persist(analysis_id, cohorts, geos, li_client=li_client, urn_res=urn_res)
    return {"cohorts": written, "n_contributors": n}


def _run_cb_ids(analysis_id, cb_ids, geos, snowflake, li_client, urn_res) -> dict:
    """Contributor-IDs path — mine the common parameters among the supplied
    qualified CBs, size the resulting cohort."""
    if not cb_ids:
        raise ValueError("cb_ids sizing requires at least one contributor id")
    return {"mode": "cb_ids", **_mine_and_size(
        analysis_id, cb_ids, geos, "Qualified contributors ICP",
        snowflake=snowflake, li_client=li_client, urn_res=urn_res,
    )}


_SHEET_ID_RE = re.compile(r"/spreadsheets/d/([A-Za-z0-9_-]+)")
_SHEET_GID_RE = re.compile(r"[#&?]gid=(\d+)")

# Quality columns M–S of the contributor quality sheet: higher = better for the
# POS metrics, higher = worse for the NEG metrics. total_tasks is a volume gate.
_QUALITY_POS = ["QMS", "PROMOTION_RATE", "WORKFORCES_MADE", "num_high_qual_tags"]
_QUALITY_NEG = ["QUALITY_DISABLE_RATE", "FAILURE_RATE"]
_QUALITY_MIN_TASKS = 20


def _num(v):
    try:
        return float(str(v).replace("%", "").replace(",", "").strip())
    except (TypeError, ValueError):
        return None


def _select_high_quality_ids(records: list[dict]) -> list[str]:
    """Pick the high-quality contributors from a quality sheet using the M–S
    metric columns (NOT the quality_tier label): gate on task volume, then take
    those with an above-median composite z-score. Falls back to quality_tier
    'Strong' if the metric columns are too sparse to score."""
    def uid(r):
        return str(r.get("user_id") or r.get("CBID") or "").strip()

    eligible = [r for r in records if uid(r) and (_num(r.get("total_tasks")) or 0) >= _QUALITY_MIN_TASKS]
    if len(eligible) < 4:
        return [uid(r) for r in records if uid(r) and str(r.get("quality_tier", "")).strip().lower() == "strong"]

    stats: dict = {}
    for col in _QUALITY_POS + _QUALITY_NEG:
        vals = [_num(r.get(col)) for r in eligible if _num(r.get(col)) is not None]
        if len(vals) >= 2:
            stats[col] = (statistics.mean(vals), statistics.pstdev(vals) or 1.0)

    def score(r):
        total, k = 0.0, 0
        for col in _QUALITY_POS:
            st, v = stats.get(col), _num(r.get(col))
            if st and v is not None:
                total += (v - st[0]) / st[1]; k += 1
        for col in _QUALITY_NEG:
            st, v = stats.get(col), _num(r.get(col))
            if st and v is not None:
                total -= (v - st[0]) / st[1]; k += 1
        return total / k if k else 0.0

    scored = sorted(((score(r), uid(r)) for r in eligible), reverse=True)
    cutoff = max(1, len(scored) // 2)  # top ~half (above-median composite)
    return [u for _, u in scored[:cutoff] if u]


def _run_quality_sheet(analysis_id, sheet_url, geos, sheets, snowflake, li_client, urn_res) -> dict:
    """Contributor-quality-sheet path — read a project lead's quality sheet
    (Google Sheet URL, shared with our service account), select the high-quality
    contributors via the M–S metric columns, and mine THEIR common resume
    parameters (subdomain-agnostic)."""
    m = _SHEET_ID_RE.search(sheet_url or "")
    if not m:
        raise ValueError("could not parse a Google Sheet id from the URL")
    sheet_id = m.group(1)
    gid_m = _SHEET_GID_RE.search(sheet_url or "")
    try:
        ss = sheets._gc.open_by_key(sheet_id)
        ws = ss.get_worksheet_by_id(int(gid_m.group(1))) if gid_m else ss.get_worksheet(0)
        records = ws.get_all_records()
    except Exception as exc:  # noqa: BLE001
        raise ValueError(
            f"could not read the sheet ({type(exc).__name__}) — is it shared with "
            "outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com?"
        ) from exc
    if not records:
        raise ValueError("the sheet has no data rows")

    ids = _select_high_quality_ids(records)
    log.info("sizing %s quality_sheet: %d rows → %d high-quality contributors",
             analysis_id, len(records), len(ids))
    if not ids:
        raise ValueError("no high-quality contributors found in the sheet")
    out = _mine_and_size(
        analysis_id, ids, geos, "High-quality contributors ICP",
        snowflake=snowflake, li_client=li_client, urn_res=urn_res,
    )
    return {"mode": "quality_sheet", "sheet_rows": len(records), "high_quality": len(ids), **out}

"""
Feature engineering — extracts contributor-level signals from raw Snowflake data.
All features are derived dynamically; no hard-coded taxonomies.
"""
import json
import logging
import re
from collections import Counter
from datetime import datetime
from typing import Any

import pandas as pd

import config

log = logging.getLogger(__name__)

CURRENT_YEAR = datetime.utcnow().year


def _safe_json(val: Any) -> Any:
    if val is None:
        return []
    if isinstance(val, (list, dict)):
        return val
    try:
        return json.loads(val)
    except Exception:
        return []


# ── Experience ────────────────────────────────────────────────────────────────

def _years_from_dates(start: Any, end: Any) -> float:
    try:
        s = int(start) if start else None
        e = int(end)   if end   else CURRENT_YEAR
        if s and e >= s:
            return max(0.0, float(e - s))
    except Exception:
        pass
    return 0.0


def _experience_band(years: float) -> str:
    if years <= 1:  return "0-1"
    if years <= 4:  return "2-4"
    if years <= 7:  return "5-7"
    if years <= 10: return "8-10"
    return "10+"


def extract_experience(job_experiences: Any) -> dict:
    jobs = _safe_json(job_experiences)
    total_years = 0.0
    role_count  = 0
    most_recent_end = 0

    for job in jobs:
        y = job.get("yearsOfExperience")
        if y:
            try:
                total_years += float(y)
            except Exception:
                pass
        else:
            start = job.get("startYear") or job.get("start", {}).get("year")
            end   = job.get("endYear")   or job.get("end",   {}).get("year")
            total_years += _years_from_dates(start, end)

        end_raw = job.get("endYear") or job.get("end", {}).get("year")
        try:
            most_recent_end = max(most_recent_end, int(end_raw))
        except Exception:
            pass
        role_count += 1

    worked_recently = (CURRENT_YEAR - most_recent_end) <= 2 if most_recent_end else False

    return {
        "total_years_experience": round(total_years, 1),
        "experience_band": _experience_band(total_years),
        "role_count": role_count,
        "worked_recently": worked_recently,
    }


# ── Skills ────────────────────────────────────────────────────────────────────

def extract_skills(job_experiences: Any) -> list[str]:
    jobs = _safe_json(job_experiences)
    skills = []
    for job in jobs:
        raw = job.get("skills") or []
        if isinstance(raw, str):
            raw = [s.strip() for s in raw.split(",") if s.strip()]
        for s in raw:
            if isinstance(s, str) and s.strip():
                skills.append(s.strip().lower())
            elif isinstance(s, dict):
                name = s.get("name") or s.get("skill") or ""
                if name:
                    skills.append(name.strip().lower())
    return list(set(skills))


# ── Job titles ────────────────────────────────────────────────────────────────

_TITLE_STOP = {"and", "of", "the", "a", "an", "at", "in", "for", "to", "with", "senior", "junior", "lead"}

def _normalize_title(t: str) -> str:
    return re.sub(r"[^a-z0-9 ]", " ", t.lower()).strip()


def extract_titles(job_titles_raw: Any) -> list[str]:
    if not job_titles_raw:
        return []
    parts = str(job_titles_raw).split(";")
    return [_normalize_title(p) for p in parts if _normalize_title(p)]


# ── Education ─────────────────────────────────────────────────────────────────

_DEGREE_RANK = {"phd": 4, "doctorate": 4, "masters": 3, "master": 3, "mba": 3,
                "bachelors": 2, "bachelor": 2, "undergraduate": 2, "other": 1}

def _degree_level(degree_str: str) -> str:
    s = degree_str.lower()
    for k in ("phd", "doctorate", "masters", "master", "mba", "bachelors", "bachelor", "undergraduate"):
        if k in s:
            return k.capitalize()
    return "Other"


def extract_education(educations: Any, fields_of_study_raw: Any) -> dict:
    edus = _safe_json(educations)
    highest = "Other"
    best_rank = 0
    school_countries = []

    for edu in edus:
        deg = edu.get("degree") or edu.get("degreeName") or ""
        level = _degree_level(deg)
        rank  = _DEGREE_RANK.get(level.lower(), 1)
        if rank > best_rank:
            best_rank = rank
            highest   = level

        country = edu.get("country") or (edu.get("location") or {}).get("country") or ""
        if country:
            school_countries.append(country.strip())

    fields = []
    if fields_of_study_raw:
        fields = [f.strip().lower() for f in str(fields_of_study_raw).split(";") if f.strip()]

    return {
        "highest_degree_level": highest,
        "fields_of_study": fields,
        "school_countries": school_countries,
    }


# ── Accreditations ────────────────────────────────────────────────────────────

def extract_accreditations(accreditations: Any) -> list[str]:
    accreds = _safe_json(accreditations)
    result = []
    for a in accreds:
        name = a.get("name") or a.get("title") or ""
        if name:
            result.append(name.strip().lower())
    return result


# ── Country derivation ────────────────────────────────────────────────────────

def derive_country(edu_info: dict, job_experiences: Any) -> str:
    # Prefer school country
    if edu_info.get("school_countries"):
        return edu_info["school_countries"][0]
    # Fallback: job company location
    jobs = _safe_json(job_experiences)
    for job in jobs:
        loc = job.get("companyLocation") or job.get("location") or ""
        if isinstance(loc, dict):
            loc = loc.get("country") or ""
        if loc:
            return str(loc).strip()
    return "UNKNOWN"


# ── Parse-once variants (avoid calling _safe_json multiple times per field) ───

def _extract_experience_from_parsed(jobs: list) -> dict:
    total_years = 0.0
    role_count  = 0
    most_recent_end = 0
    for job in jobs:
        y = job.get("yearsOfExperience")
        if y:
            try:
                total_years += float(y)
            except Exception:
                pass
        else:
            start = job.get("startYear") or job.get("start", {}).get("year")
            end   = job.get("endYear")   or job.get("end",   {}).get("year")
            total_years += _years_from_dates(start, end)
        end_raw = job.get("endYear") or job.get("end", {}).get("year")
        try:
            most_recent_end = max(most_recent_end, int(end_raw))
        except Exception:
            pass
        role_count += 1
    worked_recently = (CURRENT_YEAR - most_recent_end) <= 2 if most_recent_end else False
    return {
        "total_years_experience": round(total_years, 1),
        "experience_band":        _experience_band(total_years),
        "role_count":             role_count,
        "worked_recently":        worked_recently,
    }


def _extract_education_from_parsed(edus: list, fields_of_study_raw: Any) -> dict:
    highest   = "Other"
    best_rank = 0
    school_countries = []
    for edu in edus:
        deg   = edu.get("degree") or edu.get("degreeName") or ""
        level = _degree_level(deg)
        rank  = _DEGREE_RANK.get(level.lower(), 1)
        if rank > best_rank:
            best_rank = rank
            highest   = level
        country = edu.get("country") or (edu.get("location") or {}).get("country") or ""
        if country:
            school_countries.append(country.strip())
    fields = []
    if fields_of_study_raw:
        fields = [f.strip().lower() for f in str(fields_of_study_raw).split(";") if f.strip()]
    return {
        "highest_degree_level": highest,
        "fields_of_study":      fields,
        "school_countries":     school_countries,
    }


def _extract_accreditations_from_parsed(accreds: list) -> list[str]:
    result = []
    for a in accreds:
        name = a.get("name") or a.get("title") or ""
        if name:
            result.append(name.strip().lower())
    return result


def _derive_country_from_parsed(edu_info: dict, jobs: list) -> str:
    if edu_info.get("school_countries"):
        return edu_info["school_countries"][0]
    for job in jobs:
        loc = job.get("companyLocation") or job.get("location") or ""
        if isinstance(loc, dict):
            loc = loc.get("country") or ""
        if loc:
            return str(loc).strip()
    return "UNKNOWN"


# ── Full feature extraction ───────────────────────────────────────────────────

def _pipes_to_semicolons(series: pd.Series) -> pd.Series:
    """Map ' | '-joined resume-summary strings to '; '-joined form for engineer_features."""
    return series.fillna("").astype(str).str.replace("|", ";", regex=False)


def _parse_linkedin_certifications(raw: Any) -> str:
    """
    TNS_WORKER_LINKEDIN.LINKEDIN_CERTIFICATIONS is a JSON-array string, not a
    pipe-separated text field like the resume_* columns. Shapes observed in prod:

        None                                        → no LinkedIn record
        '[]'                                        → LinkedIn record, zero certs
        '[{"authority":"Coursera","name":"..."}]'   → real certifications

    Return a '; '-joined string of certificate `name` values, so the existing
    `_split` + frequency-map machinery can build meaningful dummies
    (`accreditations_norm__Google_Data_Analytics`, …). Returns "" for missing /
    empty / malformed input — the downstream `_split` already skips empties.
    """
    if raw is None:
        return ""
    try:
        if pd.isna(raw):
            return ""
    except (TypeError, ValueError):
        pass

    if isinstance(raw, (list, tuple)):
        items = raw
    else:
        s = str(raw).strip()
        if not s or s in ("[]", "{}", "null", "None"):
            return ""
        try:
            items = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            # Not JSON — treat as a plain pipe/semicolon-separated text field.
            return s.replace("|", ";")

    names: list[str] = []
    for item in items or []:
        if isinstance(item, dict):
            nm = item.get("name") or item.get("title") or item.get("authority") or ""
        else:
            nm = str(item)
        nm = nm.strip()
        if nm:
            names.append(nm)
    return "; ".join(names)


_STRUCTURAL_TOKENS = {"", "[]", "{}", "null", "none", "nan", "[{}]"}


def _highest_degree_from_pipes(raw: Any) -> str:
    """Pick the highest-ranking degree from a pipe-separated resume_degree string."""
    if raw is None or (isinstance(raw, float) and pd.isna(raw)):
        return "Other"
    parts = [p.strip() for p in str(raw).replace(";", "|").split("|") if p.strip()]
    best_rank, best_label = 0, "Other"
    for p in parts:
        level = _degree_level(p)
        rank = _DEGREE_RANK.get(level.lower(), 1)
        if rank > best_rank:
            best_rank, best_label = rank, level
    return best_label


def normalize_stage1_frame(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adapter: take the new STAGE1_SQL DataFrame (flat resume-summary + CESF tier bools)
    and emit the column shape that engineer_features() expects, so Stage A/B/C keep
    working unchanged.

    Writes (does not drop existing columns):
        skills_str              ← resume_job_skills (pipe → semicolon)
        job_titles              ← resume_job_title  (pipe → semicolon)
        fields_of_study         ← resume_field      (pipe → semicolon)
        highest_degree_level    ← derived from resume_degree (pipe list → best rank)
        accreditations_str      ← linkedin_certifications (pipe → semicolon, best effort)
        experience_band         ← "Other" (STAGE1_SQL has no computed band)
        total_years_experience  ← 0       (same — no signal available)
        role_count              ← 0
        country                 ← "UNKNOWN" (Stage B will treat as below_threshold)

    Leaves tier booleans (t3_activated, t2_course_pass, …) and exemplar fields
    (resume_job_title, resume_job_company, resume_degree, resume_field,
    linkedin_url, …) untouched.
    """
    out = df.copy()
    if "resume_job_skills" in out.columns and "skills_str" not in out.columns:
        out["skills_str"] = _pipes_to_semicolons(out["resume_job_skills"])
    if "resume_job_title" in out.columns and "job_titles" not in out.columns:
        out["job_titles"] = _pipes_to_semicolons(out["resume_job_title"])
    if "resume_field" in out.columns and "fields_of_study" not in out.columns:
        out["fields_of_study"] = _pipes_to_semicolons(out["resume_field"])
    if "linkedin_certifications" in out.columns and "accreditations_str" not in out.columns:
        # JSON-array field — parse and extract cert names rather than treating as pipe text.
        out["accreditations_str"] = out["linkedin_certifications"].apply(_parse_linkedin_certifications)
    if "resume_degree" in out.columns and "highest_degree_level" not in out.columns:
        out["highest_degree_level"] = out["resume_degree"].apply(_highest_degree_from_pipes)
    # STAGE1_SQL doesn't compute these — fill defaults so downstream code doesn't KeyError.
    if "experience_band" not in out.columns:
        out["experience_band"] = "Other"
    if "total_years_experience" not in out.columns:
        out["total_years_experience"] = 0
    if "role_count" not in out.columns:
        out["role_count"] = 0
    if "country" not in out.columns:
        out["country"] = "UNKNOWN"
    return out


def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build engineered feature columns from SQL pre-aggregated inputs.

    Accepts two DataFrame shapes:
      - Legacy RESUME_SQL output (skills_str, job_titles, experience_band, …).
      - New STAGE1_SQL output — will be auto-normalized via normalize_stage1_frame().

    Detection: if the new shape's hallmark column `resume_job_skills` is present
    but `skills_str` is missing, normalize first.
    """
    if "resume_job_skills" in df.columns and "skills_str" not in df.columns:
        df = normalize_stage1_frame(df)

    out = df.copy()

    # ── List columns from semicolon-separated SQL strings ────────────────────
    def _split(series: pd.Series) -> pd.Series:
        """Split '; '-separated strings into lists. Filters out structural/empty
        artifacts (e.g. the literal string "[]" left over from an unparsed JSON
        array) so they don't turn into bogus one-hot dummies downstream."""
        def _one(v):
            out: list[str] = []
            for s in str(v).split(";"):
                t = s.strip()
                if t and t.lower() not in _STRUCTURAL_TOKENS:
                    out.append(t)
            return out
        return series.fillna("").apply(_one)

    out["skills"]              = _split(df.get("skills_str",      pd.Series([""] * len(df), index=df.index)))
    out["job_titles_norm"]     = _split(df.get("job_titles",      pd.Series([""] * len(df), index=df.index)))
    out["fields_of_study"]     = _split(df.get("fields_of_study", pd.Series([""] * len(df), index=df.index)))
    out["accreditations_norm"] = _split(df.get("accreditations_str", pd.Series([""] * len(df), index=df.index)))

    # ── Scalar columns — already computed by SQL ─────────────────────────────
    out["experience_band"]       = df.get("experience_band",       "Other").fillna("Other")
    out["highest_degree_level"]  = df.get("highest_degree_level",  "Other").fillna("Other")
    out["total_years_experience"] = pd.to_numeric(df.get("total_years_experience", 0), errors="coerce").fillna(0)
    out["role_count"]            = pd.to_numeric(df.get("role_count", 0), errors="coerce").fillna(0).astype(int)
    out["country"]               = df.get("country", "UNKNOWN").fillna("UNKNOWN")

    return out


def build_frequency_maps(df: pd.DataFrame, min_freq: int) -> dict[str, Counter]:
    """
    Return frequency counters for list-valued columns.
    Only items appearing >= min_freq times are kept.
    Uses explode() instead of iterrows() — ~100x faster.
    """
    list_cols = ["skills", "job_titles_norm", "fields_of_study", "accreditations_norm"]
    result: dict[str, Counter] = {}

    for col in list_cols:
        if col not in df.columns:
            result[col] = Counter()
            continue
        series = df[col]
        # Guard against duplicate column names (shouldn't happen with new SQL-first approach)
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, -1]
        # explode turns each list element into its own row, then value_counts
        counts = (
            series
            .explode()
            .dropna()
            .loc[lambda s: s != ""]
            .value_counts()
        )
        result[col] = Counter({k: int(v) for k, v in counts.items() if v >= min_freq})

    return result


def binary_features(df: pd.DataFrame, freq_maps: dict[str, Counter]) -> pd.DataFrame:
    """
    Create one binary column per frequent feature value.
    Column names: skills__python, job_titles_norm__data_scientist, etc.

    Fully vectorized via explode + get_dummies + groupby max — no Python loops over rows.
    """
    new_frames: list[pd.DataFrame] = []

    for col, ctr in freq_maps.items():
        if not ctr or col not in df.columns:
            continue

        series = df[col]
        # Guard against duplicate column names
        if isinstance(series, pd.DataFrame):
            series = series.iloc[:, -1]

        freq_vals = set(ctr.keys())

        # explode list → one row per value, filter to frequent values only
        exploded = series.explode()
        exploded = exploded[exploded.isin(freq_vals)]

        if exploded.empty:
            continue

        # get_dummies on the exploded series → binary matrix with original index
        dummies = pd.get_dummies(exploded, dtype=int)

        # aggregate back to original row index (any occurrence = 1)
        dummies = dummies.groupby(level=0).max()

        # fill rows that had no matching values with 0
        dummies = dummies.reindex(df.index, fill_value=0)

        # prefix column names: skills__python, etc.
        dummies.columns = [f"{col}__{c.replace(' ', '_')}" for c in dummies.columns]
        new_frames.append(dummies)

    # Binary columns for categorical features (already vectorized)
    cat_frames: list[pd.DataFrame] = []
    for col in ("experience_band", "highest_degree_level"):
        if col not in df.columns:
            continue
        dummies = pd.get_dummies(df[col], prefix=col).astype(int)
        # Rename e.g. experience_band_10+ → experience_band__10plus
        dummies.columns = [
            c.replace(f"{col}_", f"{col}__", 1)
             .replace("+", "plus")
             .replace(" ", "_")
            for c in dummies.columns
        ]
        cat_frames.append(dummies)

    all_frames = new_frames + cat_frames
    if not all_frames:
        return df

    bin_df = pd.concat(all_frames, axis=1)
    return pd.concat([df, bin_df], axis=1)

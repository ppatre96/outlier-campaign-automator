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

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build engineered feature columns from SQL pre-aggregated inputs.

    All heavy extraction (skills, titles, fields_of_study, experience_band,
    degree_level, country, accreditations) is now computed by Snowflake via
    LATERAL FLATTEN CTEs — no Python JSON parsing needed.

    This function just cleans / type-converts the pre-computed columns.
    """
    out = df.copy()

    # ── List columns from semicolon-separated SQL strings ────────────────────
    def _split(series: pd.Series) -> pd.Series:
        """Split '; '-separated strings into lists, handle NaN."""
        return series.fillna("").apply(
            lambda v: [s.strip() for s in str(v).split(";") if s.strip()]
        )

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

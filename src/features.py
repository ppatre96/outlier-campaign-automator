"""
Feature engineering — extracts contributor-level signals from raw Snowflake data.
All features are derived dynamically; no hard-coded taxonomies.
"""
import json
import logging
import re
from collections import Counter, defaultdict
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


# ── Full feature extraction ───────────────────────────────────────────────────

def engineer_features(df: pd.DataFrame) -> pd.DataFrame:
    """
    Add engineered feature columns to the screening DataFrame.
    Returns the same DataFrame with new columns.
    """
    records = []
    for _, row in df.iterrows():
        exp_info  = extract_experience(row.get("job_experiences"))
        edu_info  = extract_education(row.get("educations"), row.get("fields_of_study"))
        skills    = extract_skills(row.get("job_experiences"))
        titles    = extract_titles(row.get("job_titles"))
        accredits = extract_accreditations(row.get("accreditations"))
        country   = derive_country(edu_info, row.get("job_experiences"))

        records.append({
            **exp_info,
            **edu_info,
            "skills":         skills,
            "job_titles_norm": titles,
            "accreditations_norm": accredits,
            "country":        country,
        })

    feat_df = pd.DataFrame(records, index=df.index)
    return pd.concat([df, feat_df], axis=1)


def build_frequency_maps(df: pd.DataFrame, min_freq: int) -> dict[str, Counter]:
    """
    Return frequency counters for skills, titles, fields_of_study, accreditations.
    Only items appearing >= min_freq times are kept.
    """
    counters: dict[str, Counter] = {
        "skills": Counter(),
        "job_titles_norm": Counter(),
        "fields_of_study": Counter(),
        "accreditations_norm": Counter(),
    }
    for _, row in df.iterrows():
        for col in counters:
            vals = row.get(col, [])
            if isinstance(vals, list):
                counters[col].update(vals)

    # Filter by min frequency
    return {
        col: Counter({k: v for k, v in ctr.items() if v >= min_freq})
        for col, ctr in counters.items()
    }


def binary_features(df: pd.DataFrame, freq_maps: dict[str, Counter]) -> pd.DataFrame:
    """
    Create one binary column per frequent feature value.
    Column names: skill__python, title__data_scientist, etc.
    """
    new_cols: dict[str, list] = {}

    for col, ctr in freq_maps.items():
        for val in ctr:
            col_name = f"{col}__{val.replace(' ', '_')}"
            new_cols[col_name] = [
                1 if isinstance(row.get(col), list) and val in row.get(col, []) else 0
                for _, row in df.iterrows()
            ]

    # Also binary columns for categorical features
    for col in ("experience_band", "highest_degree_level"):
        for val in df[col].unique():
            if pd.notna(val):
                col_name = f"{col}__{str(val).replace(' ', '_').replace('+', 'plus')}"
                new_cols[col_name] = (df[col] == val).astype(int).tolist()

    bin_df = pd.DataFrame(new_cols, index=df.index)
    return pd.concat([df, bin_df], axis=1)

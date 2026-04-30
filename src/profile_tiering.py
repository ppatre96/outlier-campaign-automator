"""
Profile prestige tiering — classify a CB's university + most-recent employer
as top-tier / country-specific-top / country-specific-lower / regular.

Purpose: for the ICP summary we want to show the distribution of prestige
signals across activators (e.g. "32% studied at an IIT/IIM/NIT" or "45% worked
at a product-engineering company, 18% at an IT-services firm"). This lets an
agency target LinkedIn audiences that match the prestige profile of our
converted CBs — important because two CBs with identical "Software Engineer"
titles on LinkedIn can have wildly different activation-probability based on
employer prestige within their country.

The lists are curated (not ML-learned). Extend as new countries / projects
surface under-represented credentials. Match logic is case-insensitive
substring + word-boundary.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

import pandas as pd

log = logging.getLogger(__name__)


# ── Top universities ──────────────────────────────────────────────────────────
# Global tier-1 schools (signal regardless of country). Substring matching —
# "IIT" matches "IIT Bombay", "IIT Delhi", etc.
# ISB, XLRI, NID, NLU are India-specific tier-1 grad schools.
_TOP_UNIVERSITIES_GLOBAL: list[str] = [
    # USA
    "MIT", "Massachusetts Institute of Technology",
    "Stanford", "Harvard", "Princeton", "Yale",
    "Carnegie Mellon", "CMU",
    "Caltech", "California Institute of Technology",
    "Berkeley", "UC Berkeley", "University of California, Berkeley",
    "UCLA", "University of California, Los Angeles",
    "Cornell", "Columbia University", "University of Chicago", "UChicago",
    "Wharton", "UPenn", "University of Pennsylvania",
    "Kellogg", "Northwestern University", "Duke", "Dartmouth",
    "Georgia Tech", "Georgia Institute of Technology",
    "University of Michigan",
    # UK
    "Oxford", "Cambridge", "Imperial College", "UCL", "University College London",
    "LSE", "London School of Economics", "Edinburgh",
    # Europe
    "ETH Zurich", "EPFL", "TU Munich", "Technical University of Munich",
    "RWTH Aachen", "INSEAD", "HEC Paris", "Bocconi",
    # Asia
    "Tsinghua", "Peking University", "Fudan", "Shanghai Jiao Tong",
    "NUS", "National University of Singapore", "NTU", "Nanyang Technological",
    "Hong Kong University", "HKU", "HKUST",
    "University of Tokyo", "Kyoto University", "Seoul National", "KAIST",
    # Canada / Aus
    "University of Toronto", "UofT", "McGill", "Waterloo", "University of Waterloo",
    "UBC", "University of British Columbia",
    "Melbourne", "University of Melbourne", "University of Sydney",
    "Australian National University", "ANU",
]

# India-specific tier-1 universities. Substring matching so e.g. "IIT" catches
# IIT Bombay / Madras / Delhi / Kanpur / Kharagpur / Guwahati / Roorkee / etc.
_TOP_UNIVERSITIES_INDIA: list[str] = [
    "IIT", "Indian Institute of Technology",
    "IIM", "Indian Institute of Management",
    "IIIT", "International Institute of Information Technology",
    "NIT",  "National Institute of Technology",
    "BITS Pilani", "Birla Institute of Technology and Science", "BITS, Pilani",
    "IISc", "Indian Institute of Science",
    "ISB", "Indian School of Business",
    "XLRI", "Xavier Labour Relations Institute",
    "AIIMS", "All India Institute of Medical Sciences",
    "NLU", "National Law University", "NLSIU",
    "NID", "National Institute of Design",
    "Ashoka University",
    "Jadavpur University",
    "BITS Goa", "BITS Hyderabad",
    "IIT-B", "IIT-D", "IIT-M", "IIT-K", "IIT-KGP",  # common abbreviations
]

_TOP_UNIVERSITIES_BY_COUNTRY: dict[str, list[str]] = {
    "india": _TOP_UNIVERSITIES_INDIA,
    # Add future country-specific lists here when we see projects that need them.
}


# ── Top companies ─────────────────────────────────────────────────────────────
# Global tier-1 employers — high compensation in every country they operate in.
# FAANG-plus; elite consulting / banking; well-paying unicorns.
_TOP_COMPANIES_GLOBAL: list[str] = [
    # FAANG+
    "Google", "Alphabet", "Meta", "Facebook", "Apple",
    "Microsoft", "Amazon", "Netflix",
    # AI labs / elite tech
    "OpenAI", "Anthropic", "DeepMind", "xAI",
    "NVIDIA", "Tesla", "SpaceX", "Stripe", "Databricks", "Snowflake",
    "Uber", "Airbnb", "LinkedIn", "Adobe", "Oracle", "Salesforce",
    "Palantir", "Instacart", "DoorDash",
    "Figma", "Notion", "Canva", "Atlassian",
    "Bloomberg", "Two Sigma", "Jane Street", "Citadel",
    # Consulting / banking
    "McKinsey", "Bain", "BCG",
    "Goldman Sachs", "Morgan Stanley", "JPMorgan", "JP Morgan",
    "Blackstone", "KKR", "Apollo Global", "Bridgewater",
    # Semiconductors
    "AMD", "Intel", "Qualcomm", "Broadcom",
]

# India-specific tiers. "Top" = well-paid product-engineering companies +
# global-tier multinational India offices. "Lower" = the IT-services majors
# (not bad companies — but pay ~half what product companies do for equivalent
# roles, and the CB activation signal correlates with product-eng work).
_TOP_COMPANIES_INDIA: list[str] = [
    # Indian unicorns / product cos
    "Flipkart", "Razorpay", "Zerodha", "Swiggy", "Zomato", "Paytm",
    "PhonePe", "CRED", "Meesho", "Ola", "Rapido",
    "Freshworks", "Postman", "BrowserStack", "Chargebee",
    "InMobi", "Zoho", "MakeMyTrip", "Cleartrip",
    "Nykaa", "PolicyBazaar", "Upstox", "Groww",
    "Urban Company", "Dream11", "Myntra",
    "Sharechat", "Mamaearth", "boAt",
    # India offices of global top (these also appear via GLOBAL list but include here for clarity)
    "Google India", "Microsoft India", "Amazon India", "Meta India",
    "Uber India", "Stripe India", "Netflix India",
    "Goldman Sachs India", "Morgan Stanley India", "JP Morgan India", "Deutsche Bank India",
    # Banking + consulting
    "HDFC Bank", "Kotak Mahindra",
]

_LOWER_COMPANIES_INDIA: list[str] = [
    # Indian IT services majors — large employers, lower-paying per role
    # (relative to product-eng in India).
    "TCS", "Tata Consultancy Services",
    "Infosys", "Wipro", "HCL", "HCLTech", "HCL Technologies",
    "Tech Mahindra", "Cognizant", "Capgemini",
    "Accenture",  # technically global; included here because Accenture India is IT-services
    "IBM India",  # same reasoning
    "Deloitte India", "EY India", "KPMG India", "PwC India",
    "Mindtree", "LTI", "Larsen & Toubro Infotech", "L&T Infotech",
    "Mphasis", "Hexaware", "Persistent Systems", "Tata Elxsi",
    "Genpact", "Virtusa", "Coforge", "NIIT",
]

_COMPANIES_BY_COUNTRY: dict[str, dict[str, list[str]]] = {
    "india": {"top": _TOP_COMPANIES_INDIA, "lower": _LOWER_COMPANIES_INDIA},
    # Add future country-specific lists here (e.g. US IT-services firms).
}


# ── Matcher ──────────────────────────────────────────────────────────────────
def _normalize(s: str) -> str:
    return re.sub(r"\s+", " ", (s or "").lower().strip())


def _word_match(needle: str, haystack: str) -> bool:
    """Word-boundary substring match (like `\\b<needle>s?\\b`), prevents false
    positives like 'IIM' matching 'dimension'."""
    if not needle or not haystack:
        return False
    return re.search(rf"\b{re.escape(needle.lower())}s?\b", haystack) is not None


def classify_school(school_name: str, country: str | None = None) -> str:
    """Return one of: 'top', 'regular', 'unknown'.
    Matches against global + country-specific top-tier lists. `country` is a
    free-form hint (lowercased; "india" is the current main non-global list)."""
    if not school_name:
        return "unknown"
    norm = _normalize(school_name)
    for needle in _TOP_UNIVERSITIES_GLOBAL:
        if _word_match(needle, norm):
            return "top"
    if country:
        for needle in _TOP_UNIVERSITIES_BY_COUNTRY.get(country.lower(), []):
            if _word_match(needle, norm):
                return "top"
    # If no country hint passed, try all country lists — safer than missing.
    else:
        for country_list in _TOP_UNIVERSITIES_BY_COUNTRY.values():
            for needle in country_list:
                if _word_match(needle, norm):
                    return "top"
    return "regular"


def classify_company(company_name: str, country: str | None = None) -> str:
    """Return one of: 'top_global', 'top_country', 'lower_country', 'regular', 'unknown'.

    Global matches always win over country matches. Country matches use the
    `country` hint when provided; otherwise try all country lists.
    """
    if not company_name:
        return "unknown"
    norm = _normalize(company_name)

    for needle in _TOP_COMPANIES_GLOBAL:
        if _word_match(needle, norm):
            return "top_global"

    countries = [country.lower()] if country else list(_COMPANIES_BY_COUNTRY.keys())
    for ctry in countries:
        lists = _COMPANIES_BY_COUNTRY.get(ctry, {})
        for needle in lists.get("top", []):
            if _word_match(needle, norm):
                return "top_country"
        for needle in lists.get("lower", []):
            if _word_match(needle, norm):
                return "lower_country"

    return "regular"


# ── Row-level helpers ────────────────────────────────────────────────────────
def extract_schools_from_linkedin_education(linkedin_education: Any) -> list[str]:
    """Parse the linkedin_education JSON-array field and return school names."""
    if linkedin_education is None:
        return []
    try:
        if pd.isna(linkedin_education):
            return []
    except (TypeError, ValueError):
        pass
    if isinstance(linkedin_education, (list, tuple)):
        items = linkedin_education
    else:
        s = str(linkedin_education).strip()
        if not s or s in ("[]", "{}", "null", "None"):
            return []
        try:
            items = json.loads(s)
        except (json.JSONDecodeError, ValueError):
            return []
    schools: list[str] = []
    for item in items or []:
        if isinstance(item, dict):
            school = item.get("school") or ""
            if school:
                schools.append(str(school).strip())
    return schools


def extract_companies_from_resume(resume_job_company: Any) -> list[str]:
    """Split the pipe-joined resume_job_company field into individual company names."""
    if resume_job_company is None:
        return []
    try:
        if pd.isna(resume_job_company):
            return []
    except (TypeError, ValueError):
        pass
    raw = str(resume_job_company)
    parts = [p.strip() for p in raw.replace(";", "|").split("|")]
    return [p for p in parts if p]


def compute_requirement_commonality(
    df: pd.DataFrame,
    requirements: list[str],
) -> list[dict]:
    """
    For each `requirement` term (e.g., "cardiology", "MD"), compute what
    fraction of contributors in `df` have that term anywhere in their
    resume signal columns (case-insensitive substring match across
    resume_job_title + resume_field + resume_job_skills + resume_job_company
    + linkedin_education).

    `df` is expected to have those 5 columns (use RedashClient.fetch_signal_columns
    on the chosen tier's positives).

    Returns a list of dicts, one per requirement, with shape:
      {
        "requirement":        str,
        "n_total":            int,    # rows in df
        "n_hits":             int,    # rows where the term appears in any signal column
        "hit_rate":           float,  # n_hits / n_total
        "recommended_action": "hard_filter" | "soft_hint" | "drop",
      }

    Action thresholds (calibrated 2026-04-29):
      - hit_rate >= 0.50 → "hard_filter": dominant signal in the activator pool;
        promote to a Stage A facet anchor for tighter LinkedIn targeting
      - 0.10 <= hit_rate < 0.50 → "soft_hint": substantial but not universal;
        pass to copy brief only (the LLM emphasizes it but Stage A doesn't filter on it)
      - hit_rate < 0.10 → "drop": rare; the brief mentioned it but activators don't
        share it. Probably noise from the LLM extraction. Keep out of targeting + copy.

    Empty `df` or `requirements` → returns [].
    """
    n_total = len(df)
    if n_total == 0 or not requirements:
        return []

    # Precompute lowercase concatenation per row across all signal columns we look at.
    SIG_COLS = [
        "resume_job_title", "resume_field", "resume_job_skills",
        "resume_job_company", "linkedin_education",
    ]
    blob = pd.Series([""] * n_total, index=df.index)
    for col in SIG_COLS:
        if col in df.columns:
            blob = blob + " " + df[col].fillna("").astype(str)
    blob = blob.str.lower()

    results: list[dict] = []
    for req_raw in requirements:
        req = (req_raw or "").strip()
        if not req or len(req) < 3:
            continue  # too short to be a meaningful substring
        # Plain substring match — naive but matches the analyst's ILIKE pattern.
        # Word-boundary regex would be stricter but misses "cardiologist" vs
        # "cardiology" without stemming.
        n_hits = int(blob.str.contains(re.escape(req.lower()), regex=True, na=False).sum())
        hit_rate = n_hits / n_total
        if hit_rate >= 0.50:
            action = "hard_filter"
        elif hit_rate >= 0.10:
            action = "soft_hint"
        else:
            action = "drop"
        results.append({
            "requirement": req,
            "n_total": n_total,
            "n_hits": n_hits,
            "hit_rate": round(hit_rate, 3),
            "recommended_action": action,
        })
    return results


def compute_prestige_signal(
    df: pd.DataFrame,
    country_hint: str | None = None,
    threshold: float = 0.50,
) -> dict[str, Any]:
    """
    Aggregate prestige signals across a DataFrame of contributors. Each row
    must have `linkedin_education` and/or `resume_job_company` columns
    (missing rows count as no_data, not as 'regular' — we don't want sparse
    fetches to dilute the signal).

    Returns a dict:
      {
        "n_total":              int,    # rows in df
        "n_with_data":          int,    # rows with at least one prestige column populated
        "top_school_pct":       float,  # 0..1, of n_with_data
        "top_global_company_pct": float,
        "top_country_company_pct": float,
        "top_tier_pct":         float,  # union of top_school OR top_*_company
        "applies":              bool,   # top_tier_pct >= threshold AND n_with_data >= MIN_FOR_SIGNAL
        "summary":              str,    # human-readable
      }

    `applies=True` is the "fold prestige into targeting" signal Pranav asked
    for. Use it as a gate: when True, downstream may bias targeting/copy
    toward prestige cues; when False, ignore.
    """
    MIN_FOR_SIGNAL = 10  # below this, the % is too noisy to act on
    n_total = len(df)
    if n_total == 0:
        return {
            "n_total": 0, "n_with_data": 0,
            "top_school_pct": 0.0, "top_global_company_pct": 0.0,
            "top_country_company_pct": 0.0, "top_tier_pct": 0.0,
            "applies": False, "summary": "no positives",
        }

    n_school = n_top_global = n_top_country = n_top_tier = 0
    n_with_data = 0
    for _, row in df.iterrows():
        labels = row_tier_labels(row, country_hint=country_hint)
        has_data = (
            bool(row.get("linkedin_education"))
            or bool(row.get("resume_job_company"))
        )
        if not has_data:
            continue
        n_with_data += 1
        is_top_school = labels["any_top_school"]
        is_top_global = bool(labels["top_global_company"])
        is_top_country = bool(labels["top_country_company"])
        if is_top_school:
            n_school += 1
        if is_top_global:
            n_top_global += 1
        if is_top_country:
            n_top_country += 1
        if is_top_school or is_top_global or is_top_country:
            n_top_tier += 1

    if n_with_data == 0:
        return {
            "n_total": n_total, "n_with_data": 0,
            "top_school_pct": 0.0, "top_global_company_pct": 0.0,
            "top_country_company_pct": 0.0, "top_tier_pct": 0.0,
            "applies": False,
            "summary": f"{n_total} positives but 0 had prestige data — signal unavailable",
        }

    top_school_pct = n_school / n_with_data
    top_global_pct = n_top_global / n_with_data
    top_country_pct = n_top_country / n_with_data
    top_tier_pct = n_top_tier / n_with_data
    applies = top_tier_pct >= threshold and n_with_data >= MIN_FOR_SIGNAL

    return {
        "n_total": n_total,
        "n_with_data": n_with_data,
        "top_school_pct": round(top_school_pct, 3),
        "top_global_company_pct": round(top_global_pct, 3),
        "top_country_company_pct": round(top_country_pct, 3),
        "top_tier_pct": round(top_tier_pct, 3),
        "applies": applies,
        "summary": (
            f"{int(top_tier_pct*100)}% of {n_with_data} positives are top-tier "
            f"(school={int(top_school_pct*100)}%, global-co={int(top_global_pct*100)}%, "
            f"country-co={int(top_country_pct*100)}%) — "
            f"{'APPLIES' if applies else 'below threshold, ignore'}"
        ),
    }


def row_tier_labels(row: dict | pd.Series, country_hint: str | None = None) -> dict[str, Any]:
    """
    For a single activator row, return a compact dict of prestige signals:
        {
          "top_schools":          [str, ...],   # schools classified as top
          "any_top_school":       bool,
          "top_global_company":   str | None,
          "top_country_company":  str | None,
          "lower_country_company":str | None,
          "most_recent_company":  str | None,
          "company_class":        one of "top_global" / "top_country" / "lower_country" / "regular" / "unknown",
        }

    `country_hint` is optional; pass "india" to bias country-specific matchers.
    """
    schools = extract_schools_from_linkedin_education(row.get("linkedin_education"))
    top_schools = [s for s in schools if classify_school(s, country_hint) == "top"]

    companies = extract_companies_from_resume(row.get("resume_job_company"))
    # Resume typically lists current/recent job first, so companies[0] is usually
    # the most recent. If the CB is currently at Outlier AI (common), look at the next one.
    most_recent = None
    for c in companies:
        if c and c.lower() not in ("outlier", "outlier ai", "outlierai", "outlier.ai"):
            most_recent = c
            break
    if most_recent is None and companies:
        most_recent = companies[0]

    classification = classify_company(most_recent or "", country_hint)
    top_global = most_recent if classification == "top_global" else None
    top_country = most_recent if classification == "top_country" else None
    lower_country = most_recent if classification == "lower_country" else None

    return {
        "top_schools": top_schools,
        "any_top_school": len(top_schools) > 0,
        "top_global_company": top_global,
        "top_country_company": top_country,
        "lower_country_company": lower_country,
        "most_recent_company": most_recent,
        "company_class": classification,
    }

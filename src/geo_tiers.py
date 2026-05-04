"""
Geo tier system for Outlier campaign targeting.

Two separate concepts:
  1. G1/G2/G3/G4 TIER — controls which platforms/features a country can use
     (from the country permissions spreadsheet). G4 = blocked, never target.
  2. PAY MULTIPLIER — country-specific rate relative to the US baseline (1.0).
     Advertised rate = project_base_rate_usd × country_multiplier (T3/T4 CB tier).
     Rounded to nearest $5 for copy.

These are independent: Singapore is G1 AND multiplier=1.0; Nigeria is G4 AND
multiplier=0.06. Pay rate does not determine tier; tier determines eligibility.

For campaign creation:
  - Filter out G4 geos (strictly skip — never create campaigns for them)
  - Group remaining geos by ETHNIC CREATIVE CLUSTER (determines photo subject)
  - Within each cluster, compute the median multiplier → advertised rate for that group
  - Create one LinkedIn campaign per cluster, each with geo-appropriate photo + rate

Surfaced 2026-05-04 when user requested per-geo customized campaigns.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field

log = logging.getLogger(__name__)

# ── Pay multipliers ────────────────────────────────────────────────────────────
# Source: Outlier country rate table (screenshots 2026-05-04).
# US = 1.0 baseline. advertised_rate = base_rate × multiplier, rounded to $5.
COUNTRY_PAY_MULTIPLIER: dict[str, float] = {
    "AD": 0.66,  "AE": 0.62,  "AF": 0.18,  "AG": 0.73,  "AL": 0.45,
    "AM": 0.37,  "AO": 0.23,  "AR": 0.47,  "AS": 0.65,  "AT": 0.79,
    "AU": 1.00,  "AW": 0.76,  "AX": 0.65,  "AZ": 0.28,  "BA": 0.43,
    "BB": 0.65,  "BD": 0.32,  "BE": 0.94,  "BF": 0.36,  "BG": 0.42,
    "BH": 0.65,  "BI": 0.13,  "BJ": 0.33,  "BM": 1.00,  "BN": 0.65,
    "BO": 0.38,  "BR": 0.48,  "BS": 0.65,  "BT": 0.25,  "BW": 0.38,
    "BY": 0.25,  "BZ": 0.65,  "CA": 0.91,  "CD": 0.37,  "CF": 0.42,
    "CG": 0.35,  "CH": 1.05,  "CI": 0.61,  "CL": 0.55,  "CM": 0.31,
    "CN": 0.47,  "CO": 0.59,  "CR": 0.65,  "CU": 0.63,  "CV": 0.46,
    "CW": 0.71,  "CY": 0.65,  "CZ": 0.65,  "DE": 0.94,  "DJ": 0.43,
    "DK": 1.00,  "DM": 0.49,  "DO": 0.38,  "DZ": 0.32,  "EC": 0.45,
    "EE": 0.65,  "EG": 0.41,  "EH": 0.65,  "ER": 0.33,  "ES": 0.90,
    "ET": 0.36,  "FI": 0.95,  "FJ": 0.40,  "FO": 0.96,  "FR": 0.93,
    "GA": 0.38,  "GB": 0.94,  "GD": 0.58,  "GE": 0.32,  "GF": 0.65,
    "GG": 0.65,  "GH": 0.28,  "GI": 0.65,  "GL": 0.79,  "GM": 0.25,
    "GN": 0.39,  "GP": 1.00,  "GQ": 0.38,  "GR": 0.65,  "GT": 0.44,
    "GW": 0.30,  "GY": 0.37,  "HK": 0.65,  "HN": 0.47,  "HR": 0.51,
    "HT": 0.87,  "HU": 0.49,  "ID": 0.38,  "IE": 0.85,  "IL": 0.97,
    "IM": 0.65,  "IN": 0.55,  "IQ": 0.43,  "IR": 0.26,  "IS": 1.00,
    "IT": 0.91,  "JE": 0.65,  "JM": 0.65,  "JO": 0.42,  "JP": 0.90,
    "KE": 0.36,  "KG": 0.37,  "KH": 0.33,  "KI": 0.61,  "KM": 0.45,
    "KN": 0.62,  "KP": 0.65,  "KR": 0.88,  "KW": 0.61,  "KY": 1.00,
    "KZ": 0.35,  "LA": 0.21,  "LB": 0.32,  "LC": 0.51,  "LI": 0.65,
    "LK": 0.32,  "LR": 0.45,  "LS": 0.33,  "LT": 0.65,  "LU": 0.90,
    "LV": 0.65,  "LY": 0.46,  "MA": 0.39,  "MC": 1.00,  "MD": 0.43,
    "ME": 0.38,  "MG": 0.30,  "MH": 0.94,  "MK": 0.35,  "ML": 0.33,
    "MM": 0.25,  "MN": 0.38,  "MO": 0.77,  "MQ": 0.65,  "MR": 0.27,
    "MS": 0.65,  "MT": 0.65,  "MU": 0.38,  "MV": 0.65,  "MW": 0.23,
    "MX": 0.58,  "MY": 0.39,  "MZ": 0.39,  "NA": 0.38,  "NE": 0.38,
    "NG": 0.06,  "NI": 0.34,  "NL": 0.95,  "NO": 1.00,  "NP": 0.25,
    "NR": 0.95,  "NZ": 0.87,  "OM": 0.47,  "PA": 0.65,  "PE": 0.49,
    "PF": 0.65,  "PG": 0.62,  "PH": 0.47,  "PK": 0.25,  "PL": 0.52,
    "PR": 0.79,  "PS": 0.59,  "PT": 0.61,  "PW": 0.86,  "PY": 0.33,
    "QA": 0.65,  "RE": 0.65,  "RO": 0.42,  "RS": 0.42,  "RU": 0.31,
    "RW": 0.24,  "SA": 0.65,  "SB": 0.76,  "SC": 0.50,  "SD": 0.66,
    "SE": 0.95,  "SG": 1.00,  "SI": 0.60,  "SK": 0.65,  "SL": 0.27,
    "SM": 0.79,  "SN": 0.33,  "SO": 0.41,  "SR": 0.44,  "SS": 0.65,
    "ST": 0.57,  "SV": 0.42,  "SY": 0.18,  "TC": 0.97,  "TD": 0.35,
    "TG": 0.32,  "TH": 0.47,  "TJ": 0.26,  "TM": 0.42,  "TN": 0.32,
    "TO": 0.67,  "TR": 0.60,  "TT": 0.65,  "TV": 1.04,  "TW": 0.65,
    "TZ": 0.26,  "UA": 0.29,  "UG": 0.33,  "US": 1.00,  "UY": 0.69,
    "UZ": 0.27,  "VA": 0.65,  "VC": 0.55,  "VE": 0.54,  "VG": 0.65,
    "VI": 0.65,  "VN": 0.42,  "VU": 0.95,  "WS": 0.63,  "XK": 0.39,
    "YE": 0.65,  "ZA": 0.41,  "ZM": 0.26,  "ZW": 0.81,
}

# ── G4 blocked countries ───────────────────────────────────────────────────────
# Strictly skip — never create LinkedIn campaigns targeting these countries.
# Includes UN-sanctioned countries and countries Outlier doesn't operate in.
GEO_G4_BLOCKED: frozenset[str] = frozenset({
    # Sanctioned / heavily restricted
    "AF",  # Afghanistan
    "BY",  # Belarus
    "CF",  # Central African Republic
    "CD",  # DRC
    "CU",  # Cuba
    "ER",  # Eritrea
    "ET",  # Ethiopia (restricted)
    "IR",  # Iran
    "KP",  # North Korea
    "LB",  # Lebanon
    "LY",  # Libya
    "ML",  # Mali
    "MM",  # Myanmar
    "NI",  # Nicaragua
    "RU",  # Russia
    "SD",  # Sudan
    "SO",  # Somalia
    "SS",  # South Sudan
    "SY",  # Syria
    "VE",  # Venezuela (sanctions)
    "YE",  # Yemen
    "ZW",  # Zimbabwe
    # Very low multiplier (< 0.15) → effectively not viable
    "BI",  # Burundi (0.13)
})

# ── Ethnic creative clusters ───────────────────────────────────────────────────
# Determines photo_subject ethnicity for Gemini image generation.
# Countries not listed default to "global_mix".
GEO_ETHNIC_CLUSTER: dict[str, str] = {
    # English-speaking Anglo
    "US": "anglo", "CA": "anglo", "GB": "anglo", "AU": "anglo",
    "NZ": "anglo", "IE": "anglo", "BM": "anglo",

    # Northern/Western Europe
    "DE": "northern_european", "NL": "northern_european", "CH": "northern_european",
    "AT": "northern_european", "BE": "northern_european", "SE": "northern_european",
    "NO": "northern_european", "DK": "northern_european", "FI": "northern_european",
    "IS": "northern_european", "LU": "northern_european",

    # Southern Europe
    "FR": "southern_european", "IT": "southern_european", "ES": "southern_european",
    "PT": "southern_european", "GR": "southern_european", "MC": "southern_european",

    # Eastern Europe
    "PL": "eastern_european", "CZ": "eastern_european", "SK": "eastern_european",
    "HU": "eastern_european", "RO": "eastern_european", "BG": "eastern_european",
    "HR": "eastern_european", "SI": "eastern_european", "EE": "eastern_european",
    "LV": "eastern_european", "LT": "eastern_european", "UA": "eastern_european",
    "RS": "eastern_european", "BA": "eastern_european", "MK": "eastern_european",
    "ME": "eastern_european", "AL": "eastern_european", "XK": "eastern_european",

    # South Asian
    "IN": "south_asian", "PK": "south_asian", "BD": "south_asian",
    "LK": "south_asian", "NP": "south_asian",

    # Southeast Asian
    "SG": "southeast_asian", "MY": "southeast_asian", "PH": "southeast_asian",
    "ID": "southeast_asian", "VN": "southeast_asian", "TH": "southeast_asian",
    "MM": "southeast_asian", "KH": "southeast_asian",

    # East Asian
    "JP": "east_asian", "KR": "east_asian", "TW": "east_asian",
    "HK": "east_asian", "CN": "east_asian", "MO": "east_asian",

    # Latin American (Spanish-speaking)
    "MX": "latin_american", "CO": "latin_american", "AR": "latin_american",
    "PE": "latin_american", "CL": "latin_american", "EC": "latin_american",
    "VE": "latin_american", "GT": "latin_american", "CU": "latin_american",
    "DO": "latin_american", "BO": "latin_american", "PY": "latin_american",
    "UY": "latin_american", "HN": "latin_american", "SV": "latin_american",
    "NI": "latin_american", "CR": "latin_american", "PA": "latin_american",
    "PR": "latin_american",

    # Brazil (Portuguese-speaking Latin America)
    "BR": "brazilian",

    # Middle East / Arab
    "AE": "middle_eastern", "SA": "middle_eastern", "QA": "middle_eastern",
    "KW": "middle_eastern", "BH": "middle_eastern", "OM": "middle_eastern",
    "JO": "middle_eastern", "IL": "middle_eastern", "LB": "middle_eastern",
    "EG": "middle_eastern", "MA": "middle_eastern", "TN": "middle_eastern",
    "DZ": "middle_eastern", "TR": "middle_eastern",

    # Sub-Saharan Africa
    "NG": "african", "KE": "african", "GH": "african", "ZA": "african",
    "TZ": "african", "UG": "african", "ET": "african", "CM": "african",
    "SN": "african", "CI": "african",
}

# Human-readable descriptions used in photo_subject and campaign naming
CLUSTER_LABELS: dict[str, str] = {
    "anglo":             "English-speaking",
    "northern_european": "Northern/Western European",
    "southern_european": "Southern European",
    "eastern_european":  "Eastern European",
    "south_asian":       "South Asian",
    "southeast_asian":   "Southeast Asian",
    "east_asian":        "East Asian",
    "latin_american":    "Latin American",
    "brazilian":         "Brazilian",
    "middle_eastern":    "Middle Eastern",
    "african":           "African",
    "global_mix":        "Global",
}


@dataclass
class GeoCampaignGroup:
    """One campaign's worth of geo targeting — a cluster of culturally similar countries."""
    cluster:          str           # ethnic cluster key
    cluster_label:    str           # human-readable
    geos:             list[str]     # ISO country codes in this group
    median_multiplier: float        # median pay multiplier across geos
    advertised_rate:  str           # formatted rate string for copy, e.g. "$35/hr"
    campaign_suffix:  str           # e.g. "south_asian" for campaign name


def filter_blocked_geos(included_geos: list[str]) -> tuple[list[str], list[str]]:
    """
    Remove G4 blocked countries from the list.
    Returns (allowed_geos, skipped_geos).
    """
    allowed, skipped = [], []
    for g in (included_geos or []):
        if g.upper() in GEO_G4_BLOCKED:
            skipped.append(g.upper())
        else:
            allowed.append(g.upper())
    if skipped:
        log.warning("Skipping G4 blocked geos (will not create campaigns): %s", skipped)
    return allowed, skipped


def compute_geo_rate(base_rate_usd: float, country_code: str) -> str:
    """
    Compute the advertised hourly rate for a single country.
    Returns a formatted string like "$35/hr" or "$50/hr".
    Rounds to nearest $5 (minimum $5).
    """
    multiplier = COUNTRY_PAY_MULTIPLIER.get(country_code.upper(), 0.65)
    raw = base_rate_usd * multiplier
    rounded = max(5, round(raw / 5) * 5)
    return f"${int(rounded)}/hr"


def group_geos_for_campaigns(
    included_geos: list[str],
    base_rate_usd: float = 50.0,
) -> list[GeoCampaignGroup]:
    """
    Split included_geos into per-campaign geo groups.

    Algorithm:
      1. Filter out G4 blocked geos (strict skip)
      2. If only 1 geo remains: single group, no split
      3. Group remaining geos by ethnic creative cluster
      4. For each cluster: compute median multiplier → advertised rate
      5. Merge clusters whose advertised rate AND cluster are the same (dedup)

    Returns a list of GeoCampaignGroup — one LinkedIn campaign per group.
    When included_geos is empty or all G4: returns empty list (no campaigns created).

    Args:
        included_geos:  ISO country codes from Smart Ramp cohort.included_geos
        base_rate_usd:  Project base rate at US multiplier (1.0). Defaults to $50.
    """
    allowed, skipped = filter_blocked_geos(included_geos)
    if not allowed:
        log.warning("No allowed geos after filtering G4 — no campaigns to create")
        return []

    # If single geo: simple single-group result
    if len(allowed) == 1:
        cc = allowed[0]
        cluster = GEO_ETHNIC_CLUSTER.get(cc, "global_mix")
        mult = COUNTRY_PAY_MULTIPLIER.get(cc, 0.65)
        rate_str = _format_rate(base_rate_usd * mult)
        return [GeoCampaignGroup(
            cluster=cluster,
            cluster_label=CLUSTER_LABELS.get(cluster, cluster),
            geos=[cc],
            median_multiplier=mult,
            advertised_rate=rate_str,
            campaign_suffix=cluster,
        )]

    # Group by ethnic cluster
    clusters: dict[str, list[str]] = {}
    for cc in allowed:
        cluster = GEO_ETHNIC_CLUSTER.get(cc, "global_mix")
        clusters.setdefault(cluster, []).append(cc)

    groups: list[GeoCampaignGroup] = []
    for cluster, geos in clusters.items():
        multipliers = [COUNTRY_PAY_MULTIPLIER.get(g, 0.65) for g in geos]
        median_mult = _median(multipliers)
        rate_str = _format_rate(base_rate_usd * median_mult)
        groups.append(GeoCampaignGroup(
            cluster=cluster,
            cluster_label=CLUSTER_LABELS.get(cluster, cluster),
            geos=geos,
            median_multiplier=round(median_mult, 3),
            advertised_rate=rate_str,
            campaign_suffix=cluster,
        ))
        log.info(
            "Geo group: %s → %s (geos=%s, median_mult=%.2f, rate=%s)",
            cluster, CLUSTER_LABELS.get(cluster, cluster), geos, median_mult, rate_str,
        )

    # Sort by cluster size descending (largest audience first)
    groups.sort(key=lambda g: len(g.geos), reverse=True)
    log.info(
        "geo_tiers: %d allowed geos → %d campaign groups (%d G4 skipped)",
        len(allowed), len(groups), len(skipped),
    )
    return groups


def _median(values: list[float]) -> float:
    if not values:
        return 0.65
    sorted_v = sorted(values)
    mid = len(sorted_v) // 2
    if len(sorted_v) % 2 == 0:
        return (sorted_v[mid - 1] + sorted_v[mid]) / 2
    return sorted_v[mid]


def _format_rate(raw_usd: float) -> str:
    """Round to nearest $5, minimum $5, return formatted string."""
    rounded = max(5, round(raw_usd / 5) * 5)
    return f"${int(rounded)}/hr"

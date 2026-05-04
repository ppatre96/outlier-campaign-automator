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


# ── Per-cluster ICP profiles ──────────────────────────────────────────────────
# These inform BOTH the copy LLM (via geo_icp_prompt_hint) and the Gemini image
# prompt (via photo_setting_hint). Every field is optional context — the LLMs
# should weave the relevant signals in naturally, not recite them verbatim.
#
# Structure per cluster:
#   primary_hook:       The #1 psychological lever in this market (for subject line + opener)
#   credential_signals: Prestige markers that resonate here (weave into "people like you" references)
#   copy_notes:         LLM instructions specific to this market
#   photo_setting_hint: Additional context for the Gemini photo_subject / setting description
#   avoid:             Things that actively hurt CTR or feel culturally off in this market
#
# Source: historical InMail CTR analysis, Outlier market observations, and
# Pranav's 2026-05-04 guidance on geo ICP customisation.

GEO_ICP_PROFILES: dict[str, dict[str, str]] = {
    "anglo": {
        "primary_hook": "Flexible income — remote work on your own terms, no commute, no boss",
        "credential_signals": "top university degrees (Ivy League, Russell Group, sandstone AU/CA), FAANG/Goldman/McKinsey experience",
        "copy_notes": (
            "American English (for US/CA). British English spelling for GB audiences (colour, recognise). "
            "Flexibility framing works well ('no fixed hours', 'around your existing work'). "
            "Income is important but frame it as the reward for expertise, not desperation. "
            "Use concrete dollar figures — '$50/hr' not 'competitive pay'. "
            "Tech professionals respond to 'improve AI models'; academics respond to 'your research makes AI more accurate'."
        ),
        "photo_setting_hint": (
            "Modern minimalist home office, clean shelving, MacBook, large window with natural light. "
            "For US: contemporary American interior. For UK: slightly more formal British home study. "
            "For AU: bright, airy, coastal-influenced natural light."
        ),
        "avoid": "Phrases that imply financial desperation. 'Side hustle' language. Overuse of 'amazing opportunity'.",
    },

    "northern_european": {
        "primary_hook": "Work-life balance and intellectual contribution — fit around your life, not the other way around",
        "credential_signals": "TU Munich, ETH Zürich, TU Delft, Grandes Écoles (France), top Dutch/Scandinavian research universities",
        "copy_notes": (
            "Germans and Dutch value precision and substance over hype. Be factual and specific. "
            "NO superlatives ('amazing', 'incredible', 'best'). "
            "French professionals respond to intellectual framing — 'contribute to frontier AI research'. "
            "Swiss are highly income-aware (strong CHF) — frame USD/EUR-comparable rates. "
            "Work-life balance ('Arbeitszeit' / no fixed shifts) is a primary motivator in DE/NL/FR. "
            "Avoid American-style hustle culture framing entirely."
        ),
        "photo_setting_hint": (
            "Clean, understated European home office. Scandinavian-style minimal furniture, "
            "warm indirect lighting. Bookshelf with technical books. Subject dressed professionally but not formally — "
            "relaxed, intellectual, comfortable."
        ),
        "avoid": "Exclamation points in copy. 'Amazing opportunity'. 'Don't miss this'. Any urgency-scarcity tactics.",
    },

    "southern_european": {
        "primary_hook": "Supplement your income flexibly — strong professionals here often face income ceiling despite expertise",
        "credential_signals": "Bocconi, Sapienza, Complutense, top Spanish/Italian/Portuguese research universities",
        "copy_notes": (
            "Spain, Italy, Portugal professionals are highly educated but often underpaid vs Northern Europe. "
            "Income supplementation framing ('earn more') works well alongside flexibility. "
            "Professional identity and expertise pride matter — acknowledge their specialisation. "
            "Remote income in USD/EUR feels like accessing global pay despite local salary compression."
        ),
        "photo_setting_hint": (
            "Mediterranean home office — warm tones, natural terracotta and wood, afternoon light. "
            "Subject relaxed but focused, home setting feels lived-in and professional."
        ),
        "avoid": "Implying their local work isn't valuable. Pure income-desperation framing.",
    },

    "eastern_european": {
        "primary_hook": "USD/EUR income at Western rates — your expertise is valued globally even from Kraków, Warsaw, Bucharest",
        "credential_signals": "Warsaw University of Technology, Czech Technical University, Jagiellonian University, AGH, top Romanian/Bulgarian tech universities",
        "copy_notes": (
            "Strong USD framing — earning in USD from Eastern Europe is a major life upgrade. "
            "Tech talent density is high (Poland, Czechia, Romania have strong CS cultures). "
            "Remote work has boomed here post-2020 — frame as 'global remote market you can access'. "
            "Keep tone professional and direct — Eastern European audiences are skeptical of hype. "
            "Ukraine audience: may be displaced — acknowledge remote-first explicitly ('work from anywhere')."
        ),
        "photo_setting_hint": (
            "Home office, slightly more modest than Western European — bookshelves, warm lamp light. "
            "Subject focused, determined. Setting feels Eastern European urban apartment: functional, tidy."
        ),
        "avoid": "Any stereotyping of Eastern Europe as lower-tier. Condescending 'you can earn like the West' framing.",
    },

    "south_asian": {
        "primary_hook": "USD income from India/South Asia at rates that match or exceed top domestic roles — no relocation needed",
        "credential_signals": (
            "IIT, IIM, AIIMS, NIT, BITS Pilani, IISC — these are the prestige signals. "
            "Also: Google/Microsoft/Goldman India offices, top product companies (Razorpay, CRED, Zerodha)."
        ),
        "copy_notes": (
            "USD payment is the single most powerful hook — $27-30/hr = ₹2,200-2,500/hr, well above most Indian salaries. "
            "Make the USD figure prominent and explicit. Do NOT convert to INR in copy — keep it in USD. "
            "Credential acknowledgment works: 'your IIT background', 'your clinical expertise from AIIMS'. "
            "Remote work from Bangalore, Mumbai, Hyderabad, Chennai, Pune, Delhi is the mental model. "
            "Flexibility framing: 'outside your existing consulting/hospital hours' — Indian professionals often have side income expectations. "
            "For medical/clinical ICPs: 'consulting-level income without leaving your city'. "
            "Photo subject: South Asian person in a modern home office, could be Bangalore-style apartment."
        ),
        "photo_setting_hint": (
            "Modern South Asian home office — warm, plant-rich, natural light from a window. "
            "Contemporary furniture, laptop setup. Could suggest a Bangalore or Mumbai professional apartment. "
            "Subject: South Asian (Indian, Pakistani, or Bangladeshi — vary by cohort) professional, "
            "mid-20s to 40s, focused and accomplished-looking."
        ),
        "avoid": "Framing that implies financial desperation. Western condescension. Implying this is a backup option.",
    },

    "southeast_asian": {
        "primary_hook": "Remote USD income — earn at Silicon Valley rates from Singapore, Manila, Jakarta, or anywhere in SEA",
        "credential_signals": (
            "NUS, NTU (Singapore), UP Diliman, Ateneo (Philippines), UI, ITB (Indonesia), "
            "Mahidol, Chulalongkorn (Thailand). Global tech company offices (Google SG, Meta SG, Grab, Sea Group)."
        ),
        "copy_notes": (
            "Singapore: high cost of living, USD parity — rate feels competitive vs local wages. Intellectual and financial framing both work. "
            "Philippines: USD income is the #1 hook — $25-35/hr vs typical BPO rates of $5-8/hr is transformative. "
            "Explicitly mention English proficiency as the bridge: 'your English and [domain] expertise'. "
            "Indonesia: remote USD income vs Rupiah is a massive multiplier — financial framing very strong. "
            "Vietnam, Thailand: tech talent is growing fast — frame as 'global opportunity for local expertise'. "
            "Photo subject for Philippines: Filipino professional in home/apartment office, warm casual setting. "
            "Photo subject for SG: polished multinational professional aesthetic."
        ),
        "photo_setting_hint": (
            "For Singapore: clean, modern, high-rise apartment office feel — polished professional. "
            "For Philippines/Indonesia/Vietnam: warm home office, tropical light quality, relaxed professional. "
            "Subject reflects local ethnicity — Malay/Filipino/Indonesian/Vietnamese as appropriate."
        ),
        "avoid": "Assuming all SEA markets are the same — Singapore and Philippines have very different positioning needs.",
    },

    "east_asian": {
        "primary_hook": "Contribute to frontier AI research — prestigious, intellectually rigorous work from Japan, Korea, or Taiwan",
        "credential_signals": "University of Tokyo, Kyoto University, Seoul National University, KAIST, POSTECH, NTU Taiwan, top research institutions",
        "copy_notes": (
            "Japan: prestige and intellectual rigor matter more than income alone. "
            "'Contribute to world-class AI development' framing works. Be precise and understated. "
            "Remote work is still less culturally normalised in Japan — frame as 'outside your main work hours' not 'replace your job'. "
            "Korea: tech culture is very strong (Samsung, Kakao, Naver ecosystem). USD income is appealing. "
            "Ambition and career growth framing works for Korean audience. "
            "Taiwan: semiconductor/hardware expertise — mention 'technical depth' and 'engineering excellence'. "
            "All East Asian markets: formal but not stuffy. High credibility threshold — mention $500M paid to contributors."
        ),
        "photo_setting_hint": (
            "Minimalist, precise home office setup. Clean lines, nothing extraneous. "
            "Japanese setting: tatami mat detail or sliding door visible, natural light. "
            "Korean setting: modern tech-forward apartment, dual monitor setup. "
            "Subject: East Asian professional (Japanese/Korean/Taiwanese), focused, composed expression."
        ),
        "avoid": "Casual American English tone. Slang. Anything that feels insufficiently serious or rigorous.",
    },

    "latin_american": {
        "primary_hook": "Earn in USD — protect your income from inflation and access global rates from anywhere in Latin America",
        "credential_signals": "UNAM, PUC Chile, Universidad de los Andes (Colombia), UBA (Argentina), top LatAm engineering schools",
        "copy_notes": (
            "USD payment is the single most powerful hook for ALL LatAm markets. "
            "Currency volatility (ARS, COP, PEN, CLP, MXN vs USD) is top of mind for educated professionals. "
            "'Earn in USD' or 'pagado en dólares' framing (even in English) resonates immediately. "
            "Do NOT frame as 'supplemental income' — for many, this is primary income-level work. "
            "Mexico: proximity to US market, many are familiar with remote USD work via US companies. "
            "Colombia, Peru: growing tech talent pools, aspiration to access global pay. "
            "Argentina: highest urgency — ARS inflation is extreme. USD = financial stability. "
            "Panama: already USD economy — focus on flexibility and expertise recognition instead. "
            "Photo subject: Hispanic/Latin American professional, home office in LatAm urban setting."
        ),
        "photo_setting_hint": (
            "Latin American home office — warm tones, natural light, plants. "
            "Urban apartment feel (Bogotá, Mexico City, Santiago, Buenos Aires). "
            "Subject: Hispanic/Latin American professional, friendly and professional expression, "
            "modern laptop setup, feels aspirational but accessible."
        ),
        "avoid": "Anything implying LatAm professionals are desperate. 'Side hustle'. Condescending 'even you can earn...' framing.",
    },

    "brazilian": {
        "primary_hook": "Renda em dólares — earn USD from anywhere in Brazil as BRL loses value",
        "credential_signals": "USP, UNICAMP, UFRJ, UFMG, ITA, IME, top Brazilian engineering/medicine universities",
        "copy_notes": (
            "Brazil has its own strong tech ecosystem (Nubank, Mercado Libre, iFood) — acknowledge Brazilian tech identity. "
            "BRL has depreciated significantly — USD income is financially transformative. "
            "Copy should still be in English (LinkedIn targets English-speaking professionals) but can reference "
            "'from Brazil', 'from São Paulo / Rio / Belo Horizonte / Florianópolis'. "
            "Flexibility is valued — Brazilians have strong work-life culture ('trabalho para viver'). "
            "Photo subject: Brazilian professional, warm home office setting, could be São Paulo apartment."
        ),
        "photo_setting_hint": (
            "Warm, vibrant Brazilian home office — tropical plants, warm afternoon light. "
            "São Paulo or Rio de Janeiro apartment feel — modern but warm. "
            "Subject: Brazilian professional (mixed-heritage or Afro-Brazilian or lighter-skinned Brazilian — vary across angles)."
        ),
        "avoid": "Implying Brazilian professionals are less qualified than US counterparts.",
    },

    "middle_eastern": {
        "primary_hook": "Earn at global rates for elite expertise — prestige and income at the level your credentials deserve",
        "credential_signals": "AUB (Beirut), Hebrew University, Technion, UAE University, King Saud University, expat credentials from global universities",
        "copy_notes": (
            "UAE/Saudi: professional prestige matters enormously. Photo subject should look polished. "
            "Israel: strong tech ecosystem (Unit 8200 alumni, Tel Aviv startup scene) — tech/AI angle resonates. "
            "Frame as 'global AI industry values your expertise' — appeals to professionals who feel under-recognised. "
            "USD parity in UAE (AED is pegged to USD) — rate emphasis less critical, but global credibility matters. "
            "For expat-heavy markets (UAE): diverse photo subjects; not exclusively Arab. "
            "Jordan, Lebanon: USD income is highly valued given currency instability."
        ),
        "photo_setting_hint": (
            "Polished, high-end home office. Elegant, minimal. "
            "UAE: modern skyscraper visible in background or glass-and-steel apartment aesthetic. "
            "Israel: tech-forward home office, casual professional. "
            "Subject: Middle Eastern or globally diverse professional — polished expression."
        ),
        "avoid": "Anything culturally insensitive. Do not generalise the Middle East as one homogeneous market.",
    },

    "african": {
        "primary_hook": "USD income that transforms your earning potential — access global rates from Nigeria, Kenya, Ghana, or South Africa",
        "credential_signals": "University of Lagos, University of Nairobi, University of Cape Town, Wits, KNUST, top African universities",
        "copy_notes": (
            "USD income is transformative in most African markets (NGN, KES, GHS vs USD). "
            "Nigeria: large, highly educated tech talent pool (fintech, crypto-savvy). "
            "Emphasise payment reliability — Nigerian professionals have been burned by unreliable platforms. "
            "Kenya: mobile money culture, remote work growing rapidly. M-Pesa payment comfort. "
            "South Africa: more Westernised market — flexibility and career growth framing alongside income. "
            "Ghana: growing tech sector, aspiration to access global market. "
            "Credibility signal important: mention '$500M paid to contributors worldwide' — reduces skepticism. "
            "Photo subject should reflect the specific country's dominant demographics."
        ),
        "photo_setting_hint": (
            "African home office — warm, natural light, plants, laptop. "
            "Lagos/Nairobi/Accra modern urban apartment feel. "
            "Subject: Black African professional, age 25-40, focused and accomplished. "
            "Setting feels aspirational but real — not over-styled."
        ),
        "avoid": "Any language that could read as condescending. 'You deserve better' framing done carelessly. Payment uncertainty language.",
    },

    "global_mix": {
        "primary_hook": "Remote flexible income — your expertise has global value",
        "credential_signals": "Top universities and employers in your field",
        "copy_notes": (
            "Generic global framing — use when geo cluster is unknown or truly mixed. "
            "Lead with flexibility and income. USD/hourly rate should be prominent. "
            "Keep photo subject vague — 'professional in a home office' — do not assume ethnicity."
        ),
        "photo_setting_hint": "Neutral, clean home office. No culturally specific indicators. Natural light.",
        "avoid": "Assuming any specific cultural context.",
    },
}


def get_geo_icp_prompt_hint(cluster: str, geos: list[str] | None = None) -> str:
    """
    Build a geo-ICP context block to inject into copy-gen LLM prompts.
    Returns a formatted string that the LLM should use to customise copy
    tone, hooks, and references for this specific geo cluster.

    The hint intentionally avoids hardcoding specific phrases — it gives
    the LLM enough cultural context to make natural, authentic choices
    rather than formulaic insertions.
    """
    profile = GEO_ICP_PROFILES.get(cluster, GEO_ICP_PROFILES["global_mix"])
    geo_names = ", ".join(geos[:6]) + ("..." if geos and len(geos) > 6 else "") if geos else "this region"

    return f"""
GEO-SPECIFIC ICP CONTEXT — {CLUSTER_LABELS.get(cluster, cluster).upper()} MARKET ({geo_names})
This campaign targets professionals in {geo_names}. Customise copy and photo_subject for this market:

PRIMARY HOOK FOR THIS MARKET: {profile['primary_hook']}

CREDENTIAL SIGNALS THAT RESONATE: {profile['credential_signals']}

COPY GUIDANCE: {profile['copy_notes']}

PHOTO SETTING / SUBJECT DETAIL: {profile['photo_setting_hint']}

AVOID IN THIS MARKET: {profile['avoid']}

These are guidelines, not verbatim script. Weave the relevant cultural signals naturally
into the copy — do NOT list them or make them feel like a checklist.
"""


@dataclass
class GeoCampaignGroup:
    """One campaign's worth of geo targeting — a cluster of culturally similar countries."""
    cluster:           str    # ethnic cluster key
    cluster_label:     str    # human-readable label
    geos:              list[str]  # ISO country codes in this group
    median_multiplier: float  # median pay multiplier across geos
    advertised_rate:   str    # formatted rate string for copy, e.g. "$35/hr"
    campaign_suffix:   str    # e.g. "south_asian" for campaign name
    icp_hint:          str = ""   # pre-built geo ICP prompt block for LLM injection


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
            icp_hint=get_geo_icp_prompt_hint(cluster, [cc]),
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
            icp_hint=get_geo_icp_prompt_hint(cluster, geos),
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

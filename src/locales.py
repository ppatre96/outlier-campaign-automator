"""
Locale reference data for generalist/i18n ramp targeting (Bug 2).

When a Smart Ramp cohort is a per-locale generalist cohort (e.g. "Bengali
generalist contributors"), the pipeline targets by language + geo instead of
running Stage A beam discovery over résumé features. This module maps each
locale to the channel-specific targeting IDs and a seed keyword set.

Sourcing (verified 2026-06-04 against live APIs):
- meta_locale_id        — Meta `search?type=adlocale` key
- google_language_const — Google Ads `language_constant.id`
- linkedin_locale       — BCP-47 locale for a future LinkedIn interfaceLocales
                          facet. v1 targets LinkedIn GEO-ONLY for generalist
                          cohorts (geo correlates with locale, and several of
                          these languages aren't LinkedIn interface locales),
                          so this is documentation/forward-looking only.
- generic_keywords      — SEED localized generic keywords for the Google Search
                          arm. Curated, NOT authoritative — reviewers refine
                          them via the console keyword-review card.

See data/plan_generalist_locale_targeting.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field


@dataclass(frozen=True)
class LocaleTargeting:
    locale: str                       # BCP-47, lower-case (e.g. "bn-in")
    display_language: str             # human label (e.g. "Bengali")
    meta_locale_id: int | None        # Meta adlocale key
    google_language_const: int | None # Google Ads language_constant.id
    linkedin_locale: str | None       # forward-looking; v1 LinkedIn = geo-only
    generic_keywords: list[str] = field(default_factory=list)


# Keyed by lower-cased BCP-47 locale. Seeded for the 13 GMR-0023 locales.
LOCALES: dict[str, LocaleTargeting] = {
    "bn-in": LocaleTargeting(
        "bn-in", "Bengali", 45, 1056, None,
        ["বাড়ি থেকে কাজ", "অনলাইন কাজ", "অনলাইনে আয়", "ওয়ার্ক ফ্রম হোম"],
    ),
    "de-de": LocaleTargeting(
        "de-de", "German", 5, 1001, "de_DE",
        ["heimarbeit", "online jobs", "geld verdienen online", "remote arbeit", "nebenjob online"],
    ),
    "fr-fr": LocaleTargeting(
        "fr-fr", "French", 9, 1002, "fr_FR",
        ["travail à domicile", "emploi en ligne", "gagner de l'argent en ligne", "travail à distance", "petit boulot en ligne"],
    ),
    "hi-in": LocaleTargeting(
        "hi-in", "Hindi", 46, 1023, None,
        ["घर से काम", "ऑनलाइन जॉब", "ऑनलाइन पैसे कमाएं", "वर्क फ्रॉम होम"],
    ),
    "id-id": LocaleTargeting(
        "id-id", "Indonesian", 25, 1025, "in_ID",
        ["kerja dari rumah", "kerja online", "menghasilkan uang online", "kerja remote", "kerja sampingan online"],
    ),
    "it-it": LocaleTargeting(
        "it-it", "Italian", 10, 1004, "it_IT",
        ["lavoro da casa", "lavoro online", "guadagnare online", "lavoro a distanza", "lavoretti online"],
    ),
    "ko-kr": LocaleTargeting(
        "ko-kr", "Korean", 12, 1012, "ko_KR",
        ["재택근무", "온라인 부업", "온라인 알바", "재택 알바", "부업 추천"],
    ),
    "pt-br": LocaleTargeting(
        "pt-br", "Brazilian Portuguese", 16, 1014, "pt_BR",
        ["trabalho em casa", "trabalho online", "ganhar dinheiro online", "trabalho remoto", "renda extra online"],
    ),
    "th-th": LocaleTargeting(
        "th-th", "Thai", 35, 1044, "th_TH",
        ["งานออนไลน์", "งานทำที่บ้าน", "หารายได้ออนไลน์", "งานพาร์ทไทม์ออนไลน์"],
    ),
    "tl-ph": LocaleTargeting(
        "tl-ph", "Tagalog", 26, 1042, "tl_PH",
        ["work from home", "online job", "raket online", "trabaho online", "extra income online"],
    ),
    "vi-vn": LocaleTargeting(
        "vi-vn", "Vietnamese", 27, 1040, "vi_VN",
        ["việc làm tại nhà", "việc làm online", "kiếm tiền online", "làm việc từ xa", "việc làm thêm online"],
    ),
    "zh-cn": LocaleTargeting(
        "zh-cn", "Simplified Chinese", 20, 1017, "zh_CN",
        ["在家工作", "网上兼职", "线上工作", "网上赚钱", "远程工作"],
    ),
    "ar-eg": LocaleTargeting(
        "ar-eg", "Egyptian Arabic", 28, 1019, "ar_AE",
        ["العمل من المنزل", "وظائف اون لاين", "الربح من الانترنت", "عمل عن بعد"],
    ),
}


# LinkedIn language SKILL URNs (Diego 2026-06-04: "on skills you can look for
# languages"). LinkedIn has no interface-locale facet for most of these, but it
# DOES expose each language as a skill — so a generalist locale cohort targets
# people with the language skill WITHIN the geo, not just everyone in the geo.
# Resolved via typeahead_facet('skills', display_language); id-id/zh-cn use the
# closest language-skill match (Bahasa Indonesia / Chinese).
LINKEDIN_LANGUAGE_SKILL: dict[str, str] = {
    "bn-in": "urn:li:skill:12523",   # Bengali
    "de-de": "urn:li:skill:927",     # German
    "fr-fr": "urn:li:skill:605",     # French
    "hi-in": "urn:li:skill:2851",    # Hindi
    "id-id": "urn:li:skill:16250",   # Bahasa Indonesia
    "it-it": "urn:li:skill:1510",    # Italian
    "ko-kr": "urn:li:skill:5747",    # Korean
    "pt-br": "urn:li:skill:13954",   # Brazilian Portuguese
    "th-th": "urn:li:skill:31219",   # Thai
    "tl-ph": "urn:li:skill:12884",   # Tagalog
    "vi-vn": "urn:li:skill:8669",    # Vietnamese
    "zh-cn": "urn:li:skill:2473",    # Chinese
    "ar-eg": "urn:li:skill:25093",   # Egyptian Arabic
}


def linkedin_skill_urn(locale: str | None) -> str | None:
    """LinkedIn language-skill URN for a BCP-47 locale ("bn-in" → Bengali skill).
    Case/format-insensitive. None when unknown → caller falls back to geo-only."""
    if not locale:
        return None
    return LINKEDIN_LANGUAGE_SKILL.get(locale.strip().lower().replace("_", "-"))


# ISO-2 → LinkedIn-friendly country name. Smart Ramp included_geos are ISO-2
# codes, but LinkedIn's profileLocations facet matches by NAME — a raw "BD"
# fuzzy-matches nothing (geo silently dropped) and "IN" mis-matches. Explicit
# for the names where the ISO official name wouldn't fuzzy-match LinkedIn's
# label (South Korea / Vietnam / United Kingdom / …); pycountry fills the rest.
_ISO2_COUNTRY_NAME: dict[str, str] = {
    "BD": "Bangladesh", "IN": "India", "DE": "Germany", "FR": "France",
    "ID": "Indonesia", "IT": "Italy", "KR": "South Korea", "BR": "Brazil",
    "TH": "Thailand", "PH": "Philippines", "VN": "Vietnam", "CN": "China",
    "EG": "Egypt", "US": "United States", "CA": "Canada", "GB": "United Kingdom",
    "AU": "Australia", "NZ": "New Zealand", "MY": "Malaysia", "SG": "Singapore",
}


_COUNTRY_NAME_TO_ISO2: dict[str, str] = {
    name.lower(): iso for iso, name in _ISO2_COUNTRY_NAME.items()
}
# A few aliases the LLM commonly emits in the ICP `geography` field.
_COUNTRY_NAME_TO_ISO2.update({
    "usa": "US", "u.s.": "US", "u.s.a.": "US", "united states of america": "US",
    "uk": "GB", "u.k.": "GB", "britain": "GB", "england": "GB",
    "korea": "KR", "republic of korea": "KR", "south korea": "KR",
})


def country_name_to_iso2(name: str | None) -> str | None:
    """Full country name → ISO-2 ("India" → "IN"). Inverse of country_name_for,
    used to map the job-post ICP `geography` field to a targetable geo when the
    ramp left included_geos empty. Returns None for "Global"/regions/unknowns."""
    if not name or not isinstance(name, str):
        return None
    n = name.strip().lower()
    if not n or n in ("global", "worldwide", "remote", "anywhere"):
        return None
    if len(n) == 2 and n.upper() in _ISO2_COUNTRY_NAME:
        return n.upper()
    if n in _COUNTRY_NAME_TO_ISO2:
        return _COUNTRY_NAME_TO_ISO2[n]
    try:
        import pycountry
        x = (pycountry.countries.get(name=name.strip())
             or pycountry.countries.get(common_name=name.strip()))
        if x is not None:
            return x.alpha_2
    except Exception:
        pass
    return None


def country_name_for(code: str | None) -> str | None:
    """ISO-2 country code → full country name for LinkedIn geo resolution
    ("BD" → "Bangladesh"). Returns the input unchanged for non-2-letter values
    (already a name) or unknown codes."""
    if not code or not isinstance(code, str):
        return code
    c = code.strip().upper()
    if len(c) != 2 or not c.isalpha():
        return code
    if c in _ISO2_COUNTRY_NAME:
        return _ISO2_COUNTRY_NAME[c]
    try:
        import pycountry
        x = pycountry.countries.get(alpha_2=c)
        if x is not None:
            return getattr(x, "common_name", None) or x.name
    except Exception:
        pass
    return code


def region_for_locale(locale: str | None) -> str | None:
    """Return the ISO-2 country/region from a BCP-47 locale ("ko-kr" → "KR").

    Used as the geo fallback for generalist locale cohorts whose ramp leaves
    included_geos empty: Meta's EMPLOYMENT SAC needs a non-empty country list
    on BOTH the ad set's targeting AND the parent campaign's
    special_ad_category_country (mismatch → subcode 2909035). Returns None when
    the locale has no region segment.
    """
    if not locale or "-" not in str(locale):
        return None
    region = str(locale).split("-")[-1].strip().upper()
    return region or None


def get_locale(locale: str | None) -> LocaleTargeting | None:
    """Look up targeting data for a BCP-47 locale (case/format-insensitive).

    Accepts 'bn-in', 'bn_IN', 'BN-IN', etc. Returns None for unknown locales
    so callers fall back to geo-only targeting.
    """
    if not locale:
        return None
    key = locale.strip().lower().replace("_", "-")
    return LOCALES.get(key)

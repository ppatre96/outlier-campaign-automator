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

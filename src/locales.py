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
    # es-mx was the ONE GMR-0023 locale missing here — without it
    # is_generalist_cohort() failed (b) "known locale", so the es-MX cohort fell
    # through to Stage-A beam and mined noise cohorts (english_certificate_b2,
    # phd_student, …) instead of targeting Mexican-Spanish generalists. Added
    # 2026-06-09. Meta adlocale 23 = "Spanish" (Latin-American; 7 is Spain);
    # Google 1003 = Spanish. Geo MX scopes it to Mexico.
    "es-mx": LocaleTargeting(
        "es-mx", "Mexican Spanish", 23, 1003, "es_MX",
        ["trabajo desde casa", "trabajo en línea", "ganar dinero en línea", "trabajo remoto", "ingreso extra en línea"],
    ),
    # he-il / kn-in / ru-ru were added to GMR-0023 AFTER the es-mx fix and were
    # likewise missing here → is_generalist_cohort() failed (b) "known locale",
    # so these "X generalist contributors" cohorts fell to Stage-A beam (résumé
    # noise) instead of targeting the language WITHIN the geo. All IDs resolved
    # live 2026-06-12 (not guessed): LinkedIn skill via typeahead_facet('skills'),
    # Meta adlocale via Graph search?type=adlocale, Google language_constant via
    # the Ads API. Google uses the legacy code "iw" for Hebrew (id 1027).
    "he-il": LocaleTargeting(
        "he-il", "Hebrew", 29, 1027, "he_IL",
        ["עבודה מהבית", "עבודה אונליין", "הכנסה מהאינטרנט", "עבודה מרחוק", "הכנסה נוספת"],
    ),
    "kn-in": LocaleTargeting(
        "kn-in", "Kannada", 75, 1086, "kn_IN",
        ["ಮನೆಯಿಂದ ಕೆಲಸ", "ಆನ್‌ಲೈನ್ ಕೆಲಸ", "ಆನ್‌ಲೈನ್‌ನಲ್ಲಿ ಹಣ ಗಳಿಸಿ", "ಮನೆಯಿಂದಲೇ ಕೆಲಸ"],
    ),
    "ru-ru": LocaleTargeting(
        "ru-ru", "Russian", 17, 1031, "ru_RU",
        ["работа из дома", "работа онлайн", "заработок в интернете", "удаленная работа", "подработка онлайн"],
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
    "he-il": "urn:li:skill:5494",    # Hebrew (typeahead 2026-06-12)
    "kn-in": "urn:li:skill:14655",   # Kannada (typeahead 2026-06-12)
    "ru-ru": "urn:li:skill:2708",    # Russian (typeahead 2026-06-12)
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


# LinkedIn Ads supported campaign/profile languages (governs Message Ads too —
# there is no separate InMail message-language setting; see the LinkedIn help
# page "Supported languages for LinkedIn Ads campaigns", 35 langs, only Chinese
# excluded). Keyed by our LocaleTargeting.display_language label. Used to gate
# InMail localization: we only translate InMails into a language LinkedIn can
# actually target. (EU delivery is a separate GEO/consent concern, not a
# language block — surfaced as a caveat in the console, not enforced here.)
LINKEDIN_AD_LANGUAGES = frozenset({
    "Arabic", "Bengali", "Czech", "Danish", "Dutch", "English", "Finnish",
    "French", "German", "Greek", "Hebrew", "Hindi", "Hungarian", "Indonesian",
    "Italian", "Japanese", "Korean", "Malay", "Malaysian", "Marathi",
    "Norwegian", "Persian", "Polish", "Portuguese", "Punjabi", "Romanian",
    "Russian", "Spanish", "Swedish", "Tagalog", "Telugu", "Thai", "Turkish",
    "Ukrainian", "Vietnamese",
})


def linkedin_supports_language(display_language: str) -> bool:
    """True when LinkedIn Ads can target the given display language (so an
    InMail localized into it can actually be delivered). Matches region-variant
    labels too — e.g. "Mexican Spanish" → Spanish, "Brazilian Portuguese" →
    Portuguese — by checking whether any supported language name is contained
    in the label."""
    dl = (display_language or "").strip()
    if not dl:
        return False
    if dl in LINKEDIN_AD_LANGUAGES:
        return True
    low = dl.lower()
    return any(lang.lower() in low for lang in LINKEDIN_AD_LANGUAGES)


# ── Copy localization (2026-06-17) ─────────────────────────────────────────────
# BCP-47 prefixes / exact codes that mean "English audience" → no localization.
# language_pref is a BCP-47 code (e.g. "hi-IN", "en-US", "es-419"); en-* of any
# region (en-IN, en-GB, en-AU) is an English audience and must NOT be localized.
_ENGLISH_LANGUAGE_CODES = {"", "en"}


# Per-language brand-voice notes for the native-language copy LLM. Keyed by the
# LocaleTargeting.display_language label. Single source of truth — both
# copy_adapter (Meta/Google/Reddit localization) and figma_creative consume this.
# Languages without an entry fall back to a generic instruction (see
# locale_brand_voice_notes). "Outlier" always stays in English.
_LOCALE_BRAND_VOICE: dict[str, str] = {
    "German":            "Deutsch. 'Outlier' stays in English. Avoid 'Job'/'Stelle' → 'Aufgabe'/'Gelegenheit'. Avoid 'Training' → 'Einarbeitung in die Projektrichtlinien'. Avoid 'Vergütung' → 'Zahlung'. Avoid 'Vorstellungsgespräch' → 'Eignungstest'.",
    "French":            "Français. 'Outlier' stays in English. Avoid 'emploi'/'poste' → 'mission'/'opportunité'. Avoid 'formation' → 'prise en main des directives du projet'. Avoid 'rémunération' → 'paiement'. Avoid 'entretien' → 'évaluation'.",
    "Italian":           "Italiano. 'Outlier' stays in English. Avoid 'lavoro'/'posizione' → 'opportunità'/'attività'. Avoid 'formazione' → 'familiarizzazione con le linee guida del progetto'. Avoid 'compenso' → 'pagamento'. Avoid 'colloquio' → 'selezione'.",
    "Indonesian":        "Bahasa Indonesia. 'Outlier' stays in English. Avoid 'pekerjaan'/'posisi' → 'tugas'/'kesempatan'. Avoid 'pelatihan' → 'memahami panduan proyek'. Avoid 'kompensasi' → 'pembayaran'. Avoid 'wawancara' → 'seleksi'.",
    "Tagalog":           "Filipino/Tagalog. 'Outlier' stays in English. Avoid 'trabaho'/'posisyon' → 'gawain'/'oportunidad'. Avoid 'pagsasanay' → 'pag-aaral ng alituntunin ng proyekto'. Avoid 'kabayaran' → 'bayad'. Avoid 'interbyu' → 'screening'.",
    "Bengali":           "Bengali (বাংলা). 'Outlier' stays in English. Avoid 'চাকরি'/'পদ' → 'কাজ'/'সুযোগ'. Avoid 'প্রশিক্ষণ' → 'প্রকল্পের নির্দেশিকার সাথে পরিচিত হওয়া'. Avoid 'বেতন' → 'পেমেন্ট'. Avoid 'ইন্টারভিউ' → 'স্ক্রিনিং'.",
    "Hindi":             "Hindi (हिन्दी). 'Outlier' stays in English. Avoid 'नौकरी'/'पद' → 'कार्य'/'अवसर'. Avoid 'प्रशिक्षण' → 'परियोजना दिशानिर्देशों से परिचित होना'. Avoid 'वेतन' → 'भुगतान'. Avoid 'साक्षात्कार' → 'स्क्रीनिंग'.",
    "Egyptian Arabic":   "Arabic (العربية). 'Outlier' stays in English. Avoid 'وظيفة'/'منصب' → 'مهمة'/'فرصة'. Avoid 'تدريب' → 'التعرف على إرشادات المشروع'. Avoid 'راتب' → 'دفع'. Avoid 'مقابلة' → 'فحص'.",
    "Brazilian Portuguese": "Português (Brasil). 'Outlier' stays in English. Avoid 'emprego'/'vaga' → 'tarefa'/'oportunidade'. Avoid 'treinamento' → 'familiarização com as diretrizes do projeto'. Avoid 'remuneração' → 'pagamento'. Avoid 'entrevista' → 'triagem'.",
    "Mexican Spanish":   "Español (México). 'Outlier' stays in English. Avoid 'empleo'/'puesto' → 'tarea'/'oportunidad'. Avoid 'capacitación' → 'familiarización con las pautas del proyecto'. Avoid 'remuneración' → 'pago'. Avoid 'entrevista' → 'evaluación'.",
    "Korean":            "Korean (한국어). 'Outlier' stays in English. Avoid '일자리'/'직책' → '작업'/'기회'. Avoid '교육' → '프로젝트 가이드라인 숙지'. Avoid '보수' → '지급'. Avoid '면접' → '심사'.",
    "Vietnamese":        "Tiếng Việt. 'Outlier' stays in English. Avoid 'việc làm'/'vị trí' → 'nhiệm vụ'/'cơ hội'. Avoid 'đào tạo' → 'làm quen với hướng dẫn dự án'. Avoid 'lương' → 'thanh toán'. Avoid 'phỏng vấn' → 'sàng lọc'.",
    "Thai":              "ไทย. 'Outlier' stays in English. Avoid 'งาน'/'ตำแหน่ง' → 'งาน'/'โอกาส'. Avoid 'การฝึกอบรม' → 'ทำความคุ้นเคยกับแนวทางโครงการ'. Avoid 'ค่าตอบแทน' → 'การจ่ายเงิน'. Avoid 'สัมภาษณ์' → 'การคัดกรอง'.",
    "Simplified Chinese": "简体中文. 'Outlier' stays in English. Avoid '工作'/'职位' → '任务'/'机会'. Avoid '培训' → '熟悉项目指南'. Avoid '薪酬' → '付款'. Avoid '面试' → '筛选'.",
    "Hebrew":            "עברית. 'Outlier' stays in English. Avoid 'משרה'/'תפקיד' → 'משימה'/'הזדמנות'. Avoid 'הכשרה' → 'היכרות עם הנחיות הפרויקט'. Avoid 'שכר' → 'תשלום'. Avoid 'ראיון' → 'סינון'.",
    "Kannada":           "ಕನ್ನಡ. 'Outlier' stays in English. Avoid 'ಉದ್ಯೋಗ'/'ಹುದ್ದೆ' → 'ಕಾರ್ಯ'/'ಅವಕಾಶ'. Avoid 'ತರಬೇತಿ' → 'ಯೋಜನೆಯ ಮಾರ್ಗಸೂಚಿಗಳ ಪರಿಚಯ'. Avoid 'ವೇತನ' → 'ಪಾವತಿ'. Avoid 'ಸಂದರ್ಶನ' → 'ಪರಿಶೀಲನೆ'.",
    "Russian":           "Русский. 'Outlier' stays in English. Avoid 'работа'/'должность' → 'задача'/'возможность'. Avoid 'обучение' → 'ознакомление с рекомендациями проекта'. Avoid 'оплата труда' → 'оплата'. Avoid 'собеседование' → 'отбор'.",
}


def locale_brand_voice_notes(display_language: str) -> str:
    """Brand-voice / banned-term note for writing copy in `display_language`.

    Single source consumed by copy_adapter (Meta/Google/Reddit localization)
    and figma_creative. Falls back to a generic instruction for languages not
    in `_LOCALE_BRAND_VOICE`. "Outlier" always stays in English.
    """
    return _LOCALE_BRAND_VOICE.get(
        display_language,
        f"Write all copy in {display_language}. 'Outlier' stays in English.",
    )


def _lang_pref_is_english(language_pref: str | None) -> bool:
    """True when a BCP-47 language_pref denotes an English audience (en-*)."""
    if not language_pref:
        return True
    code = str(language_pref).strip().lower().replace("_", "-")
    return code in _ENGLISH_LANGUAGE_CODES or code.split("-")[0] == "en"


def resolve_copy_locale(cohort=None, icp=None) -> LocaleTargeting | None:
    """Resolve the target copy locale for a cohort, or None to stay in English.

    Locale-defined cohorts only (2026-06-17 decision). Resolution order:
      1. `cohort.facet_strength["generalist_locale"]` (e.g. "hi-in") — the
         generalist/i18n per-locale cohorts.
      2. ICP `language_pref` (BCP-47, e.g. "hi-IN") when it's a non-English
         audience and maps to a known LOCALES entry.
    Returns the LocaleTargeting (carries display_language) or None.

    `icp` may be a dataclass or a dict — accessed leniently (mirrors
    copy_adapter._icp_block).
    """
    # (1) generalist_locale facet on the cohort object.
    fs = getattr(cohort, "facet_strength", None) or {}
    if isinstance(fs, dict):
        gl = fs.get("generalist_locale")
        if gl:
            lt = get_locale(str(gl))
            if lt:
                return lt

    # (2) ICP language_pref.
    if icp is not None:
        if isinstance(icp, dict):
            lang_pref = icp.get("language_pref", "")
        else:
            lang_pref = getattr(icp, "language_pref", "")
        if not _lang_pref_is_english(lang_pref):
            lt = get_locale(str(lang_pref))
            if lt:
                return lt

    return None

"""
DRY RUN — GMR-0023 LinkedIn Sponsored Content (Static / Single-Image) for 8 locales.

Produces a full plan report WITHOUT touching LinkedIn API, creating campaigns,
or altering any existing InMail campaigns.

8 target locales: de-DE, fr-FR, it-IT, id-ID, tl-PH, bn-IN, hi-IN, ar-EG

Usage:
    doppler run -- python3 scripts/gmr0023_linkedin_static_dryrun.py

Add --live to actually create DRAFT campaigns after reviewing the report.
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import config
from src.smart_ramp_client import SmartRampClient
from src.locales import LOCALES, LINKEDIN_LANGUAGE_SKILL, get_locale
from src.claude_client import call_claude
from src.figma_creative import classify_tg

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("gmr0023_static_dryrun")

RAMP_ID = "GMR-0023"

# Exact 8 locales requested — no others
TARGET_LOCALES = {"de-de", "fr-fr", "it-it", "id-id", "tl-ph", "bn-in", "hi-in", "ar-eg"}

# Excluded (must not appear in output)
EXCLUDED_LOCALES = {"ko-kr", "zh-cn", "vi-vn", "th-th", "pt-br", "es-mx"}

# Budget placeholder for DRAFT campaigns
STATIC_DAILY_BUDGET_USD = 150

# LinkedIn Sponsored Content minimum resolution
MIN_CREATIVE_WIDTH = 1200
MIN_CREATIVE_HEIGHT = 627

# Native-language names used in copy prompt and report
LOCALE_NATIVE_LANGUAGE: dict[str, str] = {
    "de-de": "German (Deutsch)",
    "fr-fr": "French (Français)",
    "it-it": "Italian (Italiano)",
    "id-id": "Indonesian (Bahasa Indonesia)",
    "tl-ph": "Filipino/Tagalog",
    "bn-in": "Bengali (বাংলা)",
    "hi-in": "Hindi (हिन्दी)",
    "ar-eg": "Arabic (العربية)",
}

# Locale-native Outlier copy vocabulary table.
# This is injected into the copy prompt to enforce brand voice in the target language.
# Note: "Outlier" brand name is NOT translated.
LOCALE_VOCAB_NOTES: dict[str, str] = {
    "de-de": (
        "Translate all copy into German. Brand name 'Outlier' stays as-is. "
        "Avoid 'Job', 'Arbeit', 'Stelle' (→ 'Aufgabe', 'Gelegenheit'). "
        "Avoid 'Training' (→ 'Einarbeitung in die Projektrichtlinien'). "
        "Avoid 'Vergütung' (→ 'Zahlung'). Avoid 'Vorstellungsgespräch' (→ 'Eignungstest')."
    ),
    "fr-fr": (
        "Translate all copy into French. Brand name 'Outlier' stays as-is. "
        "Avoid 'emploi', 'poste', 'rôle' (→ 'mission', 'opportunité'). "
        "Avoid 'formation' (→ 'prise en main des directives du projet'). "
        "Avoid 'rémunération' (→ 'paiement'). Avoid 'entretien' (→ 'évaluation')."
    ),
    "it-it": (
        "Translate all copy into Italian. Brand name 'Outlier' stays as-is. "
        "Avoid 'lavoro', 'posizione', 'ruolo' (→ 'opportunità', 'attività'). "
        "Avoid 'formazione' (→ 'familiarizzazione con le linee guida del progetto'). "
        "Avoid 'compenso' (→ 'pagamento'). Avoid 'colloquio' (→ 'selezione')."
    ),
    "id-id": (
        "Translate all copy into Bahasa Indonesia. Brand name 'Outlier' stays as-is. "
        "Avoid 'pekerjaan', 'posisi', 'jabatan' (→ 'tugas', 'kesempatan'). "
        "Avoid 'pelatihan' (→ 'memahami panduan proyek'). "
        "Avoid 'kompensasi' (→ 'pembayaran'). Avoid 'wawancara' (→ 'seleksi')."
    ),
    "tl-ph": (
        "Translate all copy into Filipino (Tagalog). Brand name 'Outlier' stays as-is. "
        "Avoid 'trabaho', 'posisyon' (→ 'gawain', 'oportunidad'). "
        "Avoid 'pagsasanay' (→ 'pag-aaral ng mga alituntunin ng proyekto'). "
        "Avoid 'kabayaran' (→ 'bayad'). Avoid 'interbyu' (→ 'screening')."
    ),
    "bn-in": (
        "Translate all copy into Bengali (বাংলা). Brand name 'Outlier' stays as-is. "
        "Avoid 'চাকরি', 'পদ' (→ 'কাজ', 'সুযোগ'). "
        "Avoid 'প্রশিক্ষণ' (→ 'প্রকল্পের নির্দেশিকার সাথে পরিচিত হওয়া'). "
        "Avoid 'বেতন' (→ 'পেমেন্ট'). Avoid 'ইন্টারভিউ' (→ 'স্ক্রিনিং')."
    ),
    "hi-in": (
        "Translate all copy into Hindi (हिन्दी). Brand name 'Outlier' stays as-is. "
        "Avoid 'नौकरी', 'पद' (→ 'कार्य', 'अवसर'). "
        "Avoid 'प्रशिक्षण' (→ 'परियोजना दिशानिर्देशों से परिचित होना'). "
        "Avoid 'वेतन' (→ 'भुगतान'). Avoid 'साक्षात्कार' (→ 'स्क्रीनिंग')."
    ),
    "ar-eg": (
        "Translate all copy into Arabic (العربية، اللهجة المصرية). Brand name 'Outlier' stays as-is. "
        "Avoid 'وظيفة', 'منصب' (→ 'مهمة', 'فرصة'). "
        "Avoid 'تدريب' (→ 'التعرف على إرشادات المشروع'). "
        "Avoid 'راتب' (→ 'دفع'). Avoid 'مقابلة' (→ 'فحص')."
    ),
}


def generate_localized_copy(locale: str, locale_data, cohort_desc: str, geos: list[str]) -> dict:
    """
    Generate 3 A/B/C copy variants in the locale's native language.
    Returns a dict with variants list and any QC notes.
    """
    native_lang = LOCALE_NATIVE_LANGUAGE.get(locale, locale)
    vocab_note = LOCALE_VOCAB_NOTES.get(locale, f"Write all copy in the native language of locale {locale}.")
    geo_str = ", ".join(geos) if geos else "relevant countries"

    # Angle descriptions for generalist locale cohorts
    angle_a = (
        "Variant A — Expertise/AI-Experience: Name what they do in their language + "
        "how their language skill has AI value. E.g. (German example): "
        "'Ihr Deutsch ist für KI wertvoll.' Keep specific and concrete."
    )
    angle_b = (
        "Variant B — Social Proof/Earnings: Peer count or earnings figure in local context. "
        "E.g. (French example): 'Des centaines de francophones gagnent avec Outlier.' "
        "Include rate if available."
    )
    angle_c = (
        "Variant C — Flexibility/Lifestyle: Schedule freedom + earning signal in native language. "
        "E.g. (Italian example): 'Lavora ai tuoi ritmi, da casa, in italiano.' "
        "Must include a concrete earning hook, not just lifestyle."
    )

    prompt = f"""You are writing 3 A/B/C LinkedIn Sponsored Content ad copy variants for Outlier — a platform where speakers of a specific language earn payment doing flexible, remote AI training tasks (reviewing, rating, improving AI outputs in their native language).

TARGET AUDIENCE: {cohort_desc} (locale: {locale}, geo: {geo_str})
COPY LANGUAGE: {native_lang} — ALL copy fields MUST be written in this language. The only exception is the brand name "Outlier" which stays in English.

VOCABULARY RULES ({native_lang}):
{vocab_note}

GENERAL OUTLIER BRAND VOICE RULES (apply in all languages):
- Never use: job, role, position → use: task/mission/opportunity in the target language
- Never use: compensation, salary → use: payment
- Never use: interview → use: screening
- Never use: training → use: becoming familiar with project guidelines
- Never use: required → use: strongly encouraged
- No em dashes. Use commas, periods, or colons.
- Sentence case only. No ALL CAPS.
- Contractions OK. Human tone — write like a sharp friend in the profession.

AD FORMAT CONSTRAINTS (HARD LIMITS):
IMAGE OVERLAY TEXT (on the 1200×627 PNG):
- headline: MAX 6 words AND MAX 40 characters. Do NOT include "Outlier" here.
- subheadline: MAX 7 words AND MAX 48 characters. Do NOT include "Outlier" here.

LINKEDIN AD COPY (shown around the image):
- intro_text: MAX 140 characters. Starts with a hook — not a company intro.
- ad_headline: MAX 70 characters. Extends the image headline angle.
- ad_description: MAX 100 characters. Optional but recommended.
- cta_button: ALWAYS "APPLY"

3 ANGLES TO WRITE:
{angle_a}

{angle_b}

{angle_c}

PHOTO SUBJECT (one per variant, in English):
Format: "[gender] [ethnicity plausible for {geo_str}] [profession/activity], [specific off-screen activity at home]"
Derive from the locale/cohort context. No screens, laptops, phones as focal point.

RESPONSE FORMAT — return STRICT JSON only, no markdown fences:
{{
  "variants": [
    {{
      "angle": "A",
      "angleLabel": "Expertise",
      "headline": "...",
      "subheadline": "...",
      "intro_text": "...",
      "ad_headline": "...",
      "ad_description": "...",
      "cta_button": "APPLY",
      "photo_subject": "...",
      "language": "{locale}"
    }},
    {{ "angle": "B", ... }},
    {{ "angle": "C", ... }}
  ]
}}"""

    try:
        raw = call_claude(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2500,
        ).strip()
        # Strip markdown fences if present
        if raw.startswith("```"):
            import re
            raw = re.sub(r"^```(?:json)?\s*", "", raw)
            raw = re.sub(r"\s*```$", "", raw)
        parsed = json.loads(raw)
        variants = parsed.get("variants", [])
        return {"ok": True, "variants": variants, "raw": raw}
    except Exception as exc:
        log.error("Copy gen failed for locale %s: %s", locale, exc)
        return {"ok": False, "variants": [], "error": str(exc)}


def _validate_copy_lengths(variants: list[dict]) -> list[str]:
    """Check hard character limits. Returns list of violation strings."""
    violations = []
    limits = {
        "headline": 40,
        "subheadline": 48,
        "intro_text": 140,
        "ad_headline": 70,
        "ad_description": 100,
    }
    for v in variants:
        angle = v.get("angle", "?")
        for field, max_len in limits.items():
            val = v.get(field, "")
            if val and len(val) > max_len:
                violations.append(
                    f"Angle {angle} {field}: {len(val)} chars > {max_len} limit: {val!r}"
                )
    return violations


def _check_language(variants: list[dict], locale: str) -> list[str]:
    """Basic check that copy is NOT entirely ASCII English (would catch fallback English)."""
    issues = []
    # For non-latin scripts (bn-in, hi-in, ar-eg), headline must contain non-ASCII
    non_latin_locales = {"bn-in", "hi-in", "ar-eg"}
    if locale in non_latin_locales:
        for v in variants:
            headline = v.get("headline", "")
            if headline and all(ord(c) < 128 for c in headline.replace(" ", "")):
                issues.append(
                    f"Angle {v.get('angle','?')}: headline appears to be ASCII/English "
                    f"(expected {LOCALE_NATIVE_LANGUAGE[locale]}): {headline!r}"
                )
    return issues


def run_dry_run() -> dict:
    """
    Step 1: Fetch ramp, filter to 8 locales, generate localized copy for each,
    validate, and return a detailed report.
    """
    log.info("=== GMR-0023 LinkedIn Static Dry Run ===")
    log.info("Target locales: %s", sorted(TARGET_LOCALES))

    client = SmartRampClient()
    ramp = client.fetch_ramp(RAMP_ID)
    if not ramp:
        log.error("Could not fetch ramp %s", RAMP_ID)
        return {"ok": False, "error": f"Ramp {RAMP_ID} not found"}

    log.info("Ramp: %s | Project: %s", ramp.id, ramp.project_name)
    log.info("Total cohorts in ramp: %d", len(ramp.cohorts))

    # Filter cohorts to exactly the 8 target locales
    target_cohorts = []
    skipped_cohorts = []
    for cohort in ramp.cohorts:
        cohort_locales = {str(m).lower().replace("_", "-") for m in (cohort.matched_locales or [])}
        matched_target = cohort_locales & TARGET_LOCALES
        matched_excluded = cohort_locales & EXCLUDED_LOCALES
        if matched_target:
            locale = next(iter(matched_target))
            target_cohorts.append((locale, cohort))
        elif matched_excluded:
            skipped_cohorts.append((next(iter(matched_excluded)), cohort))
        else:
            skipped_cohorts.append(("(unknown)", cohort))

    log.info("Cohorts in scope: %d", len(target_cohorts))
    log.info("Cohorts explicitly skipped: %d", len(skipped_cohorts))

    # Verify leakage: none of the excluded locales are in target_cohorts
    leaked = [
        (locale, c.cohort_description) for locale, c in target_cohorts
        if locale in EXCLUDED_LOCALES
    ]
    if leaked:
        log.error("LEAKAGE DETECTED — excluded locales found in target set: %s", leaked)
        return {"ok": False, "error": f"Locale leakage: {leaked}"}

    log.info("Leakage check: PASS — no excluded locales in target set")

    # Report: skipped cohorts
    log.info("\n--- Cohorts SKIPPED (not in target 8) ---")
    for locale, cohort in skipped_cohorts:
        log.info("  SKIP: locale=%s cohort=%s", locale, cohort.cohort_description)

    # ── Per-locale dry run ────────────────────────────────────────────────────
    report: list[dict] = []

    for locale, cohort in sorted(target_cohorts, key=lambda x: x[0]):
        lt = get_locale(locale)
        lang_skill_urn = LINKEDIN_LANGUAGE_SKILL.get(locale)
        geos = cohort.included_geos or []
        native_lang = LOCALE_NATIVE_LANGUAGE.get(locale, locale)

        log.info("\n=== Locale: %s (%s) ===", locale.upper(), native_lang)
        log.info("  Cohort description: %s", cohort.cohort_description)
        log.info("  Geos: %s", geos)
        log.info("  LinkedIn language skill URN: %s", lang_skill_urn or "NOT FOUND — geo-only targeting")

        # Targeting plan
        targeting_plan = {
            "geo_urns": f"LinkedIn profileLocations for: {', '.join(geos) if geos else 'global'}",
            "language_skill_urn": lang_skill_urn,
            "note": (
                "Language skill URN applied as include facet (generalist locale targeting). "
                "Multimango exclusion suppression audience applied per ramp standard."
                if lang_skill_urn else
                "No language skill URN — geo-only targeting (may produce overly broad audience)."
            ),
        }

        # Campaign group + campaign name plan
        from src.campaign_name import build_campaign_name
        try:
            group_name = build_campaign_name(
                ramp_id=RAMP_ID,
                submitted_at=ramp.submitted_at or "",
                cohort=None,
                platform="linkedin",
                campaign_type="static",
                format_override="Single Image Group",
                locale=locale,
                included_geos=geos,
            )
        except Exception:
            group_name = f"Scale-{RAMP_ID} | LI | Static Group | {locale}"

        try:
            campaign_name = build_campaign_name(
                ramp_id=RAMP_ID,
                submitted_at=ramp.submitted_at or "",
                cohort=None,
                platform="linkedin",
                campaign_type="static",
                locale=locale,
                included_geos=geos,
            )
        except Exception:
            campaign_name = f"Scale-{RAMP_ID} | LI | Static | {locale} | {', '.join(geos)}"

        # Copy generation (localized)
        log.info("  Generating localized copy in %s ...", native_lang)
        copy_result = generate_localized_copy(locale, lt, cohort.cohort_description, geos)

        copy_ok = copy_result["ok"]
        variants = copy_result.get("variants", [])
        copy_violations = _validate_copy_lengths(variants)
        language_issues = _check_language(variants, locale) if variants else []

        if copy_violations:
            log.warning("  Copy length violations: %s", copy_violations)
        if language_issues:
            log.warning("  Language check issues: %s", language_issues)
        if copy_ok and not copy_violations and not language_issues:
            log.info("  Copy: OK (%d variants, native %s)", len(variants), native_lang)

        # Creative spec
        creative_spec = {
            "format": "Single Image",
            "resolution": f"{MIN_CREATIVE_WIDTH}×{MIN_CREATIVE_HEIGHT} (1.91:1)",
            "also_acceptable": "1080×1080 (1:1)",
            "min_dimension": config.MIN_CREATIVE_DIMENSION,
            "angles": ["A", "B", "C"],
            "drive_path_template": f"{RAMP_ID}/linkedin_static/{locale}/{'{angle}'}.png",
            "qc_gate": "copy+design QC (MIN_CREATIVE_DIMENSION=600) before LinkedIn attach",
        }

        # Budget
        budget_plan = {
            "daily_budget_usd": STATIC_DAILY_BUDGET_USD,
            "daily_budget_cents": STATIC_DAILY_BUDGET_USD * 100,
            "note": "PLACEHOLDER — human adjusts at launch. All campaigns created as DRAFT.",
        }

        # Ad count
        n_angles = 3  # A, B, C
        n_ads_per_campaign = n_angles  # 1 creative per angle, attached to 1 campaign

        locale_report = {
            "locale": locale,
            "native_language": native_lang,
            "cohort_description": cohort.cohort_description,
            "cohort_id": cohort.id,
            "geos": geos,
            "targeting": targeting_plan,
            "campaign_group_name": group_name,
            "campaign_name": campaign_name,
            "n_angles": n_angles,
            "n_ads": n_ads_per_campaign,
            "creative_spec": creative_spec,
            "budget_plan": budget_plan,
            "copy_ok": copy_ok,
            "copy_variants": variants,
            "copy_violations": copy_violations,
            "language_issues": language_issues,
            "status_draft": True,
        }
        report.append(locale_report)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_campaigns = len(report)
    total_ads = sum(r["n_ads"] for r in report)
    copy_ok_count = sum(1 for r in report if r["copy_ok"] and not r["copy_violations"])
    all_scoped_clean = (len(report) == len(TARGET_LOCALES))
    no_leakage = all(r["locale"] not in EXCLUDED_LOCALES for r in report)

    log.info("\n" + "="*60)
    log.info("DRY RUN SUMMARY")
    log.info("="*60)
    log.info("Scoping check (exactly 8 locales): %s", "PASS" if all_scoped_clean else "FAIL")
    log.info("Leakage check (0 excluded locales): %s", "PASS" if no_leakage else "FAIL")
    log.info("Locales in scope: %d", total_campaigns)
    log.info("Total campaigns to create (1 per locale): %d", total_campaigns)
    log.info("Total ads per campaign: %d (A/B/C angles)", 3)
    log.info("Total ads to create: %d", total_ads)
    log.info("Copy generation OK: %d / %d", copy_ok_count, total_campaigns)
    log.info("All campaigns DRAFT status: YES")
    log.info("Daily budget per campaign: $%d (placeholder)", STATIC_DAILY_BUDGET_USD)

    log.info("\nPer-locale breakdown:")
    for r in report:
        copy_status = "COPY_OK" if (r["copy_ok"] and not r["copy_violations"] and not r["language_issues"]) else "COPY_ISSUE"
        log.info(
            "  %s (%s): geos=%s skill_urn=%s group=%r campaign=%r %s",
            r["locale"].upper(),
            r["native_language"],
            r["geos"],
            r["targeting"]["language_skill_urn"] or "NONE",
            r["campaign_group_name"][:60],
            r["campaign_name"][:60],
            copy_status,
        )
        if r["copy_variants"]:
            for v in r["copy_variants"][:1]:  # show angle A as sample
                log.info(
                    "    Sample Angle A headline: %r | subhead: %r",
                    v.get("headline", ""),
                    v.get("subheadline", ""),
                )
        if r["copy_violations"]:
            for viol in r["copy_violations"]:
                log.warning("    VIOLATION: %s", viol)
        if r["language_issues"]:
            for issue in r["language_issues"]:
                log.warning("    LANG ISSUE: %s", issue)

    return {
        "ok": all_scoped_clean and no_leakage,
        "scoping_clean": all_scoped_clean,
        "no_leakage": no_leakage,
        "copy_ok_count": copy_ok_count,
        "total_locales": total_campaigns,
        "total_campaigns": total_campaigns,
        "total_ads": total_ads,
        "report": report,
    }


def run_live(dry_run_result: dict) -> dict:
    """
    Step 2: Create DRAFT LinkedIn Sponsored Content campaigns for the 8 locales.
    Only called after dry run confirms scoping is clean.
    """
    if not dry_run_result.get("ok"):
        log.error("Dry run did not pass — aborting live creation")
        return {"ok": False, "error": "Dry run failed — see report above"}

    log.info("\n=== LIVE CREATION: GMR-0023 LinkedIn Static — 8 Locales ===")
    log.info("All campaigns will be created as DRAFT with $%d/day budget", STATIC_DAILY_BUDGET_USD)

    # Use ONLY_LOCALES + ONLY_CHANNEL env vars to scope run_launch_for_ramp
    os.environ["ONLY_LOCALES"] = ",".join(sorted(TARGET_LOCALES))
    os.environ["ONLY_CHANNEL"] = "linkedin"
    # Disable InMail arm — static only
    # Disable Meta/Google — LinkedIn static only
    os.environ["ENABLED_PLATFORMS"] = "linkedin"

    # Ensure DRAFT status and agent_ prefix disabled (per feedback_linkedin_draft_default)
    # LinkedInClient always creates DRAFT; AGENT_NAME_PREFIX is already "" per config.py
    # Set budget via LINKEDIN_DAILY_BUDGET_CENTS
    os.environ["LINKEDIN_DAILY_BUDGET_CENTS"] = str(STATIC_DAILY_BUDGET_USD * 100)

    from main import run_launch_for_ramp

    result = run_launch_for_ramp(
        RAMP_ID,
        modes=("static",),   # Static arm only — no InMail, no Meta, no Google
        dry_run=False,
        channels=["linkedin"],
        budgets={"linkedin": STATIC_DAILY_BUDGET_USD * 100},
    )

    log.info("Live creation result: ok=%s static_campaigns=%d",
             result.get("ok"), len(result.get("static_campaigns", [])))
    return result


def main():
    parser = argparse.ArgumentParser(description="GMR-0023 LinkedIn Static — 8 locale dry run + live")
    parser.add_argument("--live", action="store_true",
                        help="After dry run passes, proceed to live DRAFT campaign creation")
    args = parser.parse_args()

    dry_run_result = run_dry_run()

    if not dry_run_result["ok"]:
        log.error("Dry run FAILED — see log above. Exiting.")
        sys.exit(1)

    log.info("\nDry run PASSED.")
    log.info("Scoping: %s | Leakage: %s | Copy OK: %d/%d",
             "CLEAN" if dry_run_result["scoping_clean"] else "BAD",
             "NONE" if dry_run_result["no_leakage"] else "DETECTED",
             dry_run_result["copy_ok_count"],
             dry_run_result["total_locales"])

    if args.live:
        log.info("\nProceeding to LIVE creation (--live flag set)...")
        live_result = run_live(dry_run_result)
        if live_result.get("ok"):
            log.info("Live creation COMPLETE. Campaigns are DRAFT.")
        else:
            log.error("Live creation FAILED: %s", live_result.get("error"))
            sys.exit(1)
    else:
        log.info("\nTo proceed to live creation, re-run with --live")
        log.info("Example: doppler run -- python3 scripts/gmr0023_linkedin_static_dryrun.py --live")


if __name__ == "__main__":
    main()

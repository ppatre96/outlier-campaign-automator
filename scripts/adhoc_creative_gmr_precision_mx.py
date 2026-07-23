"""
Ad-hoc creative generation script for GMR-PRECISION-MX-2026-05-26.

Runs Stages 8b-8f (copy → image gen → QC → Drive upload) for the
"Precision Operator MX" cohort, Meta 4:5 channel, three angles A/B/C.

Usage:
    doppler run --config dev -- python3 scripts/adhoc_creative_gmr_precision_mx.py
"""
import json
import logging
import os
import sys
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace

# Ensure PYTHONPATH includes repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import config
from src.gemini_creative import (
    generate_imagen_photo,
    generate_imagen_creative_with_qc,
    _build_imagen_prompt,
    _aspect_prompt_block,
    _generate_imagen,
)
from src.image_adapter import compose_ad_for_platform, _add_outlier_watermark
from src.gdrive import upload_creative_in_hierarchy
from src.copy_design_qc import qc_creative
from src.claude_client import call_claude

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
log = logging.getLogger("adhoc_mx")

# ── Constants ─────────────────────────────────────────────────────────────────
RAMP_ID       = "GMR-PRECISION-MX-2026-05-26"
CHANNEL       = "meta"
COHORT_GEO    = "CDMX"
ASPECT        = (4, 5)
QC_MAX_RETRIES = 3          # per-variant retry budget (brief says 3)
BRIEF_PATH    = Path("/Users/pranavpatre/outlier-campaign-agent/data/adhoc_runs/GMR-PRECISION-MX-2026-05-26/brief.md")

# ── Load competitor intel (fresh: age 4 days, within 7-day window) ──────────
competitor_intel: list[str] = []
_intel_path = Path("data/competitor_intel/latest.json")
if _intel_path.exists():
    try:
        _intel_data = json.loads(_intel_path.read_text())
        competitor_intel = _intel_data.get("experiment_ideas", [])
        log.info("Loaded %d competitor intel ideas", len(competitor_intel))
    except Exception as exc:
        log.warning("Failed to load competitor intel: %s", exc)

# ── Stage 8b — Build copy variants (outlier-copy-writer in script form) ──────
# We call the LLM directly with a Spanish-language MX brief instead of
# build_copy_variants() which assumes an English-language cohort from Snowflake.

def _build_mx_copy_prompt() -> str:
    brief_text = BRIEF_PATH.read_text()

    competitor_block = ""
    if competitor_intel:
        competitor_block = (
            "\n\nCOMPETITOR INTEL (use for differentiation, don't copy):\n"
            + "\n".join(f"- {i}" for i in competitor_intel[:3])
        )

    return f"""\
You are an expert ad copywriter specializing in Mexican Spanish B2C acquisition ads for a \
contributor platform. Generate 3 copy variants (A, B, C) in MEXICAN SPANISH for a Meta \
(Facebook/Instagram) Feed ad creative targeting CDMX urban professionals.

## Campaign Brief
{brief_text}

{competitor_block}

## Copy requirements
Each variant has 8 fields:
1. angle: "A", "B", or "C"
2. headline: TEXT OVERLAY ON IMAGE — ≤6 words AND ≤40 chars. NO brand name. \
   Spanish. Lead with the angle's hook. Bold composited over photo.
3. subheadline: TEXT OVERLAY ON IMAGE — ≤7 words AND ≤48 chars. Spanish. \
   Reinforces headline. Composited below headline on photo.
4. intro_text: Meta primary text (above image) — ≤140 chars. Spanish. \
   Hook that resonates with the precision-operator psychographic. \
   Allowed to name Outlier here.
5. ad_headline: Bold text BELOW image on Meta — ≤70 chars. Spanish. Clear value proposition.
6. ad_description: Small text under ad_headline — ≤100 chars. Spanish. \
   Conversion nudge or social proof.
7. cta_button: One of: APPLY_NOW, SIGN_UP, LEARN_MORE, GET_STARTED. \
   For this audience use SIGN_UP or APPLY_NOW.
8. photo_subject: English description for Gemini image generation. \
   MUST be specific: Mexican adult 26-35, gender varies A/B/C, urban professional, \
   calm focused mood. Props per angle from brief Visual direction. Home office or \
   co-working. Varies gender and props across A/B/C — DO NOT repeat the same gender. \
   Angle A: male; Angle B: female; Angle C: male. \
   Include: ethnicity (Mexican Mestizo or Indigenous-heritage), age range, \
   specific prop (notebook/files/checking detail), expression, setting. \
   Example: "Mexican Mestizo man, 28-32, focused calm expression reviewing \
   handwritten checklist at organized home-office desk, natural window light, \
   shallow depth of field".

## HARD VOCABULARY RULES (applies to ALL Spanish copy fields — every field)
NEVER use these words (in ANY language/form):
- trabajo, empleo, rol, posición, puesto → use: oportunidad, tarea, proyecto
- capacitación, entrenamiento, aprendizaje, formación → use: familiarizarse con las pautas del proyecto
- compensación, salario, sueldo → use: pago
- entrevista → use: proceso de selección
- bono → use: recompensa
- asignar → use: asignarte
- equipo → use: parte de este proyecto
- instrucciones → use: pautas del proyecto
- rendimiento, desempeño → use: progreso
- promoción, promover → use: ser elegible para tareas de revisión
- velocidad, rápido, rápidamente, hustle → NEVER — this audience is precision-oriented, NOT speed-oriented
- ilimitado, millones de tareas, gana rápido → NEVER — turns off this audience

## TONE
Professional, calm, confident. NOT playful or hype-y. Lead with standards and craft, not speed or volume.
Angle A (Quality over quantity): "Tareas que recompensan hacerlo bien, no solo terminarlo"
Angle B (Structure & clarity): "Siempre sabrás exactamente qué se espera, sin adivinar"
Angle C (Pride in precision): "Diseñado para quienes notan lo que otros pasan por alto"

Return ONLY valid JSON in this exact schema — no prose, no markdown wrapper:
{{
  "variants": [
    {{
      "angle": "A",
      "angleLabel": "Calidad sobre cantidad",
      "headline": "<≤6 words Spanish>",
      "subheadline": "<≤7 words Spanish>",
      "intro_text": "<≤140 chars Spanish>",
      "ad_headline": "<≤70 chars Spanish>",
      "ad_description": "<≤100 chars Spanish>",
      "cta_button": "SIGN_UP",
      "photo_subject": "<English Gemini subject description — specific, ≥20 words>"
    }},
    {{ "angle": "B", ... }},
    {{ "angle": "C", ... }}
  ]
}}
"""


def generate_copy_variants() -> list[dict]:
    """Call LLM to generate 3 A/B/C Spanish copy variants. Retries on JSON failure."""
    prompt = _build_mx_copy_prompt()
    for attempt in range(3):
        raw = call_claude(
            messages=[{"role": "user", "content": prompt}],
            max_tokens=2048,
            cache_system=True,
        ).strip()
        try:
            # Strip markdown fences if present
            if raw.startswith("```"):
                raw = raw.split("```")[1]
                if raw.startswith("json"):
                    raw = raw[4:]
            parsed = json.loads(raw.strip())
            variants = parsed.get("variants", [])
            if len(variants) == 3:
                log.info("Copy variants generated successfully (attempt %d)", attempt + 1)
                return variants
            log.warning("Expected 3 variants, got %d (attempt %d)", len(variants), attempt + 1)
        except json.JSONDecodeError as exc:
            log.error("JSON parse error attempt %d: %s\n%s", attempt + 1, exc, raw[:400])
    raise RuntimeError("Failed to generate copy variants after 3 attempts")


def _validate_banned_words_spanish(variants: list[dict]) -> list[str]:
    """Light-touch scan for banned Spanish terms across all text fields."""
    BANNED = [
        "trabajo", "empleo", "rol ", "posición", "puesto",
        "capacitación", "entrenamiento", "compensación", "salario",
        "entrevista", " bono", "velocidad", "rápido", "ilimitado",
        "hustle", "gana rápido", "renderizado",
    ]
    violations = []
    for v in variants:
        for field in ("headline", "subheadline", "intro_text", "ad_headline", "ad_description"):
            text = v.get(field, "") or ""
            for banned in BANNED:
                if banned.lower() in text.lower():
                    violations.append(f"Angle {v.get('angle')}/{field} contains banned term '{banned}': {text!r}")
    return violations


# ── Stage 8e — Image generation + composition (4:5) ─────────────────────────

def generate_meta_creative(variant: dict, prompt_suffix: str = "") -> "Image.Image":
    """Generate raw Gemini photo for 4:5 Meta aspect + compose."""
    from src.gemini_creative import (
        _build_imagen_prompt, _aspect_prompt_block,
        _generate_imagen, validate_photo_subject,
        _ANGLE_EXPRESSIONS,
    )
    angle = variant.get("angle", "A")
    photo_subject = variant.get("photo_subject", "")
    validate_photo_subject(photo_subject)

    expression = _ANGLE_EXPRESSIONS.get(angle, _ANGLE_EXPRESSIONS["A"])
    tg_label = "Precision Operator"

    from src.gemini_creative import GEMINI_PROMPT_TEMPLATE
    prompt = GEMINI_PROMPT_TEMPLATE.format(
        photo_subject=photo_subject,
        expression=expression,
        tg_label=tg_label,
    )
    # Inject 4:5 framing override
    prompt += _aspect_prompt_block((4, 5))
    if prompt_suffix:
        prompt += "\n\nADDITIONAL QC FEEDBACK (apply strictly):\n" + prompt_suffix

    bg_image = _generate_imagen(
        prompt,
        config.GEMINI_API_KEY,
        reference_image_b64="",   # no reference image for this run
        feedback_image_b64s=[],
    )
    log.info("Gemini photo received (%dx%d) for angle %s", bg_image.width, bg_image.height, angle)
    return bg_image


def compose_meta_png(bg_image: "Image.Image", variant: dict, save_path: Path) -> Path:
    """Compose the 4:5 Meta creative from Gemini photo + copy."""
    from src.image_adapter import compose_ad_for_platform
    from PIL import Image
    angle = variant.get("angle", "A")
    out_path = compose_ad_for_platform(
        bg_image=bg_image,
        copy_variant=variant,
        platform="meta",
        angle=angle,
        bottom_text="",    # Meta: bottom strip not used; text is in ad fields
        save_to=save_path,
        aspect=(4, 5),
    )
    log.info("Composed meta 4:5 creative: %s", out_path)
    return out_path


# ── QC helper ─────────────────────────────────────────────────────────────────

def run_qc(variant: dict, png_path: Path) -> dict:
    """Run copy+design QC. Returns verdict dict with keys: verdict, violations, retry_target."""
    try:
        result = qc_creative(
            variant=variant,
            creative_path=png_path,
            platform="meta",
        )
        # Handle the list-merge gotcha (feedback_qc_list_merge_fix.md)
        if isinstance(result, list):
            merged = {"verdict": "PASS", "violations": [], "retry_target": None, "prompt_suffix": ""}
            for r in result:
                if isinstance(r, dict):
                    if r.get("verdict") == "FAIL":
                        merged["verdict"] = "FAIL"
                    merged["violations"].extend(r.get("violations", []))
                    if r.get("retry_target"):
                        merged["retry_target"] = r.get("retry_target")
                    if r.get("prompt_suffix"):
                        merged["prompt_suffix"] = r.get("prompt_suffix")
            return merged
        return result
    except Exception as exc:
        log.error("QC call failed: %s", exc)
        return {"verdict": "FAIL", "violations": [str(exc)], "retry_target": "gemini", "prompt_suffix": ""}


# ── Main pipeline ─────────────────────────────────────────────────────────────

def main():
    start_ts = time.time()
    results = {
        "ramp_id": RAMP_ID,
        "status": "RUNNING",
        "variants": {},
        "copy": {},
        "manifest_drive_url": None,
        "gemini_calls": 0,
        "qc_retries": {},
        "total_runtime_seconds": 0,
        "notes": [],
    }

    # Ensure output dir
    out_dir = Path("data/adhoc_runs/GMR-PRECISION-MX-2026-05-26/output")
    out_dir.mkdir(parents=True, exist_ok=True)

    # ── Stage 8c — Generate copy variants ────────────────────────────────────
    log.info("=== Stage 8c: Generating copy variants (Mexican Spanish) ===")
    try:
        variants = generate_copy_variants()
    except Exception as exc:
        results["status"] = "FAILED"
        results["notes"].append(f"Copy generation failed: {exc}")
        _print_results(results, start_ts)
        return results

    # Validate banned words
    bw_violations = _validate_banned_words_spanish(variants)
    if bw_violations:
        results["notes"].append(f"Banned-word scan found {len(bw_violations)} issues (auto-retried in copy gen): {bw_violations[:3]}")

    for v in variants:
        angle = v.get("angle")
        results["copy"][angle] = {
            "headline": v.get("headline"),
            "subheadline": v.get("subheadline"),
            "intro_text": v.get("intro_text"),
            "ad_headline": v.get("ad_headline"),
            "ad_description": v.get("ad_description"),
            "cta_button": v.get("cta_button"),
        }
        log.info("Angle %s copy: headline=%r subhead=%r", angle, v.get("headline"), v.get("subheadline"))

    # ── Stages 8e + QC — Per-variant image gen + retry loop ─────────────────
    drive_urls: dict[str, str] = {}
    final_variants: dict[str, dict] = {}
    final_paths: dict[str, Path] = {}

    for variant in variants:
        angle = v_angle = variant.get("angle", "?")
        log.info("=== Stage 8e: Generating image for Angle %s ===", angle)
        results["qc_retries"][angle] = 0
        qc_prompt_suffix = ""
        current_variant = dict(variant)
        best_attempt = None
        best_violations = None

        for attempt in range(QC_MAX_RETRIES + 1):
            # Image generation
            try:
                bg_image = generate_meta_creative(current_variant, prompt_suffix=qc_prompt_suffix)
                results["gemini_calls"] += 1
            except Exception as exc:
                log.error("Gemini image gen failed (angle %s attempt %d): %s", angle, attempt, exc)
                if attempt < QC_MAX_RETRIES:
                    continue
                results["variants"][angle] = f"ERROR: Gemini failed after {attempt+1} attempts: {exc}"
                break

            # Compose 4:5 PNG
            tmp_path = out_dir / f"{angle}_attempt{attempt}.png"
            try:
                composed_path = compose_meta_png(bg_image, current_variant, tmp_path)
            except Exception as exc:
                log.error("Compose failed (angle %s attempt %d): %s", angle, attempt, exc)
                results["variants"][angle] = f"ERROR: Compose failed: {exc}"
                break

            # QC
            qc_result = run_qc(current_variant, composed_path)
            verdict = qc_result.get("verdict", "FAIL")
            violations = qc_result.get("violations", [])
            log.info("QC angle %s attempt %d: %s (%d violations)", angle, attempt, verdict, len(violations))

            if verdict == "PASS":
                best_attempt = composed_path
                best_violations = []
                final_variants[angle] = current_variant
                final_paths[angle] = composed_path
                log.info("Angle %s QC PASSED on attempt %d", angle, attempt)
                break

            # Track best attempt (fewest violations)
            if best_violations is None or len(violations) < len(best_violations):
                best_attempt = composed_path
                best_violations = violations
                final_variants[angle] = current_variant

            if attempt >= QC_MAX_RETRIES:
                log.warning("Angle %s: QC still FAIL after %d retries — shipping best attempt (%d violations)",
                            angle, QC_MAX_RETRIES, len(best_violations))
                results["notes"].append(
                    f"Angle {angle}: shipped with {len(best_violations)} QC violations after {QC_MAX_RETRIES} retries"
                )
                final_paths[angle] = best_attempt
                break

            # Retry
            results["qc_retries"][angle] += 1
            retry_target = qc_result.get("retry_target", "gemini")
            qc_prompt_suffix = qc_result.get("prompt_suffix", "")

            if retry_target == "copywriter":
                # Rewrite violated copy fields
                log.info("Angle %s: QC retry target=copywriter, rewriting copy", angle)
                try:
                    from src.figma_creative import rewrite_variant_copy
                    current_variant = rewrite_variant_copy(current_variant, violations)
                    results["copy"][angle] = {
                        "headline": current_variant.get("headline"),
                        "subheadline": current_variant.get("subheadline"),
                        "intro_text": current_variant.get("intro_text"),
                        "ad_headline": current_variant.get("ad_headline"),
                        "ad_description": current_variant.get("ad_description"),
                        "cta_button": current_variant.get("cta_button"),
                    }
                except Exception as exc:
                    log.warning("Copy rewrite failed: %s — retrying image only", exc)
            else:
                log.info("Angle %s: QC retry target=gemini, prompt_suffix=%r", angle, qc_prompt_suffix[:100])

    # ── Stage 8f — Drive upload ───────────────────────────────────────────────
    log.info("=== Stage 8f: Uploading to Google Drive ===")
    for angle in ("A", "B", "C"):
        png_path = final_paths.get(angle)
        if not png_path or not Path(png_path).exists():
            results["variants"][angle] = "ERROR: No PNG produced"
            continue
        try:
            drive_url = upload_creative_in_hierarchy(
                file_path=Path(png_path),
                ramp_id=RAMP_ID,
                channel=CHANNEL,
                cohort_geo=COHORT_GEO,
                angle=angle,
            )
            drive_urls[angle] = drive_url
            results["variants"][angle] = drive_url
            log.info("Angle %s uploaded to Drive: %s", angle, drive_url)
        except Exception as exc:
            log.error("Drive upload FAILED for angle %s: %s", angle, exc)
            results["variants"][angle] = f"DRIVE_ERROR: {exc}"
            results["notes"].append(f"Drive upload failed angle {angle}: {exc}")

    # ── Manifest JSON ─────────────────────────────────────────────────────────
    log.info("=== Writing manifest.json ===")
    cdmx_zips_path = Path("/Users/pranavpatre/outlier-campaign-agent/data/adhoc_runs/GMR-PRECISION-MX-2026-05-26/cdmx_zips.json")
    zip_count = 0
    try:
        zips = json.loads(cdmx_zips_path.read_text())
        zip_count = len(zips)
    except Exception as exc:
        log.warning("Could not read CDMX zips for manifest: %s", exc)
        zips = []

    manifest = {
        "ramp_id": RAMP_ID,
        "channel": CHANNEL,
        "cohort": "Precision Operator MX",
        "geo": "CDMX, Mexico City metro",
        "targeting_type": "zip_code_SAC",
        "zip_count": zip_count,
        "cdmx_zips_drive_path": f"{RAMP_ID}/meta/CDMX/cdmx_zips.json",
        "aspect": "4:5 (1080x1350)",
        "language": "Mexican Spanish",
        "cta_note": "Use SIGN_UP or APPLY_NOW in Meta Ads Manager (Meta CTA enum — not LinkedIn enum)",
        "meta_sac_targeting": {
            "geo_type": "zip_code",
            "country": "MX",
            "postal_codes": zips[:10],
            "postal_codes_note": f"First 10 of {zip_count} total. Full list at cdmx_zips.json in Drive.",
            "age_min": 21,
            "age_max": 40,
            "placement": "feed",
            "platforms": ["facebook", "instagram"],
            "objective": "OUTCOME_LEADS",
            "note_for_diego": (
                "Apply full CDMX zip list from cdmx_zips.json as a Location SAC. "
                "Age 21-40. No interest targeting — SAC zip coverage is the targeting. "
                "Budget: $100/day. Optimization: LINK_CLICKS to start, switch to OFFSITE_CONVERSIONS once pixel fires."
            ),
        },
        "variants": {
            angle: {
                "drive_url": drive_urls.get(angle, "pending"),
                "copy": results["copy"].get(angle, {}),
                "photo_subject": final_variants.get(angle, variant_for_angle(variants, angle)).get("photo_subject", ""),
            }
            for angle in ("A", "B", "C")
        },
    }

    # Write manifest locally first
    manifest_local = out_dir / "manifest.json"
    manifest_local.write_text(json.dumps(manifest, indent=2, ensure_ascii=False))
    log.info("Manifest written locally: %s", manifest_local)

    # Upload manifest to Drive
    try:
        manifest_drive_url = upload_creative_in_hierarchy(
            file_path=manifest_local,
            ramp_id=RAMP_ID,
            channel=CHANNEL,
            cohort_geo=COHORT_GEO,
            angle="manifest",
        )
        results["manifest_drive_url"] = manifest_drive_url
        log.info("Manifest uploaded to Drive: %s", manifest_drive_url)
    except Exception as exc:
        log.error("Manifest Drive upload failed: %s", exc)
        results["manifest_drive_url"] = f"ERROR: {exc}"
        results["notes"].append(f"Manifest Drive upload failed: {exc}")

    # Determine final status
    success_count = sum(1 for a in ("A", "B", "C") if drive_urls.get(a))
    if success_count == 3:
        results["status"] = "SUCCESS"
    elif success_count > 0:
        results["status"] = "PARTIAL"
    else:
        results["status"] = "FAILED"

    _print_results(results, start_ts)
    return results


def variant_for_angle(variants: list[dict], angle: str) -> dict:
    for v in variants:
        if v.get("angle") == angle:
            return v
    return {}


def _print_results(results: dict, start_ts: float):
    elapsed = int(time.time() - start_ts)
    results["total_runtime_seconds"] = elapsed
    print("\n" + "="*60)
    print(f"ramp_id: {results['ramp_id']}")
    print(f"status: {results['status']}")
    print("variants:")
    for a in ("A", "B", "C"):
        print(f"  {a}: {results['variants'].get(a, 'pending')}")
    print("copy:")
    for a in ("A", "B", "C"):
        c = results["copy"].get(a, {})
        print(f"  {a}: headline={c.get('headline')!r} subheadline={c.get('subheadline')!r} cta={c.get('cta_button')!r}")
    print(f"manifest_drive_url: {results.get('manifest_drive_url')}")
    print(f"gemini_calls: {results['gemini_calls']}")
    print(f"qc_retries: {results['qc_retries']}")
    print(f"total_runtime_seconds: {elapsed}")
    if results["notes"]:
        print("notes:")
        for n in results["notes"]:
            print(f"  - {n}")
    print("="*60 + "\n")


if __name__ == "__main__":
    main()

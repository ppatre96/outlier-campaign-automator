"""
Targeted creative re-generation for project_id: 69cf1a039ed66cc82e0fa8f3

QC verdict from prior run:
  - Angle A: FAIL — headroom_gap_appropriate (gap TOO FAR)
  - Angle B: FAIL — headroom_gap_appropriate (gap TOO FAR) + gradient_position_correct + subject_looks_ai + professional_quality
  - Angle C: PASS — skip

Changes now in effect (already baked into GEMINI_PROMPT_TEMPLATE and _QC_PROMPT):
  1. 28% hairline placement (was "below 45%" — caused the excessive gap)
  2. Gradient anchored to quadrants: pink TOP-LEFT (x=15%,y=15%), teal BOTTOM-LEFT (x=15%,y=85%)
  3. 13-check QC: added headroom_gap_appropriate + gradient_position_correct

Run plan:
  - Regenerate Angle A and B via direct Gemini API (reference image attached)
  - Max 2 retries per angle; QC-specific suffix fed back on each failure
  - Retry 2: drop reference image if mimicry flagged
  - Save final PNGs to data/project_creatives/ (overwrite existing A/B, preserve C)

Usage:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \
  PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \
  python3 scripts/regen_creatives_69cf1a.py
"""
import json
import logging
import os
import shutil
import sys
from pathlib import Path

# cairosvg / Pillow need Homebrew libs — set before any import
os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import config  # noqa: E402  (must be after dotenv load)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_69cf1a")

# ── Paths ─────────────────────────────────────────────────────────────────────
BRIEF_PATH  = Path("/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_creative_brief.json")
OUTPUT_DIR  = Path("/Users/pranavpatre/outlier-campaign-agent/data/project_creatives")
PROJECT_ID  = "69cf1a039ed66cc82e0fa8f3"

REFERENCE_IMAGE_PATH = Path(
    "/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/"
    "Finance-Branded-BankerMale-Futureproof-1x1.png"
)

# Angles that need regeneration (C passed QC)
ANGLES_TO_REGEN = ["A", "B"]

# ── Separator helpers ─────────────────────────────────────────────────────────
def _sep(char="=", width=70): return char * width


def _log_qc_report(angle: str, attempt: int, report: dict) -> None:
    log.info("  QC result — angle=%s attempt=%d verdict=%s", angle, attempt, report.get("verdict"))
    checks = report.get("checks", {})
    for check_name, passed in checks.items():
        status = "PASS" if passed else "FAIL"
        log.info("    [%s] %s", status, check_name)
    violations = report.get("violations", [])
    if violations:
        log.info("  Violations (%d):", len(violations))
        for v in violations:
            log.info("    - %s", v[:120])


def main() -> None:
    print(_sep())
    print(f"  TARGETED REGEN — project_id={PROJECT_ID}  angles={ANGLES_TO_REGEN}")
    print(_sep())

    # Load brief
    if not BRIEF_PATH.exists():
        print(f"ERROR: Creative brief not found at {BRIEF_PATH}")
        sys.exit(1)
    brief = json.loads(BRIEF_PATH.read_text())
    variants_by_angle = {v["angle"]: v for v in brief["variants"]}
    print(f"Brief loaded: config_name='{brief['config_name']}', cohort='{brief['cohort_name']}'")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    # Resolve reference image path for QC
    ref_path = REFERENCE_IMAGE_PATH if REFERENCE_IMAGE_PATH.exists() else None
    if ref_path:
        print(f"Reference image: {ref_path}")
    else:
        print("WARNING: Reference image not found — QC will run without composition reference")

    # Import generative + QC modules AFTER env is loaded
    from src.gemini_creative import generate_imagen_creative_with_qc

    results: dict[str, dict] = {}

    for angle in ANGLES_TO_REGEN:
        print(f"\n{_sep('-')}")
        print(f"  Angle {angle}")
        print(_sep("-"))

        variant = variants_by_angle.get(angle)
        if not variant:
            print(f"  ERROR: Angle {angle} not found in brief — skipping")
            continue

        photo_subject = variant.get("photo_subject", "")
        headline      = variant.get("headline", "")
        subheadline   = variant.get("subheadline", "")

        print(f"  photo_subject : {photo_subject}")
        print(f"  headline      : {headline}")
        print(f"  subheadline   : {subheadline}")
        print()

        # Run generation + QC + retry loop
        final_path, qc_report = generate_imagen_creative_with_qc(
            variant=variant,
            photo_subject=photo_subject,
            reference_image_path=ref_path,
            max_retries=2,
            copy_rewriter=None,  # copy already within limits per brief
            initial_prompt_suffix="",  # no pre-seeded suffix — let QC drive retries
        )

        _log_qc_report(angle, attempt=1, report=qc_report)

        # Copy to final output path
        out_path = OUTPUT_DIR / f"{PROJECT_ID}_variant_{angle}.png"
        if final_path.exists() and str(final_path) != "/dev/null":
            shutil.copy2(final_path, out_path)
            size_kb = out_path.stat().st_size // 1024
            print(f"  Saved: {out_path}  ({size_kb} KB)")
        else:
            print(f"  WARNING: Generation returned no valid file for angle {angle}")

        results[angle] = {
            "verdict": qc_report.get("verdict"),
            "checks": qc_report.get("checks", {}),
            "violations": qc_report.get("violations", []),
            "path": str(out_path),
        }

    # ── Final report ──────────────────────────────────────────────────────────
    print(f"\n{_sep()}")
    print("REGEN COMPLETE — FINAL QC SUMMARY")
    print(_sep())

    # Angle C was not regenerated — report its known PASS status
    print(f"\n  Angle C: PASS (not regenerated — prior QC passed)")

    for angle in ANGLES_TO_REGEN:
        r = results.get(angle)
        if not r:
            print(f"\n  Angle {angle}: ERROR (skipped)")
            continue

        verdict = r["verdict"]
        print(f"\n  Angle {angle}: {verdict}")
        checks = r["checks"]

        # Key QC items the user specifically cares about
        key_checks = [
            "headroom_gap_appropriate",
            "gradient_position_correct",
            "subject_looks_ai",
            "professional_quality",
            "text_overlaps_subject",
        ]
        # Map label strings back to check keys (checks dict uses label strings)
        label_map = {
            "Headroom gap not too big":  "headroom_gap_appropriate",
            "Gradient matches reference": "gradient_position_correct",
            "Subject looks authentic":    "subject_looks_ai",
            "Professional quality":       "professional_quality",
            "Text doesn't overlap subject": "text_overlaps_subject",
        }
        for label, key in label_map.items():
            passed = checks.get(label, None)
            if passed is None:
                status = "N/A"
            elif passed:
                status = "PASS"
            else:
                status = "FAIL"
            print(f"    [{status}] {key} ({label})")

        if r["violations"]:
            print(f"  Remaining violations ({len(r['violations'])}):")
            for v in r["violations"]:
                print(f"    - {v[:120]}")
        else:
            print(f"  No violations.")

        print(f"  Output: {r['path']}")

    print()
    print("Specific QC items requested:")
    for angle in ANGLES_TO_REGEN:
        r = results.get(angle, {})
        checks = r.get("checks", {})
        gap_pass     = checks.get("Headroom gap not too big", None)
        grad_pass    = checks.get("Gradient matches reference", None)
        print(f"\n  Angle {angle}:")
        print(f"    (a) Gap between headline and hairline ~3-8% : {'PASS' if gap_pass else 'FAIL' if gap_pass is False else 'N/A'}")
        print(f"    (b) Pink in top-left only                   : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'} (part of gradient_position_correct)")
        print(f"    (c) Teal in bottom-left only                : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'} (part of gradient_position_correct)")
        print(f"    (d) Right half neutral                      : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'} (part of gradient_position_correct)")

    print(_sep())


if __name__ == "__main__":
    main()

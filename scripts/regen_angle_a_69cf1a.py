"""
Second pass — Angle A only, project_id: 69cf1a039ed66cc82e0fa8f3

After first regen run:
  - Angle B: PASS (all 13 checks)
  - Angle A: FAIL — gradient_position_correct + edge_border_artefact
    (headroom_gap_appropriate, subject_looks_ai, professional_quality now PASS)

Root cause of persistent gradient failure:
  Gemini keeps spreading the pink/coral wash across the entire left column
  (not just top-left quadrant) and also paints it along the left edge,
  producing the "inner border" artefact. The reference image (Finance Banker)
  has this wider pink spread and seems to be overriding the explicit quadrant
  instructions.

Strategy for this pass:
  - Drop the reference image from attempt 1 (not just as a fallback)
  - Front-load a very hard explicit suffix about the gradient quadrant constraint
  - 3 more attempts (max_retries=2 on top of attempt 0)

Usage:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \
  PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \
  python3 scripts/regen_angle_a_69cf1a.py
"""
import json
import logging
import os
import shutil
import sys
from pathlib import Path

os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

import config  # noqa

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_a_69cf1a")

BRIEF_PATH  = Path("/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_creative_brief.json")
OUTPUT_DIR  = Path("/Users/pranavpatre/outlier-campaign-agent/data/project_creatives")
PROJECT_ID  = "69cf1a039ed66cc82e0fa8f3"

# Front-loaded suffix addressing the exact failure: pink spreading beyond top-left
GRADIENT_SUFFIX = (
    "GRADIENT POSITION — CRITICAL CORRECTION REQUIRED:\n"
    "The previous attempt placed pink/coral wash across the ENTIRE left side of the image (both top-left AND bottom-left quadrants), "
    "and also created a colored stripe along the left frame edge. This is WRONG.\n"
    "CORRECT PLACEMENT:\n"
    "- Pink/coral: ONLY in the TOP-LEFT quadrant, originating at (x=15%, y=15%). It must NOT reach below y=50%. "
    "It must NOT extend to the left edge — it must fade to neutral before the outer 5% of any edge.\n"
    "- Teal/blue: ONLY in the BOTTOM-LEFT quadrant, originating at (x=15%, y=85%). It must NOT rise above y=50%.\n"
    "- The entire BOTTOM-LEFT quadrant must show teal/blue, NOT pink.\n"
    "- The LEFT EDGE of the frame (inner 5%) must show NATURAL PHOTO CONTENT — no colored stripe, no gradient line, no band.\n"
    "- Think of these as two small separate glowing spots — one warm pink dot in the top-left corner, "
    "one cool teal dot in the bottom-left corner — NOT a continuous left-side wash.\n"
    "If you cannot place ONLY a small pink dot in the top-left (separate from teal in bottom-left), "
    "it is better to have NO gradient wash at all than to spread it incorrectly."
)


def _sep(char="=", width=70): return char * width


def main() -> None:
    print(_sep())
    print(f"  ANGLE A TARGETED RETRY — project_id={PROJECT_ID}")
    print(_sep())

    brief = json.loads(BRIEF_PATH.read_text())
    variants_by_angle = {v["angle"]: v for v in brief["variants"]}
    variant = variants_by_angle["A"]

    photo_subject = variant.get("photo_subject", "")
    headline      = variant.get("headline", "")
    subheadline   = variant.get("subheadline", "")

    print(f"  photo_subject : {photo_subject}")
    print(f"  headline      : {headline}")
    print(f"  subheadline   : {subheadline}")
    print(f"  Strategy      : no_reference_image=True (reference was causing gradient spread), gradient suffix front-loaded on all attempts")
    print()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    from src.gemini_creative import generate_imagen_creative_with_qc

    final_path, qc_report = generate_imagen_creative_with_qc(
        variant=variant,
        photo_subject=photo_subject,
        no_reference_image=True,        # drop reference entirely — it was causing gradient spread across full left column
        max_retries=2,
        copy_rewriter=None,
        initial_prompt_suffix=GRADIENT_SUFFIX,
    )

    verdict = qc_report.get("verdict")
    checks  = qc_report.get("checks", {})
    violations = qc_report.get("violations", [])

    print(f"\n  Final verdict: {verdict}")
    print(f"  All 13 checks:")
    for check_label, passed in checks.items():
        print(f"    [{'PASS' if passed else 'FAIL'}] {check_label}")

    if violations:
        print(f"\n  Remaining violations ({len(violations)}):")
        for v in violations:
            print(f"    - {v[:160]}")

    out_path = OUTPUT_DIR / f"{PROJECT_ID}_variant_A.png"
    if final_path.exists() and str(final_path) != "/dev/null":
        shutil.copy2(final_path, out_path)
        size_kb = out_path.stat().st_size // 1024
        print(f"\n  Saved: {out_path}  ({size_kb} KB)")
    else:
        print(f"\n  WARNING: No valid output file generated")

    print(f"\n{_sep()}")
    print("Specific QC items requested:")
    gap_pass  = checks.get("Headroom gap not too big", None)
    grad_pass = checks.get("Gradient matches reference", None)
    print(f"  (a) Gap between headline and hairline ~3-8% : {'PASS' if gap_pass else 'FAIL' if gap_pass is False else 'N/A'}")
    print(f"  (b) Pink in top-left only                   : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'}")
    print(f"  (c) Teal in bottom-left only                : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'}")
    print(f"  (d) Right half neutral                      : {'PASS' if grad_pass else 'FAIL' if grad_pass is False else 'N/A'}")
    print(_sep())


if __name__ == "__main__":
    main()

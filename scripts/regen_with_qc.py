"""
QC-in-the-loop creative regeneration for project 69cf1a039ed66cc82e0fa8f3.

Reads the existing creative brief from data/dry_run_outputs/69cf1a_creative_brief.json,
runs all 3 angles through generate_imagen_creative_with_qc(), and saves finals to
data/project_creatives/.

11-check QC (stricter than previous 7-check version):
  1. Copy within limits (words/chars/lines)
  2. No rendered text in photo
  3. No duplicate logos
  4. Text doesn't overlap subject (hair counts, 1-pixel = FAIL)
  5. No gradient border artefact (thin colored stripes inside white frame)
  6. Photo fills frame (no white gaps inside photo rect)
  7. Logo renders correctly (actual SVG, not Inter Bold fallback)
  8. Subject looks authentic
  9. Subject differs from reference
 10. Text-zone contrast
 11. Professional quality

Prompt enforces:
  - Subject head BELOW top 45% — entire top 40% zero subject pixels
  - Gradient fades COMPLETELY TO NEUTRAL before outer 5% of any edge

Usage:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \
  PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \
  python3 scripts/regen_with_qc.py
"""
import json
import logging
import shutil
import sys
from pathlib import Path
from unittest.mock import patch

# Load .env before any config import
from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

# Set DYLD path for cairosvg (also done inside gemini_creative.py at import time,
# but belt-and-suspenders here in case the env var wasn't inherited from the shell)
import os
if Path("/opt/homebrew/lib").exists():
    os.environ.setdefault("DYLD_FALLBACK_LIBRARY_PATH", "/opt/homebrew/lib")

from src.gemini_creative import generate_imagen_creative_with_qc

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_with_qc")

PROJECT_ID = "69cf1a039ed66cc82e0fa8f3"
BRIEF_PATH = Path("data/dry_run_outputs/69cf1a_creative_brief.json")
OUTPUT_DIR = Path("data/project_creatives")
MAX_RETRIES = 2   # 3 total attempts per variant


def _sep(char="=", width=70):
    return char * width


def main():
    print(_sep())
    print(f"  QC-IN-THE-LOOP CREATIVE REGEN — project {PROJECT_ID}")
    print(_sep())

    if not BRIEF_PATH.exists():
        print(f"ERROR: Creative brief not found at {BRIEF_PATH.resolve()}")
        sys.exit(1)

    brief = json.loads(BRIEF_PATH.read_text())
    variants = brief["variants"]
    print(f"\nLoaded brief: {len(variants)} variants from {BRIEF_PATH}")
    print(f"Cohort : {brief['cohort_name']}")
    print(f"Config : {brief['config_name']}")
    print(f"TG     : {brief['tg_label']}")
    print(f"Output : {OUTPUT_DIR.resolve()}")

    # Find reference image path (used by QC for mimicry check)
    reference_image_path = None
    for candidate in (
        Path("/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
        Path("/Users/pranavpatre/Desktop/Outlier Creatives/Outlier - Static Ads v2/Finance-Branded-BankerMale-Futureproof-1x1.png"),
    ):
        if candidate.exists():
            reference_image_path = candidate
            break
    print(f"Reference image: {reference_image_path or 'NOT FOUND — mimicry check degraded'}")

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

    results = {}   # angle -> {verdict, attempts, violations, retry_instructions, path}

    print(f"\n{_sep('-')}")
    for variant in variants:
        angle = variant["angle"]
        headline = variant["headline"]
        subheadline = variant["subheadline"]
        photo_subject = variant["photo_subject"]

        print(f"\n[Variant {angle}]  {variant['angleLabel']}")
        print(f"  Headline    : {headline!r}")
        print(f"  Subheadline : {subheadline!r}")
        print(f"  Photo subj  : {photo_subject!r}")
        print(f"  Max retries : {MAX_RETRIES} (up to {MAX_RETRIES + 1} total attempts)")

        attempt_log = []   # list of per-attempt QC dicts from the retry loop

        # Instrument qc_creative to capture per-attempt results.
        # generate_imagen_creative_with_qc calls src.copy_design_qc.qc_creative internally —
        # we intercept each call to record its verdict before the loop decides to retry.
        import src.copy_design_qc as _image_qc
        _orig_qc_creative = _image_qc.qc_creative

        def _tracking_qc_creative(creative_path, reference_path, headline, subheadline):
            report = _orig_qc_creative(creative_path, reference_path, headline, subheadline)
            attempt_num = len(attempt_log) + 1
            attempt_log.append({
                "attempt": attempt_num,
                "verdict": report.verdict,
                "violations": list(report.violations),
                "checks": dict(report.checks),
            })
            print(f"    [Attempt {attempt_num}] QC verdict: {report.verdict}"
                  + (f"  violations: {len(report.violations)}" if report.violations else " — all checks passed"))
            return report

        _image_qc.qc_creative = _tracking_qc_creative
        try:
            final_path, qc_report = generate_imagen_creative_with_qc(
                variant=variant,
                photo_subject=photo_subject,
                reference_image_path=reference_image_path,
                max_retries=MAX_RETRIES,
            )
        except Exception as exc:
            print(f"  GENERATION FAILED: {exc}")
            results[angle] = {
                "verdict": "ERROR",
                "error": str(exc),
                "path": None,
                "attempts": len(attempt_log),
            }
            continue
        finally:
            _image_qc.qc_creative = _orig_qc_creative  # restore original

        verdict = qc_report.get("verdict", "UNKNOWN")
        violations = qc_report.get("violations", [])
        checks = qc_report.get("checks", {})
        retry_instruction = qc_report.get("retry_instruction", "")

        # Copy final image to output directory — filename: {project_id}_variant_{angle}.png
        out_filename = f"{PROJECT_ID}_variant_{angle}.png"
        out_path = OUTPUT_DIR / out_filename
        shutil.copy2(final_path, out_path)
        final_path.unlink(missing_ok=True)
        size_kb = out_path.stat().st_size // 1024

        total_attempts = len(attempt_log)
        print(f"\n  Attempts         : {total_attempts} / {MAX_RETRIES + 1}")
        print(f"  Final QC verdict : {verdict}")
        print(f"  Saved to         : {out_path} ({size_kb} KB)")

        # Per-attempt breakdown
        if len(attempt_log) > 1:
            print(f"\n  Per-attempt QC:")
            for att in attempt_log:
                att_v = att["verdict"]
                att_n = att["attempt"]
                if att_v == "PASS":
                    print(f"    Attempt {att_n}: PASS")
                else:
                    viol_summary = ", ".join(
                        k for k, v in att.get("checks", {}).items() if not v
                    )
                    print(f"    Attempt {att_n}: FAIL  [{viol_summary}]")

        # Per-check breakdown (all 11 checks)
        print(f"\n  QC checks ({len(checks)}/11):")
        for check_name, passed in checks.items():
            status = "PASS" if passed else "FAIL"
            print(f"    [{status}]  {check_name}")

        if violations:
            print(f"\n  Violations ({len(violations)}):")
            for v in violations:
                print(f"    - {v}")

        if retry_instruction:
            print(f"\n  Retry instruction applied:")
            for line in retry_instruction.splitlines():
                print(f"    {line}")

        if verdict == "FAIL":
            print(f"\n  *** STILL FAILING AFTER {MAX_RETRIES} RETRIES — DO NOT SHIP TO LINKEDIN ***")
        elif verdict == "PASS":
            print(f"\n  Creative cleared QC — ready for LinkedIn upload.")
        else:
            print(f"\n  QC status UNKNOWN — manual review required before shipping.")

        results[angle] = {
            "verdict": verdict,
            "violations": violations,
            "checks": checks,
            "retry_instruction": retry_instruction,
            "path": str(out_path),
            "size_kb": size_kb,
            "attempts": total_attempts,
            "attempt_log": attempt_log,
        }

        print(_sep("-"))

    # ── Final summary ─────────────────────────────────────────────────────────
    print(f"\n{_sep()}")
    print("REGEN COMPLETE — SUMMARY")
    print(_sep())

    passed = [a for a, r in results.items() if r.get("verdict") == "PASS"]
    failed = [a for a, r in results.items() if r.get("verdict") == "FAIL"]
    unknown = [a for a, r in results.items() if r.get("verdict") not in ("PASS", "FAIL")]

    print(f"  PASS    : {passed or 'none'}")
    print(f"  FAIL    : {failed or 'none'}")
    print(f"  UNKNOWN : {unknown or 'none'}")
    print()

    for angle, r in results.items():
        path = r.get("path") or "NOT GENERATED"
        verdict = r.get("verdict", "ERROR")
        attempts = r.get("attempts", "?")
        size = f"  ({r.get('size_kb', '?')} KB)" if r.get("size_kb") else ""
        print(f"  Variant {angle}: {verdict:<8}  attempts={attempts}  {path}{size}")

    # ── 4 specific confirmation checks ─────────────────────────────────────────
    print()
    print("SPECIFIC CONFIRMATION CHECKS:")
    print(_sep("-"))
    for angle, r in results.items():
        checks = r.get("checks", {})
        # (a) Top 40% no subject pixels — maps to text_overlaps_subject
        c_a = checks.get("Text doesn't overlap subject")
        pa = "PASS" if c_a else ("FAIL" if c_a is False else "UNKNOWN")
        print(f"  (a) Angle {angle}: top 40% has zero subject pixels  [{pa}]")
        # (b) No colored bands on left/right edges
        c_b = checks.get("No gradient border artefact")
        pb = "PASS" if c_b else ("FAIL" if c_b is False else "UNKNOWN")
        print(f"  (b) Angle {angle}: no colored edge bands             [{pb}]")
        # (c) Exactly 1 Outlier logo
        c_c = checks.get("No duplicate logos")
        pc = "PASS" if c_c else ("FAIL" if c_c is False else "UNKNOWN")
        print(f"  (c) Angle {angle}: exactly 1 Outlier logo            [{pc}]")
        # (d) Zero stray text inside photo
        c_d = checks.get("No rendered text in photo")
        pd = "PASS" if c_d else ("FAIL" if c_d is False else "UNKNOWN")
        print(f"  (d) Angle {angle}: zero stray text inside photo      [{pd}]")
        print()

    if failed:
        print()
        print("  BLOCKED — variants still failing QC after all retries:")
        for angle in failed:
            r = results[angle]
            print(f"    Variant {angle}:")
            for v in r.get("violations", []):
                print(f"      - {v}")
        print()
        print("  These variants MUST NOT be submitted to LinkedIn until QC passes.")
        print("  Re-run this script or inspect images manually before proceeding.")

    print()
    print(f"  Output directory: {OUTPUT_DIR.resolve()}")
    print(_sep())

    # Return exit code 1 if any variant failed QC (useful for CI)
    if failed:
        sys.exit(1)


if __name__ == "__main__":
    main()

"""
Regenerate creatives for project_id 69cf1a039ed66cc82e0fa8f3 with the full
QC + copy-rewrite loop.

Usage:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \
    PYTHONPATH=/Users/pranavpatre/outlier-campaign-agent \
    python3 scripts/regen_69cf1a_with_qc.py
"""
import json
import logging
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv
load_dotenv(Path(__file__).parent.parent / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s — %(message)s",
)
log = logging.getLogger("regen_69cf1a")

PROJECT_ID = "69cf1a039ed66cc82e0fa8f3"
BRIEF_PATH = Path("/Users/pranavpatre/outlier-campaign-agent/data/dry_run_outputs/69cf1a_creative_brief.json")
OUT_DIR    = Path("/Users/pranavpatre/outlier-campaign-agent/data/project_creatives")

OUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------------------------------------------------------------------
# 1. Load the cached brief
# ---------------------------------------------------------------------------
log.info("Loading brief from %s", BRIEF_PATH)
brief = json.loads(BRIEF_PATH.read_text())
variants = brief["variants"]
log.info("Loaded %d variants: %s", len(variants), [v["angle"] for v in variants])

# ---------------------------------------------------------------------------
# 2. Pre-flight: validate copy lengths on all 3 variants before any gen call
# ---------------------------------------------------------------------------
from src.copy_design_qc import validate_copy_lengths

log.info("=" * 60)
log.info("PRE-FLIGHT COPY VALIDATION")
log.info("=" * 60)

preflight_results = {}
for v in variants:
    angle = v["angle"]
    headline    = v["headline"]
    subheadline = v["subheadline"]
    violations  = validate_copy_lengths(headline, subheadline)
    preflight_results[angle] = violations
    status = "FAIL" if violations else "PASS"
    log.info("Variant %s — %s", angle, status)
    log.info("  Headline:    %r  (%d words, %d chars)",
             headline, len(headline.replace('\n',' ').split()), len(headline.replace('\n',' ')))
    log.info("  Subheadline: %r  (%d words, %d chars)",
             subheadline, len(subheadline.split()), len(subheadline))
    if violations:
        for viol in violations:
            log.info("  VIOLATION: %s", viol)

# ---------------------------------------------------------------------------
# 3. Run generate_imagen_creative_with_qc() per variant
# ---------------------------------------------------------------------------
from src.gemini_creative import generate_imagen_creative_with_qc
from src.figma_creative  import rewrite_variant_copy

log.info("=" * 60)
log.info("GENERATION + QC LOOP")
log.info("=" * 60)

results = []

for v in variants:
    angle   = v["angle"]
    log.info("----- Variant %s -----", angle)
    log.info("Original headline:    %r", v["headline"])
    log.info("Original subheadline: %r", v["subheadline"])

    original_head = v["headline"]
    original_sub  = v["subheadline"]

    # Track the rewritten copy — we capture it by running the rewriter once
    # ourselves before the QC loop so we always have the final copy available
    # even if generation fails partway through.
    pre_violations = preflight_results[angle]
    if pre_violations:
        log.info("Running pre-flight rewrite for angle %s before gen loop", angle)
        rewritten_v = rewrite_variant_copy(dict(v), pre_violations)
        # Check if rewrite resolved all violations; if not, one more pass
        remaining = validate_copy_lengths(rewritten_v["headline"], rewritten_v["subheadline"])
        if remaining:
            log.info("Second rewrite pass needed for angle %s (still %d violations)", angle, len(remaining))
            rewritten_v = rewrite_variant_copy(rewritten_v, remaining)
        final_head = rewritten_v["headline"]
        final_sub  = rewritten_v["subheadline"]
        # Update v-copy that we pass to gen loop (already-rewritten)
        v_for_gen = dict(rewritten_v)
    else:
        final_head = v["headline"]
        final_sub  = v["subheadline"]
        v_for_gen  = dict(v)

    log.info("Rewritten headline:    %r", final_head)
    log.info("Rewritten subheadline: %r", final_sub)

    final_copy_violations = validate_copy_lengths(final_head, final_sub)
    if final_copy_violations:
        log.warning("Copy still has violations after rewrite: %s", final_copy_violations)
    else:
        log.info("Copy passes all limits after rewrite")

    # --- generation + QC ---------------------------------------------------
    # Angle A had a confirmed bug on the previous run: Gemini painted a fake
    # earnings banner ("Earn $1,600 USD or more monthly, flexible hours, 100%
    # remote") plus a duplicate Outlier logo INSIDE the photo.
    # Front-load the fix as initial_prompt_suffix so the very first generation
    # attempt is already hardened — no wasted retry needed to surface it.
    _ANGLE_A_EARNINGS_SUFFIX = (
        "CRITICAL — previous generation failure: Gemini painted a fake earnings banner "
        "inside the photo with text 'Earn $1,600 USD or more monthly, flexible hours, "
        "100% remote' and also rendered a duplicate Outlier wordmark inside the photo. "
        "DO NOT paint any earnings banner, pricing claim, monetary figure, or Outlier "
        "wordmark inside the photograph. The earnings strip and the Outlier logo are "
        "composited onto the image post-hoc — they must NOT appear anywhere in the "
        "generated photo itself. OUTPUT ONLY THE CLEAN BACKGROUND PHOTOGRAPH."
    )
    initial_suffix = _ANGLE_A_EARNINGS_SUFFIX if angle == "A" else ""

    try:
        final_path, qc_report = generate_imagen_creative_with_qc(
            variant=v_for_gen,
            photo_subject=v.get("photo_subject"),
            max_retries=2,
            copy_rewriter=rewrite_variant_copy,
            initial_prompt_suffix=initial_suffix,
        )
        gen_error = None
    except Exception as exc:
        gen_error = str(exc)
        log.error("Generation failed for variant %s: %s", angle, exc)
        final_path = Path("/dev/null")
        qc_report  = {"verdict": "FAIL", "violations": [f"Generation exception: {exc}"],
                      "retry_target": "none"}

    # --- copy PNG to output dir -------------------------------------------
    dest_name = f"{PROJECT_ID}_variant_{angle}.png"
    dest_path = OUT_DIR / dest_name
    if final_path.exists() and str(final_path) != "/dev/null":
        shutil.copy2(str(final_path), str(dest_path))
        log.info("Saved creative: %s", dest_path)
    else:
        dest_path = Path("/dev/null")
        log.warning("Variant %s: no valid output image", angle)

    result = {
        "angle": angle,
        "original_headline":    original_head,
        "original_subheadline": original_sub,
        "original_violations":  preflight_results[angle],
        "final_headline":       final_head,
        "final_subheadline":    final_sub,
        "final_copy_violations": final_copy_violations,
        "qc_verdict":           qc_report.get("verdict", "UNKNOWN"),
        "qc_violations":        qc_report.get("violations", []),
        "qc_retry_target":      qc_report.get("retry_target", "none"),
        "output_path":          str(dest_path),
        "gen_error":            gen_error,
    }
    results.append(result)
    log.info("Variant %s result: QC=%s copy_ok=%s path=%s",
             angle, result["qc_verdict"],
             "YES" if not final_copy_violations else "NO",
             dest_path)

# ---------------------------------------------------------------------------
# 4. Update the brief file with rewritten copy
# ---------------------------------------------------------------------------
log.info("=" * 60)
log.info("UPDATING BRIEF FILE WITH REWRITTEN COPY")
log.info("=" * 60)

angle_to_result = {r["angle"]: r for r in results}
for v in brief["variants"]:
    r = angle_to_result.get(v["angle"])
    if r:
        old_head = v["headline"]
        old_sub  = v["subheadline"]
        v["headline"]    = r["final_headline"]
        v["subheadline"] = r["final_subheadline"]
        if old_head != v["headline"] or old_sub != v["subheadline"]:
            log.info("Angle %s brief updated: head %r → %r | sub %r → %r",
                     v["angle"], old_head, v["headline"], old_sub, v["subheadline"])
        else:
            log.info("Angle %s brief unchanged (copy was already clean)", v["angle"])

# Update image_paths to point to new output dir
brief["image_paths"] = {
    r["angle"]: r["output_path"] for r in results
}

BRIEF_PATH.write_text(json.dumps(brief, indent=2))
log.info("Brief written back to %s", BRIEF_PATH)

# ---------------------------------------------------------------------------
# 5. Print final report
# ---------------------------------------------------------------------------
print()
print("=" * 70)
print(f"FINAL REPORT — project_id {PROJECT_ID}")
print("=" * 70)

for r in results:
    angle = r["angle"]
    print()
    print(f"--- Variant {angle} ---")
    print(f"  Original headline:    {r['original_headline']!r}")
    print(f"  Original subheadline: {r['original_subheadline']!r}")
    if r["original_violations"]:
        print(f"  Original violations:")
        for v in r["original_violations"]:
            print(f"    - {v}")
    else:
        print(f"  Original: no violations")
    print()
    print(f"  Final headline:    {r['final_headline']!r}")
    print(f"  Final subheadline: {r['final_subheadline']!r}")
    h_plain = r["final_headline"].replace('\n', ' ')
    s_plain = r["final_subheadline"].replace('\n', ' ')
    print(f"  Final counts: headline {len(h_plain.split())}w/{len(h_plain)}c | "
          f"subheadline {len(s_plain.split())}w/{len(s_plain)}c")
    if r["final_copy_violations"]:
        print(f"  Final copy STILL FAILING:")
        for v in r["final_copy_violations"]:
            print(f"    - {v}")
    else:
        print(f"  Final copy: PASS")
    print()
    print(f"  Image QC: {r['qc_verdict']}")
    if r["qc_violations"]:
        for v in r["qc_violations"]:
            print(f"    - {v}")
    if r.get("gen_error"):
        print(f"  Generation error: {r['gen_error']}")
    print(f"  Output path: {r['output_path']}")

print()
print("=" * 70)
all_pass = all(
    r["qc_verdict"] == "PASS" and not r["final_copy_violations"]
    for r in results
)
print(f"Overall result: {'ALL PASS' if all_pass else 'SOME FAILURES — review above'}")
print("=" * 70)

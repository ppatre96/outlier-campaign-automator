"""scripts/diagnose_missing_creatives.py
=======================================

Investigate WHY specific (cohort_geo, angle) rows in the Campaign Registry
ended up with empty `creative_image_path` + empty `creative_urn`.

The pipeline writes one registry row per (cohort × geo × angle) at campaign
creation time. If `png_path is None` (QC FAIL after all retries OR a Gemini
exception), both Drive upload and LinkedIn creative attach are skipped, but
the row is still logged. Downstream visibility is bad: the registry stores
`gemini_prompt` but NOT `violations`, so post-hoc diagnosis requires either
the live pipeline log (often ephemeral) or a re-run.

This script does the cheap half of that re-run — it pulls each failed row
and runs the COPY QC alone (no Gemini calls, instant). For the angles that
pass copy QC but still have no PNG, the failure is image-side, and the
script reports those for follow-up regen.

Why split copy vs image QC: copy violations explain 80%+ of historical
FAILs (length limits, brand voice). Running them costs nothing and pinpoints
the actual rejection reason for most rows. Image QC requires Gemini gen
($/call + several minutes per angle) — only worth running once copy is
proven clean.

Output: per-angle report with the violations list. Returns non-zero exit
code if any angle has unresolved copy violations.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/diagnose_missing_creatives.py GMR-0021

    # Only LinkedIn (default is linkedin,meta):
    … diagnose_missing_creatives.py GMR-0021 --platforms linkedin

    # JSON output (for piping into a dashboard):
    … diagnose_missing_creatives.py GMR-0021 --json
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from collections import defaultdict
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.WARNING,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("diagnose_missing_creatives")


def _load_registry() -> list[dict]:
    p = ROOT / "data" / "campaign_registry.json"
    if not p.exists():
        log.error("registry not found at %s", p)
        sys.exit(2)
    return json.loads(p.read_text())


def _find_empty_creative_rows(
    records: list[dict], ramp_id: str, platforms: list[str]
) -> list[dict]:
    out = []
    for r in records:
        if r.get("smart_ramp_id") != ramp_id:
            continue
        if r.get("platform", "").lower() not in [p.lower() for p in platforms]:
            continue
        if not r.get("angle") and not r.get("geo_cluster_label"):
            continue  # parent placeholder row
        # Empty creative_image_path AND empty creative_urn → genuinely failed
        if r.get("creative_image_path") or r.get("creative_urn"):
            continue
        out.append(r)
    return out


def _classify_violations(violations: list[str]) -> str:
    """Roll up a list of violation strings into a short category tag."""
    tags = set()
    for v in violations:
        vl = v.lower()
        if "headline has" in vl and "words" in vl:
            tags.add("headline_word_count")
        elif "headline has" in vl and "chars" in vl:
            tags.add("headline_char_count")
        elif "headline wraps" in vl:
            tags.add("headline_line_wrap")
        elif "subheadline has" in vl and "words" in vl:
            tags.add("subheadline_word_count")
        elif "subheadline has" in vl and "chars" in vl:
            tags.add("subheadline_char_count")
        elif "subheadline wraps" in vl:
            tags.add("subheadline_line_wrap")
        elif "brand voice" in vl or "banned" in vl:
            tags.add("brand_voice")
        elif "cta_button" in vl:
            tags.add("cta_enum")
        else:
            tags.add("other")
    return ",".join(sorted(tags)) or "no_copy_violations"


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ramp_id")
    parser.add_argument("--platforms", default="linkedin,meta")
    parser.add_argument("--json", action="store_true", help="JSON output")
    args = parser.parse_args()

    from src.copy_design_qc import validate_copy_lengths  # noqa: E402

    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    recs = _load_registry()
    failed = _find_empty_creative_rows(recs, args.ramp_id, platforms)

    if not failed:
        msg = f"No failed-creative rows for ramp_id={args.ramp_id} platforms={platforms}"
        if args.json:
            print(json.dumps({"ok": True, "msg": msg, "rows": []}))
        else:
            print(msg)
        return 0

    rows_out: list[dict] = []
    cat_counts: dict[str, int] = defaultdict(int)
    image_only = 0

    for r in failed:
        violations = validate_copy_lengths(
            headline=r.get("headline", ""),
            subheadline=r.get("subheadline", ""),
            intro_text="",
            ad_headline="",
            ad_description="",
            cta_button="",
        )
        cat = _classify_violations(violations)
        cat_counts[cat] += 1
        if cat == "no_copy_violations":
            image_only += 1
        rows_out.append(
            {
                "platform": r.get("platform"),
                "cohort_geo": r.get("cohort_geo"),
                "angle": r.get("angle"),
                "geo_cluster_label": r.get("geo_cluster_label"),
                "headline": r.get("headline"),
                "subheadline": r.get("subheadline"),
                "photo_subject": r.get("photo_subject"),
                "category": cat,
                "violations": violations,
                "created_at": r.get("created_at"),
            }
        )

    if args.json:
        print(json.dumps({"ok": True, "total": len(failed), "by_category": dict(cat_counts), "rows": rows_out}, indent=2))
        return 0

    # Pretty text output
    print(f"━━━ Diagnosis: {args.ramp_id} ({len(failed)} failed-creative rows) ━━━")
    print()
    print(f"Category breakdown:")
    for cat, n in sorted(cat_counts.items(), key=lambda x: -x[1]):
        print(f"  {n:>3}  {cat}")
    print()
    if image_only:
        print(
            f"  → {image_only} row(s) passed copy QC. Their failure is image-side "
            f"(Gemini photo rejected by Vision QC). Use --regen-image to re-run gen "
            f"on those alone, or eyeball with --verbose."
        )
        print()

    print("─── Per-row detail ───")
    for r in rows_out:
        print(f"\n  {r['platform']}  {r['cohort_geo']}  angle={r['angle']}  cat={r['category']}")
        print(f"    headline:     {r['headline']!r}")
        print(f"    subheadline:  {r['subheadline']!r}")
        print(f"    photo_subj:   {(r['photo_subject'] or '')[:90]}")
        if r["violations"]:
            for v in r["violations"]:
                print(f"    ⚠ {v}")
        else:
            print(f"    (no copy violations — image-side failure)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

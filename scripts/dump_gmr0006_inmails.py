"""
One-off: regenerate the 5 InMail variants from the GMR-0006 dry-run so we can
inspect the full bodies (the live run's log only kept the first 100 chars).

Builds Cohort objects matching what Stage A produced for GMR-0006, then calls
build_inmail_variants for each. Dumps a markdown report to stdout + a JSON
file for posterity.
"""
from __future__ import annotations
import json
import sys
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent.parent
load_dotenv(ROOT / ".env")
sys.path.insert(0, str(ROOT))

from src.analysis import Cohort  # noqa: E402
from src.inmail_copy_writer import build_inmail_variants  # noqa: E402
from main import classify_tg  # noqa: E402


# Mirrors the 5 cohorts Stage A surfaced for GMR-0006 + which angle was selected.
GMR_0006_COHORTS = [
    {
        "rules": [("job_titles_norm__cardiologist", True), ("experience_band__10plus", True)],
        "selected_angle": "A",
    },
    {
        "rules": [("job_titles_norm__cardiologist", True), ("fields_of_study__medicine", True)],
        "selected_angle": "B",
    },
    {
        "rules": [("job_titles_norm__cardiologist", True), ("skills__cardiology", True)],
        "selected_angle": "C",
    },
    {
        "rules": [("job_titles_norm__cardiologist", True)],
        "selected_angle": "A",
    },
    {
        "rules": [("job_titles_norm__medical_doctor", True), ("fields_of_study__medicine", True)],
        "selected_angle": "B",
    },
]


def main() -> None:
    import os
    claude_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("CLAUDE_API_KEY") or ""
    out: list[dict] = []
    for i, spec in enumerate(GMR_0006_COHORTS):
        rules = spec["rules"]
        cohort = Cohort(name=" + ".join(r[0] for r in rules), rules=rules)
        tg_cat = classify_tg(cohort.name, cohort.rules)
        print(f"--- cohort {i} (tg={tg_cat}) ---", file=sys.stderr)
        variants = build_inmail_variants(tg_cat, cohort, claude_key)
        for v in variants:
            angle = getattr(v, "angle", "?")
            out.append({
                "cohort_index": i,
                "cohort_signature": cohort.name,
                "tg_cat": tg_cat,
                "angle": angle,
                "selected": angle == spec["selected_angle"],
                "subject": getattr(v, "subject", ""),
                "body": getattr(v, "body", ""),
                "cta": getattr(v, "cta", ""),
            })
    dump_path = ROOT / "data/dry_run_outputs/GMR-0006-WITHIMAGES-20260504/inmail_variants.json"
    dump_path.parent.mkdir(parents=True, exist_ok=True)
    dump_path.write_text(json.dumps(out, indent=2))
    print(f"\nDumped {len(out)} variants to {dump_path}", file=sys.stderr)


if __name__ == "__main__":
    main()

"""
Phase 3.1 validation harness — measure real Gemini image-gen wall-clock at
IMAGE_GEN_CONCURRENCY=1 vs IMAGE_GEN_CONCURRENCY=4 for the static arm.

Calls _process_static_campaigns with dry_run=True (so LinkedIn API is
skipped) + WITH_IMAGES=1 (so Gemini is still hit). One cohort × one geo
group × N angles → N real Gemini calls, each potentially looping on QC
reroll. No LinkedIn campaigns are created. No registry rows are written.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/bench_image_gen_concurrency.py

Output: per-run wall-clock, per-task png_path / qc_verdict, headline
ratio (sequential / parallel) with cost breakdown.

Cost estimate: N angles × ~1.5 attempts (avg with 43% reject rate) ×
$0.04/image. Default N=3 angles × 2 runs = ~$0.36.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

# Cap to 1 cohort × 1 geo group × N angles BEFORE config loads.
N_ANGLES = int(os.environ.get("BENCH_ANGLES", "3"))
os.environ.setdefault("MAX_COHORTS_PER_GEO_CLUSTER", "1")
os.environ.setdefault("ANGLES_PER_COHORT",           str(N_ANGLES))
os.environ.setdefault("WITH_IMAGES",                 "1")

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bench")

import config             # noqa: E402
import main as M          # noqa: E402
from src.analysis import Cohort  # noqa: E402


def _make_cohort() -> Cohort:
    """Hand-built cardiologist cohort, mirrors test_cardiologist_3channel."""
    c = Cohort(
        name="job_titles_norm__cardiologist + experience_band__10plus",
        rules=[
            ("job_titles_norm__cardiologist", "cardiologist"),
            ("experience_band__10plus",        "10plus"),
        ],
        n=200, passes=20, pass_rate=10.0, lift_pp=4.0,
        support=15, coverage=0.05,
    )
    c._stg_id   = "STG-BENCH-CARDIO"
    c._stg_name = "Bench Cardio"
    c.id        = "bench-cardio"
    c.cohort_description = "Senior cardiologists with 10+ years"
    return c


def _run_once(label: str, concurrency: int) -> dict:
    """Run _process_static_campaigns once with the given concurrency."""
    config.IMAGE_GEN_CONCURRENCY = concurrency

    cohort = _make_cohort()
    sheets    = MagicMock()
    li_client = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

    log.info("=" * 72)
    log.info(" RUN: %s — concurrency=%d, angles=%d", label, concurrency, N_ANGLES)
    log.info("=" * 72)

    t0 = time.monotonic()
    out = M._process_static_campaigns(
        [cohort],
        flow_id="BENCH-CARDIO",
        location="United States",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        figma_file="",
        figma_node="",
        included_geos=["US"],
        ramp_id="BENCH-CARDIO",
        cohort_id_override=cohort.id,
        cohort_description=cohort.cohort_description,
        base_rate_usd=50.0,
        dry_run=True,
    )
    elapsed = time.monotonic() - t0

    specs = out.get("campaign_specs", [])
    n_with_image = sum(1 for s in specs if s.get("png_path") is not None)
    n_pass       = sum(1 for s in specs if (s.get("qc_report") or {}).get("verdict") == "PASS")
    n_fail       = sum(1 for s in specs if (s.get("qc_report") or {}).get("verdict") == "FAIL")
    attempts_total = sum((s.get("qc_report") or {}).get("attempts", 1) for s in specs)

    log.info("RESULT: %s elapsed=%.1fs specs=%d with_image=%d PASS=%d FAIL=%d total_attempts=%d",
             label, elapsed, len(specs), n_with_image, n_pass, n_fail, attempts_total)
    return {
        "label":          label,
        "concurrency":    concurrency,
        "elapsed_sec":    elapsed,
        "specs":          len(specs),
        "with_image":     n_with_image,
        "pass":           n_pass,
        "fail":           n_fail,
        "total_attempts": attempts_total,
    }


def main_run() -> int:
    if not os.environ.get("LITELLM_API_KEY") and not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: neither LITELLM_API_KEY nor ANTHROPIC_API_KEY in env. "
              "Run under `doppler run --`.")
        return 2

    seq = _run_once("sequential", concurrency=1)
    par = _run_once("parallel",   concurrency=4)

    print()
    print("=" * 72)
    print(" SUMMARY")
    print("=" * 72)
    print(f"{'Run':<14} {'workers':<8} {'wall-clock':<12} {'specs':<6} {'with_image':<11} {'PASS':<5} {'FAIL':<5} {'attempts':<9}")
    for r in (seq, par):
        print(f"{r['label']:<14} {r['concurrency']:<8} "
              f"{r['elapsed_sec']:>9.1f}s   "
              f"{r['specs']:<6} {r['with_image']:<11} "
              f"{r['pass']:<5} {r['fail']:<5} {r['total_attempts']:<9}")
    if seq["elapsed_sec"] > 0:
        speedup = seq["elapsed_sec"] / max(par["elapsed_sec"], 0.001)
        print()
        print(f"SPEEDUP: {speedup:.2f}x  (sequential {seq['elapsed_sec']:.1f}s → parallel {par['elapsed_sec']:.1f}s)")
        if par["with_image"] != seq["with_image"]:
            print(f"WARN: image-success counts differ — seq={seq['with_image']} par={par['with_image']}. "
                  f"Likely transient Gemini rate-limit or QC stochasticity.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main_run())

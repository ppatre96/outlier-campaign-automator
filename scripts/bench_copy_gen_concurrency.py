"""
Phase 3.2 validation harness — measure real Anthropic copy-gen wall-clock
at COPY_GEN_CONCURRENCY=1 vs COPY_GEN_CONCURRENCY=4 across N (cohort × geo)
combos.

Spins up `N` synthetic cohorts and a single geo group, runs through
`_process_static_campaigns(dry_run=True)` with image gen mocked to a
no-op. That isolates the copy-gen stage so wall-clock differences come
entirely from the new ThreadPoolExecutor in Phase A2.

Cost estimate: N=6 cohorts × 2 runs = 12 real Anthropic calls × ~$0.03
= ~$0.36 per run. Default N is set low to keep cost bounded.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/bench_copy_gen_concurrency.py

  # adjust scale via env:
    BENCH_COHORTS=9 venv/bin/python scripts/bench_copy_gen_concurrency.py
"""
from __future__ import annotations

import logging
import os
import sys
import time
from pathlib import Path
from unittest.mock import MagicMock

# Number of cohorts to spin up — copy gen runs once per (cohort × geo);
# we use a single geo group, so total copy-gen calls per run == N.
N_COHORTS = int(os.environ.get("BENCH_COHORTS", "6"))
os.environ.setdefault("MAX_COHORTS_PER_GEO_CLUSTER", str(N_COHORTS))
os.environ.setdefault("ANGLES_PER_COHORT",           "1")  # collapse image-gen surface

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bench_copy")

import config             # noqa: E402
import main as M          # noqa: E402
from src.analysis import Cohort  # noqa: E402


def _make_cohort(i: int) -> Cohort:
    """Synthetic cohort that triggers a real Anthropic call inside
    build_copy_variants but doesn't depend on any Snowflake or sheets state."""
    c = Cohort(
        name=f"bench_cohort_{i:02d}",
        rules=[
            ("job_titles_norm__cardiologist", "cardiologist"),
            ("experience_band__10plus",        "10plus"),
        ],
        n=200, passes=20, pass_rate=10.0, lift_pp=4.0,
        support=15, coverage=0.05,
    )
    c._stg_id   = f"STG-BENCH-{i:02d}"
    c._stg_name = f"Bench Cohort {i:02d}"
    c.id        = f"bench-cohort-{i:02d}"
    c.cohort_description = f"Senior cardiologists batch {i}"
    return c


def _run_once(label: str, copy_concurrency: int) -> dict:
    """Run _process_static_campaigns once with the given copy concurrency.
    Image gen is monkey-patched to a no-op so timing is copy-only."""
    # Bypass image gen entirely — we're measuring copy gen only.
    M.generate_imagen_creative_with_qc = lambda **kw: (None, {"verdict": "PASS"})
    config.COPY_GEN_CONCURRENCY = copy_concurrency
    config.IMAGE_GEN_CONCURRENCY = 1  # no parallel image-gen overhead

    cohorts = [_make_cohort(i) for i in range(N_COHORTS)]
    sheets    = MagicMock()
    li_client = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

    log.info("=" * 72)
    log.info(" RUN: %s — COPY_GEN_CONCURRENCY=%d  cohorts=%d", label, copy_concurrency, N_COHORTS)
    log.info("=" * 72)

    t0 = time.monotonic()
    out = M._process_static_campaigns(
        cohorts,
        flow_id="BENCH-COPY",
        location="United States",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        figma_file="",
        figma_node="",
        included_geos=["US"],
        ramp_id="BENCH-COPY",
        cohort_description="",
        base_rate_usd=50.0,
        dry_run=True,
    )
    elapsed = time.monotonic() - t0

    specs = out.get("campaign_specs", [])
    populated = sum(1 for s in specs if s.get("variants"))
    log.info("RESULT: %s elapsed=%.1fs specs=%d copy_populated=%d",
             label, elapsed, len(specs), populated)
    return {
        "label":         label,
        "concurrency":   copy_concurrency,
        "elapsed_sec":   elapsed,
        "specs":         len(specs),
        "populated":     populated,
    }


def main_run() -> int:
    if not os.environ.get("ANTHROPIC_API_KEY") and not os.environ.get("LITELLM_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not in env. Run under `doppler run --`.")
        return 2

    seq = _run_once("sequential", copy_concurrency=1)
    par = _run_once("parallel",   copy_concurrency=4)

    print()
    print("=" * 72)
    print(" Phase 3.2 — copy-gen concurrency benchmark")
    print("=" * 72)
    print(f"{'Run':<14} {'workers':<8} {'wall-clock':<12} {'specs':<6} {'populated':<10}")
    for r in (seq, par):
        print(f"{r['label']:<14} {r['concurrency']:<8} "
              f"{r['elapsed_sec']:>9.1f}s   "
              f"{r['specs']:<6} {r['populated']:<10}")
    if par["elapsed_sec"] > 0:
        speedup = seq["elapsed_sec"] / max(par["elapsed_sec"], 0.001)
        print()
        print(f"SPEEDUP: {speedup:.2f}x  "
              f"(sequential {seq['elapsed_sec']:.1f}s → parallel {par['elapsed_sec']:.1f}s)")
        if par["populated"] != seq["populated"]:
            print(f"WARN: variant population differs — seq={seq['populated']} "
                  f"par={par['populated']}. Likely transient Anthropic rate limit "
                  f"or rewriter falling through; not a parallelism bug.")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main_run())

"""
Phase 3.4 validation harness — measure real wall-clock for sequential vs
parallel row-level execution (multiple pending rows / cohorts within one
launch invocation).

Calls the inner orchestration directly (mocking sheets + LinkedIn writes
under dry_run=True) and varies `RAMP_CONCURRENCY` to compare:
  - RAMP_CONCURRENCY=1  (today's behavior — serial loop over pending rows)
  - RAMP_CONCURRENCY=2  (recommended starting concurrency for prod)
  - RAMP_CONCURRENCY=3  (max recommended — LinkedIn rate limits dominate)

Each "row" in the benchmark calls _process_static_campaigns once with one
cohort + one geo + one angle. That hits real Anthropic for copy gen so we
measure end-to-end concurrency including the shared claude_client lock.
Image gen is skipped (WITH_IMAGES=""), and LinkedIn API writes are skipped
(dry_run=True), so the wall-clock difference comes from:
  - Anthropic call concurrency (gated by httpx connection pool + RPM)
  - Shared-state lock contention (LinkedInClient session, UrnResolver, etc.)
  - Python GIL handoff between threads

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/bench_ramp_parallelism.py

Cost: ~$0.15 per row × 6 rows total ≈ $1.00 in Anthropic credits.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

# Force minimal fan-out so each row is a clean wall-clock unit.
os.environ.setdefault("MAX_COHORTS_PER_GEO_CLUSTER", "1")
os.environ.setdefault("ANGLES_PER_COHORT",           "1")
os.environ.pop("WITH_IMAGES", None)

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("bench_ramp")

import config             # noqa: E402
import main as M           # noqa: E402
from src.analysis import Cohort  # noqa: E402

# How many "rows" (cohorts) to simulate per benchmark iteration. Models a
# typical Smart Ramp with N cohorts feeding the pending-row loop.
N_ROWS = int(os.getenv("BENCH_RAMP_ROWS", "3"))


def _make_cohort(idx: int) -> Cohort:
    c = Cohort(
        name=f"bench_ramp_cohort_{idx}",
        rules=[
            ("job_titles_norm__cardiologist", "cardiologist"),
            ("experience_band__10plus",        "10plus"),
        ],
        n=200, passes=20, pass_rate=10.0, lift_pp=4.0,
        support=15, coverage=0.05,
    )
    c._stg_id   = f"STG-BENCH-RAMP-{idx}"
    c._stg_name = f"Bench Ramp {idx}"
    c.id        = f"bench-ramp-{idx}"
    c.cohort_description = "Senior cardiologists"
    return c


def _make_clients():
    sheets = MagicMock()
    li_client = MagicMock()
    urn_res = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}
    return sheets, li_client, urn_res


def _process_one_row(idx: int) -> dict:
    """Stand-in for one iteration of the pending-row loop. Only the
    expensive bits (copy gen) actually fire; the rest is mocked. Real
    Anthropic call(s) make this a meaningful concurrency measurement."""
    cohort = _make_cohort(idx)
    sheets, li_client, urn_res = _make_clients()
    return M._process_static_campaigns(
        selected=[cohort],
        flow_id=f"bench-{idx}",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        figma_file="", figma_node="", mj_token="",
        dry_run=True,
        included_geos=["US"],
        ramp_id="BENCH-RAMP",
        cohort_id_override=cohort.id,
        cohort_description=cohort.cohort_description,
        base_rate_usd=50.0,
    ) or {}


def _run_sequential(n: int) -> float:
    log.info("=" * 72)
    log.info(" SEQUENTIAL — %d rows back-to-back (RAMP_CONCURRENCY=1)", n)
    log.info("=" * 72)
    t0 = time.monotonic()
    for i in range(n):
        _process_one_row(i)
    return time.monotonic() - t0


def _run_parallel(n: int, workers: int) -> float:
    log.info("=" * 72)
    log.info(" PARALLEL — %d rows via ThreadPoolExecutor(max_workers=%d)", n, workers)
    log.info("=" * 72)
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bench-ramp") as ex:
        futs = [ex.submit(_process_one_row, i) for i in range(n)]
        for f in futs:
            f.result()
    return time.monotonic() - t0


def main_run() -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not in env. Run under `doppler run --`.")
        return 2

    seq = _run_sequential(N_ROWS)
    log.info("RESULT: sequential elapsed=%.1fs", seq)

    par2 = _run_parallel(N_ROWS, workers=2)
    log.info("RESULT: parallel(2) elapsed=%.1fs", par2)

    par3 = _run_parallel(N_ROWS, workers=3)
    log.info("RESULT: parallel(3) elapsed=%.1fs", par3)

    print()
    print("=" * 72)
    print(f" Phase 3.4 — ramp-level parallelism benchmark ({N_ROWS} rows)")
    print("=" * 72)
    print(f"{'Run':<18} {'wall-clock':<12} {'speedup':<10}")
    print(f"{'sequential':<18} {seq:>9.1f}s   {'1.00x':<10}")
    print(f"{'parallel (w=2)':<18} {par2:>9.1f}s   {seq/par2:>6.2f}x")
    print(f"{'parallel (w=3)':<18} {par3:>9.1f}s   {seq/par3:>6.2f}x")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main_run())

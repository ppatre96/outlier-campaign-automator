"""
Phase 3.3 validation harness — measure real Anthropic + LinkedIn-stubbed
wall-clock for sequential vs parallel arm execution.

Calls _process_inmail_campaigns and _process_static_campaigns directly
with dry_run=True (skips LinkedIn API writes) and WITH_IMAGES="" (skips
Gemini image gen). Both arms still execute real Anthropic LLM calls:
  - Static arm:  build_copy_variants per (cohort × geo) → 1 Anthropic call
  - InMail arm:  build_inmail_variants per (cohort × geo) → 1 Anthropic call
Wall-clock differences come entirely from the arm-level executor
introduced by Phase 3.3.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/bench_arms_parallelism.py

Cost: ~$0.30 in Anthropic credits per arm pair × 2 runs ≈ $0.60.
"""
from __future__ import annotations

import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from unittest.mock import MagicMock

# Cap to 1 cohort × 1 geo × 1 angle so we measure pure arm-level
# parallelism, not within-arm fan-out (which Phases 3.1+3.2 cover).
os.environ.setdefault("MAX_COHORTS_PER_GEO_CLUSTER", "1")
os.environ.setdefault("ANGLES_PER_COHORT",           "1")
# Skip Gemini image gen — both arms still do real Anthropic copy.
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
log = logging.getLogger("bench_arms")

import config            # noqa: E402
import main as M          # noqa: E402
from src.analysis import Cohort  # noqa: E402


def _make_cohort() -> Cohort:
    c = Cohort(
        name="bench_arms_cardiologist",
        rules=[
            ("job_titles_norm__cardiologist", "cardiologist"),
            ("experience_band__10plus",        "10plus"),
        ],
        n=200, passes=20, pass_rate=10.0, lift_pp=4.0,
        support=15, coverage=0.05,
    )
    c._stg_id   = "STG-BENCH-ARMS"
    c._stg_name = "Bench Arms"
    c.id        = "bench-arms"
    c.cohort_description = "Senior cardiologists"
    return c


def _make_clients():
    """Mock clients — neither arm writes to LinkedIn under dry_run=True,
    but both arms still call read methods on the clients during setup."""
    sheets = MagicMock()
    li_client = MagicMock()
    urn_res = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}
    return sheets, li_client, urn_res


def _stub_brand_voice():
    """Pass-through validator — never flags violations. Returns a report
    with real numeric/list attrs so the InMail arm's log.info f-strings
    don't TypeError on MagicMock formatting."""
    from types import SimpleNamespace
    bv = MagicMock()
    bv.validate_copy.return_value = SimpleNamespace(
        passed=True, is_compliant=True, violations=[],
        confidence_score=0.95, suggested_fixes=[], notes="",
    )
    return bv


def _call_inmail(cohort) -> dict:
    sheets, li_client, urn_res = _make_clients()
    return M._process_inmail_campaigns(
        selected=[cohort],
        flow_id="bench",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        inmail_sender=config.LINKEDIN_INMAIL_SENDER_URN or "urn:li:organization:92583550",
        brand_voice_validator=_stub_brand_voice(),
        dry_run=True,
        included_geos=["US"],
    ) or {}


def _call_static(cohort) -> dict:
    sheets, li_client, urn_res = _make_clients()
    return M._process_static_campaigns(
        selected=[cohort],
        flow_id="bench",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        figma_file="", figma_node="", mj_token="",
        dry_run=True,
        included_geos=["US"],
        ramp_id="BENCH-ARMS",
        cohort_id_override=cohort.id,
        cohort_description=cohort.cohort_description,
        base_rate_usd=50.0,
    ) or {}


def _run_sequential(cohort) -> float:
    log.info("=" * 72)
    log.info(" SEQUENTIAL — InMail then Static")
    log.info("=" * 72)
    t0 = time.monotonic()
    _call_inmail(cohort)
    _call_static(cohort)
    return time.monotonic() - t0


def _run_parallel(cohort) -> float:
    log.info("=" * 72)
    log.info(" PARALLEL — InMail + Static via ThreadPoolExecutor(max_workers=2)")
    log.info("=" * 72)
    t0 = time.monotonic()
    with ThreadPoolExecutor(max_workers=2, thread_name_prefix="bench-arm") as ex:
        f_inmail = ex.submit(_call_inmail, cohort)
        f_static = ex.submit(_call_static, cohort)
        f_inmail.result()
        f_static.result()
    return time.monotonic() - t0


def main_run() -> int:
    if not os.environ.get("ANTHROPIC_API_KEY"):
        print("ERROR: ANTHROPIC_API_KEY not in env. Run under `doppler run --`.")
        return 2

    cohort = _make_cohort()

    seq_elapsed = _run_sequential(cohort)
    log.info("RESULT: sequential elapsed=%.1fs", seq_elapsed)

    par_elapsed = _run_parallel(cohort)
    log.info("RESULT: parallel   elapsed=%.1fs", par_elapsed)

    print()
    print("=" * 72)
    print(" Phase 3.3 — arm-level parallelism benchmark")
    print("=" * 72)
    print(f"{'Run':<14} {'wall-clock':<12}")
    print(f"{'sequential':<14} {seq_elapsed:>9.1f}s")
    print(f"{'parallel':<14} {par_elapsed:>9.1f}s")
    if par_elapsed > 0:
        speedup = seq_elapsed / par_elapsed
        print()
        print(f"SPEEDUP: {speedup:.2f}x  "
              f"(sequential {seq_elapsed:.1f}s → parallel {par_elapsed:.1f}s)")
    print()
    return 0


if __name__ == "__main__":
    raise SystemExit(main_run())

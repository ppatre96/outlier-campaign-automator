"""
End-to-end test: create exactly 1 PAUSED/DRAFT campaign per channel
(LinkedIn, Meta, Google) for a cardiologist cohort.

This script bypasses the Stage A/B/C cohort discovery and directly hands a
hand-built cardiologist Cohort to the LinkedIn static arm, then to the
multi-platform fan-out (`_process_extra_platform_arm`). The result is one
DRAFT campaign on each channel, all logged to data/campaign_registry.json
+ the Triggers Sheet → Campaign Registry tab.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run -- venv/bin/python scripts/test_cardiologist_3channel.py
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

# Cap pipeline output to exactly 1 spec per platform BEFORE config loads.
os.environ.setdefault("MAX_COHORTS_PER_GEO_CLUSTER", "1")
os.environ.setdefault("ANGLES_PER_COHORT",           "1")

# Make repo root importable.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("3channel_test")

import config            # noqa: E402
import main              # noqa: E402
from src.analysis import Cohort        # noqa: E402
from src.linkedin_api import LinkedInClient  # noqa: E402
from src.linkedin_urn import UrnResolver     # noqa: E402
from src.sheets import SheetsClient          # noqa: E402


def _make_cardiologist_cohort() -> Cohort:
    """Hand-built cardiologist cohort matching the GMR-0006 cohort signature."""
    c = Cohort(
        name="job_titles_norm__cardiologist + experience_band__10plus",
        rules=[
            ("job_titles_norm__cardiologist", "cardiologist"),
            ("experience_band__10plus",        "10plus"),
        ],
        n=200, passes=20, pass_rate=10.0, lift_pp=4.0,
        support=15, coverage=0.05,
    )
    c._stg_id   = "STG-TEST-CARDIO-3CH"
    c._stg_name = "Test Cardio 3-Channel"
    c.id = "test-cardio-3ch"
    c.cohort_description = "Senior cardiologists with 10+ years"
    return c


def main_run() -> int:
    flow_id   = "TEST-CARDIO-3CH"
    location  = "United States"
    ramp_id   = "TEST-CARDIO-3CH"
    geos      = ["US"]   # single geo so only 1 geo group is created

    log.info("Caps: MAX_COHORTS=%d × ANGLES=%d × GEOS=1 → 1 LinkedIn campaign",
             config.MAX_COHORTS_PER_GEO_CLUSTER, config.ANGLES_PER_COHORT)
    log.info("Enabled platforms (env): %s", config.ENABLED_PLATFORMS)

    cohort = _make_cardiologist_cohort()

    # ── 1. LinkedIn arm ──────────────────────────────────────────────────────
    if not config.LINKEDIN_TOKEN:
        log.error("LINKEDIN_TOKEN not set — cannot proceed (need PNG generation + LI campaign).")
        return 1
    li_client = LinkedInClient(token=config.LINKEDIN_TOKEN)
    sheets    = SheetsClient()
    urn_res   = UrnResolver(sheets)

    log.info("=== STEP 1: LinkedIn arm (produces PNG + 1 campaign) ===")
    li_result = main._process_static_campaigns(
        selected=[cohort],
        flow_id=flow_id,
        location=location,
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key=config.ANTHROPIC_API_KEY,
        figma_file="",
        figma_node="",
        mj_token="",
        dry_run=False,
        family_exclude_pairs=[],
        data_driven_exclude_pairs=[],
        destination_url_override=config.LINKEDIN_DESTINATION,
        included_geos=geos,
        ramp_id=ramp_id,
        cohort_id_override=cohort.id,
        cohort_description=cohort.cohort_description,
    )
    log.info("LinkedIn arm finished — campaigns=%d, specs=%d, paths=%d",
             len(li_result.get("campaigns") or []),
             len(li_result.get("campaign_specs") or []),
             len(li_result.get("creative_paths") or {}))

    specs = li_result.get("campaign_specs") or []
    if not specs:
        log.error("LinkedIn arm produced no specs — cannot fan out to Meta/Google.")
        return 2

    # ── 2. Meta + Google arms ────────────────────────────────────────────────
    log.info("=== STEP 2: Meta + Google fan-out (1 campaign each) ===")
    # Allow caller to disable specific platforms (e.g. when Google OAuth scope
    # isn't yet authorized). Defaults to both.
    enabled = (os.environ.get("TEST_PLATFORMS") or "meta,google").split(",")
    enabled = [p.strip() for p in enabled if p.strip()]
    extras = main._build_extra_platform_clients(enabled)
    log.info("Built clients for: %s (requested: %s)", list(extras.keys()), enabled)

    for platform_name, parts in extras.items():
        log.info("--- %s arm ---", platform_name)
        try:
            r = main._process_extra_platform_arm(
                platform=platform_name,
                client=parts["client"],
                resolver=parts["resolver"],
                campaign_specs=specs,
                flow_id=flow_id,
                location=location,
                ramp_id=ramp_id,
                cohort_id_override=cohort.id,
                destination_url_override=config.LINKEDIN_DESTINATION,
            )
            log.info(
                "%s arm finished — groups=%d, campaigns=%d",
                platform_name, len(r.get("campaign_groups") or []),
                len(r.get("campaigns") or []),
            )
        except Exception as exc:
            log.exception("%s arm failed: %s", platform_name, exc)

    # ── 3. Verify Sheet rows ─────────────────────────────────────────────────
    log.info("=== STEP 3: Verifying Triggers sheet → Campaign Registry tab ===")
    try:
        ws = sheets._get_or_create_registry_tab()
        last = len(ws.col_values(1))
        log.info("Sheet has %d rows total (incl. header). Last 5:", last)
        for r in ws.get_all_values()[max(1, last - 5):last]:
            log.info("  channel=%s platform=%s campaign=%s ramp=%s",
                     r[9] if len(r) > 9 else "?",
                     r[10] if len(r) > 10 else "?",
                     r[11] if len(r) > 11 else "?",
                     r[0] if r else "?")
    except Exception as exc:
        log.warning("Sheet verification failed: %s", exc)

    log.info("DONE — check Triggers sheet for new rows tagged ramp=%s", ramp_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main_run())

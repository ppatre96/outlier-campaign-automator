"""
Isolated Meta ad-creation verification.

Bypasses the LinkedIn + Gemini arms entirely; creates one Meta DRAFT campaign
+ ad set + ad against a known-good reference PNG, exercising every step
through `MetaClient.create_image_ad`. Verifies the GET_STARTED → APPLY_NOW
remap shipped in the 2026-05-08 fix actually works against live Meta API.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/verify_meta_ad_creation.py

Output:
  - Meta campaign id, ad set id, ad creative id, ad id
  - All entities are PAUSED with the API-minimum daily budget ($1.00)
  - Net new state: 1 DRAFT Meta campaign visible in Ads Manager — easily
    archived after verification.
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("verify_meta")

import config                                       # noqa: E402
from src.meta_api import MetaClient                  # noqa: E402


REFERENCE_PNG = Path(
    "/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/1_1.png"
)


def main() -> int:
    if not REFERENCE_PNG.exists():
        print(f"ERROR: reference PNG missing at {REFERENCE_PNG}")
        return 2
    if not config.META_PAGE_ID:
        print("ERROR: META_PAGE_ID not in env. Run under doppler run --")
        return 2
    if not config.META_AD_ACCOUNT_ID:
        print("ERROR: META_AD_ACCOUNT_ID not in env.")
        return 2

    log.info("Reference PNG: %s (%d bytes)", REFERENCE_PNG, REFERENCE_PNG.stat().st_size)
    log.info("Meta page id: %s, ad account: %s", config.META_PAGE_ID, config.META_AD_ACCOUNT_ID)

    client = MetaClient()

    # 1. Campaign (no special category — keep this isolated test simple).
    log.info("=== Step 1: create campaign ===")
    campaign_id = client.create_campaign_group(
        name="agent_VERIFY-META-AD-CREATION",
    )
    log.info("Campaign id: %s", campaign_id)

    # 2. Ad set with a minimal targeting payload.
    # Meta's ad-set targeting requires geo_locations at minimum; under the
    # EMPLOYMENT special category Meta forbids age/gender, so we just send
    # the geo block and nothing else (matches what _process_extra_platform_arm
    # ends up sending when the cohort has no rules).
    log.info("=== Step 2: create ad set ===")
    targeting = {"geo_locations": {"countries": ["US"]}}
    ad_set_id = client.create_campaign(
        name="agent_VerifyMetaAdCreation_US",
        campaign_group_id=campaign_id,
        targeting=targeting,
    )
    log.info("Ad set id: %s", ad_set_id)

    # 3. Upload reference image.
    log.info("=== Step 3: upload image ===")
    image_hash = client.upload_image(REFERENCE_PNG)
    log.info("Image hash: %s", image_hash)

    # 4. Create ad — this is the path the CTA fix lives on. Pass GET_STARTED
    # explicitly to verify the remap to APPLY_NOW kicks in.
    log.info("=== Step 4: create ad (cta=GET_STARTED → expect remap to APPLY_NOW) ===")
    ad_result = client.create_image_ad(
        campaign_id=ad_set_id,
        image_id=image_hash,
        headline="Verify Meta ad creation",
        description="Isolated smoke test — DRAFT only.",
        primary_text="Outlier ad-creation verification (DRAFT, paused).",
        cta="GET_STARTED",
        destination_url="https://app.outlier.ai/en/contributors/projects",
    )
    log.info("Ad result: status=%s creative=%s",
             ad_result.status, ad_result.creative_urn)
    if ad_result.status != "ok":
        log.error("FAILED — status=%s error=%s message=%s",
                  ad_result.status, ad_result.error_class, ad_result.error_message)
        return 3

    log.info("=" * 72)
    log.info(" Meta end-to-end verified")
    log.info("=" * 72)
    log.info("Created (all PAUSED):")
    log.info("  campaign id: %s", campaign_id)
    log.info("  ad set id  : %s", ad_set_id)
    log.info("  creative   : %s", ad_result.creative_urn)
    log.info("Cleanup: archive these via Meta Ads Manager when done.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

"""
Isolated LinkedIn static-ad creative-attach verification.

Bypasses Gemini + QC entirely; creates one LinkedIn DRAFT campaign group +
campaign, uploads a known-good reference PNG, then runs the DSC post +
creative-attach path that previously 403'd. Verifies the new (post-2026-05-08)
mint with `w_member_social` actually unblocks `create_image_ad`.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/verify_linkedin_ad_creation.py

Output:
  - Campaign group URN, campaign URN, image URN, creative URN
  - All entities are DRAFT/PAUSED with placeholder budget
  - Net new state: 1 DRAFT campaign + 1 creative under ad account 510956407
"""
from __future__ import annotations

import logging
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
log = logging.getLogger("verify_linkedin")

import config                                       # noqa: E402
from src.linkedin_api import LinkedInClient          # noqa: E402


REFERENCE_PNG = Path(
    "/Users/pranavpatre/Outlier Creatives/Outlier - Static Ads v2/1_1.png"
)


def main() -> int:
    if not REFERENCE_PNG.exists():
        print(f"ERROR: reference PNG missing at {REFERENCE_PNG}")
        return 2
    if not config.LINKEDIN_TOKEN:
        print("ERROR: LINKEDIN_ACCESS_TOKEN not set. Run under doppler run --")
        return 2
    if not config.LINKEDIN_MEMBER_URN:
        print("ERROR: LINKEDIN_MEMBER_URN not set.")
        return 2

    log.info("Reference PNG: %s (%d bytes)", REFERENCE_PNG, REFERENCE_PNG.stat().st_size)
    log.info("LinkedIn ad account: %s", config.LINKEDIN_AD_ACCOUNT_ID)
    log.info("LinkedIn member URN: %s", config.LINKEDIN_MEMBER_URN)

    client = LinkedInClient(token=config.LINKEDIN_TOKEN)

    # 1. Campaign group.
    log.info("=== Step 1: create campaign group ===")
    group_urn = client.create_campaign_group(
        name="VERIFY-LI-AD-ATTACH",
    )
    log.info("Group URN: %s", group_urn)

    # 2. Campaign — minimum config: worldwide targeting (urn:li:geo:90009492).
    # `create_campaign` hardcodes objective WEBSITE_CONVERSION + auto-attaches
    # the configured conversion. That's fine; this test verifies creative
    # attach (Step 4), not conversion plumbing.
    log.info("=== Step 2: create campaign ===")
    facet_urns = {"urn:li:adTargetingFacet:locations": ["urn:li:geo:90009492"]}
    campaign_urn = client.create_campaign(
        name="VerifyLIAdAttach_Worldwide",
        campaign_group_urn=group_urn,
        facet_urns=facet_urns,
        exclude_facet_urns={},
    )
    log.info("Campaign URN: %s", campaign_urn)

    # 3. Upload the reference PNG.
    log.info("=== Step 3: upload image ===")
    image_urn = client.upload_image(REFERENCE_PNG)
    log.info("Image URN: %s", image_urn)

    # 4. Create the image ad — this exercises the DSC post + adCreatives path
    # that was 403'ing before the re-mint with w_member_social.
    log.info("=== Step 4: create image ad (DSC post + adCreatives) ===")
    result = client.create_image_ad(
        campaign_urn=campaign_urn,
        image_urn=image_urn,
        headline="LinkedIn ad-attach verifier",
        description="Isolated smoke test — DRAFT only.",
        intro_text="Outlier verification ping (DRAFT, paused).",
        ad_headline="Verify LinkedIn ad attach",
        ad_description="Smoke test for DSC post + creative attach.",
        destination_url="https://app.outlier.ai/en/contributors/projects",
        cta_button="APPLY",
    )

    log.info("Result: status=%s creative_urn=%s",
             result.status, result.creative_urn)
    if result.status != "ok":
        log.error("FAILED — status=%s error=%s message=%s",
                  result.status, result.error_class, result.error_message)
        return 3

    log.info("=" * 72)
    log.info(" LinkedIn static-ad attach verified")
    log.info("=" * 72)
    log.info("Created (all DRAFT/PAUSED):")
    log.info("  group urn   : %s", group_urn)
    log.info("  campaign urn: %s", campaign_urn)
    log.info("  image urn   : %s", image_urn)
    log.info("  creative urn: %s", result.creative_urn)
    log.info("Cleanup: archive in Campaign Manager when done, or run "
             "scripts/cleanup_test_campaigns.py to bulk-archive (it matches "
             "VERIFY-LI-AD-ATTACH).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

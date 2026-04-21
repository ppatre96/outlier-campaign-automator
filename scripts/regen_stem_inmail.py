"""
Targeted regen entry point for STEM campaigns using angle F (Financial — rate-in-subject-line).

Regenerates InMail creatives for three existing STEM campaigns using the proven
Financial control angle. Does NOT create new campaigns or campaign groups —
attaches new creatives to existing campaign URNs only.

Usage:
  # Dry run (no LinkedIn, no Sheets writes):
  PYTHONPATH=. python3 scripts/regen_stem_inmail.py --dry-run

  # Single campaign only (for testing):
  PYTHONPATH=. python3 scripts/regen_stem_inmail.py --only-id 633412886

  # Full run (writes to LinkedIn + Sheets):
  PYTHONPATH=. python3 scripts/regen_stem_inmail.py

WARNING: Running without --dry-run creates real LinkedIn creatives and writes to
Sheets — not reversible from this script.

Prereqs: LINKEDIN_INMAIL_SENDER_URN, LINKEDIN_ACCESS_TOKEN, LITELLM_API_KEY
must be set in .env.
"""
import argparse
import logging
import sys
from dataclasses import dataclass, field
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import config
from src.inmail_copy_writer import build_inmail_variants

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("regen_stem_inmail")

# ── Target campaigns ───────────────────────────────────────────────────────────
STEM_CAMPAIGNS = [
    {"id": 633412886, "tg_cat": "ML_ENGINEER",       "name": "STEM Campaign A", "hourly_rate": "$50"},
    {"id": 635201096, "tg_cat": "SOFTWARE_ENGINEER",  "name": "STEM Campaign B", "hourly_rate": "$50"},
    {"id": 634012966, "tg_cat": "MEDICAL",            "name": "STEM Campaign C", "hourly_rate": "$50"},
]


@dataclass
class StubCohort:
    name: str
    rules: list = field(default_factory=list)
    lift_pp: float = 0.0


def main() -> None:
    """Run angle-F InMail regen for all three STEM campaigns."""
    parser = argparse.ArgumentParser(
        description="Regen InMail creatives for existing STEM campaigns using angle F (Financial)."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Build copy variants and log them; skip LinkedIn and Sheets writes.",
    )
    parser.add_argument(
        "--only-id",
        type=int,
        default=None,
        metavar="CAMPAIGN_ID",
        help="Restrict regen to a single campaign ID (for testing).",
    )
    parser.add_argument(
        "--hourly-rate",
        type=str,
        default=None,
        metavar="RATE",
        help="Override default hourly rate (e.g. '$60'). Defaults to per-campaign config or '$50'.",
    )
    args = parser.parse_args()

    # ── Preflight checks ───────────────────────────────────────────────────────
    if not config.LINKEDIN_INMAIL_SENDER_URN:
        log.error(
            "LINKEDIN_INMAIL_SENDER_URN is not set. "
            "Set it in .env (e.g. urn:li:person:vYrY4QMQH0) and retry."
        )
        sys.exit(2)

    if not config.LITELLM_API_KEY:
        log.error(
            "LITELLM_API_KEY is not set. "
            "Set it in .env and retry."
        )
        sys.exit(2)

    if not args.dry_run and not config.LINKEDIN_TOKEN:
        log.error(
            "LINKEDIN_ACCESS_TOKEN is not set. "
            "Set it in .env (required for live LinkedIn writes) and retry, "
            "or use --dry-run to test copy generation without LinkedIn."
        )
        sys.exit(2)

    # ── Lazy API client construction (skip in dry-run) ─────────────────────────
    li = None
    sheets = None
    if not args.dry_run:
        from src.linkedin_api import LinkedInClient
        from src.sheets import SheetsClient
        li = LinkedInClient(config.LINKEDIN_TOKEN)
        sheets = SheetsClient()

    # ── Campaign loop ──────────────────────────────────────────────────────────
    targets = [
        cfg for cfg in STEM_CAMPAIGNS
        if args.only_id is None or cfg["id"] == args.only_id
    ]

    if not targets:
        log.error("No campaigns matched --only-id=%s", args.only_id)
        sys.exit(2)

    any_failure = False

    for cfg in targets:
        campaign_id = cfg["id"]
        campaign_urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        rate = cfg.get("hourly_rate") or args.hourly_rate or "$50"

        log.info("Processing campaign id=%s name='%s' tg=%s rate=%s",
                 campaign_id, cfg["name"], cfg["tg_cat"], rate)

        # Build cohort stub
        cohort = StubCohort(name=cfg["name"], rules=[])

        # Generate angle-F variant
        try:
            variants = build_inmail_variants(
                cfg["tg_cat"],
                cohort,
                config.LITELLM_API_KEY,
                angle_keys=["F"],
                hourly_rate=rate,
            )
        except Exception as exc:
            log.error("build_inmail_variants failed for id=%s: %s", campaign_id, exc)
            any_failure = True
            continue

        if not variants:
            log.error("No variants returned for id=%s — skipping", campaign_id)
            any_failure = True
            continue

        variant = variants[0]

        if args.dry_run:
            log.info(
                "[DRY RUN] campaign_urn=%s angle=%s",
                campaign_urn, variant.angle,
            )
            log.info("[DRY RUN] SUBJECT: %s", variant.subject)
            log.info(
                "[DRY RUN] BODY (%d chars): %s",
                len(variant.body),
                variant.body[:120] + ("..." if len(variant.body) > 120 else ""),
            )
            log.info("[DRY RUN] CTA_LABEL: %s (%d chars)", variant.cta_label, len(variant.cta_label))
            continue

        # Live path: create LinkedIn creative + write to Sheets
        try:
            creative_urn = li.create_inmail_ad(
                campaign_urn=campaign_urn,
                sender_urn=config.LINKEDIN_INMAIL_SENDER_URN,
                subject=variant.subject,
                body=variant.body,
                cta_label=variant.cta_label,
            )
        except Exception as exc:
            log.error("create_inmail_ad failed for id=%s: %s", campaign_id, exc)
            any_failure = True
            continue

        try:
            sheets.write_creative(
                stg_id=cfg["name"],
                creative_name=cfg["name"],
                li_creative_id=creative_urn,
            )
        except Exception as exc:
            log.warning("write_creative failed for id=%s: %s — creative was created at %s",
                        campaign_id, exc, creative_urn)
            # Sheets failure is non-fatal; creative already attached to LinkedIn

        log.info("Regen OK id=%s creative=%s", campaign_id, creative_urn)

    sys.exit(1 if any_failure else 0)


if __name__ == "__main__":
    main()

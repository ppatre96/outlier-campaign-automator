#!/usr/bin/env python3
"""Set Google Ads campaigns to REMOVED (Google's "archived").

Written for GMR-0029 (2026-09-04): the Google Search launch resolved its
audience off the wrong signup flow and built software-engineer campaigns for a
graphic-design / audio / video / animation ramp. Three campaigns landed with
SWE creatives and coding keywords, and 15 of 18 ad groups were rejected
DUPLICATE_ADGROUP_NAME on top. They're PAUSED so nothing is spending, but
leaving wrong-audience shells in the account makes the relaunch unreadable.

REMOVED is terminal in Google Ads — a removed campaign cannot be re-enabled.
That's why --dry-run is the default and each campaign's name is printed for
confirmation before anything is mutated.

Usage:
    doppler run -- python3 scripts/archive_google_campaigns.py \\
        --campaign-ids 24221946160,24216776618,24216841346 --dry-run
    doppler run -- python3 scripts/archive_google_campaigns.py \\
        --campaign-ids 24221946160,24216776618,24216841346 --apply
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from src.google_ads_api import GoogleAdsClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("archive_google_campaigns")


def _lookup(client, customer_id: str, campaign_ids: list[str]) -> dict[str, dict]:
    """Current name/status/channel for each campaign id, so the operator can
    see what they're about to remove."""
    ga_service = client.get_service("GoogleAdsService")
    ids_csv = ", ".join(campaign_ids)
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.advertising_channel_type
        FROM campaign
        WHERE campaign.id IN ({ids_csv})
    """
    out: dict[str, dict] = {}
    for batch in ga_service.search_stream(customer_id=customer_id, query=query):
        for row in batch.results:
            out[str(row.campaign.id)] = {
                "name": row.campaign.name,
                "status": row.campaign.status.name,
                "channel": row.campaign.advertising_channel_type.name,
            }
    return out


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--campaign-ids", required=True,
                    help="comma-separated Google Ads campaign ids")
    ap.add_argument("--customer-id", default="",
                    help="override the configured customer id (digits only)")
    group = ap.add_mutually_exclusive_group()
    group.add_argument("--dry-run", action="store_true", default=True,
                       help="show what would be removed (default)")
    group.add_argument("--apply", action="store_true",
                       help="actually set the campaigns to REMOVED (irreversible)")
    args = ap.parse_args()

    campaign_ids = [c.strip() for c in args.campaign_ids.split(",") if c.strip()]
    if not campaign_ids:
        log.error("No campaign ids given")
        return 2
    if not all(c.isdigit() for c in campaign_ids):
        log.error("Campaign ids must be digits only, got: %s", campaign_ids)
        return 2

    api = GoogleAdsClient()
    client = api._ensure_client()
    customer_id = (args.customer_id or api._customer_id_str).replace("-", "")

    found = _lookup(client, customer_id, campaign_ids)
    missing = [c for c in campaign_ids if c not in found]
    for c in missing:
        log.warning("Campaign %s not found under customer %s — skipping", c, customer_id)

    targets = [c for c in campaign_ids if c in found]
    if not targets:
        log.error("Nothing to do — none of the given ids exist under %s", customer_id)
        return 1

    for c in targets:
        meta = found[c]
        log.info("  %s  [%s/%s]  %s", c, meta["channel"], meta["status"], meta["name"])

    if not args.apply:
        log.info(
            "DRY RUN — %d campaign(s) would be set to REMOVED. Re-run with --apply "
            "to commit. This is irreversible in Google Ads.", len(targets),
        )
        return 0

    campaign_service = client.get_service("CampaignService")
    ops = []
    for c in targets:
        op = client.get_type("CampaignOperation")
        op.update.resource_name = f"customers/{customer_id}/campaigns/{c}"
        op.update.status = client.enums.CampaignStatusEnum.REMOVED
        client.copy_from(op.update_mask, client.get_type("FieldMask")(paths=["status"]))
        ops.append(op)

    resp = campaign_service.mutate_campaigns(customer_id=customer_id, operations=ops)
    for result in resp.results:
        log.info("REMOVED %s", result.resource_name)
    log.info("Archived %d campaign(s) under customer %s", len(resp.results), customer_id)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

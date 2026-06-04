"""Backfill the Postgres `campaigns` table for a ramp.

The console reads campaigns from Postgres (see ui_decisions.upsert_campaign /
console lib/db.ts:listCampaignsForRamp). Ramps launched before that persistence
existed have nothing in the table. This backfills from:

  1. the local Campaign Registry (entry_dict rows — full data incl. creatives), and
  2. a LIVE Meta + Google by-name scan — their campaign names carry the ramp id
     ("Scale-GMR-0023 | …"), so we can recover campaigns the registry missed
     (e.g. created when CI lacked credentials.json → NullSheetsClient no-op).

LinkedIn is intentionally skipped: its campaign names don't carry the ramp id,
so it can't be matched by name. LinkedIn populates on its next pipeline run.

Idempotent — safe to re-run. Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/backfill_campaigns_to_pg.py --ramp-id GMR-0023
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from src.campaign_registry import _load  # noqa: E402
from src.ui_decisions import upsert_campaign  # noqa: E402


def backfill_local(ramp_id: str) -> tuple[int, set[str]]:
    rows = [r for r in _load() if str(r.get("smart_ramp_id", "")) == ramp_id]
    seen: set[str] = set()
    for r in rows:
        upsert_campaign(r)
        cid = str(r.get("platform_campaign_id", "") or "")
        if cid:
            seen.add(cid)
    return len(rows), seen


def scan_meta(ramp_id: str, seen: set[str]) -> int:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign

    FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                        api_version=config.META_API_VERSION or "v21.0")
    acct_id = config.META_AD_ACCOUNT_ID
    if acct_id and not acct_id.startswith("act_"):
        acct_id = f"act_{acct_id}"
    camps = AdAccount(acct_id).get_campaigns(
        params={"limit": 500},
        fields=[Campaign.Field.id, Campaign.Field.name, Campaign.Field.effective_status],
    )
    n = 0
    for c in camps:
        name = c.get("name") or ""
        if ramp_id not in name:
            continue
        cid = str(c["id"])
        if cid in seen:
            continue
        # Minimal row — name carries the cohort/locale; no creative recovered.
        upsert_campaign({
            "smart_ramp_id": ramp_id, "platform": "meta", "channel": "Meta",
            "campaign_type": "static", "cohort_signature": name,
            "geo_cluster": "", "angle": "", "platform_campaign_id": cid,
            "campaign_name": name, "status": str(c.get("effective_status", "")),
            "campaign_link": f"https://business.facebook.com/adsmanager/manage/campaigns?selected_campaign_ids={cid}",
        })
        seen.add(cid)
        n += 1
    return n


def scan_google(ramp_id: str, seen: set[str]) -> int:
    from src.google_ads_api import GoogleAdsClient
    gc = GoogleAdsClient(channel="display")
    client = gc._ensure_client()
    svc = client.get_service("GoogleAdsService")
    q = (
        "SELECT campaign.id, campaign.name, campaign.status "
        f"FROM campaign WHERE campaign.name LIKE '%{ramp_id}%'"
    )
    n = 0
    for row in svc.search(customer_id=gc._customer_id_str, query=q):
        cid = str(row.campaign.id)
        if cid in seen:
            continue
        name = row.campaign.name
        upsert_campaign({
            "smart_ramp_id": ramp_id, "platform": "google", "channel": "Google",
            "campaign_type": "static", "cohort_signature": name,
            "geo_cluster": "", "angle": "", "platform_campaign_id": cid,
            "campaign_name": name, "status": row.campaign.status.name,
            "campaign_link": f"https://ads.google.com/aw/campaigns?campaignId={cid}",
        })
        seen.add(cid)
        n += 1
    return n


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--ramp-id", required=True)
    ap.add_argument("--skip-meta", action="store_true")
    ap.add_argument("--skip-google", action="store_true")
    args = ap.parse_args()

    local_n, seen = backfill_local(args.ramp_id)
    print(f"local registry: upserted {local_n} rows ({len(seen)} distinct campaign ids)")

    if not args.skip_meta:
        try:
            print(f"live Meta scan: +{scan_meta(args.ramp_id, seen)} new campaigns")
        except Exception as exc:
            print(f"live Meta scan FAILED (non-fatal): {type(exc).__name__}: {exc}")
    if not args.skip_google:
        try:
            print(f"live Google scan: +{scan_google(args.ramp_id, seen)} new campaigns")
        except Exception as exc:
            print(f"live Google scan FAILED (non-fatal): {type(exc).__name__}: {exc}")

    print(f"done — {len(seen)} distinct campaign ids now backfilled for {args.ramp_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

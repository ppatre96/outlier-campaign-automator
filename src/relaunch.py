"""Relaunch-replace: archive a ramp's existing campaigns on one channel before
re-launching, so re-launching doesn't pile up duplicates.

Triggered by the console's "Relaunch (replace)" button → poller `replace` input
→ config.REPLACE_EXISTING → _launch_ramp calls archive_channel_campaigns()
before run_launch_for_ramp creates fresh campaigns.

Archive semantics (reversible, per platform):
  - meta     → campaign status DELETED (hides + cascades child ad sets/ads)
  - linkedin → campaign status ARCHIVED (best-effort; DRAFT campaigns may reject)
  - google   → campaign status PAUSED (REMOVED is forbidden via the API)

Campaign IDs come from the Postgres `campaigns` table (exact IDs — no
name-guessing). After a successful archive the rows are dropped so the
console's per-channel "created" count reflects only live campaigns. Entirely
best-effort: any platform failure is logged, never raised, so the subsequent
fresh launch still proceeds.
"""
from __future__ import annotations

import logging
import time

import config
from src.ui_decisions import list_campaign_platform_ids, delete_campaign_rows

log = logging.getLogger(__name__)


def _archive_meta(ids: list[str]) -> list[str]:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.campaign import Campaign

    FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                        api_version=config.META_API_VERSION or "v21.0")
    done: list[str] = []
    for cid in ids:
        for attempt in range(4):
            try:
                Campaign(cid).api_update(params={Campaign.Field.status: Campaign.Status.deleted})
                done.append(cid)
                break
            except Exception as exc:
                msg = str(exc)
                if "2446079" in msg or "rate" in msg.lower():
                    time.sleep(5 * (attempt + 1))
                    continue
                log.warning("relaunch: Meta archive failed for %s: %s", cid, msg[:200])
                break
    return done


def _archive_linkedin(ids: list[str]) -> list[str]:
    import os
    from src.linkedin_api import LinkedInClient

    token = (os.getenv("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN")
             or config.LINKEDIN_TOKEN)
    if not token:
        log.warning("relaunch: no LinkedIn token — skipping LinkedIn archive")
        return []
    li = LinkedInClient(token)
    done: list[str] = []
    for urn in ids:
        cid = str(urn).rsplit(":", 1)[-1]
        try:
            resp = li._req(
                "POST",
                li._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{cid}"),
                json={"patch": {"$set": {"status": "ARCHIVED"}}},
                headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
            )
            if resp.status_code < 300:
                done.append(urn)
            else:
                log.warning("relaunch: LinkedIn archive %s → HTTP %s %s",
                            cid, resp.status_code, resp.text[:200])
        except Exception as exc:
            log.warning("relaunch: LinkedIn archive failed for %s: %s", cid, str(exc)[:200])
    return done


def _archive_google(ids: list[str]) -> list[str]:
    from src.google_ads_api import GoogleAdsClient

    gc = GoogleAdsClient(channel="display")
    client = gc._ensure_client()
    svc = client.get_service("CampaignService")
    cid_str = gc._customer_id_str
    done: list[str] = []
    for cid in ids:
        try:
            op = client.get_type("CampaignOperation")
            camp = op.update
            camp.resource_name = f"customers/{cid_str}/campaigns/{cid}"
            camp.status = client.enums.CampaignStatusEnum.PAUSED
            from google.api_core import protobuf_helpers
            op.update_mask.CopyFrom(protobuf_helpers.field_mask(None, camp._pb))
            svc.mutate_campaigns(customer_id=cid_str, operations=[op])
            done.append(cid)
        except Exception as exc:
            log.warning("relaunch: Google pause failed for %s: %s", cid, str(exc)[:200])
    return done


_ARCHIVERS = {"meta": _archive_meta, "linkedin": _archive_linkedin, "google": _archive_google}


def archive_channel_campaigns(ramp_id: str, channel: str) -> dict:
    """Archive every existing campaign for (ramp_id × channel) recorded in the
    campaigns table, then drop the archived rows. Best-effort. Returns a small
    summary dict {channel, found, archived}."""
    channel = (channel or "").strip().lower()
    archiver = _ARCHIVERS.get(channel)
    if archiver is None:
        log.warning("relaunch: no archiver for channel=%r — skipping", channel)
        return {"channel": channel, "found": 0, "archived": 0}

    ids = list_campaign_platform_ids(ramp_id, channel)
    if not ids:
        log.info("relaunch: no existing %s campaigns recorded for ramp=%s — nothing to archive",
                 channel, ramp_id)
        return {"channel": channel, "found": 0, "archived": 0}

    log.info("relaunch: archiving %d existing %s campaign(s) for ramp=%s before relaunch",
             len(ids), channel, ramp_id)
    try:
        archived = archiver(ids)
    except Exception as exc:
        log.error("relaunch: %s archive pass raised (%s) — continuing to fresh launch", channel, exc)
        archived = []

    if archived:
        delete_campaign_rows(ramp_id, channel, archived)
    log.info("relaunch: archived %d/%d %s campaign(s) for ramp=%s",
             len(archived), len(ids), channel, ramp_id)
    return {"channel": channel, "found": len(ids), "archived": len(archived)}

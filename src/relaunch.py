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
    """Archive a ramp's LinkedIn campaigns.

    A bare `$set status=ARCHIVED` PATCH fails on DRAFT campaigns (which is the
    common relaunch case) with 400 MULTIPLE_VALIDATIONS_FAILED. Per the rules
    surfaced 2026-05-13 (see feedback_linkedin_archive_rules), a DRAFT→ARCHIVED
    transition needs TWO things in the same call/sequence:
      1. `runSchedule.start` bumped to the future (DATE_TOO_EARLY validator), and
      2. the parent campaign GROUP already non-DRAFT — so archive groups FIRST,
         then the child campaigns.
    """
    import os
    from src.linkedin_api import LinkedInClient

    token = (os.getenv("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN")
             or config.LINKEDIN_TOKEN)
    if not token:
        log.warning("relaunch: no LinkedIn token — skipping LinkedIn archive")
        return []
    li = LinkedInClient(token)

    def _future_start_ms() -> int:
        return int(time.time() * 1000) + 60 * 60 * 1000  # now + 1h, ms

    def _patch_archive(path: str, entity_id: str) -> bool:
        try:
            resp = li._req(
                "POST",
                li._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/{path}/{entity_id}"),
                json={"patch": {"$set": {
                    "status": "ARCHIVED",
                    "runSchedule": {"start": _future_start_ms()},
                }}},
                headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
            )
            if resp.status_code in (200, 204):
                return True
            log.warning("relaunch: LinkedIn archive %s %s → HTTP %s %s",
                        path, entity_id, resp.status_code, resp.text[:200])
            return False
        except Exception as exc:
            log.warning("relaunch: LinkedIn archive %s %s failed: %s",
                        path, entity_id, str(exc)[:200])
            return False

    # Resolve each campaign's parent group so we can archive groups first.
    cid_to_urn = {str(u).rsplit(":", 1)[-1]: u for u in ids}
    group_ids: set[str] = set()
    for cid in cid_to_urn:
        try:
            grp = (li.get_campaign(cid) or {}).get("campaignGroup") or ""
            gid = str(grp).rsplit(":", 1)[-1]
            if gid:
                group_ids.add(gid)
        except Exception as exc:
            log.warning("relaunch: couldn't resolve parent group for campaign %s: %s",
                        cid, str(exc)[:150])

    # Step 1 — archive parent groups (best-effort; makes DRAFT children archivable).
    for gid in group_ids:
        _patch_archive("adCampaignGroups", gid)

    # Step 2 — archive the campaigns.
    done: list[str] = []
    for cid, urn in cid_to_urn.items():
        if _patch_archive("adCampaigns", cid):
            done.append(urn)
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


def archive_channel_campaigns(
    ramp_id: str, channel: str, locales: list[str] | None = None
) -> dict:
    """Archive every existing campaign for (ramp_id × channel) recorded in the
    campaigns table, then drop the archived rows. Best-effort. Returns a small
    summary dict {channel, found, archived}.

    When `locales` is given (a REPLACE run scoped with ONLY_LOCALES), archive
    ONLY campaigns for those locales — never the rest of the ramp's other-
    language campaigns, which the scoped launch would not recreate."""
    channel = (channel or "").strip().lower()
    archiver = _ARCHIVERS.get(channel)
    if archiver is None:
        log.warning("relaunch: no archiver for channel=%r — skipping", channel)
        return {"channel": channel, "found": 0, "archived": 0}

    scope = f"locales={locales}" if locales else "whole ramp"
    ids = list_campaign_platform_ids(ramp_id, channel, locales)
    if not ids:
        log.info("relaunch: no existing %s campaigns recorded for ramp=%s (%s) — nothing to archive",
                 channel, ramp_id, scope)
        return {"channel": channel, "found": 0, "archived": 0}

    log.info("relaunch: archiving %d existing %s campaign(s) for ramp=%s before relaunch (%s)",
             len(ids), channel, ramp_id, scope)
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

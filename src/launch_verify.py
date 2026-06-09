"""Piece C — inline post-launch verify-and-heal.

After an arm creates a campaign / ad set's child ads (and after the arm has
retried any that failed — the retry stays inline because only the arm has the
PNG / variant / UTM in scope), it calls `heal_empty()` for each container that
ended the run with ZERO ads/creatives. An empty shell is archived on-platform
and flagged so no empty campaign survives a launch.

Heal is scoped to the child container THIS run created (NOT the shared parent
campaign group, which may legitimately hold other non-empty ad sets):
  - meta            → ad set DELETED
  - google/_search  → ad group PAUSED  (REMOVED is forbidden on campaigns; we
                      pause the ad group to mirror relaunch's reversible intent)
  - linkedin        → campaign ARCHIVED

Every healed empty also writes a `launch_empty_healed` audit-log row (the
Postgres flag the console reads). The caller collects the returned summaries
and fires ONE Slack ping per arm via `notify_healed()`.

All best-effort: nothing here raises into the launch path.
"""
from __future__ import annotations

import logging
from typing import Optional

import config

log = logging.getLogger(__name__)


# ── Per-platform child-container archivers ──────────────────────────────────

def _archive_meta_adset(adset_id: str) -> bool:
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adset import AdSet

    FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN,
                        api_version=config.META_API_VERSION or "v21.0")
    AdSet(adset_id).api_update(params={"status": "DELETED"})
    return True


def _archive_google_adgroup(adgroup_resource: str, *, channel: str = "display") -> bool:
    from src.google_ads_api import GoogleAdsClient
    from google.api_core import protobuf_helpers

    gc = GoogleAdsClient(channel=channel)
    client = gc._ensure_client()
    svc = client.get_service("AdGroupService")
    op = client.get_type("AdGroupOperation")
    ag = op.update
    # create_campaign returns the full resource name (customers/x/adGroups/y).
    ag.resource_name = (
        adgroup_resource if str(adgroup_resource).startswith("customers/")
        else f"customers/{gc._customer_id_str}/adGroups/{adgroup_resource}"
    )
    ag.status = client.enums.AdGroupStatusEnum.PAUSED
    op.update_mask.CopyFrom(protobuf_helpers.field_mask(None, ag._pb))
    svc.mutate_ad_groups(customer_id=gc._customer_id_str, operations=[op])
    return True


def _archive_linkedin_campaign(campaign_urn: str, *, li_client=None) -> bool:
    cid = str(campaign_urn).rsplit(":", 1)[-1]
    if li_client is None:
        import os
        from src.linkedin_api import LinkedInClient
        token = (os.getenv("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN")
                 or config.LINKEDIN_TOKEN)
        if not token:
            log.warning("launch_verify: no LinkedIn token — can't archive empty %s", cid)
            return False
        li_client = LinkedInClient(token)
    resp = li_client._req(
        "POST",
        li_client._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{cid}"),
        json={"patch": {"$set": {"status": "ARCHIVED"}}},
        headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
    )
    if resp.status_code < 300:
        return True
    log.warning("launch_verify: LinkedIn archive %s → HTTP %s %s",
                cid, resp.status_code, resp.text[:200])
    return False


def heal_empty(
    *,
    platform: str,
    container_id: str,
    ramp_id: str,
    campaign_name: str,
    reason: str = "",
    li_client=None,
) -> Optional[dict]:
    """Archive an empty child container and flag it. Returns a small summary
    dict {platform, container_id, campaign_name, reason} when an empty was
    healed, or None if nothing was archived. Best-effort — never raises.

    Call this ONLY after the arm's retry pass still left the container with
    zero ads/creatives.
    """
    platform = (platform or "").strip().lower()
    try:
        if platform == "meta":
            ok = _archive_meta_adset(container_id)
        elif platform in ("google", "google_search"):
            ok = _archive_google_adgroup(
                container_id,
                channel="search" if platform == "google_search" else "display",
            )
        elif platform == "linkedin":
            ok = _archive_linkedin_campaign(container_id, li_client=li_client)
        else:
            log.warning("launch_verify: no healer for platform=%r — skipping", platform)
            return None
    except Exception as exc:
        log.error("launch_verify: heal of empty %s %s failed: %s",
                  platform, container_id, str(exc)[:200])
        ok = False

    summary = {
        "platform": platform,
        "container_id": str(container_id),
        "campaign_name": campaign_name,
        "reason": reason or "no ads/creatives attached after retry",
        "archived": ok,
    }
    log.warning(
        "launch_verify: EMPTY %s container %s (%s) — %s; reason=%s",
        platform, container_id, campaign_name,
        "archived" if ok else "archive FAILED", summary["reason"],
    )
    # Postgres flag (audit log the console reads). Best-effort.
    try:
        from src.ui_decisions import log_event
        log_event(ramp_id or "", "launch_empty_healed", summary)
    except Exception as exc:
        log.debug("launch_verify: log_event skipped: %s", exc)
    return summary


def record_keywords_dropped(
    *,
    ramp_id: str,
    container_id: str,
    campaign_name: str,
    dropped: list[str],
) -> Optional[dict]:
    """Flag keywords Google rejected (policy/invalid) on an otherwise-healthy
    Search campaign. The ad-group still went live with the surviving keywords —
    this is a "needs review", not a heal. Writes a `launch_keywords_dropped`
    audit row (the console reads it) and returns a summary for the Slack ping.
    Best-effort — never raises."""
    if not dropped:
        return None
    summary = {
        "platform": "google_search",
        "container_id": str(container_id),
        "campaign_name": campaign_name,
        "dropped": list(dropped),
        "reason": (
            f"{len(dropped)} keyword(s) rejected by Google (policy/invalid), "
            f"campaign live with the rest: {', '.join(dropped)}"
        )[:400],
    }
    log.warning(
        "launch_verify: %d keyword(s) dropped on %s (%s): %s",
        len(dropped), container_id, campaign_name, dropped,
    )
    try:
        from src.ui_decisions import log_event
        log_event(ramp_id or "", "launch_keywords_dropped", summary)
    except Exception as exc:
        log.debug("launch_verify: keywords-dropped log_event skipped: %s", exc)
    return summary


def notify_keywords_dropped(ramp_id: str, dropped_notes: list[dict]) -> None:
    """One threaded Slack ping summarising keywords dropped this arm.
    Best-effort — Slack outage never blocks the launch."""
    if not dropped_notes:
        return
    lines = [
        f"⚠️ Verify: {len(dropped_notes)} Search campaign(s) for {ramp_id} went live "
        f"with some keywords dropped (Google policy/invalid):",
    ]
    for n in dropped_notes:
        _kw = n.get("dropped") or []
        lines.append(
            f"  • {n.get('campaign_name') or n.get('container_id')} — "
            f"{len(_kw)} dropped: {', '.join(_kw)}"
        )
    text = "\n".join(lines)
    try:
        from src.smart_ramp_notifier import _send_to_all_targets, _lookup_thread_ts
        _send_to_all_targets(text, ramp_id=ramp_id, thread_ts=_lookup_thread_ts(ramp_id))
    except Exception as exc:
        log.warning("launch_verify: notify_keywords_dropped Slack ping failed (non-fatal): %s", exc)


def notify_healed(ramp_id: str, healed: list[dict]) -> None:
    """Fire ONE Slack ping summarising the empties healed in this arm, threaded
    under the ramp. Best-effort — Slack outage never blocks the launch."""
    if not healed:
        return
    lines = [
        f"⚠️ Verify-and-heal: archived {len(healed)} empty campaign(s) for {ramp_id} "
        f"(zero ads attached after retry):",
    ]
    for h in healed:
        lines.append(
            f"  • {h.get('platform')} — {h.get('campaign_name') or h.get('container_id')} "
            f"({'archived' if h.get('archived') else 'archive failed'}) — "
            f"{h.get('reason') or 'no ads/creatives attached after retry'}"
        )
    text = "\n".join(lines)
    try:
        from src.smart_ramp_notifier import _send_to_all_targets, _lookup_thread_ts
        _send_to_all_targets(text, ramp_id=ramp_id, thread_ts=_lookup_thread_ts(ramp_id))
    except Exception as exc:
        log.warning("launch_verify: notify_healed Slack ping failed (non-fatal): %s", exc)

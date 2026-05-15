"""Archive all DRAFT LinkedIn campaigns + campaign groups for a given ramp.

Reusable counterpart to scripts/archive_stale_drafts.py — instead of a
hard-coded URN list, this script reads the Campaign Registry tab in the
Triggers Sheet, filters by `smart_ramp_id`, derives the parent campaign
groups via `LinkedInClient.get_campaign()`, then PATCHes groups + campaigns
to ARCHIVED in the correct order.

LinkedIn ordering rules (surfaced 2026-05-13, see archive_stale_drafts.py):
  - DRAFT groups: must bump runSchedule.start to future before ARCHIVED.
  - DRAFT campaigns: must bump runSchedule.start AND parent group must
    already be non-DRAFT. So we archive groups FIRST, then campaigns.

Idempotent: re-running on a partially-archived ramp will skip entities
already in the ARCHIVED state and only act on remaining DRAFTs.

Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/archive_by_ramp.py --ramp-id GMR-0020 --dry-run
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/archive_by_ramp.py --ramp-id GMR-0020
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from src.linkedin_api import LinkedInClient  # noqa: E402
from src.sheets import SheetsClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("archive_by_ramp")


def _future_start_ms() -> int:
    """LinkedIn rejects DRAFT→ARCHIVED transitions when runSchedule.start is
    in the past (DATE_TOO_EARLY validator). PATCH the start to ~1 hour ahead
    as part of the same partial-update call."""
    return int(time.time() * 1000) + 60 * 60 * 1000


def _id_from_urn(urn: str) -> str:
    """LinkedIn URNs are colon-delimited; the numeric id is the trailing part.
    Accepts a bare numeric id too (returns it unchanged)."""
    return urn.rsplit(":", 1)[-1] if urn else ""


def _patch_archive(client: LinkedInClient, entity_type: str, entity_id: str,
                   dry_run: bool) -> tuple[bool, str]:
    """PATCH one entity to ARCHIVED. Returns (success, message)."""
    path = "adCampaignGroups" if entity_type == "group" else "adCampaigns"
    url = client._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/{path}/{entity_id}")
    payload = {"patch": {"$set": {
        "status": "ARCHIVED",
        "runSchedule": {"start": _future_start_ms()},
    }}}
    headers = {"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"}

    if dry_run:
        return True, f"[dry-run] would PATCH {entity_type} {entity_id} → ARCHIVED"
    try:
        resp = client._req("POST", url, json=payload, headers=headers)
        if resp.status_code in (200, 204):
            return True, f"archived {entity_type} {entity_id}"
        return False, f"HTTP {resp.status_code}: {resp.text[:300]}"
    except Exception as exc:
        return False, f"exception: {type(exc).__name__}: {exc}"


def _collect_ramp_entities(sheets: SheetsClient, ramp_id: str,
                           ) -> list[dict]:
    """Read the Campaign Registry tab and return rows whose smart_ramp_id
    matches. Uses gspread's get_all_records() — header row maps to dict keys
    via the title-cased COLUMNS contract enforced by _get_or_create_registry_tab."""
    ws = sheets._get_or_create_registry_tab()
    records = ws.get_all_records()
    matches = [r for r in records if str(r.get("Smart Ramp Id", "")).strip() == ramp_id]
    return matches


def _derive_groups_from_campaigns(client: LinkedInClient, campaign_ids: list[str],
                                  ) -> dict[str, str]:
    """For each campaign id, GET it from LinkedIn and read the campaignGroup
    URN. Returns {campaign_id: group_id}. Skips campaigns the GET can't
    resolve (already-deleted, permission, etc.) with a warning."""
    out: dict[str, str] = {}
    for cid in campaign_ids:
        try:
            data = client.get_campaign(cid)
            group_urn = data.get("campaignGroup") or ""
            if group_urn:
                out[cid] = _id_from_urn(group_urn)
            else:
                log.warning("Campaign %s: no campaignGroup in GET response", cid)
        except Exception as exc:
            log.warning("Campaign %s: GET failed (%s) — skipping group derivation", cid, exc)
    return out


def _status_of(client: LinkedInClient, entity_type: str, entity_id: str) -> str:
    """GET the entity and return its current status. Returns '' on any error
    so the caller treats unknown-state as 'try to PATCH anyway' rather than
    silently skipping a stale-but-still-DRAFT entity."""
    path = "adCampaignGroups" if entity_type == "group" else "adCampaigns"
    url = client._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/{path}/{entity_id}")
    try:
        resp = client._req("GET", url)
        if resp.status_code == 200:
            return str(resp.json().get("status", ""))
    except Exception:
        pass
    return ""


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ramp-id", required=True,
                        help="Smart Ramp id whose DRAFT entities to archive (e.g. GMR-0020)")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be archived without making changes")
    args = parser.parse_args()

    log.info("Archive plan for ramp=%s dry_run=%s", args.ramp_id, args.dry_run)

    sheets = SheetsClient()
    client = LinkedInClient(config.LINKEDIN_TOKEN)

    rows = _collect_ramp_entities(sheets, args.ramp_id)
    log.info("Found %d registry rows for ramp_id=%s", len(rows), args.ramp_id)
    if not rows:
        log.warning("Nothing to archive — exiting")
        return 0

    # platform_campaign_id is the numeric LinkedIn campaign id (preferred);
    # linkedin_campaign_urn is the full URN fallback for older rows.
    # Registry has two columns that may carry the campaign reference, and
    # historically BOTH have been written with `urn:li:sponsoredCampaign:`
    # prefixes (the "id" column was renamed but old rows weren't migrated).
    # Always strip the prefix so the PATCH URL has the bare numeric id.
    campaign_ids: set[str] = set()
    for r in rows:
        cid = _id_from_urn(str(r.get("Platform Campaign Id", "")).strip())
        if not cid:
            cid = _id_from_urn(str(r.get("Linkedin Campaign Urn", "")).strip())
        if cid:
            campaign_ids.add(cid)
    log.info("Distinct campaign ids: %d", len(campaign_ids))

    # Derive parent groups by GET'ing each campaign once. Cheap (one HTTP per
    # campaign) and avoids relying on the Triggers sheet's stale master_campaign
    # column (which the handoff flagged as out-of-sync with reality).
    campaign_to_group = _derive_groups_from_campaigns(client, sorted(campaign_ids))
    group_ids = {gid for gid in campaign_to_group.values() if gid}
    log.info("Distinct parent groups: %d", len(group_ids))

    # Filter to entities still in DRAFT (skip ARCHIVED to keep the script idempotent
    # and to avoid wasting HTTP cycles + spurious 422s on stale rows).
    drafts_groups: list[str] = []
    for gid in sorted(group_ids):
        st = _status_of(client, "group", gid)
        if st == "ARCHIVED":
            log.info("group %s already ARCHIVED — skipping", gid)
            continue
        drafts_groups.append(gid)

    drafts_campaigns: list[str] = []
    for cid in sorted(campaign_ids):
        st = _status_of(client, "campaign", cid)
        if st == "ARCHIVED":
            log.info("campaign %s already ARCHIVED — skipping", cid)
            continue
        drafts_campaigns.append(cid)

    log.info(
        "Will archive: %d group(s), %d campaign(s) (groups first)",
        len(drafts_groups), len(drafts_campaigns),
    )

    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    # Groups before campaigns — campaign archive PATCH requires parent group
    # to already be non-DRAFT.
    for gid in drafts_groups:
        ok, msg = _patch_archive(client, "group", gid, args.dry_run)
        (successes if ok else failures).append(f"group {gid}: {msg}" if ok
                                                else (f"group {gid}", msg))
        log.info(msg) if ok else log.warning("group %s: %s", gid, msg)

    for cid in drafts_campaigns:
        ok, msg = _patch_archive(client, "campaign", cid, args.dry_run)
        (successes if ok else failures).append(f"campaign {cid}: {msg}" if ok
                                                else (f"campaign {cid}", msg))
        log.info(msg) if ok else log.warning("campaign %s: %s", cid, msg)

    log.info("=" * 60)
    log.info("Summary: %d succeeded, %d failed", len(successes), len(failures))
    if failures:
        for ent, reason in failures:
            log.warning("  FAIL  %s — %s", ent, reason)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Archive a manually-curated list of stale LinkedIn DRAFT campaigns and
campaign groups via URN-direct PATCH.

This is a ONE-SHOT cleanup script — not a recurring tool. The list below
combines two sources:

  (a) Original handoff (2026-05-13) — 8 entities from the manual GMR-0020 run
      that were created during failed/aborted attempts.
  (b) Cron run 25763743170 (2026-05-12 21:38 UTC) — the run that processed
      GMR-0020 immediately after the poller fix merged, before the manual
      backfill. Created 6 campaign groups (3 Static + 3 InMail) and 21
      Static campaigns; all are duplicates of the manual-run artifacts.

Why URN-direct PATCH and not search-based cleanup: ad account 510956407
has >10K campaign groups, and LinkedIn's q=search endpoint caps offset at
10K — older test entities are beyond reach. PATCH against a known URN is
the only reliable cleanup path on this account.

Usage:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib doppler run \\
        --project outlier-campaign-agent --config dev -- \\
        venv/bin/python scripts/archive_stale_drafts.py [--dry-run]

Status transitions:
    DRAFT → ARCHIVED  (canonical end state for entities that never went live)
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Path so we can import src/* without installing the package.
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from src.linkedin_api import LinkedInClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("archive_stale_drafts")


# ── Entities to archive ─────────────────────────────────────────────────────
# Numeric IDs only. The script appends the right URN prefix per entity type.

# Campaign Groups — both Static and InMail go through /adCampaignGroups
GROUPS_TO_ARCHIVE: list[str] = [
    # ── From original handoff (2026-05-13) ──────────────────────────────────
    "948325076",  # InMail group from failed earlier run
    "949020436",  # InMail group from failed earlier run
    "948821576",  # InMail group from failed earlier run
    "946127606",  # InMail group from failed earlier run
    "946228056",  # InMail group from failed earlier run
    "947828606",  # InMail group from failed earlier run
    "946123876",  # InMail group from first hung run
    "946423926",  # Static group from first hung run

    # ── From cron run 25763743170 (2026-05-12 21:38 UTC) ────────────────────
    "947124486",  # Static
    "946124196",  # Static
    "947424696",  # Static
    "948424096",  # InMail
    "946124186",  # InMail
    "948920456",  # InMail
]

# Campaigns — both Static and InMail go through /adCampaigns
CAMPAIGNS_TO_ARCHIVE: list[str] = [
    # ── From original handoff ───────────────────────────────────────────────
    "779624096",  # Static, from first hung run

    # ── From cron run 25763743170 ───────────────────────────────────────────
    # 21 Static campaigns the cron created across 3 cohorts × geo clusters
    "730024736", "730324726", "730524226", "730824716",
    "779124516", "779424386", "779823796", "779823806", "779823816",
    "779924706", "779924716",
    "849074846", "849084776", "849084796", "849093866", "849114036",
    "849114046", "849132936", "849142556", "849151456", "849161286",
]


def _future_start_ms() -> int:
    """LinkedIn rejects DRAFT→ARCHIVED transitions when `runSchedule.start` is
    in the past (DATE_TOO_EARLY validator). PATCH the start to ~1 hour in the
    future as part of the same partial-update call."""
    import time
    return int(time.time() * 1000) + 60 * 60 * 1000  # now + 1h, ms


def _archive_one(client: LinkedInClient, entity_type: str, entity_id: str, dry_run: bool) -> tuple[str, bool, str]:
    """Returns (entity_id, success, message).

    LinkedIn rules surfaced 2026-05-13:
    - DRAFT groups: must bump runSchedule.start to future before ARCHIVED.
    - DRAFT campaigns: must bump runSchedule.start AND parent group must
      already be non-DRAFT (so archive groups first).
    """
    path = "adCampaignGroups" if entity_type == "group" else "adCampaigns"
    url = client._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/{path}/{entity_id}")
    payload = {"patch": {"$set": {
        "status": "ARCHIVED",
        "runSchedule": {"start": _future_start_ms()},
    }}}
    headers = {"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"}

    if dry_run:
        return entity_id, True, f"[dry-run] would PATCH {entity_type} {entity_id} → ARCHIVED"

    try:
        resp = client._req("POST", url, json=payload, headers=headers)
        if resp.status_code in (200, 204):
            return entity_id, True, f"archived {entity_type} {entity_id}"
        return entity_id, False, f"HTTP {resp.status_code}: {resp.text[:400]}"
    except Exception as exc:
        return entity_id, False, f"exception: {type(exc).__name__}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true",
                        help="Show what would be archived without making changes.")
    args = parser.parse_args()

    log.info(
        "Archive plan: %d campaign groups + %d campaigns (dry_run=%s)",
        len(GROUPS_TO_ARCHIVE), len(CAMPAIGNS_TO_ARCHIVE), args.dry_run,
    )

    client = LinkedInClient(config.LINKEDIN_TOKEN)

    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    for group_id in GROUPS_TO_ARCHIVE:
        entity_id, ok, msg = _archive_one(client, "group", group_id, args.dry_run)
        if ok:
            log.info(msg)
            successes.append(f"group {entity_id}")
        else:
            log.warning("group %s: %s", entity_id, msg)
            failures.append((f"group {entity_id}", msg))

    for campaign_id in CAMPAIGNS_TO_ARCHIVE:
        entity_id, ok, msg = _archive_one(client, "campaign", campaign_id, args.dry_run)
        if ok:
            log.info(msg)
            successes.append(f"campaign {entity_id}")
        else:
            log.warning("campaign %s: %s", entity_id, msg)
            failures.append((f"campaign {entity_id}", msg))

    log.info("=" * 60)
    log.info("Summary: %d succeeded, %d failed", len(successes), len(failures))
    if failures:
        for ent, reason in failures:
            log.warning("  FAIL  %s — %s", ent, reason)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

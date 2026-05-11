"""
One-time migration: backfill the multi-platform columns on the existing
campaign registry (data/campaign_registry.json + .xlsx).

Before this PR the registry only knew about LinkedIn — every row stored its
campaign id in `linkedin_campaign_urn` and creative id in `creative_urn`.
The new schema adds:
  - platform              ("linkedin" | "meta" | "google")
  - platform_campaign_id  (platform-native id; URN / numeric / resource name)
  - platform_creative_id  (platform-native creative or ad id)

This script copies legacy fields into the new columns so older rows show up
correctly in the Sheets view and pass the new lookup helpers.

Idempotent: safe to re-run.

Usage:
    venv/bin/python scripts/migrate_registry_to_multiplatform.py
"""
from __future__ import annotations

import json
import logging
import sys
from pathlib import Path

# Make `src` importable when run as a script
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from src.campaign_registry import _REGISTRY_PATH, _save  # type: ignore  # noqa

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(message)s")
log = logging.getLogger("migrate_registry")


def main() -> int:
    if not _REGISTRY_PATH.exists():
        log.warning("No registry JSON at %s — nothing to migrate.", _REGISTRY_PATH)
        return 0

    records = json.loads(_REGISTRY_PATH.read_text())
    log.info("Loaded %d existing records from %s", len(records), _REGISTRY_PATH)

    from src.campaign_registry import _channel_label  # type: ignore

    changed = 0
    for rec in records:
        # Default platform to "linkedin" only if no value is set yet.
        if not rec.get("platform"):
            rec["platform"] = "linkedin"
            changed += 1
        # Backfill channel label from platform.
        if not rec.get("channel"):
            rec["channel"] = _channel_label(rec.get("platform", "linkedin"))
            changed += 1
        # Backfill platform_campaign_id from legacy linkedin_campaign_urn.
        if not rec.get("platform_campaign_id"):
            legacy = rec.get("linkedin_campaign_urn") or ""
            if legacy:
                rec["platform_campaign_id"] = legacy
                changed += 1
        # Backfill platform_creative_id from legacy creative_urn.
        if not rec.get("platform_creative_id"):
            legacy = rec.get("creative_urn") or ""
            if legacy:
                rec["platform_creative_id"] = legacy
                changed += 1

    log.info("Touched %d field(s) across %d records.", changed, len(records))

    if changed:
        _save(records)
        log.info("Wrote migrated registry back to %s + Excel mirror.", _REGISTRY_PATH)
    else:
        log.info("No changes needed — registry already migrated.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

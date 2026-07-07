"""Reconstruct DELETED prior-generation registry rows for an already-relaunched
ramp (issue #75), so the exact-utm funnel join has a row to attribute the old
generation's historical conversions to — instead of fuzzily merging them onto
the surviving relaunch row.

This writes REGISTRY ROWS ONLY (Postgres `campaigns` table). It creates NO
campaigns on any ad platform — no LinkedIn/Meta/Reddit/Google API create calls,
no spend. Each reconstructed row is flagged status="superseded" (a prior
generation) with a blank platform_campaign_id (the deleted row's real id can't
be recovered from the warehouse), so delivery metrics won't refresh for it, but
its funnel attribution (sign-ups / screening / activations) is correct.

How it works: for each UTM-keyed channel it reads the ramp's distinct
UTM_CAMPAIGN values from SCALE_PROD.VIEW.APPLICATION_CONVERSION (via the same
query_campaign_funnel the daily writeback uses), canonicalizes them, and any
canonical key that has NO matching registry row is proposed as a genesis row.

READ-ONLY by default (prints proposed rows). Pass --apply to upsert.

    doppler run -- venv/bin/python scripts/backfill_genesis_rows.py --ramp GMR-0023
    doppler run -- venv/bin/python scripts/backfill_genesis_rows.py --ramp GMR-0023 --apply
    doppler run -- venv/bin/python scripts/backfill_genesis_rows.py --ramp GMR-0023 --window 240
"""
from __future__ import annotations

import argparse
import hashlib
import sys
from dataclasses import asdict
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

_UTM_CHANNELS = ("linkedin", "meta", "reddit")


def _existing_canonical_keys(ramp: str, platform: str) -> set[str]:
    """Canonical utm keys already present in the registry for (ramp × platform)."""
    from src.campaign_registry import _canonical_utm
    from src.ui_decisions import list_all_campaign_data
    keys: set[str] = set()
    for row in list_all_campaign_data():
        if (row.get("smart_ramp_id") or "") != ramp:
            continue
        if (row.get("platform") or "").lower() != platform:
            continue
        k = _canonical_utm(row.get("utm_campaign") or row.get("campaign_name") or "")
        if k:
            keys.add(k)
    return keys


def build_genesis_rows(ramp: str, window: int, min_apps: int = 10) -> list[dict]:
    """Propose one genesis row per warehouse UTM_CAMPAIGN for this ramp that has
    no matching registry row. Network reads only (Redash + Postgres) — no writes."""
    from src.campaign_registry import CampaignEntry, _canonical_utm
    from src.redash_db import RedashClient

    client = RedashClient()
    proposed: list[dict] = []
    for platform in _UTM_CHANNELS:
        try:
            df = client.query_campaign_funnel(platform, days_back=window)
        except Exception as exc:  # noqa: BLE001
            print(f"  ! {platform}: funnel query failed ({type(exc).__name__}) — skipping")
            continue
        existing = _existing_canonical_keys(ramp, platform)

        # Aggregate the warehouse rows by canonical key, keeping a representative
        # raw string for display; only for THIS ramp; only unmatched keys.
        agg: dict[str, dict] = {}
        for _, r in df.iterrows():
            raw = str(r.get("ad_key") or "")
            key = _canonical_utm(raw)
            segs = key.split(" | ")
            # Guards (validated against GMR-0023 dry-run 2026-07-07):
            #  - must be this ramp;
            #  - 2nd segment must equal the query channel — drops cross-channel
            #    leaks (a linkedin-sourced conversion whose UTM_CAMPAIGN says
            #    "meta"/"joveo"/"ebit") and truncated bare keys;
            #  - already-present canonical keys aren't genesis gaps.
            if not key or ramp.lower() not in key:
                continue
            if len(segs) < 2 or segs[1] != platform:
                continue
            if key in existing:
                continue
            a = agg.setdefault(key, {"raw": raw, "applications": 0, "skill_passes": 0, "activations": 0})
            a["applications"] += int(r.get("applications") or 0)
            a["skill_passes"] += int(r.get("screening_passes") or 0)
            a["activations"]  += int(r.get("activations") or 0)

        for key, a in agg.items():
            # Volume floor — drop truncation/typo noise (1–8 apps). Real campaigns
            # clear this comfortably; the threshold is configurable.
            if a["applications"] < min_apps:
                continue
            uniq = hashlib.md5(key.encode()).hexdigest()[:12]   # stable unique upsert key
            entry = CampaignEntry(
                smart_ramp_id=ramp,
                cohort_signature=a["raw"],
                geo_cluster=uniq,          # part of the unique key — keep distinct
                angle="",
                campaign_type="static",
                channel=platform.title(),
                platform=platform,
                campaign_name=a["raw"],
                utm_campaign=a["raw"],
                platform_campaign_id="",   # deleted generation — real id unrecoverable
                status="superseded",
                deprecation_reason="genesis backfill (#75) — prior generation, deleted on relaunch",
                applications=a["applications"],
                skill_passes=a["skill_passes"],
                activations=a["activations"],
            )
            proposed.append(asdict(entry))
    return proposed


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ramp", required=True, help="Ramp id, e.g. GMR-0023")
    ap.add_argument("--window", type=int, default=180,
                    help="Look-back days for warehouse conversions (default 180 — genesis is old).")
    ap.add_argument("--min-apps", type=int, default=10,
                    help="Skip keys below this sign-up count — drops truncation/typo noise (default 10).")
    ap.add_argument("--apply", action="store_true",
                    help="Upsert rows to Postgres (prod). Omit for a read-only dry-run.")
    args = ap.parse_args()
    ramp = args.ramp.strip().upper()

    rows = build_genesis_rows(ramp, args.window, args.min_apps)
    if not rows:
        print(f"\nNo genesis gaps for {ramp} — every warehouse UTM already has a registry row.")
        return 0

    print(f"\n{'APPLY' if args.apply else 'DRY-RUN'} — {len(rows)} genesis row(s) for {ramp} "
          f"(reconstructed, status=superseded, NO ad-platform campaigns created):\n")
    by_plat: dict[str, int] = {}
    tot_act = 0
    for r in rows:
        by_plat[r["platform"]] = by_plat.get(r["platform"], 0) + 1
        tot_act += r.get("activations") or 0
        print(f"  [{r['platform']:>8}] apps={r['applications'] or 0:>5} "
              f"passes={r['skill_passes'] or 0:>4} activations={r['activations'] or 0:>4}  "
              f"utm={r['campaign_name'][:78]}")
    print(f"\n  by platform: {by_plat} — total activations recovered: {tot_act}")

    if not args.apply:
        print("\nDry-run only — nothing written. Re-run with --apply to upsert to Postgres.")
        return 0

    from src.ui_decisions import upsert_campaign
    written = 0
    for r in rows:
        try:
            upsert_campaign(r)
            written += 1
        except Exception as exc:  # noqa: BLE001
            print(f"  ! upsert failed: {exc}")
    print(f"\nWrote {written}/{len(rows)} genesis rows to Postgres.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

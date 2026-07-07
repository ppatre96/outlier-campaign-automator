"""Backfill registry rows for Reddit campaigns that exist on the Reddit Ads
account but were never recorded in the campaigns registry (issue #75 / #14).

Reddit campaigns created before the registry persisted Reddit rows (or created
outside the pipeline) deliver real traffic but have no registry row, so the
console shows nothing for Reddit and their funnel/delivery metrics are
unattributed. This reconstructs a row per live GMR Reddit campaign from the
Reddit Ads API — keying on the campaign's exact name (== its stamped
utm_campaign), so the #75 exact-utm funnel join attributes its conversions.

READ-ONLY by default: prints the rows it WOULD write. Pass --apply to upsert
them into Postgres (prod). Never deletes anything.

    doppler run -- venv/bin/python scripts/backfill_reddit_registry.py            # dry-run
    doppler run -- venv/bin/python scripts/backfill_reddit_registry.py --apply    # write
    doppler run -- venv/bin/python scripts/backfill_reddit_registry.py --ramp GMR-0011
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config  # noqa: E402


# Campaign name → GMR ramp id ("Scale-GMR-0011 | Reddit | …" → "GMR-0011").
_RAMP_RE = re.compile(r"(GMR-\d{3,4})", re.IGNORECASE)


def _ramp_id(name: str) -> str:
    m = _RAMP_RE.search(name or "")
    return m.group(1).upper() if m else ""


def _seg(name: str, idx: int) -> str:
    parts = [p.strip() for p in (name or "").split("|")]
    return parts[idx] if 0 <= idx < len(parts) else ""


def _status_from_reddit(reddit_status: str) -> str:
    """Map Reddit's lifecycle to a registry status. ARCHIVED → superseded (a
    prior generation, per #75); PAUSED/ACTIVE stay live so delivery keeps
    refreshing."""
    s = (reddit_status or "").upper()
    return "superseded" if s in ("ARCHIVED", "DELETED") else "active"


def _list_reddit_campaigns(client) -> list[dict]:
    client._ensure_init()
    data = client._api("GET", f"/ad_accounts/{client._account_id}/campaigns")
    items = data if isinstance(data, list) else (data.get("data") or [])
    out = []
    for it in items:
        d = it.get("data", it) if isinstance(it, dict) else {}
        cid, name = str(d.get("id") or ""), (d.get("name") or "")
        if not cid or not name:
            continue
        out.append({
            "id": cid, "name": name,
            "status": d.get("configured_status") or d.get("effective_status") or "",
        })
    return out


def build_rows(ramp_filter: str | None = None) -> list[dict]:
    """Construct the registry rows we'd write for GMR Reddit campaigns that have
    a real ramp id. Attaches 30d delivery metrics when the reporting API has
    them. Pure-ish (network reads only) — no writes."""
    from src.reddit_api import RedditClient
    from src.campaign_registry import CampaignEntry
    from dataclasses import asdict

    client = RedditClient()
    campaigns = _list_reddit_campaigns(client)
    metrics = {}
    try:
        metrics = client.fetch_campaign_metrics(30)
    except Exception as exc:  # noqa: BLE001
        print(f"  ! metrics fetch failed ({type(exc).__name__}) — rows built without delivery")

    rows = []
    for c in campaigns:
        rid = _ramp_id(c["name"])
        if not rid:
            continue  # skip non-GMR / PROBE / manual campaigns — not tied to a ramp
        if ramp_filter and rid.upper() != ramp_filter.upper():
            continue
        m = metrics.get(c["id"]) or {}
        entry = CampaignEntry(
            smart_ramp_id=rid,
            cohort_signature=c["name"],
            # geo_cluster is part of the Postgres upsert UNIQUE key — set it to the
            # campaign id so two same-named campaigns don't collide + overwrite.
            # Display uses geo_cluster_label, so the raw id stays invisible.
            geo_cluster=c["id"],
            geo_cluster_label=_seg(c["name"], 7),
            angle="",
            campaign_type="static",
            channel="Reddit",
            platform="reddit",
            campaign_name=c["name"],
            utm_campaign=c["name"],       # == stamped UTM_CAMPAIGN → #75 exact join
            platform_campaign_id=c["id"],
            status=_status_from_reddit(c["status"]),
            deprecation_reason=("archived on Reddit — backfilled as prior generation"
                                if _status_from_reddit(c["status"]) == "superseded" else ""),
            impressions=m.get("impressions"),
            clicks=m.get("clicks"),
            spend_usd=m.get("spend_usd"),
        )
        rows.append(asdict(entry))
    return rows


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true",
                    help="Write rows to Postgres (prod). Omit for a read-only dry-run.")
    ap.add_argument("--ramp", default=None, help="Only this ramp id (e.g. GMR-0011).")
    args = ap.parse_args()

    if not config.REDDIT_API_ENABLED:
        print("REDDIT_API_ENABLED is false — nothing to do.")
        return 0

    rows = build_rows(args.ramp)
    if not rows:
        print("No GMR Reddit campaigns found to backfill.")
        return 0

    live = [r for r in rows if r["status"] == "active"]
    supd = [r for r in rows if r["status"] == "superseded"]
    print(f"\n{'APPLY' if args.apply else 'DRY-RUN'} — {len(rows)} Reddit row(s) "
          f"({len(live)} active, {len(supd)} superseded):\n")
    for r in rows:
        impr = r.get("impressions")
        spend = r.get("spend_usd")
        delivery = (f"impr={impr:,} spend=${spend:,.0f}"
                    if impr else "no recent delivery")
        print(f"  [{r['status']:>10}] {r['smart_ramp_id']} {r['platform_campaign_id']}  {delivery}")
        print(f"               utm={r['campaign_name'][:80]}")

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
            print(f"  ! upsert failed for {r['platform_campaign_id']}: {exc}")
    print(f"\nWrote {written}/{len(rows)} Reddit rows to Postgres.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

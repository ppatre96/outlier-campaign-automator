"""Reconcile the `campaigns` table against Meta's real delivery state.

The shared Postgres `campaigns` table (written by src.ui_decisions.upsert_campaign,
read by the console lib/db.ts:listCampaignsForRamp) marks every Meta row it wrote
as live. But Meta can DELETE or ARCHIVE an ad set / campaign out from under us —
e.g. the verify-and-heal path hard-DELETES an ad set that launched with zero ads
(this is what removed German "v2"). When that happens the console keeps showing a
dead campaign and builds Ads-Manager deep links that open nothing.

This pass queries Meta `effective_status` for each Meta `campaigns` row and writes
it back into the row's `data->>'status'` (already surfaced to the console at
lib/db.ts:1536). The console then hides / labels rows whose status is
`deleted` / `archived` and only links to live campaigns.

- `campaign_type='static'` rows hold a Meta AD-SET id  → checked at level "adset".
- `campaign_type='parent'` rows hold a Meta CAMPAIGN id → checked at level "campaign".

A hard-deleted / non-existent object is treated as `deleted`. Writes are a targeted
`jsonb_set` UPDATE, so all other fields (creatives, utm, audience size) are preserved
and every angle row sharing an ad-set id is updated in one statement.

Idempotent — safe to re-run. Usage:
    doppler run -- python3 scripts/reconcile_meta_status.py --ramp GMR-0023
    doppler run -- python3 scripts/reconcile_meta_status.py            # all ramps
    doppler run -- python3 scripts/reconcile_meta_status.py --dry-run  # report only
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.meta_api import MetaClient  # noqa: E402
from src.ui_decisions import _connect  # noqa: E402


def _normalize(effective_status: str) -> str:
    """Map a Meta effective_status to the lowercase status string the console
    keys off. DELETED/ARCHIVED become the not-live sentinels the console filters
    on; every other state is passed through lowercased (active, paused,
    campaign_paused, adset_paused, …)."""
    s = (effective_status or "").upper()
    if s in ("DELETED", ""):
        # "" only happens on a transient read error; caller skips those.
        return "deleted" if s == "DELETED" else ""
    if s == "ARCHIVED":
        return "archived"
    return s.lower()


def _distinct_meta_containers(ramp_id: str | None) -> list[tuple[str, str, str]]:
    """Return distinct (platform_campaign_id, campaign_type, ramp_id) for Meta
    rows with a non-empty platform_campaign_id."""
    where = "platform = 'meta' AND coalesce(platform_campaign_id, '') <> ''"
    params: list = []
    if ramp_id:
        where += " AND ramp_id = %s"
        params.append(ramp_id)
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            f"SELECT DISTINCT platform_campaign_id, campaign_type, ramp_id "
            f"FROM campaigns WHERE {where} "
            f"ORDER BY ramp_id, campaign_type, platform_campaign_id",
            tuple(params),
        )
        return [(str(r[0]), str(r[1] or ""), str(r[2] or "")) for r in cur.fetchall()]


def _apply_status(platform_campaign_id: str, status: str) -> int:
    """Set data->>'status' for every Meta row with this platform_campaign_id.
    Returns the number of rows updated."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE campaigns "
            "SET data = jsonb_set(coalesce(data, '{}'::jsonb), '{status}', to_jsonb(%s::text), true), "
            "    updated_at = NOW() "
            "WHERE platform = 'meta' AND platform_campaign_id = %s",
            (status, platform_campaign_id),
        )
        n = cur.rowcount
        conn.commit()
        return n


def _apply_link(platform_campaign_id: str, link: str) -> int:
    """Write the correct Ads-Manager deep link into every Meta row with this
    platform_campaign_id (data->>'campaign_link')."""
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "UPDATE campaigns "
            "SET data = jsonb_set(coalesce(data, '{}'::jsonb), '{campaign_link}', to_jsonb(%s::text), true), "
            "    updated_at = NOW() "
            "WHERE platform = 'meta' AND platform_campaign_id = %s",
            (link, platform_campaign_id),
        )
        n = cur.rowcount
        conn.commit()
        return n


def _adset_parent_campaign_id(client, adset_id: str) -> str:
    """The real parent Meta campaign id for an ad set (authoritative — the
    meta_root 'parent' registry rows don't reliably map per-cohort)."""
    try:
        from facebook_business.adobjects.adset import AdSet
        o = AdSet(adset_id).api_get(fields=["campaign_id"])
        return str(o.get("campaign_id") or "")
    except Exception as exc:
        log = __import__("logging").getLogger("reconcile")
        log.warning("parent lookup failed for ad set %s: %s", adset_id, exc)
        return ""


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ramp", default=None, help="Limit to one ramp id (e.g. GMR-0023). Default: all ramps.")
    ap.add_argument("--dry-run", action="store_true", help="Report what would change; write nothing.")
    ap.add_argument("--fix-links", action="store_true",
                    help="Also rewrite each live ad set's campaign_link to its REAL parent "
                         "campaign (selected_campaign_ids=<parent>&selected_adset_ids=<adset>).")
    args = ap.parse_args()
    acct = (config.META_AD_ACCOUNT_ID or "").replace("act_", "")

    containers = _distinct_meta_containers(args.ramp)
    if not containers:
        print(f"No Meta campaigns rows found{f' for {args.ramp}' if args.ramp else ''}.")
        return 0

    client = MetaClient()
    client._ensure_init()

    n_checked = n_dead = n_updated = n_skipped = 0
    for platform_campaign_id, campaign_type, ramp_id in containers:
        level = "campaign" if campaign_type == "parent" else "adset"
        effective = client.get_effective_status(platform_campaign_id, level=level)
        status = _normalize(effective)
        n_checked += 1
        if not status:
            n_skipped += 1
            print(f"  ?  {ramp_id} {campaign_type} {platform_campaign_id} → unreadable (transient); skipped")
            continue
        dead = status in ("deleted", "archived")
        marker = "💀" if dead else "  "
        # Only PERSIST the dead sentinels. Live rows (active/paused) keep the
        # status the pipeline already wrote — the console dashboard's "live"
        # filter keys off that (and treats our intentionally-paused campaigns as
        # live via the pipeline's 'active' default); overwriting it with Meta's
        # raw 'paused' would wrongly drop live campaigns from that view. The goal
        # here is only to stop showing/linking to DELETED/ARCHIVED campaigns.
        if not dead:
            # Live ad set: rewrite its deep link to the REAL parent campaign so the
            # console 'Open' opens the exact campaign (not an ad-set id / empty shell).
            if args.fix_links and campaign_type == "static" and acct:
                parent = _adset_parent_campaign_id(client, platform_campaign_id)
                if parent:
                    link = (
                        f"https://business.facebook.com/adsmanager/manage/ads?act={acct}"
                        f"&selected_campaign_ids={parent}&selected_adset_ids={platform_campaign_id}"
                    )
                    if not args.dry_run:
                        nlinks = _apply_link(platform_campaign_id, link)
                        print(f"  🔗 {ramp_id} static {platform_campaign_id} → parent {parent} ({nlinks} row link(s))")
                    else:
                        print(f"  🔗 {ramp_id} static {platform_campaign_id} → parent {parent} (dry-run link)")
            print(f"  {marker} {ramp_id} {campaign_type} {platform_campaign_id} → {status} (live; unchanged)")
            continue
        n_dead += 1
        if args.dry_run:
            print(f"  {marker} {ramp_id} {campaign_type} {platform_campaign_id} → {status} (dry-run)")
            continue
        updated = _apply_status(platform_campaign_id, status)
        n_updated += updated
        print(f"  {marker} {ramp_id} {campaign_type} {platform_campaign_id} → {status} ({updated} row(s))")

    print(
        f"\nDone. checked={n_checked} dead(deleted/archived)={n_dead} "
        f"rows_updated={n_updated} unreadable_skipped={n_skipped}"
        + (" [DRY-RUN — nothing written]" if args.dry_run else "")
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

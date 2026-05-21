"""Selectively archive Meta campaigns for a ramp — keep the top-N geo clusters,
archive the rest.

Why this exists: GMR-0021 spawned 71 Meta campaigns because MAX_GEO_CLUSTERS
defaulted to "unlimited" (12 natural clusters × 3 cohorts × 3 angles → 108
theoretical). Per Pranav's experimentation rule, the cap should be
3 × 3 × 3 = 27 campaigns/channel. config.py + src/geo_tiers.py were patched
2026-05-22 to enforce that default for FUTURE ramps. This script cleans up the
71 Meta campaigns that already shipped for GMR-0021.

Selection logic:
  1. Read Campaign Registry — filter rows by smart_ramp_id + platform=meta.
  2. Group rows by geo_cluster; count campaigns per cluster.
  3. Rank clusters by campaign count DESC (proxy for cluster size — a healthy
     cluster has 9 rows = 3 cohorts × 3 angles). Keep top --keep N (default 3).
  4. The "archive" set is every row whose geo_cluster is NOT in the keep list.
  5. PATCH each Meta campaign status → DELETED via facebook-business SDK.
     Meta's DELETED == hidden from default views (the LinkedIn ARCHIVED
     equivalent). The campaign isn't actually destroyed.

Override the auto-pick: pass --keep-clusters "slug1,slug2,slug3" to pin the
exact 3 cluster slugs to keep (e.g. "north_america,western_europe,south_asia").

Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/archive_meta_by_ramp.py --ramp-id GMR-0021 --dry-run
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/archive_meta_by_ramp.py --ramp-id GMR-0021
"""
from __future__ import annotations

import argparse
import logging
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from src.sheets import SheetsClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("archive_meta_by_ramp")


def _collect_meta_rows(sheets: SheetsClient, ramp_id: str) -> list[dict]:
    """Read registry rows where smart_ramp_id matches AND platform == 'meta'.

    Registry COLUMNS use title-case headers (see src/campaign_registry.py).
    """
    ws = sheets._get_or_create_registry_tab()
    records = ws.get_all_records()
    matches = []
    for r in records:
        if str(r.get("Smart Ramp Id", "")).strip() != ramp_id:
            continue
        platform = str(r.get("Platform", "")).strip().lower()
        if platform != "meta":
            continue
        matches.append(r)
    return matches


def _campaign_id_from_row(row: dict) -> str:
    """The Meta campaign id lives in 'Platform Campaign Id'. Strip any URN
    prefix defensively even though Meta ids are bare numerics."""
    raw = str(row.get("Platform Campaign Id", "")).strip()
    if not raw:
        return ""
    # Meta ids are pure numeric; LinkedIn URNs have colons. Take the trailing
    # segment as a no-op safety net.
    return raw.rsplit(":", 1)[-1]


def _archive_meta_campaign(campaign_id: str, dry_run: bool) -> tuple[bool, str]:
    """PATCH the Meta campaign status → DELETED. Meta cascades child AdSets
    and Ads automatically when the parent transitions out of PAUSED/ACTIVE."""
    if dry_run:
        return True, f"[dry-run] would set Meta campaign {campaign_id} → DELETED"
    try:
        # Import lazily so --dry-run on a machine without facebook-business
        # installed still gives a usable plan.
        from facebook_business.api import FacebookAdsApi
        from facebook_business.adobjects.campaign import Campaign

        FacebookAdsApi.init(access_token=config.META_ACCESS_TOKEN)
        campaign = Campaign(campaign_id)
        campaign.api_update(params={Campaign.Field.status: Campaign.Status.deleted})
        return True, f"archived Meta campaign {campaign_id}"
    except Exception as exc:
        return False, f"exception: {type(exc).__name__}: {exc}"


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ramp-id", required=True,
                        help="Smart Ramp id whose Meta campaigns to thin (e.g. GMR-0021)")
    parser.add_argument("--keep", type=int, default=3,
                        help="Number of geo clusters to KEEP (default 3, matching MAX_GEO_CLUSTERS)")
    parser.add_argument("--keep-clusters", default="",
                        help="Comma-separated geo_cluster slugs to keep — overrides auto-pick. "
                             "Example: --keep-clusters north_america,western_europe,south_asia")
    parser.add_argument("--dry-run", action="store_true",
                        help="Show the plan without calling Meta")
    args = parser.parse_args()

    log.info("Archive plan ramp=%s keep=%d keep_clusters=%r dry_run=%s",
             args.ramp_id, args.keep, args.keep_clusters or "(auto)", args.dry_run)

    sheets = SheetsClient()
    rows = _collect_meta_rows(sheets, args.ramp_id)
    log.info("Found %d Meta registry rows for ramp_id=%s", len(rows), args.ramp_id)
    if not rows:
        log.warning("Nothing to archive — exiting")
        return 0

    # Bucket rows by geo_cluster slug so we can rank and split keep / archive.
    by_cluster: dict[str, list[dict]] = {}
    for r in rows:
        slug = str(r.get("Geo Cluster", "") or "").strip() or "(unknown)"
        by_cluster.setdefault(slug, []).append(r)

    counts = Counter({slug: len(rs) for slug, rs in by_cluster.items()})
    log.info("Meta geo cluster breakdown for %s:", args.ramp_id)
    for slug, n in counts.most_common():
        label_sample = (by_cluster[slug][0].get("Geo Cluster Label") or "").strip()
        log.info("  %-30s %3d campaigns  (%s)", slug, n, label_sample or "—")

    # Pick which clusters to KEEP. Manual override wins; otherwise auto-pick
    # the top-N by row count (proxy for cluster size).
    if args.keep_clusters.strip():
        keep_slugs = {s.strip() for s in args.keep_clusters.split(",") if s.strip()}
        missing = keep_slugs - set(by_cluster)
        if missing:
            log.warning("--keep-clusters references unknown slug(s): %s — they will be ignored",
                        sorted(missing))
        keep_slugs &= set(by_cluster)
    else:
        keep_slugs = {slug for slug, _ in counts.most_common(args.keep)}

    archive_slugs = set(by_cluster) - keep_slugs

    keep_rows    = [r for r in rows if (str(r.get("Geo Cluster", "") or "").strip() or "(unknown)") in keep_slugs]
    archive_rows = [r for r in rows if (str(r.get("Geo Cluster", "") or "").strip() or "(unknown)") in archive_slugs]

    log.info("=" * 72)
    log.info("KEEP   %d cluster(s) → %d campaign(s): %s",
             len(keep_slugs), len(keep_rows), sorted(keep_slugs))
    log.info("ARCHIVE %d cluster(s) → %d campaign(s): %s",
             len(archive_slugs), len(archive_rows), sorted(archive_slugs))
    log.info("=" * 72)

    # Extract distinct Meta campaign IDs for the archive set. A registry row
    # is one (cohort × geo × angle) campaign, so ids should be unique per row;
    # use a set defensively against accidental dups.
    archive_ids: list[str] = []
    seen: set[str] = set()
    for r in archive_rows:
        cid = _campaign_id_from_row(r)
        if not cid:
            log.warning("Row missing Platform Campaign Id (cohort=%s angle=%s) — skipping",
                        r.get("Cohort Signature"), r.get("Angle"))
            continue
        if cid in seen:
            continue
        seen.add(cid)
        archive_ids.append(cid)

    log.info("Distinct Meta campaign ids to archive: %d", len(archive_ids))

    successes: list[str] = []
    failures: list[tuple[str, str]] = []
    for cid in archive_ids:
        ok, msg = _archive_meta_campaign(cid, args.dry_run)
        if ok:
            log.info(msg)
            successes.append(cid)
        else:
            log.warning("campaign %s: %s", cid, msg)
            failures.append((cid, msg))

    log.info("=" * 72)
    log.info("Summary: %d archived, %d failed (kept %d campaigns across %d clusters)",
             len(successes), len(failures), len(keep_rows), len(keep_slugs))
    if failures:
        for cid, reason in failures:
            log.warning("  FAIL  campaign %s — %s", cid, reason)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""Attach ads to EXISTING Google ad groups for a ramp — no new parent
campaigns or ad groups are created.

Use case: a prior replay (`replay_extra_arms.py`) created the campaign
hierarchy (parent campaign + ad groups) but the ad-creation step failed
(e.g., aspect-ratio rejection before the dual-image upload patch). After
fixing the upload code, this script attaches the missing ads under the
existing ad groups without churning the hierarchy.

How it works:
  1. Discover ad groups under the named parent campaign(s).
  2. Parse each ad group's name to recover the (cohort_stg_id, geo_cluster_label)
     it represents (we added these as suffixes in the 2026-05-18 naming patch).
  3. Read the latest Drive manifests for the ramp (same `--since` / `--until`
     filtering as `replay_extra_arms.py`).
  4. For each manifest entry, find the matching ad group by (stg_id, cluster_label)
     and attach the rendered PNG as an ad via the patched `create_image_ad`
     (which uploads both 1.91:1 landscape and 1:1 square assets).

Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/replay_google_ads_only.py --ramp-id GMR-0020 \\
        --parents 23861903416 23857406543 \\
        --since 2026-05-15T00:00:00 --until 2026-05-18T00:00:00
"""
from __future__ import annotations

import argparse
import io
import json
import logging
import re
import sys
import tempfile
from pathlib import Path
from types import SimpleNamespace

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from googleapiclient.http import MediaIoBaseDownload  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

import main as M  # noqa: E402
from src.copy_adapter import adapt_copy_for_platform  # noqa: E402
from src.google_ads_api import GoogleAdsClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("replay_google_ads_only")

_DRIVE_FILE_RE = re.compile(r"/d/([^/]+)/")


def _drive_id_from_url(url: str) -> str:
    if not url:
        return ""
    m = _DRIVE_FILE_RE.search(url)
    return m.group(1) if m else ""


def _drive_client():
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def _download_to_tmp(drive, file_id: str, dest_dir: Path, suffix: str = ".png") -> Path:
    dest_dir.mkdir(parents=True, exist_ok=True)
    out_path = dest_dir / f"{file_id}{suffix}"
    if out_path.exists() and out_path.stat().st_size > 0:
        return out_path
    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    buf = io.BytesIO()
    downloader = MediaIoBaseDownload(buf, request)
    done = False
    while not done:
        _status, done = downloader.next_chunk()
    out_path.write_bytes(buf.getvalue())
    return out_path


def _list_manifests(drive, ramp_id: str, since: str, until: str) -> list[dict]:
    """Find all Google manifests for the ramp within the window."""
    q = (
        "name = '_manual_handoff.json' and "
        f"fullText contains '\"ramp_id\": \"{ramp_id}\"' and "
        f"fullText contains '\"platform\": \"google\"' and "
        "trashed = false"
    )
    resp = drive.files().list(
        q=q,
        fields="files(id, name, parents, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
        corpora="allDrives",
        pageSize=50,
    ).execute()
    out: list[dict] = []
    for f in resp.get("files", []):
        try:
            content = drive.files().get_media(
                fileId=f["id"], supportsAllDrives=True,
            ).execute()
            data = json.loads(content)
            if (data.get("ramp_id") == ramp_id
                    and data.get("platform") == "google"):
                gen_at = data.get("generated_at") or ""
                if since and gen_at < since:
                    continue
                if until and gen_at >= until:
                    continue
                data["__file_id"] = f["id"]
                out.append(data)
        except Exception as exc:
            log.warning("Could not parse manifest %s: %s", f["id"], exc)
    out.sort(key=lambda m: m.get("generated_at") or "")
    return out


def _discover_ad_groups(client, customer_id: str, parent_ids: list[str],
                       ) -> dict[tuple[str, str], str]:
    """For each ENABLED ad group under any of the named parent campaigns,
    parse its name to extract (cohort_stg_id, geo_cluster_label) and return
    a lookup map → ad_group resource_name.

    Naming convention (set by main.py:_process_extra_platform_arm 2026-05-18):
      `<Smart-Ramp-v2-name> | <STG-id> | <Geo Cluster Label>`
    Example suffix: `... | Agent | STG-20260515-40599 | English-speaking`.
    """
    ga = client.get_service("GoogleAdsService")
    parents_clause = ", ".join(
        f"'customers/{customer_id}/campaigns/{cid}'" for cid in parent_ids
    )
    # Outlier creates ad groups as PAUSED (Pranav's DRAFT-only rule). We
    # include PAUSED here AND filter out REMOVED so we don't try to attach
    # ads to renamed-archived groups from earlier replay attempts.
    query = (
        f"SELECT ad_group.resource_name, ad_group.name "
        f"FROM ad_group "
        f"WHERE ad_group.campaign IN ({parents_clause}) "
        f"AND ad_group.status IN ('ENABLED', 'PAUSED')"
    )
    out: dict[tuple[str, str], str] = {}
    resp = ga.search(customer_id=customer_id, query=query)
    for row in resp:
        name = row.ad_group.name
        rn = row.ad_group.resource_name
        # Parse trailing " | STG-... | <Label>"
        parts = [p.strip() for p in name.split("|")]
        if len(parts) < 2:
            continue
        # Walk from the end to find a part starting with STG- and the next-to-last
        stg = next((p for p in parts if p.startswith("STG-")), "")
        # Last part should be the geo cluster label
        cluster_label = parts[-1] if parts else ""
        if not stg:
            log.warning("Ad group %s name has no STG- suffix — skipping: %r", rn, name)
            continue
        out[(stg, cluster_label)] = rn
        log.info("Discovered ad group %s for (%s, %s)", rn, stg, cluster_label)
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ramp-id", required=True)
    parser.add_argument("--parents", nargs="+", required=True,
                        help="Numeric parent campaign IDs to attach ads under")
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    drive = _drive_client()
    g = GoogleAdsClient()
    sdk = g._ensure_client()
    customer_id = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")

    # 1. Manifests
    manifests = _list_manifests(drive, args.ramp_id, args.since, args.until)
    log.info("Found %d google manifest(s) in window", len(manifests))
    if not manifests:
        return 0

    # 2. Existing ad groups
    ad_groups = _discover_ad_groups(sdk, customer_id, args.parents)
    log.info("Discovered %d ENABLED ad group(s) under parents %s",
             len(ad_groups), args.parents)
    if not ad_groups:
        log.error("No ad groups found — nothing to attach ads to. Exiting.")
        return 1

    png_dir = Path(tempfile.mkdtemp(prefix=f"replay_ads_{args.ramp_id}_"))
    log.info("PNG download dir: %s", png_dir)

    # 3. For each manifest entry, find matching ad group and create ad
    successes: list[str] = []
    failures: list[tuple[str, str]] = []
    no_match: list[tuple[str, str]] = []

    for mi, manifest in enumerate(manifests):
        log.info("Manifest %d/%d (%s, %d entries)", mi+1, len(manifests),
                 manifest["__file_id"], manifest.get("entries_count", 0))
        for ei, entry in enumerate(manifest.get("entries", [])):
            stg = entry.get("cohort_stg_id", "")
            cluster_label = entry.get("geo_cluster_label", "")
            log.info(
                "ENTRY[%d.%d] stg=%s cluster=%r angle=%s has_url=%s",
                mi+1, ei+1, stg, cluster_label,
                entry.get("angle", "?"),
                bool(entry.get("image_drive_url")),
            )
            ad_group_rn = ad_groups.get((stg, cluster_label))
            if not ad_group_rn:
                log.warning("No ad group for (%s, %s) — skipping entry",
                            stg, cluster_label)
                no_match.append((stg, cluster_label))
                continue

            angle = entry.get("angle", "A")
            png_url = entry.get("image_drive_url", "")
            png_id = _drive_id_from_url(png_url)
            if not png_id:
                log.warning("Entry has no image_drive_url — skipping (%s, %s, %s)",
                            stg, cluster_label, angle)
                failures.append((f"{stg}/{cluster_label}/{angle}", "no png_url"))
                continue

            # Reconstruct the variant dict so we can re-run the copy adapter,
            # which produces the multi-headline / multi-description shape the
            # patched create_image_ad expects.
            variant = {
                "angle":         angle,
                "headline":      entry.get("headline", ""),
                "subheadline":   entry.get("subheadline", ""),
                "photo_subject": entry.get("photo_subject", ""),
            }
            variant.update(entry.get("platform_copy") or {})
            platform_copy = adapt_copy_for_platform(variant, "google")

            if args.dry_run:
                log.info(
                    "[dry-run] Would create ad: ad_group=%s angle=%s headlines=%d "
                    "descriptions=%d png_id=%s",
                    ad_group_rn, angle,
                    len(platform_copy.get("headlines") or []),
                    len(platform_copy.get("descriptions") or []),
                    png_id,
                )
                successes.append(f"[dry-run] {stg}/{cluster_label}/{angle}")
                continue

            try:
                local_png = _download_to_tmp(drive, png_id, png_dir)
                image_id = g.upload_image(local_png)
                result = g.create_image_ad(
                    campaign_id=ad_group_rn,   # NB: kwarg name is historical
                    image_id=image_id,
                    headline=(platform_copy.get("headlines") or [""])[0],
                    description=(platform_copy.get("descriptions") or [""])[0],
                    destination_url=entry.get("destination_url") or config.LINKEDIN_DESTINATION,
                    headlines=platform_copy.get("headlines") or [],
                    long_headline=platform_copy.get("long_headline") or "",
                    descriptions=platform_copy.get("descriptions") or [],
                    local_png_path=str(local_png),
                )
                if getattr(result, "status", "") == "ok":
                    log.info("Ad created %s/%s/%s under %s",
                             stg, cluster_label, angle, ad_group_rn)
                    successes.append(f"{stg}/{cluster_label}/{angle}")
                else:
                    log.warning("Ad create returned non-ok for %s/%s/%s: %s",
                                stg, cluster_label, angle,
                                getattr(result, "error_message", ""))
                    failures.append((
                        f"{stg}/{cluster_label}/{angle}",
                        getattr(result, "error_message", "non-ok") or "non-ok",
                    ))
            except Exception as exc:
                log.exception("Ad create FAILED for %s/%s/%s", stg, cluster_label, angle)
                failures.append((
                    f"{stg}/{cluster_label}/{angle}",
                    f"{type(exc).__name__}: {exc}"[:300],
                ))

    log.info("=" * 60)
    log.info("Summary: %d ads created, %d failed, %d no-match",
             len(successes), len(failures), len(no_match))
    for s in successes:
        log.info("  OK  %s", s)
    for k, v in failures:
        log.warning("  FAIL %s — %s", k, v[:200])
    for stg, cl in no_match:
        log.warning("  NO-MATCH (%s, %s)", stg, cl)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

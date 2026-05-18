"""Regenerate creatives + attach Google ads for manifest entries that were
left without a PNG by the original pipeline run.

Use case: a previous full-pipeline run hit transient QC failures (e.g., the
2026-05-18 array-wrapped Vision response bug) and ~10-15% of
(cohort × geo × angle) entries finished with `image_drive_url=""` in the
handoff manifest. After the QC bug is patched, this script:

  1. Reads Drive manifests for the ramp (Google channel) in the time window.
  2. Filters to entries with empty `image_drive_url`.
  3. For each missing entry, re-runs `generate_imagen_creative_with_qc` with
     the original variant copy (preserved in the manifest) so the new PNG
     matches the original ad-group + cohort/geo context.
  4. Uploads the new PNG to Drive at the canonical
     `<ramp>/google/<cohort_geo>/<angle>.png` path.
  5. Finds the corresponding ad group (by cohort_stg_id + geo_cluster_label
     suffix in the name) and attaches the ad via patched `create_image_ad`
     (which now uploads both 1.91:1 landscape + 1:1 square).

Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/regen_missing_creatives.py --ramp-id GMR-0020 \\
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

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import config  # noqa: E402
from googleapiclient.discovery import build  # noqa: E402
from google.oauth2 import service_account  # noqa: E402

from src.copy_adapter import adapt_copy_for_platform  # noqa: E402
from src.figma_creative import rewrite_variant_copy  # noqa: E402
from src.gemini_creative import generate_imagen_creative_with_qc  # noqa: E402
from src.google_ads_api import GoogleAdsClient  # noqa: E402
from src.gdrive import upload_creative_in_hierarchy  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("regen_missing_creatives")


def _drive_client():
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def _list_google_manifests(drive, ramp_id: str, since: str, until: str) -> list[dict]:
    q = (
        "name = '_manual_handoff.json' and "
        f"fullText contains '\"ramp_id\": \"{ramp_id}\"' and "
        f"fullText contains '\"platform\": \"google\"' and "
        "trashed = false"
    )
    resp = drive.files().list(
        q=q, fields="files(id, name, modifiedTime)",
        supportsAllDrives=True, includeItemsFromAllDrives=True,
        corpora="allDrives", pageSize=50,
    ).execute()
    out: list[dict] = []
    for f in resp.get("files", []):
        try:
            content = drive.files().get_media(
                fileId=f["id"], supportsAllDrives=True,
            ).execute()
            data = json.loads(content)
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


def _discover_ad_groups(sdk_client, customer_id: str, parent_ids: list[str]) -> dict[tuple[str, str], str]:
    ga = sdk_client.get_service("GoogleAdsService")
    parents_clause = ", ".join(
        f"'customers/{customer_id}/campaigns/{cid}'" for cid in parent_ids
    )
    query = (
        f"SELECT ad_group.resource_name, ad_group.name FROM ad_group "
        f"WHERE ad_group.campaign IN ({parents_clause}) "
        f"AND ad_group.status IN ('ENABLED', 'PAUSED')"
    )
    out: dict[tuple[str, str], str] = {}
    for row in ga.search(customer_id=customer_id, query=query):
        parts = [p.strip() for p in row.ad_group.name.split("|")]
        stg = next((p for p in parts if p.startswith("STG-")), "")
        cluster_label = parts[-1] if parts else ""
        # Skip renamed-archived ad groups whose cluster label has an
        # `_archived_<ts>` suffix — those are paused leftovers, not v6.
        if "_archived_" in cluster_label:
            continue
        if stg:
            out[(stg, cluster_label)] = row.ad_group.resource_name
    return out


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ramp-id", required=True)
    parser.add_argument("--parents", nargs="+", required=True)
    parser.add_argument("--since", default="")
    parser.add_argument("--until", default="")
    parser.add_argument("--dry-run", action="store_true",
                        help="Identify missing entries and matching ad groups, but skip image gen + ad create")
    args = parser.parse_args()

    drive = _drive_client()
    g = GoogleAdsClient()
    sdk = g._ensure_client()
    customer_id = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")

    manifests = _list_google_manifests(drive, args.ramp_id, args.since, args.until)
    log.info("Found %d google manifest(s) in window", len(manifests))

    ad_groups = _discover_ad_groups(sdk, customer_id, args.parents)
    log.info("Discovered %d ad group(s) under parents", len(ad_groups))

    # Collect missing entries (no image_drive_url) that also have a matching
    # ad group. Dedup by (cohort_stg_id, geo_cluster_label, angle) — manifests
    # can contain the same (cohort × geo × angle) more than once when Phase-1
    # was called multiple times (e.g., from replay runs); regenerating + attaching
    # twice would create duplicate ads under the same ad group.
    missing_entries: list[tuple[dict, str, dict]] = []  # (entry, ad_group_rn, manifest)
    seen: set[tuple[str, str, str]] = set()
    for manifest in manifests:
        for entry in manifest.get("entries", []):
            if entry.get("image_drive_url"):
                continue  # already has a PNG
            stg = entry.get("cohort_stg_id", "")
            cluster_label = entry.get("geo_cluster_label", "")
            angle = entry.get("angle", "A")
            key = (stg, cluster_label, angle)
            if key in seen:
                continue
            ad_group_rn = ad_groups.get((stg, cluster_label))
            if not ad_group_rn:
                log.warning("Entry (%s, %s, %s) has no ad group — skipping",
                            stg, cluster_label, angle)
                continue
            seen.add(key)
            missing_entries.append((entry, ad_group_rn, manifest))

    log.info("Found %d missing entries with matching ad groups", len(missing_entries))
    if not missing_entries:
        return 0

    if args.dry_run:
        for entry, rn, _ in missing_entries:
            log.info("[dry-run] Would regen + attach: cohort=%s geo=%s angle=%s -> %s",
                     entry.get("cohort_name"), entry.get("geo_cluster_label"),
                     entry.get("angle"), rn)
        return 0

    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    for entry, ad_group_rn, manifest in missing_entries:
        stg = entry.get("cohort_stg_id", "")
        cohort_name = entry.get("cohort_name", "")
        cluster_label = entry.get("geo_cluster_label", "")
        angle = entry.get("angle", "A")
        label = f"{stg}/{cluster_label}/{angle}"
        log.info("=== Regenerating %s ===", label)

        # Build the variant dict that generate_imagen_creative_with_qc expects.
        # The manifest preserves the original headline + subheadline that QC
        # rejected the first time around — feeding them back in lets the new
        # creative match the ad group's intended copy.
        variant = {
            "angle":         angle,
            "headline":      entry.get("headline", ""),
            "subheadline":   entry.get("subheadline", ""),
            "photo_subject": entry.get("photo_subject", ""),
        }
        variant.update(entry.get("platform_copy") or {})

        try:
            png_path, qc_report = generate_imagen_creative_with_qc(
                variant=variant,
                copy_rewriter=rewrite_variant_copy,
                photo_subject=variant.get("photo_subject"),
            )
            verdict = (qc_report or {}).get("verdict", "?")
            attempts = (qc_report or {}).get("attempts", "?")
            log.info("Regen verdict=%s after %s attempt(s) — png=%s",
                     verdict, attempts, png_path)
            if not png_path or not Path(str(png_path)).exists():
                failures.append((label, f"no png produced (verdict={verdict})"))
                continue
        except Exception as exc:
            log.exception("Regen failed for %s", label)
            failures.append((label, f"regen exception: {type(exc).__name__}: {exc}"[:200]))
            continue

        # Upload the fresh PNG to Drive at the canonical path so future
        # callers (registry reconciliation, manual review) can find it.
        try:
            cohort_geo_label = f"{stg}__{cluster_label.lower().replace(' ', '_').replace('/', '_')}"
            drive_url = upload_creative_in_hierarchy(
                file_path=Path(str(png_path)),
                ramp_id=args.ramp_id,
                channel="google",
                cohort_geo=cohort_geo_label,
                angle=angle,
            )
            log.info("Uploaded fresh PNG to Drive: %s", drive_url)
        except Exception as exc:
            log.warning("Drive upload failed (non-fatal): %s", exc)
            drive_url = ""

        # Now attach as Google ad under the existing ad group.
        try:
            platform_copy = adapt_copy_for_platform(variant, "google")
            image_id = g.upload_image(png_path)
            result = g.create_image_ad(
                campaign_id=ad_group_rn,
                image_id=image_id,
                headline=(platform_copy.get("headlines") or [""])[0],
                description=(platform_copy.get("descriptions") or [""])[0],
                destination_url=entry.get("destination_url") or config.LINKEDIN_DESTINATION,
                headlines=platform_copy.get("headlines") or [],
                long_headline=platform_copy.get("long_headline") or "",
                descriptions=platform_copy.get("descriptions") or [],
                local_png_path=str(png_path),
            )
            if getattr(result, "status", "") == "ok":
                log.info("Ad attached: %s -> %s", label, ad_group_rn)
                successes.append(label)
            else:
                err = getattr(result, "error_message", "non-ok")
                failures.append((label, f"ad attach: {err}"))
        except Exception as exc:
            log.exception("Ad attach failed for %s", label)
            failures.append((label, f"attach exception: {type(exc).__name__}: {exc}"[:200]))

    log.info("=" * 60)
    log.info("Summary: %d ads created, %d failed", len(successes), len(failures))
    for s in successes:
        log.info("  OK  %s", s)
    for k, v in failures:
        log.warning("  FAIL %s — %s", k, v[:200])
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())

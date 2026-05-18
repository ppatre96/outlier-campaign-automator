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
from src.smart_ramp_client import SmartRampClient  # noqa: E402
from src.utm_builder import build_utm_url, resolve_base_lp_url  # noqa: E402
import main as M  # noqa: E402

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

    # Fetch the Smart Ramp record so we can look up the right matched_domain
    # per cohort. The manifest stores cohort_stg_name (which contains the
    # domain) but the canonical source is the ramp's CohortSpec.matched_domain.
    # We index by cohort_stg_id → matched_domain. STG ids aren't stored on the
    # ramp directly; instead we map by cohort_description prefix match against
    # the manifest's cohort_stg_name (which embeds the description).
    ramp = SmartRampClient().fetch_ramp(args.ramp_id)
    rows = M._ramp_to_rows(ramp) if ramp else []
    # Build a name → row lookup keyed by the first 40 chars of cohort_description
    desc_to_row: dict[str, dict] = {}
    for row in rows:
        cd = (row.get("cohort_description") or "").strip()
        if cd:
            desc_to_row[cd[:40]] = row
    log.info("Built ramp-row lookup: %d rows", len(desc_to_row))

    # Collect missing entries. Each entry tags along with its origin ramp
    # row so we can use the right matched_domain when building the URL.
    # Heuristic: manifest_index 0 → first ramp row that produced specs,
    # last manifest → last ramp row (mirrors the row-matching in
    # replay_extra_arms.py).
    missing_entries: list[tuple[dict, str, dict, dict]] = []  # (entry, ad_group_rn, manifest, row)
    seen: set[tuple[str, str, str]] = set()
    for mi, manifest in enumerate(manifests):
        # Map manifest → ramp row. mi=0 → rows[0]; last mi → rows[-1]; middle → rows[mi].
        if rows:
            if mi == 0:
                matched_row = rows[0]
            elif mi == len(manifests) - 1:
                matched_row = rows[-1]
            else:
                matched_row = rows[mi] if mi < len(rows) else rows[-1]
        else:
            matched_row = {}
        log.info("Manifest %d/%d (%s) -> ramp row matched_domain=%r",
                 mi+1, len(manifests), manifest.get("__file_id", "?"),
                 matched_row.get("matched_domain"))
        for entry in manifest.get("entries", []):
            if entry.get("image_drive_url"):
                continue
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
            missing_entries.append((entry, ad_group_rn, manifest, matched_row))

    log.info("Found %d missing entries with matching ad groups", len(missing_entries))
    if not missing_entries:
        return 0

    if args.dry_run:
        for entry, rn, _, row in missing_entries:
            md = row.get("matched_domain")
            log.info("[dry-run] Would regen + attach: cohort=%s geo=%s angle=%s -> %s (domain=%r)",
                     entry.get("cohort_name"), entry.get("geo_cluster_label"),
                     entry.get("angle"), rn, md)
        return 0

    successes: list[str] = []
    failures: list[tuple[str, str]] = []

    for entry, ad_group_rn, manifest, matched_row in missing_entries:
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

        # Build the proper destination URL via resolve_base_lp_url +
        # build_utm_url — matching what main.py's _process_extra_platform_arm
        # does. Without this, we'd send the auth-walled fallback
        # (LINKEDIN_DESTINATION → app.outlier.ai/contributors/projects) which
        # Google's policy review correctly rejects as DESTINATION_NOT_WORKING.
        base_lp = resolve_base_lp_url(
            campaign_state=matched_row.get("campaign_state"),
            platform="google",
            fallback=matched_row.get("selected_lp_url") or config.LINKEDIN_DESTINATION,
            matched_domain=matched_row.get("matched_domain"),
        )
        utm_url = build_utm_url(
            base_url=base_lp,
            platform="google",
            campaign_name=f"Scale-{args.ramp_id}",  # short campaign tag — full v2 name not needed for UTM
            pod=matched_row.get("job_post_pod"),
            domain=matched_row.get("matched_domain"),
            locale=matched_row.get("job_post_language_code"),
            language="EN",
            utm_content=f"{stg}-google-{angle}",
        ) if base_lp else (matched_row.get("selected_lp_url") or "")
        log.info("URL resolved: base_lp=%s utm_url=%s", base_lp, utm_url[:120])

        # Now attach as Google ad under the existing ad group.
        try:
            platform_copy = adapt_copy_for_platform(variant, "google")
            image_id = g.upload_image(png_path)
            result = g.create_image_ad(
                campaign_id=ad_group_rn,
                image_id=image_id,
                headline=(platform_copy.get("headlines") or [""])[0],
                description=(platform_copy.get("descriptions") or [""])[0],
                destination_url=utm_url or config.LINKEDIN_DESTINATION,
                headlines=platform_copy.get("headlines") or [],
                long_headline=platform_copy.get("long_headline") or "",
                descriptions=platform_copy.get("descriptions") or [],
                local_png_path=str(png_path),
            )
            if getattr(result, "status", "") == "ok":
                log.info("Ad attached: %s -> %s", label, ad_group_rn)
                successes.append(label)
                # Log to Campaign Registry sheet for parity with main pipeline.
                # Without this, the regen'd ads exist on Google Ads but never
                # show up in the registry, leaving Bryan/Diego with a stale view.
                try:
                    from src.campaign_registry import log_campaign as _reg_log
                    _reg_log(
                        smart_ramp_id=args.ramp_id,
                        cohort_id=matched_row.get("cohort_id") or stg,
                        cohort_signature=cohort_name,
                        geo_cluster=entry.get("geo_cluster", ""),
                        geo_cluster_label=cluster_label,
                        geos=entry.get("geos") or [],
                        angle=angle,
                        campaign_type="static",
                        advertised_rate=entry.get("advertised_rate", ""),
                        headline=entry.get("headline", ""),
                        subheadline=entry.get("subheadline", ""),
                        photo_subject=entry.get("photo_subject", ""),
                        creative_image_path=drive_url or "",
                        cohort_geo=cohort_geo_label,
                        platform="google",
                        campaign_name=ad_group_rn.split("/")[-1],
                        platform_campaign_id=ad_group_rn,
                        platform_creative_id=getattr(result, "creative_id", "") or "",
                        gemini_prompt=(qc_report or {}).get("gemini_prompt", "") if qc_report else "",
                    )
                except Exception as _exc:
                    log.warning("Registry log failed for %s (non-fatal): %s", label, _exc)
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

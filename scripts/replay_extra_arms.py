"""Replay Meta + Google arms for a ramp whose LinkedIn arm already ran but
whose extra-platform arms failed (e.g. SDK missing locally during the
original run). Reads the existing `_manual_handoff.json` manifests + the
referenced PNGs from Drive, reconstructs the campaign_specs that the
in-process Static arm produced, and invokes `_process_extra_platform_arm`
directly — without touching LinkedIn.

Why this exists: the original `--ramp-id` rerun couldn't import
facebook_business / google.ads on a local Mac that hadn't pip-installed
them yet. The pipeline gracefully degraded (Phase 1 wrote manifest +
PNGs to Drive), but Phase 2 (the actual Meta/Google API calls) was
skipped. After the SDKs are installed, this script reuses the Phase-1
artifacts to fulfil Phase 2 — no Anthropic + Gemini re-spend, no LinkedIn
duplicates.

Usage:
    doppler run --project outlier-campaign-agent --config dev -- \\
        python scripts/replay_extra_arms.py --ramp-id GMR-0020 \\
        --platforms meta google
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

import main as M  # noqa: E402  — we reuse _process_extra_platform_arm
from src.smart_ramp_client import SmartRampClient  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("replay_extra_arms")


_DRIVE_FILE_RE = re.compile(r"/d/([^/]+)/")


def _drive_id_from_url(url: str) -> str:
    """Extract the file id from a `https://drive.google.com/file/d/<id>/view...` URL."""
    if not url:
        return ""
    m = _DRIVE_FILE_RE.search(url)
    return m.group(1) if m else ""


def _drive_client():
    """Build a Drive v3 client from the service account credentials."""
    creds = service_account.Credentials.from_service_account_file(
        "credentials.json",
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    return build("drive", "v3", credentials=creds)


def _download_to_tmp(drive, file_id: str, dest_dir: Path, suffix: str = ".png") -> Path:
    """Download a Drive file to dest_dir. Returns the local Path."""
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


def _list_manifests(drive, ramp_id: str, platform: str) -> list[dict]:
    """Find every `_manual_handoff.json` under `<ramp_id>/<platform>/` and
    return their parsed contents (one per ramp-row invocation — the original
    pipeline creates a new file per call rather than overwriting)."""
    # Find the platform folder under the ramp folder. We rely on canonical
    # naming `<ramp_id>/<platform>` matching the upload_text_in_hierarchy
    # convention.
    q = (
        f"name = '_manual_handoff.json' and "
        f"fullText contains '\"ramp_id\": \"{ramp_id}\"' and "
        f"fullText contains '\"platform\": \"{platform}\"' and "
        f"trashed = false"
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
            if data.get("ramp_id") == ramp_id and data.get("platform") == platform:
                data["__file_id"] = f["id"]
                data["__modified_at"] = f.get("modifiedTime")
                out.append(data)
        except Exception as exc:
            log.warning("Could not parse manifest %s: %s", f["id"], exc)
    out.sort(key=lambda m: m.get("__modified_at") or "")
    return out


def _reconstruct_specs_for_manifest(manifest: dict, png_dir: Path, drive,
                                    ) -> tuple[list[dict], dict]:
    """Convert one manifest's `entries` list into the campaign_specs shape
    `_process_extra_platform_arm` expects. Also returns a "row-context" dict
    with the cohort + geo + LP info shared across all entries in that
    manifest (used to build naming_meta + cohort_id_override).

    Each manifest came from ONE (ramp-row × platform) invocation, so all
    entries share row-level context (cohort_id_override == first entry's
    cohort_stg_id is fine if we treat the FIRST cohort as the row's primary)."""
    entries = manifest.get("entries", [])
    if not entries:
        return [], {}

    # Group entries by (cohort_stg_id × geo_cluster) → 3-variant block
    by_cohort_geo: dict[tuple[str, str], list[dict]] = {}
    for e in entries:
        key = (e["cohort_stg_id"], e["geo_cluster"])
        by_cohort_geo.setdefault(key, []).append(e)

    angle_order = ["A", "B", "C"]
    specs: list[dict] = []
    for (stg_id, cluster), group in by_cohort_geo.items():
        # Sort by angle so variants[0]=A, [1]=B, [2]=C
        group.sort(key=lambda e: angle_order.index(e["angle"])
                                  if e["angle"] in angle_order else 99)
        first = group[0]
        cohort = SimpleNamespace(
            id=first["cohort_stg_id"],
            _stg_id=first["cohort_stg_id"],
            _stg_name=first["cohort_stg_name"],
            name=first["cohort_name"],
            rules=[tuple(r) if isinstance(r, list) else r for r in (first.get("rules") or [])],
            cohort_description="",
            exclude_add=[],
            exclude_remove=[],
            lift_pp=0.0,
        )
        geo_group = SimpleNamespace(
            cluster=first["geo_cluster"],
            cluster_label=first["geo_cluster_label"],
            geos=first.get("geos") or [],
            advertised_rate=first.get("advertised_rate", ""),
            campaign_suffix=first["geo_cluster"],
            median_multiplier=1.0,
            icp_hint=None,
        )
        # Build the variants list — one slot per angle. Any missing angle is
        # filled with an empty dict so the indexed lookup downstream doesn't
        # IndexError; in practice the original pipeline always produced all 3.
        variants_by_angle: dict[str, dict] = {}
        for e in group:
            v = {
                "angle":         e["angle"],
                "headline":      e.get("headline", ""),
                "subheadline":   e.get("subheadline", ""),
                "photo_subject": e.get("photo_subject", ""),
            }
            v.update(e.get("platform_copy") or {})
            variants_by_angle[e["angle"]] = v
        variants = [variants_by_angle.get(a, {}) for a in angle_order]

        # One spec per angle, all sharing the same variants list (matches the
        # original Static-arm fan-out).
        for e in group:
            png_url = e.get("image_drive_url") or ""
            png_id = _drive_id_from_url(png_url)
            png_path = ""
            if png_id:
                try:
                    png_path = str(_download_to_tmp(drive, png_id, png_dir))
                except Exception as exc:
                    log.warning(
                        "Could not download PNG %s for cohort=%s angle=%s: %s",
                        png_id, first["cohort_name"], e["angle"], exc,
                    )
            specs.append({
                "cohort":      cohort,
                "geo_group":   geo_group,
                "geo_label":   first["geo_cluster_label"],
                "group_geos":  first.get("geos") or [],
                "angle_idx":   angle_order.index(e["angle"]) if e["angle"] in angle_order else 0,
                "angle_label": e["angle"],
                "variants":    variants,
                "png_path":    png_path,
            })

    # Row-context: take any entry — destination_url + first stg_id seed.
    row_context = {
        "cohort_id_override":      entries[0]["cohort_stg_id"],
        "destination_url_override": entries[0].get("destination_url") or "",
        "first_cohort_stg_name":    entries[0].get("cohort_stg_name", ""),
    }
    return specs, row_context


def _naming_meta_for_row(row: dict) -> dict:
    """Mirror _process_row_both_modes:3362-3369."""
    return {
        "submitted_at":   row.get("ramp_submitted_at", "") or "",
        "pod":            row.get("job_post_pod"),
        "domain":         row.get("matched_domain"),
        "locale":         row.get("job_post_language_code"),
        "included_geos":  row.get("included_geos") or [],
        "campaign_state": row.get("campaign_state"),
    }


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ramp-id", required=True,
                        help="Smart Ramp id whose extra-platform arms to replay (e.g. GMR-0020)")
    parser.add_argument("--platforms", nargs="+", default=["meta", "google"],
                        choices=["meta", "google"],
                        help="Which non-LinkedIn platforms to replay (default: meta google)")
    parser.add_argument("--since", default="",
                        help=("ISO-8601 cutoff (e.g. 2026-05-15T00:00:00Z). Only manifests "
                              "with generated_at >= cutoff are replayed. Critical when a "
                              "ramp has been re-run multiple times — without this, stale "
                              "manifests from earlier runs would also produce campaigns."))
    parser.add_argument("--dry-run", action="store_true",
                        help="Read manifests + download PNGs but don't call platform APIs")
    args = parser.parse_args()

    drive = _drive_client()
    smart = SmartRampClient()
    ramp = smart.fetch_ramp(args.ramp_id)
    if not ramp:
        log.error("Could not fetch ramp %s", args.ramp_id)
        return 1
    rows = M._ramp_to_rows(ramp)
    log.info("Fetched ramp %s with %d cohort row(s)", args.ramp_id, len(rows))

    # Build per-row metadata lookup keyed by cohort_id so we can match a
    # manifest (which carries a primary cohort_stg_id) back to its row.
    rows_by_cohort_id: dict[str, dict] = {r.get("cohort_id"): r for r in rows if r.get("cohort_id")}

    clients = M._build_extra_platform_clients(args.platforms)
    if not clients:
        log.error("No platform clients constructed — env vars likely missing. Cannot replay.")
        return 1
    log.info("Built clients for platforms: %s", list(clients.keys()))

    png_dir = Path(tempfile.mkdtemp(prefix=f"replay_{args.ramp_id}_"))
    log.info("PNG download dir: %s", png_dir)

    summary: list[str] = []
    failures: list[tuple[str, str, str]] = []

    for platform in args.platforms:
        if platform not in clients:
            log.warning("Skipping %s — client not built", platform)
            continue
        manifests = _list_manifests(drive, args.ramp_id, platform)
        log.info("Found %d %s manifest(s) for ramp %s", len(manifests), platform, args.ramp_id)
        if args.since:
            before = len(manifests)
            manifests = [
                m for m in manifests
                if (m.get("generated_at") or "") >= args.since
            ]
            log.info(
                "Filtered to %d manifest(s) with generated_at >= %s (dropped %d stale)",
                len(manifests), args.since, before - len(manifests),
            )
        if not manifests:
            continue

        for mi, manifest in enumerate(manifests):
            log.info(
                "Manifest %d/%d for %s: file_id=%s entries_count=%d generated_at=%s",
                mi+1, len(manifests), platform, manifest["__file_id"],
                manifest.get("entries_count", 0), manifest.get("generated_at"),
            )
            specs, row_ctx = _reconstruct_specs_for_manifest(manifest, png_dir, drive)
            log.info(
                "Reconstructed %d spec(s) for %s manifest %s",
                len(specs), platform, manifest["__file_id"],
            )

            # Row matching is messy: manifest entries carry the STG cohort id
            # (Stage-A output) but the ramp's CohortSpec uses an unrelated
            # uuid. Stage A's mapping STG→Smart-Ramp-row isn't persisted.
            #
            # Pragmatic heuristic: manifests appear in chronological order in
            # `manifests`, and the pipeline processes ramp rows in order with
            # dedup'd rows skipped. For an N-manifest, M-row ramp where
            # M >= N (some rows produced no manifest):
            #   - manifest 0 → rows[0] (first row always produces if not empty)
            #   - manifest N-1 → rows[-1] (last manifest came from last row
            #     that wasn't fully deduped)
            #   - middle manifests fall back to rows[mi] best-effort
            # Worst case: naming_meta encodes the wrong row's pod/domain/locale.
            # The campaign is still CREATED — just slightly mislabelled.
            stg_ids = sorted({s["cohort"]._stg_id for s in specs})
            log.info("Manifest spans %d cohort_stg_id(s): %s", len(stg_ids), stg_ids)
            if mi == 0:
                matched_row = rows[0] if rows else {}
            elif mi == len(manifests) - 1:
                matched_row = rows[-1] if rows else {}
            else:
                matched_row = rows[mi] if mi < len(rows) else (rows[-1] if rows else {})
            log.info(
                "Manifest %d → row index %s (cohort_description=%r)",
                mi+1,
                rows.index(matched_row) if matched_row in rows else "?",
                (matched_row.get("cohort_description") or "")[:60],
            )
            naming_meta = _naming_meta_for_row(matched_row)
            cohort_id_override = matched_row.get("cohort_id") or row_ctx["cohort_id_override"]
            destination_url_override = (matched_row.get("selected_lp_url")
                                         or row_ctx["destination_url_override"])
            flow_id = matched_row.get("flow_id", "") or ""
            unique_id = f"REPLAY_{args.ramp_id}_{platform}_{mi}"

            if args.dry_run:
                log.info(
                    "[dry-run] Would call _process_extra_platform_arm(%s) "
                    "with %d specs (cohort_id=%s flow_id=%s lp=%s)",
                    platform, len(specs), cohort_id_override, flow_id,
                    destination_url_override,
                )
                summary.append(f"[dry-run] {platform} manifest {mi+1}: {len(specs)} specs")
                continue

            try:
                r = M._process_extra_platform_arm(
                    platform=platform,
                    client=clients[platform]["client"],
                    resolver=clients[platform]["resolver"],
                    campaign_specs=specs,
                    flow_id=flow_id,
                    location="",
                    ramp_id=args.ramp_id,
                    cohort_id_override=cohort_id_override,
                    destination_url_override=destination_url_override,
                    unique_id=unique_id,
                    naming_meta=naming_meta,
                )
                campaigns = r.get("campaigns") or []
                groups = r.get("campaign_groups") or []
                log.info(
                    "%s replay manifest %d done: %d campaign(s), %d group(s), manual_handoff=%s",
                    platform, mi+1, len(campaigns), len(groups),
                    r.get("manual_handoff_url") or "(none)",
                )
                summary.append(
                    f"{platform} manifest {mi+1}: {len(campaigns)} campaigns, "
                    f"{len(groups)} groups created"
                )
                if r.get("manual_handoff_url"):
                    summary.append(f"  manual-handoff: {r['manual_handoff_url']}")
            except Exception as exc:
                log.exception("%s replay manifest %d FAILED", platform, mi+1)
                failures.append((platform, str(mi+1), f"{type(exc).__name__}: {exc}"))

    log.info("=" * 60)
    log.info("Replay summary:")
    for s in summary:
        log.info("  %s", s)
    if failures:
        log.warning("Failures: %d", len(failures))
        for f in failures:
            log.warning("  %s manifest %s: %s", *f)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())

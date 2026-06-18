"""
scripts/generate_secondary_creatives.py
========================================

Generate FB / IG / TikTok ad creatives for a given ramp_id WITHOUT touching
the live LinkedIn / Meta / Google ad APIs. Outputs PNGs to Drive only.

Surfaced 2026-05-20 for GMR-0021 (Short-Form Video Creators) — the live
pipeline only renders LinkedIn 1200×1200 creatives, so anything destined for
FB Feed, IG Feed, or TikTok needs to be produced out-of-band until those arms
are wired natively.

Architecture
------------
1. Fetch ramp via SmartRampClient.fetch_ramp(ramp_id)
2. For each Smart Ramp cohort:
   a. Build minimal Cohort (description-driven — no Snowflake stage A/B/C)
   b. Run group_geos_for_campaigns to derive ethnic clusters + advertised rates
   c. For each geo cluster:
      - build_copy_variants → 3 A/B/C variants
      - For each angle × each TARGET ASPECT (Gemini photo generated once per
        aspect, shared between platforms that share the aspect):
        - generate_imagen_photo (returns raw bg_image)
        - For each platform consuming that aspect:
          - compose_ad_for_platform → temp PNG
          - upload_creative_in_hierarchy → <ramp>/<platform>/<cohort_geo>/<angle>.png

Cost
----
For GMR-0021 (1 cohort × N geo clusters × 3 angles × 2 aspects):
  - Gemini photos: N × 3 × 2  (FB + IG share the 4:5 photo)
  - Claude copy:   N
  - Drive uploads: N × 3 × #platforms  (one PNG per platform even when bytes
                                        are identical)

Platforms / aspects
-------------------
  - fb     → 4:5 (1080×1350)            Meta Feed best-converting per 2026 Meta guidance
  - ig     → 4:5 (1080×1350)            Same as FB; identical photo, separate Drive folder
  - tiktok → 9:16 (1080×1920)           In-feed / Carousel primary
            + 1:1  (1080×1080)          square fallback for crop placements
                               TikTok in-feed is video-first → single static images can't run
                               standalone, so we render BOTH ratios (filenames suffixed
                               A_9x16.png / A_1x1.png) and drop a _HANDOFF.md in <ramp>/tiktok/
                               with Carousel / video-base-frame guidance.

Usage
-----
  doppler run --config dev -- python3 scripts/generate_secondary_creatives.py \\
      --ramp-id GMR-0021 \\
      --platforms fb,ig,tiktok

  doppler run --config dev -- python3 scripts/generate_secondary_creatives.py \\
      --ramp-id GMR-0021 --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

# Bootstrap so `from src.…` works when invoked as a script
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.smart_ramp_client import SmartRampClient, CohortSpec  # noqa: E402
from src.analysis import Cohort  # noqa: E402
from src.geo_tiers import group_geos_for_campaigns  # noqa: E402
from src.figma_creative import build_copy_variants  # noqa: E402
from src.gemini_creative import generate_imagen_photo  # noqa: E402
from src.image_adapter import compose_ad_for_platform  # noqa: E402
from src.gdrive import upload_creative_in_hierarchy, upload_text_in_hierarchy  # noqa: E402

log = logging.getLogger("generate_secondary_creatives")


# Platform → target aspect ratio(s) (integer-tuple form so we can compare
# cheaply and use the same key in image_adapter._PIXEL_DIMS). A platform may
# render MULTIPLE aspects — TikTok ships both 9:16 (in-feed / Carousel) and 1:1
# (square fallback for placements that crop). Photos are still generated once
# per (angle, aspect) and shared across platforms that request the same aspect.
#
# Sourced from 2026-05-20 web research + 2026-06 TikTok spec refresh:
#   - FB/IG Feed at 4:5 → ~33% more vertical real-estate than 1:1 (Meta Help)
#   - TikTok in-feed image / Carousel slide → 9:16 1080×1920 optimal (TikTok spec)
#   - TikTok 1:1 1080×1080 → fallback for placements that crop to square
PLATFORM_ASPECTS: dict[str, list[tuple[int, int]]] = {
    "fb":     [(4, 5)],
    "ig":     [(4, 5)],
    "tiktok": [(9, 16), (1, 1)],
}


def _angle_stem(angle: str, platform: str, aspect: tuple[int, int]) -> str:
    """Drive filename stem for one creative.

    Single-aspect platforms (fb, ig) keep the bare angle label ("A") for
    backwards compatibility. Multi-aspect platforms (TikTok renders both 9:16
    and 1:1) get an aspect suffix so the two PNGs don't collide in the same
    cohort folder: "A_9x16", "A_1x1".
    """
    if len(PLATFORM_ASPECTS.get(platform, [])) > 1:
        return f"{angle}_{aspect[0]}x{aspect[1]}"
    return angle


_TIKTOK_HANDOFF_MD = """\
# TikTok creative handoff — {ramp_id}

These PNGs are STATIC images. TikTok in-feed is video-first: a standalone
single image cannot run as a normal in-feed ad. Use them one of two ways:

1. Carousel ad — combine the per-angle slides into a TikTok Carousel
   (2–35 images). Prefer the 9:16 slides; 1:1 is a crop fallback.
2. Video base frame — drop a slide into CapCut or TikTok's editor as the
   opening / closing frame, add 3–5s of motion plus a trending audio track,
   and export as 9:16 1080×1920 MP4.

## Files in this folder
Per angle (A/B/C), two ratios are rendered:
  - <angle>_9x16.png  →  1080×1920  (in-feed / Carousel primary)
  - <angle>_1x1.png   →  1080×1080  (square fallback for crop placements)

## Specs
  - Keep text clear of ~150px top / ~175px bottom (UI chrome + caption overlay).
  - Target under 500KB per image where possible.
  - Do NOT upload these as standalone single-image in-feed ads.

Generated by scripts/generate_secondary_creatives.py (Drive-only; no TikTok API).
"""


def _write_tiktok_handoff(ramp_id: str, dry_run: bool) -> None:
    """Drop a video-first guidance note in <ramp>/tiktok/_HANDOFF.md so the
    human uploading these static slides knows TikTok in-feed is video-first
    and how to use the two ratios."""
    if dry_run:
        log.info("[dry-run] WOULD write %s/tiktok/_HANDOFF.md", ramp_id)
        return
    try:
        url = upload_text_in_hierarchy(
            _TIKTOK_HANDOFF_MD.format(ramp_id=ramp_id),
            ramp_id=ramp_id,
            channel="tiktok",
            filename="_HANDOFF.md",
            mimetype="text/markdown",
        )
        log.info("TikTok handoff note → %s/tiktok/_HANDOFF.md (%s)", ramp_id, url or "drive-disabled")
    except Exception as exc:
        log.exception("Failed to write TikTok handoff note (non-fatal): %s", exc)


def _build_minimal_cohort(spec: CohortSpec) -> Cohort:
    """Construct a Cohort suitable for build_copy_variants without running
    stage A/B/C analysis.

    `rules` stays empty — the copy LLM falls back to `description_hint`
    (cohort_description from Smart Ramp) plus the geo ICP hint. This is
    deliberate: re-running Snowflake stage analysis just to render a few
    extra creatives is wasted compute when the requester's own description
    is more specific than any resume-mined signal anyway.
    """
    return Cohort(
        name=spec.cohort_description or "Cohort",
        rules=[],
    )


def _process_cohort_geo(
    *,
    ramp_id: str,
    cohort_spec: CohortSpec,
    cohort: Cohort,
    geo_group,
    platforms: list[str],
    angle_labels: list[str],
    dry_run: bool,
) -> int:
    """Process one (cohort × geo cluster). Returns count of PNGs uploaded."""
    log.info(
        "Build copy variants for %r × %s (rate=%s, %d geos)",
        cohort.name, geo_group.cluster_label,
        geo_group.advertised_rate, len(geo_group.geos),
    )

    if dry_run:
        # Even in dry-run we want the plan to show fan-out, so synthesize the
        # would-be Drive paths without spending Gemini.
        for angle in angle_labels:
            for platform in platforms:
                for aspect in PLATFORM_ASPECTS[platform]:
                    log.info(
                        "[dry-run] WOULD upload %s/%s/%s/%s.png  (aspect=%s)",
                        ramp_id, platform, geo_group.cluster,
                        _angle_stem(angle, platform, aspect), aspect,
                    )
        return 0

    variants = build_copy_variants(
        cohort, {},
        geos=geo_group.geos,
        description_hint=cohort_spec.cohort_description,
        hourly_rate=geo_group.advertised_rate,
        geo_icp_hint=geo_group.icp_hint,
    )
    if not variants:
        log.warning(
            "build_copy_variants returned 0 variants for %r × %s — skipping",
            cohort.name, geo_group.cluster_label,
        )
        return 0
    variants_by_angle: dict[str, dict] = {
        v.get("angle", ""): v for v in variants if isinstance(v, dict)
    }

    # Group platforms by aspect so we only call Gemini once per (angle, aspect)
    # even when multiple platforms render the same aspect (FB + IG at 4:5). A
    # platform requesting several aspects (TikTok = 9:16 + 1:1) appears under
    # each of its aspects.
    aspects_to_platforms: dict[tuple[int, int], list[str]] = {}
    for p in platforms:
        for aspect in PLATFORM_ASPECTS[p]:
            aspects_to_platforms.setdefault(aspect, []).append(p)

    uploaded = 0
    for angle in angle_labels:
        variant = variants_by_angle.get(angle)
        if not variant:
            log.warning("No variant for angle=%s — skipping", angle)
            continue

        for aspect, plats in aspects_to_platforms.items():
            log.info(
                "Gemini photo: cohort=%r angle=%s aspect=%s (used by %s)",
                cohort.name, angle, aspect, plats,
            )
            try:
                bg_image = generate_imagen_photo(variant, aspect=aspect)
            except Exception as exc:
                log.exception(
                    "Gemini photo generation FAILED for angle=%s aspect=%s: %s",
                    angle, aspect, exc,
                )
                continue

            for platform in plats:
                try:
                    png_path = compose_ad_for_platform(
                        bg_image=bg_image,
                        copy_variant=variant,
                        platform=platform,
                        angle=angle,
                        aspect=aspect,
                    )
                except Exception as exc:
                    log.exception(
                        "compose_ad_for_platform FAILED platform=%s angle=%s: %s",
                        platform, angle, exc,
                    )
                    continue

                stem = _angle_stem(angle, platform, aspect)
                try:
                    url = upload_creative_in_hierarchy(
                        png_path,
                        ramp_id=ramp_id,
                        channel=platform,
                        cohort_geo=geo_group.cluster,
                        angle=stem,
                    )
                    log.info(
                        "Drive upload OK: %s/%s/%s/%s.png → %s",
                        ramp_id, platform, geo_group.cluster, stem,
                        (url[:60] + "…") if len(url) > 60 else url,
                    )
                    uploaded += 1
                except Exception as exc:
                    log.exception(
                        "Drive upload FAILED platform=%s angle=%s: %s",
                        platform, angle, exc,
                    )
    return uploaded


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Generate FB / IG / TikTok creatives for a Smart Ramp ramp_id (Drive-only, no platform API calls)",
    )
    parser.add_argument("--ramp-id", required=True, help="Smart Ramp id, e.g. GMR-0021")
    parser.add_argument(
        "--platforms", default="fb,ig,tiktok",
        help="Comma-separated subset of fb,ig,tiktok (default: all)",
    )
    parser.add_argument(
        "--angles", default="A,B,C",
        help="Comma-separated angle labels (default: A,B,C)",
    )
    parser.add_argument(
        "--max-clusters", type=int, default=0,
        help="Process at most N largest geo clusters per cohort (0 = no limit). "
             "Use --max-clusters 1 for a single-cluster smoke test.",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Print the upload plan; skip Gemini calls and Drive uploads",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    platforms = [p.strip() for p in args.platforms.split(",") if p.strip()]
    angles = [a.strip() for a in args.angles.split(",") if a.strip()]
    unknown = [p for p in platforms if p not in PLATFORM_ASPECTS]
    if unknown:
        log.error("Unknown platforms: %s (supported: %s)", unknown, list(PLATFORM_ASPECTS))
        return 2

    client = SmartRampClient()
    ramp = client.fetch_ramp(args.ramp_id)
    if ramp is None:
        log.error(
            "Smart Ramp returned None for %s — check Doppler "
            "VERCEL_AUTOMATION_BYPASS_SECRET and that the ramp exists",
            args.ramp_id,
        )
        return 2
    log.info(
        "Loaded ramp %s — '%s' (%d cohort(s))",
        ramp.id, ramp.summary[:80], len(ramp.cohorts),
    )

    # Pay-rate resolution mirrors main.py's _process_row_both_modes priority
    # chain (per feedback_smart_ramp_authoritative_data — pull from Smart Ramp,
    # NEVER guess/hardcode):
    #   1. OUTLIER_BASE_RATE_USD env override (manual one-off; US baseline →
    #      gets the country pay-multiplier)
    #   2. Smart Ramp per-cohort job_post_pay_rates (authoritative, geo-specific
    #      → must NOT be re-multiplied or $5-rounded)
    #   3. None → rate-free copy
    # The env override is global; the Smart Ramp rate is resolved per cohort
    # inside the loop because each language cohort can carry its own rate.
    import os as _os
    from src.attribution_resolver import parse_job_post_pay_rate  # noqa: E402

    env_rate: float | None = None
    _env_rate_raw = (_os.environ.get("OUTLIER_BASE_RATE_USD") or "").strip()
    if _env_rate_raw:
        try:
            env_rate = float(_env_rate_raw)
            log.info("OUTLIER_BASE_RATE_USD env override → base_rate_usd=$%.2f/hr (applies to all cohorts)", env_rate)
        except ValueError:
            log.warning("OUTLIER_BASE_RATE_USD=%r is not a valid float — ignoring", _env_rate_raw)

    total_uploaded = 0
    for cohort_spec in ramp.cohorts:
        log.info(
            "Cohort id=%s desc=%r target=%s geos=%d",
            cohort_spec.id[:8],
            cohort_spec.cohort_description[:80],
            cohort_spec.target_activations,
            len(cohort_spec.included_geos),
        )
        cohort = _build_minimal_cohort(cohort_spec)

        # Per-cohort pay rate: env override wins (US baseline → apply multiplier);
        # else the Smart Ramp job_post_pay_rate (geo-specific → no multiplier).
        if env_rate is not None:
            base_rate_usd: float | None = env_rate
            rate_geo_specific = False
        else:
            base_rate_usd = parse_job_post_pay_rate(cohort_spec.job_post_pay_rates)
            rate_geo_specific = base_rate_usd is not None
            if base_rate_usd is not None:
                log.info(
                    "  pay rate from Smart Ramp job_post_pay_rates %r → $%.2f/hr (geo-specific)",
                    cohort_spec.job_post_pay_rates, base_rate_usd,
                )
            else:
                log.warning(
                    "  base_rate_usd unresolved for cohort %s (no env override, no Smart Ramp "
                    "job_post_pay_rates) — copy will skip $/hr mentions",
                    cohort_spec.id[:8],
                )

        geo_groups = group_geos_for_campaigns(
            cohort_spec.included_geos, base_rate_usd=base_rate_usd,
            apply_geo_multiplier=not rate_geo_specific,
        )
        log.info(
            "  → %d geo cluster(s): %s",
            len(geo_groups),
            [(g.cluster, len(g.geos), g.advertised_rate) for g in geo_groups],
        )
        if args.max_clusters and len(geo_groups) > args.max_clusters:
            # Pick the N largest (most geos) so the smoke test exercises the
            # biggest cohort first. Stable sort by descending geo count.
            geo_groups = sorted(geo_groups, key=lambda g: -len(g.geos))[: args.max_clusters]
            log.info(
                "  → --max-clusters %d → keeping %s",
                args.max_clusters, [g.cluster for g in geo_groups],
            )
        if not geo_groups:
            log.warning(
                "No geo clusters survived filtering — skipping cohort %s",
                cohort_spec.id[:8],
            )
            continue

        for geo_group in geo_groups:
            total_uploaded += _process_cohort_geo(
                ramp_id=args.ramp_id,
                cohort_spec=cohort_spec,
                cohort=cohort,
                geo_group=geo_group,
                platforms=platforms,
                angle_labels=angles,
                dry_run=args.dry_run,
            )

    # TikTok ships static slides, but in-feed is video-first — leave a handoff
    # note in the tiktok folder so whoever uploads knows how to use them.
    if "tiktok" in platforms:
        _write_tiktok_handoff(args.ramp_id, args.dry_run)

    log.info(
        "DONE — %d PNG(s) uploaded to Drive under %s/{%s}/",
        total_uploaded, args.ramp_id, ",".join(platforms),
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())

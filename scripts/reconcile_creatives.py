"""scripts/reconcile_creatives.py
================================

Backfill empty `creative_image_path` entries in the Campaign Registry by
walking Drive at the canonical `<ramp>/<platform>/<cohort_geo>/<angle>.png`
hierarchy. Wraps `src.campaign_registry.reconcile_creative_paths`.

Why it's needed: when the pipeline runs, the registry row is written at
campaign-creation time. If the PNG render or Drive upload completes AFTER
that (QC retry, slow Drive sync, async upload), the row ends up with an
empty `creative_image_path` even though the PNG eventually lands in Drive.
The console then renders a placeholder instead of the thumbnail.

This script is idempotent — running twice patches nothing new on the second
pass.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/reconcile_creatives.py GMR-0021

    # both LinkedIn + Meta:
    … reconcile_creatives.py GMR-0021 --platforms linkedin,meta
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("reconcile_creatives")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("ramp_id", help="Smart Ramp ID, e.g. GMR-0021")
    parser.add_argument(
        "--platforms",
        default="linkedin,meta",
        help="Comma-separated platforms to reconcile (default: linkedin,meta).",
    )
    parser.add_argument(
        "--legacy-positional",
        action="store_true",
        help="Use legacy positional matching for rows missing cohort_geo. Off by default.",
    )
    args = parser.parse_args()

    from src.campaign_registry import reconcile_creative_paths  # noqa: E402

    grand = {"patched": 0, "unmatched": 0, "ambiguous_legacy": 0}
    for platform in [p.strip() for p in args.platforms.split(",") if p.strip()]:
        log.info("─── Reconciling %s for ramp %s ───", platform, args.ramp_id)
        result = reconcile_creative_paths(
            args.ramp_id,
            platform=platform,
            legacy_positional=args.legacy_positional,
        )
        log.info("%s result: %s", platform, result)
        for k in grand:
            grand[k] += int(result.get(k, 0) or 0)

    log.info("Total across all platforms: %s", grand)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

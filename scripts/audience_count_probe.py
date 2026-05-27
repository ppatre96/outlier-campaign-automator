"""Quick audience-count probe across Meta + Google for current targeting.

Hits each platform's reach-estimate API with a representative targeting spec
(post-EMPLOYMENT-SAC: country + Bachelor's education + 4 exclusion audiences)
and reports the audience-size midpoint per platform. Useful for:

1. **Baseline before LAL.** Today's Meta targeting under SAC=EMPLOYMENT only
   gets country + education stripped of interests → audience counts return
   the inflated country-level numbers. This script makes that explicit.

2. **Sanity check after LAL.** Once `src/meta_lal.py` ships, re-run with
   --include-lal to compare LAL'd audience size vs broad. Expect ~1% of
   country population for 1% LALs.

3. **API plumbing health check.** Confirms the bypass secrets, API tokens,
   and SDK paths still work end-to-end (cheap call, no campaign creation).

Usage:
    doppler run -- python3 scripts/audience_count_probe.py --countries US,IN,BR
    doppler run -- python3 scripts/audience_count_probe.py --countries US --platform meta --verbose
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path
from typing import Any, Optional

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("audience_count_probe")


# ── Targeting builders ──────────────────────────────────────────────────


def build_meta_targeting(countries: list[str]) -> dict[str, Any]:
    """Build a representative Meta targeting spec matching production defaults.

    Mirrors `src/meta_targeting.py:resolve_cohort` minimally — just enough to
    get a reach estimate that reflects what production ad sets would see:
      - Country list (geo_locations)
      - Bachelor's+ education
      - Exclusion audiences (the 4 active-contributor lists)
    """
    countries_upper = [c.upper() for c in countries if c]
    targeting: dict[str, Any] = {
        "geo_locations":      {"countries": countries_upper},
        "education_statuses": [4, 5, 6],   # bachelor / master / phd
    }
    excl = list(config.META_EXCLUDE_AUDIENCE_IDS or [])
    if excl:
        targeting["excluded_custom_audiences"] = [{"id": aid} for aid in excl]
    return targeting


def build_google_targeting(countries: list[str]) -> dict[str, Any]:
    """Build a representative Google targeting spec.

    Google's reach_estimate accepts a simpler payload than Meta — geo + a
    handful of segments. We send geo only for the baseline probe (no LAL,
    no interest segments).
    """
    return {
        "countries":     [c.upper() for c in countries if c],
        "user_interests": [],
        "user_lists":     [],
    }


# ── Per-platform probes ─────────────────────────────────────────────────


def probe_meta(countries: list[str]) -> Optional[int]:
    """Return Meta delivery_estimate midpoint or None on failure."""
    try:
        from src.meta_api import MetaClient
    except ImportError as exc:
        log.error("Could not import MetaClient: %s", exc)
        return None
    if not getattr(config, "META_ACCESS_TOKEN", ""):
        log.warning("META_ACCESS_TOKEN not set — skipping Meta probe")
        return None
    targeting = build_meta_targeting(countries)
    log.info("Meta targeting spec:\n  geo=%s edu=%s excludes=%d",
             targeting["geo_locations"]["countries"],
             targeting.get("education_statuses"),
             len(targeting.get("excluded_custom_audiences") or []))
    client = MetaClient()
    return client.get_reach_estimate(targeting, optimization_goal="REACH")


def probe_google(countries: list[str]) -> Optional[int]:
    """Return Google reach-estimate midpoint or None on failure."""
    try:
        from src.google_ads_api import GoogleAdsClient
    except ImportError as exc:
        log.error("Could not import GoogleAdsClient: %s", exc)
        return None
    if not getattr(config, "GOOGLE_ADS_CUSTOMER_ID", ""):
        log.warning("GOOGLE_ADS_CUSTOMER_ID not set — skipping Google probe")
        return None
    targeting = build_google_targeting(countries)
    log.info("Google targeting spec: geo=%s", targeting["countries"])
    client = GoogleAdsClient()
    return client.get_reach_estimate(targeting)


# ── CLI ─────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--countries", default="US",
        help="Comma-separated ISO-2 country codes (default: US)",
    )
    parser.add_argument(
        "--platform", choices=["meta", "google", "both"], default="both",
        help="Which platform(s) to probe (default: both)",
    )
    parser.add_argument("--verbose", action="store_true", help="DEBUG logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    countries = [c.strip().upper() for c in args.countries.split(",") if c.strip()]
    if not countries:
        log.error("--countries must specify at least one ISO-2 code")
        return 2

    log.info("Probing audience counts for countries: %s", countries)

    print()
    print("=" * 60)
    print(f"  Audience Count Probe — countries: {','.join(countries)}")
    print("=" * 60)

    if args.platform in ("meta", "both"):
        print()
        print("▼ Meta")
        size = probe_meta(countries)
        if size is None:
            print("  ✗ Failed (see logs)")
        else:
            print(f"  ✓ delivery_estimate midpoint: {size:,}")
        print(f"  spec: country + Bachelor's+ edu + {len(config.META_EXCLUDE_AUDIENCE_IDS or [])} exclusion audiences")
        print("  NB: under SAC=EMPLOYMENT this returns inflated country-level count")
        print("      until LAL Custom Audiences land (Task #23 → ~1% of country).")

    if args.platform in ("google", "both"):
        print()
        print("▼ Google")
        size = probe_google(countries)
        if size is None:
            print("  ✗ Failed (see logs)")
        else:
            print(f"  ✓ reach_estimate midpoint: {size:,}")
        print("  spec: country only (geo_targets) — pre-Customer-Match baseline")

    print()
    return 0


if __name__ == "__main__":
    sys.exit(main())

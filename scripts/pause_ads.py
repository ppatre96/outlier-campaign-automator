"""Pause specific ads (creatives) by id — the "remove weak creatives" action.

Invoked by .github/workflows/pause_ads.yml when the console's "Fatigue" tab
approves pausing the weak ads in a fatiguing campaign. Surgical: pauses only the
named ad ids, leaving the winners + the ad set/campaign live (pairs with the
additive "add fresh creatives" refresh).

Standalone use:

    doppler run -- python3 scripts/pause_ads.py --platform meta --ad-ids 123,456

Exits non-zero if EVERY pause failed so the workflow run status reflects it; a
partial failure (some ids paused) still exits 0 and logs the failures.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

log = logging.getLogger("pause_ads")

PLATFORM_CHOICES = ("meta", "google", "linkedin", "reddit", "tiktok")


def _client_for(platform: str):
    if platform == "meta":
        from src.meta_api import MetaClient
        return MetaClient()
    if platform == "google":
        from src.google_ads_api import GoogleAdsClient
        return GoogleAdsClient()
    if platform == "linkedin":
        from src.linkedin_api import LinkedInClient
        import config as _config
        return LinkedInClient(token=_config.LINKEDIN_TOKEN)
    if platform == "reddit":
        from src.reddit_api import RedditClient
        return RedditClient()
    if platform == "tiktok":
        from src.tiktok_api import TikTokClient
        return TikTokClient()
    raise ValueError(f"unknown platform: {platform}")


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=PLATFORM_CHOICES)
    parser.add_argument("--ad-ids", required=True, help="Comma-separated platform-native ad ids.")
    parser.add_argument("--ramp-id", default="", help="Optional ramp id for the audit log.")
    args = parser.parse_args()

    ad_ids = [a.strip() for a in args.ad_ids.split(",") if a.strip()]
    if not ad_ids:
        log.error("No ad ids provided.")
        return 2

    client = _client_for(args.platform)
    if hasattr(client, "_ensure_init"):
        try:
            client._ensure_init()
        except Exception:
            pass  # pause_ad re-inits as needed

    ok, failed = [], []
    for ad_id in ad_ids:
        try:
            client.pause_ad(ad_id)
            ok.append(ad_id)
            log.info("paused %s ad %s", args.platform, ad_id)
        except Exception as exc:
            failed.append(ad_id)
            log.warning("failed to pause %s ad %s: %s", args.platform, ad_id, exc)

    # Best-effort audit row so the console/history shows what was paused.
    try:
        from src.ui_decisions import log_event
        log_event(
            args.ramp_id or "", "fatigue_ads_paused",
            {"platform": args.platform, "paused": ok, "failed": failed},
        )
    except Exception:
        pass

    log.info("pause_ads done: %d paused, %d failed", len(ok), len(failed))
    return 1 if ad_ids and not ok else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as exc:
        logging.getLogger("pause_ads").exception("pause_ads failed: %s", exc)
        sys.exit(1)

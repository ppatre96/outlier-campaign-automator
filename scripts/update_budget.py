"""Phase 7 entrypoint — push a campaign-level daily budget update to a
single ad platform.

Invoked by .github/workflows/budget_update.yml when the console's editable
budget cell fires a workflow_dispatch. The workflow passes platform +
campaign_id + new_budget_cents as inputs; this script routes to the right
client's update_campaign_budget method.

Standalone use:

    doppler run -- python3 scripts/update_budget.py \
        --platform linkedin \
        --campaign-id urn:li:sponsoredCampaign:1234567 \
        --budget-cents 5000

Exits non-zero on failure so the workflow's run status reflects platform
errors. The console reads the workflow run status to surface success /
failure back to the reviewer.
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


log = logging.getLogger("update_budget")


PLATFORM_CHOICES = ("linkedin", "meta", "google")


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--platform", required=True, choices=PLATFORM_CHOICES)
    parser.add_argument(
        "--campaign-id",
        required=True,
        help="Platform-native campaign id. LinkedIn URN, Meta numeric ad-set id, "
             "or Google resource name / numeric campaign id.",
    )
    parser.add_argument(
        "--budget-cents",
        required=True,
        type=int,
        help="New daily budget in cents (5000 = $50/day; 0 = effectively pause).",
    )
    args = parser.parse_args()

    if args.platform == "linkedin":
        from src.linkedin_api import LinkedInClient
        import config as _config
        client = LinkedInClient(token=_config.LINKEDIN_TOKEN)
        client.update_campaign_budget(args.campaign_id, args.budget_cents)

    elif args.platform == "meta":
        from src.meta_api import MetaClient
        client = MetaClient()
        client.update_campaign_budget(args.campaign_id, args.budget_cents)

    elif args.platform == "google":
        from src.google_ads_api import GoogleAdsClient
        client = GoogleAdsClient()
        client.update_campaign_budget(args.campaign_id, args.budget_cents)

    else:                                                      # pragma: no cover
        log.error("Unknown platform: %s", args.platform)
        return 2

    log.info("OK — %s %s → %d cents/day", args.platform, args.campaign_id, args.budget_cents)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception as exc:
        logging.getLogger("update_budget").exception("Budget update failed: %s", exc)
        sys.exit(1)

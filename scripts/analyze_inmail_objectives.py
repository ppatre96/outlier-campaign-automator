"""Analyze the spread of InMail (SPONSORED_INMAILS) campaigns on the Outlier
ad account over the last N days, grouped by `objectiveType`.

Fetches every SPONSORED_INMAILS campaign from LinkedIn's adCampaigns API,
filters to ones created in the lookback window, then prints:
  - count + % per objective
  - status breakdown per objective (DRAFT / ACTIVE / PAUSED / etc.)
  - first + last creation timestamps

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/analyze_inmail_objectives.py [--days 365]
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402
load_dotenv(ROOT / ".env")

import config  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("inmail_objectives")


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument(
        "--days", type=int, default=365,
        help="Lookback window in days (default 365)",
    )
    p.add_argument(
        "--page-size", type=int, default=100,
        help="LinkedIn page size (max 100)",
    )
    p.add_argument(
        "--include-all-types", action="store_true",
        help="Don't filter to SPONSORED_INMAILS — show every campaign type's "
             "objective spread (useful as a comparison baseline).",
    )
    return p.parse_args()


def fetch_campaigns(
    account_id: str,
    token: str,
    *,
    page_size: int = 100,
    types: list[str] | None = None,
) -> list[dict]:
    """Fetch every campaign on the ad account, paginated. `types` filters
    server-side via Rest.li ((type:(values:List(...))))."""
    import requests

    base = f"{config.LINKEDIN_API_BASE}/adAccounts/{account_id}/adCampaigns"
    headers = {
        "Authorization":             f"Bearer {token}",
        "LinkedIn-Version":          "202506",
        "X-Restli-Protocol-Version": "2.0.0",
    }

    out: list[dict] = []
    start = 0
    page = 0
    while True:
        # Build the URL by hand — same Rest.li-encoded targeting trick we use
        # for audienceCounts so requests.params= doesn't double-encode.
        query = f"q=search&start={start}&count={page_size}"
        if types:
            type_list = ",".join(types)
            query += f"&search=(type:(values:List({type_list})))"
        url = f"{base}?{query}"
        resp = requests.get(url, headers=headers)
        if not resp.ok:
            log.error("Page %d failed (status=%d): %s", page, resp.status_code, resp.text[:300])
            resp.raise_for_status()
        data = resp.json()
        elements = data.get("elements", [])
        out.extend(elements)
        log.info("  page %d: +%d campaigns (running total %d)", page, len(elements), len(out))
        if len(elements) < page_size:
            break
        start += page_size
        page += 1
    return out


def main() -> int:
    args = _parse_args()

    if not config.LINKEDIN_TOKEN:
        log.error("LINKEDIN_TOKEN not set — run under doppler run --")
        return 1

    types = None if args.include_all_types else ["SPONSORED_INMAILS"]
    label = "ALL types" if args.include_all_types else "SPONSORED_INMAILS only"

    print(f"Fetching campaigns ({label}) from ad account {config.LINKEDIN_AD_ACCOUNT_ID}...")
    campaigns = fetch_campaigns(
        config.LINKEDIN_AD_ACCOUNT_ID,
        config.LINKEDIN_TOKEN,
        page_size=args.page_size,
        types=types,
    )
    print(f"\nTotal {label} campaigns on the account: {len(campaigns)}")

    # Filter to lookback window using changeAuditStamps.created.time (epoch ms)
    cutoff = datetime.now(timezone.utc) - timedelta(days=args.days)
    cutoff_ms = int(cutoff.timestamp() * 1000)

    in_window: list[dict] = []
    for c in campaigns:
        created_ms = (
            (c.get("changeAuditStamps") or {})
            .get("created", {})
            .get("time", 0)
        )
        if created_ms >= cutoff_ms:
            in_window.append(c)

    print(f"Created in last {args.days} days (since {cutoff.date()}): {len(in_window)}")
    if not in_window:
        print("No campaigns in the window.")
        return 0

    # Group by objectiveType + status
    by_objective: Counter[str] = Counter()
    by_obj_status: dict[str, Counter[str]] = defaultdict(Counter)
    by_obj_dates: dict[str, list[int]] = defaultdict(list)

    for c in in_window:
        obj = c.get("objectiveType") or "(unset)"
        status = c.get("status") or "(unset)"
        created_ms = (c.get("changeAuditStamps") or {}).get("created", {}).get("time", 0)
        by_objective[obj] += 1
        by_obj_status[obj][status] += 1
        if created_ms:
            by_obj_dates[obj].append(created_ms)

    total = sum(by_objective.values())
    width = max(len(o) for o in by_objective) + 2

    print()
    print("=" * 76)
    print(f" InMail campaign objective spread — Outlier ad account {config.LINKEDIN_AD_ACCOUNT_ID}")
    print(f" Window: last {args.days} days  •  total {total} campaigns")
    print("=" * 76)
    print(f"  {'Objective'.ljust(width)} {'count':>6} {'pct':>6}   {'status breakdown'}")
    print(f"  {'-' * width} {'-' * 6} {'-' * 6}   {'-' * 40}")
    for obj, n in by_objective.most_common():
        pct = 100.0 * n / total
        statuses = by_obj_status[obj]
        status_str = ", ".join(f"{k}={v}" for k, v in statuses.most_common())
        print(f"  {obj.ljust(width)} {n:>6} {pct:>5.1f}%   {status_str}")

    print()
    print("Date range per objective:")
    for obj in by_objective:
        dates = sorted(by_obj_dates[obj])
        if not dates:
            continue
        first = datetime.fromtimestamp(dates[0] / 1000, tz=timezone.utc).date()
        last  = datetime.fromtimestamp(dates[-1] / 1000, tz=timezone.utc).date()
        print(f"  {obj.ljust(width)}  first={first}  last={last}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

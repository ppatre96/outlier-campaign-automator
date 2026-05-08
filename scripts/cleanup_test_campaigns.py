"""
List + (optionally) archive test/draft campaigns that accumulated this week.

Test patterns to match (substring, case-insensitive — lower-cased name compared):
  TEST-CARDIO-3CH        / test-cardio
  inmail_objective_test  / inmail_smoke_test  / inmail_full_test
  agent_TEST-... / agent_inmail_... (auto-prefix variants)

Two modes:
  --list        (default) print matching campaigns and campaign groups, no writes
  --archive     PATCH each match to status=ARCHIVED on LinkedIn (soft-delete)

LinkedIn's q=search caps at 10K id-ascending and forbids dateRange filters
(see feedback memory). Strategy: walk recent campaign GROUPS via paginated
search, then list campaigns within each group. Filter client-side by name
substring.

Run:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
  doppler run --project outlier-campaign-agent --config dev -- \\
  venv/bin/python scripts/cleanup_test_campaigns.py --list

  # then, after reviewing the list:
  DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
  doppler run --project outlier-campaign-agent --config dev -- \\
  venv/bin/python scripts/cleanup_test_campaigns.py --archive
"""
from __future__ import annotations

import argparse
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

# Substrings (lowercased) that mark a campaign or group as test scaffolding.
TEST_PATTERNS = [
    "test-cardio",
    "test_cardio",
    "inmail_objective_test",
    "inmail_smoke_test",
    "inmail_full_test",
    "inmail_test",
    "test-3ch",
    "test_3ch",
    "smoke_test",
    "test_v",  # matches inmail_full_test_v1, _v2, ...
]

# Statuses we are willing to touch. ACTIVE campaigns are NOT in this list —
# we only archive things that are clearly draft/paused/test scaffolding.
ARCHIVABLE_STATUSES = {"DRAFT", "PAUSED", "PENDING_DELETION"}


def _name_is_test(name: str) -> bool:
    if not name:
        return False
    n = name.lower()
    return any(p in n for p in TEST_PATTERNS)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--list",    action="store_true", help="List matches only (default)")
    parser.add_argument("--archive", action="store_true", help="PATCH matches to ARCHIVED")
    parser.add_argument("--limit",   type=int, default=200, help="Max campaigns to scan")
    args = parser.parse_args()

    if not args.archive:
        args.list = True  # default

    import requests

    token = os.environ.get("LINKEDIN_ACCESS_TOKEN", "").strip()
    account = os.environ.get("LINKEDIN_AD_ACCOUNT_ID", "").strip()
    api_version = os.environ.get("LINKEDIN_VERSION", "202506").strip()
    if not token or not account:
        print("ERROR: LINKEDIN_ACCESS_TOKEN / LINKEDIN_AD_ACCOUNT_ID missing.")
        print("       Run under `doppler run --project outlier-campaign-agent --config dev --`")
        return 2

    base = "https://api.linkedin.com/rest"
    common_headers = {
        "Authorization": f"Bearer {token}",
        "LinkedIn-Version": api_version,
        "X-Restli-Protocol-Version": "2.0.0",
    }

    # ── walk campaign GROUPS (q=search has very few allowed filters at the
    # adCampaigns endpoint — neither name nor status are accepted; per the
    # known gotcha, we walk groups first, then list campaigns per group) ────
    groups_url    = f"{base}/adAccounts/{account}/adCampaignGroups"
    campaigns_url = f"{base}/adAccounts/{account}/adCampaigns"

    # Pull groups under the account. q=search returns id-ascending and caps
    # at 10K offset. Most test groups were created this week (high IDs at end)
    # so we walk to the end with progress prints, then take the tail.
    group_rows: list[dict] = []
    start = 0
    page_size = 100
    max_groups = 5000  # safety cap; each ramp creates ~3-4 groups
    while start < max_groups:
        params = {"q": "search", "start": start, "count": page_size}
        resp = requests.get(groups_url, headers=common_headers, params=params, timeout=30)
        if not resp.ok:
            print(f"ERROR: list groups failed {resp.status_code} at start={start}: {resp.text[:200]}", flush=True)
            return 3
        body = resp.json()
        elements = body.get("elements", []) or []
        group_rows.extend(elements)
        # Live progress so a long enumeration is visible.
        print(f"  ...fetched {len(group_rows)} groups so far (page start={start}, "
              f"this_page={len(elements)})", flush=True)
        if len(elements) < page_size:
            break
        start += len(elements)
    # Take the most-recent N groups (id-desc) so we focus on this week's tests.
    group_rows.sort(key=lambda g: int(g.get("id") or 0), reverse=True)
    group_rows = group_rows[:300]

    test_groups = [g for g in group_rows if _name_is_test(g.get("name", ""))]
    print(f"Found {len(group_rows)} campaign groups under account {account}; "
          f"{len(test_groups)} match test patterns.")

    matches:    list[dict] = []
    seen_ids:   set[str]   = set()
    scanned                = 0

    # Drill into each test group: list all campaigns under that group.
    for g in test_groups:
        gid = str(g.get("id") or "")
        if not gid:
            continue
        start = 0
        while scanned < args.limit:
            params = {
                "q":     "search",
                "start": start,
                "count": page_size,
                "search.campaignGroup.values[0]": f"urn:li:sponsoredCampaignGroup:{gid}",
            }
            resp = requests.get(campaigns_url, headers=common_headers, params=params, timeout=30)
            if not resp.ok:
                print(f"WARN: list campaigns under group {gid} failed "
                      f"{resp.status_code}: {resp.text[:200]}")
                break
            body = resp.json()
            elements = body.get("elements", []) or []
            if not elements:
                break
            for c in elements:
                scanned += 1
                cid = str(c.get("id") or "")
                if cid and cid not in seen_ids:
                    seen_ids.add(cid)
                    matches.append({
                        "id":     cid,
                        "name":   c.get("name", ""),
                        "status": c.get("status", ""),
                        "group":  g.get("name", "")[:40],
                    })
            if len(elements) < page_size:
                break
            start += len(elements)

    print(f"Scanned {scanned} campaigns inside test groups; matched {len(matches)} campaigns.")
    print()

    if not matches:
        print("Nothing to archive.")
        return 0

    # ── show matches ────────────────────────────────────────────────────────
    print(f"{'STATUS':<10} {'ID':<14} {'GROUP':<42} NAME")
    print("-" * 110)
    for m in matches:
        print(f"{m['status']:<10} {m['id']!s:<14} {m.get('group', '')[:40]:<42} {m['name']}")

    if args.list and not args.archive:
        print()
        print("Re-run with --archive to PATCH these to ARCHIVED.")
        return 0

    # ── archive in safe statuses only ───────────────────────────────────────
    targets = [m for m in matches if m["status"] in ARCHIVABLE_STATUSES]
    skipped = [m for m in matches if m["status"] not in ARCHIVABLE_STATUSES]

    print()
    print(f"Will archive: {len(targets)}  /  Skipping (not in safe statuses): {len(skipped)}")
    if skipped:
        print("Skipped names (status not in DRAFT/PAUSED/PENDING_DELETION):")
        for m in skipped:
            print(f"  [{m['status']}] {m['name']}")

    failures = 0
    for m in targets:
        cid = m["id"]
        patch_url = f"{base}/adAccounts/{account}/adCampaigns/{cid}"
        payload = {"patch": {"$set": {"status": "ARCHIVED"}}}
        headers = {**common_headers,
                   "X-RestLi-Method": "PARTIAL_UPDATE",
                   "Content-Type": "application/json"}
        r = requests.post(patch_url, headers=headers, json=payload, timeout=30)
        if r.ok or r.status_code == 204:
            print(f"  ARCHIVED  {cid}  {m['name']}")
        else:
            failures += 1
            print(f"  FAILED    {cid}  ({r.status_code}) {r.text[:200]}")
        time.sleep(0.15)  # gentle pacing — LinkedIn dislikes burst PATCHes

    print()
    print(f"Done. archived={len(targets) - failures}  failed={failures}  skipped={len(skipped)}")
    return 0 if failures == 0 else 4


if __name__ == "__main__":
    raise SystemExit(main())

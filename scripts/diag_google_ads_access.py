"""
Diagnose Google Ads API access — list every customer the OAuth'd user can
reach, flag whether the configured customer is reachable, and tell you what
LOGIN_CUSTOMER_ID to use.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/diag_google_ads_access.py
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))


def main() -> int:
    target_cid = os.environ.get("GOOGLE_ADS_CUSTOMER_ID", "").replace("-", "")
    login_cid  = os.environ.get("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")

    creds = {
        "developer_token":  os.environ["GOOGLE_ADS_DEVELOPER_TOKEN"],
        "refresh_token":    os.environ["GOOGLE_ADS_REFRESH_TOKEN"],
        "client_id":        os.environ["GOOGLE_ADS_CLIENT_ID"],
        "client_secret":    os.environ["GOOGLE_ADS_CLIENT_SECRET"],
        "use_proto_plus":   True,
    }
    if login_cid:
        creds["login_customer_id"] = login_cid

    from google.ads.googleads.client import GoogleAdsClient as SDK
    client = SDK.load_from_dict(creds)

    print("=" * 72)
    print(" Google Ads access probe")
    print("=" * 72)
    print(f"  configured customer_id        : {target_cid}")
    print(f"  configured login_customer_id  : {login_cid or '(none)'}")
    print()

    # Step 1: List accessible customers (no login_customer_id needed for this call).
    cs = client.get_service("CustomerService")
    accessible = cs.list_accessible_customers().resource_names
    print("=== ListAccessibleCustomers (top-level resources visible to OAuth user) ===")
    if not accessible:
        print("  (none — your Google account isn't tied to ANY Google Ads account)")
        print()
        print("FIX: ask the Google Ads account owner to add your work account as a")
        print("     User in Tools → Setup → Access and security → Users → Add.")
        return 1
    accessible_ids = []
    for resource_name in accessible:
        cid = resource_name.split("/")[-1]
        accessible_ids.append(cid)
        print(f"  ✓ {cid}  ({resource_name})")
    print()

    target_visible = target_cid in accessible_ids
    print(f"  target ({target_cid}) directly accessible? {'✅ YES' if target_visible else '❌ NO'}")
    print()

    # Step 2: For each accessible customer, check if it's an MCC and whether
    # target_cid is one of its children.
    if not target_visible:
        print("=== Checking if target is a child of an accessible MCC ===")
        ga_service = client.get_service("GoogleAdsService")
        for cid in accessible_ids:
            try:
                # Use this customer as login_customer_id and query its hierarchy
                child_creds = dict(creds, login_customer_id=cid)
                child_client = SDK.load_from_dict(child_creds)
                child_ga = child_client.get_service("GoogleAdsService")
                query = (
                    "SELECT customer_client.id, customer_client.descriptive_name, "
                    "customer_client.manager, customer_client.level "
                    "FROM customer_client"
                )
                resp = child_ga.search_stream(customer_id=cid, query=query)
                children = []
                for batch in resp:
                    for row in batch.results:
                        children.append({
                            "id": str(row.customer_client.id),
                            "name": row.customer_client.descriptive_name,
                            "manager": row.customer_client.manager,
                            "level": row.customer_client.level,
                        })
                print(f"\n  MCC candidate {cid}:")
                if not children or all(c["level"] == 0 for c in children):
                    print(f"    (not an MCC, single-tenant account)")
                    continue
                print(f"    descendant accounts ({len(children)}):")
                for c in children:
                    is_target = "  ⭐ TARGET" if c["id"] == target_cid else ""
                    kind = "MCC" if c["manager"] else "leaf"
                    print(f"      lvl={c['level']} id={c['id']:>12} {kind:<4} '{c['name']}'{is_target}")
                if any(c["id"] == target_cid for c in children):
                    print()
                    print("=" * 72)
                    print(f"  FIX: set GOOGLE_ADS_LOGIN_CUSTOMER_ID={cid}")
                    print("=" * 72)
                    print()
                    print(f"    doppler secrets set GOOGLE_ADS_LOGIN_CUSTOMER_ID='{cid}' \\")
                    print(f"      --project outlier-campaign-agent --config dev")
                    return 0
            except Exception as exc:
                print(f"\n  could not enumerate children of {cid}: {exc}")
                continue

        print()
        print("=" * 72)
        print(" Target customer not reachable through any MCC.")
        print("=" * 72)
        print(" The user you OAuth'd as needs to be added to either:")
        print(f"   - customer {target_cid} directly, or")
        print(f"   - an MCC that manages {target_cid}")
        print(" Tools → Setup → Access and security → Users → Add (in the Ads UI).")
        return 2

    # Step 3: target visible — try a real query to confirm end-to-end.
    print("=== Sanity query against target customer ===")
    try:
        ga = client.get_service("GoogleAdsService")
        query = "SELECT customer.descriptive_name, customer.manager FROM customer"
        resp = ga.search_stream(customer_id=target_cid, query=query)
        for batch in resp:
            for row in batch.results:
                print(f"  ✓ name='{row.customer.descriptive_name}' is_manager={row.customer.manager}")
        print()
        print("ACCESS OK — pipeline should now work. Re-run the test.")
        return 0
    except Exception as exc:
        print(f"  ✗ query failed: {exc}")
        return 3


if __name__ == "__main__":
    raise SystemExit(main())

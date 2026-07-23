"""
Extended audit: find actual GMR-0023 campaign names + check wider spend window.
"""
from __future__ import annotations
import sys
sys.path.insert(0, "/Users/pranavpatre/outlier-campaign-agent")
import config
from google.ads.googleads.client import GoogleAdsClient as _GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException

CUSTOMER_ID       = "8840244968"
LOGIN_CUSTOMER_ID = "8840244968"

def _build_client():
    cfg = {
        "developer_token":    config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "client_id":          config.GOOGLE_ADS_CLIENT_ID,
        "client_secret":      config.GOOGLE_ADS_CLIENT_SECRET,
        "refresh_token":      config.GOOGLE_ADS_REFRESH_TOKEN,
        "login_customer_id":  LOGIN_CUSTOMER_ID,
        "use_proto_plus":     True,
    }
    return _GoogleAdsClient.load_from_dict(cfg)

def section(title):
    print(f"\n{'='*70}\n  {title}\n{'='*70}")

client = _build_client()
ga_svc = client.get_service("GoogleAdsService")

# -----------------------------------------------------------------------
# A. List ALL campaigns (no date filter, no cost filter) to find GMR-0023
# -----------------------------------------------------------------------
section("A. ALL campaigns on account — list all names + status")
query = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        campaign.serving_status,
        campaign.primary_status,
        campaign.primary_status_reasons
    FROM campaign
    ORDER BY campaign.id DESC
    LIMIT 100
"""
try:
    rows = list(ga_svc.search(customer_id=CUSTOMER_ID, query=query))
    print(f"  Total campaigns found: {len(rows)}")
    for row in rows:
        c = row.campaign
        status  = c.status.name if hasattr(c.status, "name") else str(c.status)
        serving = c.serving_status.name if hasattr(c.serving_status, "name") else str(c.serving_status)
        primary = c.primary_status.name if hasattr(c.primary_status, "name") else str(c.primary_status)
        reasons = [r.name if hasattr(r, "name") else str(r) for r in c.primary_status_reasons]
        print(f"  id={c.id:>15}  status={status:10}  serving={serving:20}  primary={primary:20}  reasons={reasons}")
        print(f"               name: {c.name}")
except GoogleAdsException as ex:
    print(f"  ERROR: {ex}")

# -----------------------------------------------------------------------
# B. Spend across the FULL June — broader window 2026-05-25→2026-06-10
# -----------------------------------------------------------------------
section("B. Account-wide spend 2026-05-25→2026-06-10 (all campaigns)")
query2 = """
    SELECT
        campaign.id,
        campaign.name,
        campaign.status,
        metrics.cost_micros,
        metrics.impressions,
        metrics.clicks
    FROM campaign
    WHERE segments.date BETWEEN '2026-05-25' AND '2026-06-10'
    ORDER BY metrics.cost_micros DESC
    LIMIT 30
"""
try:
    rows2 = list(ga_svc.search(customer_id=CUSTOMER_ID, query=query2))
    total = sum(r.metrics.cost_micros for r in rows2) / 1_000_000
    spending = [r for r in rows2 if r.metrics.cost_micros > 0]
    print(f"  Campaigns with ANY spend in window: {len(spending)} / {len(rows2)}")
    print(f"  Total spend across all: ${total:.2f}")
    if spending:
        for row in spending:
            c = row.campaign
            m = row.metrics
            print(f"  ${m.cost_micros/1e6:>8.2f}  impr={m.impressions:>7}  clicks={m.clicks:>5}  | {c.name[:80]}")
    else:
        print("  (ZERO dollars spent by any campaign in this 16-day window)")
except GoogleAdsException as ex:
    print(f"  ERROR: {ex}")

# -----------------------------------------------------------------------
# C. Billing setup — check if the PENDING setup (started 2026-06-04)
#    has an end_date that might signal a gap
# -----------------------------------------------------------------------
section("C. Billing setup gap analysis")
query3 = """
    SELECT
        billing_setup.id,
        billing_setup.status,
        billing_setup.start_date_time,
        billing_setup.end_date_time,
        billing_setup.payments_account_info.payments_account_id,
        billing_setup.payments_account_info.payments_account_name
    FROM billing_setup
    ORDER BY billing_setup.id DESC
    LIMIT 10
"""
try:
    rows3 = list(ga_svc.search(customer_id=CUSTOMER_ID, query=query3))
    for row in rows3:
        bs = row.billing_setup
        status = bs.status.name if hasattr(bs.status, "name") else str(bs.status)
        pai = bs.payments_account_info
        print(f"  id={bs.id}  status={status}")
        print(f"    start={bs.start_date_time}  end={bs.end_date_time}")
        print(f"    payments_account_id={pai.payments_account_id}  name={pai.payments_account_name}")
except GoogleAdsException as ex:
    print(f"  ERROR: {ex}")

# -----------------------------------------------------------------------
# D. Historical spend — when did this account LAST spend anything?
#    Check 2026-05-01→2026-05-31 (before billing gap)
# -----------------------------------------------------------------------
section("D. Historical spend 2026-05-01→2026-05-31 — was account spending at all?")
query4 = """
    SELECT
        campaign.id,
        campaign.name,
        metrics.cost_micros,
        metrics.impressions
    FROM campaign
    WHERE segments.date BETWEEN '2026-05-01' AND '2026-05-31'
      AND metrics.cost_micros > 0
    ORDER BY metrics.cost_micros DESC
    LIMIT 20
"""
try:
    rows4 = list(ga_svc.search(customer_id=CUSTOMER_ID, query=query4))
    total_may = sum(r.metrics.cost_micros for r in rows4) / 1_000_000
    print(f"  Campaigns with spend in May 2026: {len(rows4)}")
    print(f"  Total May 2026 spend: ${total_may:.2f}")
    for row in rows4[:10]:
        c = row.campaign
        m = row.metrics
        print(f"  ${m.cost_micros/1e6:>8.2f}  impr={m.impressions:>7}  | {c.name[:80]}")
except GoogleAdsException as ex:
    print(f"  ERROR: {ex}")

print("\n" + "="*70)
print("  EXTENDED AUDIT COMPLETE")
print("="*70)

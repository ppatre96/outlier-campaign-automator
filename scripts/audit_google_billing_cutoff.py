"""
Pinpoint the exact date the account stopped spending.
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

client = _build_client()
ga_svc = client.get_service("GoogleAdsService")

# Daily spend breakdown 2026-05-05 → 2026-06-10 across all campaigns
print("Daily account spend 2026-05-05 → 2026-06-10")
print(f"{'Date':<14}  {'Cost':>10}  {'Impressions':>12}  {'Clicks':>8}")
print("-" * 50)

query = """
    SELECT
        segments.date,
        metrics.cost_micros,
        metrics.impressions,
        metrics.clicks
    FROM customer
    WHERE segments.date BETWEEN '2026-05-05' AND '2026-06-10'
    ORDER BY segments.date ASC
"""
try:
    rows = list(ga_svc.search(customer_id=CUSTOMER_ID, query=query))
    for row in rows:
        d = row.segments.date
        m = row.metrics
        cost_usd = m.cost_micros / 1_000_000
        marker = "  <-- LAST SPEND DAY?" if cost_usd > 0 else ""
        print(f"  {d}  ${cost_usd:>9.2f}  {m.impressions:>12}  {m.clicks:>8}{marker}")
except GoogleAdsException as ex:
    print(f"ERROR: {ex}")
    for e in ex.failure.errors:
        print(f"  -> {e.error_code}  {e.message}")

"""
Audit script: Google Ads billing status + control campaign spend test.

Tests two hypotheses for GMR-0023 $0 spend:
  1. Account-level payment/billing freeze
  2. Campaign-specific issue (keywords/SAC/bidding)

Read-only. No mutations.
"""
from __future__ import annotations

import sys
import os

# Ensure project root is on path
sys.path.insert(0, "/Users/pranavpatre/outlier-campaign-agent")

import config

from google.ads.googleads.client import GoogleAdsClient as _GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException


CUSTOMER_ID     = "8840244968"
LOGIN_CUSTOMER_ID = "8840244968"

GMR0023_PREFIX  = "agent_GMR-0023"
DATE_RANGE      = ("2026-06-01", "2026-06-10")

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


def section(title: str):
    print(f"\n{'='*70}")
    print(f"  {title}")
    print(f"{'='*70}")


# ---------------------------------------------------------------------------
# 1. ACCOUNT STATUS
# ---------------------------------------------------------------------------
def check_account_status(client):
    section("1. ACCOUNT STATUS — customer.status + suspension reasons")
    ga_svc = client.get_service("GoogleAdsService")
    query = """
        SELECT
            customer.id,
            customer.descriptive_name,
            customer.status,
            customer.pay_per_conversion_eligibility_failure_reasons
        FROM customer
        LIMIT 1
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        for row in response:
            c = row.customer
            status = c.status.name if hasattr(c.status, "name") else str(c.status)
            ppc_reasons = list(c.pay_per_conversion_eligibility_failure_reasons) if c.pay_per_conversion_eligibility_failure_reasons else []
            ppc_str = [r.name if hasattr(r, "name") else str(r) for r in ppc_reasons]
            print(f"  Account ID   : {c.id}")
            print(f"  Name         : {c.descriptive_name}")
            print(f"  Status       : {status}")
            print(f"  PPC ineligibility reasons: {ppc_str or '(none)'}")
    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")


# ---------------------------------------------------------------------------
# 2. BILLING SETUP
# ---------------------------------------------------------------------------
def check_billing_setup(client):
    section("2. BILLING SETUP — billing_setup status")
    ga_svc = client.get_service("GoogleAdsService")
    query = """
        SELECT
            billing_setup.id,
            billing_setup.status,
            billing_setup.start_date_time,
            billing_setup.end_date_time
        FROM billing_setup
        LIMIT 10
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        rows = list(response)
        if not rows:
            print("  (no billing_setup rows returned)")
            return
        for row in rows:
            bs = row.billing_setup
            status = bs.status.name if hasattr(bs.status, "name") else str(bs.status)
            print(f"  billing_setup.id={bs.id}  status={status}  start={bs.start_date_time}  end={bs.end_date_time}")
    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")


# ---------------------------------------------------------------------------
# 3. ACCOUNT BUDGET
# ---------------------------------------------------------------------------
def check_account_budget(client):
    section("3. ACCOUNT BUDGET — pending/approved budgets")
    ga_svc = client.get_service("GoogleAdsService")
    query = """
        SELECT
            account_budget.id,
            account_budget.status,
            account_budget.approved_spending_limit_micros,
            account_budget.approved_spending_limit_type,
            account_budget.amount_served_micros,
            account_budget.total_adjustments_micros
        FROM account_budget
        LIMIT 10
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        rows = list(response)
        if not rows:
            print("  (no account_budget rows returned — likely invoice/monthly billing, not prepaid)")
            return
        for row in rows:
            ab = row.account_budget
            status = ab.status.name if hasattr(ab.status, "name") else str(ab.status)
            limit_type = ab.approved_spending_limit_type.name if hasattr(ab.approved_spending_limit_type, "name") else str(ab.approved_spending_limit_type)
            served_usd = ab.amount_served_micros / 1_000_000
            print(f"  budget.id={ab.id}  status={status}  limit_type={limit_type}  "
                  f"approved_limit={ab.approved_spending_limit_micros}  served=${served_usd:.2f}")
    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")


# ---------------------------------------------------------------------------
# 4. CONTROL TEST — top campaigns by spend June 2026
# ---------------------------------------------------------------------------
def control_test_top_campaigns(client):
    section("4. CONTROL TEST — top campaigns by cost 2026-06-01→2026-06-10")
    ga_svc = client.get_service("GoogleAdsService")
    start, end = DATE_RANGE
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.serving_status,
            campaign.primary_status,
            campaign.primary_status_reasons,
            metrics.cost_micros,
            metrics.impressions,
            metrics.clicks
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
        ORDER BY metrics.cost_micros DESC
        LIMIT 25
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        rows = list(response)
        if not rows:
            print("  (no campaign rows with data in this date range)")
            return

        total_spend = 0.0
        gmr0023_rows = []
        other_rows   = []

        for row in rows:
            c = row.campaign
            m = row.metrics
            cost_usd = m.cost_micros / 1_000_000
            total_spend += cost_usd
            status = c.status.name if hasattr(c.status, "name") else str(c.status)
            serving = c.serving_status.name if hasattr(c.serving_status, "name") else str(c.serving_status)
            primary = c.primary_status.name if hasattr(c.primary_status, "name") else str(c.primary_status)
            reasons = [r.name if hasattr(r, "name") else str(r) for r in c.primary_status_reasons]

            entry = {
                "id": c.id,
                "name": c.name,
                "status": status,
                "serving_status": serving,
                "primary_status": primary,
                "primary_status_reasons": reasons,
                "cost_usd": cost_usd,
                "impressions": m.impressions,
                "clicks": m.clicks,
            }
            if GMR0023_PREFIX in c.name:
                gmr0023_rows.append(entry)
            else:
                other_rows.append(entry)

        print(f"\n  -- Non-GMR-0023 campaigns (control group) --")
        if other_rows:
            for e in other_rows:
                print(f"  ${e['cost_usd']:>8.2f}  impr={e['impressions']:>7}  clicks={e['clicks']:>5}  "
                      f"status={e['status']:12}  serving={e['serving_status']:20}  primary={e['primary_status']:20}  "
                      f"reasons={e['primary_status_reasons']}  | {e['name'][:70]}")
        else:
            print("  (none in top-25)")

        print(f"\n  -- GMR-0023 campaigns --")
        if gmr0023_rows:
            for e in gmr0023_rows:
                print(f"  ${e['cost_usd']:>8.2f}  impr={e['impressions']:>7}  clicks={e['clicks']:>5}  "
                      f"status={e['status']:12}  serving={e['serving_status']:20}  primary={e['primary_status']:20}  "
                      f"reasons={e['primary_status_reasons']}  | {e['name'][:70]}")
        else:
            print("  (none in top-25 — all at $0)")

        print(f"\n  TOTAL spend across top-25 in date range: ${total_spend:.2f}")

    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")
        for error in ex.failure.errors:
            print(f"    -> {error.error_code}  {error.message}")


# ---------------------------------------------------------------------------
# 5. GMR-0023 campaigns — primary_status_reasons deep-dive
# ---------------------------------------------------------------------------
def check_gmr0023_primary_status(client):
    section("5. GMR-0023 CAMPAIGNS — serving_status + primary_status_reasons (no date filter)")
    ga_svc = client.get_service("GoogleAdsService")
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.serving_status,
            campaign.primary_status,
            campaign.primary_status_reasons
        FROM campaign
        WHERE campaign.name LIKE '%{GMR0023_PREFIX}%'
        LIMIT 50
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        rows = list(response)
        if not rows:
            print(f"  No campaigns found matching '{GMR0023_PREFIX}'")
            return
        for row in rows:
            c = row.campaign
            status  = c.status.name if hasattr(c.status, "name") else str(c.status)
            serving = c.serving_status.name if hasattr(c.serving_status, "name") else str(c.serving_status)
            primary = c.primary_status.name if hasattr(c.primary_status, "name") else str(c.primary_status)
            reasons = [r.name if hasattr(r, "name") else str(r) for r in c.primary_status_reasons]
            print(f"  id={c.id}  status={status:10}  serving={serving:20}  primary={primary:20}  "
                  f"reasons={reasons}  | {c.name[:70]}")
    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")
        for error in ex.failure.errors:
            print(f"    -> {error.error_code}  {error.message}")


# ---------------------------------------------------------------------------
# 6. Modifly / other known-active campaigns sanity check
# ---------------------------------------------------------------------------
def check_modifly_campaigns(client):
    section("6. MODIFLY campaigns — spending in June 2026?")
    ga_svc = client.get_service("GoogleAdsService")
    start, end = DATE_RANGE
    query = f"""
        SELECT
            campaign.id,
            campaign.name,
            campaign.status,
            campaign.serving_status,
            metrics.cost_micros,
            metrics.impressions
        FROM campaign
        WHERE segments.date BETWEEN '{start}' AND '{end}'
          AND campaign.name LIKE '%odifly%'
        ORDER BY metrics.cost_micros DESC
        LIMIT 20
    """
    try:
        response = ga_svc.search(customer_id=CUSTOMER_ID, query=query)
        rows = list(response)
        if not rows:
            print("  No 'odifly' campaigns found with spend in this range.")
            return
        for row in rows:
            c = row.campaign
            m = row.metrics
            cost_usd = m.cost_micros / 1_000_000
            status  = c.status.name if hasattr(c.status, "name") else str(c.status)
            serving = c.serving_status.name if hasattr(c.serving_status, "name") else str(c.serving_status)
            print(f"  ${cost_usd:>8.2f}  impr={m.impressions:>7}  status={status:10}  serving={serving:20}  | {c.name[:70]}")
    except GoogleAdsException as ex:
        print(f"  ERROR: {ex}")


# ---------------------------------------------------------------------------
# MAIN
# ---------------------------------------------------------------------------
if __name__ == "__main__":
    print("Building Google Ads client...")
    client = _build_client()
    print("Client ready.")

    check_account_status(client)
    check_billing_setup(client)
    check_account_budget(client)
    control_test_top_campaigns(client)
    check_gmr0023_primary_status(client)
    check_modifly_campaigns(client)

    print("\n" + "="*70)
    print("  AUDIT COMPLETE")
    print("="*70)

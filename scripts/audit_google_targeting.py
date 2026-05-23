"""One-off audit of historical Google Ads targeting on customer 8840244968.

Reads campaigns + ad-groups created by the pipeline (smart_ramp_id starts
with "GMR-") and prints every criterion attached to each. Used to answer:
  - what audience_segments did past campaigns actually receive?
  - what geo/location targets were applied?
  - any exclusions in place?
  - any common patterns we should preserve when refactoring the resolver?

Run via:  doppler run -- python3 scripts/audit_google_targeting.py
"""
from __future__ import annotations
import json
import sys
from collections import Counter, defaultdict

import config


def main():
    from google.ads.googleads.client import GoogleAdsClient as _SDK

    creds = {
        "developer_token": config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "refresh_token":   config.GOOGLE_ADS_REFRESH_TOKEN,
        "client_id":       config.GOOGLE_ADS_CLIENT_ID,
        "client_secret":   config.GOOGLE_ADS_CLIENT_SECRET,
        "use_proto_plus":  True,
    }
    if getattr(config, "GOOGLE_ADS_LOGIN_CUSTOMER_ID", ""):
        creds["login_customer_id"] = str(config.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace("-", "")
    client = _SDK.load_from_dict(creds)
    ga = client.get_service("GoogleAdsService")
    cust = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")

    # 1. List campaigns matching agent_Scale-GMR-* naming pattern
    print("=" * 80)
    print("1. AGENT-CREATED CAMPAIGNS (name LIKE 'agent_%' or 'Scale-GMR-%')")
    print("=" * 80)
    # GAQL doesn't support OR in WHERE — run the LIKE clause twice and merge.
    base = (
        "SELECT campaign.id, campaign.name, campaign.status, "
        "campaign.bidding_strategy_type, campaign.advertising_channel_type "
        "FROM campaign "
    )
    rows = []
    seen_ids: set[int] = set()
    for like_clause in ("campaign.name LIKE '%GMR-%'", "campaign.name LIKE 'agent_%'"):
        q = base + f"WHERE {like_clause} ORDER BY campaign.id DESC LIMIT 40"
        for r in ga.search(customer_id=cust, query=q):
            if r.campaign.id in seen_ids:
                continue
            seen_ids.add(r.campaign.id)
            rows.append(r)
    campaign_ids: list[int] = []
    for r in rows:
        c = r.campaign
        print(f"  [{c.id}] status={c.status.name} bid={c.bidding_strategy_type.name} "
              f"channel={c.advertising_channel_type.name}")
        print(f"      name={c.name[:120]}")
        campaign_ids.append(c.id)
    if not campaign_ids:
        print("  (no matching campaigns found)")
        return

    # 2. Per-campaign criteria (geo, language, audience, etc.)
    print()
    print("=" * 80)
    print("2. CAMPAIGN-LEVEL CRITERIA (geo, language, ad-schedule, device, audience)")
    print("=" * 80)
    cid_filter = "(" + ", ".join(str(i) for i in campaign_ids) + ")"
    q = (
        "SELECT campaign.id, campaign_criterion.type, campaign_criterion.negative, "
        "campaign_criterion.location.geo_target_constant, "
        "campaign_criterion.language.language_constant, "
        "campaign_criterion.user_interest.user_interest_category, "
        "campaign_criterion.keyword.text, campaign_criterion.gender.type, "
        "campaign_criterion.age_range.type "
        "FROM campaign_criterion "
        f"WHERE campaign.id IN {cid_filter}"
    )
    grouped: dict[int, list[dict]] = defaultdict(list)
    try:
        for r in ga.search(customer_id=cust, query=q):
            cc = r.campaign_criterion
            grouped[r.campaign.id].append({
                "type":     cc.type_.name,
                "negative": cc.negative,
                "geo":      cc.location.geo_target_constant or "",
                "lang":     cc.language.language_constant or "",
                "interest": cc.user_interest.user_interest_category or "",
                "kw":       cc.keyword.text or "",
                "gender":   cc.gender.type_.name if cc.gender.type_ else "",
                "age":      cc.age_range.type_.name if cc.age_range.type_ else "",
            })
    except Exception as exc:
        print(f"  query failed: {exc}")
        grouped = {}
    for cid, crits in grouped.items():
        type_hist = Counter(c["type"] for c in crits)
        neg = sum(1 for c in crits if c["negative"])
        print(f"  campaign {cid}: {len(crits)} criteria  ({dict(type_hist)})  negatives={neg}")
        # Show first 5 of each type
        by_type: dict[str, list[dict]] = defaultdict(list)
        for c in crits:
            by_type[c["type"]].append(c)
        for t, items in by_type.items():
            print(f"    {t}:")
            for it in items[:5]:
                detail = it["geo"] or it["lang"] or it["interest"] or it["kw"] or it["gender"] or it["age"]
                tag = "(NEGATIVE)" if it["negative"] else ""
                print(f"      {tag}{detail}")
            if len(items) > 5:
                print(f"      ... and {len(items) - 5} more")

    # 3. Per ad-group criteria — audience + keyword + demographics
    print()
    print("=" * 80)
    print("3. AD-GROUP-LEVEL CRITERIA (sample 20 ad-groups; audience + keyword)")
    print("=" * 80)
    q = (
        "SELECT ad_group.id, ad_group.name, ad_group.campaign "
        "FROM ad_group "
        f"WHERE campaign.id IN {cid_filter} "
        "LIMIT 50"
    )
    ag_ids: list[int] = []
    ag_names: dict[int, str] = {}
    for r in ga.search(customer_id=cust, query=q):
        ag_ids.append(r.ad_group.id)
        ag_names[r.ad_group.id] = r.ad_group.name
    if not ag_ids:
        print("  (no ad-groups found)")
        return

    ag_filter = "(" + ", ".join(str(i) for i in ag_ids[:30]) + ")"
    q = (
        "SELECT ad_group.id, ad_group_criterion.type, ad_group_criterion.negative, "
        "ad_group_criterion.user_interest.user_interest_category, "
        "ad_group_criterion.keyword.text, "
        "ad_group_criterion.user_list.user_list, "
        "ad_group_criterion.custom_audience.custom_audience, "
        "ad_group_criterion.gender.type, "
        "ad_group_criterion.age_range.type "
        "FROM ad_group_criterion "
        f"WHERE ad_group.id IN {ag_filter}"
    )
    ag_crits: dict[int, list[dict]] = defaultdict(list)
    for r in ga.search(customer_id=cust, query=q):
        gc = r.ad_group_criterion
        ag_crits[r.ad_group.id].append({
            "type":     gc.type_.name,
            "negative": gc.negative,
            "interest": gc.user_interest.user_interest_category or "",
            "kw":       gc.keyword.text or "",
            "ulist":    gc.user_list.user_list or "",
            "caud":     gc.custom_audience.custom_audience or "",
            "gender":   gc.gender.type_.name if gc.gender.type_ else "",
            "age":      gc.age_range.type_.name if gc.age_range.type_ else "",
        })
    aggregate_types = Counter()
    audience_count = 0
    keyword_count = 0
    keyword_examples: list[str] = []
    interest_examples: list[str] = []
    for aid, crits in ag_crits.items():
        for c in crits:
            aggregate_types[c["type"]] += 1
            if c["interest"]:
                audience_count += 1
                if len(interest_examples) < 10:
                    interest_examples.append(c["interest"].rsplit("/", 1)[-1])
            if c["kw"]:
                keyword_count += 1
                if len(keyword_examples) < 10:
                    keyword_examples.append(c["kw"])
    print(f"  Sampled ad-groups: {len(ag_crits)} / {len(ag_ids)} agent-created total")
    print(f"  Criterion-type histogram: {dict(aggregate_types)}")
    print(f"  audience_segment criteria: {audience_count}")
    print(f"  keyword criteria: {keyword_count}")
    print(f"  Sample user_interest resources: {interest_examples}")
    print(f"  Sample keywords: {keyword_examples}")

    # 4. Ad-groups with ZERO criteria — count
    zero_crit = sum(1 for aid in ag_ids[:30] if aid not in ag_crits or len(ag_crits.get(aid, [])) == 0)
    print(f"  ad-groups with ZERO criteria attached: {zero_crit} / {min(len(ag_ids), 30)}")

if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        print(f"AUDIT FAILED: {type(exc).__name__}: {exc}", file=sys.stderr)
        sys.exit(1)

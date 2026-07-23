"""Ad-hoc: GMR-0023 live Meta spend + ad-set tracking/status snapshot.

Pulls, for every Meta campaign whose name references GMR-0023:
  - per-day spend (last 7d) + today, via the Insights API (campaign level)
  - each ad set's status / effective_status / daily_budget / promoted_object /
    optimization_goal (to assess tracking + find a rebuild-validation target)
Aggregates spend by locale. Read-only. Run: doppler run -- python3 scripts/adhoc_gmr0023_meta_spend.py
"""
import json
import re
import sys
import os
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.meta_api import MetaClient

LOCALES = ["ar-EG", "bn-IN", "de-DE", "es-MX", "fr-FR", "hi-IN", "id-ID",
           "it-IT", "ko-KR", "pt-BR", "th-TH", "tl-PH", "vi-VN", "zh-CN"]
RAMP = "GMR-0023"


def locale_of(name: str) -> str:
    if not name:
        return "?"
    for loc in LOCALES:
        if loc.lower() in name.lower():
            return loc
    # loose match on language half (e.g. "ko" / "vi")
    for loc in LOCALES:
        lang = loc.split("-")[0]
        if re.search(rf"[^a-z]{lang}[^a-z]", name.lower()):
            return loc
    return "?"


def _is_correct_tracking(po: dict) -> bool:
    return (
        str(po.get("pixel_id") or "") == str(config.META_PIXEL_ID)
        and (po.get("custom_event_type") or "").upper() == "OTHER"
        and (po.get("custom_event_str") or "") == config.META_CUSTOM_EVENT_STR
    )


def main():
    c = MetaClient()
    c._ensure_init()
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.campaign import Campaign
    from facebook_business.adobjects.adset import AdSet

    acct = AdAccount(c._ad_account_id)

    # 1) Find GMR-0023 campaigns by name.
    camps = acct.get_campaigns(
        params={"filtering": [{"field": "name", "operator": "CONTAIN", "value": "0023"}],
                "limit": 500},
        fields=["id", "name", "status", "effective_status", "daily_budget"],
    )
    camps = [dict(x) for x in camps]
    camps = [x for x in camps if "0023" in (x.get("name") or "")]
    print(f"== {len(camps)} GMR-0023 Meta campaigns ==")
    camp_ids = [x["id"] for x in camps]

    # 2) Insights: per-day spend last 7d (time_increment=1) + today, campaign level.
    def insights(date_preset, time_increment=None):
        params = {"level": "campaign", "date_preset": date_preset, "limit": 1000,
                  "filtering": [{"field": "campaign.name", "operator": "CONTAIN", "value": "0023"}]}
        if time_increment:
            params["time_increment"] = time_increment
        rows = acct.get_insights(
            params=params,
            fields=["campaign_id", "campaign_name", "spend", "impressions", "clicks", "actions"],
        )
        return [dict(r) for r in rows]

    last7 = insights("last_7d", time_increment=1)
    today = insights("today")

    # spend by locale (last 7d total + today)
    sp7 = defaultdict(float); sp_today = defaultdict(float); imp7 = defaultdict(int)
    days_seen = set()
    per_campaign_7 = defaultdict(float)
    for r in last7:
        loc = locale_of(r.get("campaign_name"))
        s = float(r.get("spend") or 0)
        sp7[loc] += s
        per_campaign_7[r.get("campaign_name")] += s
        if r.get("date_start"):
            days_seen.add(r["date_start"])
        imp7[loc] += int(r.get("impressions") or 0)
    for r in today:
        loc = locale_of(r.get("campaign_name"))
        sp_today[loc] += float(r.get("spend") or 0)

    ndays = max(1, len(days_seen))
    print(f"\n== SPEND BY LOCALE (Meta) — last 7d window covers {ndays} day(s) with data ==")
    print(f"{'locale':8} {'7d_spend':>10} {'avg/day':>9} {'today':>8} {'7d_impr':>9}")
    for loc in LOCALES + ["?"]:
        if sp7.get(loc) or sp_today.get(loc):
            print(f"{loc:8} {sp7.get(loc,0):>10.2f} {sp7.get(loc,0)/7:>9.2f} "
                  f"{sp_today.get(loc,0):>8.2f} {imp7.get(loc,0):>9}")
    print(f"{'TOTAL':8} {sum(sp7.values()):>10.2f} {sum(sp7.values())/7:>9.2f} {sum(sp_today.values()):>8.2f}")

    # 3) Ad sets: status + tracking + budget (for gap + rebuild candidate)
    print("\n== AD SETS (status / tracking / daily_budget) ==")
    rebuild_candidates = []
    adset_rows = []
    for cid in camp_ids:
        try:
            adsets = Campaign(cid).get_ad_sets(
                fields=["id", "name", "status", "effective_status", "daily_budget",
                        "optimization_goal", "promoted_object"],
                params={"limit": 200},
            )
        except Exception as e:
            print(f"  ! campaign {cid}: get_ad_sets failed: {e}")
            continue
        for a in adsets:
            a = dict(a)
            po = dict(a.get("promoted_object") or {})
            ok = _is_correct_tracking(po)
            loc = locale_of(a.get("name"))
            db = a.get("daily_budget")
            adset_rows.append({
                "locale": loc, "id": a["id"], "name": a.get("name"),
                "status": a.get("status"), "eff": a.get("effective_status"),
                "daily_budget": (int(db)/100 if db else None),
                "opt": a.get("optimization_goal"), "tracking_ok": ok, "po": po,
            })
            if not ok:
                rebuild_candidates.append(adset_rows[-1])

    for r in adset_rows:
        flag = "OK " if r["tracking_ok"] else "BAD"
        print(f"  [{flag}] {r['locale']:6} adset={r['id']} eff={r['eff']:<22} "
              f"budget={r['daily_budget']} opt={r['opt']} po={json.dumps(r['po'])[:60]}")

    print(f"\n== REBUILD CANDIDATES (wrong tracking): {len(rebuild_candidates)} ==")
    for r in rebuild_candidates:
        print(f"  adset={r['id']} locale={r['locale']} eff={r['eff']} po={json.dumps(r['po'])}")

    # Active-adset count per locale (so we can see coverage gaps)
    active_by_loc = defaultdict(int)
    for r in adset_rows:
        if (r["eff"] or "").upper() in ("ACTIVE", "CAMPAIGN_PAUSED", "ADSET_PAUSED", "PAUSED", "LEARNING"):
            pass
        if (r["status"] or "").upper() == "ACTIVE":
            active_by_loc[r["locale"]] += 1
    print("\n== ACTIVE ad sets per locale ==")
    for loc in LOCALES + ["?"]:
        print(f"  {loc:8} active_adsets={active_by_loc.get(loc,0)}")


if __name__ == "__main__":
    main()

"""Validate MetaClient.rebuild_adset_with_correct_tracking on ONE paused,
already-published, broken ad set. Proves the gated META_TRACKING_AUTO_REBUILD
copy path end-to-end WITHOUT pausing the old ad set or flipping the config gate.

Target: fr-FR old archived-conversion ad set (PAUSED). Run:
  doppler run -- python3 scripts/adhoc_validate_meta_rebuild.py
"""
import json
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
from src.meta_api import MetaClient

OLD = "120247747655780257"  # fr-FR, PAUSED, promoted_object = archived custom_conversion


def po_of(client, adset_id):
    return client.get_promoted_object(adset_id)


def status_of(adset_id):
    from facebook_business.adobjects.adset import AdSet
    a = dict(AdSet(adset_id).api_get(
        fields=["id", "name", "status", "effective_status", "campaign_id",
                "optimization_goal", "promoted_object"]))
    if a.get("promoted_object") is not None:
        a["promoted_object"] = dict(a["promoted_object"])
    return a


def ads_count(adset_id):
    from facebook_business.adobjects.adset import AdSet
    ads = list(AdSet(adset_id).get_ads(fields=["id", "name", "status"], params={"limit": 50}))
    return [dict(x) for x in ads]


def main():
    c = MetaClient()
    c._ensure_init()

    print(f"=== BEFORE: old ad set {OLD} ===")
    before = status_of(OLD)
    print(json.dumps({k: before.get(k) for k in
                      ("id", "name", "status", "effective_status", "campaign_id",
                       "optimization_goal", "promoted_object")}, indent=2))
    old_ads = ads_count(OLD)
    print(f"old ad set has {len(old_ads)} ad(s)")
    assert not c._is_correct if False else True  # noqa

    from src.meta_tracking_audit import _is_correct
    if _is_correct(before.get("promoted_object") or {}):
        print("!! old ad set already has correct tracking — pick a different target")
        return

    print("\n=== REBUILD: rebuild_adset_with_correct_tracking(OLD) ===")
    new_id = c.rebuild_adset_with_correct_tracking(OLD)
    print(f"new ad set id = {new_id}")

    print("\n=== AFTER: new copy ===")
    after = status_of(new_id)
    print(json.dumps({k: after.get(k) for k in
                      ("id", "name", "status", "effective_status", "campaign_id",
                       "optimization_goal", "promoted_object")}, indent=2))
    new_ads = ads_count(new_id)
    print(f"new ad set has {len(new_ads)} ad(s)")

    print("\n=== VALIDATION CHECKS ===")
    checks = {
        "new tracking correct (pixel-event)": _is_correct(after.get("promoted_object") or {}),
        "new ad set PAUSED": (after.get("status") == "PAUSED"),
        "ads copied (deep_copy)": len(new_ads) >= 1,
        "same parent campaign": (after.get("campaign_id") == before.get("campaign_id")),
        "old ad set untouched (still its prior status)": (status_of(OLD).get("status") == before.get("status")),
    }
    for k, v in checks.items():
        print(f"  [{'PASS' if v else 'FAIL'}] {k}")
    print("\nRESULT:", "ALL PASS" if all(checks.values()) else "SOME FAILED")
    print(f"\nNOTE: old ad set {OLD} was NOT paused (validation only). "
          f"New paused copy {new_id} created — delete it if you don't want the duplicate.")


if __name__ == "__main__":
    main()

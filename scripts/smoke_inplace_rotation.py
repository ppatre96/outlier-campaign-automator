"""One-campaign smoke test for the in-place creative-rotation primitives
(FEEDBACK_INPLACE_ROTATION, PR#83) — validates the LIVE LinkedIn pause calls
that couldn't be checked offline BEFORE you enable the flag in prod.

It exercises the two new/uncertain primitives against ONE real entity:
  • LinkedInClient.set_creative_status  (pause/resume a single creative —
    the risky one: /rest creatives endpoint + intendedStatus + URN key)
  • LinkedInClient.set_campaign_status  (pause/resume a campaign)

The add-creative side (create_image_ad(campaign=existing)) is already proven in
production, so it's not re-tested here.

READ-ONLY by default (prints what it WOULD do). Pass --apply to make the one
call. Fully reversible — re-run with --status ACTIVE to flip it back.

    # dry-run (no API call)
    doppler run -- venv/bin/python scripts/smoke_inplace_rotation.py --creative urn:li:sponsoredCreative:123

    # actually pause it, then flip it back
    doppler run -- venv/bin/python scripts/smoke_inplace_rotation.py --creative urn:li:sponsoredCreative:123 --apply
    doppler run -- venv/bin/python scripts/smoke_inplace_rotation.py --creative urn:li:sponsoredCreative:123 --status ACTIVE --apply
"""
from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

import config  # noqa: E402


def _token() -> str:
    return (os.getenv("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_ACCESS_TOKEN")
            or getattr(config, "LINKEDIN_TOKEN", "") or "")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--creative", help="Creative URN/id to pause+resume (the key primitive to validate).")
    ap.add_argument("--campaign", help="Campaign URN/id to pause+resume (optional).")
    ap.add_argument("--status", default="PAUSED", choices=["PAUSED", "ACTIVE", "ARCHIVED", "DRAFT"],
                    help="Target status (default PAUSED). Use ACTIVE to flip back.")
    ap.add_argument("--apply", action="store_true",
                    help="Make the live API call. Omit for a read-only dry-run.")
    args = ap.parse_args()

    if not (args.creative or args.campaign):
        print("Pass --creative <urn> and/or --campaign <urn>. Pick a LOW-STAKES live entity.")
        return 2

    token = _token()
    if not token:
        print("No LinkedIn token (LINKEDIN_TOKEN / LINKEDIN_ACCESS_TOKEN). Run under `doppler run`.")
        return 2

    plan = []
    if args.creative:
        plan.append(("creative", args.creative, f"PATCH /rest/adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives/<urn> intendedStatus={args.status}"))
    if args.campaign:
        plan.append(("campaign", args.campaign, f"PATCH /rest/adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/<id> status={args.status}"))

    print(f"\n{'APPLY' if args.apply else 'DRY-RUN'} — in-place rotation smoke test (target status={args.status}):\n")
    for kind, ref, desc in plan:
        print(f"  {kind:9} {ref}\n            → {desc}")

    if not args.apply:
        print("\nDry-run only — no API call. Re-run with --apply to make the single call above.")
        print("After --apply, verify in LinkedIn Campaign Manager, then flip back with --status ACTIVE --apply.")
        return 0

    from src.linkedin_api import LinkedInClient
    li = LinkedInClient(token)
    ok = True
    if args.creative:
        r = li.set_creative_status(args.creative, args.status)
        print(f"\n  set_creative_status({args.creative}, {args.status}) → {'OK' if r else 'FAILED'}")
        ok = ok and r
    if args.campaign:
        r = li.set_campaign_status(args.campaign, args.status)
        print(f"  set_campaign_status({args.campaign}, {args.status}) → {'OK' if r else 'FAILED'}")
        ok = ok and r

    print("\n" + ("✅ Primitive(s) worked. Verify the status flipped in Campaign Manager, flip back with "
                  "--status ACTIVE --apply, then enable FEEDBACK_INPLACE_ROTATION in Doppler (dev → prd)."
                  if ok else
                  "❌ A call failed — do NOT enable FEEDBACK_INPLACE_ROTATION yet. Check the logged HTTP error "
                  "(likely the creatives endpoint shape / intendedStatus / URN encoding) and fix the primitive first."))
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())

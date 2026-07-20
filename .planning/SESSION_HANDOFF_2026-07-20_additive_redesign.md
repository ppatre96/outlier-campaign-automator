# Handoff — Additive launch is the WRONG model + Meta reconciliation (2026-07-20)

## 🔴 CRITICAL redesign: "additive" must add creatives to the SAME campaign

**User intent (correct):** an additive / "new variation" launch should **add new ad
creatives (ads) to the EXISTING ad set / campaign** for that (cohort × geo × channel),
so one campaign accumulates fresh creatives as earlier ones fatigue / hit threshold.

**What was built this session (WRONG):** additive creates a brand-new Meta **campaign +
ad set per generation** via a new `generation` column. Result: separate v1/v2/v3
*campaigns* instead of one campaign gaining creatives. Undo/repurpose this.

### What that implies
- The `generation` dimension is at the **wrong grain**. It's on the campaigns table
  key `(ramp_id, platform, campaign_type, cohort_signature, geo_cluster, angle, generation)`
  (automator PR #114 + console PR #43, migration `012_campaigns_generation.sql`,
  index `campaigns_gen_key`). Generations should belong to **creatives/ads within an
  ad set**, not to campaigns/ad sets.
- Correct additive flow (Meta, and mirror for other channels): on additive launch,
  **look up the existing ad set** for (cohort × geo) and **create new Ads (creatives)
  under it** — do NOT create a new campaign group / ad set. Meta: `create_image_ad`
  against the existing `sub_id` (ad set), skip `create_campaign_group`/`create_campaign`.
- Pipeline entry points: `_process_extra_platform_arm` (main.py ~4316) currently always
  creates a new group + ad set. Additive needs a branch: resolve the live ad-set id for
  the cohort/geo (from the `campaigns` table / Meta) and attach ads only.
- Console: campaign browser should show ONE campaign per (cohort×geo) with its
  accumulating creatives, not v1/v2/v3 cards. Revisit `buildIndex` generation split
  (campaign-browser.tsx) + the "v{n}" badge.
- LinkedIn/Google/Reddit equivalents: add creatives to the existing campaign too.

### Config/flags in play (from the wrong model — reconcile during redesign)
- `config.ADDITIVE_LAUNCH` (automator) + `additive` workflow input + console Launch
  panel "Launch a new variation" toggle → currently triggers new-generation creation.
- `next_generation()` / `upsert_launch_progress` generation logic in `src/ui_decisions.py`.

## 🔴 Follow-up 1: reconcile DELETED / stale Meta campaigns
- Meta ad sets can be **DELETED** but the registry still marks them `active`, so the
  console shows dead campaigns + links that open empty campaigns ("no ad sets found").
- **Confirmed:** German **v2** ad set `120250890123710257` (campaign `120250890122910257`)
  is `effective_status=DELETED` on Meta, yet shown in console as active.
- Fix: a reconciliation pass — query Meta `effective_status` per campaign/ad set, mark
  registry rows `deleted`/`superseded` when DELETED, and have the console hide/label them
  + only link to live ones. (Graph API: `GET /{adset_id}?fields=campaign_id,effective_status`.)

## 🔴 Follow-up 2: why did v2 get DELETED?
- German v2 = DELETED (not paused/archived — unusual). Investigate what deleted it:
  the additive-as-new-campaign path, a cleanup, verify-and-heal, or manual. Likely tied
  to the wrong additive model. Confirm additive relaunches aren't destroying prior work.

## Ground truth (verified via Meta Graph API 2026-07-20, ramp GMR-0023)
- **German v1** ad set `120249164594820257` → PAUSED (LIVE). Campaign `120249164594620257`.
  Working link: `.../manage/campaigns?act=179828021490349&selected_campaign_ids=120249164594620257`
- **German v2** ad set `120250890123710257` → DELETED (campaign `120250890122910257`, empty).
- **Thai (anglo US/CA/GB/AU/NZ)** v1/v2/v3 exist (paused). **Thai SE-asian TH/MY/SG** still
  NOT on Meta — young-market block (subcode 1870249); the skip+flag-Tuan fix (PR #117) needs
  a FRESH additive run to create MY/SG + DM Tuan for TH.

## What shipped this session (context: some now known-wrong)
- Additive/generation: automator #114, console #43, migration 012. ← WRONG model, redesign.
- Young-market skip + Tuan flag: automator #117, console #49.
- QC retry cap 10→5 (both loops): automator #115, #116; `QC_MAX_RETRIES=5` in Doppler prd.
- Launch status: page-crash fix (console #48 — timestamptz Date vs string), in-flight from
  locks + h/d time (#47), "Last 5 triggered" (#50), created datetime + links (#51).
- Meta deep-link: parent-id swap (automator #119, console #53). Works, but surfaces DELETED
  gen2 → see Follow-up 1.
- Launch-progress table (earlier), Approve button removed, Review-prep tab, analytics
  channel filter, etc.

## Recommended next-session order
1. Redesign additive → **add creatives to existing ad set/campaign** (the core ask).
   Likely deprecate the generation-as-campaign approach + migration.
2. Reconcile deleted/stale Meta campaigns (Follow-up 1) so console shows only live ones.
3. Diagnose the v2 deletion cause (Follow-up 2).
4. Fresh additive Thai run to validate young-market skip (MY/SG created + Tuan DM'd).

See memory: [[reference_additive_launch_generation]] (mark superseded),
[[reference_meta_young_market_targeting]].

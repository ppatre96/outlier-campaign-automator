# Reddit Ads Channel — Plan & Activation Guide

## Where it stands today (Phase 1 — SHIPPED, on `main` via PR #27)

The Reddit channel is built as a **full peer of LinkedIn/Meta/Google** through the
same `_process_extra_platform_arm`, and runs end-to-end **except** the live
programmatic campaign create. Per `(cohort × geo × A/B/C angle)` it already:

- resolves the cohort + copy briefs,
- writes `_adapt_for_reddit` copy — TWO ad forms: a promoted-post **image ad**
  (title = headline + CTA button) and a **free-form/native text post**
  (peer-voice body, ≤1200 chars, brand-voice scanned),
- generates a **1:1 1200×1200** creative + runs QC + the low-res guard,
- resolves **subreddit / interest / keyword** targeting (`src/reddit_targeting.py`,
  per-pod subreddits from `config.REDDIT_POD_SUBREDDITS`),
- attaches the intended **per-pod `worker_skill_grant` conversion event**,
- exports the PNGs + a `_manual_handoff.json` manifest (targeting + conversion +
  copy + budget + formats) to Drive for **manual upload in Reddit Ads Manager**,
- logs to the campaign registry.

It is **gated OFF**: `config.REDDIT_API_ENABLED=false`, and `reddit` is **not** in
`ENABLED_PLATFORMS`. `RedditClient.create_image_ad` returns `local_fallback`;
`create_campaign_group` / `create_campaign` / `upload_image` raise
`NotImplementedError`. So today you get creatives + a manifest to upload by hand.

## Why it's gated: the Reddit Ads API is allow-list gated

Reddit's Ads API is **separate from the Ads Manager UI** and requires Reddit to
grant your account allow-list access. Until that's granted we cannot create
campaigns programmatically — hence Phase 1 (creatives + manifest) is the live
behavior and Phase 2 (programmatic) is stubbed.

---

## Phase 2 — programmatic create (the remaining build)

Once you have API access (see guide below), the agent implements the 4 stubbed
`RedditClient` methods in `src/reddit_api.py` against the **Reddit Ads API v3**
(`https://ads-api.reddit.com/api/v3`), each verified against Reddit's current
reference before coding:

1. **`create_campaign_group`** → `POST /ad_accounts/{id}/campaigns`
   (objective `CONVERSIONS`, `status: PAUSED`, budget).
2. **`create_campaign`** (= ad group) → `POST /ad_accounts/{id}/ad_groups`
   (campaign_id, targeting `{subreddits, interests, keywords, geo}`, conversion
   `{pixel_id, event}`, bid strategy, daily budget, `status: PAUSED`).
3. **`upload_image`** → Reddit media-upload endpoint → asset id.
4. **`create_image_ad`** → promoted-post image ad (title=headline, CTA) **and**
   the free-form/native text post, both referencing the ad group.

Plus: OAuth access/refresh-token handling, retry/backoff, and confirming whether
Reddit needs an employment/special-ad-category equivalent. Everything is created
**PAUSED** (mirrors the LinkedIn DRAFT / Meta PAUSED default) for human review.

After implementation: validate on ONE cohort (PAUSED, verify against the
manifest), then flip the gate.

---

## STEP-BY-STEP — what YOU need to do

These are the external prerequisites the agent can't do. Do them in order; the
last step is what turns the channel on.

1. **Apply for Reddit Ads API access (allow-list).** This is the gating blocker.
   Request via your Reddit account / sales rep or the Reddit Ads API access form.
   Outcome: your ad account is approved for programmatic API use.

2. **Create a Reddit OAuth app** (Reddit account → app preferences → create app,
   type "script"/"web"). Gives you a **client id** + **client secret**.
   → set `REDDIT_CLIENT_ID`, `REDDIT_CLIENT_SECRET`.

3. **Run the OAuth flow once** to mint an **access token** + **refresh token**
   (scopes for ads/creatives). → set `REDDIT_ACCESS_TOKEN`, `REDDIT_REFRESH_TOKEN`.

4. **Grab the Reddit Ad Account ID** (the `a2_…`/`t2_…` id from Ads Manager).
   → set `REDDIT_AD_ACCOUNT_ID`.

5. **From Tuan (conversion tracking):** set up the **Reddit Conversion Pixel** and
   the per-pod `worker_skill_grant` events, then give me:
   - `REDDIT_PIXEL_ID`
   - per-pod event names → `REDDIT_WS_EVENT_ALL`, `REDDIT_WS_EVENT_CODERS`,
     `REDDIT_WS_EVENT_SPECIALIST`, `REDDIT_WS_EVENT_LANGUAGES`,
     `REDDIT_WS_EVENT_GENERALIST`.
   (These are currently `"(pending Tuan)"` placeholders in the manifest.)

6. **Confirm the per-pod subreddit lists** with marketing (defaults live in
   `config.REDDIT_POD_SUBREDDITS`; override without code via
   `REDDIT_POD_SUBREDDITS_JSON`). Optionally `REDDIT_INTERESTS` / `REDDIT_KEYWORDS`.

7. **Put all of the above in Doppler** (`outlier-campaign-agent` → `dev` **and**
   `prd`).

8. **Turn it on** (the agent does this with you, after Phase 2 is coded + tested):
   - `REDDIT_API_ENABLED=true`
   - add `reddit` to `ENABLED_PLATFORMS`
   - `REDDIT_DEFAULT_DAILY_USD` if you want a non-$50 default.

**Minimum to unblock the agent's Phase-2 build:** step 1 (API access) + steps 2–4
(credentials). Steps 5–6 are needed before going live but not to start coding.

# Smart Ramp API — Requirements for the Outlier Campaign Pipeline

What the campaign automation needs from `genai-smart-ramp-v2` to run end-to-end:
auth/permissions, the read endpoints + exact field contracts, and the
UTM/landing-page data we want Smart Ramp to own. Written for Quintin to build
the API suite against.

Two consumers, **same endpoints, same auth**:
- **Pipeline** — Python, headless on GitHub Actions. `smart_ramp_poller.yml`
  every 5 min (discovery + detail) and `launch.yml` hourly. No browser.
- **Console** — Next.js on Vercel, server-side render of the ramp page
  (`lib/smartramp.ts`).

---

## 0. The one hard blocker — a machine credential

Everything below is read-only HTTP and already coded; the only thing actually
broken is **auth for headless callers**.

- Today: `Authorization: Bearer <TARGETS_API_TOKEN>` + `x-vercel-protection-bypass: <secret>`.
- Current state: bypass alone → **401**; bypass + our token → **403 `{"error":"Forbidden"}`**.
  So the bypass is fine — **the `TARGETS_API_TOKEN` itself is rejected** (revoked/expired/wrong scope).
- The MCP (`/api/mcp`) is **not** a fix for CI: its OAuth server advertises
  `grant_types_supported: ["authorization_code"]` only — a human browser flow.
  GitHub Actions has no browser and there's no `client_credentials` / no
  advertised refresh grant, so a bot can never mint or renew a token that way.

**Need one of:**
1. **Reissue a long-lived bearer** `TARGETS_API_TOKEN` (scope `ramps:read`) we
   set in Doppler `dev` + `prd`. Simplest — unblocks today. *(preferred)*
2. **Add a `client_credentials` grant** (machine token) to the OAuth server so
   CI can mint/renew non-interactively. Bigger, but the "proper" long-term door.

Either way: please document the **renewal path** so it can't silently expire the
way our LinkedIn token did (that caused a silent prod outage).

**Scopes we need:** `ramps:read` (required, all the reads in §1–§3) +
`targets:write` (confirmed — write-back in §6 is a go). `metrics:read` /
`checks:read` — not needed.

---

## 1. Read endpoints (REQUIRED — this is the whole input to the pipeline)

We are **read-only** against Smart Ramp today (no POST/PUT/PATCH anywhere). We
hit exactly two endpoints.

### 1.1 `GET /api/ramps/all` — discovery
The poller lists ramps every 5 min to find newly-ready ones. Lightweight items.

| Field | Type | Use |
|---|---|---|
| `id` | string | ramp id (e.g. `GMR-0024`) |
| `name` | string | display |
| `alias` | string | shown as "summary" in console |
| `status` | string | **drives the poller** — which ramps are ready to process |
| `startDate` / `endDate` | string | display |

Asks: reliable `status`; an `updatedAt` per item so we can poll deltas; and
pagination/a `?status=` filter if the list grows (today it's unpaginated).

### 1.2 `GET /api/ramps/{id}` — full detail
**The critical one.** Everything we build campaigns from. Today we read these
(field names per our current parsers — please confirm exact casing, see §7):

**Ramp-level:**

| Field | Type | Use |
|---|---|---|
| `id` | string | |
| `status` | string | gate |
| `submittedAt` | string | campaign naming (date segment) |
| `updatedAt` | string | freshness |
| `linearIssueId`, `linearUrl` | string\|null | console link-out |
| `formData.project_id` | string | identity |
| `formData.project_name` | string\|null | display + naming |
| `formData.requester_name` | string | display |
| `formData.summary` | string | display |
| `formData.cohorts[]` | array | **the targeting spec — see below** |

**Per-cohort (`formData.cohorts[]`):**

| Field | Type | Use |
|---|---|---|
| `id` | string | cohort identity |
| `cohort_description` | string | seeds cohort mining + ICP + (cold-start) the LLM job-post derivation |
| `signup_flow_id` | string\|null | **joins Stage A's Triggers Sheet rows + pay-rate resolution** |
| `selected_lp_url` | string\|null | a landing page — see §2 (we currently DON'T read this; want to converge) |
| `included_geos[]` | string[] | geo targeting + geo-cluster splitting |
| `matched_locales[]` | string[]\|null | locale/i18n cohorts |
| `target_activations` | number\|null | sizing |
| `job_post_id` | string\|null | LP-sheet join + pay-rate |
| `job_post_pod` | string\|null | naming + UTM `pod=` (`specialist\|generalist\|coders\|languages`) |
| `matched_domain` | string\|null | naming + UTM `domain=` + LP slug lookup |
| `job_post_domain` | string\|null | naming (Smart Ramp domain segment, e.g. `bn-IN`) |
| `job_post_language_code` | string\|null | naming + UTM `language=` (e.g. `en-US`) |
| `campaign_state` | object | channel-manager overrides — see §3 |

---

## 2. UTM + Landing Pages — what we want Smart Ramp to OWN

This is the messiest part today and the biggest win if Smart Ramp becomes the
source of truth. Right now the **final destination URL is assembled in the
pipeline** from three scattered sources, which means marketing's intent lives in
several places and we re-implement their mapping.

**How the pipeline resolves the LP today** (`src/utm_builder.py:resolve_base_lp_url`,
first match wins):
1. Console per-ramp override (Postgres) — reviewer manual.
2. `campaign_state.utm_<channel>.{base_url|url|lp_url|landing_page}` from Smart Ramp.
3. `matched_domain` → slug (`config.LP_URL_BY_DOMAIN`) → full URL via a
   **marketing-owned Google Sheet** ([Outlier Landing Pages](https://docs.google.com/spreadsheets/d/1AmM78xzVDf7UWV9icxODw6vxY12nSNZ6121Gcf0ibx4/edit), Published rows only).
4. `config.LINKEDIN_DESTINATION` hardcoded fallback.

**How we build the UTM string today** (`build_utm_url`): we append
`utm_source` (LinkedIn/Facebook/Google), `utm_medium=paid`, `pod`, `domain`,
`locale` (country only), `utm_campaign` (= the full campaign-name spec),
`language`, and for static creatives `utm_content` (`{stg_id}-{channel}-{angle}`)
+ `utm_concept`.

### DECISION: Smart Ramp can NOT own the full name/UTM — it's a two-part composite

The campaign **name** and **`utm_campaign`** can't be fully resolved upstream,
because the experimentation dimension — **which cohorts and which A/B/C angles to
test — is generated in OUR pipeline** (Stage A cohort mining + angle generation).
Smart Ramp doesn't know those until after we run. So naming/UTM is composed of
two halves:

- **Part 1 — imported from Smart Ramp (the upstream identity):** ramp id,
  `submittedAt` (date), `job_post_pod`, `matched_domain`, `job_post_language_code`
  (→ locale/language), `included_geos`. These are the leading segments of the
  campaign-name spec + the `pod`/`domain`/`locale`/`language` UTM params. **All
  already in §1.2 — no new fields needed; just keep them populated.**
- **Part 2 — generated by the pipeline (the experiment):** the mined cohort
  signature, the angle (A/B/C), `utm_content` (`{stg_id}-{channel}-{angle}`), and
  `utm_concept`. These don't exist upstream.

The pipeline assembles the final name (`src/campaign_name.py`) and UTM
(`src/utm_builder.py`) by concatenating **Part 1 (from you) + Part 2 (from us)**.
**→ So we do NOT want Smart Ramp to return a resolved `utm_campaign` or campaign
name** (it structurally can't). Just keep the Part-1 fields accurate.

### Landing page: `selected_lp_url` IS owned by Smart Ramp, per cohort

The **destination** is different from the name/UTM — the angle/sub-cohort
experimentation only changes the UTM *tags*, not *where the user lands*. So the LP
is per (upstream) cohort and Smart Ramp owns it:

- **We'll read `selected_lp_url` as the authoritative landing page** (one per
  cohort, shared across channels and across all our angle/sub-cohort splits) and
  **drop** the two pipeline-side LP layers we have today: the marketing
  Google-Sheet slug lookup AND `campaign_state.utm_<channel>.base_url`. *(The
  console per-ramp manual override stays — a reviewer escape hatch, not a data
  source.)*
- Net ask: **keep `selected_lp_url` populated per cohort** — it's the one LP field
  we'll now trust. (Confirm the angle/sub-cohort never needs its own LP; per the
  above it doesn't.)

---

## 3. `campaign_state` — channel-manager overrides (formalize, please)

`formData.cohorts[].campaign_state` carries per-cohort overrides a channel
manager saved at `/ramps/<id>/campaigns`. Current shape we consume:

```
campaign_state: {
  linkedin: { userGeo, liTargetingFacet, liAdLanguage, liAdFormat, groupingType,
              mainCountry, cohortBucketOverride, cohortLevelGroupingType,
              cohortLevelMainLocaleGroup, savedAt },
  meta:     { …same shape… },          // currently comes back empty
  utm_linkedin: { savedAt, …LP/UTM… },
  utm_joveo:    { savedAt, …LP/UTM… }   // = Google bucket
}
```

Today we actually wire: `linkedin.{liAdLanguage, mainCountry}` → LinkedIn API
locale; `linkedin.{liTargetingFacet, liAdLanguage, mainCountry, groupingType}` →
naming; `utm_linkedin`/`utm_joveo` → UTM/LP. Asks:
- **Stabilize the schema** (documented field set + types) so we can wire more
  safely. Pre-save cohorts currently return `{}` — keep that contract.
- **Add a `google.*` block** (doesn't exist yet) if Google should have
  channel-manager overrides like LinkedIn.
- Clarify the intended `meta.*` fields (your sample showed them all empty).

---

## 4. Targeting ("targets") — DECISION: status quo, pipeline keeps mining

**No targeting endpoint needed.** The pipeline continues to **mine targeting
itself** (Stage A off the screening data + ICP). Smart Ramp does **not** need to
serve a resolved target spec. The only inputs we need are the **seed fields
already in §1.2** — `cohort_description`, `included_geos`, `matched_locales`,
`job_post_*`. Your existing "update ramp targets" POST is unrelated to our read
path; we just won't consume a targets GET.

---

## 5. Bulk / N+1 (nice-to-have, scales the poller)

The poller does `GET /api/ramps/all` then a `GET /api/ramps/{id}` per ramp →
N+1 calls every 5 minutes. A single
`GET /api/ramps?status=ready&include=cohorts` that returns full detail for the
ready set in one response would remove the fan-out and the rate-limit risk.

---

## 6. Write-back — CONFIRMED (scope `targets:write`)

**This is a go.** After the pipeline creates/heals campaigns it will **POST
launch results per (ramp × cohort × channel)** so the Smart Ramp ramp page
reflects what's actually live. We'll build the client side; we need one write
endpoint.

**Preferred:** if your existing "update ramp targets" POST already accepts a
per-cohort/channel result payload, **point us at its path + schema and we'll
conform to it.** Otherwise, a dedicated endpoint:

```
POST /api/ramps/{id}/launch-results          // idempotent upsert
{
  "cohort_id": "…",
  "channel":   "linkedin" | "meta" | "google" | "google_search",
  "status":    "draft" | "launched" | "paused" | "healed" | "failed",
  "campaigns": [
    { "platform_campaign_id": "…",
      "name":          "Scale-GMR-0024 | LinkedIn | …",
      "audience_size": 124000,
      "angle":         "A",
      "creative_url":  "https://…",          // Shared-Drive PNG
      "landing_page":  "https://outlier.ai/experts/…" }
  ],
  "console_url": "https://outlier-campaign-console.vercel.app/ramps/GMR-0024",
  "reason":      "RSA disapproved (Health policy)",   // populated for healed/failed
  "updated_at":  "2026-06-09T…Z"
}
```

Upsert key: `(ramp_id, cohort_id, channel, platform_campaign_id)` — the pipeline
re-runs and may resend; please make it idempotent (update, don't duplicate).
Needs to accept the `healed` / `failed` + `reason` states so the upstream ramp
can show the same "couldn't create / keywords dropped" signals the console does.

**Question for you:** does the existing POST want one row per call, or a batch
array per ramp? We can do either — tell us the shape.

---

## 7. Contract hygiene — DECISION: we conform to Smart Ramp's CURRENT shape

**No schema change on your side.** Our Python and TS clients drifted (snake vs
camel casing; `summary` read from `formData` vs top-level; `job_post_domain`
present in one client only). Rather than ask you to change anything, **we'll
align both clients to whatever `/api/ramps/{id}` returns today.** All we need
from you: **confirm the canonical response shape as it currently stands** (a
sample JSON for one ramp incl. a fully-populated cohort is perfect) and we'll fix
our parsers to match it field-for-field.

Only genuine ask: keep status codes distinct — **401** (no/invalid auth) vs
**403** (authed, not allowed) vs **404** (no such ramp) — with JSON
`{"error": "..."}` bodies, so our error handling can branch correctly.

---

## TL;DR for the API suite (decisions locked 2026-06-09)

1. **Auth (only true blocker):** a headless machine token — reissue
   `TARGETS_API_TOKEN` or add a `client_credentials` grant — scopes
   `ramps:read` + `targets:write`, with a documented renewal path.
2. **`GET /api/ramps/all`** — discovery (+ reliable `status`; ideally
   `updatedAt`/filter/pagination).
3. **`GET /api/ramps/{id}`** — full detail per the §1.2 field contract.
4. **Name/UTM:** two-part composite — Smart Ramp owns **Part 1** (ramp/cohort
   identity, already in §1.2), the pipeline generates **Part 2** (mined cohort +
   angle). Smart Ramp does **not** resolve the full name/`utm_campaign`. **LP:**
   per-cohort `selected_lp_url` is authoritative — just keep it populated; we
   retire our LP sheet + `campaign_state` LP layers (§2).
5. **Targeting:** status quo — **pipeline keeps mining**, no targets endpoint (§4).
6. **`campaign_state`:** formalize the schema + add a `google.*` block (§3).
7. **Write-back: confirmed** — one idempotent POST of launch results per
   (cohort × channel), incl. `healed`/`failed` + `reason` (§6). Point us at your
   existing POST schema or use the proposed one.
8. **Contract hygiene:** no change on your side — **send us a current sample
   `/api/ramps/{id}` JSON** and we conform both clients to it (§7).

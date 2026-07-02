# Outlier Campaign Agent — Product Requirements & Operating Guide

**Audience:** Diego, Bryan (and anyone operating the campaign system)
**Owner:** Pranav Patre
**Last updated:** 2026-07-02
**Status:** Living document — the system is in active use and evolving.

---

## 1. TL;DR — what this is

The **Outlier Campaign Agent** turns a **Smart Ramp request** into ready-to-launch
ad campaigns across LinkedIn, Meta, and Google — automatically. Instead of hand-building
audiences, copy, and creatives for every candidate segment, you submit a Smart Ramp
(who you want to hire + where), and the system:

1. Mines our screening/application data to find the **audience segments most likely to pass screening**.
2. Writes **on-brand ad copy** and generates **AI images** for three angles (Expertise / Earnings / Flexibility).
3. Builds the campaigns on each ad platform **as drafts (paused)** — nothing spends money yet.
4. Shows everything in the **Outlier Console** (the web app) for you to **review, approve, and launch**.
5. Watches live performance daily and **recommends** which campaigns to keep, pause, or replace.

**The golden rule:** *The system only ever creates drafts. Nothing serves an ad or spends a
dollar until a human approves it in the Console and un-pauses it in the ad platform.*

There are two pieces:

| Piece | What it is | Who touches it |
|---|---|---|
| **Outlier Campaign Agent** | The automated backend "pipeline" (runs on a schedule in the cloud). | Pranav / engineering |
| **Outlier Console** | The web app you use — review, approve, launch, monitor. | **You (Diego & Bryan)** |

You live in the **Console**. This doc explains both so you understand what's happening behind the buttons.

---

## 2. Who does what

| Automatic (the pipeline does it) | Manual (you do it) |
|---|---|
| Reads the Smart Ramp request | Submit the Smart Ramp (project, cohorts, geos, landing page) |
| Finds the best audience cohorts from data | Review the cohorts/ICP/angles and sanity-check them |
| Writes copy + generates images (3 angles) | Approve channels + set daily budgets |
| Creates **draft/paused** campaigns on every channel | Launch each channel from the Console |
| Uploads creatives to Google Drive | Set Google "Employment" category toggle in Ads Manager |
| Posts status to Slack | Confirm landing page + budget, then **un-pause to go live** |
| Scores live performance daily + recommends actions | Accept/Reject the recommendations |

> **You never build a campaign by hand — including Google Search.** The pipeline builds
> Search (keywords + Responsive Search Ads) too. Building anything manually creates duplicates.

---

## 3. The pipeline, stage by stage

This is what runs behind the scenes after a Smart Ramp is submitted. You don't trigger these
individually — they run in order automatically. Listed so you know what each Console card is showing you.

| # | Stage | Plain English |
|---|---|---|
| 0–2 | **Data pull + features** | Pulls screening/application data from our warehouse; extracts resume signals (skills, titles, education). |
| A | **Cohort discovery** | Statistically finds which combinations of skills/titles/education best predict passing screening. Ranks the top audience segments. |
| B | **Country validation** | Confirms each cohort has real geographic demand in the target countries. |
| C | **Audience sizing (LinkedIn)** | Maps cohorts to LinkedIn targeting and checks the audience is a usable size (roughly 1K–500K). |
| 1 | **Competitor intelligence** | Weekly scrape of competitor job boards, ad libraries, Reddit, and search trends → informs copy hooks and what to avoid. |
| 8a | **Brief generation** | For each cohort × angle, writes a creative brief (headline direction, who's in the photo, mood, colors). |
| 8b | **Copy generation** | Writes 3 copy variants per angle and enforces Outlier's approved vocabulary (e.g. "task" not "job," "payment" not "compensation"). |
| 8c | **Creative generation** | Generates the ad images with AI, composites the copy on top, uploads to Google Drive. |
| 9 | **Campaign creation** | Builds the actual campaigns on LinkedIn / Meta / Google as **DRAFT / PAUSED**. |
| 10 | **Save + notify** | Records everything to the database + Campaign Registry sheet, posts a Slack update with a Console link. |
| 11 | **Brief review gate** | (If enabled) Ramp waits for you to review the briefs in the Console. |
| 12 | **Per-channel launch** | You approve + launch each channel from the Console. Campaigns get built on-platform (still paused). |
| 13 | **Daily performance feedback** | Every day, pulls live metrics and recommends keep / pause / replace per campaign. |
| 14 | **Weekly audit** | Every Monday, QC-checks recent campaigns (targeting, copy, creative, spend) and posts a summary to Slack. |

---

## 4. Channels supported

| Channel | Status | Notes |
|---|---|---|
| **LinkedIn — Sponsored Content (static image)** | ✅ Live | Created as DRAFT. |
| **LinkedIn — InMail (message ads)** | ✅ Live | Created as DRAFT. |
| **Meta (Facebook / Instagram)** | ✅ Live | Created as PAUSED. "Employment" special ad category applied. |
| **Google — Display** | ✅ Live | Created as PAUSED. Responsive display ads. |
| **Google — Search** | ✅ Live | Created as PAUSED. Pipeline owns keywords + Responsive Search Ads. |
| **TikTok** | 🟡 Creative-only | Generates the images/video frames to Drive; upload is manual. |
| **Reddit** | 🟡 Creative-only | Generates creatives + targeting notes to Drive; programmatic launch pending API access. |

---

## 5. How you use the Console — step by step

**1. Open the ramp.** Go to the Console → click the ramp (e.g. `GMR-0024`). Prep runs
automatically within ~5 min of submission. When ready, the ramp shows **"awaiting review/approval."**

**2. Review the cards (top to bottom):**

| Card | What to check |
|---|---|
| **ICP** | Does the described person match who you actually want? |
| **Cohorts / Reach per channel** | Audience size per channel. Green ≥100k, amber 50–100k, red <50k. "not gated" = no number available (fine for niche/keyword targeting). |
| **Angles we'd test** | The A/B/C copy directions. Sanity-check the messaging. |
| **Targeting** | Skills/titles/interests/keywords per channel. |
| **Competitor landscape** | Differentiators + competitor signals (context only). |

If something's off, leave a comment on the card or ping Pranav — **don't approve yet.**

**3. Approve (the gate).** In **"Approve channels & budgets"**: pick channels, set the daily
budget per channel, click **Approve**. 👉 *Approval does NOT launch anything — it records your decision.*

**4. Launch by channel.** In **"Launch by channel,"** launch each approved channel. The pipeline
builds the draft campaigns + creatives on that platform and reports back (a "created" badge appears).
Use **Relaunch (replace)** only when you want to rebuild a channel from scratch (it archives the old set first, so no duplicates).

**5. Verify on the ad platform.** Open LinkedIn / Meta / Google, confirm campaigns + creatives look
right, do the manual bits (Google Employment toggle, LP/budget), then **un-pause to go live.**

**6. Check "⚠️ Needs review."** Appears only when the pipeline couldn't fully build something.
Each row shows the real reason from the ad platform. Two kinds:
- **Couldn't create — auto-archived:** the campaign was built but no ad could attach (policy block,
  image failure). The empty shell is auto-archived so nothing broken goes live. Fix the source, then Relaunch (replace).
- **Live with keywords dropped:** a Google Search campaign is live, but some keywords were rejected.
  The campaign runs on the survivors. Usually safe to leave.

---

## 6. Decision points that are yours (and only yours)

1. **Which channels to run** and **the daily budget** for each (the Approve step).
2. **The Google "Employment" special ad category toggle** — must be set manually in Google Ads Manager
   (the API can't set it) before un-pausing Google campaigns.
3. **Landing page + budget confirmation** per channel.
4. **The final un-pause** in each ad platform — nothing serves until you do this.
5. **Accept/Reject the daily performance recommendations** (see below).

---

## 7. The performance feedback loop

Once campaigns are live and accumulating spend, the system watches them:

- **Daily (08:30 UTC):** Pulls live impressions / clicks / spend / CPA from each platform, scores every
  campaign as **working / underperforming / failing**, and writes a **keep / pause / replace**
  recommendation with a reason (e.g. *"CPA above ceiling," "CTR below floor — test a fresh angle"*).
- These appear in the Console's **"Live performance & recommendations"** section with **Accept / Reject** buttons.
  - **Accept "pause"** → the campaign's budget is set to $0 on LinkedIn/Google automatically (Meta must be paused manually in Ads Manager).
  - **Accept "replace"** → flags the campaign for a fresh creative in the winning direction.
- **Angle winners:** The system also compares the A/B/C angles per cohort and flags the winner (to scale)
  and losers (to pause/refresh).
- **Weekly (Monday):** A QC audit of recent campaigns posts to Slack.

**Current state (2026-07-02):** The loop **recommends** reliably every day, and the daily pass now also
**executes the safe angle actions automatically** in production (pause losing-angle Meta ads so delivery
shifts to the winner; increase-only budget bumps on winners, capped; creative "refresh" drafts). Losing
angles still appear as Console recommendations for your visibility. The **weekly** cross-ramp analysis
(funnel + sentiment + ICP drift) is now on a Monday schedule and posts a consolidated Slack summary.
LinkedIn recommendations use fresh metrics (refreshed daily via Redash). Remaining roadmap item: auto-replacing
LinkedIn creatives on its own schedule.

---

## 8. Where things live

- **Creatives (images):** Google Drive, under `<ramp_id>/<channel>/<cohort_geo>/<angle>.png`.
- **Campaign tracking:** the Campaign Registry Google Sheet + the Console database.
- **Campaigns:** LinkedIn / Meta / Google Ads Managers — as DRAFT/PAUSED until you launch + un-pause.
- **Notifications:** Slack (DMs to Pranav/Diego/Bryan + the shared automation channel).
- **The Console:** the web app you log into (Google SSO, @scale.com allowlist).

---

## 9. Scheduling (how often things run)

| What | When |
|---|---|
| Poll for new Smart Ramps + run the pipeline | Every 5 minutes |
| Daily performance recommendations | 08:30 UTC daily |
| Weekly QC audit | Mondays 17:00 UTC |
| Competitor intelligence refresh | Weekly |
| Secondary creatives (TikTok/FB/IG) | On-demand (Console button) |
| Budget updates | On-demand (Console) |

---

## 10. Known limitations / roadmap

- **LinkedIn creative attach** can be blocked by a pending LinkedIn platform entitlement (MDP) — sometimes requires a manual creative upload.
- **Google Ads write access** on the child account is subject to an access approval.
- **Reddit** is creative-only until the Ads API allow-list clears.
- **Feedback loop** (mostly closed 2026-07-02): weekly funnel/sentiment/drift analysis is now scheduled;
  auto-act on angle winners/losers is now enabled in production (safe actions only). Remaining: put the
  LinkedIn creative auto-replacement on its own schedule.
- **In-Console chat assistant** — a help chat so you can ask "how do I…" questions without pinging Pranav (shipping alongside this doc).

---

## 11. Glossary

- **Smart Ramp** — the campaign request (project + cohorts + geos + landing page) that kicks everything off.
- **Cohort** — an audience segment defined by skills/titles/education/geo.
- **ICP** — Ideal Candidate Profile; the described person we're targeting.
- **Angle (A/B/C)** — a messaging direction: Expertise, Earnings, Flexibility.
- **Draft / Paused** — a campaign that exists on the platform but isn't spending. The default for everything.
- **CPA** — cost per acquisition (per application/activation). Lower is better.
- **CTR** — click-through rate. Higher generally means the creative resonates.

---

*Questions? Use the in-Console **Chat** (ask it anything about using the tool), comment on a ramp card, or ping Pranav in Slack.*

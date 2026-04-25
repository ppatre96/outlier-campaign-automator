# ROADMAP: Outlier Campaign Agent

**Project:** End-to-end LinkedIn campaign automation from screening data to live campaign  
**Granularity:** Coarse (3 phases)  
**Coverage:** 17/17 v1 requirements mapped  
**Last updated:** 2026-04-21

---

## Phases

- [x] **Phase 1: Pipeline Integrity** — Fix all silent skips and hard blockers so a full dry run completes end-to-end
- [x] **Phase 2: Observability & Storage** — Close the reporting loop with Slack delivery, Drive persistence, and lifecycle monitoring (completed 2026-04-21)
- [~] **Phase 2.5: Feedback Loops & Experimentation** — v1 shipped 2026-04-21 (creative/cohort CTR+CPA + experiment backlog + Slack reaction reanalysis). V2 re-opened 2026-04-24 to extend with full-funnel conversion (signup → screening pass → activation), social + support sentiment mining, automatic weekly ICP drift detection, and cron-scheduled orchestration.
- [ ] **Phase 3.1: Figma Creative Replication Integration** — Integrate completed Figma layer builder into agent pipeline; pass photo_base64 and create editable frames
- [ ] **Phase 3: Campaign Expansion** — Regenerate STEM InMails with the winning financial angle and extend targeting buckets

---

## Phase Details

### Phase 1: Pipeline Integrity

**Goal**: The full pipeline — screening fetch through creative generation and LinkedIn publish — runs to completion without silent skips, NameErrors, or hard-fails caused by missing configuration guards.

**Why here**: Nothing in Phase 2 or 3 is testable until the core pipeline executes reliably. Observability is useless if the thing being observed never completes. Campaign expansion is pointless if image ad creatives cannot attach.

**Depends on**: Nothing (first phase)

**Requirements**: PIPE-01, PIPE-02, PIPE-03, PIPE-04, PIPE-05, LI-01, LI-02, LI-03, LI-04

**Tasks**:

1. **Remove `mj_token`/`claude_key` guard — `main.py` lines ~66, 283–287**
   - The guard `has_mj = bool(mj_token and claude_key)` gates `generate_midjourney_creative()` on a Midjourney token that the pipeline does not use (Gemini is the actual backend). Remove the guard entirely; call `generate_midjourney_creative()` unconditionally.
   - File: `main.py` — locate `has_mj` assignment and the conditional block wrapping the `generate_midjourney_creative` call; delete both.

2. **Fix `SCREENING_END_DATE` stale default — `config.py` line 31, call sites**
   - Change the default in `config.py` from `"2025-12-31"` to `datetime.utcnow().date().isoformat()` (add `from datetime import datetime` if not already imported).
   - Additionally, pass `end_date=date.today().isoformat()` explicitly at every `fetch_screenings()` call site in `main.py` and `scripts/dry_run.py` so no callers depend on the config default.

3. **Confirm `classify_tg` import is in `main.py` lines 28–32**
   - PIPE-01 is marked Complete (2026-04-21), but verify the import block includes `classify_tg` from `src.figma_creative`. If not yet committed, add it.
   - File: `main.py` lines 28–32.

4. **Unblock `create_image_ad` — `LINKEDIN_MEMBER_URN` — `config.py` line 43, `src/linkedin_api.py` lines 359–365**
   - Option A (preferred): Request `r_liteprofile` scope on the existing OAuth app. After token re-auth, call `GET https://api.linkedin.com/v2/me` to retrieve the person URN and set `LINKEDIN_MEMBER_URN=urn:li:person:<id>` in `.env`.
   - Option B (immediate): Look up the LinkedIn profile URL for the OAuth token owner manually. The profile URL slug is the `<id>` in `urn:li:person:<id>`. Set `.env` accordingly.
   - Whichever path: document the lookup steps and expiry date in `README.md` under a "LinkedIn Token Setup" section.
   - File: `.env` (set `LINKEDIN_MEMBER_URN`), `README.md` (document lookup).

5. **Add graceful Stage C bypass in `main.py` matching `scripts/dry_run.py` pattern**
   - `scripts/dry_run.py` already catches Stage C exceptions and falls back to Stage B top cohorts with a logged reason. `main.py` currently propagates the `RuntimeError`, aborting the entire row.
   - Wrap the Stage C call in `main.py` `_process_row()` in a `try/except` block. On failure: log the reason (MDP not approved), use Stage B top cohorts (slice to `MAX_CAMPAIGNS`), and continue. Do not abort the row.
   - File: `main.py` — `_process_row()` Stage C call site; `src/stage_c.py` lines 48–68 for reference.

6. **Confirm LinkedIn token auto-refresh path works — `src/linkedin_api.py` lines 64–71, 95–108**
   - Verify `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, and `LINKEDIN_REFRESH_TOKEN` are all set in `.env`.
   - Trigger a manual refresh by calling `linkedin_client.refresh_access_token()` directly (or let a real API call hit a 401). Confirm the new token is written back to `.env` and subsequent calls succeed.
   - Document expiry (`expires_at=1781441848` ≈ June 2026) and re-auth steps in `README.md`.

7. **Run full dry run — `scripts/dry_run.py`**
   - Execute `python scripts/dry_run.py --flow-id <a real flow_id>` and confirm all 5 observable outputs appear:
     - Stage A: cohort list printed
     - Stage B: country validation printed
     - Stage C: audience counts printed (or bypass log line)
     - Creative gen: PNG saved to `data/dry_run_outputs/`
     - LinkedIn publish: dry-run log lines for campaign group, campaign, image upload, creative attach

**Success Criteria** (what must be TRUE):
1. `python scripts/dry_run.py --flow-id <id>` completes without NameError, RuntimeError, or silent skip — all 5 stage outputs are printed to stdout.
2. A PNG file appears in `data/dry_run_outputs/` after a dry run that does not use `--skip-creatives`.
3. `python main.py --mode launch --dry-run` processes a PENDING row through creative generation without the `mj_token` guard blocking it.
4. Stage C failure (MDP not approved) logs a clear reason and the pipeline continues with Stage B cohorts rather than aborting.
5. A manual `refresh_access_token()` call succeeds and writes a new `LINKEDIN_ACCESS_TOKEN` line to `.env`.

**Blockers / external dependencies**:
- LinkedIn `r_liteprofile` scope: requires OAuth app settings change + token re-auth (not a code change)
- LinkedIn MDP approval for audienceCounts: currently pending; Phase 1 adds the graceful bypass so the pipeline runs regardless
- `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, `LINKEDIN_REFRESH_TOKEN` must be populated in `.env` before token refresh test

**Plans**: 4 plans

Plans:
- [x] 01-01-PLAN.md — Core pipeline bug fixes (has_mj guard, SCREENING_END_DATE, Stage C bypass, InMail gate, GEMINI_API_KEY check)
- [x] 01-02-PLAN.md — Environment config + LinkedIn hardening (INMAIL_SENDER_URN, classify_tg verify, create_image_ad blocker logging)
- [x] 01-03-PLAN.md — Verification (LinkedIn token refresh test + full dry-run acceptance)
- [x] 01-04-PLAN.md — Sub-agent pipeline orchestration (agent trace logging, AGENT-PIPELINE.md, context validation)

---

### Phase 2: Observability & Storage

**Goal**: Weekly Slack reports arrive automatically, generated creatives are persisted to Drive and logged to Sheets, and underperforming campaigns are flagged by the lifecycle monitor — closing every reporting loop the pipeline currently skips silently.

**Why here**: Observability requires the pipeline to complete successfully (Phase 1). Storage and reporting can then run reliably without chasing phantom errors from broken pipeline stages. These two areas are grouped because they share the same "did the run produce output?" dependency and neither blocks Phase 3.

**Depends on**: Phase 1

**Requirements**: OBS-01, OBS-02, OBS-03, OBS-04, DATA-01, DATA-02

**Tasks**:

1. **Get Slack reporting working — `scripts/post_weekly_reports.py` lines 29–43, `config.py` line 101**
   - Decision required: webhook vs Bot Token. Workspace restrictions blocked incoming webhooks (per PROJECT.md). Recommended path: create a Slack Bot with `chat:write` scope, install to workspace, and use `client.chat_postMessage(channel="...", text=report_text)` in `post_weekly_reports.py` instead of a webhook POST.
   - If webhook is unblocked: set `SLACK_WEBHOOK_URL` in `.env`. The existing code already handles this path.
   - After either path: run `python scripts/post_weekly_reports.py` manually and confirm the report appears in the target Slack channel.
   - File: `scripts/post_weekly_reports.py` (lines 29–43), `config.py` (line 101).

2. **Enable cron for Monday 9 AM IST delivery**
   - Current cron entry: `30 3 * * 1` (Monday 3:30 AM UTC = 9 AM IST). Verify this entry exists in the host's crontab or equivalent scheduler. If running on a dev machine, document the launchd plist or equivalent macOS scheduler entry.
   - After cron is confirmed, Monday delivery is automatic (OBS-01).

3. **Enable Google Drive upload — `config.py` lines 78–82, `src/gdrive.py`, `main.py` lines 328–334**
   - Create a Google Workspace Shared Drive (personal Drive folders do not support service account Content Manager role).
   - Add `outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com` as Content Manager on the Shared Drive.
   - Update `GDRIVE_FOLDER_ID` in `.env` to the new Shared Drive folder ID.
   - Set `GDRIVE_ENABLED=true` in `.env`.
   - Run a creative generation with `--dry-run=false` and confirm: PNG uploaded to Drive, Drive URL logged to `Creatives` tab column matching DATA-02.
   - Files: `.env` (two values), `src/gdrive.py` (setup instructions already in lines 1–22).

4. **Log creative URN and Drive URL to Sheets after successful creative upload — `src/sheets.py`, `main.py` lines 328–334**
   - Verify `SheetsClient.write_creative()` writes the creative URN to the `Creatives` tab (LI-03). Add Drive URL write if not already present when `GDRIVE_ENABLED=true`.
   - Files: `main.py` lines 328–334, `src/sheets.py` `write_creative()` method.

5. **Verify campaign lifecycle monitor is running — `src/campaign_monitor.py`, `main.py --mode monitor`**
   - Run `python main.py --mode monitor` against a real row with `li_status` starting with `"Created:"`. Confirm the monitor reads campaigns, checks learning phase, and logs KEEP/PAUSE/TEST_NEW verdicts.
   - Confirm `pause_campaign()` issues the correct PATCH to `status=PAUSED` for underperformers (OBS-04).
   - File: `src/campaign_monitor.py`, `main.py run_monitor()`.

6. **Static ad weekly report — `src/static_weekly_report.py`**
   - OBS-03 requires the static report to populate when static campaigns are active. Confirm the report query (`VIEW.LINKEDIN_CREATIVE_COSTS` filtered to image ad campaigns) runs without error. If no static campaigns have run yet, add a clear "no active static campaigns" log line rather than posting an empty/broken report.
   - File: `src/static_weekly_report.py`.

7. **Document audienceCounts MDP status — `src/stage_c.py`, `README.md`**
   - Add a `README.md` section: "Stage C: audienceCounts Status". State clearly: MDP approval pending for account 510956407. Until approved, Stage C gracefully bypasses and logs the reason. Link to the LinkedIn MDP application.
   - File: `README.md`.

**Success Criteria** (what must be TRUE):
1. Running `python scripts/post_weekly_reports.py` manually delivers an InMail performance report to the configured Slack channel within 60 seconds.
2. A cron or scheduler entry for Monday 3:30 AM UTC is confirmed active on the host — no manual trigger required after that.
3. After a live creative generation run, the generated PNG appears in the Shared Drive folder and the Drive URL appears in the `Creatives` Google Sheet tab.
4. `python main.py --mode monitor` reads existing campaigns and logs a KEEP/PAUSE/TEST_NEW verdict for each without crashing.
5. `post_weekly_reports.py` does not post a blank or broken static report when no static campaigns are active — it posts a "no active campaigns" notice or skips the static section.

**Blockers / external dependencies**:
- Slack Bot Token or webhook: requires Workspace Admin to approve the integration
- Shared Drive creation: requires Google Workspace Admin (if Workspace account) or Google account with Drive access
- Service account must be invited as Content Manager to the Shared Drive — manual step outside code
- LinkedIn MDP approval: does not block Phase 2 (bypass already added in Phase 1); document status only

**Plans**: 4 plans

Plans:
- [x] 02-01-PLAN.md — Slack Bot Integration (slack-sdk, WebClient, crontab)
- [x] 02-02-PLAN.md — Drive URL fix + Sheets write_creative() extension
- [x] 02-03-PLAN.md — LLM context quality: validate_photo_subject() + flow docs
- [x] 02-04-PLAN.md — Lifecycle monitor Slack wiring (depends on 02-01)

**UI hint**: no

---

### Phase 2.5: Feedback Loops & Experimentation

**Goal**: Enable continuous optimization by collecting creative and cohort performance feedback, generating experiment hypotheses, and feeding results back into future creative generation and cohort analysis.

**Why here**: Feedback loops require Phase 2's observability infrastructure (Slack reports, Drive storage, campaign monitoring) to provide the performance data. Once we can measure what works, we can systematically test hypotheses and improve. This enables Phase 3 (Campaign Expansion) to expand based on data-driven insights rather than guesswork.

**Depends on**: Phase 1, Phase 2

**Requirements (v1 — shipped 2026-04-21)**: FEED-01, FEED-02, FEED-03, FEED-04, FEED-05, FEED-06, FEED-07, FEED-08, FEED-09, FEED-10, FEED-11, FEED-12, FEED-13, FEED-14

**Requirements (V2 — re-opened 2026-04-24)**: FEED-15, FEED-16, FEED-17, FEED-18, FEED-19, FEED-20, FEED-21, FEED-22, FEED-23

**Two New Agents**:

1. **feedback_agent** — Analyzes creative and cohort performance
   - Creative scope: Identifies best/worst performing headline/subheadline/photo_subject per cohort. Generates hypotheses on why.
   - Cohort scope: Identifies underperforming cohorts (CPA > baseline, CTR trending down). Posts weekly Slack alert with recommendations.

2. **experiment_scientist_agent** — Designs and tracks experiments
   - Ingests feedback_agent output + competitor_bot insights + new screening data
   - Maintains experiment backlog (priority queue of hypotheses)
   - Communicates test directives to ad-creative-brief-generator
   - Tracks results for next week's feedback loop

**Weekly Cycle**:
1. feedback_agent analyzes creative & cohort performance → generates hypotheses
2. experiment_scientist_agent accumulates insights → decides what to test
3. Per-run: ad-creative-brief-generator checks backlog → 20% test variants, 80% baseline
4. User reviews weekly Slack alert → pauses underperforming cohorts or requests tests
5. outlier_data_analyst reruns on fresh screening data → discovers new cohorts

**Success Criteria** (what must be TRUE):
1. feedback_agent runs weekly, identifies underperforming cohorts (CPA > 2σ or CTR ↓ trend)
2. experiment_scientist_agent receives feedback + competitor data, maintains experiment backlog
3. ad-creative-brief-generator receives test directive and uses test variant 20% of the time
4. Weekly Slack alert posts with top 3 underperforming cohorts + recommendation
5. User can react to Slack alert to pause cohort or request new angle test
6. outlier_data_analyst reruns Stage A on fresh data, discovers new cohorts
7. New cohort definitions fed back to campaign-manager for next run

**Blockers / external dependencies**:
- Phase 2 must be complete first (Slack reporting, lifecycle monitoring)
- Redash queries must be accurate for creative + cohort performance analysis
- Memory system must persist experiment backlog across restarts

**Plans (v1)**: 02.5-01 (feedback_agent core), 02.5-02 (Slack alerts + reactions), 02.5-03 (experiment_scientist_agent + backlog), 02.5-04 (reanalysis loop) — all shipped.

---

### Phase 2.5 V2 Extension (2026-04-24)

**Goal**: Close the optimization loop end-to-end by (a) extending feedback to full-funnel conversion, (b) ingesting contributor sentiment from public + internal channels, (c) detecting ICP drift automatically week-over-week, and (d) running the whole loop on a weekly cron without manual trigger.

**Why here**: v1 tells us which creatives get clicks but not which creatives produce paying contributors. Without full-funnel data, every "winner" is a hypothesis. Sentiment mining surfaces the pain points that copy should address (or avoid), and ICP drift detection catches audience shift before CPA degrades rather than after.

**V2 Plans**:

1. **Plan 05 — Full-funnel conversion tracking**
   - Extend `feedback_agent` (or wire `campaign_feedback_agent`) to decompose creative × cohort into four funnel stages: click → signup → screening-pass → activation
   - Identify the stage where each underperforming cohort loses contributors (top-of-funnel vs conversion vs retention)
   - Inject funnel-drop diagnosis into weekly Slack alert

2. **Plan 06 — Sentiment miner**
   - New `src/sentiment_miner.py` that scrapes Reddit (r/Outlier_AI, r/BeerMoney, r/WorkOnline), Trustpilot, Glassdoor, Apple/Play Store reviews, Outlier Discourse forum weekly
   - Ingests internal Zendesk/Intercom tickets (auth-gated) for current-contributor issues
   - Uses LLM to extract issue themes and produces `data/sentiment_callouts.json` feeding `ad-creative-brief-generator` with validated "address X" / "avoid Y" copy directives

3. **Plan 07 — Automatic ICP drift detector**
   - New `src/icp_drift_monitor.py` snapshots Stage 1 ICP output weekly
   - Computes week-over-week feature distribution diff (KL divergence or chi-square on categorical bins) → drift score
   - Auto-triggers `outlier-data-analyst` reanalysis when drift > configurable threshold — no Slack reaction needed

4. **Plan 08 — Weekly cron orchestrator**
   - `scripts/weekly_feedback_loop.py` wires plans 05/06/07 + existing 02.5 `feedback_agent` + `reanalysis_loop` into a single ordered run
   - Crontab entry: `30 3 * * 1` (Monday 9 AM IST) with idempotency guard (skip if a run succeeded within last 6 days)
   - Consolidated Slack report covers funnel drops, sentiment themes, ICP drift alerts

**Success Criteria (V2)**:
1. Weekly Slack report breaks down every cohort by click → signup → screening → activation rates
2. Underperformer alerts name the exact funnel stage where drop occurs
3. `data/sentiment_callouts.json` refreshed weekly with at least 3 scored issue themes per active TG
4. `ad-creative-brief-generator` reads `sentiment_callouts.json` and honors at least one "address" / "avoid" directive in test briefs
5. ICP drift scored weekly; drift > threshold triggers `outlier-data-analyst` rerun without user intervention; new cohorts reach `campaign-manager` within the same weekly cycle
6. Full pipeline runs from cron on Monday 9 AM IST unattended; logs + artifacts persist on failure

**Blockers / external dependencies (V2)**:
- Reddit read access (no auth needed for public subs, rate-limit aware)
- Trustpilot scrape path or API — Firecrawl acceptable
- Zendesk/Intercom credentials in `.env` (scope: read tickets)
- Outlier Discourse API token (if available) or scrape-friendly access
- Server/container with reliable cron (macOS laptop cron is unreliable; document host choice)
- LLM budget for sentiment classification (LiteLLM gateway OK)

**V2 Plans**: 4 plans

Plans:
- [x] 02.5-05-PLAN.md — Full-funnel conversion tracking (FEED-15, FEED-16)
- [x] 02.5-06-PLAN.md — Sentiment miner (FEED-17, FEED-18, FEED-19)
- [x] 02.5-07-PLAN.md — Automatic ICP drift detector (FEED-20, FEED-21)
- [ ] 02.5-08-PLAN.md — Weekly cron orchestrator (FEED-22, FEED-23)

---

### Phase 3.1: Figma Creative Replication Integration

**Goal**: Connect completed Figma layer builder (`build_figma_layered_frame_js()`) into the campaign pipeline by updating agent instructions, passing `photo_base64` from image generation, and verifying end-to-end frame creation with editable photo + gradient + text layers.

**Why here**: Image generation (Gemini) is complete but pipeline doesn't embed photos in editable Figma frames. Phase 3.1 bridges this gap, enabling designers to customize generated creatives. This is a **prerequisite for Phase 3** because STEM InMail campaigns will use photo-backed frames.

**Depends on**: Phase 1 (pipeline must run), Phase 2.5 complete

**Requirements**: IMG-01, IMG-02, IMG-03

**Tasks**:

1. **Update agent instructions** — `~/.claude/agents/outlier-creative-generator.md`
   - Replace Stage 8g to use `build_figma_layered_frame_js()` instead of text-only clone
   - Document input: `context["photo_base64"]` as Gemini PNG in base64 format
   - Document output: 3 Figma frames (A/B/C) with raster photo, gradient overlays, editable text

2. **Modify pipeline to pass `photo_base64`**
   - Convert Gemini PNG to base64 in `src/midjourney_creative.py` or `scripts/dry_run.py`
   - Add `context["photo_base64"]` in `main.py` context assembly before agent call
   - Verify base64 conversion works for typical image sizes without NameError

3. **End-to-end test**
   - Run `python scripts/dry_run.py --flow-id <test-id> --skip-linkedin` to test creative generation
   - Verify in Figma: 3 frames with correct names, visible photo backgrounds, angle-specific gradients, editable text
   - Confirm no regressions in LinkedIn publish, Sheets logging, dry-run output

**Success Criteria** (what must be TRUE):
1. `outlier-creative-generator.md` Stage 8g references `build_figma_layered_frame_js()` and documents `photo_base64` input
2. Pipeline passes `context["photo_base64"]` to agent without NameError
3. End-to-end test creates 3 Figma frames with:
   - Correct frame names (`project_id_A/B/C_v1`)
   - Visible raster photo backgrounds
   - Angle-specific gradient colors (A ≠ B ≠ C)
   - Editable text layers with correct content
4. No regressions in other pipeline stages

**Blockers / external dependencies**:
- None (implementation complete, integration pending)

**Plans**: 1 plan

Plans:
- [ ] 03.1-01-PLAN.md — Agent instructions + pipeline photo_base64 + end-to-end test

---

### Phase 3: Campaign Expansion

**Goal**: STEM InMail campaigns regenerate with the proven financial angle and the targeting classifier is reviewed for new cohort types, so the pipeline can address audiences discovered in ongoing screening data.

**Why here**: Expansion makes no sense until the pipeline runs reliably (Phase 1), its output is observable (Phase 2), and we have feedback loops to guide expansion decisions (Phase 2.5). STEM regen specifically requires the InMail pipeline to be unblocked end-to-end, which Phase 1 delivers. New TG buckets are low-risk once the classifier is known-good and we have data on what works.

**Depends on**: Phase 1, Phase 2, Phase 2.5, Phase 3.1

**Requirements**: EXP-01, EXP-02

**Tasks**:

1. **Regen STEM InMail variants with financial angle — campaigns 633412886, 635201096, 634012966**
   - These are the STEM campaigns where the financial angle (rate in subject line) has not yet been applied.
   - Run `build_inmail_variants(tg_cat, cohort, claude_key)` via `scripts/generate_experiment_creatives.py` or a targeted `main.py` run, using angle `F` (financial) for the three campaign IDs.
   - Set the trigger rows in `Triggers 2` to `PENDING` with `ad_type=INMAIL` pointing at these campaign groups, or run a direct regeneration script.
   - Confirm new InMail content URNs are created and logged to Sheets.
   - Files: `src/inmail_copy_writer.py` (`build_inmail_variants`), `main.py` `_process_inmail_campaigns()`, `scripts/generate_experiment_creatives.py`.

2. **Review and extend `classify_tg` TG buckets — `src/figma_creative.py` line 57**
   - Current buckets: `DATA_ANALYST`, `ML_ENGINEER`, `MEDICAL`, `LANGUAGE`, `SOFTWARE_ENGINEER`, `GENERAL`.
   - Review Stage A output from recent pipeline runs. If any cohort names or rule features do not match any bucket's regex (falling through to `GENERAL`), assess whether a new bucket would produce better-targeted copy.
   - If new cohort types are surfaced (e.g. `FINANCE`, `LEGAL`, `DESIGN`): add regex patterns to `classify_tg()` and corresponding prompt templates to `build_inmail_variants()`.
   - File: `src/figma_creative.py` `classify_tg()` function, `src/inmail_copy_writer.py` angle-specific prompt blocks.

**Success Criteria** (what must be TRUE):
1. Campaigns 633412886, 635201096, and 634012966 each have at least one new InMail creative with angle F (financial / rate-in-subject-line) attached and their `li_status` updated in `Triggers 2`.
2. A `classify_tg` review pass is completed: every cohort produced by Stage A in recent runs maps to a non-`GENERAL` bucket, OR new buckets are added for unmatched cohort types, OR the `GENERAL` fallback is explicitly confirmed as correct for those cohorts.
3. Running `python main.py --mode launch` on a fresh PENDING row produces InMail copy where `LINKEDIN_INMAIL_SENDER_URN` is set and the financial angle is applied for STEM cohorts.

**Blockers / external dependencies**:
- `LINKEDIN_INMAIL_SENDER_URN` must be set in `.env` (currently empty; all InMail rows are silently skipped without it — see `main.py` lines 365–370 and `config.py` line 39). This is a hard prerequisite for EXP-01.
- Recent Stage A output needed to assess new TG bucket requirements (run pipeline after Phase 1 is complete)

**Plans**: TBD

---

## Progress Table

| Phase | Plans Complete | Status | Completed |
|-------|----------------|--------|-----------|
| 1. Pipeline Integrity | 4/4 | Complete | 2026-04-20 |
| 2. Observability & Storage | 4/4 | Complete   | 2026-04-21 |
| 2.5. Feedback Loops & Experimentation | 4/4 | Complete    | 2026-04-21 |
| 3.1. Figma Creative Integration | 1/1 | Complete | 2026-04-21 |
| 3. Campaign Expansion | 0/2 | Ready | — |

---

## Coverage

| Requirement | Phase | Status |
|-------------|-------|--------|
| PIPE-01 | Phase 1 | Complete |
| PIPE-02 | Phase 1 | Complete |
| PIPE-03 | Phase 1 | Complete |
| PIPE-04 | Phase 1 | Complete |
| PIPE-05 | Phase 1 | Complete |
| LI-01 | Phase 1 | Blocked |
| LI-02 | Phase 1 | Blocked |
| LI-03 | Phase 1 | Pending |
| LI-04 | Phase 1 | Complete |
| OBS-01 | Phase 2 | Pending |
| OBS-02 | Phase 2 | Pending |
| OBS-03 | Phase 2 | Pending |
| OBS-04 | Phase 2 | Pending |
| DATA-01 | Phase 2 | Pending |
| DATA-02 | Phase 2 | Pending |
| FEED-01 | Phase 2.5 | Pending |
| FEED-02 | Phase 2.5 | Pending |
| FEED-03 | Phase 2.5 | Pending |
| FEED-04 | Phase 2.5 | Pending |
| FEED-05 | Phase 2.5 | Pending |
| FEED-06 | Phase 2.5 | Pending |
| FEED-07 | Phase 2.5 | Pending |
| FEED-08 | Phase 2.5 | Pending |
| FEED-09 | Phase 2.5 | Pending |
| FEED-10 | Phase 2.5 | Pending |
| FEED-11 | Phase 2.5 | Pending |
| FEED-12 | Phase 2.5 | Pending |
| FEED-13 | Phase 2.5 | Pending |
| FEED-14 | Phase 2.5 | Pending |
| FEED-15 | Phase 2.5 V2 | Pending |
| FEED-16 | Phase 2.5 V2 | Pending |
| FEED-17 | Phase 2.5 V2 | Pending |
| FEED-18 | Phase 2.5 V2 | Pending |
| FEED-19 | Phase 2.5 V2 | Pending |
| FEED-20 | Phase 2.5 V2 | Pending |
| FEED-21 | Phase 2.5 V2 | Pending |
| FEED-22 | Phase 2.5 V2 | Pending |
| FEED-23 | Phase 2.5 V2 | Pending |
| IMG-01 | Phase 3.1 | Pending |
| IMG-02 | Phase 3.1 | Pending |
| IMG-03 | Phase 3.1 | Pending |
| EXP-01 | Phase 3 | Pending |
| EXP-02 | Phase 3 | Pending |

**v1 requirements: 17 total — 17 mapped | v2 requirements: 14 feedback/experimentation requirements (Phase 2.5) + 3 Figma integration requirements (Phase 3.1)**

---

*Roadmap initialized: 2026-04-21*

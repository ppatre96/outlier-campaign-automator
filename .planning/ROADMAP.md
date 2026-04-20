# ROADMAP: Outlier Campaign Agent

**Project:** End-to-end LinkedIn campaign automation from screening data to live campaign  
**Granularity:** Coarse (3 phases)  
**Coverage:** 17/17 v1 requirements mapped  
**Last updated:** 2026-04-21

---

## Phases

- [ ] **Phase 1: Pipeline Integrity** — Fix all silent skips and hard blockers so a full dry run completes end-to-end
- [ ] **Phase 2: Observability & Storage** — Close the reporting loop with Slack delivery, Drive persistence, and lifecycle monitoring
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

**Plans**: TBD

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

**Plans**: TBD
**UI hint**: no

---

### Phase 3: Campaign Expansion

**Goal**: STEM InMail campaigns regenerate with the proven financial angle and the targeting classifier is reviewed for new cohort types, so the pipeline can address audiences discovered in ongoing screening data.

**Why here**: Expansion makes no sense until the pipeline runs reliably (Phase 1) and its output is observable (Phase 2). STEM regen specifically requires the InMail pipeline to be unblocked end-to-end, which Phase 1 delivers. New TG buckets are low-risk once the classifier is known-good.

**Depends on**: Phase 1, Phase 2

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
| 1. Pipeline Integrity | 0/7 | Not started | - |
| 2. Observability & Storage | 0/7 | Not started | - |
| 3. Campaign Expansion | 0/2 | Not started | - |

---

## Coverage

| Requirement | Phase | Status |
|-------------|-------|--------|
| PIPE-01 | Phase 1 | Complete |
| PIPE-02 | Phase 1 | Pending |
| PIPE-03 | Phase 1 | Pending |
| PIPE-04 | Phase 1 | Pending |
| PIPE-05 | Phase 1 | Pending |
| LI-01 | Phase 1 | Blocked |
| LI-02 | Phase 1 | Blocked |
| LI-03 | Phase 1 | Pending |
| LI-04 | Phase 1 | Pending |
| OBS-01 | Phase 2 | Pending |
| OBS-02 | Phase 2 | Pending |
| OBS-03 | Phase 2 | Pending |
| OBS-04 | Phase 2 | Pending |
| DATA-01 | Phase 2 | Pending |
| DATA-02 | Phase 2 | Pending |
| EXP-01 | Phase 3 | Pending |
| EXP-02 | Phase 3 | Pending |

**v1 requirements: 17 total — 17 mapped — 0 unmapped**

---

*Roadmap initialized: 2026-04-21*

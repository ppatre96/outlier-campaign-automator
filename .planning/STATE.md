---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: Ready to plan
last_updated: "2026-04-25T04:17:18.555Z"
progress:
  total_phases: 5
  completed_phases: 4
  total_plans: 21
  completed_plans: 20
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-21)

**Core value:** End-to-end campaign automation from screening data to live LinkedIn campaign — zero manual steps once triggered.
**Current focus:** Phase 02.5 — feedback-loops-experimentation

## Current Phase

**Phase 2.5 — Feedback Loops & Experimentation (V2 extension)**
Goal: Enable continuous optimization by collecting creative/cohort performance feedback, generating experiment hypotheses, and driving weekly A/B testing.

**Status: Phase 2.5 v1 COMPLETE; V2 extension code-COMPLETE (8/8 plans done). FEED-22 fully live pending user-side launchd plist + crontab dedup (commands documented in README + 02.5-08-SUMMARY).**

- Plan 01 (FeedbackAgent) — COMPLETE
- Plan 02 (Weekly Slack Alert + Reaction Handler) — COMPLETE (commit c9fc4dd)
- Plan 03 (ExperimentScientistAgent) — COMPLETE (commits 8c932e1, 444c2f4)
- Plan 04 (Reanalysis Loop) — COMPLETE (commits 10780b6, c0dad93, f92e0d6)
- Plan 05 (Full-Funnel Conversion Tracking, V2) — COMPLETE (commits 8d3218f, 6bad53c, f4dbfdf, 5948e60)
- Plan 06 (Sentiment Miner, V2) — COMPLETE (commits 55ce247, d787c20, 724e1f5, 207c557)
- Plan 07 (ICP Drift Monitor, V2) — COMPLETE (commits 598bbe7, a7aac61, 741dcf5)
- Plan 08 (Weekly Cron Orchestrator, V2) — COMPLETE in code (commits bec625c, bc8660b, d591c47, dca53d0); awaiting USER ACTION on launchd plist + crontab dedup (see 02.5-08-SUMMARY.md)

## Completed Phases

- Phase 1 — Pipeline Integrity (COMPLETE)
- Phase 2 — Observability & Storage (COMPLETE)

## Planned Phases

- Phase 2.5 — Feedback Loops & Experimentation (PLANNED, 4 plans ready for execution)

## Known Blockers

- `LINKEDIN_MEMBER_URN` — needs correct OAuth token owner identity to unblock `create_image_ad`
- LinkedIn MDP approval — needed for audienceCounts Stage C (account 510956407)
- `SLACK_BOT_TOKEN` — current value is xoxe.xoxp- (user token); needs xoxb- bot token from new Slack App for DM delivery (create app at api.slack.com, chat:write scope)
- Google Drive — needs Shared Drive created and service account added as Content Manager

## Decisions

- Set `LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0` (Tuan's URN) for InMail testing (D-06) — 2026-04-20
- `create_image_ad` failure now logs tiered RuntimeError handler with LINKEDIN_MEMBER_URN check and scope explanation (`r_liteprofile`/`rw_organization_admin`) (D-07) — 2026-04-20
- Remove has_mj guard — creative generation now runs via LITELLM_API_KEY unconditionally (D-01) — 2026-04-21
- Dynamic SCREENING_END_DATE default — datetime.utcnow().date().isoformat() in config.py + explicit call site (D-04) — 2026-04-21
- Stage C graceful bypass — try/except with cohorts_b[:config.MAX_CAMPAIGNS] fallback (D-05) — 2026-04-21
- InMail gate removed — build_inmail_variants unconditional via LiteLLM (D-03) — 2026-04-21
- Premature GEMINI_API_KEY raise removed — _generate_imagen() handles LiteLLM-first correctly (D-02) — 2026-04-21
- [Phase 02]: Bot Token (chat.postMessage) tried first in _post_to_slack(); Incoming Webhook kept as fallback (D-08)
- [Phase 02]: validate_photo_subject uses regex matching on lowercased input against 7 known generic patterns (D-08)
- [Phase 02]: drive_url defaults to empty string in write_creative() for backward compatibility; Drive upload wrapped in try/except so LinkedIn creative attach continues even on Drive failure (D-08)
- [Phase 02]: read_monitor_summary() takes sheets arg (not constructing own SheetsClient) to avoid second auth round-trip in post_weekly_reports.py
- [Phase 02.5-01]: FeedbackAgent delegates all Redash queries to RedashClient (dependency injection) for testability
- [Phase 02.5-01]: identify_underperforming_cohorts uses numpy median+ddof=1 std (not mean) for z-score — robust to outliers
- [Phase 02.5-02]: FeedbackAgent created inline (Rule 3): plan 02.5-01 not yet run in parallel execution
- [Phase 02.5-02]: SlackReactionHandler uses callback registry pattern: register_reaction_callback(emoji, fn); in-memory dedup for one-time reactions
- [Phase 02.5-feedback-loops-experimentation]: ReanalysisOrchestrator uses async trigger_reanalysis() for Stage A rediscovery; sync wrapper exported for CLI
- [Phase 02.5-feedback-loops-experimentation]: Financial angle A assigned by default to all newly staged cohorts from reanalysis (locked decision from CONTEXT.md)
- [Phase 02.5]: priority_score = impact × confidence (0.9) × feasibility (0.9); 20/80 test/baseline allocation; dedup by (cohort, angle, photo_subject)
- [Phase 03-campaign-expansion]: MATH bucket added to classify_tg at priority slot 3 (between ML_ENGINEER and MEDICAL); locked regex covers math/statistics/actuary/quantitative/physicist/probability/econometrics/biostatistics (EXP-02)
- [Phase 03-campaign-expansion]: StubCohort dataclass sufficient for build_inmail_variants; lazy client construction in live mode only; angle-F fallback gap documented as known limitation
- [Phase 02.5-05]: Median-of-stage rates as cohort baseline (robust to outliers); earliest-stage-wins drop classification
- [Phase 02.5-05]: FUNNEL_DROP_ALERT_THRESHOLD=0.30 default (configurable via .env); 'Funnel Drop Diagnosis:' Slack section header chosen for vocabulary compliance
- [Phase 02.5-05]: generate_slack_alert extended with optional funnel_diagnosis kwarg (defaults None) — v1 two-arg callers unchanged
- [Phase 02.5]: [Phase 02.5-06]: Apple App Store + Google Play Store fetchers SKIPPED — Outlier has no native mobile app (RESEARCH-V2 verified 2026-04-24)
- [Phase 02.5]: [Phase 02.5-06]: sentiment_miner LLM defaults to anthropic/claude-haiku-4-5 with one-time fallback to config.LITELLM_MODEL on model-not-found
- [Phase 02.5]: [Phase 02.5-06]: Defense-in-depth vocabulary scrub — LLM system prompt enforces CLAUDE.md vocabulary AND _scrub_vocab regex post-process catches leakage before write
- [Phase 02.5]: [Phase 02.5-06]: PII rule — Zendesk + Intercom ticket bodies truncated to 800 chars in-memory + 400 chars on disk; never persists requester name/email
- [Phase 02.5]: [Phase 02.5-07]: scipy.stats.entropy for KL divergence (no hand-rolled math); EPSILON=1e-10 on both p and q vectors keeps disjoint distributions finite per Pitfall 4
- [Phase 02.5]: [Phase 02.5-07]: Strict < 7-day rate-limit comparison so a trigger exactly 7 days after the last is allowed to re-fire; per-project state in data/icp_drift_state.json
- [Phase 02.5]: [Phase 02.5-07]: _invoke_trigger uses inspect.iscoroutine to handle both sync (current) and async ReanalysisOrchestrator.trigger_reanalysis signatures
- [Phase 02.5]: [Phase 02.5-08]: scripts/weekly_feedback_loop.py orchestrator wires v1 alerts + V2 funnel/sentiment/drift into one Monday cron with filelock idempotency, 6-day skip window, step isolation, dry-run safety. FEED-22/FEED-23 code-complete; system-level launchd+crontab edits documented as USER ACTION REQUIRED in README.
- [Phase 02.5]: [Phase 02.5-08]: load_dotenv() runs before import config (Pitfall 6 fix); filelock.FileLock(timeout=10) wraps run_once (Pitfall 5 fix); --dry-run skips both Slack post AND check_and_trigger reanalysis trigger.

## Session Notes

- 2026-04-25 (plan 02.5-08): Weekly Cron Orchestrator complete in code. scripts/weekly_feedback_loop.py (546 LOC) wires v1 alerts (Step A) + V2 funnel (Step B) + V2 sentiment (Step C) + V2 ICP drift (Step D) into one Monday 09:00 IST cron with filelock.FileLock(timeout=10) idempotency guard, 6-day SKIP_WINDOW from data/weekly_feedback_loop_state.json.last_success_ts (--force bypasses), step isolation via per-step try/except (one step failing never aborts others), loud-failure Slack contract (always posts even on failure), dry-run safety (--dry-run skips both Slack post and check_and_trigger reanalysis). load_dotenv() before import config (Pitfall 6 fix). 4-section consolidated Slack message (Creative Progress Alerts -> Funnel Drop Diagnosis -> Sentiment Themes -> ICP Drift) using approved Outlier vocabulary. 5 unit tests passing (test_idempotency, test_dry_run, test_step_isolation, test_consolidated_slack, test_slack_vocabulary), 48/48 total tests green. requirements.txt: filelock>=3.28.0 pinned. README.md +92 lines documenting cron setup + manual run commands + log/state paths. USER ACTION REQUIRED: launchd plist edit + crontab dedup commands (system-level changes the agent does NOT execute per critical_constraints; documented in README + 02.5-08-SUMMARY). FEED-22/FEED-23 code-complete. Plan 08 of Phase 2.5 V2 COMPLETE. Commits: bec625c (filelock pin), bc8660b (orchestrator), d591c47 (tests), dca53d0 (README). Progress: [██████████] 95%
- 2026-04-25 (plan 02.5-07): ICP Drift Monitor complete. src/icp_drift_monitor.py (264 LOC) with snapshot/compute_drift/check_and_trigger/categorical_kl public API. scipy.stats.entropy for KL divergence with EPSILON=1e-10 zero-bin guard (no hand-rolled math). Drift score = max(categorical KL across worker_source/resume_degree/resume_field/resume_job_title/experience_band) + sum(numeric abs-mean-shifts across total_payout_attempts/task_count_30d). Auto-triggers ReanalysisOrchestrator.trigger_reanalysis(reason="icp_drift") when drift > ICP_DRIFT_THRESHOLD AND n_rows >= ICP_DRIFT_MIN_ROWS AND no reanalysis in past 7d. Per-project last_reanalysis_ts persisted in data/icp_drift_state.json with strict < 7-day comparison (boundary-tested). 5 unit tests (4 required + 1 boundary), all mocked, all green. config.py +3 constants (ICP_DRIFT_THRESHOLD=0.15, ICP_DRIFT_MIN_ROWS=200, ICP_DRIFT_LOOKBACK_WEEKS=4). requirements.txt pinned pyarrow>=23.0.0. FEED-20, FEED-21 complete. Plan 07 of Phase 2.5 V2 COMPLETE. Commits: 598bbe7, a7aac61, 741dcf5. Progress: [█████████░] 90%
- 2026-04-25 (plan 02.5-06): Sentiment Miner complete. src/sentiment_miner.py (611 lines) with 6 fetchers (Reddit, Trustpilot, Glassdoor, Discourse, Zendesk, Intercom) + LiteLLM theme extractor + JSON writer. Apple/Google Play deliberately skipped (no native mobile app). 5 unit tests passing, all mocked. .env.example created (first in repo). Brief generator agent extended with Sentiment-Driven Copy Inputs section. FEED-17, FEED-18, FEED-19 complete. Plan 06 of Phase 2.5 V2 COMPLETE. Commits: 55ce247, d787c20, 724e1f5, 207c557. Progress: [█████████░] 86%
- 2026-04-21 (plan 02.5-02): Weekly Slack alert + reaction handler complete. post_weekly_feedback_alert() integrated into scripts/post_weekly_reports.py. SlackReactionHandler class created with parse_cohort_from_message() helper. Config values added (SLACK_REACTION_BOT_USER_ID, SLACK_FEEDBACK_CHANNEL_ID, CPA thresholds, REACTION_EMOJI_MAPPING). FEED-07, FEED-08 complete. Phase 2.5-02 COMPLETE. Commit: c9fc4dd
- 2026-04-20 (plan 02-04): Lifecycle monitor Slack wiring complete. read_monitor_summary() added to campaign_monitor.py, wired into post_weekly_reports.py as 3rd report section. monitor dry-run verified (exit 0). OBS-04 marked complete. Phase 02 COMPLETE. Progress: [███████░░░] 67%
- 2026-04-21 (plan 02-02): Drive URL + Sheets write_creative() fix complete. drive_url wired as optional 5th column, GDRIVE_ENABLED guard + try/except, README.md created with Shared Drive admin steps. Requirements DATA-01, DATA-02 marked complete. Progress: [██████░░░░] 58%
- 2026-04-20 (plan 03): Acceptance test complete. Token refresh verified (new token persisted to .env, API call 200 OK). Dry-run pipeline executed: stages 0-3 tested, PNG files confirmed in data/dry_run_outputs/, main.py --dry-run processed 4 rows without crash. Phase 1 COMPLETE. Progress: [████████████] 100%
- 2026-04-20 (plan 02): Set LINKEDIN_INMAIL_SENDER_URN, verified classify_tg import callable, hardened create_image_ad blocker log. Progress: [███████░░░] 67%
- 2026-04-21: Project initialized. Codebase map written (7 docs). Critical bug fixed: `classify_tg` import added to `main.py`.
- 2026-04-20: LinkedIn API session — campaign group, campaign, image upload all working. `create_image_ad` blocked on DSC post author. Performance: Stage A/B 8 sec (was 43 min).

## Last Session

Completed Phase 02.5 V2 Plan 08 (Weekly Cron Orchestrator) — 2026-04-25

- scripts/weekly_feedback_loop.py (546 LOC): main() + run_once() + 4 step runners (_step_v1, _step_funnel, _step_sentiment, _step_drift) + 4 section formatters + idempotency helpers (_should_skip, _read_state, _write_state) + active-projects resolver
- CLI flags: --dry-run (skips Slack post + reanalysis trigger), --force (bypasses 6-day skip), --only {v1,funnel,sentiment,drift} (debugging)
- load_dotenv() runs at module top BEFORE import config to fix Pitfall 6 (config.py reads env vars at import time)
- filelock.FileLock(str(LOCK_PATH), timeout=10) wraps run_once entirely; concurrent invocation exits 0 cleanly with "another instance running" log (Pitfall 5 fix)
- 6-day SKIP_WINDOW with strict < timedelta(days=6) comparison; --force bypasses; missing/malformed last_success_ts falls through to run (defensive parsing)
- Step isolation: every step body wrapped in try/except, returns dict with "ok" bool + "error" field on failure; even _step_drift's outer module import is wrapped
- Loud failure: run_once ALWAYS posts to Slack — _build_failure_message on any failure naming step + error class; _build_consolidated_message on full success with 4 sections
- Dry-run safety: _step_v1 short-circuits without calling post_weekly_reports.main(); _step_drift calls idm.snapshot + idm.compute_drift but BYPASSES idm.check_and_trigger (no outlier-data-analyst invocation); run_once logs the consolidated message via log.info instead of _post_to_slack
- Section order locked per CONTEXT-V2: Creative Progress Alerts (v1) -> Funnel Drop Diagnosis -> Sentiment Themes (top 5) -> ICP Drift
- Vocabulary: every Slack-facing string passes the banned-token regex (no compensation/project rate/job/role/interview/bonus/promote/required); verified by both runtime regex AND test_slack_vocabulary unit test
- In-process v1 call: Step A imports scripts.post_weekly_reports and calls main() directly (vs subprocess) so its SystemExit and exceptions are catchable
- Active projects resolution: data/active_projects.json -> OUTLIER_TRACKING_PROJECT_ID env var -> empty list with warning (drift step short-circuits cleanly when no projects)
- Logging: date-stamped logs/weekly_feedback_loop/<yyyy-mm-dd>.log + stdout; old handlers cleared on each invocation so re-running in same Python process doesn't double-log
- 5 unit tests, all using tmp_path/monkeypatch, all mocked: test_idempotency (5 cases inc. malformed timestamps), test_dry_run (zero Slack calls), test_step_isolation (4 step keys present even when funnel fails; loud-failure Slack post issued), test_consolidated_slack (all 4 section headers + per-step content), test_slack_vocabulary (banned regex against both success and failure messages)
- 48/48 total tests green (5 new + 43 prior); no regressions
- requirements.txt: filelock>=3.28.0 pinned (forward-looking — installed venv has 3.25.2)
- README.md +92 lines: "Weekly Feedback Loop (Phase 2.5 V2)" section with USER ACTION REQUIRED commands (launchd plist edit + launchctl reload + crontab dedup + verification), manual run examples, log/state/lock/snapshots/callouts paths, idempotency contract docs
- FEED-22 + FEED-23 marked complete (FEED-22 with caveat: code-complete; live cron requires user-side commands)
- USER ACTION REQUIRED (per critical_constraints — system-level edits NOT performed by agent): (1) edit ~/Library/LaunchAgents/com.outlier.weekly-reports.plist ProgramArguments to point at scripts/weekly_feedback_loop.py, (2) launchctl unload + load that plist, (3) crontab -l | grep -v post_weekly_reports | crontab - to remove duplicate cron line, (4) verify via crontab -l | grep post_weekly_reports (must be empty) and grep weekly_feedback_loop.py ~/Library/LaunchAgents/com.outlier.weekly-reports.plist
- Commits: bec625c (chore filelock pin), bc8660b (feat orchestrator), d591c47 (test 5 unit tests), dca53d0 (docs README section)

Phase 2.5 V2 is now 8/8 plans complete. Optimization loop closed end-to-end: weekly cron -> v1 alerts + V2 funnel/sentiment/drift -> consolidated Slack post -> outlier-data-analyst auto-reanalysis on drift -> new cohorts back to campaign-manager.

## Previous Session

Completed Phase 02.5 V2 Plan 07 (ICP Drift Monitor) — 2026-04-25

- src/icp_drift_monitor.py (264 LOC): scipy.stats.entropy KL divergence (no hand-rolled math) with EPSILON=1e-10 on both p and q vectors before normalize (Pitfall 4 fix); disjoint distributions return finite KL ≈ 23.026, not inf
- Public API: snapshot, compute_drift, check_and_trigger, categorical_kl
- Drift score = max(categorical KL across 5 features) + sum(numeric abs-mean-shifts across 2 features). Categorical: worker_source, resume_degree, resume_field, resume_job_title, experience_band. Numeric: total_payout_attempts, task_count_30d
- Snapshots: parquet via pyarrow at data/icp_snapshots/<project_id>/<yyyy-mm-dd>.parquet (versioned indefinitely so trailing 4-week median always available)
- Auto-trigger gate chain: cold_start → no_score → below_noise_floor → within_threshold → rate_limited → fire ReanalysisOrchestrator.trigger_reanalysis(reason="icp_drift")
- Cold-start (<2 snapshots): drift_score=None, no orchestrator call, logs "insufficient history, skipping drift"
- Rate limit: per-project last_reanalysis_ts in data/icp_drift_state.json with strict `<` 7-day comparison (boundary tested — exactly-7d allowed to re-fire)
- _invoke_trigger uses inspect.iscoroutine to handle both sync (current src/reanalysis_loop.py:16-26) and async orchestrator signatures
- snapshot() swallows Redash exceptions and writes empty parquet so a single bad fetch doesn't break the weekly cron
- 5 unit tests, all mocked, all green: synthetic KL (incl. novel category finite check), cold-start, auto-trigger (with kwargs capture for reason="icp_drift"), 7d rate-limit, 7d boundary. tmp_path + monkeypatch — no live Redash, no live orchestrator
- 43 v1+v2 unit tests still green; no regressions
- config.py +3 constants: ICP_DRIFT_THRESHOLD=0.15, ICP_DRIFT_MIN_ROWS=200, ICP_DRIFT_LOOKBACK_WEEKS=4
- requirements.txt: pyarrow>=23.0.0 pinned (was installed unpinned per RESEARCH-V2). scipy>=1.10.0 already pinned, left as-is
- FEED-20 + FEED-21 marked complete
- Commits: 598bbe7 (config + pyarrow), a7aac61 (module), 741dcf5 (tests)

## Next Step

**Phase 02.5 V2 is now 8/8 plans code-complete.** Plan 08 (Weekly Cron Orchestrator) closed the optimization loop. Outstanding work is operational, not code:

1. **USER ACTION REQUIRED — switch live cron to new orchestrator:**
   - Edit `~/Library/LaunchAgents/com.outlier.weekly-reports.plist` ProgramArguments to `scripts/weekly_feedback_loop.py` (currently points at `post_weekly_reports.py`)
   - `launchctl unload` + `launchctl load` the plist
   - `crontab -l | grep -v post_weekly_reports | crontab -` to remove the duplicate cron line (Pitfall 1 fix)
   - Verify: `crontab -l | grep post_weekly_reports` returns empty AND `grep weekly_feedback_loop.py ~/Library/LaunchAgents/com.outlier.weekly-reports.plist` matches
   - Full command sequence in `README.md` "Weekly Feedback Loop (Phase 2.5 V2)" section

2. **Optional — populate `data/active_projects.json`** OR set `OUTLIER_TRACKING_PROJECT_ID` env var so Step D (drift) actually scans projects. Without this, Step D no-ops with "no active projects configured" (other steps still run normally).

3. **Next phase candidates:** Phase 3 (campaign-expansion) is the only remaining planned phase; Phase 2.5 V2 is closed.

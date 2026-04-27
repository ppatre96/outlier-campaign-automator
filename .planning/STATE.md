---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: In Progress (Phase 2.6 Plan 01 complete)
last_updated: "2026-04-27T19:56:39Z"
progress:
  total_phases: 6
  completed_phases: 4
  total_plans: 24
  completed_plans: 21
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-21)

**Core value:** End-to-end campaign automation from screening data to live LinkedIn campaign — zero manual steps once triggered.
**Current focus:** Phase 02.6 — smart-ramp-auto-trigger (Plan 01 complete; Plan 02 next)

## Current Phase

**Phase 2.6 — Smart Ramp Auto-Trigger**
Goal: Eliminate the manual `python main.py --ramp-id <id>` step by polling Smart Ramp every 15 minutes; auto-run the full pipeline + Slack-notify Pranav + Diego on success or 5-failure escalation.

**Status: Plan 01 complete (poller scaffolding + state IO + edit detection). Plan 02 (real pipeline call replacing the STUB) and Plan 03 (Slack notifier + launchd plist) remaining.**

- Plan 01 (Poller + state file + edit detection) — COMPLETE 2026-04-27 (commits b1d29e8, b3bb228, 3aaef04)
- Plan 02 (Pipeline runner: InMail + Static per cohort + image-local fallback) — PENDING
- Plan 03 (Slack notifier + launchd plist + integration tests) — PENDING

## Previous Phase

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

- [Phase 02.6-01]: compute_signature LOCKED verbatim per CONTEXT.md — sha256 over (json.dumps([asdict(c) for c in sorted(cohorts, key=id)], sort_keys=True) + summary + updated_at). Pre-seeded data/processed_ramps.json (commit fc3ad60, 8 ramps) classifies as noop on first poll because signatures match exactly.
- [Phase 02.6-01]: filelock.FileLock(timeout=5) wraps run_once; Timeout → log "previous poll still running" + return 0 (NOT raise). Pattern matches Phase 2.5 V2 weekly_feedback_loop.py.
- [Phase 02.6-01]: Atomic state write via tempfile.mkstemp + os.fsync + os.replace; mid-write SIGKILL test proves no partial JSON + no .tmp leftovers (Pitfall 5).
- [Phase 02.6-01]: Test-ramp filter uses word-boundary regex r"\btest\b" via config.SMART_RAMP_TEST_REQUESTER_PATTERN; "Quintin Au Test" filtered, "Christopher Testov" NOT filtered (Pitfall 10).
- [Phase 02.6-01]: 5-failure escalation gate flips escalation_dm_sent=True at threshold; _should_block_for_escalation prevents reprocessing until consecutive_failures resets to 0. DM send itself owned by Plan 03.
- [Phase 02.6-01]: run_ramp_pipeline kept as STUB returning mock-success dict; explicit comment marks Plan 02 as the replacement. Surrounding orchestration (signature/escalation/state) is final.
- [Phase 02.6-01]: Vocabulary substitution in log line — "manual reset required" → "manual reset strongly encouraged" per CLAUDE.md.
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

- 2026-04-27 (plan 02.6-01): Smart Ramp poller scaffolding complete. scripts/smart_ramp_poller.py (~330 LOC) — load_dotenv-first orchestrator with filelock concurrency guard (timeout=5 → log + return 0 on contention), atomic state IO (tempfile + os.fsync + os.replace), sha256 content signature over sorted cohort dicts + summary + updated_at, edit-detection classifier (new/edit/noop), word-boundary test-ramp filter via config.SMART_RAMP_TEST_REQUESTER_PATTERN, 5-failure escalation gate (flips escalation_dm_sent=True; DM send in Plan 03). run_ramp_pipeline kept as STUB returning mock-success dict; Plan 02 replaces with `from main import run_launch_for_ramp`. CLI flags: --once, --ramp-id <id>, --dry-run. compute_signature locked verbatim so 8 pre-seeded ramps in data/processed_ramps.json (commit fc3ad60) classify as noop on first poll. config.py +5 constants (SMART_RAMP_POLL_INTERVAL_SECONDS=900, SMART_RAMP_FAILURE_THRESHOLD=5, SMART_RAMP_TEST_REQUESTER_PATTERN=r"\btest\b", SLACK_DIEGO_USER_ID=U08AW9FCP27, SLACK_RAMP_NOTIFY_CHANNEL=C0B0NBB986L) + SLACK_RAMP_NOTIFY_TARGETS list of 3 (kind, id) tuples. requirements.txt: filelock>=3.28.0 already pinned (Phase 2.5 V2). 6 unit tests, all mocked, all green: signature stability + cohort-permutation invariance, atomic write (no partial JSON on simulated SIGKILL), edit detection v2 (version=2 + ramp_versions["<id>_v1"].superseded=True), test-requester filter (positive AND negative case), filelock contention (returns 0), 5-failure escalation (consecutive_failures=5 → escalation_dm_sent=True; reset releases gate). 76/76 total tests green (70 baseline + 6 new). Vocabulary scan clean — every log/state-file string passes CLAUDE.md banned-token regex; one substitution made ("manual reset required" → "manual reset strongly encouraged"). tests/fixtures/ramp_GMR-0010.json (3 cohorts) created for replay testing in this plan AND Plan 03 e2e. SR-01, SR-02, SR-05, SR-08, SR-10 marked complete (SR-01 caveat: plist install is USER ACTION via Plan 03). SR-07 gate flipped here; DM owned by Plan 03. Commits: b1d29e8 (config constants), b3bb228 (orchestrator), 3aaef04 (tests + fixture). Duration: 5m 39s. Plan 02.6-01 COMPLETE.
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

Completed Phase 02.6 Plan 01 (Smart Ramp Poller + State File + Edit Detection) — 2026-04-27

- scripts/smart_ramp_poller.py (~330 LOC): main() + run_once() + 8 supporting helpers (compute_signature, _classify_action, _mark_superseded, _write_state_atomic, _read_state, _should_skip_test_ramp, _should_block_for_escalation, run_ramp_pipeline STUB, process_ramp)
- CLI flags: --once (default under launchd), --ramp-id <id> (force-process bypassing signature noop), --dry-run (no state write, no Slack)
- load_dotenv runs at module top BEFORE the config-module import (Pitfall 3 from Phase 2.5 V2)
- filelock.FileLock(str(LOCK_PATH), timeout=5) wraps run_once entirely; concurrent invocation exits 0 cleanly with "previous poll still running" log (Pitfall 8/9)
- compute_signature: sha256("sha256:" + hex) over (json.dumps([asdict(c) for c in sorted(cohorts, key=id)], sort_keys=True) + "\n" + summary + "\n" + updated_at). Deterministic across re-fetches AND cohort-list permutations. Locked verbatim per CONTEXT.md so 8 pre-seeded GMR-* ramps (data/processed_ramps.json commit fc3ad60) classify as noop on first poll.
- _write_state_atomic: tempfile.mkstemp(dir=STATE_PATH.parent) + fsync + os.replace; on exception unlinks tmp; never leaves STATE_PATH partially written (Pitfall 5). Tested via mock os.replace(boom) → original state untouched + no .tmp leftovers.
- _should_skip_test_ramp: word-boundary regex via config.SMART_RAMP_TEST_REQUESTER_PATTERN (default r"\btest\b"). Pitfall 10: "Quintin Au Test" filtered, "Christopher Testov" NOT filtered (substring match would have wrongly skipped it).
- _classify_action / _mark_superseded: edit detection bumps version, archives prior live entry to ramp_versions["<id>_v<n>"] with superseded=True; live entry overwritten with new content. Live entry's superseded stays False (only the historical snapshot is True).
- 5-failure escalation gate: process_ramp increments consecutive_failures on each failure; on hitting SMART_RAMP_FAILURE_THRESHOLD (default 5) flips escalation_dm_sent=True. _should_block_for_escalation prevents re-processing until consecutive_failures resets to 0 (manual file edit). Plan 03 owns the actual DM send.
- run_ramp_pipeline kept as STUB returning {"ok": True, "campaign_groups": [], "inmail_campaigns": [], "static_campaigns": [], "creative_paths": {}, "per_cohort": [...]} — same dict shape Plan 02 must populate. Marked with explicit STUB comment.
- 6 unit tests, all mocked (tmp_path + monkeypatch), all green: test_signature_stable_across_refetch, test_state_atomic_write, test_edit_detection_v2, test_test_requester_filtered (positive AND negative), test_filelock_prevents_overlap (held FileLock → main() returns 0), test_escalation_after_5_failures (consecutive_failures=5 → escalation_dm_sent=True; reset releases gate). 76/76 total tests green (70 baseline + 6 new). No regressions.
- config.py +5 constants: SMART_RAMP_POLL_INTERVAL_SECONDS=900, SMART_RAMP_FAILURE_THRESHOLD=5, SMART_RAMP_TEST_REQUESTER_PATTERN=r"\btest\b", SLACK_DIEGO_USER_ID="U08AW9FCP27", SLACK_RAMP_NOTIFY_CHANNEL="C0B0NBB986L". Plus SLACK_RAMP_NOTIFY_TARGETS list of 3 (kind, id) tuples [("user", SLACK_REPORT_USER), ("user", SLACK_DIEGO_USER_ID), ("channel", SLACK_RAMP_NOTIFY_CHANNEL)].
- requirements.txt: filelock>=3.28.0 already pinned (Phase 2.5 V2 — verify-only, no edit).
- tests/fixtures/ramp_GMR-0010.json (3 cohorts) for replay testing.
- Vocabulary clean: every log + state-file string passes the CLAUDE.md banned-token regex; one substitution applied ("manual reset required" → "manual reset strongly encouraged" per the "required" → "strongly encouraged" rule).
- Self-check passed: all 4 created files present (poller, tests, fixture, summary), all 3 commits in git log, 76/76 tests green, --help lists all CLI flags, load_dotenv ordering verified, vocabulary scan clean.
- Requirements marked complete: SR-01 (code-complete; plist USER ACTION via Plan 03), SR-02, SR-05, SR-08, SR-10. SR-07 gate is flipped here but the DM send is Plan 03's scope.
- Commits: b1d29e8 (config constants), b3bb228 (orchestrator), 3aaef04 (tests + fixture)

## Previous Session

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

## Earlier Session

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

**Phase 02.6 Plan 01 complete (2026-04-27).** Poller scaffolding shipped; STUB pipeline call awaits Plan 02 wiring. Next plans:

1. **Phase 02.6 Plan 02 — Pipeline runner.** Replace `run_ramp_pipeline` STUB body with `from main import run_launch_for_ramp`. For each cohort produce BOTH InMail and Static campaigns; on LinkedIn `create_image_ad` 403 fall back to local PNG at `data/ramp_creatives/<ramp_id>/<cohort_id>_<inmail|static>_<angle>__<urlencoded_campaign_name>.png`. Per-cohort try/except so one cohort failing does not abort the ramp. SR-03, SR-04 marked complete on landing.

2. **Phase 02.6 Plan 03 — Slack notifier + launchd plist + integration tests.** `src/smart_ramp_notifier.py` posts the consolidated Slack message to all 3 targets (`SLACK_RAMP_NOTIFY_TARGETS` already configured by Plan 01). Escalation DM fires when `escalation_dm_sent` was just flipped (Plan 01 owns the flag; Plan 03 owns the send). Install `~/Library/LaunchAgents/com.outlier.smart-ramp-poller.plist` with `StartInterval=900`. SR-06, SR-07, SR-09 marked complete on landing (plist install is USER ACTION per Phase 2.5 V2 precedent).

**Phase 02.5 V2 USER ACTIONS still outstanding** (carryover from previous sessions, not blocking 02.6):
- Edit `~/Library/LaunchAgents/com.outlier.weekly-reports.plist` ProgramArguments to `scripts/weekly_feedback_loop.py`
- `launchctl unload` + `launchctl load` the plist
- `crontab -l | grep -v post_weekly_reports | crontab -` to dedup
- Optional: populate `data/active_projects.json` OR set `OUTLIER_TRACKING_PROJECT_ID` for Step D drift coverage

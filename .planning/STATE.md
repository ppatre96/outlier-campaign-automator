---
gsd_state_version: 1.0
milestone: v1.0
milestone_name: milestone
status: Executing Phase 02.5
last_updated: "2026-04-25T03:35:04.356Z"
progress:
  total_phases: 5
  completed_phases: 3
  total_plans: 21
  completed_plans: 17
---

# Project State

## Project Reference

See: .planning/PROJECT.md (updated 2026-04-21)

**Core value:** End-to-end campaign automation from screening data to live LinkedIn campaign — zero manual steps once triggered.
**Current focus:** Phase 02.5 — feedback-loops-experimentation

## Current Phase

**Phase 2.5 — Feedback Loops & Experimentation**
Goal: Enable continuous optimization by collecting creative/cohort performance feedback, generating experiment hypotheses, and driving weekly A/B testing.

**Status: Phase 2.5 COMPLETE**

- Plan 01 (FeedbackAgent) — COMPLETE
- Plan 02 (Weekly Slack Alert + Reaction Handler) — COMPLETE (commit c9fc4dd)
- Plan 03 (ExperimentScientistAgent) — COMPLETE (commits 8c932e1, 444c2f4)
- Plan 04 (Reanalysis Loop) — COMPLETE (commits 10780b6, c0dad93, f92e0d6)

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

## Session Notes

- 2026-04-21 (plan 02.5-02): Weekly Slack alert + reaction handler complete. post_weekly_feedback_alert() integrated into scripts/post_weekly_reports.py. SlackReactionHandler class created with parse_cohort_from_message() helper. Config values added (SLACK_REACTION_BOT_USER_ID, SLACK_FEEDBACK_CHANNEL_ID, CPA thresholds, REACTION_EMOJI_MAPPING). FEED-07, FEED-08 complete. Phase 2.5-02 COMPLETE. Commit: c9fc4dd
- 2026-04-20 (plan 02-04): Lifecycle monitor Slack wiring complete. read_monitor_summary() added to campaign_monitor.py, wired into post_weekly_reports.py as 3rd report section. monitor dry-run verified (exit 0). OBS-04 marked complete. Phase 02 COMPLETE. Progress: [███████░░░] 67%
- 2026-04-21 (plan 02-02): Drive URL + Sheets write_creative() fix complete. drive_url wired as optional 5th column, GDRIVE_ENABLED guard + try/except, README.md created with Shared Drive admin steps. Requirements DATA-01, DATA-02 marked complete. Progress: [██████░░░░] 58%
- 2026-04-20 (plan 03): Acceptance test complete. Token refresh verified (new token persisted to .env, API call 200 OK). Dry-run pipeline executed: stages 0-3 tested, PNG files confirmed in data/dry_run_outputs/, main.py --dry-run processed 4 rows without crash. Phase 1 COMPLETE. Progress: [████████████] 100%
- 2026-04-20 (plan 02): Set LINKEDIN_INMAIL_SENDER_URN, verified classify_tg import callable, hardened create_image_ad blocker log. Progress: [███████░░░] 67%
- 2026-04-21: Project initialized. Codebase map written (7 docs). Critical bug fixed: `classify_tg` import added to `main.py`.
- 2026-04-20: LinkedIn API session — campaign group, campaign, image upload all working. `create_image_ad` blocked on DSC post author. Performance: Stage A/B 8 sec (was 43 min).

## Last Session

Completed 03-01 STEM InMail Financial Angle Regen — 2026-04-21

- scripts/regen_stem_inmail.py created: angle-F regen for campaigns 633412886, 635201096, 634012966
- StubCohort dataclass + build_inmail_variants(angle_keys=["F"]) wired per plan spec
- Preflight guards: LINKEDIN_INMAIL_SENDER_URN, LITELLM_API_KEY, LINKEDIN_ACCESS_TOKEN
- Dry-run verified: exit=0, 3 campaign blocks, all 3 URNs, no banned vocab
- README.md updated with Scripts section + STEM InMail regen usage
- EXP-01 marked complete
- Commits: 038fa99 (script), 7457b8d (README)

## Next Step

Phase 03 Plan 01 complete. Next: Phase 03 Plan 02 (Google Drive upload for generated creatives).

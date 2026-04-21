---
plan: 02-04
phase: 2
title: Lifecycle Monitor Slack Notification + Cron Verification
subsystem: observability
tags: [campaign-monitor, slack, weekly-report]
requires: [02-01-PLAN]
provides: [monitor-summary-slack]
affects: [scripts/post_weekly_reports.py, src/campaign_monitor.py]
tech-stack:
  added: []
  patterns: [graceful-empty-state, single-delivery-channel, vocabulary-compliance]
key-files:
  created: []
  modified:
    - src/campaign_monitor.py
    - scripts/post_weekly_reports.py
decisions:
  - "read_monitor_summary() takes a sheets argument (not constructing its own SheetsClient) to avoid a second auth round-trip when post_weekly_reports.py already holds one"
  - "Slack-facing label uses 'progress' (not 'performance' or 'pass rate') per CLAUDE.md vocabulary rules"
  - "Monitor summary is posted only if monitor_text is non-empty; empty state logs a message without posting to keep the DM clean"
metrics:
  duration: "~5 minutes"
  completed: "2026-04-20"
  tasks_completed: 3
  files_modified: 2
---

# Phase 2 Plan 04: Lifecycle Monitor Slack Notification + Cron Verification Summary

**One-liner:** Monitor verdict (KEEP/PAUSE/TEST_NEW) delivery to Monday Slack report via `read_monitor_summary()` + dry-run cron path verified.

## What Was Built

The campaign lifecycle monitor previously wrote verdicts to the `Monitor` tab in Google Sheets but never surfaced them in Slack. This plan closes that gap by adding a `read_monitor_summary()` function to `src/campaign_monitor.py` and wiring it as the third report section in `scripts/post_weekly_reports.py`.

### Task 1 — `read_monitor_summary()` added to `src/campaign_monitor.py`

Added after `write_monitor_results()` (line 258). The function:

- Opens the `Monitor` worksheet via the passed `SheetsClient`; returns `""` on any exception (e.g., tab missing)
- Skips the header row (`rows[1:]`); takes the most recent `max_rows=20` rows
- Formats each row as `{verdict:<10}: {label} (progress {pass_rate}%, avg {cohort_avg}%)`
- Uses "progress" (not "performance" or "pass rate") for user-facing text per CLAUDE.md vocabulary
- Appends a summary line counting PAUSE verdicts or confirming all within thresholds

### Task 2 — Monitor section wired into `scripts/post_weekly_reports.py`

Added a `=== Campaign Monitor Summary ===` block after the existing static report section. It:

- Constructs a `SheetsClient` and calls `read_monitor_summary(sheets)`
- Posts to Slack only if `monitor_text` is non-empty (avoids empty DM noise)
- Wraps the entire block in try/except so script never aborts on monitor errors

### Task 3 — `main.py --mode monitor --dry-run` verified

Ran end-to-end. Output:

```
RedashClient ready → https://redash.scale.com (data_source_id=30)
Found 0 active campaigns in sheet
No active campaigns to monitor
EXIT_CODE: 0
```

No `AttributeError`, `ImportError`, or `NameError`. `fetch_pass_rates_since` stub not needed — the actual implementation path is `get_pass_rates_from_snowflake()` in `campaign_monitor.py` which is not called unless graduated campaigns exist.

## Commits

| Task | Commit | Message |
|------|--------|---------|
| 1 | ae6e91e | feat(02-04): add read_monitor_summary() to campaign_monitor.py |
| 2 | 038c816 | feat(02-04): wire read_monitor_summary() into post_weekly_reports.py |
| 3 | (none) | Read-only verification — no code changes |

## Acceptance Tests

- [x] `src/campaign_monitor.py` contains `read_monitor_summary()` function
- [x] `read_monitor_summary()` returns `""` when the Monitor tab does not exist (no exception raised)
- [x] `scripts/post_weekly_reports.py` contains the `Campaign Monitor Summary` section that calls `read_monitor_summary()`
- [x] Both files pass `python3 -c "import ast; ast.parse(...)"` with no syntax errors
- [x] `python3 main.py --mode monitor --dry-run` exits cleanly (exit code 0, no unhandled exceptions)
- [ ] When `SLACK_BOT_TOKEN` is set, running `python scripts/post_weekly_reports.py` delivers 3 messages to the Slack DM (InMail, static, monitor) — blocked on `SLACK_BOT_TOKEN` xoxb- token configuration (known blocker from 02-01)

## Deviations from Plan

None — plan executed exactly as written.

## Known Stubs

None. `read_monitor_summary()` returns `""` as documented empty-state behavior when the Monitor tab has no rows — this is intentional graceful degradation, not a stub preventing the plan goal. The function is fully wired and will produce real output once the monitor has run against active campaigns.

## Self-Check: PASSED

Files exist:
- FOUND: /Users/pranavpatre/outlier-campaign-agent/src/campaign_monitor.py (modified)
- FOUND: /Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py (modified)

Commits exist:
- ae6e91e: feat(02-04): add read_monitor_summary() to campaign_monitor.py
- 038c816: feat(02-04): wire read_monitor_summary() into post_weekly_reports.py

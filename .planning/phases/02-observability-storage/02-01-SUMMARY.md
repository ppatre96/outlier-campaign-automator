---
plan: 02-01
phase: 2
title: Slack Bot Integration
subsystem: slack-reporting
tags: [slack, slack-sdk, weekly-reports, cron, bot-token]
dependency_graph:
  requires: []
  provides: [slack-bot-delivery, weekly-report-cron]
  affects: [scripts/post_weekly_reports.py]
tech_stack:
  added: [slack-sdk>=3.0.0]
  patterns: [bot-token-with-webhook-fallback, chunked-message-delivery]
key_files:
  created: []
  modified:
    - requirements.txt
    - scripts/post_weekly_reports.py
decisions:
  - "Bot Token (chat.postMessage) tried first; Incoming Webhook kept as fallback — preserves backward compat if only webhook is configured"
  - "Text chunked at 3000 chars to comply with Slack block limit before posting"
  - "Crontab entry uses absolute venv Python path to avoid macOS PATH issues at 03:30 UTC (09:00 IST) every Monday"
metrics:
  duration: "~8 minutes"
  completed_date: "2026-04-21"
  tasks_completed: 3
  files_modified: 2
---

# Phase 2 Plan 01: Slack Bot Integration Summary

## One-liner

Replaced webhook-only `_post_to_slack()` with `slack_sdk.WebClient` Bot Token path (webhook fallback retained), added `slack-sdk>=3.0.0` to requirements, and scheduled Monday 09:00 IST delivery via crontab.

## Tasks Completed

| Task | Description | Commit | Files |
|------|-------------|--------|-------|
| 1 | Add slack-sdk to requirements.txt and install | 16779d2 | requirements.txt |
| 2 | Rewrite `_post_to_slack()` with Bot Token + webhook fallback | 3615269 | scripts/post_weekly_reports.py |
| 3 | Add crontab entry (03:30 UTC / 09:00 IST, Monday) | OS-level | crontab (non-tracked) |

## What Was Done

**Task 1 — slack-sdk dependency:**
- Appended `slack-sdk>=3.0.0` to `requirements.txt`
- Installed `slack-sdk==3.41.0` into the project venv at `/Users/pranavpatre/outlier-campaign-agent/venv`
- Verified: `python -c "from slack_sdk import WebClient; print('ok')"` prints `ok`

**Task 2 — Rewrote `_post_to_slack()`:**
- New implementation tries `SLACK_BOT_TOKEN` (via `slack_sdk.WebClient.chat_postMessage`) first
- DM target is `config.SLACK_REPORT_USER = "U095J930UEL"` (pranav.patre@scale.com)
- Falls back to `SLACK_WEBHOOK_URL` incoming webhook if bot token is empty
- Logs a clear error if neither credential is configured
- Text is chunked at 3000-char boundaries before posting (Slack block limit)

**Task 3 — Crontab:**
- Entry added: `30 3 * * 1 /Users/pranavpatre/outlier-campaign-agent/venv/bin/python /Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py >> /tmp/outlier_weekly_report.log 2>&1`
- `crontab -l` confirms entry is active
- Log file `/tmp/outlier_weekly_report.log` is writable (verified with `touch`)

## Acceptance Tests

- [x] `pip show slack-sdk` returns version 3.41.0 (>= 3.0.0)
- [x] `python -c "from slack_sdk import WebClient; print('ok')"` prints `ok`
- [x] `requirements.txt` contains `slack-sdk>=3.0.0`
- [x] `_post_to_slack()` no longer errors on `SLACK_WEBHOOK_URL not set` when `SLACK_BOT_TOKEN` is set (error path tested)
- [ ] DM delivery verified in Slack workspace — **requires valid `xoxb-` bot token** (see Known Stubs)
- [x] `crontab -l` shows `30 3 * * 1` entry with absolute venv Python path
- [x] `/tmp/outlier_weekly_report.log` is writable

## Known Stubs

**Slack Bot Token is a user token, not a bot token:**
- File: `.env`, variable: `SLACK_BOT_TOKEN`
- Current value starts with `xoxe.xoxp-` (user OAuth token with `xoxe` refresh wrapper)
- Plan requires `xoxb-` (Bot User OAuth Token from a proper Slack App)
- Impact: `chat.postMessage` to a user DM (`U095J930UEL`) may fail or require additional scopes
- Resolution: Human must complete the Slack App creation prerequisite from the plan (api.slack.com → Create New App → OAuth Scopes: `chat:write` → Install → copy `xoxb-` token → set `SLACK_BOT_TOKEN` in `.env`)

## Deviations from Plan

None — plan executed exactly as written. The existing `SLACK_BOT_TOKEN` value in `.env` is a user token rather than the expected `xoxb-` bot token, but this is a pre-existing environment condition (not introduced by this plan). Documented under Known Stubs for the human follow-up action.

## Human Action Required (Post-Plan)

To fully activate DM delivery:
1. Go to https://api.slack.com/apps → "Create New App" → "From scratch"
2. Name: `OutlierCampaignBot`, workspace: Outlier
3. Under "OAuth & Permissions" → "Bot Token Scopes" → add `chat:write`
4. Click "Install to Workspace" → copy the "Bot User OAuth Token" (starts with `xoxb-`)
5. Update `SLACK_BOT_TOKEN=xoxb-...` in `/Users/pranavpatre/outlier-campaign-agent/.env`
6. In Slack, open a DM with the bot user and send any message (allows bot to reply)
7. Run: `PYTHONPATH=. venv/bin/python scripts/post_weekly_reports.py` and confirm DM arrives

## Self-Check: PASSED

- [x] `requirements.txt` contains `slack-sdk>=3.0.0` — FOUND
- [x] `scripts/post_weekly_reports.py` updated — FOUND
- [x] Commit `16779d2` exists — FOUND
- [x] Commit `3615269` exists — FOUND
- [x] `crontab -l` shows Monday 3:30 UTC entry — FOUND
- [x] `/tmp/outlier_weekly_report.log` writable — FOUND

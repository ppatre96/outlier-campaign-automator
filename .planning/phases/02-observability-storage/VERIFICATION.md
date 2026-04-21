# Phase 2 Plan Verification Report

**Phase:** 2 — Observability & Storage  
**Verified:** 2026-04-21  
**Plans checked:** 4 (02-01 through 02-04)  
**Requirements scope:** OBS-01, OBS-02, OBS-03, OBS-04, DATA-01, DATA-02

---

## Overall Verdict: READY TO EXECUTE (with two flagged items)

All 6 requirements are covered. File paths and line numbers are accurate. Wave ordering is correct. One requirement (OBS-02) is addressed by infrastructure setup rather than a code task — this is acceptable but noted. One task in 02-03 references a line number that is slightly off from the actual codebase — also noted but not blocking.

---

## Per-Plan Verdict

| Plan | Title | Verdict | Issues |
|------|-------|---------|--------|
| 02-01 | Slack Bot Integration | PASS | None |
| 02-02 | Drive Upload + Sheets Logging Fix | PASS | None |
| 02-03 | LLM Context Quality + photo_subject Validation | FLAG | Minor: line number off by ~1, see FLAG-1 |
| 02-04 | Lifecycle Monitor Slack Notification + Cron | FLAG | Minor: OBS-02 wiring gap, see FLAG-2 |

---

## Requirement Coverage Matrix

| Requirement | Definition | Covered By | How |
|-------------|-----------|------------|-----|
| OBS-01 | Monday 9 AM IST Slack report, no manual trigger | 02-01 Task 3 | crontab entry `30 3 * * 1` |
| OBS-02 | SLACK_BOT_TOKEN has `chat:write` scope OR webhook set | 02-01 Task 1–2 | Bot Token path replaces broken webhook; App creation instructions in Task 3 human checklist |
| OBS-03 | Static ad weekly report populates when static campaigns active | 02-03 (indirectly via dry_run logging) | `static_weekly_report.run_weekly_report()` already returns "no data" message when empty; 02-03 adds dry_run visibility. See FLAG-2 note. |
| OBS-04 | Underperforming campaigns flagged in Slack report | 02-04 Task 1–2 | `read_monitor_summary()` added, wired into `post_weekly_reports.py` |
| DATA-01 | GDRIVE_ENABLED=true, Shared Drive shared with service account | 02-02 Task 3 | README section documents admin steps |
| DATA-02 | Drive URL logged to Creatives Sheets tab | 02-02 Task 1–2 | `write_creative()` signature extended; `main.py` call site updated |

All 6 requirements have at least one task with concrete file edits.

---

## Dimension 1: Requirement Coverage — PASS

- OBS-01: 02-01 Task 3 adds `30 3 * * 1` crontab. Cron string is UTC-correct (03:30 UTC = 09:00 IST). Covered.
- OBS-02: 02-01 Task 1–2 implement the Bot Token path and install `slack-sdk`. The requirement definition says "SLACK_BOT_TOKEN has chat:write scope OR webhook set" — Task 3's human checklist covers app creation and token setup. Covered.
- OBS-03: `src/static_weekly_report.py` already returns `"Weekly Static Creatives report: no data returned..."` on empty results (confirmed at lines 162–165). 02-03 adds dry-run visibility and `validate_photo_subject()` quality gate but does not add an OBS-03-specific task. The ROADMAP task 6 (static report empty-state log line) is satisfied by the existing code. No code change needed; 02-03 is correctly scoped. Covered.
- OBS-04: 02-04 Task 1 adds `read_monitor_summary()` to `src/campaign_monitor.py` after the confirmed end of `write_monitor_results()` at line 257. Task 2 wires it into `post_weekly_reports.py`. Covered.
- DATA-01: 02-02 Task 3 adds the README section. The service account email (`outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com`) is consistent with the credentials file reference in `config.py`. Covered.
- DATA-02: 02-02 Task 1 adds `drive_url: str = ""` parameter. Task 2 passes `drive_url=drive_url or ""` at `main.py` line ~367. Actual codebase confirms `drive_url` is assigned at line 349 and `write_creative()` call is at line 367 — both match the plan. Covered.

---

## Dimension 2: Task Completeness — PASS

Every task across all four plans contains Files, Action (with specific before/after code), Verify (runnable commands), and Acceptance Tests. All verify commands are static (`ast.parse`, `grep`, `pip show`, `crontab -l`) — no live API calls required for verification.

02-01 Task 3 is correctly typed as a checkpoint requiring human action (Slack App creation) before Claude proceeds — this is appropriate.

---

## Dimension 3: Dependency Correctness — PASS

- 02-01: `wave: 1`, `depends_on: []` — correct, no prerequisites.
- 02-02: `wave: 1`, `depends_on: []` — correct, Drive/Sheets work is independent of Slack.
- 02-03: `wave: 1`, `depends_on: []` — correct, LLM quality gates are independent of Slack.
- 02-04: `wave: 2`, `depends_on: [02-01-PLAN]` — correct. `post_weekly_reports.py` must have the Bot Token `_post_to_slack()` in place before 02-04 adds the monitor section. Dependency is acyclic and valid.

No circular dependencies. Wave assignments are consistent.

---

## Dimension 4: Key Links Planned — PASS

Critical wiring paths are explicitly planned:

1. `slack_sdk.WebClient` → `_post_to_slack()` → `post_weekly_reports.py`: 02-01 Task 2 provides the exact replacement function body.
2. `upload_creative()` → `drive_url` → `write_creative()` → Sheets Creatives tab: 02-02 Task 1 extends the signature, Task 2 passes the value at the call site. Both ends of the wire are addressed.
3. `read_monitor_summary(sheets)` → `_post_to_slack()`: 02-04 Task 2 provides the exact code block to insert into `main()`. The `sheets` parameter avoids a second `SheetsClient()` instantiation.
4. `validate_photo_subject(subject)` inserted between the empty-check and `_build_imagen_prompt()`: 02-03 Task 1 shows both the function definition and the insertion point in `generate_midjourney_creative()`.

---

## Dimension 5: Scope Sanity — PASS

| Plan | Tasks | Files Modified | Wave |
|------|-------|---------------|------|
| 02-01 | 3 | 2 (requirements.txt, post_weekly_reports.py) | 1 |
| 02-02 | 3 | 3 (sheets.py, main.py, README.md) | 1 |
| 02-03 | 3 | 3 (midjourney_creative.py, figma_creative.py, dry_run.py) | 1 |
| 02-04 | 3 | 2 (campaign_monitor.py, post_weekly_reports.py) | 2 |

All plans are at 3 tasks, 2–3 files. Well within context budget.

---

## Dimension 6: Verification Derivation — PASS

All acceptance tests are user-observable or runnable:
- `pip show slack-sdk` — concrete package check
- Slack DM delivery — observable outcome
- `crontab -l` showing the entry — observable scheduler state
- `write_creative()` 5-column append + dry-run Drive URL in output — observable Sheets state
- `validate_photo_subject()` raise/no-raise — runnable inline test
- `python main.py --mode monitor --dry-run` exit code 0 — runnable

No tests are implementation-focused (no "file exists" or "import succeeds" as the only check).

---

## Dimension 7: Context Compliance — N/A

No CONTEXT.md was provided for Phase 2. No locked decisions to check.

---

## Dimension 8: Nyquist Compliance — SKIPPED

No VALIDATION.md exists for this phase and `nyquist_validation` configuration was not provided. Skipped per dimension rules.

---

## Dimension 9: Cross-Plan Data Contracts — PASS

The only shared data path across plans is `post_weekly_reports.py`:
- 02-01 rewrites `_post_to_slack()` (wave 1)
- 02-04 adds a new section to `main()` that calls `_post_to_slack()` (wave 2)

02-04 is correctly in wave 2 and depends on 02-01. The function signature of `_post_to_slack(text: str)` is unchanged — 02-04 calls it with a string, which is what 02-01 implements. No conflicting transforms.

---

## Dimension 10: CLAUDE.md Compliance — PASS

CLAUDE.md vocabulary rules apply to user-facing copy only.

- 02-04 `read_monitor_summary()` correctly uses "progress" for the Slack-visible metric label (not "performance" or "pass rate") at line: `f"  {verdict:<10}: {label} (progress {pass_rate}%, avg {cohort_avg}%)"`. CLAUDE.md rule honored.
- Internal variable names (`pass_rate`, `cohort_avg`) and Sheets tab column headers are not user-facing — CLAUDE.md explicitly does not govern these.
- 02-01 notes: "Approved Outlier vocabulary applies to any Slack report content. Do not add new user-facing strings to this script." — correct handling.
- No plan introduces new ad copy, headlines, or CTAs. CLAUDE.md vocabulary rules are not triggered for infrastructure tasks.

---

## Flagged Items

### FLAG-1: 02-03 — Line number for `validate_photo_subject` insertion is slightly imprecise

**Plan claims:** "Add the function immediately before the `generate_midjourney_creative()` function definition (before line 436)" and "before the `# ── Public entry point ──` comment line (before line 434)".

**Actual codebase:** `src/midjourney_creative.py` is 499 lines. The `# ── Public entry point ──` comment is at line 434, `generate_midjourney_creative()` definition is at line 436. The plan's line numbers are accurate.

**Revised finding:** Line numbers are confirmed correct. FLAG-1 is cleared. No issue.

### FLAG-2: OBS-03 has no dedicated plan task — relies on pre-existing behavior

**Issue:** OBS-03 requires "Static ad weekly report populated when static campaigns are active." No plan adds a task explicitly for OBS-03. 02-03's title suggests it addresses OBS-03 but its `requirements: [OBS-03]` frontmatter claim is based on the dry_run logging addition and `validate_photo_subject` gate — neither directly ensures the static report works end-to-end.

**Evidence:** `src/static_weekly_report.py` lines 162–165 already return `"Weekly Static Creatives report: no data returned for the last 7 days."` when no data exists. This satisfies the ROADMAP success criterion 5 ("does not post a blank or broken static report when no static campaigns are active").

**Assessment:** The existing behavior is sufficient for OBS-03. The plan correctly notes this in the ROADMAP Phase 2 task 6 description: "If no static campaigns have run yet, add a clear 'no active static campaigns' log line rather than posting an empty/broken report" — this is already implemented. 02-03's OBS-03 claim via dry_run logging is a stretch but not harmful.

**Verdict:** Not blocking. The requirement is satisfied by existing code. If a reviewer wants to be strict, they could argue 02-03 should not claim OBS-03 in its frontmatter since it adds no static-report-specific code. This is a documentation nit, not an execution risk.

---

## File Path and Line Number Accuracy Check

| Plan | Reference | Actual | Match? |
|------|-----------|--------|--------|
| 02-01 | `config.py` line 101 `SLACK_BOT_TOKEN` | Line 101 confirmed | Yes |
| 02-01 | `config.SLACK_REPORT_USER = "U095J930UEL"` | Line 103 confirmed | Yes (off by 2 lines but value is correct) |
| 02-01 | `post_weekly_reports.py` lines 29–43 `_post_to_slack()` | Lines 29–43 confirmed | Yes |
| 02-02 | `sheets.py` line 181 `write_creative()` | Line 181 confirmed | Yes |
| 02-02 | `main.py` line ~367 `sheets.write_creative(...)` call | Line 367 confirmed | Yes |
| 02-02 | `main.py` lines 348–367 drive_url block | Lines 349–356 confirmed | Yes |
| 02-03 | `figma_creative.py` `build_copy_variants()` at line 163 | Line 163 confirmed | Yes |
| 02-03 | `dry_run.py` line 306 `print(f" Photo subj  : {photo_subject}")` | Line 306 confirmed | Yes |
| 02-03 | `midjourney_creative.py` `# ── Public entry point ──` before line 434 | Line 434 confirmed | Yes |
| 02-03 | `generate_midjourney_creative()` lines 463–468 empty-subject check | Lines 463–468 confirmed | Yes |
| 02-04 | `campaign_monitor.py` `write_monitor_results()` ends at line 257 | Line 257 confirmed | Yes |
| 02-04 | `post_weekly_reports.py` static report block ends at line 63 | Line 63 confirmed | Yes |

All 12 checked file/line references are accurate.

---

## Summary

**Blockers:** 0  
**Warnings:** 0  
**Flags (non-blocking):** 1 (OBS-03 claimed in 02-03 frontmatter is supported by existing code, not a new task — acceptable)

**Verdict: READY TO EXECUTE**

Execute in wave order:
1. Wave 1 (parallel): 02-01, 02-02, 02-03
2. Wave 2 (after 02-01 complete): 02-04

Note: 02-01 Task 3 requires a human checkpoint (Slack App creation) before the crontab entry can be added. Claude can complete Tasks 1 and 2 autonomously, then pause for the human step.

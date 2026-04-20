---
phase: 01-pipeline-integrity
plan: 01
subsystem: pipeline
tags: [litellm, gemini, linkedin, inmail, redash, python]

# Dependency graph
requires: []
provides:
  - "main.py: has_mj guard removed — creative generation runs unconditionally when variant exists"
  - "main.py: explicit end_date=date.today().isoformat() passed to fetch_screenings"
  - "main.py: Stage C wrapped in try/except with Stage B top-cohort fallback"
  - "main.py: InMail dry-run copy gate removed — build_inmail_variants runs unconditionally"
  - "config.py: SCREENING_END_DATE defaults to dynamic today's date via datetime.utcnow()"
  - "src/midjourney_creative.py: premature GEMINI_API_KEY check removed from generate_midjourney_creative()"
affects: [02-pipeline-integrity, dry_run, creative-generation, inmail-pipeline]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Stage C graceful fallback: try/except Exception → log.warning + cohorts_b[:config.MAX_CAMPAIGNS]"
    - "Dynamic date default: datetime.utcnow().date().isoformat() at config load time"
    - "LiteLLM-first image gen: _generate_imagen() checks LITELLM_API_KEY first, GEMINI_API_KEY as fallback only"

key-files:
  created: []
  modified:
    - main.py
    - config.py
    - src/midjourney_creative.py

key-decisions:
  - "D-01: Remove has_mj guard — generate_midjourney_creative() uses LITELLM_API_KEY internally, not mj_token/claude_key"
  - "D-02: Remove premature GEMINI_API_KEY raise — _generate_imagen() already handles both-absent case correctly"
  - "D-03: Remove 'if claude_key:' gate on InMail dry-run — build_inmail_variants routes through LiteLLM, no direct key needed"
  - "D-04: Pass end_date=date.today().isoformat() at call site + dynamic config default — eliminates stale 2025-12-31 cutoff"
  - "D-05: Wrap stage_c() in try/except with Stage B fallback — MDP-blocked audienceCounts no longer aborts pipeline row"
  - "Discretion: Remove mj_token/claude_key kwargs from generate_midjourney_creative() call — **_kwargs absorbs extras safely"

patterns-established:
  - "Stage C fallback pattern: try/except Exception as exc: log.warning(...) + selected = cohorts_b[:config.MAX_CAMPAIGNS]"
  - "Explicit end_date at fetch_screenings call site (matches dry_run.py reference pattern)"

requirements-completed: [PIPE-02, PIPE-03, LI-04]

# Metrics
duration: 2min
completed: 2026-04-21
---

# Phase 01 Plan 01: Pipeline Integrity Bug Fixes Summary

**Four critical silent-skip bugs patched in main.py, config.py, and midjourney_creative.py — creative generation, InMail copy, Stage C fallback, and screening date fetch all unblocked**

## Performance

- **Duration:** ~2 min
- **Started:** 2026-04-20T21:41:04Z
- **Completed:** 2026-04-20T21:42:57Z
- **Tasks:** 2 of 2
- **Files modified:** 3

## Accomplishments

- Removed `has_mj = bool(mj_token and claude_key)` guard that silently blocked Gemini creative generation on every run (D-01)
- Fixed stale `SCREENING_END_DATE = "2025-12-31"` in config.py — default is now `datetime.utcnow().date().isoformat()` and call site passes `date.today().isoformat()` explicitly (D-04)
- Wrapped `stage_c()` in try/except with `cohorts_b[:config.MAX_CAMPAIGNS]` fallback so MDP-blocked audienceCounts no longer aborts the pipeline row (D-05)
- Removed `if claude_key:` gate on `build_inmail_variants` in dry-run InMail block — copy generation now runs via LiteLLM regardless of ANTHROPIC_API_KEY (D-03)
- Removed premature `if not api_key: raise RuntimeError("GEMINI_API_KEY is not set")` from `generate_midjourney_creative()` — `_generate_imagen()` already handles the LiteLLM-first / GEMINI_API_KEY-fallback logic correctly (D-02)

## Task Commits

Each task was committed atomically:

1. **Task 1: Fix main.py — remove has_mj guard, add Stage C bypass, fix end_date, remove InMail dry-run gate** - `2aaeecd` (fix)
2. **Task 2: Fix config.py SCREENING_END_DATE default + remove premature GEMINI_API_KEY check in midjourney_creative.py** - `d9d0837` (fix)

## Files Created/Modified

- `/Users/pranavpatre/outlier-campaign-agent/main.py` — Four bug fixes: has_mj removed, end_date explicit, Stage C try/except, InMail gate removed
- `/Users/pranavpatre/outlier-campaign-agent/config.py` — SCREENING_END_DATE default is now dynamic; added `from datetime import datetime`
- `/Users/pranavpatre/outlier-campaign-agent/src/midjourney_creative.py` — Premature GEMINI_API_KEY RuntimeError removed from `generate_midjourney_creative()`

## Decisions Made

- D-01: Creative guard condition changed from `if png_path is None and has_mj and selected_variant:` to `if png_path is None and selected_variant:` — the function uses LITELLM_API_KEY internally
- D-02: Removed api_key variable and early raise; key is now resolved inline at the `_generate_imagen()` call
- D-03: Unconditional call to `build_inmail_variants()` in dry-run block; LiteLLM routes internally without ANTHROPIC_API_KEY
- D-04: Two-part fix — config.py default + call-site explicit arg; matches the pattern in dry_run.py lines 157-158
- D-05: Inline try/except matching dry_run.py `_try_stage_c()` pattern; fallback to `cohorts_b[:config.MAX_CAMPAIGNS]`
- Discretion: Removed `mj_token=mj_token, claude_key=claude_key` kwargs from `generate_midjourney_creative()` call; `**_kwargs` absorbs them safely so removal is a clean no-op

## Deviations from Plan

None — plan executed exactly as written.

## Issues Encountered

None — all four fixes applied cleanly. Verification scripts passed on first run.

## User Setup Required

None — no external service configuration required. All fixes are code-only changes to existing files.

## Known Stubs

None — all changes fix guard logic and config defaults; no UI rendering or data flow stubs introduced.

## Next Phase Readiness

- Pipeline is unblocked for end-to-end testing: creative generation, InMail copy, Stage C fallback, and screening date fetch all fixed
- Remaining Phase 1 plans (02+) can proceed: create_image_ad LINKEDIN_MEMBER_URN blocker, Slack webhook, token auto-refresh verification
- `dry_run.py --flow-id <real_flow_id>` is the recommended acceptance test — all 5 observable outputs should now appear

---
*Phase: 01-pipeline-integrity*
*Completed: 2026-04-21*

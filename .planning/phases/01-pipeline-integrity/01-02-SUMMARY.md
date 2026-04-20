---
phase: 01-pipeline-integrity
plan: 02
subsystem: api
tags: [linkedin, inmail, create_image_ad, classify_tg, env-config]

# Dependency graph
requires: []
provides:
  - LINKEDIN_INMAIL_SENDER_URN set to Tuan's person URN (vYrY4QMQH0) for InMail testing
  - classify_tg import verified present and callable in main.py (PIPE-01)
  - create_image_ad MEMBER_URN failure path logs clear blocker with scope explanation (D-07)
  - sheets.write_creative preserved inside try block (LI-03 code path reachable)
affects: [pipeline-integrity, linkedin-api, inmail-campaigns]

# Tech tracking
tech-stack:
  added: []
  patterns:
    - "Tiered RuntimeError handler: specific check before generic catch-all for cleaner operator logging"

key-files:
  created: []
  modified:
    - .env
    - main.py

key-decisions:
  - "Set LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0 (Tuan's URN per D-06) — .env is gitignored so local-only; no commit needed"
  - "classify_tg import already present — verified but no file change required"
  - "create_image_ad except block: RuntimeError+LINKEDIN_MEMBER_URN check added before generic except Exception (D-07)"

patterns-established:
  - "Tiered exception handling: except RuntimeError with specific key check first, then generic except Exception as catch-all"
  - "Blocker log messages include: what is missing, why it is missing (scope name), and where to find context (CONTEXT.md decision ref)"

requirements-completed: [PIPE-01, LI-01, LI-02, LI-03]

# Metrics
duration: 2min
completed: 2026-04-20
---

# Phase 01 Plan 02: Pipeline Integrity — InMail URN, classify_tg, create_image_ad Hardening Summary

**InMail sender URN set for testing (vYrY4QMQH0), classify_tg import verified callable, and create_image_ad MEMBER_URN failure now logs a clear scope-dependency blocker instead of a generic exception**

## Performance

- **Duration:** ~2 min
- **Started:** 2026-04-20T21:41:11Z
- **Completed:** 2026-04-20T21:43:00Z
- **Tasks:** 2
- **Files modified:** 2 (.env locally, main.py committed)

## Accomplishments
- Set `LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0` in `.env` — InMail campaigns no longer silently skip due to empty sender URN (D-06)
- Verified `classify_tg` is importable from `src.figma_creative` and callable at runtime (PIPE-01 confirmed)
- Hardened `create_image_ad` failure path: specific `RuntimeError` handler with `LINKEDIN_MEMBER_URN` check logs a clear blocker message referencing the required `r_liteprofile`/`rw_organization_admin` LinkedIn scope (D-07)
- `sheets.write_creative` (LI-03 code path) confirmed inside the `try` block — will execute when `create_image_ad` eventually succeeds

## Task Commits

Each task was committed atomically:

1. **Task 1: Set LINKEDIN_INMAIL_SENDER_URN and verify classify_tg import** - local `.env` change (gitignored, no commit) + classify_tg already present (no file change needed)
2. **Task 2: Harden create_image_ad failure path** - `3c53d3c` (fix)

**Plan metadata:** (docs commit below)

## Files Created/Modified
- `.env` - Set `LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0` (local only — gitignored)
- `main.py` - Replaced generic `except Exception` with tiered handler: `except RuntimeError` checking `LINKEDIN_MEMBER_URN` first, then `except Exception` as catch-all

## Decisions Made
- `.env` is correctly gitignored; the URN update is applied locally only. No commit deviation — this is the expected security posture.
- `classify_tg` was already present in `main.py` from prior session (STATE.md note 2026-04-21 confirmed). No file modification needed; task verified and marked complete without a code commit.
- Added tiered RuntimeError handler per D-07. The blocker log message explicitly names the required LinkedIn scopes (`r_liteprofile` or `rw_organization_admin`) and references CONTEXT.md D-07 for operator context.

## Deviations from Plan

### Auto-handled: classify_tg already present

- **Found during:** Task 1
- **Issue:** Plan assumed classify_tg might be missing — prior session had already added it
- **Resolution:** Verified present via automated check, no file edit needed. This is correct behavior (no regression).
- **Impact:** None — acceptance criteria satisfied without code change.

### Auto-handled: .env gitignored — no Task 1 commit

- **Found during:** Task 1 commit attempt
- **Issue:** `.env` is in `.gitignore` (correct security practice); `git add .env` rejected
- **Resolution:** URN set locally; no commit created for Task 1 (no tracked files changed). Documented in SUMMARY.
- **Impact:** None — the plan artifact requirement (`.env` contains the URN) is satisfied at the file level.

---

**Total deviations:** 2 auto-handled (pre-existing state, gitignore boundary)
**Impact on plan:** No scope impact. Both cases are correct behavior. All acceptance criteria satisfied.

## Issues Encountered
- Verification script for Task 2 used `src.split('except RuntimeError')[1]` which fails when multiple `except RuntimeError` blocks exist (line 119 has one too). Adapted the verification to search the relevant section of main.py rather than splitting on first occurrence. All checks passed.

## User Setup Required
None — LINKEDIN_INMAIL_SENDER_URN is already applied to `.env` locally. No external service configuration steps needed.

## Known Stubs
None — no stub patterns introduced in this plan.

## Next Phase Readiness
- InMail pipeline no longer silently skips due to empty sender URN
- classify_tg import confirmed functional (PIPE-01 complete)
- create_image_ad failure is now informative — operators know exactly what LinkedIn scope change is needed (D-07 blocker documented)
- Remaining Phase 1 plans (01-01, 01-03) can proceed; this plan's requirements are unblocked

---
*Phase: 01-pipeline-integrity*
*Completed: 2026-04-20*

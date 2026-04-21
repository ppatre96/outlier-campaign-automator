---
phase: 03-campaign-expansion
title: Campaign Expansion
type: milestone
status: planning
wave: 1
depends_on: [2.5]
files_modified:
  - scripts/regen_stem_inmail.py
  - src/figma_creative.py
  - tests/test_classify_tg.py
  - README.md
autonomous: false
requirements:
  - EXP-01
  - EXP-02
user_setup: []
---

# Phase 3: Campaign Expansion

## Goal

Regenerate proven STEM InMail campaigns with the proven financial angle (F) and extend the TG classifier to cover the MATH cohort type, closing gaps in audience segmentation and copy targeting.

## Phase Requirements

| ID | Description | Type |
|----|-------------|------|
| EXP-01 | STEM InMail variants regenerated with financial angle (F) for campaigns 633412886, 635201096, 634012966 | Execute |
| EXP-02 | `classify_tg` extended with MATH bucket for cohorts with statistics/quantitative/actuarial signals | Execute |

## Subphases

### 03-01: STEM InMail Financial Angle Regen (EXP-01)

**Objective:** Create and attach angle-F InMail creatives to three existing STEM campaigns.

**Key deliverables:**
- `scripts/regen_stem_inmail.py` — targeted regen script with `--dry-run` support
- Three new sponsoredCreative URNs attached to existing campaigns
- Three new rows logged to Sheets Creatives tab
- README usage documentation

**Success criteria (from 03-01-PLAN.md must_haves):**
- Angle-F InMail creative created for each campaign ID (633412886, 635201096, 634012966)
- Each creative attached to existing campaign via `urn:li:sponsoredCampaign:{id}` (no new campaigns)
- Each creative URN appended to Sheets with campaign identifier and timestamp
- Script exposes `--dry-run` mode (builds copy without hitting LinkedIn/Sheets)
- Script fails fast with clear error if `LINKEDIN_INMAIL_SENDER_URN` or `LITELLM_API_KEY` missing

**Estimated effort:** 3–4 hours

---

### 03-02: TG Classifier MATH Bucket (EXP-02)

**Objective:** Extend `classify_tg()` with MATH bucket and regression test coverage.

**Key deliverables:**
- MATH regex added to `classify_tg()` (between ML_ENGINEER and MEDICAL in priority)
- `TG_PALETTES["MATH"]` and `TG_ILLUS_VARIANTS["MATH"]` entries
- `tests/test_classify_tg.py` — full test coverage for all 7 buckets
- README updated with new bucket list

**Success criteria (from 03-02-PLAN.md must_haves):**
- `classify_tg()` returns `"MATH"` for math/statistics/quantitative/actuarial keywords
- MATH evaluated BEFORE SOFTWARE_ENGINEER (prevents `python` false-positives)
- Both `TG_PALETTES` and `TG_ILLUS_VARIANTS` contain MATH entries
- All existing buckets (DATA_ANALYST, ML_ENGINEER, MEDICAL, LANGUAGE, SOFTWARE_ENGINEER) still work correctly
- Unit tests cover all 7 buckets + priority ordering edge cases

**Estimated effort:** 2–3 hours

---

## Scope & Boundaries

### In Scope
- EXP-01: Regen script for three known STEM campaign IDs with angle F
- EXP-02: MATH bucket regex + Figma design token entries + regression tests
- LinkedIn API calls via existing `li_client` (no scope expansion to new endpoints)
- Sheets write-back via existing `sheets.write_creative()` (no new Sheets structure)

### Out of Scope
- New campaign creation (use campaign-manager for that)
- Full A/B test harness (Phase 2.5 handles experimentation)
- Google Drive upload (Phase 3 backlog item, not in EXP-01/EXP-02)
- LinkedIn MDP approval (Phase 3 backlog, not required for this phase)
- New TG buckets beyond MATH (FINANCE/LEGAL/DESIGN deferred per CONTEXT)

---

## Execution Plan

Both subphases execute in **Wave 1** (parallel if resources allow, sequential if needed).

### Dependency Graph
```
03-01: STEM InMail Regen
  - Depends on: Phase 2.5 (completed) ✓
  - No blockers identified ✓
  - Reads from: config.py, src/inmail_copy_writer.py, src/linkedin_api.py, src/sheets.py
  - Writes to: scripts/regen_stem_inmail.py (new file)

03-02: TG Classifier MATH Bucket
  - Depends on: Phase 2.5 (completed) ✓
  - No blockers identified ✓
  - Reads from: src/figma_creative.py (current buckets)
  - Writes to: src/figma_creative.py (add regex), tests/test_classify_tg.py (new file)
```

**Parallelization:** Both subphases are independent; can be executed in parallel.

---

## Prerequisites Checklist

- ✓ Phase 2.5 (Feedback Loops & Experimentation) complete
- ✓ RESEARCH.md complete (2026-04-20)
- ✓ CONTEXT.md complete (Phase 3 decisions locked)
- ✓ `LINKEDIN_INMAIL_SENDER_URN` set (`urn:li:person:vYrY4QMQH0`)
- ✓ `LITELLM_API_KEY` available in environment
- ✓ `LINKEDIN_ACCESS_TOKEN` valid (expires ~June 2026)
- ✓ Google Sheets credentials (`credentials.json`) available
- ⚠ STEM campaign TG labels (ML_ENGINEER/SOFTWARE_ENGINEER/MEDICAL) — assumed based on "STEM" classification

---

## Definition of Done

### Phase 3 Complete When:
1. ✓ EXP-01 complete: `scripts/regen_stem_inmail.py` is executable, passes `--dry-run` test, README documents usage
2. ✓ EXP-02 complete: `classify_tg()` returns MATH, priority ordering correct, test suite at 100% coverage for new bucket
3. ✓ All code merged to main branch
4. ✓ SUMMARY.md written with outcomes and any discovered gaps

### Verification Gates:
- Code review: All files follow existing patterns (camelCase for functions, existing error handling style)
- Testing: 03-02 test suite passes; 03-01 `--dry-run` mode produces expected output (copy, no API calls)
- Integration: No regressions to existing pipeline (Phase 1 + 2 + 2.5 still work)

---

## Key Constraints & Gotchas

1. **Angle F selection** — EXP-01 MUST call `build_inmail_variants(..., angle_keys=["F"])` directly. Calling without this param will generate F but select A/B/C by index.
2. **MATH priority ordering** — MATH regex MUST be checked BEFORE SOFTWARE_ENGINEER to avoid `python` catching statisticians. This is a silent bug if skipped.
3. **TG_PALETTES/TG_ILLUS_VARIANTS** — Both dicts MUST have MATH entry or Figma customizeDesign() silently falls back to GENERAL styling.
4. **Cohort stub for regen** — The stub cohort passed to `build_inmail_variants()` must have at minimum: `.name`, `.rules` (list, can be empty), `.lift_pp` (float).
5. **Sheets logging** — `sheets.write_creative()` signature is `(stg_id, creative_name, li_creative_id)`. Do NOT confuse with `update_li_campaign_id()`.

---

## Risk & Mitigation

| Risk | Severity | Mitigation |
|------|----------|-----------|
| LinkedIn API token expires mid-execution | LOW | Token expires ~June 2026; refresh path is tested. Monitor `LINKEDIN_ACCESS_TOKEN` in config during execution. |
| `python` keyword in MATH regex check causes false-positives | MEDIUM | MATH check BEFORE SOFTWARE_ENGINEER in 03-02. Test coverage includes edge case. |
| STEM campaign IDs have unknown TG labels | LOW | Assume ML_ENGINEER/SOFTWARE_ENGINEER based on "STEM" classification. If wrong, copy quality degrades but campaigns still attach. |
| Sheets credentials missing or expired | LOW | Check `credentials.json` exists before running 03-01. Fail-fast error in write_creative(). |

---

## Notes for Executor

- **Test the dry-run path first** (03-01): Ensure `scripts/regen_stem_inmail.py --dry-run` builds copy without API calls before running live.
- **Regression test early** (03-02): Run `pytest tests/test_classify_tg.py` after adding MATH to verify all 7 buckets work correctly.
- **No campaign group creation** (03-01): The regen path attaches to existing campaigns only. Do NOT call `create_campaign_group()` or `create_inmail_campaign()`.
- **Vocabulary compliance** (both): Any hardcoded strings (log messages, fallback copy) MUST follow CLAUDE.md rules (no "job", "role", "compensation", etc.).

---

*Phase: 03-campaign-expansion*  
*Created: 2026-04-21 for GSD plan-phase orchestrator*  
*Subphase plans: 03-01-PLAN.md, 03-02-PLAN.md*

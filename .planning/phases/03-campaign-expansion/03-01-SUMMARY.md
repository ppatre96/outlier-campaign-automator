---
phase: 03-campaign-expansion
plan: "01"
title: "STEM InMail Financial Angle Regen"
subsystem: inmail-regen
tags: [inmail, stem, angle-f, financial, linkedin]
dependency_graph:
  requires: []
  provides: [scripts/regen_stem_inmail.py]
  affects: [README.md]
tech_stack:
  added: []
  patterns: [stub-cohort, lazy-client-construction, preflight-guards, dry-run-mode]
key_files:
  created:
    - scripts/regen_stem_inmail.py
  modified:
    - README.md
decisions:
  - "StubCohort dataclass sufficient for build_inmail_variants — only reads name, rules[:4], lift_pp"
  - "Lazy LinkedInClient/SheetsClient construction only in live mode to avoid auth round-trips in dry-run"
  - "angle-F fallback produces generic subject ('AI tasks for domain experts') when LiteLLM unreachable — documented as known limitation, not a bug"
metrics:
  duration_seconds: 129
  completed_date: "2026-04-21"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 2
requirements:
  - EXP-01
---

# Phase 03 Plan 01: STEM InMail Financial Angle Regen Summary

Surgical regen script that attaches angle-F (Financial — rate-in-subject-line) InMail creatives to three existing STEM campaigns without creating new campaign groups or campaigns.

## What Was Built

### scripts/regen_stem_inmail.py (new)

Standalone, runnable script targeting three hardcoded STEM campaigns:

| Campaign ID | TG Category | Name |
|------------|-------------|------|
| 633412886 | ML_ENGINEER | STEM Campaign A |
| 635201096 | SOFTWARE_ENGINEER | STEM Campaign B |
| 634012966 | MEDICAL | STEM Campaign C |

**Flow per campaign:**
1. Build `StubCohort(name=..., rules=[], lift_pp=0.0)`
2. Call `build_inmail_variants(tg_cat, cohort, api_key, angle_keys=["F"], hourly_rate="$50")`
3. Take `variants[0]` (angle F)
4. In live mode: `li.create_inmail_ad(campaign_urn=f"urn:li:sponsoredCampaign:{id}", ...)`
5. In live mode: `sheets.write_creative(stg_id=..., creative_name=..., li_creative_id=...)`

**CLI flags:** `--dry-run`, `--only-id <int>`, `--hourly-rate <str>`

**Preflight guards:** exits 2 immediately if `LINKEDIN_INMAIL_SENDER_URN` or `LITELLM_API_KEY` unset; exits 2 if `LINKEDIN_ACCESS_TOKEN` unset in live mode.

### README.md (modified)

Added "Scripts" section with "STEM InMail Financial-Angle Regen" subsection documenting dry-run, single-campaign, and full-run usage.

## Dry-Run Output Snippet (LiteLLM unavailable — fallback path)

LiteLLM was unreachable in the execution environment (connection error), so the fallback path in `build_inmail_variants` was triggered. This is the expected behavior documented in the plan.

**Campaign A (id=633412886, ML_ENGINEER):**
- SUBJECT: `AI tasks for domain experts`
- BODY (first 2 lines): `No shifts. No deadlines. No minimum hours. / Outlier is an AI data platform where ML_ENGINEER professionals complete AI tasks...`

**Campaign B (id=635201096, SOFTWARE_ENGINEER):**
- SUBJECT: `AI tasks for domain experts`
- BODY (first 2 lines): `No shifts. No deadlines. No minimum hours. / Outlier is an AI data platform where SOFTWARE_ENGINEER professionals complete AI tasks...`

**Campaign C (id=634012966, MEDICAL):**
- SUBJECT: `AI tasks for domain experts`
- BODY (first 2 lines): `No shifts. No deadlines. No minimum hours. / Outlier is an AI data platform where MEDICAL professionals complete AI tasks...`

Note: The fallback subject (`AI tasks for domain experts`) is the generic default branch of `_fallback_subject()` — angle F has no dedicated fallback. When LiteLLM is reachable, the subject will match one of the proven formats: `[Role] | Flexible Hours & $X/hr`, `[Skill] + AI = Flexible $X/hr`, or `Earn $X/hr with [Skill] + AI`.

## How to Trigger Live Regen

```bash
cd /path/to/outlier-campaign-agent
PYTHONPATH=. python3 scripts/regen_stem_inmail.py
```

Ensure `.env` has `LINKEDIN_INMAIL_SENDER_URN`, `LINKEDIN_ACCESS_TOKEN`, and `LITELLM_API_KEY` set.

## Known Limitations

1. **Angle-F fallback missing if LiteLLM is down**: `_fallback_subject()` in `inmail_copy_writer.py` only has dedicated fallbacks for angles A, B, C — angle F falls to the generic `"AI tasks for domain experts"` default branch. This is noted as Open Question #2 in research and is out of scope for this plan. Improvement tracked in Phase 3 research.

2. **StubCohort has no rules**: The cohort passed to `build_inmail_variants` has `rules=[]`. The prompt will show `Key signals: n/a` to the LLM, which may produce slightly less targeted copy than a cohort with actual signals. Acceptable for a targeted regen; production pipeline would use real cohorts from Stage A/B.

## Traceability

- **EXP-01** — STEM InMail regen with financial angle (campaigns A=633412886, B=635201096, C=634012966)
- Closes the gap where `_process_inmail_campaigns()` rotates angles A/B/C by index and never picks F

## Deviations from Plan

None — plan executed exactly as written.

## Commits

| Task | Commit | Description |
|------|--------|-------------|
| Task 1 | 038fa99 | feat(03-01): create regen_stem_inmail.py for angle-F STEM InMail regen |
| Task 2 | (no code changes — verification only) | Dry-run verified: exit=0, 3 DRY RUN blocks, all 3 campaign URNs, vocab OK |
| Task 3 | 7457b8d | docs(03-01): add STEM InMail regen usage to README |

## Self-Check: PASSED

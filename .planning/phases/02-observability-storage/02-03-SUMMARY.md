---
phase: 2
plan: 3
subsystem: creative-pipeline
tags: [llm-quality, validation, observability, photo_subject]
dependency_graph:
  requires: []
  provides: [validate_photo_subject, llm-context-logging, llm-context-docs]
  affects: [src/midjourney_creative.py, src/figma_creative.py, scripts/dry_run.py]
tech_stack:
  added: []
  patterns: [input-validation-guard, structured-print-logging, inline-architecture-docs]
key_files:
  created: []
  modified:
    - src/midjourney_creative.py
    - src/figma_creative.py
    - scripts/dry_run.py
decisions:
  - validate_photo_subject uses regex matching on lowercased input against 7 known generic patterns
  - re module imported at module level in midjourney_creative.py — no inner import needed
  - token_count field shows n/a until build_copy_variants attaches LiteLLM usage — intentional per plan
metrics:
  duration: 8 minutes
  completed: "2026-04-20"
  tasks_completed: 3
  tasks_total: 3
  files_changed: 3
---

# Phase 2 Plan 3: LLM Context Quality + photo_subject Validation Summary

## One-liner

Added `validate_photo_subject()` guard with 7 regex patterns to block generic Gemini prompts, plus per-variant LiteLLM model/photo_subject logging in dry_run.py Stage 8, and full two-stage LLM context flow documentation in figma_creative.py.

## Tasks Completed

| # | Task | Commit | Files |
|---|------|--------|-------|
| 1 | Add `validate_photo_subject()` to midjourney_creative.py | a34d960 | src/midjourney_creative.py |
| 2 | Add LiteLLM logging in dry_run.py | a746002 | scripts/dry_run.py |
| 3 | Document LLM context flow in figma_creative.py | 9d66168 | src/figma_creative.py |

## What Was Built

**Task 1 — `validate_photo_subject()` guard (src/midjourney_creative.py)**

Added `_GENERIC_SUBJECT_PATTERNS` list with 7 regex patterns covering common stock-photo descriptions: "professional person at a laptop", "scientist at a computer", "person working at a laptop", bare "male/female professional/scientist/person", "domain expert", "remote worker", and "knowledge worker". The `validate_photo_subject()` function matches against all patterns on a lowercased copy of the input and raises `ValueError` with a descriptive error message including an example of a valid subject. Called inside `generate_midjourney_creative()` after the empty-string `RuntimeError` guard, before `_build_imagen_prompt()`.

**Task 2 — LiteLLM logging in dry_run.py Stage 8 (scripts/dry_run.py)**

Inserted a logging block after the `if not variants` guard that prints `LITELLM_MODEL` and per-variant `photo_subject` + `token_count` (shows `n/a` until `build_copy_variants()` is extended to attach LiteLLM token usage — intentionally deferred per plan scope). Enables operators to spot generic subjects in dry-run terminal output before Gemini is called.

**Task 3 — LLM context flow documentation (src/figma_creative.py)**

Inserted a 30-line comment block immediately before `build_copy_variants()` explaining the complete two-stage LLM pipeline: Stage 1 inputs (cohort name, rules, human-readable signals, Figma layer map), Stage 1 outputs (headline, subheadline, cta, photo_subject, layerUpdates), Stage 2 reference (generate_midjourney_creative in midjourney_creative.py), why photo_subject is the critical bridge, and a reminder about Outlier approved vocabulary enforcement.

## Acceptance Tests

- [x] `validate_photo_subject("professional person at a laptop")` raises `ValueError`
- [x] `validate_photo_subject("female South Asian cardiologist, reviewing ECG data at home")` does NOT raise
- [x] `validate_photo_subject("")` does NOT raise (empty handled by RuntimeError guard before it)
- [x] `generate_midjourney_creative()` calls `validate_photo_subject(subject)` before `_build_imagen_prompt()`
- [x] `scripts/dry_run.py` prints `LiteLLM model :` and per-variant `photo_subject` in Stage 8 output
- [x] `src/figma_creative.py` contains the `Stage 1 — Copy generation` comment block before `build_copy_variants()`
- [x] All three files pass `python3 -c "import ast; ast.parse(open('<file>').read())"` with no syntax errors

## Deviations from Plan

### Auto-fixed Issues

None — plan executed exactly as written.

One minor adaptation: the inline `import re` inside `validate_photo_subject()` was omitted since `re` is already imported at the top of `midjourney_creative.py` (line 23). The plan notes noted this as "alternatively omit the inner import if re is confirmed at module level" — confirmed and omitted.

## Known Stubs

None. All three changes are fully wired: validation guard is called in the live Gemini path, logging block runs in Stage 8 when variants are present, and documentation comment is static.

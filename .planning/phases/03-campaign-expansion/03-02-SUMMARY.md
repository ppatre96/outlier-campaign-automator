---
phase: 03-campaign-expansion
plan: "02"
title: "TG Classifier MATH Bucket"
subsystem: classify_tg
tags: [tg-classifier, math, figma, pytest, regression]
dependency_graph:
  requires: []
  provides: [MATH bucket in classify_tg, TG_PALETTES.MATH, TG_ILLUS_VARIANTS.MATH]
  affects: [src/figma_creative.py, tests/test_classify_tg.py, README.md]
tech_stack:
  added: []
  patterns: [regex priority ordering, parametrized pytest]
key_files:
  created:
    - tests/test_classify_tg.py
    - README.md
  modified:
    - src/figma_creative.py
decisions:
  - MATH regex inserted at priority slot 3 (between ML_ENGINEER and MEDICAL) per CONTEXT locked decision
  - MATH palette matches DATA_ANALYST blue tones (analytical feel)
  - MATH illus variants = ['chart','neural','chart'] per research recommendation
metrics:
  duration: "~8 minutes"
  completed: "2026-04-20"
  tasks_completed: 3
  files_changed: 3
requirements:
  - EXP-02
---

# Phase 03 Plan 02: TG Classifier MATH Bucket Summary

**One-liner:** Added MATH TG bucket to `classify_tg()` with locked regex at priority slot 3, plus matching palette/illus entries and 25-test pytest suite covering all 7 buckets.

## What Was Built

### src/figma_creative.py

Three changes:

1. **TG_PALETTES** — new `"MATH"` entry (after ML_ENGINEER, before SOFTWARE_ENGINEER):
   ```python
   "MATH": [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}]
   ```

2. **TG_ILLUS_VARIANTS** — new `"MATH"` entry:
   ```python
   "MATH": ["chart", "neural", "chart"]
   ```

3. **classify_tg() docstring + regex branch** — inserted between ML_ENGINEER (slot 2) and MEDICAL (now slot 4):
   ```python
   if re.search(r'\b(math|mathematics|statistics|statistician|actuary|actuarial|quantitative|physicist|physics|algebra|calculus|probability|stochastic|mathematician|econometrics|biostatistics)\b', text):
       return "MATH"
   ```

### tests/test_classify_tg.py (new)

25 pytest cases:
- 8 MATH cases (6 name-based + 2 rules-field via `__` substitution)
- 10 cases covering the 5 pre-existing non-GENERAL buckets
- 2 GENERAL fallback cases
- 2 priority-ordering tests (MATH beats SOFTWARE_ENGINEER when both signals present)
- 3 dict-shape guardrail tests

pytest run: **25 passed, 0 failed** in 1.10s.

### README.md (new)

Created root-level README with TG bucket priority table documenting all 7 buckets in order, pointing to `src/figma_creative.py` for exact regex patterns.

## Downstream Impact

`classify_tg()` output flows as `tg_cat` into `_process_inmail_campaigns()` → `build_inmail_variants()` prompt interpolation. The `"MATH"` string label passes through automatically — no further code changes required for InMail copy to reference the MATH bucket.

`TG_PALETTES["MATH"]` and `TG_ILLUS_VARIANTS["MATH"]` are consumed by `customizeDesign()` in the injected Figma JavaScript — MATH cohorts now get analytical blue palette + chart/neural illustrations instead of silently falling through to GENERAL.

## Pytest Run Snippet

```
25 passed, 1 warning in 1.10s
```

Warning is pre-existing `datetime.utcnow()` deprecation in `config.py` — out of scope.

## Deferred Items (carried from CONTEXT §deferred)

- **FINANCE bucket** — deferred; no performance data yet
- **LEGAL bucket** — deferred; no performance data yet
- **DESIGN bucket** — deferred; no performance data yet
- **DATA_SCIENTIST bucket** — deferred; `\bdata\b` in DATA_ANALYST regex already captures most data science cohorts; splitting requires careful regex design to avoid regressions

## Deviations from Plan

None — plan executed exactly as written.

## Traceability

- Requirement: **EXP-02** — MATH TG bucket classification
- CONTEXT decision: MATH regex LOCKED as specified in 03-CONTEXT.md
- Performance basis: MATH audience $14.14 CPA (PROJECT.md performance data)

## Self-Check: PASSED

- `src/figma_creative.py` — exists, AST parses clean
- `tests/test_classify_tg.py` — exists, 25/25 tests pass
- `README.md` — exists, contains MATH + all 7 bucket names
- Commits: 6278313 (figma_creative.py), 6504d4e (test file), 469fbd4 (README)

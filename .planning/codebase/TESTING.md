# Testing Patterns

**Analysis Date:** 2026-04-20

## Test Framework

**Runner:**
- None. There is no pytest, unittest, or any test runner configured.
- `requirements.txt` has no `pytest`, `pytest-*`, `unittest`, or `mock` packages.
- No `pytest.ini`, `setup.cfg`, `pyproject.toml`, `tox.ini`, or `conftest.py` files exist.
- No files named `test_*.py` or `*_test.py` exist under `src/` (zero test files in source tree).

**What exists instead:**
Manual validation is the only testing mechanism. Two categories:
1. `scripts/dry_run.py` — full pipeline dry run (reads live data, stops before writes)
2. `scripts/test_nano_banana_litellm.py` — connectivity smoke test for the LiteLLM image generation endpoint

## dry_run.py — Full Pipeline Validation

**Location:** `scripts/dry_run.py`

**What it validates (8 stages):**

| Stage | What runs | External calls made |
|-------|-----------|---------------------|
| 0 | Resolve project_id → signup_flow_id + config_name | Redash SQL |
| 1 | Fetch screening data | Redash SQL (RESUME_SQL — full Snowflake query) |
| 2 | Feature engineering (binary features) | None — pure pandas |
| 3 | Stage A cohort discovery (univariate + beam search) | None — pure numpy/scipy |
| 4 | Stage B country validation | None — pure pandas |
| 5+6 | Stage C URN resolution + LinkedIn audience count | Google Sheets, LinkedIn API |
| 7 | Print cohort summary (no sheet writes) | None |
| 8 | Creative generation (Gemini via LiteLLM) + Drive upload | LiteLLM /images/generations, Google Drive |

**Invocation:**
```bash
# By flow_id (original entry point):
PYTHONPATH=. python3 scripts/dry_run.py --flow-id 69a7a186d91acccdf955b912

# By project_id (auto-resolves flow + config):
PYTHONPATH=. python3 scripts/dry_run.py --project-id 698a172324c01532c2f92a0d

# Skip creative generation (faster iteration on analysis):
PYTHONPATH=. python3 scripts/dry_run.py --project-id 698a172324c01532c2f92a0d --skip-creatives

# Override screening config name:
PYTHONPATH=. python3 scripts/dry_run.py --flow-id <id> --config-name "Clinical Medicine - Cardiology"
```

**What it does NOT do:**
- Does not write cohorts to Google Sheets (explicitly skipped)
- Does not create LinkedIn campaigns or creatives
- Does not write to the Creatives tab
- Does not send Slack messages

**Stage C behavior in dry_run:**
Stage C calls the LinkedIn Audience Counts API (`audienceCounts?q=targetingCriteriaV2`), which requires LinkedIn Marketing Developer Platform (MDP) approval. This endpoint returns 400 in unapproved environments. `dry_run.py` wraps Stage C in a `try/except` and falls back to the top-N Stage B cohorts when the API is blocked:
```python
def _try_stage_c(cohorts_b, li_token: str) -> list:
    try:
        ...
        return stage_c(cohorts_b, urn_res, li_client)
    except Exception as exc:
        log.warning("Stage C unavailable (%s) — will use Stage B top cohorts", exc)
        return []
```

**Output artifacts:**
Creative PNGs saved to `data/dry_run_outputs/` with naming `dry_<stg_id_slug>_<angle>.png`.
Existing outputs are in `data/dry_run_outputs/` — 20+ PNGs from prior runs. These are committed to the repo (not in `.gitignore`).

**Stage C inline dry-run in `src/stage_c.py`:**
`stage_c.py` also has its own `if __name__ == "__main__":` block (lines 121–172) that can be run directly:
```bash
PYTHONPATH=. python src/stage_c.py
```
This creates a hardcoded sample cohort (`[DRY RUN] Python Dev es-419`) and exercises URN resolution + the audience count API call in isolation. Useful for validating the LinkedIn token and MDP approval status independently.

## test_nano_banana_litellm.py — Connectivity Smoke Test

**Location:** `scripts/test_nano_banana_litellm.py`

**What it validates:**
Three sequential checks, each exits with code 1 on failure:
1. LiteLLM text completions endpoint (`/chat/completions`) using `gemini/gemini-2.5-flash`
2. LiteLLM image generation (`/images/generations`) using `gemini/gemini-2.5-flash-image` — saves PNG to `/tmp/nano_banana_test.png`
3. LiteLLM image generation using `gemini/imagen-4.0-generate-001` — saves PNG to `/tmp/imagen4_test.png`

**Invocation:**
```bash
cd /Users/pranavpatre/outlier-campaign-agent
source venv/bin/activate
PYTHONPATH=. python scripts/test_nano_banana_litellm.py
```

**Output:** Prints PASSED/FAILED with HTTP status codes. Saves PNG artifacts to `/tmp/`. Not a structured test — uses `print()` and `sys.exit(1)` rather than assertions.

## generate_experiment_creatives.py — One-Shot Creative Batch

**Location:** `scripts/generate_experiment_creatives.py`

**What it does:**
Reads hardcoded `EXPERIMENTS` list (3 ad briefs from 2026-04-16) and calls `generate_midjourney_creative()` for each, saving composed PNGs to `data/experiment_outputs/`. Not a validation script — it's a production batch run for specific experiment IDs. Outputs committed to `data/experiment_outputs/`.

## Data Fixtures and Mocks

**No mocking infrastructure exists.** There are no `unittest.mock`, `pytest-mock`, `MagicMock`, or `patch` usages anywhere in the codebase.

**No fixture files for testing.** The `data/` directory contains:
- `data/dry_run_outputs/` — PNG artifacts from past dry_run executions
- `data/experiment_outputs/` — PNG artifacts from experiment creative runs
- `data/test_outputs/` — 6 PNG files: `composed_ad_angle_A.png`, `composed_ad_v2.png`, model-named PNGs from Gemini/Imagen testing
- `data/experiment_queue.json` — hardcoded experiment brief data (not a fixture file, used by `generate_experiment_creatives.py`)
- `data/creative_vision_cache.json` — cached creative vision description

None of these are test fixtures in the pytest sense — they are production artifacts from manual runs.

## What Is and Is Not Covered

**Covered by dry_run.py (manual, live calls):**
- `src/redash_db.py` — full SQL query execution path is exercised
- `src/features.py` — `engineer_features`, `build_frequency_maps`, `binary_features` all run
- `src/analysis.py` — `stage_a` (univariate tests + beam search) and `stage_b` (country validation) both run
- `src/stage_c.py` — URN resolution always runs; audience count runs if token has MDP approval
- `src/linkedin_urn.py` — `UrnResolver.resolve_cohort_rules` exercised in Stage C
- `src/midjourney_creative.py` — full Gemini image generation + PIL compositing run (unless `--skip-creatives`)
- `src/figma_creative.py` — `build_copy_variants` (LiteLLM call) runs during Stage 8 copy generation
- `src/gdrive.py` — upload runs if `GDRIVE_ENABLED=true`

**Covered by test_nano_banana_litellm.py:**
- LiteLLM proxy connectivity (text + image endpoints)
- Gemini/Imagen model availability

**Not covered by any validation script:**
- `src/linkedin_api.py` — `create_campaign`, `create_campaign_group`, `create_inmail_ad`, `create_image_ad`, `upload_image` (all create/mutate API calls are never exercised in dry-run mode)
- `src/sheets.py` — `write_cohorts`, `update_li_campaign_id`, `write_creative` are never called in dry-run
- `src/inmail_copy_writer.py` — never exercised by `dry_run.py` (InMail path requires `--dry-run` flag on `main.py` with an INMAIL row in the sheet, not a standalone script)
- `src/campaign_monitor.py` — monitor mode has no dry-run script; only exercised via `python main.py --mode monitor --dry-run`
- `src/inmail_weekly_report.py`, `src/static_weekly_report.py` — exercised only by `scripts/post_weekly_reports.py` against live data
- `src/sponsored_content_analysis.py`, `src/competitor_intel.py`, `src/campaign_feedback_agent.py` — no validation scripts; run manually against live APIs
- Error paths — no tests for token expiry handling, LinkedIn 4xx responses, Redash timeout, or parse failures
- `src/analysis.py` — statistical correctness of `two_prop_z_test`, `passes_thresholds`, and beam search diversity multiplier has no unit test coverage
- `src/features.py` — edge-case handling in `_safe_json`, `extract_titles`, `derive_country` is untested
- `config.py` — import-time evaluation and env var priority logic has no test coverage

## Test Coverage Gaps

**High-impact gaps (likely to cause silent regressions):**

**`src/analysis.py` statistical correctness:**
- `two_prop_z_test` computes a z-test manually — no verification against known values
- `passes_thresholds` gates all cohort selection — threshold logic changes are untested
- Beam search diversity multiplier (`0.7` / `0.4`) affects which campaigns get created
- Files: `src/analysis.py` (lines 55–96, 136–236)
- Risk: A silent bug in cohort scoring would produce wrong audiences in LinkedIn without any error

**`src/features.py` data parsing:**
- `_safe_json`, `extract_skills`, `extract_titles` handle malformed Snowflake JSON — edge cases (None, empty string, unexpected types) are untested
- `binary_features` duplicate-column guard (`isinstance(series, pd.DataFrame)`) is a workaround with no regression test
- Files: `src/features.py`
- Risk: New Snowflake schema changes would silently produce empty features

**`src/inmail_copy_writer.py` response parsing:**
- `_parse_response` is a custom line-by-line parser for Claude's structured output — brittle to format variations
- `_shorten_field` fallback truncation would silently produce bad copy
- Files: `src/inmail_copy_writer.py` (lines 310–336)
- Risk: Model format changes silently produce empty subjects or bodies

**`src/linkedin_urn.py` fuzzy matching:**
- `URN_FUZZY_MATCH_THRESHOLD = 0.85` gates which cohort features resolve to LinkedIn URNs
- No test for cases where fuzzy match returns wrong URN (e.g., "Python" matching "Python Developer" vs. "Python Scripting")
- Files: `src/linkedin_urn.py`
- Risk: Wrong URN resolution creates campaigns targeting incorrect LinkedIn audiences

**`src/figma_creative.py` `classify_tg` regex:**
- All regex patterns are inline — correct routing of cohorts to TG category affects copy, palette, and illustration variant
- Files: `src/figma_creative.py` (lines 57–77)
- Risk: A new cohort name format could silently fall through to GENERAL instead of a targeted category

## How to Add Tests

No test runner is configured. To add tests:
1. Install pytest: `pip install pytest`
2. Create `tests/` directory at project root
3. Reference `PYTHONPATH=.` when running: `PYTHONPATH=. pytest tests/`
4. Priority targets for unit testing: `src/analysis.py` (statistics), `src/features.py` (data parsing), `src/inmail_copy_writer.py` (`_parse_response`), `src/figma_creative.py` (`classify_tg`)

Pure-function modules (`analysis.py`, `features.py`, `figma_creative.classify_tg`) can be unit tested with no external dependencies. API-calling modules (`linkedin_api.py`, `redash_db.py`, `sheets.py`) would require mocking.

---

*Testing analysis: 2026-04-20*

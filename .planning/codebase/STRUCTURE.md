# Codebase Structure

**Analysis Date:** 2026-04-20

## Directory Layout

```
outlier-campaign-agent/
├── main.py                         # Orchestrator entry point (launch + monitor modes)
├── config.py                       # All runtime constants + env var reads
├── credentials.json                # Google service account key (gitignored)
├── .env                            # Secrets (gitignored)
├── src/                            # Core library modules
│   ├── __init__.py
│   ├── redash_db.py                # Snowflake-via-Redash data client + SQL definitions
│   ├── snowflake_db.py             # Legacy direct Snowflake connector (inactive)
│   ├── features.py                 # Feature engineering (binary columns from SQL output)
│   ├── analysis.py                 # Stage A (univariate + beam search) + Stage B (country validation)
│   ├── stage_c.py                  # Stage C (URN resolution + audience validation + greedy selection)
│   ├── linkedin_urn.py             # Fuzzy URN resolution against Google Sheets mapping
│   ├── linkedin_api.py             # LinkedIn Marketing API client
│   ├── sheets.py                   # Google Sheets read/write client
│   ├── figma_creative.py           # TG classifier + copy generation (LiteLLM) + Figma clone path
│   ├── midjourney_creative.py      # Gemini image generation + PIL ad composition
│   ├── inmail_copy_writer.py       # InMail angle variants via LiteLLM
│   ├── campaign_monitor.py         # Monitor loop: learning phase check, scoring, pause, ICP discovery
│   ├── campaign_feedback_agent.py  # Creative performance analysis via Claude Vision + experiment queue
│   ├── inmail_weekly_report.py     # Weekly InMail performance report builder
│   ├── static_weekly_report.py     # Weekly static ad performance report builder
│   ├── competitor_intel.py         # Competitor ad/listing scraping + hypothesis generation
│   ├── sponsored_content_analysis.py  # LinkedIn sponsored content analysis helpers
│   └── gdrive.py                   # Google Drive upload helper
├── scripts/
│   ├── dry_run.py                  # End-to-end dry run (no Sheets/LinkedIn writes)
│   ├── post_weekly_reports.py      # Cron entry point: generates + posts both weekly reports
│   ├── generate_experiment_creatives.py  # Batch creative generator from experiment_queue.json
│   └── test_nano_banana_litellm.py # Manual LiteLLM connectivity test
├── data/
│   ├── dry_run_outputs/            # PNGs from dry_run.py runs (gitignored)
│   ├── experiment_outputs/         # PNGs from generate_experiment_creatives.py
│   ├── test_outputs/               # PNGs from ad hoc tests
│   ├── creative_vision_cache.json  # Claude Vision thumbnail cache for campaign_feedback_agent
│   └── competitor_hypotheses.json  # Latest competitor intel output (consumed by weekly reports)
├── .planning/
│   └── codebase/                   # Architecture docs (this directory)
└── venv/                           # Python 3.13 virtual environment
```

## Directory Purposes

**`src/` — Core library:**
- Purpose: All pipeline logic. Imported by `main.py` and all scripts.
- Contains: Data access clients, statistical analysis, creative generation, LinkedIn API, reporting
- Key design: Each module is independently importable. No circular imports except the deliberate local import of `_feature_to_facet` in `src/linkedin_urn.py` to avoid circular dependency with `src/analysis.py`.
- `__pycache__/` is present and committed (not gitignored for CPython 3.13 bytecode)

**`scripts/` — Executable entry points:**
- Purpose: Runnable scripts that compose `src/` modules for specific workflows
- All scripts set `PYTHONPATH=.` before running (see usage docstrings); they load `.env` explicitly at top of file
- Each script's `if __name__ == "__main__"` block handles argparse
- Not importable as a package (no `__init__.py`)

**`data/` — Runtime artifacts:**
- Purpose: Local storage for generated images and persisted agent state
- `dry_run_outputs/`: Named `dry_{stg_id_fragment}_{angle}.png` — e.g. `dry_2026041612017_A.png`
- `experiment_outputs/`: Named by brief — e.g. `exp_deepanshu_enUS_angleA.png`
- JSON files are read/written at runtime; not committed (except `creative_vision_cache.json` which is tracked)

## Key File Locations

**Entry Points:**
- `main.py`: Full pipeline orchestrator — `python main.py --mode launch|monitor [--dry-run]`
- `scripts/dry_run.py`: Non-destructive test run — `PYTHONPATH=. python scripts/dry_run.py --flow-id <id>`
- `scripts/post_weekly_reports.py`: Weekly cron script — `PYTHONPATH=. python scripts/post_weekly_reports.py`
- `scripts/generate_experiment_creatives.py`: Batch creative generation from experiment queue

**Configuration:**
- `config.py`: All constants and env var defaults — the single source of truth for tunable parameters. Import-time evaluation: all values are set when `config` is imported. Any module that needs a config value imports `config` and reads `config.CONSTANT`.
- `.env`: Runtime secrets — never read by code directly (python-dotenv loads it). `main.py` and each script call `load_dotenv()` at the top.
- `credentials.json`: Google service account key — path configured as `config.GOOGLE_CREDENTIALS`

**Data Access:**
- `src/redash_db.py`: Active data client. Contains all SQL as module-level string constants. `RedashClient` is instantiated in `main.py` as `snowflake = RedashClient()` (variable name preserved from legacy).
- `src/snowflake_db.py`: Legacy direct Snowflake connector — same SQL, same public interface. Not used in the active pipeline; kept for reference.
- `src/sheets.py`: Google Sheets client — `SheetsClient` handles both the `TRIGGERS_SHEET_ID` (campaign workflow) and `URN_SHEET_ID` (URN mappings). `COL` dict maps column names to 0-based indices for `Triggers 2`.

**Analysis Pipeline:**
- `src/features.py`: Feature engineering — `engineer_features()`, `build_frequency_maps()`, `binary_features()`
- `src/analysis.py`: Stage A + Stage B — `Cohort` dataclass, `stage_a()`, `stage_b()`, `_feature_to_facet()`
- `src/stage_c.py`: Stage C — `stage_c()` — also has a `__main__` block for standalone CLI testing
- `src/linkedin_urn.py`: URN resolution — `UrnResolver`, `_col_to_human()`, `FACET_TAB_MAP`, `FACET_API_NAME`

**Creative Generation:**
- `src/figma_creative.py`: `classify_tg()`, `FigmaCreativeClient`, `build_copy_variants()`, `apply_plugin_logic()`, TG palettes + illustration variant tables
- `src/midjourney_creative.py`: `generate_midjourney_creative()`, `compose_ad()`, `_generate_imagen()`, Angle gradient constants, expression modifiers per angle
- `src/inmail_copy_writer.py`: `build_inmail_variants()`, `InMailVariant` dataclass, `ANGLE_CONFIGS` dict, `VOCAB_RULES` string

**LinkedIn API:**
- `src/linkedin_api.py`: `LinkedInClient`, `refresh_access_token()`, `_build_targeting_criteria()`, `_build_restli_targeting()`

**Reporting and Monitoring:**
- `src/campaign_monitor.py`: `check_learning_phase()`, `score_campaigns()`, `discover_new_icps()`, `read_active_campaigns()`
- `src/campaign_feedback_agent.py`: Claude Vision analysis, experiment queue management, creative scoring
- `src/inmail_weekly_report.py`: Weekly InMail metrics SQL + report text builder
- `src/static_weekly_report.py`: Weekly image ad metrics SQL + report text builder
- `src/competitor_intel.py`: `CompetitorIntel` dataclass, web scraping of competitor ad libraries, `save_hypotheses()`, `load_pending_hypotheses()`

**Utilities:**
- `src/gdrive.py`: `upload_creative(file_path)` — uploads PNG to Google Drive Shared Drive folder, returns web view URL

## Naming Conventions

**Files:**
- `src/` modules: `snake_case.py` matching the primary class or concept — `redash_db.py`, `linkedin_api.py`, `campaign_monitor.py`
- Scripts: `snake_case.py` describing the action — `dry_run.py`, `post_weekly_reports.py`
- Data artifacts: `{type}_{identifier}_{angle}.png` — e.g. `dry_2026041612017_A.png`, `exp_deepanshu_enUS_angleA.png`

**Functions:**
- Public module functions: `snake_case` — `engineer_features`, `build_copy_variants`, `stage_a`
- Private helpers: `_snake_case` prefix — `_build_imagen_prompt`, `_feature_to_facet`, `_col_to_human`
- Class methods: `snake_case` — `fetch_screenings`, `resolve_cohort_rules`, `create_campaign`

**Variables and Column Names:**
- Binary feature columns: `{category}__{value}` using double underscore — `skills__python`, `job_titles_norm__data_scientist`
- Cohort STG IDs: `STG-{YYYYMMDD}-{5digit_random}` — e.g. `STG-20260420-47291`
- LinkedIn URNs: `urn:li:{type}:{id}` — preserved verbatim from API responses

**Constants:**
- Config constants: `UPPER_SNAKE_CASE` in `config.py` — `MIN_LIFT_PP`, `BEAM_CANDIDATES`, `LINKEDIN_API_BASE`
- Angle labels: single uppercase letter — `"A"`, `"B"`, `"C"`, `"F"` (Financial control)
- TG categories: `UPPER_SNAKE_CASE` string — `"DATA_ANALYST"`, `"ML_ENGINEER"`, `"MEDICAL"`, `"LANGUAGE"`, `"SOFTWARE_ENGINEER"`, `"GENERAL"`

## Where to Add New Code

**New analysis stage or cohort filter:**
- Implementation: `src/analysis.py` — add a `stage_d()` function following the existing pattern (takes `list[Cohort]`, returns `list[Cohort]`)
- Wire it in: `main.py` `_process_row()` between existing stage calls; also add to `src/campaign_monitor.py` `discover_new_icps()` which replicates the A+B pipeline

**New ad type (beyond image ad and InMail):**
- Add a new `ad_type` branch in `main.py` `_process_row()` alongside the `is_inmail` check
- Add a new `li_client.create_{type}_campaign()` and `li_client.create_{type}_ad()` method in `src/linkedin_api.py`
- Add a new copy generation function in `src/inmail_copy_writer.py` or a new module under `src/`

**New creative angle (beyond A/B/C/F):**
- InMail: add entry to `ANGLE_CONFIGS` dict in `src/inmail_copy_writer.py`; call `build_inmail_variants()` with `angle_keys=["F","A","B","C","new"]`
- Image ads: add an entry to `ANGLE_GRADIENTS` in `src/midjourney_creative.py` and a new `_ANGLE_EXPRESSIONS` entry

**New Snowflake query:**
- Add SQL as a module-level constant string in `src/redash_db.py` following the `{placeholder}` format convention
- Add a public method to `RedashClient` that calls `self._run_query(sql, label=...)`
- Mirror the same method in `src/snowflake_db.py` if the legacy path needs to stay in sync

**New competitor:**
- Add an entry to `COMPETITORS` dict in `src/competitor_intel.py`

**New configuration parameter:**
- Add to `config.py` with `os.getenv("VAR_NAME", "default")` pattern
- Document the env var in `.env` (template)

**New weekly report section:**
- Add SQL and aggregation logic to `src/inmail_weekly_report.py` or `src/static_weekly_report.py`
- The report text string is returned by `run_weekly_report()`; the caller in `scripts/post_weekly_reports.py` handles Slack delivery

**Utilities and shared helpers:**
- Shared helpers with no external dependencies: add to the most relevant existing module (e.g., statistical helpers in `src/analysis.py`, LinkedIn formatting helpers in `src/linkedin_api.py`)
- New external service client: create `src/{service}.py` following the `RedashClient` / `LinkedInClient` pattern

## Special Directories

**`data/`:**
- Purpose: Runtime-generated artifacts (images, cached JSON state)
- Generated: Yes — created by pipeline runs
- Committed: Partially — `creative_vision_cache.json` and `competitor_hypotheses.json` are tracked; PNG output subdirectories are gitignored

**`data/dry_run_outputs/`:**
- Purpose: PNGs generated by `scripts/dry_run.py`
- Generated: Yes
- Committed: No (gitignored; the PNGs in the repo listing are from in-progress sessions)

**`data/experiment_outputs/`:**
- Purpose: Challenger creative PNGs from `scripts/generate_experiment_creatives.py`
- Generated: Yes
- Committed: No

**`.planning/codebase/`:**
- Purpose: Architecture documentation consumed by planning and execution agents
- Generated: By mapping agents
- Committed: Yes

**`venv/`:**
- Purpose: Python 3.13 virtual environment
- Generated: Yes
- Committed: No

## Config Flow

`config.py` is a pure module — it reads all env vars at import time using `os.getenv("VAR", "default")`. There are no lazy-evaluated properties. This means:
- `load_dotenv()` must be called before `import config` (or before any module that imports `config`)
- All scripts call `load_dotenv(Path(__file__).parent.parent / ".env")` at the very top
- `main.py` calls `load_dotenv()` (without path argument, finds `.env` in cwd) before any other imports

Config priority resolution (documented in `main.py` `run_launch()`):
1. Google Sheets Config tab — runtime override by operator
2. `LINKEDIN_ACCESS_TOKEN` environment variable
3. `LINKEDIN_TOKEN` environment variable
4. `config.LINKEDIN_TOKEN` constant (set from env at import time)

Analysis thresholds all live in `config.py` and are referenced by name throughout `src/analysis.py` and `src/stage_c.py` — changing a threshold requires only a `config.py` edit or the corresponding env var.

---

*Structure analysis: 2026-04-20*

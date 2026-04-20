# Architecture

**Analysis Date:** 2026-04-20

## Pattern Overview

**Overall:** Linear data pipeline with a feedback loop — screening data from Snowflake flows through three statistical analysis stages, emerges as a set of LinkedIn-ready audience cohorts, generates ad creatives, and publishes to the LinkedIn Marketing API. A separate monitor loop reads performance data and pauses underperformers or queues new cohort discovery.

**Key Characteristics:**
- SQL-first feature engineering: all feature extraction (skills, titles, degree, experience band, country) is done by Snowflake via `LATERAL FLATTEN` CTEs before Python ever sees the data. Python's `engineer_features()` in `src/features.py` only type-converts the already-computed columns.
- Cohort-driven targeting: the unit of work is a `Cohort` dataclass (defined in `src/analysis.py`) — a named set of `(feature, value)` rules with associated statistical metadata (lift_pp, p_value, score). Every downstream step operates on cohorts, not raw rows.
- Branch at ad_type: after Stage C, the pipeline forks into an image ad path (Figma clone → Gemini fallback → LinkedIn Sponsored Content) or an InMail path (LiteLLM copy generation → LinkedIn Message Ad).
- Google Sheets as control plane: the `Triggers 2` tab in `TRIGGERS_SHEET_ID` is the human-facing interface. An operator adds a row with `flow_id`, `figma_file`, `figma_node`, `ad_type`, and sets `tg_status=PENDING`. `main.py` polls this sheet on each run.
- Dry-run flag threads through: every write to Sheets and every LinkedIn API call is gated on `dry_run=True/False`. The `scripts/dry_run.py` script is an independent end-to-end exerciser that never writes.

## Layers

**Data Layer (Snowflake via Redash):**
- Purpose: Provide resume screening data — one row per contributor-screening pair with pre-aggregated features
- Location: `src/redash_db.py`, SQL strings defined inline as module-level constants (`RESUME_SQL`, `PASS_RATES_SQL`, `PROJECT_FLOW_LOOKUP_SQL`, `JOB_POST_SQL`)
- Contains: `RedashClient` class, 4-step Redash polling pattern (create → trigger → poll → fetch)
- Depends on: `config.REDASH_URL`, `config.REDASH_API_KEY`, `config.REDASH_DATA_SOURCE_ID`
- Used by: `main.py` (`_process_row`), `src/campaign_monitor.py` (`discover_new_icps`, `get_pass_rates_from_snowflake`), `scripts/dry_run.py`
- Note: `src/snowflake_db.py` is the legacy direct connector; `RedashClient` is the active client and is a drop-in replacement with identical public method signatures

**Feature Engineering Layer:**
- Purpose: Convert semicolon-delimited SQL output strings into Python lists and binary indicator columns
- Location: `src/features.py`
- Contains: `engineer_features()`, `build_frequency_maps()`, `binary_features()`
- Pattern: `binary_features()` uses `pd.get_dummies` + `explode` + `groupby.max` — fully vectorized, no `iterrows`. Binary column naming convention: `skills__python`, `job_titles_norm__data_scientist`, `highest_degree_level__Masters`, `experience_band__5-7`
- Depends on: pandas only (no external services)
- Used by: `main.py`, `src/campaign_monitor.py` (discover_new_icps), `scripts/dry_run.py`

**Analysis Layer — Stage A:**
- Purpose: Discover high-lift audience segments via univariate tests + beam search
- Location: `src/analysis.py`, function `stage_a()`
- Contains: `SignalResult`, `Cohort` dataclasses; `two_prop_z_test()`, `segment_stats()`, `passes_thresholds()`
- Algorithm:
  1. Compute global baseline pass rate
  2. Univariate two-proportion z-test on every binary column (p < 0.05, min lift 3pp, min n=30, min passes=10, min pass rate 5%)
  3. Beam search over top-20 accepted univariates: generate 2-way combos (requiring +1pp incremental lift over best parent), then 3-way combos over top-10 two-way cohorts (same +1pp gate)
  4. Apply diversity multiplier: penalise cohorts whose primary facet/features are already in the selected set (1.0 / 0.7 / 0.4)
- Output: up to `config.BEAM_CANDIDATES` (12) `Cohort` objects sorted by `lift_pp * log10(n)` score
- Depends on: `pandas`, `scipy.stats`, `numpy`, `config`

**Analysis Layer — Stage B:**
- Purpose: Country-level directional validation — ensures the lift signal is not driven by a single geography
- Location: `src/analysis.py`, function `stage_b()`
- Algorithm: For each country with ≥75 observations, compute per-country lift. Mark `validated=True` if same-sign, lift > -5pp, and lift ≥ 50% of global lift. Attaches `cohort.country_results` dict.
- Output: Same cohorts list, with `country_results` populated. No cohorts are dropped — Stage B is informational.

**Analysis Layer — Stage C:**
- Purpose: LinkedIn audience-size validation and greedy cohort selection with a uniqueness gate
- Location: `src/stage_c.py`
- Algorithm:
  1. Resolve each cohort's rules to LinkedIn URNs via `UrnResolver`
  2. Call `LinkedInClient.get_audience_count()` — hard-stop on 401/403 auth errors
  3. Reject cohorts with audience < `config.AUDIENCE_SIZE_MIN` (50,000 default)
  4. Greedy selection: sort by audience size descending; add a cohort only if ≥ `MIN_UNIQUE_AUDIENCE_PCT` (80%) of its (facet, urn) pairs are new; stop at `MAX_CAMPAIGNS` (5)
- Output: final `selected: list[Cohort]` with `audience_size`, `unique_pct`, `intersection_score` populated

**URN Resolution Layer:**
- Purpose: Fuzzy-match human-readable feature values (e.g. "python") to LinkedIn targeting URNs
- Location: `src/linkedin_urn.py`
- Contains: `UrnResolver` class with in-memory `_cache` per tab; `_col_to_human()` to strip column prefixes; `FACET_TAB_MAP` and `FACET_API_NAME` dicts
- Algorithm: `rapidfuzz.process.extractOne` with `fuzz.WRatio` scorer; threshold configurable via `config.URN_FUZZY_MATCH_THRESHOLD` (0.85)
- Data source: Google Sheets `URN_SHEET_ID` — separate spreadsheet with tabs named `Skills`, `Titles`, `FieldsOfStudy`, `Degrees`, `ProfileLocations`, `Industries`
- Depends on: `src/sheets.py` (passed in constructor), `rapidfuzz`, `config`

**Creative Layer — Copy Generation:**
- Purpose: Generate 3 angle variants (Expertise / Earnings / Flexibility) of headline, subheadline, photo_subject, and CTA from cohort signals
- Location: `src/figma_creative.py`, function `build_copy_variants()`
- Uses: LiteLLM proxy (OpenAI-compatible client pointed at `config.LITELLM_BASE_URL`) with `config.LITELLM_MODEL` (default `anthropic/claude-sonnet-4-6`)
- Input: `cohort.name`, `cohort.rules` (converted to human labels via `_col_to_human`), optional Figma text layer map
- Output: `[{angle, angleLabel, headline, subheadline, cta, photo_subject, tgLabel, layerUpdates}, ...]`

**Creative Layer — Image Generation:**
- Purpose: Generate a composed 1200×1200 ad PNG from a Gemini background photo
- Location: `src/midjourney_creative.py` (file named after legacy Midjourney path; actually calls Gemini)
- Pipeline: `_build_imagen_prompt(photo_subject, angle)` → `_generate_imagen()` (LiteLLM `/images/generations` endpoint, model `config.GEMINI_IMAGE_MODEL`) → `compose_ad()` (PIL: crop, gradient overlay, text, bottom strip)
- Gradient: Left-side soft wash — angle-specific color pair (A: pink+blue, B: orange+blue, C: pink+green); numpy-computed radial falloff
- Fallback: direct Google Gemini API if LiteLLM unavailable
- Output: `Path` to temp PNG file

**Creative Layer — Figma Clone Path:**
- Purpose: Clone an existing Figma design template, apply cohort-specific text/color updates, export as PNG
- Location: `src/figma_creative.py`, `FigmaCreativeClient`, `apply_plugin_logic()`
- Uses: Figma REST API (`GET /v1/files/{key}/nodes`, `GET /v1/images/{key}`), Anthropic Claude API (via `anthropic` SDK) for plugin execution logic via MCP
- Falls back to Gemini path on any exception

**LinkedIn Publication Layer:**
- Purpose: Create campaign group, campaign, upload image, create DSC post, attach creative (image ad path) or create InMail content + creative (InMail path)
- Location: `src/linkedin_api.py`, `LinkedInClient`
- Auth: Bearer token with auto-refresh on 401 using `LINKEDIN_REFRESH_TOKEN` + `LINKEDIN_CLIENT_ID` + `LINKEDIN_CLIENT_SECRET`; writes new token back to `.env`
- Image ad flow: `initializeUpload` → PUT binary → create DSC post (`/rest/posts`, `lifecycleState=DRAFT`) → create creative referencing post URN
- InMail flow: `POST /v2/adInMailContents` → create creative referencing content URN
- Targeting format: `_build_targeting_criteria()` produces `{"include": {"and": [{"or": {facetUrn: [urn, ...]}}]}}` for campaign creation; `_build_restli_targeting()` produces Rest.li string for audience counts

**Control Plane Layer (Google Sheets):**
- Purpose: Human-facing configuration, queue management, and result logging
- Location: `src/sheets.py`, `SheetsClient`
- Sheets: `TRIGGERS_SHEET_ID` (`Triggers 2`, `Config`, `Creatives`, `Monitor` tabs); `URN_SHEET_ID` (URN mapping tabs)
- Column layout for `Triggers 2`: A=date, B=flow_id, C=tg_status, D=master_campaign, E=location, F=figma_file, G=figma_node, H=stg_id, I=stg_name, J=targeting_facet, K=targeting_criteria, L=li_status, M=li_campaign_id, N=error_detail, O=ad_type

## Data Flow

**Launch Pipeline (happy path — image ad):**

1. `main.py run_launch()` reads `PENDING` rows from `Triggers 2` via `SheetsClient.read_pending_rows()`
2. Config (LinkedIn token, Figma creds, etc.) resolved from Config tab → env vars → `config.py` constants, in that priority order
3. `RedashClient.fetch_screenings(flow_id, config_name)` executes `RESUME_SQL` via Redash polling; returns DataFrame with pre-aggregated feature columns
4. `engineer_features(df_raw)` converts semicolon strings to Python lists and numeric types
5. `build_frequency_maps(df, min_freq=5)` counts occurrences of each skill/title/field/accreditation value
6. `binary_features(df, freqs)` creates one binary column per frequent value
7. `stage_a(df_bin, bin_cols)` runs univariates + beam search → up to 12 `Cohort` objects
8. `stage_b(df_bin, cohorts_a)` adds `country_results` metadata to each cohort
9. `stage_c(cohorts_b, urn_resolver, li_client)` resolves URNs, checks audience sizes, greedy-selects ≤5 cohorts
10. `SheetsClient.write_cohorts()` writes `stg_id`, `stg_name`, `targeting_criteria_json` to `Triggers 2`, sets `tg_status=Completed`
11. For each cohort (rotating angle A→B→C): `build_copy_variants(cohort, layer_map)` → LiteLLM generates headline/subheadline/photo_subject
12. `generate_midjourney_creative(variant, photo_subject)` → Gemini image → PIL compose → temp PNG
13. `li_client.create_campaign_group()` → `li_client.create_campaign()` → `li_client.upload_image()` → `li_client.create_image_ad()`
14. `SheetsClient.write_creative()` logs creative URN to `Creatives` tab

**Launch Pipeline (InMail path):**

Steps 1–10 are identical. At step 11, `ad_type == "INMAIL"` branches to `_process_inmail_campaigns()`:
1. `classify_tg(cohort.name, cohort.rules)` → regex classification into DATA_ANALYST / ML_ENGINEER / MEDICAL / LANGUAGE / SOFTWARE_ENGINEER / GENERAL
2. `build_inmail_variants(tg_cat, cohort, claude_key)` → LiteLLM generates SUBJECT / BODY / CTA_LABEL for angles F/A/B/C
3. `li_client.create_inmail_campaign()` → `li_client.create_inmail_ad()`

**Monitor Loop:**

1. `run_monitor()` reads rows with `li_status` starting with `"Created:"` from `Triggers 2`
2. `check_learning_phase(li_client, campaign_ids)` — batch GET `/rest/adCampaigns`; checks `servingStatuses` and `runningStatus`
3. For graduated campaigns: `get_pass_rates_from_snowflake()` runs `PASS_RATES_SQL` via Redash
4. `score_campaigns()` — verdict KEEP / PAUSE / TEST_NEW based on `cohort_avg * (1 - 0.20)` threshold
5. `pause_campaign()` — PATCH `status=PAUSED` for underperformers
6. `discover_new_icps()` — re-runs Stages A+B for flows with pauses; returns cohorts with < 50% feature overlap with existing campaigns; queues as new `PENDING` rows

**Weekly Report Loop:**

`scripts/post_weekly_reports.py` (cron: Monday 3:30 AM UTC):
1. `src/inmail_weekly_report.run_weekly_report()` — queries `VIEW.LINKEDIN_CREATIVE_COSTS` for last 7 days; ranks angle variants by OR; surfaces competitor hypotheses from `data/competitor_hypotheses.json`
2. `src/static_weekly_report.run_weekly_report()` — similar for image ad campaigns
3. Each report posted to Slack via `config.SLACK_WEBHOOK_URL`

**State Management:**
- Primary state: `Triggers 2` Google Sheet — status columns (`tg_status`, `li_status`, `li_campaign_id`) are the source of truth
- Cohort targeting state: serialized to JSON in column K (`targeting_criteria_json`) as `[{feature, value, lift_pp}, ...]`
- Token state: LinkedIn access token written back to `.env` on auto-refresh
- Competitor hypotheses: cached to `data/competitor_hypotheses.json` by `src/competitor_intel.py`
- Experiment queue: `data/experiment_queue.json` managed by `src/campaign_feedback_agent.py`
- Creative vision cache: `data/creative_vision_cache.json` — base64 thumbnails for Claude Vision analysis

## Key Abstractions

**Cohort:**
- Purpose: The primary unit of work — a statistically validated audience segment defined by 1-3 feature rules
- Location: `src/analysis.py`, `@dataclass class Cohort`
- Fields: `name`, `rules: list[tuple]`, `n`, `passes`, `pass_rate`, `lift_pp`, `p_value`, `score`, `facet_strength`, `country_results`, `audience_size`, `intersection_score`, `unique_pct`
- Dynamic attributes added by `main.py`: `_stg_id`, `_stg_name`, `_facet`, `_criteria` (not in the dataclass — set as ad-hoc attributes)

**RedashClient:**
- Purpose: Drop-in replacement for `SnowflakeClient` — executes Snowflake SQL via Redash REST API with async polling
- Location: `src/redash_db.py`
- Pattern: create ad-hoc query → POST to trigger → poll `GET /api/jobs/{job_id}` every 4 seconds (max 60 polls) → fetch `query_result_id`
- SQL strings are module-level constants: `RESUME_SQL`, `PASS_RATES_SQL`, `PROJECT_FLOW_LOOKUP_SQL`, `JOB_POST_SQL`

**UrnResolver:**
- Purpose: Bridge between internal binary column names and LinkedIn targeting URN strings
- Location: `src/linkedin_urn.py`
- Pattern: lazy-load URN tabs from Google Sheets into `self._cache`; rapidfuzz WRatio fuzzy match; threshold 0.85

**LinkedInClient:**
- Purpose: Encapsulates all LinkedIn Marketing API calls with auto-refresh and Rest.li encoding
- Location: `src/linkedin_api.py`
- Key methods: `get_audience_count`, `create_campaign_group`, `create_campaign`, `upload_image`, `create_image_ad`, `create_inmail_campaign`, `create_inmail_ad`

## Entry Points

**`main.py`:**
- Location: `/Users/pranavpatre/outlier-campaign-agent/main.py`
- Triggers: Manual run or scheduler; two modes via `--mode launch|monitor`; `--dry-run` flag; `--log-level`
- Responsibilities: Reads config + PENDING rows, runs full analysis pipeline per row, creates LinkedIn campaigns

**`scripts/dry_run.py`:**
- Location: `/Users/pranavpatre/outlier-campaign-agent/scripts/dry_run.py`
- Triggers: Manual; accepts `--flow-id` or `--project-id`; `--skip-creatives` to stop before image generation
- Responsibilities: Runs all stages 0-8 without writing to Sheets or LinkedIn; saves PNGs to `data/dry_run_outputs/`; prints stage-by-stage diagnostic output

**`scripts/post_weekly_reports.py`:**
- Location: `/Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py`
- Triggers: Cron `30 3 * * 1` (Monday 3:30 AM UTC)
- Responsibilities: Generates InMail + static weekly performance reports; posts each to Slack via webhook

**`scripts/generate_experiment_creatives.py`:**
- Location: `/Users/pranavpatre/outlier-campaign-agent/scripts/generate_experiment_creatives.py`
- Triggers: Manual; reads `data/experiment_queue.json` for briefs; generates challenger PNGs and optionally uploads to Drive

## Error Handling

**Strategy:** Partial-failure tolerance at the campaign level — exceptions for one cohort are caught and logged; the loop continues for remaining cohorts. Hard stops (`RuntimeError` raised → caught by `main.py` → `raise` re-raises) are reserved for auth failures only.

**Patterns:**
- Stage C auth error: `RuntimeError("LinkedIn Audience Counts API blocked...")` propagates to `main.py` which re-raises, aborting the entire flow for that row
- Creative failures: caught with `log.warning` — campaign is created without a creative rather than abandoned
- LinkedIn 401: `LinkedInClient._req()` automatically refreshes the token and retries once; writes new token to `.env`
- LiteLLM copy gen failure: falls back to hardcoded default variants in `src/inmail_copy_writer.py`
- Gemini image gen failure: `log.warning`; Figma path tried first, Gemini is already the fallback
- Redash timeout: `TimeoutError` raised after 60 polls (~4 minutes)

## Cross-Cutting Concerns

**Logging:** `logging.basicConfig` configured in `main.py` and each script entry point. Format: `%(asctime)s %(levelname)-7s %(name)s — %(message)s`. Each module creates its own `log = logging.getLogger(__name__)`. No structured logging or external log aggregation.

**Validation:** Statistical thresholds in `config.py` (`MIN_SAMPLE_INTERNAL=30`, `MIN_ABSOLUTE_PASSES=10`, `MIN_LIFT_PP=3.0`, `MIN_PASS_RATE_FLOOR=5.0`, `p < 0.05`). LinkedIn audience threshold: `AUDIENCE_SIZE_MIN=50,000`. URN fuzzy match threshold: `URN_FUZZY_MATCH_THRESHOLD=0.85`.

**Authentication:** All credentials flow from `.env` → `config.py` module-level reads. Google Sheets uses a service account JSON file at `GOOGLE_CREDENTIALS` path. LinkedIn token auto-refreshes on 401.

**Configuration Priority (LinkedIn token example):** Config tab (Google Sheets) → `LINKEDIN_ACCESS_TOKEN` env var → `LINKEDIN_TOKEN` env var → `config.LINKEDIN_TOKEN` constant. This pattern is used consistently across `run_launch()` and `run_monitor()`.

---

*Architecture analysis: 2026-04-20*

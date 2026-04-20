# Codebase Concerns

**Analysis Date:** 2026-04-20

---

## Critical Bugs

### `classify_tg` Used But Not Imported in `main.py`

- Issue: `classify_tg` is called at lines 377, 392, and 480 of `main.py` — in `_process_inmail_campaigns()` and `_retry_li_campaign()` — but it is never imported. The import block at lines 28–32 pulls only `FigmaCreativeClient`, `build_copy_variants`, and `apply_plugin_logic` from `src/figma_creative.py`.
- Files: `main.py` (lines 28–32, 377, 392, 480), `src/figma_creative.py` (line 57)
- Trigger: Any InMail campaign run (`ad_type == "INMAIL"`) or any retry of an InMail row raises `NameError: name 'classify_tg' is not defined` at runtime.
- Fix: Add `classify_tg` to the import on line 28 of `main.py`:
  ```python
  from src.figma_creative import (
      FigmaCreativeClient,
      build_copy_variants,
      apply_plugin_logic,
      classify_tg,
  )
  ```

---

## LinkedIn API Blockers

### `LINKEDIN_MEMBER_URN` Not Set — `create_image_ad` Hard-Fails

- Issue: `create_image_ad()` in `src/linkedin_api.py` (line 359–365) raises `RuntimeError` if `config.LINKEDIN_MEMBER_URN` is empty. The env var defaults to `""` in `config.py` (line 43). Creating Direct Sponsored Content (DSC) posts requires the public profile URN of whoever authorized the OAuth token.
- Files: `config.py` (line 43), `src/linkedin_api.py` (lines 359–365)
- Impact: Every image ad campaign creation attempt silently skips the creative attach step. The campaign is created (paid) but has no creative attached — it cannot run.
- Fix: Set `LINKEDIN_MEMBER_URN=urn:li:person:<id>` in `.env`. Find the `<id>` portion at `linkedin.com/in/<id>` for the account that owns the OAuth token.

### `audienceCounts` API Blocked — Requires MDP Approval

- Issue: The LinkedIn Audience Counts API (`GET /rest/audienceCounts?q=targetingCriteriaV2`) returns HTTP 400 or 403 without Marketing Developer Platform (MDP) approval. Stage C in `src/stage_c.py` calls this API for every cohort.
- Files: `src/stage_c.py` (lines 48–68), `src/linkedin_api.py` (lines 112–142), `scripts/dry_run.py` (line 15, 169)
- Impact: Stage C currently bypasses audience size validation entirely in the dry-run path (falls back to Stage B top cohorts). In the main pipeline, any auth error raises `RuntimeError` and halts the entire flow for that row.
- Current workaround: `scripts/dry_run.py` catches Stage C exceptions and uses Stage B top cohorts. Main pipeline (`main.py`) has no such fallback — it propagates the `RuntimeError` up.
- Fix: Apply for LinkedIn MDP access. Until approved, add a graceful Stage C bypass in `main.py` matching the `scripts/dry_run.py` pattern.

### LinkedIn Token Expiry — April 2026

- Issue: The access token stored in `.env` has `expires_at=1781441848` (Unix timestamp), which corresponds to approximately **April 2026** — near or at the current date. A refresh token is present (`LINKEDIN_REFRESH_TOKEN`) and the auto-refresh logic in `src/linkedin_api.py` (lines 24–71, 95–108) will attempt to exchange it on the first 401 response.
- Files: `src/linkedin_api.py` (lines 95–108, `_refresh_and_retry`), `config.py` (lines 45–52)
- Impact: If the access token has expired and `LINKEDIN_CLIENT_ID` / `LINKEDIN_CLIENT_SECRET` are not set in `.env`, the auto-refresh fails with `RuntimeError` and all LinkedIn API calls are blocked.
- Fix: Ensure `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, and `LINKEDIN_REFRESH_TOKEN` are all populated in `.env`. Run the refresh manually once to confirm the flow works and write a fresh token to `.env`.

### InMail Content API Uses Deprecated `v2` Endpoint

- Issue: `create_inmail_ad()` in `src/linkedin_api.py` (line 315) calls `https://api.linkedin.com/v2/adInMailContents` — the legacy v2 endpoint — with a hardcoded comment "adInMailContents requires an older API version (202502)". The rest of the client uses the `rest` base with version header `202510`. The v2 endpoint may be deprecated or return unexpected errors as LinkedIn migrates to REST.
- Files: `src/linkedin_api.py` (lines 298–319)
- Impact: InMail creative creation may break without notice if LinkedIn deprecates the v2 endpoint.

---

## Stale / Hardcoded Config Values

### `SCREENING_END_DATE` Hardcoded to `2025-12-31`

- Issue: `config.py` line 31 defaults `SCREENING_END_DATE` to `"2025-12-31"`. This date is in the past as of April 2026. If `fetch_screenings()` is called without an explicit `end_date` argument, it will only query data through 2025-12-31, silently missing all 2026 screenings.
- Files: `config.py` (line 31), `src/redash_db.py` (lines 315–330)
- Impact: Any production pipeline run that relies on the default date range will produce stale or incomplete cohorts without any error or warning. The `RESUME_SQL` in `redash_db.py` uses `config.SCREENING_END_DATE` directly as the `{end_date}` substitution parameter.
- Fix: Either update the default to a dynamic value (e.g. `datetime.utcnow().date().isoformat()`), or always pass `end_date` explicitly at the call site. Never rely on the hardcoded default.

### `REDASH_DATA_SOURCE_ID` Hardcoded to `30`

- Issue: `config.py` line 18: `int(os.getenv("REDASH_DATA_SOURCE_ID", "30"))`. The comment says `30 = _Snowflake (GenAI Ops)`. If the Redash data source ID ever changes (reprovisioning, migration), all queries will silently route to the wrong data source.
- Files: `config.py` (line 18)
- Fix: Document the source ID in the Config Google Sheet tab, or enforce verification at startup.

### Google Sheets IDs Hardcoded in Source

- Issue: `TRIGGERS_SHEET_ID = "1yM2bA_gbdki-IKSf14ddyshsKYh2FATEiv1CtdfYDQY"` and `URN_SHEET_ID = "10S5QhB46l-f_ncR7fEGnkT9E2QIAs07RBuoLnahJhW0"` are literal strings in `config.py` (lines 8–9). They are not read from environment variables.
- Files: `config.py` (lines 8–9)
- Impact: Changing either sheet (e.g. re-creating after accidental deletion, migration) requires a code change and redeploy. The URN sheet is also noted as needing to be shared with the service account separately.

### `SLACK_REPORT_USER` Hardcoded to Personal User ID

- Issue: `config.py` line 102: `SLACK_REPORT_USER = "U095J930UEL"   # pranav.patre@scale.com`. This Slack user ID is hardcoded and not read from any environment variable. This value is not used anywhere in the current source (no references found in `src/`), but if it were picked up by a future agent or report module it would hardcode routing to a specific individual.
- Files: `config.py` (line 102)

### Campaign Budget and Bid Hardcoded

- Issue: `create_campaign()` defaults `daily_budget_cents=5000` ($50/day) and has a hardcoded `"unitCost": {"currencyCode": "USD", "amount": "10.00"}`. `create_inmail_campaign()` has a hardcoded `"unitCost": {"amount": "0.40"}`. Neither parameter is exposed through config or the sheet trigger row.
- Files: `src/linkedin_api.py` (lines 171, 185, 246, 261)
- Impact: Budget changes require a code edit. No per-flow budget customization is possible.

### `LINKEDIN_AD_ACCOUNT_ID` Defaults to Hardcoded Value

- Issue: `config.py` line 36: `LINKEDIN_AD_ACCOUNT_ID = os.getenv("LINKEDIN_AD_ACCOUNT_ID", "510956407")`. The default `"510956407"` is the literal production ad account ID. If `.env` is absent or misconfigured, the agent silently operates against the production account without any warning.
- Files: `config.py` (line 36)
- Impact: A misconfigured dev/test environment would create real campaigns in production.

---

## Config Import-Time Evaluation

### `config.py` Evaluated at Module Import — `load_dotenv()` in `main.py` Is Too Late

- Issue: `config.py` calls `os.getenv(...)` at module import time (all 30+ variable assignments at the top level). `main.py` calls `load_dotenv()` at line 18, but then immediately imports `config` at line 20 — meaning `config` is evaluated before `load_dotenv()` runs only if something triggers `config` import earlier. The specific risk is any module that calls `import config` at module level and that module is imported before `load_dotenv()` in `main.py`.
- Files: `config.py` (all lines), `main.py` (lines 18–20), `src/campaign_feedback_agent.py` (lines 41–47 — this file calls `load_dotenv()` before `import config`, correctly), `src/stage_c.py` (lines 123–127 — CLI `__main__` path does this correctly too)
- Impact: If a module is imported before `load_dotenv()` resolves (e.g. in test runners, importers that eagerly load submodules), `config.*` values will be empty strings or defaults, not the `.env` values. `REDASH_API_KEY` being empty raises `ValueError` at `RedashClient.__init__()`.
- Fix: Call `load_dotenv()` in `config.py` itself (first line), or restructure `config.py` to use lazy property accessors.

---

## Disabled Features

### Google Drive Upload Disabled (`GDRIVE_ENABLED=false`)

- Issue: `config.py` line 81: `GDRIVE_ENABLED = os.getenv("GDRIVE_ENABLED", "false").lower() == "true"`. The default is `false` and the comment on lines 78–80 explains that the target folder must be a Google Workspace Shared Drive with the service account added as Content Manager. The current `GDRIVE_FOLDER_ID = "1TrpyIOq6hS4eGAc0sYUIJom4MAanbnm4"` points to what is likely a personal Drive folder.
- Files: `config.py` (lines 78–82), `src/gdrive.py`, `main.py` (lines 328–334)
- Impact: Generated PNG creatives are saved to local temp files only. They are not persisted or shareable. The Drive upload block in `main.py` is completely skipped.
- Fix: Create a Google Workspace Shared Drive, add `outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com` as Content Manager, update `GDRIVE_FOLDER_ID` to the new folder ID, and set `GDRIVE_ENABLED=true` in `.env`. Instructions are in `src/gdrive.py` lines 1–22.

### Midjourney MCP Not Active

- Issue: `config.py` lines 92–97 configure `MIDJOURNEY_API_TOKEN` and `MIDJOURNEY_MCP_URL` under a comment "pending MCP — not yet active". The function `generate_midjourney_creative()` in `src/midjourney_creative.py` is actually a Gemini/Imagen pipeline — it does not use Midjourney at all. The `mj_token` parameter passed through `main.py` is accepted via `**_kwargs` and ignored.
- Files: `config.py` (lines 92–97), `src/midjourney_creative.py` (lines 436–441), `main.py` (lines 66, 233, 284–287)
- Impact: The `mj_token` guard in `main.py` (`has_mj = bool(mj_token and claude_key)`) means image creative generation never runs if `MIDJOURNEY_API_TOKEN` is not set — even though the actual pipeline only requires `LITELLM_API_KEY`. The check is against the wrong variable.

---

## Missing / Placeholder Configuration

### `SLACK_WEBHOOK_URL` Not Populated

- Issue: `SLACK_WEBHOOK_URL` defaults to `""` in `config.py` (line 101). `scripts/post_weekly_reports.py` (lines 31–33) checks for the empty string and logs an error without posting. Weekly reports are generated but never reach Slack.
- Files: `config.py` (line 101), `scripts/post_weekly_reports.py` (lines 29–43)
- Impact: Automated Monday 9 AM IST weekly report delivery is silently broken. The script runs, builds the report, logs "SLACK_WEBHOOK_URL not set in .env — cannot post report", and exits without error.

### `LINKEDIN_INMAIL_SENDER_URN` Not Set

- Issue: `config.py` line 39 defaults to `""`. `_process_inmail_campaigns()` in `main.py` (line 365–370) logs an error and returns without creating any InMail campaigns if this is empty. The value must be a `urn:li:person:...` for a LinkedIn member connected to the ad account.
- Files: `config.py` (line 39), `main.py` (lines 365–370)
- Impact: All InMail ad type rows are silently skipped.

### `LITELLM_API_KEY` Not Set Raises at Redash Init

- Issue: If `LITELLM_API_KEY` is absent, copy generation (InMail, image ad variants) silently falls back to hardcoded placeholder variants in `src/inmail_copy_writer.py`. However, `RedashClient.__init__()` raises `ValueError("REDASH_API_KEY is not set")` — a hard crash — if `REDASH_API_KEY` is absent.
- Files: `src/redash_db.py` (lines 308–311), `src/inmail_copy_writer.py` (lines 210–219)

---

## Security Considerations

### SQL Built Via String Formatting With Minimal Escaping

- Issue: All SQL in `src/redash_db.py` is constructed via Python `.format()` with only single-quote escaping in `_esc()` (line 488–490). Parameters include `signup_flow_id`, `config_name`, `project_id`, `flow_id`, and `since_date` — all originating from Google Sheets user input.
- Files: `src/redash_db.py` (lines 323–400, 488–490)
- Impact: A malicious value in a Sheets cell could inject SQL executed against the Snowflake production database via Redash. The `_esc()` function only replaces single quotes with doubled quotes — it does not handle all SQL injection vectors.
- Recommendation: Parameterize queries using Redash's `parameters` field, or at minimum add input validation for expected formats (hex IDs, date strings) before substitution.

### Token Written to `.env` In-Process

- Issue: `refresh_access_token()` in `src/linkedin_api.py` (lines 64–71) writes the new `LINKEDIN_ACCESS_TOKEN` directly to `.env` by reading the file, regex-substituting, and writing it back. This is fragile: concurrent process runs could corrupt `.env`, and `.env` is not gitignored.
- Files: `src/linkedin_api.py` (lines 64–71)

### OCR Pipeline Blocked by Missing LinkedIn Scope

- Issue: `src/sponsored_content_analysis.py` (lines 8–13) documents that image URL retrieval for Direct Sponsored Content requires `r_organization_social` scope or similar, which the current token does not have. The OCR creative analysis in `src/campaign_feedback_agent.py` relies on downloading creative images via LinkedIn API.
- Files: `src/sponsored_content_analysis.py` (lines 8–13), `src/campaign_feedback_agent.py`
- Impact: Visual creative analysis (Claude Vision scoring) is blocked for all DSC creatives. The feedback agent scores campaigns on metrics alone.

---

## Fragile Areas

### `generate_midjourney_creative` Raises if `photo_subject` Missing

- Issue: `generate_midjourney_creative()` in `src/midjourney_creative.py` (lines 464–469) raises `RuntimeError("photo_subject is required...")` if neither `photo_subject` argument nor `variant["photo_subject"]` key is present. The call site in `main.py` (line 284) passes `variant=selected_variant` but does not pass `photo_subject`. The `photo_subject` key must come from the LLM-generated variant dict produced by `build_copy_variants()` in `src/figma_creative.py`. If copy generation fails (LLM error, parse failure), `selected_variant` will be an empty dict `{}`, causing this raise.
- Files: `main.py` (lines 282–294), `src/midjourney_creative.py` (lines 464–469), `src/figma_creative.py`
- Current mitigation: The `generate_midjourney_creative` call is wrapped in `try/except` (main.py line 283), so a failure logs a warning and the campaign is created without a creative.

### Sheet Row Index Off-By-One Risk in `write_cohorts`

- Issue: `write_cohorts()` in `src/sheets.py` (lines 161–163) reads `all_rows[sheet_row - 1]` to copy A–G columns when appending additional cohort rows. If any previous `append_row` call shifted row numbering (gspread does not refresh row indices automatically), the `sheet_row` stored in the cohort object may no longer match the actual sheet row.
- Files: `src/sheets.py` (lines 161–163)
- Impact: Additional cohort rows (2nd through 5th) may copy A–G data from the wrong source row.

### URN Fuzzy Match Produces False Positives at Default Threshold

- Issue: `UrnResolver.resolve()` in `src/linkedin_urn.py` uses `fuzz.WRatio` with a default threshold of `0.85` (85 points out of 100). Cohort feature values from the Snowflake resume data are free-form strings (e.g. job titles like `"sr. data scientist"`, `"data science lead"`). A sufficiently close but wrong match (e.g. `"data analyst"` matching `"data scientist"` at 86) would silently target the wrong LinkedIn audience.
- Files: `src/linkedin_urn.py` (lines 86–102), `config.py` (line 72)
- Recommendation: Log all matches with scores during a run. Review borderline matches (85–92 range) manually.

### `_retry_li_campaign` Uses Angle A Only

- Issue: `_retry_li_campaign()` in `main.py` (line 490) always uses `variants[0]` for InMail retries: `variant = variants[0]  # default to Angle A for retries`. This means retried rows always re-send angle A regardless of the original angle intent.
- Files: `main.py` (line 490)

---

## Test Coverage Gaps

### No Tests Exist

- Issue: There are no `*.test.py` or `*.spec.py` files in the repository. No test runner configuration (`pytest.ini`, `setup.cfg [tool:pytest]`, etc.) is present.
- Files: Entire `src/` directory
- Risk: All business-logic paths — Stage A statistics, Stage B beam search, URN resolution fuzzy matching, targeting criteria construction, InMail copy parsing — are untested. The `classify_tg` import bug described above would have been caught by any import-level unit test.
- Priority: High — the pipeline writes directly to production LinkedIn campaigns and a Google Sheet that controls budget spend.

---

## Performance Bottlenecks

### Redash Polling Loop — 4-Minute Max Wait Per Query

- Issue: `_trigger_and_poll()` in `src/redash_db.py` polls every 4 seconds for up to 60 attempts (4 minutes max). Each `_process_row()` call executes one Redash query. For a launch run with 5 PENDING rows and no caching, this adds up to 5 sequential Redash round-trips before any LinkedIn API work begins.
- Files: `src/redash_db.py` (lines 434–470, constants `_POLL_INTERVAL=4`, `_MAX_POLLS=60`)
- Note: Redash does return cached results immediately if the same query was recently run (lines 444–448), so repeat runs on the same flow are fast.

### No Connection Pooling or Rate-Limit Handling for LinkedIn API

- Issue: `LinkedInClient` in `src/linkedin_api.py` makes one campaign group create, one campaign create, one image upload, and one creative create per cohort — all synchronous, sequential. No rate-limit back-off beyond the single 401 auto-refresh is implemented. LinkedIn Marketing API has per-app rate limits.
- Files: `src/linkedin_api.py`

---

*Concerns audit: 2026-04-20*

# External Integrations

**Analysis Date:** 2026-04-20

## APIs & External Services

---

### LinkedIn Marketing API

**Purpose:** Create and manage ad campaigns (Sponsored Content + Sponsored InMail), upload image assets, validate audience sizes, check campaign learning phase status.

**SDK/Client:** Raw `requests` via `src/linkedin_api.py` (`LinkedInClient` class)

**Auth:**
- Bearer token: `LINKEDIN_ACCESS_TOKEN` env var (also checked as `LINKEDIN_TOKEN`)
- Auto-refresh on 401: exchanges `LINKEDIN_REFRESH_TOKEN` + `LINKEDIN_CLIENT_ID` + `LINKEDIN_CLIENT_SECRET` for a new token via `https://www.linkedin.com/oauth/v2/accessToken`; writes the refreshed token back to `.env` automatically
- Required OAuth scopes: `rw_ads`, `w_member_social` (for DSC post creation)

**API Version:** `202510` (sent as `LinkedIn-Version` header on all REST calls)

**Base URL:** `https://api.linkedin.com/rest`

**Key endpoints used:**
| Endpoint | Method | Purpose |
|----------|--------|---------|
| `GET /rest/audienceCounts?q=targetingCriteriaV2` | GET | Validate estimated audience size for a facet set (Stage C) |
| `POST /adAccounts/{id}/adCampaignGroups` | POST | Create campaign group |
| `POST /adAccounts/{id}/adCampaigns` | POST | Create Sponsored Content campaign or Sponsored InMail campaign |
| `POST /images?action=initializeUpload` → PUT to upload URL | POST + PUT | Two-step image asset upload |
| `POST /rest/posts` | POST | Create Direct Sponsored Content (DSC) post (dark post) |
| `POST /adAccounts/{id}/creatives` | POST | Attach creative to campaign |
| `POST https://api.linkedin.com/v2/adInMailContents` | POST | Create InMail content object (uses v2, not REST, due to API versioning constraint) |
| `GET /adCampaigns?ids=...` | GET | Fetch campaign status for learning phase check |

**Known quirks:**
- `audienceCounts` requires `q=targetingCriteriaV2` (NOT `targetingCriteria`). The param value must be Rest.li-encoded string format, not JSON. `requests` URL-encodes it exactly once.
- DSC image ad creation is a two-step process: create a DRAFT post via `/rest/posts` with `lifecycleState=DRAFT` and `adContext.dscAdAccount`, then reference the resulting share URN in the creative payload.
- Campaign IDs are extracted from the `x-linkedin-id` or `x-restli-id` response header (not the body).
- InMail content creation (`adInMailContents`) uses the older `/v2` endpoint, not the `202510` REST API.
- `LINKEDIN_MEMBER_URN` must be the `urn:li:person:...` of the OAuth token holder — required as `author` on DSC posts.
- `LINKEDIN_AD_ACCOUNT_ID` is hardcoded default `510956407` in `config.py`.

---

### LiteLLM Proxy (Scale internal)

**Purpose:** Unified gateway for all LLM inference (chat completions) and image generation (Gemini/Imagen). Avoids direct model API calls from the agent.

**SDK/Client:** `openai.OpenAI` with `base_url` overridden (`src/inmail_copy_writer.py`, `src/figma_creative.py`, `src/campaign_feedback_agent.py`)

**Auth:** Bearer token `LITELLM_API_KEY` env var

**Base URL:** `https://litellm-proxy.ml-serving-internal.scale.com/v1` (`LITELLM_BASE_URL`)

**Key endpoints used:**
| Endpoint | Purpose |
|----------|---------|
| `POST /chat/completions` | InMail copy generation, ad copy variants, weekly report synthesis, creative feedback agent |
| `POST /images/generations` | Gemini/Imagen background photo generation for static ads |

**Default model:** `anthropic/claude-sonnet-4-6` (`LITELLM_MODEL`)

**Image model:** `gemini/gemini-2.5-flash-image` (`GEMINI_IMAGE_MODEL`)
- Other confirmed working: `gemini/imagen-4.0-generate-001`, `gemini/imagen-4.0-fast-generate-001`, `gemini/gemini-3.1-flash-image-preview`
- Image response format: `data[0].b64_json` (base64-encoded PNG)

**Known quirks:**
- Image endpoint returns `b64_json` in the OpenAI images response shape, not a URL
- `config.py` `LITELLM_BASE_URL` is evaluated at import time — `load_dotenv()` must be called before importing `config`
- Falls back to direct Google Gemini API (`GEMINI_API_KEY`) if LiteLLM returns non-200 on image generation

---

### Redash (Scale internal)

**Purpose:** Execute Snowflake SQL queries via REST API without a direct Snowflake connection. Primary data source for all analytics in the active pipeline.

**SDK/Client:** Raw `requests` in `src/redash_db.py` (`RedashClient` class)

**Auth:** `REDASH_API_KEY` env var, passed as `?api_key=` query parameter on all endpoints

**Base URL:** `https://redash.scale.com` (`REDASH_URL`)

**Data source ID:** `30` (Snowflake GenAI Ops, `REDASH_DATA_SOURCE_ID`)

**Key endpoints used (3-step polling pattern):**
| Step | Endpoint | Purpose |
|------|----------|---------|
| 1 | `POST /api/queries` | Create ad-hoc query |
| 2 | `POST /api/queries/{id}/results` | Trigger execution → returns job ID or cached result |
| 3 | `GET /api/jobs/{job_id}` (polled) | Wait for job status=3 (done) or status=4 (error) |
| 4 | `GET /api/query_results/{qrid}` | Fetch final rows |

**SQL queries executed:**
- `RESUME_SQL` — joins `PUBLIC.RESUMEMETADATAS`, `PUBLIC.GROWTHRESUMESCREENINGRESULTS`, `PUBLIC.RESUMESCREENINGCONFIGS`, `VIEW.APPLICATION_CONVERSION` to produce ICP screening data per signup flow
- `PROJECT_FLOW_LOOKUP_SQL` — resolves Outlier `project_id` → dominant `signup_flow_id` + `config_name`
- `JOB_POST_SQL` — fetches job description from `public.jobposts`
- `PASS_RATES_SQL` — compute UTM-level pass rates for a flow
- Weekly report SQLs in `src/inmail_weekly_report.py` and `src/static_weekly_report.py` — query `VIEW.LINKEDIN_CREATIVE_COSTS`, `PC_FIVETRAN_DB.LINKEDIN_ADS.*`

**Polling config:** 4-second interval, max 60 polls (~4 minute timeout)

**Known quirks:**
- Redash returns row dicts with UPPERCASE keys; `RedashClient._fetch_result()` normalises all column names to lowercase
- Cached query results are returned immediately (no job ID) — `_trigger_and_poll` handles both paths
- `RedashClient` is a drop-in replacement for `SnowflakeClient` — same public method signatures

---

### Snowflake (direct connector — legacy)

**Purpose:** Original direct data source, now replaced by Redash in the active pipeline. Still present in `src/snowflake_db.py` as fallback.

**SDK/Client:** `snowflake-connector-python[pandas]` in `src/snowflake_db.py` (`SnowflakeClient` class)

**Auth:** Username/password via `SNOWFLAKE_USER` + `SNOWFLAKE_PASSWORD` env vars

**Config:** `SNOWFLAKE_ACCOUNT`, `SNOWFLAKE_WAREHOUSE` (`COMPUTE_WH`), `SNOWFLAKE_DATABASE` (`OUTLIER_PROD`), `SNOWFLAKE_SCHEMA` (`PUBLIC`), `SNOWFLAKE_ROLE` env vars

**Status:** Not used in `main.py` — `RedashClient` is instantiated instead. `snowflake_db.py` kept for reference.

---

### Google Sheets

**Purpose:** Two sheets drive the pipeline: (1) Triggers sheet (campaign launch queue, cohort results, config), (2) URN sheet (LinkedIn URN lookup tables for facets).

**SDK/Client:** `gspread>=6.0.0` in `src/sheets.py` (`SheetsClient` class)

**Auth:** Service account JSON file at path `GOOGLE_CREDENTIALS` env var (default: `credentials.json` at project root). Scopes: `spreadsheets`, `drive.readonly`.

**Sheets:**
| Sheet | ID (in `config.py`) | Purpose |
|-------|---------------------|---------|
| Triggers sheet | `TRIGGERS_SHEET_ID` = `1yM2bA_gbdki-IKSf14ddyshsKYh2FATEiv1CtdfYDQY` | Campaign launch queue (Triggers 2 tab), campaign results (Creatives tab), config (Config tab) |
| URN sheet | `URN_SHEET_ID` = `10S5QhB46l-f_ncR7fEGnkT9E2QIAs07RBuoLnahJhW0` | LinkedIn URN mapping tabs: Skills, Titles, FieldsOfStudy, Degrees, ProfileLocations, Industries |

**Key operations:**
- `read_pending_rows()` — reads "Triggers 2" tab for rows where column C = "PENDING"
- `write_cohorts()` — writes cohort targeting data (stg_id, stg_name, facet, criteria) back to columns H-L
- `update_li_campaign_id()` — marks LI campaign as Created + writes campaign ID to column M
- `read_urn_tab(tab_name)` — reads a URN mapping tab, returns list of `{name, urn}` dicts
- `read_config()` — reads Config tab as `{key: value}` dict; values can override env vars at runtime

**Column layout ("Triggers 2" tab):**
`A=date, B=flow_id, C=tg_status, D=master_campaign, E=location, F=figma_file, G=figma_node, H=stg_id, I=stg_name, J=targeting_facet, K=targeting_criteria, L=li_status, M=li_campaign_id, N=error_detail, O=ad_type`

**Known quirks:**
- Service account must be shared on the sheet with at least Viewer access; URN sheet requires explicit share separately
- `GDRIVE_ENABLED` must be `true` and target folder must be a Google Workspace Shared Drive (not personal Drive) — personal Drive raises `storageQuotaExceeded` 403 for service accounts

---

### Google Drive

**Purpose:** Upload generated ad creative PNGs to a shared Drive folder for team access.

**SDK/Client:** `google-api-python-client` Drive v3 in `src/gdrive.py` (`upload_creative()`)

**Auth:** Same service account credentials as Sheets (`GOOGLE_CREDENTIALS`), scope: `https://www.googleapis.com/auth/drive` (full drive, not `drive.file` — required for Shared Drive member access)

**Config:** `GDRIVE_FOLDER_ID` env var (default: `1TrpyIOq6hS4eGAc0sYUIJom4MAanbnm4`); `GDRIVE_ENABLED` must be `"true"` to activate (default `false`)

**Behavior:** Uploads PNG, then creates an `anyone/reader` permission making the file publicly viewable via link. Returns `webViewLink`.

**Key API calls:** `files().create(supportsAllDrives=True)`, `permissions().create(supportsAllDrives=True)`

**Status:** Disabled by default (`GDRIVE_ENABLED=false`). Must be enabled once a Shared Drive is configured and service account added as Content Manager.

---

### Figma REST API + MCP

**Purpose:** Two integration paths — (1) REST API for reading file node structure and exporting PNGs, (2) Claude MCP (`use_figma` tool) for applying plugin logic to clone and modify frames.

**SDK/Client:** Raw `requests` via `src/figma_creative.py` (`FigmaCreativeClient` class) for REST; `anthropic` SDK with MCP tool use for plugin automation

**Auth:**
- REST: `X-Figma-Token: {FIGMA_TOKEN}` header
- MCP: Figma MCP server running locally at `http://127.0.0.1:3845/sse` (`MCP_FIGMA_URL`)

**Base URL (REST):** `https://api.figma.com/v1`

**Key REST endpoints:**
| Endpoint | Purpose |
|----------|---------|
| `GET /v1/files/{fileKey}/nodes?ids={nodeId}` | Fetch text layer map from base template node |
| `GET /v1/images/{fileKey}?ids={nodeIds}&format=png&scale=2.0` | Export cloned frames as PNG |

**Key MCP operations:**
- `apply_plugin_logic()` in `src/figma_creative.py` — calls Claude with `use_figma` MCP tool to clone the base frame 3×, apply text layer updates, and call `customizeDesign(variantIndex)` per copy variant

---

### Gemini / Google Generative AI (direct fallback)

**Purpose:** Background photo generation for static ad creatives when LiteLLM proxy is unavailable.

**SDK/Client:** Raw `requests` in `src/midjourney_creative.py` (`_generate_imagen()`)

**Auth:** `GEMINI_API_KEY` env var, passed as `?key=` query parameter

**Endpoint:** `POST https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash-image:generateContent`

**Response format:** `candidates[0].content.parts[N].inlineData.data` (base64 PNG)

**Status:** Fallback only. Primary path is LiteLLM proxy (`LITELLM_API_KEY` checked first).

---

### Slack

**Purpose:** Post weekly performance reports (InMail + Static) to a Slack DM.

**SDK/Client:** Raw `requests` (Incoming Webhook), no Slack SDK

**Auth:** `SLACK_WEBHOOK_URL` env var (Incoming Webhook URL)

**Config:** `SLACK_REPORT_USER = "U095J930UEL"` (pranav.patre@scale.com) hardcoded in `config.py`; `SLACK_BOT_TOKEN` also present but reports use webhook only

**Key behavior:**
- Posts report text in 3000-char chunks to avoid Slack block limits
- Called from `scripts/post_weekly_reports.py` on a Monday 3:30 AM UTC cron
- The weekly report modules (`inmail_weekly_report.py`, `static_weekly_report.py`) only build text — the Slack post is the caller's responsibility

---

### Competitor Intelligence (web scraping)

**Purpose:** Scrape competitor ad creatives, task listings, Trustpilot reviews, and Reddit sentiment to generate copy angle hypotheses that feed into InMail and static ad generation.

**SDK/Client:** `playwright` (browser automation) + `beautifulsoup4` (HTML parsing) in `src/competitor_intel.py`

**Competitors tracked:** DataAnnotation, Mercor, Alignerr, Micro1, Appen, Surge AI, Turing AI, Handshake, Remotasks

**Data sources scraped:**
- Meta Ads Library (`facebook.com/ads/library`) — competitor ad creatives
- Competitor task/opportunity pages (e.g., `work.turing.com/jobs`, `app.dataannotation.tech/workers/projects`)
- Trustpilot review pages per `trustpilot_slug`
- Reddit threads via keyword search

**Persistence:** Hypotheses saved to `data/competitor_hypotheses.json`; loaded by `inmail_weekly_report.py` and `static_weekly_report.py` to surface angle ideas

---

## Data Storage

**Databases:**
- Snowflake (`OUTLIER_PROD` database, `PUBLIC` schema) — accessed exclusively via Redash REST API in active pipeline
- Fivetran-synced LinkedIn Ads tables in `PC_FIVETRAN_DB.LINKEDIN_ADS.*` — queried via Redash

**File Storage:**
- Local filesystem: `data/dry_run_outputs/` (PNG creatives), `data/experiment_queue.json`, `data/creative_vision_cache.json`, `data/competitor_hypotheses.json`
- Google Drive: optional upload via `src/gdrive.py` (disabled by default)

**Caching:**
- `data/creative_vision_cache.json` — caches Claude Vision analysis of LinkedIn creative images to avoid re-analyzing unchanged creatives
- `data/competitor_hypotheses.json` — caches latest competitor intel hypotheses between weekly runs

---

## Authentication & Identity

**Auth Provider:** No identity provider — all auth is service-account or API token based

**Implementation:**
- LinkedIn: OAuth 2.0 Bearer token with refresh token rotation; token auto-written back to `.env` on refresh
- Google services: Service account JSON (`credentials.json`) — single account used for Sheets, Drive, and (if needed) other Google APIs
- All other integrations: API key via env var

---

## Monitoring & Observability

**Error Tracking:** None (no Sentry, Datadog, etc.)

**Logs:** Python `logging` module, `basicConfig` in `main.py` at INFO level with format `%(asctime)s %(levelname)-7s %(name)s — %(message)s`. Log to stdout only.

---

## CI/CD & Deployment

**Hosting:** Local machine / manual execution only — no containerization or deployment config found

**CI Pipeline:** None

**Scheduled jobs:** Cron for weekly reports (`scripts/post_weekly_reports.py`): `30 3 * * 1`

---

## Environment Configuration

**Required env vars (minimum for full pipeline):**
- `LINKEDIN_ACCESS_TOKEN` — LinkedIn Bearer token
- `LINKEDIN_REFRESH_TOKEN` — for auto-refresh
- `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET` — OAuth app credentials
- `LINKEDIN_AD_ACCOUNT_ID` — defaults to `510956407`
- `LINKEDIN_MEMBER_URN` — `urn:li:person:...` of token holder
- `LINKEDIN_INMAIL_SENDER_URN` — sender for InMail campaigns
- `LITELLM_API_KEY` — Scale LiteLLM proxy key
- `LITELLM_BASE_URL` — defaults to `https://litellm-proxy.ml-serving-internal.scale.com/v1`
- `LITELLM_MODEL` — defaults to `anthropic/claude-sonnet-4-6`
- `GEMINI_IMAGE_MODEL` — defaults to `gemini/gemini-2.5-flash-image`
- `REDASH_API_KEY` — Redash REST API key
- `REDASH_DATA_SOURCE_ID` — defaults to `30`
- `GOOGLE_CREDENTIALS` — path to service account JSON (defaults to `credentials.json`)
- `SLACK_WEBHOOK_URL` — Slack Incoming Webhook for reports
- `FIGMA_TOKEN` — Figma personal access token

**Optional:**
- `GDRIVE_ENABLED=true` + `GDRIVE_FOLDER_ID` — to enable Drive upload
- `GEMINI_API_KEY` — direct Gemini fallback (not needed if LiteLLM works)
- `MIDJOURNEY_API_TOKEN`, `MIDJOURNEY_MCP_URL` — pending Midjourney integration (not yet active)
- `SNOWFLAKE_*` vars — legacy direct connector (not used in active pipeline)

**Secrets location:** `.env` file at project root; also readable from Google Sheets "Config" tab at runtime via `SheetsClient.read_config()`

---

## Webhooks & Callbacks

**Incoming:** None

**Outgoing:**
- Slack Incoming Webhook (`SLACK_WEBHOOK_URL`) — weekly report posts
- LinkedIn token refresh callback (`https://www.linkedin.com/oauth/v2/accessToken`) — triggered automatically on 401

---

*Integration audit: 2026-04-20*

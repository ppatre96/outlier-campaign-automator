# Technology Stack

**Analysis Date:** 2026-04-20

## Languages

**Primary:**
- Python 3.13 — all agent logic, API clients, data analysis, image composition

**Secondary:**
- SQL (Snowflake dialect) — analytics queries embedded as string constants in `src/redash_db.py` and `src/snowflake_db.py`

## Runtime

**Environment:**
- Python 3.13.7 (confirmed via `.python-version` file and `venv/lib/python3.13/`)

**Package Manager:**
- pip (standard) — no `pyproject.toml` or `setup.py`; dependencies listed flat in `requirements.txt`
- Lockfile: not present (no `requirements.lock` or `pip-lock`)

**Virtual Environment:**
- `venv/` at project root (Python 3.13 venv)

## Frameworks

**Core:**
- No web framework — this is a CLI agent, not a server. Entry point is `main.py` (argparse modes: `launch`, `monitor`).

**AI / LLM:**
- `openai>=1.30.0` — used as OpenAI-compatible client to call the LiteLLM proxy for all chat completions (`src/inmail_copy_writer.py`, `src/figma_creative.py`, `src/campaign_feedback_agent.py`)
- `anthropic>=0.40.0` — direct Anthropic SDK used in `src/figma_creative.py` for Figma plugin automation via `use_figma` MCP tool

**Data / Analytics:**
- `pandas>=2.0.0` — DataFrame return type for all Redash/Snowflake query results
- `numpy>=1.24.0` — gradient overlay math in `src/midjourney_creative.py` (`np.zeros`, `np.meshgrid`, `np.clip`)
- `scipy>=1.10.0` — statistical testing (`scipy.stats` two-proportion z-test) in `src/analysis.py`

**Image:**
- `Pillow>=10.0.0` — full ad compositing pipeline in `src/midjourney_creative.py` (crop, resize, gradient overlay, text rendering, PNG export); also used in `src/figma_creative.py` for dry-run PNG generation

**Scraping:**
- `playwright>=1.40.0` — browser automation for competitor intelligence (Meta Ads Library, Trustpilot) in `src/competitor_intel.py`
- `beautifulsoup4>=4.12.0` — HTML parsing of competitor task listing pages in `src/competitor_intel.py`

**Templating:**
- `jinja2>=3.1.0` — present in requirements but not observed being imported in current source; likely used in HTML creative generation or report templating

**Fuzzy Matching:**
- `rapidfuzz>=3.0.0` — fuzzy URN resolution in `src/linkedin_urn.py` (matches human-readable skill/title names to LinkedIn URNs)

**Testing:**
- No test framework configured (`pytest`, `unittest` not in `requirements.txt`)
- Manual test scripts in `scripts/` (e.g., `scripts/test_nano_banana_litellm.py`, `scripts/dry_run.py`)

**Build/Dev:**
- `python-dotenv>=1.0.0` — `.env` loading at startup in `main.py`, `src/campaign_feedback_agent.py`, `scripts/post_weekly_reports.py`

## Key Dependencies

**Critical:**
- `gspread>=6.0.0` — Google Sheets read/write (trigger rows, cohort results, URN maps) — `src/sheets.py`
- `google-auth>=2.0.0` + `google-auth-oauthlib>=1.0.0` — service account authentication for Sheets and Drive
- `google-api-python-client>=2.0.0` — Google Drive v3 API client in `src/gdrive.py`
- `snowflake-connector-python[pandas]>=3.0.0` — legacy direct Snowflake connector in `src/snowflake_db.py` (replaced in active pipeline by `RedashClient` but kept for fallback)
- `requests>=2.31.0` — all HTTP calls to LinkedIn REST API, Redash API, Figma REST API, Slack webhook, Gemini direct API

**Infrastructure:**
- `openai>=1.30.0` — LiteLLM proxy interface (not OpenAI directly; base_url overridden to `https://litellm-proxy.ml-serving-internal.scale.com/v1`)

## Configuration

**Environment:**
- All secrets loaded from `.env` at project root via `python-dotenv`
- `config.py` reads every secret with `os.getenv()` at module level — **import-time evaluation** means `config.py` must be imported only after `load_dotenv()` is called
- Key env vars required for full operation: `LINKEDIN_ACCESS_TOKEN`, `LINKEDIN_REFRESH_TOKEN`, `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, `LINKEDIN_AD_ACCOUNT_ID`, `LINKEDIN_MEMBER_URN`, `LINKEDIN_INMAIL_SENDER_URN`, `LITELLM_API_KEY`, `REDASH_API_KEY`, `GOOGLE_CREDENTIALS` (path to service account JSON), `SLACK_WEBHOOK_URL`, `GEMINI_IMAGE_MODEL`, `FIGMA_TOKEN`
- Optional overrides via Config tab in Google Sheets (read by `SheetsClient.read_config()`)

**Build:**
- No build step — pure Python, run directly with `PYTHONPATH=. python main.py`
- Cron schedule for weekly reports: `30 3 * * 1` (Monday 3:30 AM UTC = 9 AM IST), run via `scripts/post_weekly_reports.py`

## Platform Requirements

**Development:**
- macOS (font path `/System/Library/Fonts/Avenir Next.ttc` hardcoded in `src/midjourney_creative.py` with Linux fallbacks)
- Python 3.13
- Google service account credentials JSON file (`credentials.json` at project root by default)

**Production:**
- Same Python 3.13 environment; no containerization artifacts found
- Stateful local paths: `data/dry_run_outputs/`, `data/experiment_queue.json`, `data/creative_vision_cache.json`, `data/competitor_hypotheses.json`

---

*Stack analysis: 2026-04-20*

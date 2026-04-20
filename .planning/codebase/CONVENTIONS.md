# Coding Conventions

**Analysis Date:** 2026-04-20

## Naming Patterns

**Files:**
- Modules use `snake_case.py` throughout: `linkedin_api.py`, `inmail_copy_writer.py`, `redash_db.py`
- Scripts are `snake_case.py` in `scripts/`: `dry_run.py`, `post_weekly_reports.py`
- Entry point is flat: `main.py`, `config.py` at project root

**Functions:**
- Public functions use `snake_case`: `engineer_features`, `build_copy_variants`, `stage_a`, `stage_b`, `stage_c`
- Private helpers use `_snake_case` prefix: `_raise_for_status`, `_build_restli_targeting`, `_cohort_display_name`, `_now_ms`, `_esc`
- Helper constants prefixed with `_`: `_POLL_INTERVAL`, `_MAX_POLLS`, `_ENV_FILE`, `_TITLE_STOP`

**Variables / Local Names:**
- Short, idiomatic names for standard objects: `log` (not `logger`), `resp` (not `response`), `df` (not `dataframe`), `exc` (not `error` or `e`)
- Multi-word locals in `snake_case`: `li_token`, `urn_res`, `facet_urns`, `cohort_sheet_rows`
- Alignment padding with spaces is used in `config.py` for constant blocks — columns line up visually

**Classes:**
- `PascalCase`: `LinkedInClient`, `RedashClient`, `SheetsClient`, `UrnResolver`, `FigmaCreativeClient`, `InMailVariant`, `Cohort`, `SignalResult`

**Dataclasses:**
- `@dataclass` used for domain objects in `src/analysis.py` (`Cohort`, `SignalResult`) and `src/inmail_copy_writer.py` (`InMailVariant`)
- Field defaults use `field(default_factory=dict)` for mutable defaults

**Constants:**
- Module-level constants in `UPPER_SNAKE_CASE`: `FACET_TAB_MAP`, `ANGLE_CONFIGS`, `TG_PALETTES`, `COL`
- Dict-style column maps use `UPPER_SNAKE_CASE`: `COL` dict in `src/sheets.py`

## Code Style

**Formatting:**
- No automated formatter configured (no `.prettierrc`, no `pyproject.toml` with Black/Ruff settings, no `setup.cfg`)
- Consistent 4-space indentation throughout
- Trailing alignment spaces used in `config.py` constant blocks for readability
- Long strings broken with implicit concatenation in parentheses, not with `\`

**Linting:**
- No linter config files detected (no `.flake8`, no `pylintrc`, no `ruff.toml`)
- `# noqa: E402` used exactly once in `scripts/test_nano_banana_litellm.py` for post-`load_dotenv` imports

**Line Length:**
- No enforced limit; some lines in SQL strings go very long; Python logic lines are kept short

## Import Organization

**Order in source modules:**
1. Standard library: `logging`, `os`, `re`, `json`, `pathlib`, `dataclasses`, `typing`, `time`, `math`
2. Third-party: `requests`, `pandas`, `numpy`, `scipy`, `openai`, `anthropic`, `gspread`
3. Local project: `import config` then `from src.<module> import ...`

**`load_dotenv` placement (scripts only):**
- In `main.py` and scripts, `load_dotenv()` is called **before** `import config`
- This is the critical ordering requirement: `config.py` evaluates `os.getenv(...)` at import time — if `load_dotenv()` is called after `import config`, all env vars in config will be empty strings
- Pattern used in every script:
  ```python
  from dotenv import load_dotenv
  load_dotenv(Path(__file__).parent.parent / ".env")
  import config  # must come after load_dotenv
  ```
- `main.py` calls `load_dotenv()` with no path (uses CWD `.env`), then imports config

**`import config` style:**
- All modules do `import config` (not `from config import ...`) and reference values as `config.LINKEDIN_VERSION`, `config.REDASH_API_KEY`, etc.
- Exception: inside functions where config is needed lazily (e.g., `_retry_li_campaign` and `build_inmail_variants` do `import config` inside the function body to avoid import-time issues)

**Path Aliases:**
- None. All internal imports use `from src.<module> import ...` with `PYTHONPATH=.` set at runtime

## Config Access Patterns

**Pattern: `os.getenv` with inline default at import time**
All config values are evaluated once when `config.py` is first imported:
```python
REDASH_API_KEY         = os.getenv("REDASH_API_KEY", "")
REDASH_DATA_SOURCE_ID  = int(os.getenv("REDASH_DATA_SOURCE_ID", "30"))
LITELLM_API_KEY        = os.getenv("LITELLM_API_KEY", "")
LINKEDIN_TOKEN         = (
    os.getenv("LINKEDIN_ACCESS_TOKEN") or
    os.getenv("LINKEDIN_TOKEN", "")
)
```

**Gotcha — import-time eval:**
`config.py` has no functions — it is pure module-level assignment. Every `os.getenv()` call runs at the moment `import config` executes. If a process calls `load_dotenv()` after importing `config`, the env vars will not be reflected. This is why `load_dotenv()` **must** precede `import config` in every entry point.

**Validation at use-site, not in config:**
Config does not validate required values. Instead, individual modules validate at instantiation:
- `RedashClient.__init__` raises `ValueError("REDASH_API_KEY is not set")` if the key is empty
- `main.run_launch` checks `if not li_token:` and calls `sys.exit(1)`
- `LinkedInClient.create_image_ad` raises `RuntimeError` if `LINKEDIN_MEMBER_URN` is not set

**Runtime override from Google Sheets:**
`main.py` reads a Config tab from Google Sheets (`sheets.read_config()`) and prefers its values over env/config:
```python
li_token = (
    sheet_cfg.get("LINKEDIN_TOKEN") or
    os.getenv("LINKEDIN_ACCESS_TOKEN") or
    os.getenv("LINKEDIN_TOKEN") or
    config.LINKEDIN_TOKEN
)
```
Priority: Sheet Config tab > LINKEDIN_ACCESS_TOKEN env > LINKEDIN_TOKEN env > config module default.

## Error Handling

**Pattern: `_raise_for_status(resp, context)` wrapper**
`LinkedInClient` in `src/linkedin_api.py` defines a private helper instead of calling `resp.raise_for_status()` directly:
```python
def _raise_for_status(self, resp: requests.Response, context: str) -> None:
    if not resp.ok:
        log.error("%s failed %d: %s", context, resp.status_code, resp.text[:500])
        resp.raise_for_status()
```
This logs the full error body (truncated to 500 chars) before re-raising, so the log always contains the LinkedIn API error message. Called consistently across all `LinkedInClient` methods with a descriptive context string (`"createCampaign"`, `"initializeImageUpload"`, etc.).

**`resp.raise_for_status()` directly** is used in `RedashClient`, `FigmaCreativeClient._get`, and `competitor_intel.py` where there is no shared wrapper.

**Auto-refresh retry pattern (401 handling):**
`LinkedInClient._req` wraps every HTTP call:
```python
def _req(self, method: str, url: str, **kwargs) -> requests.Response:
    resp = self._session.request(method, url, **kwargs)
    if resp.status_code == 401 and config.LINKEDIN_REFRESH_TOKEN and config.LINKEDIN_CLIENT_ID:
        log.warning("LinkedIn 401 — attempting token refresh")
        resp = self._refresh_and_retry(method, url, **kwargs)
    return resp
```
- Retry is done exactly **once** on 401
- Requires both `LINKEDIN_REFRESH_TOKEN` and `LINKEDIN_CLIENT_ID` to be set; otherwise the 401 propagates up
- New token is written back to `.env` via `_update_env_token()` so subsequent process restarts pick it up

**Hard stop vs. best-effort distinction:**
- Hard stop (`raise RuntimeError`): auth failures in Stage C (`_is_auth_error`), missing required env vars, Redash job failures
- Best-effort (`except Exception as exc: log.warning(...)`): creative generation failures, Drive upload, LinkedIn creative attach, audience count per-cohort
- In `main.py`, per-row exceptions are caught and logged but do not halt the loop:
  ```python
  except Exception as exc:
      log.exception("Unexpected error for flow %s: %s", flow_id, exc)
  ```
  Only `RuntimeError` from `_process_row` is re-raised and halts the entire run.

**Redash polling timeout:**
`RedashClient._trigger_and_poll` polls up to `_MAX_POLLS = 60` times with `_POLL_INTERVAL = 4` seconds (~4 minutes total), then raises `TimeoutError`.

## Logging

**Setup (entry points only):**
`logging.basicConfig` is called in `main.py` and in scripts:
```python
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
```
Modules never call `basicConfig` — they only create a module-level logger:
```python
log = logging.getLogger(__name__)
```
This means logger names match module paths: `src.linkedin_api`, `src.redash_db`, etc.

**Level conventions:**
- `log.info(...)`: normal pipeline progress — every significant state transition is logged at INFO
- `log.warning(...)`: recoverable failures — creative generation failure, Drive upload failure, missing URNs, LinkedIn creative attach failure
- `log.error(...)`: hard failures — missing required token, API error responses before raising
- `log.debug(...)`: only in `RedashClient` internal polling (`_trigger_and_poll`, `_create_query`) and `stage_b` per-country validation
- `log.exception(...)`: used in `main.py` catch-all for unexpected exceptions (logs full traceback)

**Format string style:**
Always `%`-style formatting (never f-strings) in logging calls:
```python
log.info("Fetched %d screening rows", len(df))
log.error("%s failed %d: %s", context, resp.status_code, resp.text[:500])
```

**Dry-run logging:**
Operations that are skipped under `--dry-run` log at INFO with `[dry-run]` prefix:
```python
log.info("[dry-run] Would write %d cohorts", len(cohort_sheet_rows))
log.info("[dry-run] Skipping LinkedIn campaign creation")
```

## Type Hints Usage

**Return types:** Consistently annotated on all public and private functions in `src/`:
```python
def fetch_screenings(...) -> pd.DataFrame:
def resolve_project_to_flow(...) -> tuple[str, str] | None:
def _raise_for_status(...) -> None:
def stage_c(...) -> list[Cohort]:
```

**Parameter types:** Annotated on all function signatures. Union types use Python 3.10+ `X | Y` syntax (not `Optional[X]`):
```python
def upload_image(self, image_path: str | Path) -> str:
def fetch_screenings(self, signup_flow_id: str, config_name: str, start_date: str | None = None, ...) -> pd.DataFrame:
```

**Variables:** Type hints on local variables are rare — used only when needed to clarify complex types:
```python
facet_urns: dict[str, list[str]] = {}
sized: list[tuple[Cohort, dict, int]] = []
committed_urn_pairs: set[tuple[str, str]] = set()
```

**`Any` from typing:** Used in `src/features.py` for JSON parsing functions where input type is unknown:
```python
def _safe_json(val: Any) -> Any:
def extract_experience(job_experiences: Any) -> dict:
```

**Missing hints:** `main.py` helper functions (`_process_row`, `_process_inmail_campaigns`, `_retry_li_campaign`, `_queue_new_icps`) have no parameter type hints — parameters are untyped plain names.

## Module Organization Conventions

**Module structure pattern:**
Each `src/` module follows this order:
1. Module docstring explaining what it does
2. Standard library imports
3. Third-party imports
4. `import config`
5. `log = logging.getLogger(__name__)`
6. Module-level constants / lookup tables
7. Dataclasses or named-tuple definitions (if any)
8. Class definition(s) with methods grouped by sub-topic using `# ── Section ──` banners
9. Module-level helper functions (prefixed `_`)
10. Optional `if __name__ == "__main__":` block for standalone testing

**Section banners:**
Visual section separators use an em-dash style across the codebase:
```python
# ── Stage A ───────────────────────────────────────────────────────────────────
# ── Targeting helpers ─────────────────────────────────────────────────────────
# ── Utility ────────────────────────────────────────────────────────────────────
```

**SQL strings:**
Large SQL queries are defined as module-level constants in ALL_CAPS in `src/redash_db.py`:
`RESUME_SQL`, `PROJECT_FLOW_LOOKUP_SQL`, `PASS_RATES_SQL`, `JOB_POST_SQL`
They use `'{placeholder}'` style for Python `.format()` substitution.

**Lazy imports inside functions:**
Used sparingly when an import would create a circular dependency or when a heavy import is only needed in one rarely-called path:
```python
# In main.py _retry_li_campaign:
import json as _json
from dataclasses import dataclass, field as dc_field
# In main.py _process_row:
from src.gdrive import upload_creative
```

**`src/__init__.py`:**
Empty file — present only to make `src` a package. No re-exports.

## Function Design

**Size:**
Most functions are 10–50 lines. `main._process_row` (~150 lines) and `scripts/dry_run.run` (~200 lines) are the largest. Both handle multi-step pipeline flows where splitting would lose readability.

**Parameters:**
- Clients (`sheets`, `li_client`, `urn_res`) are passed as parameters, not accessed as module-level globals — enables swapping for testing
- `dry_run: bool = False` is the standard parameter for skipping writes
- Long parameter lists in `main.py` private functions are not `**kwargs`-collapsed — each parameter is explicit

**Return values:**
- Prefer returning domain objects (`Cohort`, `InMailVariant`) over raw dicts for structured data
- Return empty list `[]` (not `None`) on no-results cases
- Return `None` explicitly from functions that may find no result (e.g., `UrnResolver.resolve`)
- Tuple returns for compound results: `resolve_project_to_flow` returns `tuple[str, str] | None`

## Comments

**When to Comment:**
- Every module has a docstring listing what it does and the pipeline stages it implements
- Complex algorithmic logic is explained inline (e.g., beam search diversity multiplier in `analysis.py`)
- LinkedIn API quirks are documented at the call site with comments explaining why a non-obvious approach is used
- Data gotchas are documented: `NOTE:` comments on OCR pipeline status in `sponsored_content_analysis.py`, geo restriction caveats in `campaign_monitor.py` and `inmail_copy_writer.py`

**No docstrings on private helpers:**
Private functions (`_`, `__`) generally have no docstring; they have a one-line comment only if the logic is non-obvious. Public methods have docstrings.

---

*Convention analysis: 2026-04-20*

# Phase 1: Pipeline Integrity - Context

**Gathered:** 2026-04-21
**Status:** Ready for planning

<domain>
## Phase Boundary

Fix all silent-skip bugs so the full pipeline — screening fetch through creative generation and LinkedIn publish — runs to completion without NameErrors, stale-config data cutoffs, or uncaught RuntimeErrors. No new features. No Drive upload (Phase 2). No create_image_ad unblock (PARKED — external LinkedIn scope dependency).

</domain>

<decisions>
## Implementation Decisions

### D-01: Remove has_mj guard — creative generation always runs
**File:** `main.py` line 234
**Current broken code:**
```python
has_mj = bool(mj_token and claude_key)   # always False — neither variable is set
...
if png_path is None and has_mj and selected_variant:   # always skipped
    png_path = generate_midjourney_creative(...)
```
**Fix:** Remove `has_mj` entirely. Replace the condition with:
```python
if png_path is None and selected_variant:
    png_path = generate_midjourney_creative(...)
```
`generate_midjourney_creative()` is the Gemini LiteLLM pipeline — it does not need `mj_token` or `claude_key`. It only needs `LITELLM_API_KEY` which is already set in `.env`.

### D-02: Fix premature GEMINI_API_KEY check in midjourney_creative.py
**File:** `src/midjourney_creative.py` lines 458–472
**Current broken code:**
```python
api_key = gemini_api_key or config.GEMINI_API_KEY
if not api_key:
    raise RuntimeError("GEMINI_API_KEY is not set — add it to .env")
```
**Problem:** This raises before calling `_generate_imagen()`. `_generate_imagen()` uses LiteLLM (`LITELLM_API_KEY`) and only needs `GEMINI_API_KEY` as a direct-API fallback. Since `LITELLM_API_KEY` is set, `GEMINI_API_KEY` is not required.
**Fix:** Remove the early check. Let `_generate_imagen()` raise naturally if both LiteLLM and `GEMINI_API_KEY` are absent (already handled correctly inside `_generate_imagen()` at line 150).

### D-03: Remove InMail dry-run copy gate
**File:** `main.py` line 380
**Current broken code:**
```python
if claude_key:
    variants = build_inmail_variants(tg_cat, cohort, claude_key)
```
**Fix:** Remove the `if claude_key:` gate. Call unconditionally:
```python
variants = build_inmail_variants(tg_cat, cohort, claude_key)
```
`build_inmail_variants()` uses LiteLLM internally; the `claude_key` arg is passed but the function routes through `config.LITELLM_BASE_URL`. No gate on `ANTHROPIC_API_KEY` anywhere in this codebase.

### D-04: Fix SCREENING_END_DATE — both config.py and call site
**Files:** `config.py` line 31, `main.py` line 152
**Current broken code:**
```python
# config.py
SCREENING_END_DATE = os.getenv("SCREENING_END_DATE", "2025-12-31")  # stale default

# main.py
df_raw = snowflake.fetch_screenings(flow_id, config_name)  # uses stale default
```
**Fix A (config.py):** Change default to dynamic:
```python
from datetime import datetime
SCREENING_END_DATE = os.getenv("SCREENING_END_DATE", datetime.utcnow().date().isoformat())
```
**Fix B (main.py call site):** Always pass explicit end_date:
```python
from datetime import date
df_raw = snowflake.fetch_screenings(flow_id, config_name, end_date=date.today().isoformat())
```
`dry_run.py` already does this correctly (line 157–158) — match that pattern.

### D-05: Stage C graceful bypass in main.py
**File:** `main.py` lines 182–185
**Current broken code:**
```python
selected = stage_c(cohorts_b, urn_res, li_client)  # RuntimeError propagates up → aborts row
```
**Fix:** Wrap in try/except matching `dry_run.py`'s `_try_stage_c()` pattern:
```python
try:
    selected = stage_c(cohorts_b, urn_res, li_client)
except Exception as exc:
    log.warning("Stage C unavailable (%s) — falling back to Stage B top cohorts", exc)
    selected = cohorts_b[:config.MAX_CAMPAIGNS]
```
On MDP-blocked Stage C, the pipeline continues with Stage B top cohorts. Do not abort the row.

### D-06: LINKEDIN_INMAIL_SENDER_URN — set to Tuan's person URN for testing
**File:** `.env`
**Decision:** Set `LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0`
- `vYrY4QMQH0` is Tuan's LinkedIn internal person ID (discovered via `/rest/posts` 422 error response in prior session — member numeric ID 177861413 resolves to this person URN)
- If LinkedIn rejects this URN as InMail sender (not connected to ad account), fallback: query `GET /rest/adAccountUsers?q=byAdAccount&account=urn:li:sponsoredAccount:510956407` to list valid connected members
- This is for testing only; correct production sender should be confirmed with the account owner

### D-07: create_image_ad — PARKED (document blocker only)
**Requirements:** LI-01, LI-02
**Decision:** Do NOT plan to unblock `create_image_ad` in Phase 1.
**Blocker:** `LINKEDIN_MEMBER_URN` requires `urn:li:person:{internal_id}` of the OAuth token owner. Token owner identity cannot be determined without `r_liteprofile` or `rw_organization_admin` LinkedIn scope (neither currently granted).
**Plan task:** Ensure `create_image_ad` failure is logged clearly and does NOT crash the pipeline. The try/except wrapper at `main.py` line 337–348 already handles this — confirm it's in place and add a clear log message explaining the blocker.

### D-08: LinkedIn token auto-refresh — verify test
**File:** `src/linkedin_api.py` lines 64–71, 95–108
**Context:** `LINKEDIN_CLIENT_ID`, `LINKEDIN_CLIENT_SECRET`, `LINKEDIN_REFRESH_TOKEN` are confirmed set in `.env`. Token expires ~June 2026 (`expires_at=1781441848`).
**Plan task:** Trigger manual `li_client.refresh_access_token()` call. Confirm:
1. No RuntimeError raised
2. A new `LINKEDIN_ACCESS_TOKEN` is written back to `.env`
3. A subsequent API call using the new token succeeds (no 401)
Add a note in `README.md` under "LinkedIn Token Setup" documenting the expiry and re-auth steps.

### D-09: Dry-run success criteria
**The full dry-run is the Phase 1 acceptance test. Run:**
```bash
PYTHONPATH=. python scripts/dry_run.py --flow-id <real_flow_id>
```
All 5 observable outputs must appear:
1. Stage A: cohort list printed (at least 1 cohort)
2. Stage B: country validation output
3. Stage C: audience counts printed OR bypass log line (MDP blocked → graceful fallback)
4. Creative gen: PNG saved to `data/dry_run_outputs/` (after removing has_mj guard)
5. LinkedIn (dry-run log lines): campaign group, campaign, image upload, creative attach

**Also confirm:**
```bash
PYTHONPATH=. python main.py --mode launch --dry-run
```
Processes a PENDING row through creative generation without `mj_token` guard blocking it.

### Claude's Discretion
- Whether to remove `mj_token` and `claude_key` parameters from `_process_row()` signature entirely (they serve no purpose) or leave them as dead params for minimal diff
- How to structure the Stage C fallback (inline try/except vs. extracted `_try_stage_c()` helper matching dry_run.py)
- Whether to leave `has_figma` gated on `claude_key` (Figma clone is out of scope — keeping it gated is correct)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Pipeline entry points
- `main.py` — orchestrator: lines 56–143 (run_launch), 146–351 (_process_row), 356–421 (_process_inmail_campaigns), 423–537 (_retry_li_campaign)
- `scripts/dry_run.py` — reference implementation with correct patterns: lines 84–101 (_try_stage_c), 157–158 (end_date fix)

### Broken code locations
- `main.py:234` — `has_mj = bool(mj_token and claude_key)` (D-01)
- `main.py:283` — `if png_path is None and has_mj and selected_variant:` (D-01)
- `main.py:152` — `snowflake.fetch_screenings(flow_id, config_name)` — no end_date (D-04)
- `main.py:182` — `selected = stage_c(...)` — no try/except (D-05)
- `main.py:380` — `if claude_key:` gate on InMail copy (D-03)
- `src/midjourney_creative.py:458–472` — premature GEMINI_API_KEY check (D-02)

### Config
- `config.py:31` — `SCREENING_END_DATE = "2025-12-31"` (D-04)
- `config.py:43` — `LINKEDIN_MEMBER_URN` (D-07 context)
- `config.py:39` — `LINKEDIN_INMAIL_SENDER_URN` (D-06)
- `config.py:81–82` — `GDRIVE_ENABLED` (Phase 2 only — do not touch)

### Reference patterns
- `scripts/dry_run.py:84–101` — `_try_stage_c()` — the Stage C fallback pattern to replicate in main.py
- `scripts/dry_run.py:157–158` — correct `end_date=date.today().isoformat()` call site pattern

### LinkedIn API
- `src/linkedin_api.py:64–71` — `refresh_access_token()` writes new token to .env
- `src/linkedin_api.py:95–108` — `_refresh_and_retry()` — auto-refresh on 401
- `src/linkedin_api.py:337–365` — `create_image_ad()` — DSC post flow (PARKED blocker)

### Creative pipeline
- `src/midjourney_creative.py:116–164` — `_generate_imagen()` — LiteLLM primary, direct Gemini fallback
- `src/midjourney_creative.py:436–500` — `generate_midjourney_creative()` — outer function with broken GEMINI_API_KEY check

</canonical_refs>

<code_context>
## Existing Code Insights

### Reusable Assets
- `_try_stage_c()` in `dry_run.py` (lines 84–101): exact pattern to copy into `main.py` for Stage C fallback
- `dry_run.py:157–158`: exact `end_date` fix pattern for `main.py:152`
- `src/linkedin_api.py` refresh_access_token: already implemented correctly, just needs verification

### Established Patterns
- `try/except Exception as exc: log.warning(...)`: partial-failure tolerance pattern used throughout `main.py` for creative failures
- `config.GDRIVE_ENABLED` gating: correct approach for feature flags — do not bypass
- `**_kwargs` in `generate_midjourney_creative()`: already absorbs `mj_token` and `claude_key` — removing them from the call site is safe

### Integration Points
- `main.py:66–67`: `claude_key` and `mj_token` read but both always empty — can be removed after guard fixes
- `build_inmail_variants(tg_cat, cohort, claude_key)`: `claude_key` arg is accepted but unused internally; LiteLLM handles routing
- `_generate_imagen()` line 128: `if config.LITELLM_API_KEY:` — LiteLLM path runs first; GEMINI_API_KEY only for direct API fallback

</code_context>

<specifics>
## Specific Ideas

- The function `generate_midjourney_creative()` is named "midjourney" but is actually the Gemini LiteLLM pipeline. The name is legacy. Do NOT rename it in Phase 1 (unnecessary churn). Just fix the bugs.
- `LINKEDIN_INMAIL_SENDER_URN` test value: `urn:li:person:vYrY4QMQH0` (Tuan). If rejected, query `/rest/adAccountUsers` as fallback.
- Token expiry: `expires_at=1781441848` ≈ June 2026. Auto-refresh via refresh token already coded. Verify once in Phase 1.

</specifics>

<deferred>
## Deferred Ideas

- Drive upload (DATA-01) — Phase 2. Plan already exists. Do not include in Phase 1.
- `create_image_ad` unblock via `r_liteprofile` scope — PARKED. External LinkedIn app settings change required.
- Rename `generate_midjourney_creative()` → `generate_gemini_creative()` — nice cleanup but out of scope for Phase 1.
- `LINKEDIN_INMAIL_SENDER_URN` production setup — Phase 1 sets test value only.

</deferred>

---

*Phase: 01-pipeline-integrity*
*Context gathered: 2026-04-21*

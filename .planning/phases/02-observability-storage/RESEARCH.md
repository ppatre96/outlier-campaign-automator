# Phase 2: Observability & Storage — Research

**Researched:** 2026-04-21
**Domain:** Slack delivery, Google Drive upload, Sheets logging, campaign lifecycle monitoring, LLM context flow
**Confidence:** HIGH — all findings derived from direct source-code reading of the actual modules

---

## Summary

Phase 2 closes every reporting loop the pipeline currently skips silently. The code infrastructure is largely written; the blockers are mostly environment configuration (tokens, Shared Drive setup) and two concrete code gaps: `write_creative()` does not accept a `drive_url` argument yet, and `_post_to_slack()` only supports webhooks while the workspace has blocked incoming webhooks.

The pipeline is a single Python process — there are no Claude sub-agents per stage. LLMs appear at exactly two points: (1) `build_copy_variants()` in `src/figma_creative.py` calls LiteLLM/Claude to produce copy + `photo_subject`, and (2) `generate_midjourney_creative()` in `src/midjourney_creative.py` calls Gemini via LiteLLM `/images/generations` to produce the background photo. The `photo_subject` field is the only bridge between these two stages — if it is missing or generic, the Gemini prompt degrades silently.

The Figma path (`apply_plugin_logic` via MCP) is de-facto disabled: `figma_creative.py` in `REQUIREMENTS.md` and `PROJECT.md` both mark Figma MCP as out of scope in favour of the Gemini image pipeline. The `figma_upload.py` file does not exist — there is no upload module to rethink. Any work on native Figma elements is therefore out of scope for Phase 2.

**Primary recommendation:** Fix the two code gaps (Slack Bot Token path, Drive URL write to Sheets), set three env vars (`SLACK_BOT_TOKEN`, `GDRIVE_ENABLED`, `GDRIVE_FOLDER_ID`), set up the Shared Drive, add the crontab entry, and run `main.py --mode monitor` against a live row to verify the lifecycle monitor end-to-end.

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| OBS-01 | Slack weekly report posted automatically every Monday 9 AM IST without manual trigger | crontab entry `30 3 * * 1`; script already correct; needs token |
| OBS-02 | `SLACK_WEBHOOK_URL` filled OR Slack Bot Token with `chat:write` scope | Webhook path blocked; Bot Token path requires code addition in `post_weekly_reports.py` |
| OBS-03 | Static ad weekly report populated when static campaigns are active | `src/static_weekly_report.py` fully implemented; needs empty-state guard |
| OBS-04 | Campaign lifecycle monitor flagging underperforming campaigns in Slack report | `src/campaign_monitor.py` fully implemented; monitor results written to Sheet tab only — no Slack post today |
| DATA-01 | Google Drive upload enabled — `GDRIVE_ENABLED=true`, Shared Drive shared with service account | `src/gdrive.py` fully implemented; blocked on Shared Drive creation + env var |
| DATA-02 | Generated PNG and Drive URL logged to Sheets after each creative run | `drive_url` captured in `main.py` but NOT passed to `write_creative()` — code gap |
</phase_requirements>

---

## Project Constraints (from CLAUDE.md)

All generated copy (headlines, subheadlines, CTAs, Slack report labels) must use Outlier's approved vocabulary:

| Don't Say | Say Instead |
|-----------|-------------|
| Job | Task, opportunity |
| Compensation | Payment |
| Performance | Progress |
| Training | Become familiar with project guidelines |
| Bonus | Reward |
| Instructions | Project guidelines |

This applies to any user-facing strings in Slack reports, Sheets labels, and ad copy generation. The `build_copy_variants()` prompt in `figma_creative.py` already enforces these rules via its `## STRICT RULES` section.

---

## Module Status Audit

### OBS-01 / OBS-02: Slack reporting (`scripts/post_weekly_reports.py`, `config.py` line 101)

**Current state:**
- `_post_to_slack()` at lines 29–43 is webhook-only: reads `config.SLACK_WEBHOOK_URL`, fails with an error log if empty.
- `config.SLACK_BOT_TOKEN` is defined (line 101) but never used anywhere in the codebase.
- `SLACK_WEBHOOK_URL` is empty in `.env` — workspace restrictions blocked webhook setup (confirmed in STATE.md Known Blockers).
- `SLACK_REPORT_USER = "U095J930UEL"` (pranav.patre@scale.com) — intended DM target.
- `slack_sdk` is NOT installed in the venv (verified: import fails). It is also absent from `requirements.txt`.
- No crontab entry exists on the host (verified: `crontab -l` returned nothing).

**Gap:** The only working delivery path requires a webhook URL that is blocked. The Bot Token path has config support but zero code implementation.

**Implementation approach:**
1. Add `slack-sdk>=3.0.0` to `requirements.txt` and install: `pip install slack-sdk`.
2. In `_post_to_slack()`, add a Bot Token branch above the webhook branch:
   ```python
   if config.SLACK_BOT_TOKEN:
       from slack_sdk import WebClient
       client = WebClient(token=config.SLACK_BOT_TOKEN)
       for chunk in chunks:
           client.chat_postMessage(channel=config.SLACK_REPORT_USER, text=chunk)
       return
   ```
3. Create a Slack App at api.slack.com/apps → "From scratch" → Outlier workspace. Under "OAuth & Permissions" add Bot Token Scope `chat:write`. Install to workspace. Copy Bot OAuth Token to `.env` as `SLACK_BOT_TOKEN`.
4. Invite the bot to the DM channel by messaging it or using `/invite @botname`.
5. Add crontab entry: `30 3 * * 1 cd /Users/pranavpatre/outlier-campaign-agent && /path/to/venv/python scripts/post_weekly_reports.py >> /tmp/weekly_report.log 2>&1`

**What already works:** Report text generation in both `src/inmail_weekly_report.py` and `src/static_weekly_report.py` is complete and does not need changes.

---

### OBS-03: Static ad weekly report (`src/static_weekly_report.py`)

**Current state:**
- Fully implemented: SQL query against `AD_ANALYTICS_BY_CREATIVE` (not `LINKEDIN_CREATIVE_COSTS` — correctly avoids the SENDS/impressions bug noted in the module docstring), data parsing, winner/loser identification, hypothesis generation, competitor intel integration.
- `run_weekly_report()` returns a formatted string. Called from `post_weekly_reports.py` at line 59.
- If Redash returns no data, it returns `"Weekly Static Creatives report: no data returned for the last 7 days."` — this is a safe empty state, not a crash.
- Currently no static campaigns are running, so the report will always return the empty-state message.

**Gap:** The empty-state message is clear but plain. No code change is strictly required; OBS-03 is met once the Slack delivery path works (OBS-02).

**Risk:** The SQL uses `VIEW.APPLICATION_CONVERSION` which requires the Redash DS 30 view to expose that table. No evidence this view is blocked, but untested for the static path specifically.

---

### OBS-04: Campaign lifecycle monitor (`src/campaign_monitor.py`, `main.py --mode monitor`)

**Current state:**
- `run_monitor()` in `main.py` lines 573–655: reads active campaigns from `Triggers 2` where `li_status` starts with `"Created:"`, checks LinkedIn learning phase, fetches Snowflake pass rates, scores KEEP/PAUSE/TEST_NEW, pauses underperformers, discovers new ICPs, writes monitor results to a `Monitor` tab.
- All logic is implemented. `write_monitor_results()` creates the `Monitor` worksheet tab if it does not exist.
- **The monitor does NOT post to Slack** — it only writes to Google Sheets. OBS-04 says "flagged in Slack report." This means either:
  - The weekly Slack report should pull monitor verdict summary from the `Monitor` tab, OR
  - `run_monitor()` should post a summary via `_post_to_slack()` after writing results.
- Currently: no Slack post from the monitor at all.

**Gap:** Monitor verdict summary is not surfaced in Slack. The weekly report script does not read the Monitor tab.

**Implementation approach:**
- Simplest path: after `write_monitor_results()` in `run_monitor()`, build a summary text (e.g. "Campaign monitor: 2 KEEP, 1 PAUSE, 0 TEST_NEW") and call `_post_to_slack()` from `scripts/post_weekly_reports.py` OR add the summary to the InMail/static weekly report text. The latter is cleaner because it keeps the Monday 9 AM delivery as the single touchpoint.
- Add a `read_monitor_summary()` function to `src/campaign_monitor.py` that reads the most recent rows from the `Monitor` tab and returns a text block. Call it from `post_weekly_reports.py`.

---

### DATA-01: Google Drive upload (`src/gdrive.py`, `config.py` lines 78–82)

**Current state:**
- `src/gdrive.py` is fully implemented: `upload_creative(file_path, folder_id)` uploads a PNG to the configured folder, sets public-readable permissions, returns `webViewLink`.
- Uses `google-api-python-client` with `supportsAllDrives=True` (already handles Shared Drive).
- `GDRIVE_ENABLED=false` in config — gated by env var.
- `GDRIVE_FOLDER_ID = "1TrpyIOq6hS4eGAc0sYUIJom4MAanbnm4"` — this is a personal Drive folder ID (confirmed in module docstring: "Personal Google Drive folders do not allocate storage quota to service accounts").
- In `main.py` lines 349–356: upload is called when `GDRIVE_ENABLED` is true. The `drive_url` variable is assigned but never used after that — it is NOT passed to `write_creative()`.

**External blockers:**
1. Shared Drive must be created in Google Workspace.
2. Service account `outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com` must be added as Content Manager.
3. New Shared Drive folder ID must replace current `GDRIVE_FOLDER_ID`.
4. `GDRIVE_ENABLED=true` must be set in `.env`.

**No code changes required in `src/gdrive.py`** — it is already correct.

---

### DATA-02: Drive URL logged to Sheets (`src/sheets.py` `write_creative()`, `main.py` lines 367)

**Current state:**
- `write_creative(stg_id, creative_name, li_creative_id)` at `src/sheets.py` line 181 appends 4 columns to the Creatives tab: `stg_id`, `creative_name`, `li_creative_id`, `timestamp`.
- `drive_url` is captured in `main.py` line 353 but is local to the `if config.GDRIVE_ENABLED:` block. It is **never passed** to `write_creative()`.
- The `write_creative()` signature has no `drive_url` parameter.

**Gap:** This is a concrete code gap — `drive_url` is assigned but dropped. Requires two changes:
1. Add `drive_url: str = ""` parameter to `write_creative()` in `src/sheets.py`.
2. Append `drive_url` as a 5th column in the `ws.append_row()` call.
3. In `main.py`, pass `drive_url=drive_url` to `write_creative()` at line 367.

**Risk:** The `drive_url` may be `None` if `GDRIVE_ENABLED=false`. The parameter default of `""` handles this — pass `drive_url or ""`.

---

## LLM / Model Context Flow Analysis

### Overview

The pipeline has **no Claude sub-agents per stage**. It is a linear Python process. LLMs appear at exactly two points:

```
cohort.name + cohort.rules
        |
        v
[1] build_copy_variants()          src/figma_creative.py lines 163–203
    LiteLLM → claude-sonnet-4-6
    Produces: headline, subheadline, cta, photo_subject (per angle A/B/C)
        |
        | photo_subject (string)
        v
[2] generate_midjourney_creative()  src/midjourney_creative.py lines 436–499
    _build_imagen_prompt(photo_subject, angle)
    LiteLLM → Gemini /images/generations
    Produces: background PNG
        |
        v
    compose_ad() composites: PNG + gradient + text
```

### Stage 1: Cohort → Copy (LiteLLM/Claude)

**Input context passed to LLM:**
- `cohort.name` — raw feature label string (e.g. `"skills__diagnosis__healthcare"`)
- `cohort.rules` — list of `(feature, value)` tuples from Stage A/B analysis
- Human-readable signal strings derived from `_col_to_human(feat)` and `_feature_to_facet(feat)`
- Figma text layer map (if available; empty dict `{}` when Figma is not configured)

**What the prompt asks the LLM to do:**
1. Derive professional identity from signals (no pre-defined TG buckets)
2. Infer geography, schedule constraints, emotional state
3. Write 3 copy variants (angles A/B/C) with different opening structures
4. Produce a `photo_subject` per variant: `"[gender] [ethnicity] [specific profession], [specific activity at home]"`

**Output consumed downstream:**
- `headline`, `subheadline`, `cta` → used in `compose_ad()` text overlay
- `photo_subject` → **the only signal passed to Gemini** for background image generation
- `layerUpdates` → used in Figma MCP path (currently out of scope)

**Gap identified:** The `photo_subject` is the sole bridge between the copy stage and the image stage. The prompt instructs the LLM to be specific ("male Northern European DNA sequencing researcher, reviewing sequencing data on a laptop at home"), but if the LLM produces a generic description (e.g. "professional person at a laptop"), Gemini will generate a generic image with no connection to the cohort. There is no validation of `photo_subject` specificity before passing it to Gemini.

**Secondary gap:** `build_copy_variants()` is called with `claude_key=""` (empty string) when the ANTHROPIC_API_KEY env var is unset. In that case, `_llm_client()` returns an OpenAI client pointed at LiteLLM with an empty `api_key`. LiteLLM requires the key — if it is missing the call will fail with a 401. This is handled by a `try/except` at the call site in `main.py` line 277 (logs warning and continues with empty variants), but it means creatives will be generated without copy.

---

### Stage 2: photo_subject → Image (Gemini via LiteLLM)

**Input context passed to image model:**
- `photo_subject` from the selected angle variant
- Hard-coded template strings: framing, background description, lighting, film style suffix
- `angle` → maps to expression modifier (`_ANGLE_EXPRESSIONS`)

**What Gemini receives:**
```
"a close-up environmental portrait of a {photo_subject},
face and upper body filling most of the frame,
lush plant-filled home interior visible in background around subject,
bookshelves, wall art, and potted plants behind them,
warm natural window light,
85mm prime lens, {expression},
shot on film, 85mm prime lens, shallow depth of field,
warm natural color grade, authentic lifestyle photo, NOT stock photo..."
```

**Gap identified:** The Gemini prompt has no knowledge of:
- The headline or subheadline text being overlaid on the image
- The copy angle (A=Expertise, B=Earnings, C=Flexibility) beyond the expression modifier
- Whether the generated subject's ethnicity/attire will contrast legibly with the left-side gradient

The image is generated and then text is overlaid in `compose_ad()` without any feedback loop. If the Gemini-generated face area is too bright or has a busy pattern that reduces text legibility on the gradient, there is no retry or quality check.

**What works well:** The gradient overlay (`_make_gradient_overlay`) is always applied regardless of the background content, so text contrast is structurally guaranteed on the left side. The template prompt's constraints (plant background, close-up portrait, home interior) are derived from analysis of 7 real reference ads — this is HIGH confidence design direction.

---

### Figma Path Status

`figma_upload.py` does not exist in the repository. The Figma path in `figma_creative.py` uses:
- Figma REST API for text layer reading (`FigmaCreativeClient`)
- `apply_plugin_logic()` for MCP-based plugin execution (cloning frames, applying text)
- `export_clone_pngs()` for PNG export

This path is **conditionally invoked** in `main.py` only when `figma_file` and `figma_node` are set in the trigger row. In practice, both values are empty for all current rows (none of the Triggers 2 rows have Figma file/node populated based on the sheet structure).

**The `figma.createImage()` size limit issue** (mentioned in the phase brief — ~100 byte limit for images passed through MCP) is relevant to the `apply_plugin_logic()` path only. Since that path is disabled by missing env values, this is a future concern, not a Phase 2 blocker.

**Native Figma elements approach:** If the Figma path is ever re-enabled, the correct approach is to use Figma's native shapes (rectangles for gradient fills) + text nodes (with `fontName` set to Avenir Next) rather than embedding a PNG via `figma.createImage()`. This avoids the MCP image size limit entirely. However, this is out of scope per `REQUIREMENTS.md` ("Figma MCP creative design" listed as Out of Scope).

---

## Common Pitfalls

### Pitfall 1: Shared Drive vs Personal Drive
**What goes wrong:** Service account uploads to a personal Drive folder return `403 storageQuotaExceeded` because personal Drive does not allocate quota to service accounts.
**Why it happens:** The existing `GDRIVE_FOLDER_ID` value is a personal Drive folder ID.
**How to avoid:** Create a Google Workspace Shared Drive (Team Drive). The `supportsAllDrives=True` flag in `upload_creative()` is already set — but the folder must actually be inside a Shared Drive.
**Warning signs:** `HttpError 403` with `storageQuotaExceeded` in the upload attempt.

### Pitfall 2: Slack workspace webhook restrictions
**What goes wrong:** `SLACK_WEBHOOK_URL` creation is blocked by workspace admin policy.
**Why it happens:** Confirmed in STATE.md. Incoming Webhooks require admin approval in restricted workspaces.
**How to avoid:** Use Bot Token (`chat:write` scope) instead. Bot tokens bypass webhook restrictions; they only need the bot to be invited to the target channel/DM.
**Warning signs:** Webhook creation UI is greyed out or returns "not allowed by workspace admin."

### Pitfall 3: drive_url silently dropped
**What goes wrong:** Drive upload succeeds, URL is logged, but the Sheets Creatives tab never receives it.
**Why it happens:** `drive_url` is a local variable in `main.py` that is never passed to `write_creative()`. The method signature does not accept it.
**How to avoid:** Add `drive_url` parameter to `write_creative()` and pass it at the call site.
**Warning signs:** Creatives tab has 4 columns (stg_id, name, li_creative_id, timestamp) but no Drive URL column after a run with `GDRIVE_ENABLED=true`.

### Pitfall 4: photo_subject generic fallback
**What goes wrong:** LLM produces a generic `photo_subject` like "professional at a laptop." Gemini generates a stock-photo-looking image that does not reflect the cohort.
**Why it happens:** The prompt warns against this but there is no post-generation validation. If the copy prompt times out or the LLM truncates its output, the `photo_subject` field may be absent or minimal.
**How to avoid:** The `generate_midjourney_creative()` function already raises `RuntimeError` if `photo_subject` is empty (lines 463–467). But a non-empty generic string passes through silently.
**Warning signs:** Generated PNGs all look similar regardless of cohort; check if `photo_subject` values in the log are specific or generic.

### Pitfall 5: Monitor tab missing when `write_monitor_results()` is called
**What goes wrong:** First `run_monitor()` call attempts to access `Monitor` worksheet; if the tab doesn't exist, it creates it. This is handled in `write_monitor_results()` with a `try/except` that creates the tab on `gspread.exceptions.WorksheetNotFound`. Safe.
**Note:** This is actually handled correctly — documenting here to confirm it is not a risk.

### Pitfall 6: crontab path resolution on macOS
**What goes wrong:** crontab entries on macOS do not inherit shell PATH. The `python3` command in a bare crontab refers to the system Python, not the venv.
**How to avoid:** Use the full absolute path to the venv Python:
```
30 3 * * 1 /Users/pranavpatre/outlier-campaign-agent/venv/bin/python /Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py >> /tmp/weekly_report.log 2>&1
```
Also use absolute path to the working directory or `cd` first.

---

## Don't Hand-Roll

| Problem | Don't Build | Use Instead | Why |
|---------|-------------|-------------|-----|
| Slack message delivery | Custom HTTP POST to Slack API | `slack_sdk.WebClient` | Handles retries, rate limits, chunking, error codes |
| Google Drive upload | Manual `requests.put` to Drive API | `google-api-python-client` (already in use) | Handles resumable uploads, auth, Shared Drive flags |
| Google Sheets auth | Re-implement service account auth | `google.oauth2.service_account.Credentials` (already in use) | Handles token refresh, scope validation |
| Cron scheduling on macOS | launchd plist from scratch | `crontab` (simpler for a single script) | launchd is more reliable for production; crontab is sufficient for dev/single-user |

---

## Environment Availability

| Dependency | Required By | Available | Version | Fallback |
|------------|------------|-----------|---------|----------|
| Python 3.13 | All scripts | Yes | 3.13.7 | — |
| `requests` | Slack webhook (existing), Drive | Yes | 2.33.1 | — |
| `google-api-python-client` | `src/gdrive.py` | Yes (global) | unknown | — |
| `gspread` | `src/sheets.py` | Yes (global) | unknown | — |
| `slack-sdk` | `_post_to_slack()` Bot Token path | No | Not installed | Webhook (blocked) or install slack-sdk |
| Slack Bot Token | OBS-01, OBS-02 | Not set | — | None — must create app |
| Slack Webhook URL | OBS-01, OBS-02 (existing code) | Not set | — | Bot Token (preferred) |
| Shared Drive folder | DATA-01 | Not configured | — | None — must create |
| `GDRIVE_ENABLED=true` | DATA-01, DATA-02 | false | — | None — must set in .env |
| crontab entry | OBS-01 | None | — | Manual execution |
| `LITELLM_API_KEY` | Copy gen, image gen | Set (via .env) | — | — |

**Missing with no fallback:**
- Slack Bot Token (OBS-02 is explicitly a blocker — either webhook or bot token required)
- Shared Drive folder (DATA-01 is blocked until this exists)

**Missing with fallback or workaround:**
- `slack-sdk` → installable in one command: `pip install slack-sdk`
- crontab entry → pipeline can be triggered manually until cron is set up

---

## Implementation Task Order

Given the dependency structure, the recommended execution order is:

1. **Install slack-sdk** (`pip install slack-sdk`, add to `requirements.txt`) — unblocks OBS-02 code work.
2. **Add Bot Token path to `_post_to_slack()`** — code change in `scripts/post_weekly_reports.py`.
3. **Create Slack App and get Bot Token** — external step; set `SLACK_BOT_TOKEN` in `.env`.
4. **Test Slack delivery** — run `python scripts/post_weekly_reports.py` manually.
5. **Add Drive URL parameter to `write_creative()`** — code change in `src/sheets.py` + `main.py`.
6. **Create Shared Drive + add service account** — external step.
7. **Set `GDRIVE_ENABLED=true` and `GDRIVE_FOLDER_ID`** — `.env` change.
8. **Test Drive upload + Sheets logging** — run a creative generation with Drive enabled.
9. **Add crontab entry** — `crontab -e` with absolute venv path.
10. **Add monitor summary to weekly Slack report** — add `read_monitor_summary()` to bridge OBS-04.
11. **Run `main.py --mode monitor`** — verify KEEP/PAUSE/TEST_NEW verdicts.
12. **Document audienceCounts MDP status** — `README.md` section.

---

## audienceCounts / Stage C Status (OBS-04 adjacent)

Per `REQUIREMENTS.md` LI-04: "audienceCounts Stage C either approved (MDP) or gracefully bypassed with logged reason" — marked Complete in Phase 1.

The bypass is already in place (STATE.md decision D-05: `try/except` with `cohorts_b[:config.MAX_CAMPAIGNS]` fallback). The Phase 2 task is purely documentation: add a `README.md` section stating MDP approval is pending for account 510956407 and explaining the bypass.

No code change needed — document only.

---

## Sources

### Primary (HIGH confidence — direct source code reading)
- `/Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py` — Slack delivery logic, both paths
- `/Users/pranavpatre/outlier-campaign-agent/src/gdrive.py` — Drive upload implementation
- `/Users/pranavpatre/outlier-campaign-agent/src/sheets.py` — `write_creative()` signature
- `/Users/pranavpatre/outlier-campaign-agent/src/campaign_monitor.py` — lifecycle monitor implementation
- `/Users/pranavpatre/outlier-campaign-agent/src/static_weekly_report.py` — static report implementation
- `/Users/pranavpatre/outlier-campaign-agent/src/figma_creative.py` — `build_copy_variants()`, LiteLLM call, prompt
- `/Users/pranavpatre/outlier-campaign-agent/src/midjourney_creative.py` — Gemini image pipeline, `compose_ad()`
- `/Users/pranavpatre/outlier-campaign-agent/main.py` lines 290–384, 573–655 — Drive upload call, `write_creative()` call, `run_monitor()`
- `/Users/pranavpatre/outlier-campaign-agent/config.py` — `SLACK_BOT_TOKEN`, `GDRIVE_ENABLED`, `GDRIVE_FOLDER_ID`
- `/Users/pranavpatre/outlier-campaign-agent/.planning/STATE.md` — known blockers, decisions
- `/Users/pranavpatre/outlier-campaign-agent/.planning/REQUIREMENTS.md` — requirement definitions
- `/Users/pranavpatre/outlier-campaign-agent/.planning/ROADMAP.md` — Phase 2 task list
- Environment probes: `crontab -l`, package import tests

### Secondary (MEDIUM confidence)
- `requirements.txt` — confirms `slack-sdk` is absent from declared dependencies
- `.planning/config.json` — confirms `nyquist_validation: false` (Validation Architecture section omitted)

---

## Metadata

**Confidence breakdown:**
- Module current state: HIGH — read directly from source files
- Code gaps (drive_url, Slack Bot Token): HIGH — confirmed by reading call sites and signatures
- LLM context flow: HIGH — traced through actual function signatures and prompt templates
- Environment availability: HIGH — verified via import tests and crontab probe
- External blockers (Shared Drive, Slack App): HIGH — confirmed in STATE.md and config

**Research date:** 2026-04-21
**Valid until:** 2026-05-21 (stable codebase; env/external blockers may change sooner)

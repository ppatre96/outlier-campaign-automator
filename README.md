# Outlier Campaign Agent

Automated LinkedIn campaign pipeline for Outlier (Scale AI). Discovers high-signal audience cohorts from screening data, generates ad copy and image creatives via LLM, and publishes LinkedIn InMail and Static Ad campaigns — all from a single trigger.

## Quick Start

```bash
python main.py --dry-run
```

See `WORKFLOW.md` for full pipeline walkthrough and `AGENT-PIPELINE.md` for sub-agent architecture.

## Performance Tuning (Phase 3.x parallelism)

Four pools control concurrency. `IMAGE_GEN_CONCURRENCY`/`COPY_GEN_CONCURRENCY` default to 4; `RAMP_CONCURRENCY` defaults to 1 (opt-in). Set any to 1 to fall back to fully sequential behavior.

| Doppler key | Default | What it parallelizes | Measured win |
|---|---|---|---|
| `IMAGE_GEN_CONCURRENCY` | `4` | Gemini image-gen across `(cohort × geo × angle)` tasks in `_process_static_campaigns` | **2.67x** live |
| `COPY_GEN_CONCURRENCY` | `IMAGE_GEN_CONCURRENCY` | Anthropic copy-gen via `build_copy_variants` across `(cohort × geo)` combos | **2.50x** live |
| _(implicit, no knob)_ | `2` | `_process_inmail_campaigns` + `_process_static_campaigns` run as parallel arms in `_process_row_both_modes` | **1.47x–2x** live |
| `RAMP_CONCURRENCY` | `1` | Pending-rows loop in `run_launch` — multiple cohorts (and full Stage 1+2+C+creative+campaign-create) run in parallel | **TBD** (2x expected at workers=2) |

Combined per-ramp wall-clock on a typical 9-cell ramp: **~50 min → ~13 min** of LLM/image work (~4x). `RAMP_CONCURRENCY` is the next-tier knob (multiplies on top of the above by parallelizing across cohorts of the same ramp).

Bench harnesses to re-measure against live APIs (each ~$0.30–$1 in credits):

```bash
DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib doppler run --project outlier-campaign-agent --config dev -- \
    venv/bin/python scripts/bench_image_gen_concurrency.py    # image gen
    venv/bin/python scripts/bench_copy_gen_concurrency.py     # copy gen
    venv/bin/python scripts/bench_arms_parallelism.py         # InMail + Static arm overlap
    venv/bin/python scripts/bench_ramp_parallelism.py         # ramp/cohort-loop fan-out
```

Shared-state locks (no config — transparent):
- `src/campaign_registry.py::_registry_lock` (RLock) — atomic load → mutate → save across concurrent writers
- `src/sheets.py::SheetsClient._write_lock` (RLock) — serializes ALL gspread access (reads + writes) since gspread is not thread-safe
- `src/linkedin_api.py::LinkedInClient._refresh_lock` — serializes OAuth token refresh + session header update
- `src/linkedin_api.py::LinkedInClient._session_lock` — serializes `requests.Session.request()` so concurrent reads can't fire with a stale Authorization header
- `src/linkedin_urn.py::UrnResolver._cache_lock` — double-checked guard on the URN fuzzy-match cache populate path
- `src/gdrive.py::_folder_cache_lock` — guards the module-level folder-id cache against duplicate folder creation
- `src/figma_creative.py::FigmaCreativeClient._session_lock` — serializes `requests.Session.get()` on the Figma client
- `src/claude_client.py::get_client` — double-checked `threading.Lock` around lazy Anthropic SDK singleton

## Current External Blockers (2026-05-11)

| Blocker | Impact | Owner | Status |
|---|---|---|---|
| LinkedIn **Marketing Developer Platform (MDP)** entitlement on OAuth app `86g4m92v2vfq68` | `/rest/posts` with `adContext.dscAdAccount` 403s → static-ad creative attach blocked; campaigns + ad sets still create cleanly | Manager → apply at https://www.linkedin.com/developers/apps/86g4m92v2vfq68/products → "Marketing Developer Platform" → Request Access | 2-4 week LinkedIn approval. Diagnostic detail at `src/linkedin_api.py:692` (DSC create site). |
| Google Ads access on customer `8840244968` (Outlier ad-serving account, child of MCC `6301406350`) | `OPERATION_NOT_PERMITTED_FOR_CONTEXT` — pipeline can read MCC but can't create campaigns on the child | Manager → direct Standard-user invite on `8840244968` (previous approval request not yet sign-off-ed) | Diag: `scripts/diag_google_ads_access.py`. Per-channel isolation means LinkedIn + Meta keep running. |

**Working channels:** LinkedIn campaigns + InMail (with conversion auto-attach), Meta (end-to-end verified via `scripts/verify_meta_ad_creation.py`).

**Drive-only policy:** Generated creative PNGs live exclusively in Shared Drive `0ALHAgK4RPbnfUk9PVA` at `<ramp_id>/<channel>/<cohort_geo>/<angle>.png`. The `_save_creative_locally` helper was removed; no local-disk fallback paths exist anywhere in the pipeline.

## TG Classifier Buckets

`classify_tg()` in `src/figma_creative.py` maps a cohort (name + rules) to one of these buckets. First regex match wins, so order matters.

| Priority | Bucket | Example signals |
|----------|--------|-----------------|
| 1 | DATA_ANALYST | data, sql, analyst, tableau, snowflake, bigquery, excel |
| 2 | ML_ENGINEER | ml, machine learning, pytorch, tensorflow, llm, nlp |
| 3 | MATH | math, statistics, actuary, quantitative, probability, econometrics |
| 4 | MEDICAL | doctor, clinical, cardiology, oncology, healthcare |
| 5 | LANGUAGE | hindi, spanish, translator, linguist |
| 6 | SOFTWARE_ENGINEER | software, developer, devops, python, java, react |
| 7 | GENERAL | (fallback — no match) |

Each bucket also has a matching entry in `TG_PALETTES` and `TG_ILLUS_VARIANTS` (same file) driving Figma illustration selection.

For exact regex patterns, see `src/figma_creative.py::classify_tg`.

## Scripts

### STEM InMail Financial-Angle Regen

Regenerates InMail creatives for the three existing STEM campaigns using angle F (Financial — rate-in-subject-line). Does NOT create new campaigns or campaign groups; attaches new creatives to the existing campaign URNs.

Dry run (no LinkedIn, no Sheets writes):
```bash
PYTHONPATH=. python3 scripts/regen_stem_inmail.py --dry-run
```

Single campaign only:
```bash
PYTHONPATH=. python3 scripts/regen_stem_inmail.py --only-id 633412886
```

Full run (writes to LinkedIn + Sheets):
```bash
PYTHONPATH=. python3 scripts/regen_stem_inmail.py
```

Prereqs: `LINKEDIN_INMAIL_SENDER_URN`, `LINKEDIN_ACCESS_TOKEN`, `LITELLM_API_KEY` must be set in `.env`.

## Brand Voice Enforcement

All copy generated by the campaign agent must comply with Outlier brand voice guidelines. This is enforced through automated validation integrated into the pipeline.

### Single Source of Truth

See `.claude/brand-voice.md` for:
- Complete terminology rules (18 banned terms + approved alternatives)
- 14 AI pattern checks (active voice, no superlatives, no vague claims, etc.)
- Tone guidelines
- Platform-specific notes (email, LinkedIn, SMS)

### Validation System

**Automated Validation:**
- Runs after each copy generation (InMail, email, image ad)
- Scans for restricted vocabulary (exact term matching)
- Detects AI patterns via regex and heuristics (active voice, staccato, anaphora, etc.)
- Returns structured report with violations + severity levels

**Severity Levels:**
- `MUST` — Hard block. Copy cannot be submitted with MUST violations. Must revise and re-validate.
- `SHOULD` — Soft advisory. Copy can be submitted but flagged for human review. Strongly recommended fixes.
- `NICE_TO_HAVE` — Quality enhancement. Acceptable as-is; recommended for polish.

**Enforcement Policy:**

| Scenario | Action |
|----------|--------|
| 0 MUST, 0-N SHOULD | **APPROVED** — Submit as-is |
| 1+ MUST violations | **BLOCKED** — Revise copy, re-validate, re-submit |
| 0 MUST, 3+ SHOULD | **FLAGGED** — Approved but escalate to human review |

### Accessing Validation Reports

**In Pipeline (main.py):**
- Validation runs automatically after copy generation
- MUST violations → RuntimeError logged, row processing stops
- SHOULD violations → Warning logged, processing continues
- Full report (violations + suggestions) → Logged to stdout + saved

**In Dry-Run (scripts/dry_run.py):**
```bash
python scripts/dry_run.py --flow-id <flow_id>
```

Output includes brand voice validation inline with copy generation:
```
     Headline    : Flexible $X/hr + Your Expertise
     Subheadline : Join AI projects with 100% remote flexibility
     Brand voice : ✓ COMPLIANT (confidence: 100%)
```

Skip validation during testing with:
```bash
python scripts/dry_run.py --flow-id <flow_id> --skip-brand-voice
```

### Brand Voice Terminology Quick Reference

| Don't Say | Instead, Say |
|-----------|--------------|
| Job | Task, opportunity |
| Required | Strongly encouraged |
| Compensation | Payment |
| Bonus | Reward |
| Promote | Eligible to work on review-level tasks |
| Training, growth, learning | Become familiar with project guidelines |
| Interview | Screening |
| Team | Part of this project |

**Full list:** See `.claude/brand-voice.md`

### Agent Self-Check Gates

Before submitting copy, agents run a 14-point self-check:

1. No banned terminology (see quick reference above)
2. Active voice throughout
3. No staccato sentences (3+ short <8 words)
4. No anaphora (repeated sentence openings)
5. No parallel rhetoric
6. No superlatives ("best", "amazing", "incredible")
7. No vague claims ("unlimited", "cutting-edge")
8. No hype language ("revolutionary", "life-changing")
9. No consecutive colons
10. No LLM filler ("In today's world", "Whether you're")
11. Lists limited to 3-4 items
12. Sufficient personal pronouns (30%+)
13. Sentence variety
14. Human tone (warm, not corporate)

If any violation found → revise and re-check before submission.

### Testing Brand Voice Validation

**Quick test:**
```bash
python -c "
from src.brand_voice_validator import BrandVoiceValidator
validator = BrandVoiceValidator()
report = validator.validate_copy('This is the best job opportunity')
print(report.summary())
"
```

**Integration test (dry-run):**
```bash
python scripts/dry_run.py --flow-id <test_flow>
# Check console output for inline validation reports
```

## Weekly Feedback Loop (Phase 2.5 V2)

`scripts/weekly_feedback_loop.py` runs every Monday 09:00 IST via launchd. It wires
the v1 creative/cohort alerts + V2 full-funnel analysis + sentiment mining + ICP
drift detection into one consolidated Slack post.

### Cron setup (macOS) — USER ACTION REQUIRED

The launchd plist at `~/Library/LaunchAgents/com.outlier.weekly-reports.plist` is
the ONLY scheduled entry. Any `crontab` entry calling `scripts/post_weekly_reports.py`
should be DELETED — the orchestrator already calls v1 in-process. Currently the
machine has BOTH a crontab line AND a launchd plist running the v1 script (Pitfall
1 from RESEARCH-V2). Run these commands to switch over to the new orchestrator:

**1. Update the launchd plist `ProgramArguments`** to point at the orchestrator:

Open the plist:

    open -e ~/Library/LaunchAgents/com.outlier.weekly-reports.plist

Find the `<key>ProgramArguments</key>` block and change:

    <string>/Users/pranavpatre/outlier-campaign-agent/scripts/post_weekly_reports.py</string>

to:

    <string>/Users/pranavpatre/outlier-campaign-agent/scripts/weekly_feedback_loop.py</string>

**2. Reload launchd** so the change takes effect:

    launchctl unload ~/Library/LaunchAgents/com.outlier.weekly-reports.plist
    launchctl load   ~/Library/LaunchAgents/com.outlier.weekly-reports.plist

**3. Remove the duplicate crontab entry** (the line that calls
`post_weekly_reports.py`):

    crontab -l | grep -v post_weekly_reports | crontab -

**4. Verify both are correct:**

    crontab -l | grep post_weekly_reports        # MUST return empty
    launchctl list | grep com.outlier.weekly      # shows the loaded job
    grep weekly_feedback_loop.py ~/Library/LaunchAgents/com.outlier.weekly-reports.plist

Cron line equivalent: `30 3 * * 1` (Monday 03:30 UTC = 09:00 IST).

### Manual runs

    # Dry run — runs all four steps, writes nothing to Slack, does not trigger reanalysis
    venv/bin/python3 scripts/weekly_feedback_loop.py --dry-run

    # Force a run even if last_success_ts < 6 days ago
    venv/bin/python3 scripts/weekly_feedback_loop.py --force

    # Run only one step (debugging)
    venv/bin/python3 scripts/weekly_feedback_loop.py --only funnel --dry-run

### Logs and state

- Run logs: `logs/weekly_feedback_loop/<yyyy-mm-dd>.log`
- State file: `data/weekly_feedback_loop_state.json` (idempotency + last_failure_reason)
- File-lock: `data/weekly_feedback_loop_state.lock` (prevents concurrent runs)
- ICP snapshots: `data/icp_snapshots/<project_id>/<yyyy-mm-dd>.parquet`
- Sentiment callouts (consumed by ad-creative-brief-generator): `data/sentiment_callouts.json`
- Drift state: `data/icp_drift_state.json`

### Active projects for ICP drift

Drift Step D iterates over `data/active_projects.json` (a JSON list of project_id
strings). If that file does not exist, the orchestrator falls back to the single
project in `OUTLIER_TRACKING_PROJECT_ID` env var. If neither is set, drift step
no-ops with a logged warning (other steps still run).

### Env vars to configure

See `.env.example`. The Zendesk and Intercom env vars are optional — without them
the sentiment miner runs on public sources only.

### Failure handling

Each of the four steps is wrapped in try/except. On any step failure, a minimal
Slack message is still posted naming the failed step + error class. Check
`logs/weekly_feedback_loop/<date>.log` for the full traceback.

### Idempotency

The orchestrator persists `last_success_ts` to `data/weekly_feedback_loop_state.json`.
A re-run within 6 days of a successful run exits cleanly with a "skipping" log
(unless `--force` is passed). A `filelock.FileLock` with a 10-second timeout
prevents two concurrent invocations from racing — the second invocation exits
cleanly with "another instance running".


## Smart Ramp Poller (Phase 2.6)

`scripts/smart_ramp_poller.py` runs every 15 minutes via launchd. It polls Smart
Ramp for newly-submitted ramps, runs the campaign pipeline (BOTH InMail and
Static for every cohort), and posts a single consolidated message to **three
Slack targets** per ramp: Pranav DM, Diego DM, and channel `C0B0NBB986L`.

### USER ACTION REQUIRED — first-time setup

Per critical_constraints, the agent does NOT edit `~/Library/LaunchAgents/*`
or run `launchctl` commands. The following steps are manual:

#### 1. Create the launchd plist

Drop the following EXACT content into `~/Library/LaunchAgents/com.outlier.smart-ramp-poller.plist`:

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.outlier.smart-ramp-poller</string>

    <key>ProgramArguments</key>
    <array>
        <string>/Users/pranavpatre/outlier-campaign-agent/venv/bin/python3</string>
        <string>/Users/pranavpatre/outlier-campaign-agent/scripts/smart_ramp_poller.py</string>
        <string>--once</string>
    </array>

    <key>WorkingDirectory</key>
    <string>/Users/pranavpatre/outlier-campaign-agent</string>

    <!-- Every 15 minutes -->
    <key>StartInterval</key>
    <integer>900</integer>

    <!-- Run immediately on launchctl load (catch-up + smoke test) -->
    <key>RunAtLoad</key>
    <true/>

    <!-- launchd stdout/stderr is constant file; date-stamped logs are
         the script's responsibility (launchd doesn't support strftime in path) -->
    <key>StandardOutPath</key>
    <string>/tmp/outlier-smart-ramp-poller.stdout.log</string>
    <key>StandardErrorPath</key>
    <string>/tmp/outlier-smart-ramp-poller.stderr.log</string>
</dict>
</plist>
```

Critical: the venv python path MUST be absolute. launchd inherits a minimal
`$PATH` (per Pitfall 4 in RESEARCH.md), so `/usr/bin/python3` would fail to
import project deps.

#### 2. Validate + load the plist

```bash
plutil -lint ~/Library/LaunchAgents/com.outlier.smart-ramp-poller.plist
# expected: "OK"

launchctl unload ~/Library/LaunchAgents/com.outlier.smart-ramp-poller.plist 2>/dev/null
launchctl load   ~/Library/LaunchAgents/com.outlier.smart-ramp-poller.plist

launchctl list | grep com.outlier.smart-ramp-poller
# expected: a line like "-  0  com.outlier.smart-ramp-poller"
```

#### 3. Invite the bot to channel `C0B0NBB986L`

Without this, all channel posts return `not_in_channel` (the notifier catches
this and continues with the two DMs, but the channel will never receive
notifications).

```
In Slack, navigate to the channel C0B0NBB986L.
Type: /invite @<bot_name>
   (the bot's username is in the Slack App config — typically "outlier-campaign-bot")
```

#### 4. (Optional) Diego accepts the bot DM (covers Pitfall 7)

Pranav messages Diego asking him to send any one-character DM to
`@outlier-campaign-bot` once. Without this, Diego's first DM may return
`cannot_dm_bot` — the notifier handles this gracefully (logs warning,
continues with the other 2 targets), but it means Diego won't receive that
notification until he replies once.

#### 5. Verify the first poll

```bash
# Wait ~15 seconds after `launchctl load` (RunAtLoad=true triggers immediate run)
tail -f logs/smart_ramp_poller/$(date -u +%Y-%m-%d).log

# Expected log lines:
#   "Smart Ramp Poller starting (once=True ramp_id=None dry_run=False) -> ..."
#   "Fetched N ramps; M submitted"
#   "Ramp <id> action=<new|edit|noop> sig=sha256..."
```

### Manual runs

```bash
# Single poll (matches what launchd does every 15 min)
venv/bin/python3 scripts/smart_ramp_poller.py --once

# Force-process one ramp (debugging — bypasses signature noop check)
venv/bin/python3 scripts/smart_ramp_poller.py --ramp-id GMR-0010

# Dry run (no state write, no Slack)
venv/bin/python3 scripts/smart_ramp_poller.py --once --dry-run
```

### Logs and state

- Run logs: `logs/smart_ramp_poller/<yyyy-mm-dd>.log` (script-managed; rotated by file naming)
- launchd stdout/stderr: `/tmp/outlier-smart-ramp-poller.std{out,err}.log` (only for crashes BEFORE the script's logger initializes)
- State file: `data/processed_ramps.json` (atomic-write idempotent)
- File-lock: `data/smart_ramp_poller.lock` (prevents concurrent polls)
- Local-fallback creatives (when LinkedIn upload is blocked):
  `data/ramp_creatives/<ramp_id>/<cohort_id>_<inmail|static>_<angle>__<urlencoded_campaign_name>.png`

### Reset retry counter (after fixing a stuck ramp)

After 5 consecutive failures, the poller stops retrying that ramp and sends an
escalation message. To resume processing once the underlying issue is fixed:

```bash
venv/bin/python3 -c "
import json
p = 'data/processed_ramps.json'
ramp_id = 'GMR-XXXX'  # replace with the failed ramp ID
s = json.load(open(p))
s['ramps'][ramp_id]['consecutive_failures'] = 0
s['ramps'][ramp_id]['escalation_dm_sent'] = False
json.dump(s, open(p, 'w'), indent=2)
"
```

### Notification targets

The 3 Slack targets are resolved from `config.SLACK_RAMP_NOTIFY_TARGETS`:

```python
SLACK_RAMP_NOTIFY_TARGETS = [
    ("user",    SLACK_REPORT_USER),       # U095J930UEL — Pranav
    ("user",    SLACK_DIEGO_USER_ID),     # U08AW9FCP27 — Diego
    ("channel", SLACK_RAMP_NOTIFY_CHANNEL),  # C0B0NBB986L — shared channel
]
```

To add or remove a target, edit the list in `config.py`. No code changes needed.

### Failure handling

- Per-ramp isolation: one ramp's exception NEVER aborts the rest of the poll.
- Per-target Slack isolation: one of the 3 Slack targets failing (e.g., Diego
  has not accepted the bot DM) does NOT block the other 2 — the notifier logs
  a warning and continues.
- 5 consecutive failures on the same ramp → escalation message to all 3
  targets; subsequent polls do NOT retry that ramp until you reset the counter
  (snippet above).

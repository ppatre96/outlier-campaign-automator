# Outlier Campaign Agent — Sub-agent Pipeline Reference

## Architecture Overview

Two processes run independently and intersect at Stage 8b:

```
┌─────────────────────────────────────────────────────────────────┐
│  WEEKLY (parallel, cron)                                        │
│  competitor-bot → data/competitor_intel/latest.json             │
│      Scrapes: Turing, Surge, Handshake, Meta Ads Library,       │
│               Reddit, Trustpilot, Google autocomplete           │
│      Produces: experiment_ideas[], competitor_hooks[], avoid[]  │
└─────────────────────────────┬───────────────────────────────────┘
                              │ read at Stage 8b
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  PER RUN (on-demand, Claude Code session)                       │
│                                                                 │
│  campaign-manager  ← TOP-LEVEL ORCHESTRATOR                     │
│      Supervises all stages. Holds shared context:               │
│      project_id, flow_id, config_name, cohort list, TG labels   │
│      Passes context explicitly to every sub-agent.              │
│      Tracks blocked stages, falls back gracefully.              │
│      Updates memory after every run.                            │
│                                                                 │
│  Stage 0–2:  campaign-manager (Python: redash_db, features)    │
│  Stage 3–4:  campaign-manager (Python: stage_a, stage_b)       │
│  Stage 5–6:  campaign-manager (Python: stage_c, linkedin_urn)  │
│  Stage 7:    campaign-manager (sheets write / dry-run print)   │
│                                                                 │
│  ┌── For each cohort (Stage 8) ──────────────────────────────┐ │
│  │                                                            │ │
│  │  Stage 1:  outlier-data-analyst                           │ │
│  │            Input:  flow_id, config_name, date_range       │ │
│  │            Output: df_raw (screening DataFrame)           │ │
│  │                                                            │ │
│  │  Stage 8a: competitor-bot (weekly, skip if <7 days old)   │ │
│  │            Input:  TG category, config_name               │ │
│  │            Output: latest.json (already on disk)          │ │
│  │                                                            │ │
│  │  Stage 8b: ad-creative-brief-generator                    │ │
│  │            Input:  tg_category, angle (A/B/C),            │ │
│  │                    cohort.name, cohort.rules,              │ │
│  │                    cohort.pass_rate, config_name,          │ │
│  │                    competitor_intel (from latest.json)     │ │
│  │            Output: brief JSON {headline, subheadline,      │ │
│  │                    photo_subject, gradient, angle_mood,    │ │
│  │                    experiment_ideas_incorporated}          │ │
│  │                                                            │ │
│  │  Stage 8c: outlier-copy-writer                            │ │
│  │            Input:  full brief from 8b                     │ │
│  │            Output: variants[] × 3                         │ │
│  │                    {angle, headline, subheadline,          │ │
│  │                     cta, photo_subject}                    │ │
│  │                                                            │ │
│  │  Stage 8d: outlier-creative-generator                     │ │
│  │            Input:  variants[] from 8c, brief from 8b       │ │
│  │                    Builds Gemini imagen prompt using       │ │
│  │                    GEMINI_PROMPT_TEMPLATE                  │ │
│  │                    (src/gemini_creative.py)            │ │
│  │            Output: refined imagen_prompt per variant       │ │
│  │                    (varies gender, ethnicity, props,       │ │
│  │                     expression per A/B/C)                  │ │
│  │            Fallback: in cron/script mode (no Claude Code), │ │
│  │                    _build_imagen_prompt() used directly    │ │
│  │                                                            │ │
│  │  Stage 8e: campaign-manager (Python)                      │ │
│  │            Calls _generate_imagen(imagen_prompt)          │ │
│  │            Output: PNG × 3 → data/dry_run_outputs/        │ │
│  │                                                            │ │
│  │  Stage 8f: campaign-manager (Python, gated)               │ │
│  │            upload_creative() → Drive URL                  │ │
│  │                                                            │ │
│  │  Stage 8g: campaign-manager (Claude Code session only)    │ │
│  │            use_figma MCP → native Figma frame              │ │
│  │            Frame name: {project_id}_{angle}_v1            │ │
│  │            NOTE: figma.createImage() broken for large PNGs │ │
│  │            Use gradient fills + text nodes instead         │ │
│  │                                                            │ │
│  └────────────────────────────────────────────────────────────┘ │
│                                                                 │
│  Stage 9:   campaign-manager (LinkedIn API, live only)          │
│  Stage 10:  outlier-data-analyst (performance report → Slack)   │
│  Stage 11:  campaign-manager (memory update)                    │
└─────────────────────────────────────────────────────────────────┘
```

---

## How to Run (Claude Code Session)

Invoke the `campaign-manager` agent:

```
project_id=698a172324c01532c2f92a0d   # or flow_id=<24-char hex>
mode=dry-run                           # or live
skip_competitor_intel=false            # set true if run <7 days ago
```

The campaign-manager orchestrates every stage and spawns sub-agents with correct context.

**Do NOT run `scripts/dry_run.py` from a Claude Code session** — it bypasses all sub-agent specialization.

---

## Script Path (Cron / CI)

`scripts/dry_run.py` and `main.py` use direct Python API calls. No sub-agents are spawned.  
This is intentional: Claude Code is unavailable in cron context.  
Logs print `[Agent 8b/8c/8d]` markers to show which agent WOULD run in an interactive session.

---

## competitor-bot — Weekly Competitor Intelligence

**Schedule:** Every Monday (runs BEFORE the weekly campaign review, not per campaign).

**What it does:**
1. Scrapes 9 competitor task/browse pages for demand signals
2. Pulls Meta Ads Library for competitor creative hooks and earnings claims
3. Reads Reddit/Trustpilot/YouTube for pain points and praise
4. Writes structured output to `data/competitor_intel/latest.json`

**Output schema:**
```json
{
  "updated_at": "2026-04-21T...",
  "experiment_ideas": [
    "Test '$X/hr or more' earnings framing — seen in 3 Turing ads this week",
    "Flexibility angle outperforming expertise in Surge creatives"
  ],
  "competitor_hooks": ["..."],
  "avoid": ["'passive income' framing — flagged negative on Reddit"]
}
```

**How `ad-creative-brief-generator` uses it:**  
At Stage 8b, the brief generator reads `data/competitor_intel/latest.json`. Any `experiment_ideas` from the past 14 days are appended to the LiteLLM prompt as optional angles to test. This is how competitor learnings flow into future campaigns without manual intervention.

---

## Context Each Agent Requires

### campaign-manager (orchestrator)
Maintains the full run context object:
```python
{
  "project_id": str,
  "flow_id": str,
  "config_name": str,        # e.g. "Clinical Medicine - Cardiology"
  "cohorts": [...],          # Stage B output
  "selected_cohorts": [...], # Stage C output (or Stage B fallback)
  "tg_labels": {},           # {cohort_name: tg_category}
  "creatives": {},           # {cohort_name: {angle: png_path}}
  "drive_urls": {},          # {cohort_name: {angle: drive_url}}
  "blocked": []              # list of blocked stage names + reasons
}
```

### outlier-data-analyst
- Input: `flow_id`, `config_name`, `date_range` (start → today)
- Output: `df_raw` DataFrame (screening results with resume features)

### ad-creative-brief-generator
- Input: `tg_category`, `angle` (A/B/C), `cohort.name`, `cohort.rules`, `cohort.pass_rate`, `config_name`, `competitor_intel[]` (from latest.json)
- Output: structured brief JSON with `headline`, `subheadline`, `photo_subject`, `gradient`, `angle_mood`, `experiment_notes`

### outlier-copy-writer
- Input: full brief JSON from Stage 8b
- Output: `variants[]` — 3 dicts with `{angle, headline, subheadline, cta, photo_subject}`
- Enforces: Outlier vocabulary (payment not compensation, task not job, etc.)

### outlier-creative-generator
- Input: `variants[]` from Stage 8c + `brief` from Stage 8b (for visual direction, photo_subject, angle_mood)
- Output: refined Gemini `imagen_prompt` per variant — strings ready for `_generate_imagen()` call
  - Uses `GEMINI_PROMPT_TEMPLATE` constant (src/gemini_creative.py) as canonical structure
  - Varies gender, ethnicity, props, expression per A/B/C angle
  - Ensures composition constraints embedded ("25% clear space ABOVE head", "EMPTY SPACE on sides", etc.)
- **Fallback** (cron/script mode): `_build_imagen_prompt()` generates prompt inline when agent is not spawned

---

## Figma Upload (Stage 8g)

`figma.createImage()` in the Plugin API MCP sandbox has a ~100 byte image size limit — large PNGs render black.

**Correct approach:** Create the creative as native Figma elements via `use_figma` MCP:
- 1200×1200 white canvas frame
- Rounded-corner gradient rectangle (dark ambient background simulating photo area)
- Gradient overlay rectangles (pink/coral top-left + teal/blue bottom-left for Angle A)
- Headline: Inter Bold, white, centered
- Subheadline: Inter Regular, white, centered
- Bottom strip: white rect + "Earn $X–$Y USD per hour." bold + "Fully remote." regular + "Outlier" wordmark

Frame naming: `{project_id}_{angle}_v{version}` (e.g. `69cf1a039ed66cc82e0fa8f3_A_v1`)  
Figma file key: `j16txqhVXak2TON1w5sdAH`

---

## Memory Update (Stage 11)

After every run, `campaign-manager` writes to:
- `memory/project_outlier_campaign_agent.md` — run summary, cohorts, TG labels, blocked items
- `memory/feedback_outlier_agent_api_gotchas.md` — any new API quirks discovered

---

*Last updated: 2026-04-21*

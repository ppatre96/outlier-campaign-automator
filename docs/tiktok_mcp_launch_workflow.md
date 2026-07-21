# TikTok campaign launch via the TikTok for Business MCP (stopgap runbook)

**Status:** stopgap until the TikTok **app API** access lands and the pipeline's
TikTok arm can place campaigns natively (like Meta/Google). Until then, TikTok
campaigns are created **manually, agent-assisted**, through the TikTok for Business
MCP inside a Claude Code session.

## What this is (and isn't)

- ✅ **On-request campaign creation.** In a Claude Code session, you ask the agent
  to create TikTok campaigns from the creatives the pipeline already renders to the
  Shared Drive. The agent calls the MCP tools to build them (paused).
- ❌ **Not wired into the pipeline.** `smart_ramp_poller` / `launch.yml` (GitHub
  Actions) call each platform's REST API directly; they do **not** use this MCP. A
  ramp launch will not auto-create a TikTok campaign.
- ❌ **Not autonomous / not headless.** The MCP tools exist only inside a running
  Claude Code session, and the OAuth is an interactive, browser-authorized, 30-day
  credential. It does **not** work when the window is closed or in CI/cron.

## One-time setup

1. **Config (done).** `~/.claude/mcp.json` → `tiktok-ads` →
   `https://business-api.tiktok.com/open_mcp/tt-ads-mcp-layer` (the *layer* /
   Progressive-Disclosure URL — the flat URL loads ~400 tools and Claude omits
   tools past 256; the layer loads ~40 and discovers the rest on demand).
2. **Restart Claude Code** so the server loads (MCP servers load at session start).
3. **Authorize (per 30 days).** Ask the agent to authenticate TikTok; it returns an
   OAuth URL → open it → sign into TikTok Ads Manager / TikTok for Business →
   review permissions → **Authorize**. No developer app needed. Re-authorize every
   30 days.

## Per-launch workflow

1. **Launch the ramp normally** from the console (Meta/Google/etc.). The pipeline
   renders TikTok creatives (**9:16 and 1:1**) to the Shared Drive under
   `<RAMP_ID>/tiktok/<cohort_geo>/` with a `_HANDOFF.md`.
2. **In a Claude Code session, ask the agent**, e.g.
   *"Create TikTok campaigns for GMR-XXXX from the Drive creatives."*
3. **Agent builds them (paused):** resolves the creatives from Drive, creates the
   campaign → ad group(s) → ads via the MCP with the budget/targeting/schedule you
   provide, and returns the TikTok Ads Manager links.
4. **You review + un-pause** in TikTok Ads Manager.

## Checklist — what the agent needs from you per launch

Copy-paste this and fill it in when you ask for a TikTok launch:

```
Ramp:            GMR-XXXX
Cohorts/locales: (which cohorts × geos to run on TikTok; e.g. Thai SE-Asian, th-TH)
Advertiser ID:   (TikTok advertiser / ad-account id to create under)
Objective:       (traffic | conversions | lead-gen | reach | ...)
Optimization:    (event to optimize for, if conversions/lead-gen)
Budget:          (daily or lifetime; per campaign or per ad group)
Bid strategy:    (lowest cost | cost cap $X | ...)
Targeting:       (countries/locales, age range, gender, interests — or Smart+ auto)
Landing page:    (URL per cohort; UTMs matching the pipeline UTM format)
Schedule:        (start/end; default = create PAUSED)
Pixel/CAPI:      (which pixel + conversion event, if a conversions objective)
Creative format: (9:16 in-feed video base / 1:1 carousel — see note below)
```

Defaults the agent will assume unless you say otherwise: **create everything
PAUSED**, one ad group per (cohort × geo), UTMs per the pipeline's format, copy
following Outlier's approved vocabulary.

## Gotchas

- **Video-first placement.** TikTok in-feed can't run a single static image. The
  pipeline's PNGs are for **carousel slides** or **video base frames** — confirm
  which per launch (see the creative's `_HANDOFF.md`).
- **India-banned account.** The Outlier TikTok ad account is banned in India;
  target other geos only.
- **Session + auth bound.** Works only with a live session + valid 30-day OAuth.
  Nothing runs when the window is closed or in CI.
- **Copy vocabulary.** Any campaign/ad copy must follow Outlier's "Don't Say" list
  (see the repo `CLAUDE.md`).
- **When the app API lands:** wire the native TikTok API into the pipeline's TikTok
  arm for real automation (parity with Meta/Google), then decide whether to retire
  or keep this MCP path.

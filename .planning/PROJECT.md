# Outlier Campaign Agent

## What This Is

An automated LinkedIn campaign pipeline for Outlier (Scale AI) that discovers high-signal audience cohorts from screening data, generates ad copy and image creatives via LLM, and publishes LinkedIn InMail and Static Ad campaigns — all from a single trigger. The system is designed to run autonomously: from cohort discovery through creative generation to LinkedIn publishing, with weekly performance reports posted to Slack.

## Core Value

End-to-end campaign automation from screening data to live LinkedIn campaign — zero manual steps once triggered.

## Requirements

### Validated

- ✓ Stage A/B cohort discovery — SQL-first feature engineering + `_is_pass` pre-computation (43 min → 8 sec) — Session 2026-04-20
- ✓ InMail copy generation via LiteLLM (claude-sonnet-4-6) — angles F/A/B/C working
- ✓ Campaign group + campaign creation via LinkedIn REST API v202510 — Session 2026-04-20
- ✓ Image upload to LinkedIn (sponsoredAccount owner) — Session 2026-04-20
- ✓ Gemini image generation via LiteLLM `/images/generations` (imagen-4.0-generate-001) — Session 2026-04-20
- ✓ URN resolution from Google Sheets (dynamic column detection) — Session 2026-04-20
- ✓ Weekly InMail performance report generation (Redash/Snowflake)

### Active

**Phase 1 — Pipeline Integrity**
- [ ] Fix all silent-skip bugs so the full pipeline runs end-to-end without NameError or skipped steps
- [ ] `classify_tg` import added to main.py (DONE — 2026-04-21)
- [ ] `mj_token` / `claude_key` guard removed so creative generation runs unconditionally
- [ ] `SCREENING_END_DATE` guard — always pass `date.today()` explicitly
- [ ] `create_image_ad` unblocked — either via `rw_organization_admin`/`r_liteprofile` LinkedIn scope, or `LINKEDIN_MEMBER_URN` correctly set
- [ ] audienceCounts Stage C working or gracefully falling back (MDP approval OR confirmed bypass)
- [ ] Slack weekly report delivery working (SLACK_WEBHOOK_URL filled or Slack MCP approach confirmed)
- [ ] LinkedIn token auto-refresh confirmed working (client_id + secret set, refresh path tested)

**Phase 2 — Observability & Reporting**
- [ ] Automated weekly Slack report posting (no manual trigger required)
- [ ] Per-creative performance tracking written back to Sheets after campaigns run
- [ ] Campaign lifecycle monitor active — flag underperforming campaigns
- [ ] Static ad weekly report populated (currently empty — no static campaigns running)

**Phase 3 — Expansion**
- [ ] STEM InMail regen with financial angle (campaigns A=633412886, B=635201096, C=634012966)
- [ ] Google Drive upload for generated creatives (Shared Drive setup + GDRIVE_ENABLED=true)
- [ ] Additional TG buckets beyond current MEDICAL / SOFTWARE_ENGINEER / LANGUAGES / MATH / GENERAL
- [ ] LinkedIn MDP approval — unblocks audienceCounts for true Stage C validation

### Out of Scope

- Snowflake direct connection — replaced by Redash proxy; keep Redash as the data layer
- Midjourney MCP — Gemini via LiteLLM is working; Midjourney integration deferred indefinitely
- Multi-account LinkedIn support — single account (510956407) only for now
- Figma MCP creative generation — `figma_creative.py` exists but pipeline uses `midjourney_creative.py` for image gen

## Context

**Pipeline flow (Stage A → LinkedIn publish):**
```
Screening data (Redash/Snowflake)
  → Stage A: SQL feature engineering → cohort scoring
  → Stage B: beam search (12 candidates) → top campaigns
  → Stage C: audienceCounts validation (blocked on MDP; falls back to Stage B)
  → Creative gen: LiteLLM copy (claude-sonnet-4-6) + Gemini image (imagen-4.0)
  → LinkedIn API: campaign group → campaign → image upload → creative attach
  → Sheets write-back: creative URNs logged
  → Weekly reports: Redash queries → Slack
```

**Key env constraints:**
- `LINKEDIN_ACCESS_TOKEN` — expires ~June 2026 (expires_at=1781441848); auto-refresh via refresh token available
- `LINKEDIN_MEMBER_URN` — required for `create_image_ad`; token owner identity unknown (needs `r_liteprofile` or `rw_organization_admin` scope)
- `SLACK_WEBHOOK_URL` — placeholder; workspace restrictions blocked webhook setup
- `GDRIVE_ENABLED=false` — needs Shared Drive + service account as Content Manager
- `SCREENING_END_DATE=2025-12-31` — stale; always override with `date.today()`

**Key findings from performance data:**
- Financial angle (rate in subject line) consistently beats Expertise/Flexibility on CTR/CPA
- "V4 Side Income" framing always worst — never use
- Geo is #1 variable: India CTR=21.7% ($0.55 CPA) vs US CTR=3.36% ($14 CPA)
- Languages TG: $2.83 CPA (best). Coders T2: $8.18. Math: $14.14

**Sub-agent concerns being addressed:**
- Silent skips: `classify_tg` missing import, `mj_token` guard, `SCREENING_END_DATE` stale — all cause silent failures
- Context rot: This PROJECT.md + ROADMAP.md are the canonical source of truth for all agents
- No verification: Pipeline must write observable outputs (Sheets URNs, Slack posts, local PNG) at each step

## Constraints

- **LinkedIn API**: REST API v202510 — `w_member_social`, `rw_ads`, `r_ads`, `r_ads_reporting` scopes only; `r_liteprofile` and `rw_organization_admin` not yet granted
- **LinkedIn MDP**: audienceCounts requires MDP approval for account 510956407 — pending
- **Data**: All queries must filter to account_id=510956407; Redash DS 30 = GenAI Ops Snowflake
- **Copy vocabulary**: Must follow CLAUDE.md terminology (no "job", "role", "compensation", "training", etc.)
- **Image format**: 1200×1200 PNG, close-up portrait, plant-background, Avenir Next font, centered text, left-side gradient

## Key Decisions

| Decision | Rationale | Outcome |
|----------|-----------|---------|
| SQL-first feature engineering | Vectorized pandas was 10+ min; Snowflake CTEs = 3 sec | ✓ Good |
| LiteLLM proxy for all LLM calls | Internal Scale proxy handles routing; no direct Anthropic/Gemini keys needed | ✓ Good |
| `/images/generations` for Gemini | `/chat/completions` with modality flag stripped by proxy | ✓ Good |
| sponsoredAccount owner for images | `urn:li:organization:` requires `r_organization_social` scope we don't have | ✓ Good |
| `/rest/posts` DSC for image ads | `/v2/ugcPosts` author validation fails; `/rest/posts` with `lifecycleState:DRAFT` is correct path | — Pending (blocked on LINKEDIN_MEMBER_URN) |
| One `or` block per facet in audienceCounts | LinkedIn native UI merges facets but API requires separate blocks | ✓ Good |
| Redash over direct Snowflake | Avoids credential management; Redash handles auth | ✓ Good |

---
*Last updated: 2026-04-21 — Phase 3.1 Figma Creative Integration complete, Phase 3 ready*

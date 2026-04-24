# Requirements: Outlier Campaign Agent

**Defined:** 2026-04-21
**Core Value:** End-to-end campaign automation from screening data to live LinkedIn campaign — zero manual steps once triggered.

## v1 Requirements

### Pipeline Integrity

- [x] **PIPE-01**: `main.py` imports `classify_tg` correctly — no NameError on InMail campaign runs
- [x] **PIPE-02**: Creative generation runs unconditionally — no `mj_token`/`claude_key` guard blocking Gemini
- [x] **PIPE-03**: Screening data always fetched with `end_date=date.today()` — never cut off by stale `SCREENING_END_DATE`
- [ ] **PIPE-04**: Full dry run completes end-to-end without any silent skips or unhandled exceptions
- [ ] **PIPE-05**: LinkedIn token auto-refresh tested and confirmed working before expiry

### LinkedIn Creative Upload

- [x] **LI-01**: `create_image_ad` successfully creates a DSC post and attaches it to a campaign as a creative
- [x] **LI-02**: `LINKEDIN_MEMBER_URN` correctly identifies the OAuth token owner (via `r_liteprofile`, `rw_organization_admin`, or manual lookup)
- [x] **LI-03**: Image ad creative URN written back to Google Sheets after successful upload
- [x] **LI-04**: audienceCounts Stage C either approved (MDP) or gracefully bypassed with logged reason

### Observability

- [x] **OBS-01**: Slack weekly report posted automatically every Monday 9 AM IST without manual trigger
- [x] **OBS-02**: `SLACK_WEBHOOK_URL` filled in `.env` OR Slack Bot Token has `chat:write` scope for DM posting
- [x] **OBS-03**: Static ad weekly report populated when static campaigns are active
- [x] **OBS-04**: Campaign lifecycle monitor running — underperforming campaigns flagged in Slack report

### Data & Storage

- [x] **DATA-01**: Google Drive upload enabled for generated creatives — `GDRIVE_ENABLED=true`, Shared Drive shared with service account
- [x] **DATA-02**: Generated PNG files and Drive URLs logged to Sheets after each creative run

### Figma Creative Integration

- [ ] **IMG-01**: `outlier-creative-generator.md` agent instructions updated to use `build_figma_layered_frame_js()` with `photo_base64` input documented
- [ ] **IMG-02**: Pipeline passes `context["photo_base64"]` to agent without NameError; base64 conversion works for typical image sizes
- [ ] **IMG-03**: End-to-end test creates 3 Figma frames (A/B/C) with visible photo backgrounds, angle-specific gradients, and editable text layers

### Campaign Expansion

- [ ] **EXP-01**: STEM InMail variants regenerated with financial angle (F/A/C) for campaigns 633412886, 635201096, 634012966
- [ ] **EXP-02**: `classify_tg` extended with any new TG buckets needed for additional cohort types

## v2 Requirements

### Audience Validation

- **AUD-01**: LinkedIn MDP access approved for account 510956407 — audienceCounts returns real counts
- **AUD-02**: Stage C full validation runs (not fallback) — cohorts validated before campaign creation
- **AUD-03**: Unique audience overlap check (MIN_UNIQUE_AUDIENCE_PCT=80%) enforced

### Advanced Analytics

- **ANLT-01**: Per-creative A/B/C angle performance tracked automatically — CTR/CPA by angle written to Sheets
- **ANLT-02**: Conversion tracking (APPLICATION_CONVERSION view) integrated into weekly report
- **ANLT-03**: Automated cohort comparison: current week vs prior week pass rate delta

### Infrastructure

- **INFRA-01**: LinkedIn OAuth re-authorization flow documented + automated for token refresh beyond June 2026
- **INFRA-02**: Redash query caching — avoid re-running expensive queries within same session

### Phase 2.5 V2 — Full-Funnel, Sentiment, ICP Drift, Cron (2026-04-24)

- [ ] **FEED-15**: `feedback_agent` reports signup / screening-pass / activation conversion rates per creative × cohort (full-funnel decomposition), not just CTR + CPA
- [ ] **FEED-16**: `feedback_agent` identifies the funnel stage where each underperforming cohort loses contributors (top-of-funnel vs signup vs screening vs activation) and injects the stage-of-drop into the weekly Slack alert
- [ ] **FEED-17**: `sentiment_miner` scrapes public sources weekly — Reddit (r/Outlier_AI, r/BeerMoney, r/WorkOnline), Trustpilot, Glassdoor, Outlier Community (Discourse) — and extracts issue themes via LLM. Apple App Store and Google Play reviews are excluded from V2 scope (Outlier has no native iOS or Android app — verified via WebSearch 2026-04-24); revisit if/when a native app ships
- [ ] **FEED-18**: `sentiment_miner` ingests internal Zendesk/Intercom tickets (auth-gated via `.env`) for current-contributor issues
- [ ] **FEED-19**: `sentiment_miner` writes `data/sentiment_callouts.json` — scored issue themes that `ad-creative-brief-generator` consumes as copy directives (address X / avoid Y)
- [ ] **FEED-20**: `icp_drift_monitor` snapshots Stage 1 ICP output weekly and computes a drift score over key categorical + numeric feature distributions (skills, degree, experience band, job title, pay band)
- [ ] **FEED-21**: `icp_drift_monitor` auto-triggers `outlier-data-analyst` reanalysis when drift exceeds a configurable threshold — no Slack reaction required
- [ ] **FEED-22**: `scripts/weekly_feedback_loop.py` cron orchestrator runs full-funnel + sentiment + ICP drift every Monday 9 AM IST (cron `30 3 * * 1`), idempotent within a 6-day window
- [ ] **FEED-23**: Consolidated weekly Slack report combines v1 creative/cohort alerts with V2 funnel-drop diagnosis, top sentiment themes, and ICP drift notifications into a single Monday post

## Out of Scope

| Feature | Reason |
|---------|--------|
| Midjourney MCP image generation | Gemini via LiteLLM works; Midjourney deferred indefinitely |
| Direct Snowflake connection | Redash proxy handles auth; adding direct connection adds credential risk |
| Multi-LinkedIn-account support | Single account (510956407) only; multi-account adds complexity with no near-term need |
| Figma MCP creative design (v1) | Replaced by Phase 3.1: Figma Creative Replication Integration (editable frames with photo + gradient + text) |
| Real-time campaign monitoring | Weekly cadence sufficient; real-time adds infra complexity |
| Web UI / dashboard | Script-based CLI is sufficient; web UI is out of scope |

## Traceability

| Requirement | Phase | Status |
|-------------|-------|--------|
| PIPE-01 | Phase 1 | Complete |
| PIPE-02 | Phase 1 | Complete |
| PIPE-03 | Phase 1 | Complete |
| PIPE-04 | Phase 1 | Pending |
| PIPE-05 | Phase 1 | Pending |
| LI-01 | Phase 1 | Blocked |
| LI-02 | Phase 1 | Blocked |
| LI-03 | Phase 1 | Complete |
| LI-04 | Phase 1 | Complete |
| OBS-01 | Phase 2 | Complete |
| OBS-02 | Phase 2 | Complete |
| OBS-03 | Phase 2 | Complete |
| OBS-04 | Phase 2 | Complete |
| DATA-01 | Phase 2 | Complete |
| DATA-02 | Phase 2 | Complete |
| IMG-01 | Phase 3.1 | Pending |
| IMG-02 | Phase 3.1 | Pending |
| IMG-03 | Phase 3.1 | Pending |
| EXP-01 | Phase 3 | Pending |
| EXP-02 | Phase 3 | Pending |
| FEED-15 | Phase 2.5 V2 | Pending |
| FEED-16 | Phase 2.5 V2 | Pending |
| FEED-17 | Phase 2.5 V2 | Pending |
| FEED-18 | Phase 2.5 V2 | Pending |
| FEED-19 | Phase 2.5 V2 | Pending |
| FEED-20 | Phase 2.5 V2 | Pending |
| FEED-21 | Phase 2.5 V2 | Pending |
| FEED-22 | Phase 2.5 V2 | Pending |
| FEED-23 | Phase 2.5 V2 | Pending |

**Coverage:**
- v1 requirements: 17 total
- v3 requirements: 3 Figma integration requirements
- Phase 2.5 V2 requirements: 9 (FEED-15..FEED-23)
- Mapped to phases: 29
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-21*
*Last updated: 2026-04-24 — added Phase 2.5 V2 requirements (FEED-15..FEED-23) for full-funnel, sentiment mining, ICP drift, and cron orchestration*

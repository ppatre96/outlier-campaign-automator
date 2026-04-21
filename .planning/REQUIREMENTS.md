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
- [ ] **OBS-04**: Campaign lifecycle monitor running — underperforming campaigns flagged in Slack report

### Data & Storage

- [ ] **DATA-01**: Google Drive upload enabled for generated creatives — `GDRIVE_ENABLED=true`, Shared Drive shared with service account
- [ ] **DATA-02**: Generated PNG files and Drive URLs logged to Sheets after each creative run

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

## Out of Scope

| Feature | Reason |
|---------|--------|
| Midjourney MCP image generation | Gemini via LiteLLM works; Midjourney deferred indefinitely |
| Direct Snowflake connection | Redash proxy handles auth; adding direct connection adds credential risk |
| Multi-LinkedIn-account support | Single account (510956407) only; multi-account adds complexity with no near-term need |
| Figma MCP creative design | `figma_creative.py` exists but is superseded by Gemini image gen pipeline |
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
| OBS-04 | Phase 2 | Pending |
| DATA-01 | Phase 2 | Pending |
| DATA-02 | Phase 2 | Pending |
| EXP-01 | Phase 3 | Pending |
| EXP-02 | Phase 3 | Pending |

**Coverage:**
- v1 requirements: 17 total
- Mapped to phases: 17
- Unmapped: 0 ✓

---
*Requirements defined: 2026-04-21*
*Last updated: 2026-04-21 after initial definition*

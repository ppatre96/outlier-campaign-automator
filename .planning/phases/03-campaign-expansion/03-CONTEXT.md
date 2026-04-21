# Phase 3: Campaign Expansion - Context

**Gathered:** 2026-04-21  
**Status:** Ready for planning  
**Source:** Research + Project context

---

<domain>
## Phase Boundary

Regenerate existing STEM InMail campaigns (633412886, 635201096, 634012966) with the proven financial angle (rate-in-subject-line) and review the TG classifier to add MATH bucket and ensure all Stage A cohort types map to non-GENERAL buckets.

**Scope:**
- EXP-01: Create new InMail creative with angle F for three existing campaigns
- EXP-02: Add MATH bucket to classify_tg(), update TG_PALETTES and TG_ILLUS_VARIANTS

**Out of scope:**
- New campaign creation (use campaign-manager for that)
- Full A/B test harness (Phase 2.5 handles experimentation)
- Statistical significance testing (Phase 2.5 provides signals)

</domain>

<decisions>
## Implementation Decisions

### STEM InMail Regeneration (EXP-01)

- **Targeted regen script:** Create `scripts/regen_stem_inmail.py` that calls `build_inmail_variants(..., angle_keys=["F"])` directly and attaches new creative to existing campaigns via `li_client.create_inmail_ad(campaign_urn=...)` 
- **Campaign IDs:** 633412886, 635201096, 634012966 — hardcode as reasonable default; planner can make configurable if needed
- **TG assumption:** Assume ML_ENGINEER or SOFTWARE_ENGINEER for STEM campaigns (exact labels should be verified from Triggers 2, but not a blocking issue)
- **Sheets logging:** New InMail URNs logged to Sheets Creatives tab via standard `write_creative()` call
- **No campaign group creation:** Attach to existing campaign groups; do not regenerate groups

### TG Classifier Review (EXP-02)

- **MATH bucket is LOCKED:** Add before SOFTWARE_ENGINEER check with regex `/\b(math|statistics|statistician|quantitative|actuary|actuarial)\b/i`
- **TG_PALETTES addition:** Add MATH entry with appropriate color/styling (planner to decide or match existing)
- **TG_ILLUS_VARIANTS addition:** Add MATH entry with illustration guidance (planner to decide or match existing)
- **DATA_SCIENTIST decision:** DATA_ANALYST's `\bdata\b` pattern matches adequately; no new bucket needed (but planner can revisit if copy differentiation is valuable)
- **Fallback angle F:** Recommend adding angle F fallback to `_fallback_subject()` and `_fallback_body()` (low effort, consistency improvement)

### Prerequisites & Blockers

- ✓ `LINKEDIN_INMAIL_SENDER_URN` is set (`urn:li:person:vYrY4QMQH0`) — EXP-01 not blocked
- ✓ Angle F is fully implemented and proven in PROJECT.md performance data
- ✓ `build_inmail_variants()` and `create_inmail_ad()` are fully functional
- ⚠ Campaign TG labels (633412886, 635201096, 634012966) not hardcoded — planner to verify from Triggers 2 or document assumption

### Claude's Discretion

- **Fallback copy for angle F:** Planner may choose to skip if deployment risk is too high
- **New TG bucket beyond MATH:** If Stage A produces cohort types beyond current coverage, planner may add more buckets (FINANCE, LEGAL, DESIGN, etc.)
- **Regression testing:** Scope of testing after TG changes (full pipeline, dry-run, Sheets-only, etc.)

</decisions>

<canonical_refs>
## Canonical References

**Downstream agents MUST read these before planning or implementing.**

### Phase 2.5 Prerequisite (Experiment Integration)
- `.planning/phases/02.5-feedback-loops-experimentation/02.5-CONTEXT.md` — experiment_scientist_agent feeds test directives to ad-creative-brief-generator; financial angle is baseline, not a test variant

### Core Files for EXP-01
- `src/inmail_copy_writer.py` — `build_inmail_variants()` with angle F implementation (confirm `ANGLE_CONFIGS["F"]` and prompt template)
- `src/linkedin_api.py` — `create_inmail_ad()` method signature and campaign URN format
- `src/sheets.py` — `write_creative()` for logging new InMail URNs
- `.env` — `LINKEDIN_INMAIL_SENDER_URN` is set; `LINKEDIN_ACCESS_TOKEN` and refresh token must be valid

### Core Files for EXP-02
- `src/figma_creative.py` — `classify_tg()` function (line 57), all 6 current bucket regexes, and `TG_PALETTES` dictionary
- `src/inmail_copy_writer.py` — `TG_ILLUS_VARIANTS` dictionary for illustration guidance per bucket
- `src/midjourney_creative.py` — Confirm no TG bucket usage (should only be in inmail path)
- `.planning/PROJECT.md` — Performance data by TG (Math $14.14 CPA, current vs. new buckets)
- `.planning/REQUIREMENTS.md` — EXP-02 acceptance criteria for TG coverage

### Research Artifact
- `.planning/phases/03-campaign-expansion/03-RESEARCH.md` — Full investigation of angle F, TG gaps, STEM campaign state

</canonical_refs>

<specifics>
## Specific Requirements

### EXP-01: STEM InMail Financial Angle Regen
- Target campaigns: 633412886, 635201096, 634012966
- Angle: F (financial / rate-in-subject-line)
- Output: New InMail URN per campaign, logged to Sheets Creatives tab
- Flow: `build_inmail_variants(..., angle_keys=["F"])` → `create_inmail_ad(campaign_urn=...)` → `write_creative()`

### EXP-02: TG Classifier Gap Closure
- Add MATH bucket with pattern `/\b(math|statistics|statistician|quantitative|actuary|actuarial)\b/i`
- Insert MATH check BEFORE SOFTWARE_ENGINEER (to prevent `python` false positives)
- Update `TG_PALETTES[MATH]` — add styling (planner decides: match existing or new color)
- Update `TG_ILLUS_VARIANTS[MATH]` — add illustration guidance
- Verify all Stage A cohort types from recent runs map to non-GENERAL buckets

</specifics>

<deferred>
## Deferred Ideas

- Full automation of cohort TG classification (Phase 4+) — MVP uses manual review + MATH addition
- Automated palette/illustration selection (Phase 4+) — MVP adds hardcoded MATH entry
- New cohort bucket candidates (FINANCE, LEGAL, DESIGN, PSYCHOLOGY) — defer until Stage A produces them consistently

</deferred>

---

*Phase: 03-campaign-expansion*  
*Context gathered: 2026-04-21 from research findings*

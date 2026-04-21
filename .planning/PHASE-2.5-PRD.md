# Phase 2.5: Feedback Loops & Experimentation — PRD

## Problem Statement

The current pipeline generates creatives and runs campaigns, but has no mechanism to:
1. Identify which creatives and cohorts are performing well vs. poorly
2. Generate hypotheses about *why* performance varies
3. Design controlled experiments to test those hypotheses
4. Feed learnings back into future creative generation and cohort selection
5. Detect underperforming cohorts and alert the user for intervention

This creates a one-way flow: data → campaign → fire-and-forget. We need a feedback loop.

---

## Solution Overview

Introduce two new specialist agents and a weekly feedback loop:

### 1. **feedback_agent** (Recursive Performance Analyst)
- **Scope 1: Creatives Performance**
  - Ingests: All creative variants (A/B/C) grouped by cohort + their performance metrics (impressions, clicks, spend, CPA)
  - Analyzes: Which headlines, subheadlines, photos work best for each cohort
  - Generates hypotheses: Why does photo_subject "X" outperform "Y" for DATA_ANALYSTs?
  - Outputs: Creative feedback JSON to experiment_scientist_agent
  
- **Scope 2: Cohorts Performance**
  - Ingests: Cohort-level performance (cohort name, rules, pass_rate, CPA, CTR, spend trend)
  - Analyzes: Which cohorts are performing above/below baseline, trending up/down
  - Generates alerts: "Marketing cohort CPA $22 (+35% vs. baseline $16.30) — recommend pause"
  - Outputs: Weekly Slack alert to user with:
    - Underperforming cohort name + metric + recommendation (PAUSE/TEST_NEW)
    - Link to Sheets for drill-down

### 2. **experiment_scientist_agent** (Hypothesis → Experiment Design)
- Ingests from 3 sources:
  1. **feedback_agent** output (creative + cohort hypotheses)
  2. **competitor_bot** output (what competitors are testing, new angles)
  3. **outlier_data_analyst** (new screening data arrivals, new ICP discovery)
  
- Accumulates: All external signals + prior experiment results into experiment backlog
- Decides: "Based on feedback + competitor data, we should test: [angle], [photo_subject], [targeting change]"
- Communicates: "Test hypothesis" message to **ad-creative-brief-generator**
  - "For DATA_ANALYST cohort, test 'side income' angle with photo_subject='person at home with laptop'"
  - "For LANGUAGE cohort, test earnings claims from Surge AI creatives"

### 3. Integration Points

**ad-creative-brief-generator** receives test hypothesis:
- Instead of always using best-performing angle, occasionally use test angle
- Incorporates experiment_scientist's hypothesis into brief
- Tracks which briefs are experiments vs. baseline

**outlier_data_analyst** receives weekly cohort reanalysis request:
- After feedback_agent identifies underperforming cohort
- Rerun Stage A on fresh screening data (new resumes since last run)
- Discover new cohorts that might outperform stale ones
- Return new cohort definitions to campaign-manager for next run

---

## Requirements

### Creative Performance Loop
- **FEED-01**: feedback_agent reads creative performance from `VIEW.LINKEDIN_CREATIVE_COSTS` (cohort × angle × metric)
- **FEED-02**: feedback_agent generates hypothesis JSON `{angle, photo_subject, reason_hypothesis, expected_impact}`
- **FEED-03**: experiment_scientist_agent receives hypothesis JSON + competitor_bot JSON
- **FEED-04**: experiment_scientist_agent outputs test decision: `{cohort, angle, photo_subject, priority}`

### Cohort Performance Loop
- **FEED-05**: feedback_agent reads cohort-level metrics from `VIEW.APPLICATION_CONVERSION` (cohort × CPA, CTR, spend, trend)
- **FEED-06**: feedback_agent identifies underperforming cohorts (CPA > baseline + 2σ OR CTR trending down)
- **FEED-07**: feedback_agent posts weekly Slack alert with underperforming cohort list + recommendation
- **FEED-08**: User acknowledges Slack alert (👍 = pause cohort, 🧪 = test new angles)

### Reanalysis Loop
- **FEED-09**: On user action (pause), outlier_data_analyst reruns Stage A with fresh screening data
- **FEED-10**: New cohorts discovered from fresh data fed back to campaign-manager
- **FEED-11**: campaign-manager schedules new campaigns for newly discovered cohorts

### Experiment Backlog & Tracking
- **FEED-12**: experiment_scientist_agent maintains experiment backlog (priority queue of hypotheses)
- **FEED-13**: ad-creative-brief-generator checks backlog; occasionally uses test variant instead of baseline
- **FEED-14**: After test runs, results fed back to feedback_agent to validate hypothesis

---

## Workflow

### Weekly Cycle (Runs Every Monday with Slack reports)

```
1. feedback_agent runs (Monday 9 AM IST)
   ├─ Creative analysis: identify best/worst performers
   ├─ Cohort analysis: identify underperformers
   ├─ Generate hypotheses
   └─ Post Slack alert with underperforming cohorts

2. experiment_scientist_agent runs
   ├─ Reads feedback_agent output
   ├─ Reads competitor_bot latest.json
   ├─ Reads experiment backlog from memory
   ├─ Decides what to test
   └─ Updates memory with new experiment decisions

3. Per-run cycle (on-demand, when campaign triggered)
   ├─ ad-creative-brief-generator checks experiment backlog
   ├─ 20% of briefs use test angle/subject (80/20 rule)
   ├─ 80% of briefs use known-good baseline
   └─ Track experiment flag in creative metadata

4. User action (async)
   └─ Reacts to Slack alert → pause or test new angles
   └─ Triggers outlier_data_analyst reanalysis
   └─ New cohorts discovered → fed to campaign-manager
```

---

## Scope & Constraints

### In Scope
- feedback_agent (2 scopes: creatives, cohorts)
- experiment_scientist_agent
- Weekly Slack alerts for underperforming cohorts
- Experiment backlog & decision tracking
- Integration with existing agents (ad-creative-brief-generator, outlier_data_analyst)
- 80/20 baseline vs. test rule
- Memory persistence (experiment results, backlog)

### Out of Scope (Phase 3+)
- Automated campaign pause (user must confirm via Slack reaction)
- Statistical significance testing (MVP uses rule-of-thumb thresholds)
- Full A/B test harness (we run campaigns, we don't control test/control assignment)
- Causal inference beyond simple correlation hypotheses

---

## Success Criteria

1. **Creative feedback loop working:**
   - feedback_agent analyzes creative performance weekly
   - Generates 3+ hypothesis per cohort (why is angle X better for cohort Y?)
   - experiment_scientist_agent receives hypotheses and decides which to test

2. **Cohort feedback loop working:**
   - feedback_agent identifies underperforming cohorts (CPA > threshold OR CTR trend)
   - Slack alert posts weekly with top 3 underperformers + recommendation
   - User can react with emoji to pause/test

3. **Experiment integration:**
   - ad-creative-brief-generator receives test directive from experiment_scientist_agent
   - 20% of briefs use test variant, 80% use baseline
   - Experiment flag tracked in creative metadata

4. **Reanalysis loop:**
   - User triggers reanalysis via Slack reaction
   - outlier_data_analyst reruns Stage A on fresh screening data
   - New cohorts surface → fed to campaign-manager for next run

5. **Memory persistence:**
   - Experiment backlog stored in memory (not lost on restart)
   - Hypothesis results logged for next week's analysis
   - Competitor insights accumulated over time

---

## Data Sources

- `VIEW.LINKEDIN_CREATIVE_COSTS` — Creative-level performance (impressions, clicks, spend, CPA by creative)
- `VIEW.APPLICATION_CONVERSION` — Cohort-level performance (CTR, CPA, spend, trend)
- `PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE` — Detailed creative metrics
- `data/competitor_intel/latest.json` — Competitor experiment ideas (from competitor_bot)
- Memory: experiment backlog, hypothesis results, test decisions

---

## Technical Dependencies

- Phase 2 must be complete first (Slack bot integration, Drive setup, lifecycle monitoring)
- Redash DS 30 (GenAI Ops Snowflake) must be accessible
- Memory system must be persistent (survive process restarts)
- ad-creative-brief-generator must be extendable to receive experiment directives

---

## Timeline & Effort Estimate

**Phase 2.5: Feedback Loops & Experimentation**
- feedback_agent implementation: 4-6 hours
- experiment_scientist_agent implementation: 3-4 hours
- Integration + testing: 2-3 hours
- **Total: 9-13 hours**

---

*PRD created: 2026-04-21*

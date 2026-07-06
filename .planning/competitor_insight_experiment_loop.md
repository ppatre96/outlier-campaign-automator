# Plan — Close the loop: competitor insights → tracked experiment arm → weekly result readback

**Status:** DRAFT for approval (plan-first). No code changes yet.
**Author:** Pranav (via Claude)  **Date:** 2026-07-06

## Goal
Turn a competitor insight from the weekly Slack digest into an in-market, *measured* test,
then report win/loss back in the same Monday Slack post. Cover all 5 channels:
LinkedIn, Meta, Google, TikTok, Reddit (with honest limits on the last two).

## Current state (verified in code)
- Competitor intel produced by `run_competitor_intel()` (`src/competitor_intel.py:1292`),
  written to `data/competitor_intel/latest.json`; `copy_recommendations` stored under the
  key `experiment_ideas` (`competitor_intel.py:199`).
- CONSUMED today ONLY as LLM prompt text: copy writer (`figma_creative.py:534`) and brief
  generator (`brief_generator.py:177`); each emits a `competitor_signal` tag per variant.
- NOT consumed: targeting, audiences, Google keywords (`search_terms` is a dead-end),
  rendered pixels. `competitor_signal` is metadata → console card only.
- Experiment unit already exists: angles **A/B/C** per cohort×geo, logged to registry
  `angle` column (`campaign_registry.py:84`).
- Purpose-built but DEAD: `src/experiment_scientist_agent.py` — `ingest_feedback()` reads
  `competitor_intel['experiment_ideas']`, scores into `ExperimentBacklog` (`src/memory.py:17`),
  `generate_test_directive(cohort)` returns {angle, photo_subject, priority, test_allocation}.
  Zero call sites repo-wide.
- Readback gap: weekly funnel `FeedbackAgent.analyze_funnel_by_cohort(days_back=7)` aggregates
  by cohort, NOT by angle. No `competitor_signal`/`experiment_id` column in registry to join on.

## Design (reuse angles, don't invent a framework)
- **Angle C = competitor-signal challenger arm.** A/B stay business-as-usual; C's brief/copy is
  forced to the top-priority competitor hypothesis. One test cell per cohort, no new campaign objects.
- Bridge the shape mismatch: `experiment_ideas` in latest.json are currently **strings**
  (copy_recommendations), but `ExperimentScientistAgent.ingest_feedback` expects **dicts**
  {cohort, angle, photo_subject, description, expected_impact}. Add a normalizer.

## Workstreams

### WS1 — Activate the scientist (main.py wiring)
- Instantiate `ExperimentScientistAgent` once per run before copy/brief gen.
- `ingest_feedback(feedback_hyps, competitor_intel_dict)` after Phase-5 intel refresh
  (`main.py:6066-6078`).
- Per cohort at brief/copy time (`main.py:5556` briefs, `main.py:628`/`:3577` copy):
  call `generate_test_directive(cohort_name)` → pass its {angle=C, photo_subject, reason}
  into the angle-C brief so C is the competitor-derived variant.
- Persist backlog (`ExperimentBacklog.save()` → `data/experiment_backlog.json`).
- Normalizer: map string `copy_recommendations` → dicts keyed to current cohort (angle=C default).

### WS2 — Tag the arm in the registry (append-only)
- APPEND (never insert mid-list — see append-only rule) two columns to `campaign_registry.py:84`:
  `experiment_id` and `competitor_signal`. Populate on log for angle-C rows.
- Thread `experiment_id`/`competitor_signal` from the directive → `log_campaign(...)`
  (`campaign_registry.py:350`, call sites in main.py per platform).
- This is the join key readback needs.

### WS3 — Readback by angle (measurement, the real new capability)
- Add `FeedbackAgent.analyze_by_angle(days_back=7)` (or extend analyze_funnel_by_cohort):
  join registry rows (cohort, angle, platform, Platform Campaign Id, is_test_arm) →
  platform performance (impressions/clicks/CTR/conversions) via existing Redash/metrics.
  Dedup Meta rows by Platform Campaign Id first (~2 rows/campaign).
- Compute challenger C vs baseline mean(A,B) per cohort×channel: CTR lift, CVR lift, n, significance flag.
- Write outcome back to backlog: `mark_completed` + result; so a losing competitor idea is retired,
  a winner is promoted.

### WS4 — Report in the Monday Slack post
- New "🧪 Experiment Results" section in `_build_consolidated_message()`
  (`scripts/weekly_feedback_loop.py:382`), fed by WS3.
- Format: `{cohort} · {channel} · C ({competitor_signal}) vs A/B → CTR +X% / CVR +Y% (n=…, sig?)`.
- Runs on the existing Mon 03:30 UTC cron (`.github/workflows/weekly_feedback_loop.yml`).

### WS5 — Channel coverage
| Channel | Challenger creative ships | Auto readback | Notes |
|---|---|---|---|
| LinkedIn (InMail+Static) | ✅ angle C | ✅ | best fit; copy-driven insight |
| Meta (Display) | ✅ angle C | ✅ | dedup registry rows by campaign id |
| Google (Display+Search) | ✅ angle C | ✅ | BONUS: wire dead `search_terms` → Search keyword arm |
| TikTok | ✅ creative only | ❌ manual | creative-only/Drive export; no programmatic id → no auto metrics |
| Reddit | ✅ creative only | ❌ manual | Phase-1 creative-only; same limit |

For TikTok/Reddit the challenger creative is generated + tagged, but result readback is manual
until those channels go programmatic. `log()` this limit — don't imply full coverage.

## Risks / caveats
- Angle C = ~33% of a cohort's spend, not the agent's nominal 20% test_allocation — reconcile
  (treat C as the test cell; ignore the 20/80 field or gate C to a spend cap).
- Small n per cohort×angle×week → significance may be weak; report n and a caution flag, don't over-claim.
- Registry column add must be APPEND-only or header auto-sync misreads old rows.
- Shape mismatch (strings vs dicts) must be handled or ingest silently no-ops.

## Success criteria
1. A competitor hypothesis appears in `data/experiment_backlog.json` with a priority score.
2. That hypothesis drives angle-C copy for ≥1 cohort on LinkedIn+Meta+Google in a live/dry run.
3. Registry angle-C rows carry `experiment_id` + `competitor_signal`.
4. Monday Slack post shows an "Experiment Results" line comparing C vs A/B for that cohort.
5. Backlog hypothesis flips to completed with a recorded outcome.

## Rough effort
WS1 ~0.5d, WS2 ~0.25d, WS3 ~1d (the real work), WS4 ~0.25d, WS5-Google-keywords ~0.5d (optional bonus).
~2–2.5 days total; WS3 (angle-level readback) is the critical path.

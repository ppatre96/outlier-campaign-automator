# Session handoff — campaign metrics + competitor-experiment loop (2026-07-07)

You are resuming work on the **outlier-campaign-agent** pipeline and its **console**. Read this fully before acting.

## Goal
Make every campaign surface its full metric set across all channels — impressions/clicks/CTR/spend AND sign-ups/skill-passes/activations — in the registry, Postgres, and the console. Started from a competitor-insight experiment loop, expanded into fixing the whole metrics pipeline. All requested work is DONE and merged; remaining work is one root-cause fix (issue #75) + steady-state verification.

## Repos, access, run commands (CRITICAL — get these right)
- Pipeline: `/Users/pranavpatre/outlier-campaign-agent` → GitHub `ppatre96/outlier-campaign-automator`.
- Console: `/Users/pranavpatre/outlier-campaign-console` → GitHub `ppatre96/outlier-campaign-console` (Next.js on Vercel; merges to `main` auto-deploy to Production in a few min).
- **gh push/merge requires the `ppatre96` account, NOT `Pranavpatre`.** If a push 403s: `gh auth switch --user ppatre96`. (Two accounts are authed; `Pranavpatre` was active at session start and is denied.)
- **Run Python via `doppler run -- venv/bin/python …` (Python 3.13.7).** Do NOT use `python3` — it resolves to 3.14 with no pytest/dotenv. Tests: `doppler run -- venv/bin/python -m pytest …`.
- Redash: base `https://redash.scale.com`, API key `9W4SOwp0VJzrQCJFrZ4sD4HdPtDnCQOnL744fafP`, data_source_id `30` (Snowflake GenAI Ops). Funnel view: `SCALE_PROD.VIEW.APPLICATION_CONVERSION`.
- Daily refresh: `.github/workflows/daily_feedback.yml`, cron `30 8 * * *` (08:30 UTC) but historically LANDS 10:30–12:23 UTC. Runs `scripts/refresh_metrics.py`.

## Current state (all merged to main)
Everything below is live. Pipeline HEAD `5f4b17f` (#78). Console HEAD `cedd089` (#17).

**Metrics pipeline architecture:**
- `scripts/refresh_metrics.py` (daily) = hydrate_from_postgres → `refresh_linkedin_metrics(window)` → `fetch_metrics_for_active_extra_platforms(window)` (Meta/Google/Reddit delivery) → `backfill_funnel_metrics_all_channels(funnel_window)`. Args: `--window` (default **30**, all delivery) + `--funnel-window` (default **30**, funnel).
- Delivery metrics come from ad-platform reporting APIs → `campaign_registry.update_metrics`. Funnel (sign-ups/skill-passes/activations) comes from `APPLICATION_CONVERSION` → `campaign_registry.update_funnel_metrics`.
- **Join keys (per channel, all validated live by outlier-data-analyst):**
  - LinkedIn: `by="name_norm"` (normalized campaign_name = UTM_CAMPAIGN). AD_ID/creative never matched (static rows are DRAFT; InMail creative ids differ). Angle A/B/C granularity is UNRECOVERABLE for relaunched ramps.
  - Meta / Reddit: `by="name_norm"` (UTM_CAMPAIGN = campaign_name).
  - Google: `by="campaign"` (CAMPAIGN_ID) for campaign/bare rows; `by="adgroup"` (ADGROUP_ID) for `.../adGroups/<id>` rows. `by="campaign"` EXCLUDES adGroup rows.
  - TikTok: creative-only, no attribution (reported as such).
- `campaign_registry._normalize_campaign_name`: lowercase, drop `agent_` prefix, "message ads"→"message ad", strip date tokens (`dd/dd/dddd`) anywhere, drop empty pipe segments. Tolerates relaunch date-drift + format variants. Safe: only same-locale relaunches merge, never distinct locales/formats.
- Config lives in `redash_db._CHANNEL_FUNNEL` + `CHANNEL_JOIN_MODE`; `query_campaign_funnel(channel)` returns `ad_key` + apps/screening_passes/activations. LinkedIn per-creative funnel is `FUNNEL_METRICS_SQL`/`analyze_funnel_by_cohort` (still used by read_results + drop-diagnosis, NOT by the writeback).

**LinkedIn InMail delivery (PR #76):** Most LinkedIn is InMail (`SPONSORED_INMAIL`), which has NO impressions/clicks — it reports **sends/opens** + `LANDING_PAGE_CLICKS` (generic `CLICKS`=0). New `_METRICS_REFRESH_SQL` in campaign_feedback_agent.py includes SPONSORED_INMAIL/SPONSORED_MESSAGE, carries campaign_name in the deduped ROW_NUMBER CTE (do NOT re-join CAMPAIGN_HISTORY — it fans out SUMs ~100x), floor = impressions+sends≥100. Registry has append-only `sends`/`opens` columns. update_metrics gained `sends`/`opens`/`by` params, first-representative-row.

**Reddit (PR #71):** IS programmatic — `src/reddit_api.py` `RedditClient` (not RedditAdsClient) creates campaigns; `RedditClient.fetch_campaign_metrics(window)` → POST `https://ads-api.reddit.com/api/v3/ad_accounts/{acct}/reports`, breakdowns=[CAMPAIGN_ID], fields=[impressions,clicks,spend]; **spend is MICROS ($1=1_000_000)**. `platform_metrics._fetch_reddit_metrics` caches one call per window.

**Competitor-experiment loop (PR #69):** `src/competitor_experiment.py` activates the previously-dead `ExperimentScientistAgent`; competitor `experiment_ideas` → prioritized `ExperimentBacklog` → top pinned to `data/experiment_directive.json`; `directive_prompt_block` pins angle C in brief_generator + figma_creative; `read_results` compares challenger-C vs baseline LinkedIn CTR; weekly_feedback_loop step E. Also fixed garbage `experiment_ideas` (raw ramp-summary leak) via `competitor_intel._clean_tg_label` + `_is_clean_term`.

**Console:** `campaigns` table stores the whole entry as `data` JSONB → new fields need NO migration. `lib/db.ts` coerceCampaignRows + `lib/sheets.ts` RegistryRow type carry sends/opens. `channel-performance.tsx` = **Analytics** tab (metrics, has a Sends column). `performance-card.tsx` = **Recommendations** tab (filters out `action="keep"` + `classification="insufficient_data"` — non-actionable). Post-launch tabs: Analytics · Recommendations · Campaigns · Details · Audit (ViewKey in ramp-workspace.tsx; status "completed"→"analytics").

**Live prod data I wrote this session (verified):** GMR-0023 LinkedIn 1.87M sends/152k clicks/861k opens + 461 funnel activations; Meta 1,052 activations + 9.6M impr/$47k; Google 29 activations + 46k impr/$7.3k (billing frozen ~05-14 → low); Reddit delivery 0 (id mismatch — see watch-outs).

## Windows (all 30d now)
Delivery (LinkedIn/Meta/Google/Reddit) = 30d; funnel/activations = 30d. Rationale: metrics accrue over a campaign's whole flight, so 7d dropped older ramps ~10x. There is NO principled reason to differ per channel — unified in #78.

## Git this session (all squash-merged)
Pipeline (`ppatre96/outlier-campaign-automator`, HEAD `5f4b17f`):
- #69 `feat: close competitor-insight → experiment → weekly-readback loop`
- #70 `feat: all-channel campaign metrics — impressions/clicks + sign-ups/skill-passes/activations` (funnel writeback, LinkedIn+Meta+Google)
- #71 `feat: Reddit campaign metrics (impressions/clicks/spend + sign-ups/activations)`
- #72 `feat: switch Google funnel attribution to CAMPAIGN_ID join (full coverage)`
- #73 `fix: use a wider (30d) window for funnel activations, decoupled from delivery`
- #74 `fix: reconcile funnel attribution join for relaunched ramps (name-norm + adGroup)`
- #76 `fix: LinkedIn delivery metrics — include InMail (sends/opens) + relaunch-tolerant match`
- #77 `fix: widen LinkedIn delivery window to 30 days`
- #78 `fix: unify delivery window to 30d for all channels (Meta/Google/Reddit too)`
- **Issue #75** (NOT a PR, still OPEN): "Relaunch write-back gap: relaunched campaigns mint new platform IDs the registry never records" — the root cause behind all the join workarounds.
Console (`ppatre96/outlier-campaign-console`, HEAD `cedd089`):
- #16 `feat: show LinkedIn InMail sends/opens in channel performance`
- #17 `feat: split Performance tab into Analytics + Recommendations; hide non-actionable recs`

## The user's errors you corrected
- None significant. The user's directions were sound. (One near-miss: they chose "ramp-level" activations from an AskUserQuestion, but that was based on YOUR incorrect framing that per-campaign was infeasible — not their error. You then built the better per-campaign version.)

## YOUR errors the user caught (do NOT repeat)
1. **You claimed per-campaign activation attribution was impossible** ("signup_flow-level only, no per-creative linkage"). WRONG — the FEED-15 funnel already attributes per-creative via `AD_ID`. Verify claims against the actual SQL before asserting limits.
2. **You told the user Reddit was "creative-only, no attribution."** WRONG — Reddit is fully programmatic with real campaign IDs and funnel attribution. Don't trust stale memory (`feedback_reddit_channel_phase1`) over the live registry/code.
3. **You ran un-verified/buggy writeback code against PROD Postgres repeatedly**, polluting it (240M inflated sends from a CAMPAIGN_HISTORY fan-out + all-rows writes). You had to reset 66 rows and repopulate. RULE: verify writeback SQL/match logic on a read-only query FIRST; never loop buggy writes against prod.
4. **You created an unprincipled 7d-LinkedIn vs 7d-others → 30d-LinkedIn split** reactively; the user asked "why" and you had to admit it wasn't justified, then unify. Think about consistency before shipping asymmetric defaults.

## YOUR misses the user had to raise (stay alert)
1. LinkedIn delivery showed nothing — the user had to ask "why is linkedin not showing clicks/impressions." You hadn't proactively audited the LinkedIn delivery path (InMail excluded by FORMAT filter + 50k HAVING floor + campaign_id match).
2. For GMR-0023 "still can't see" you first blamed timing/window and MISSED the dominant cause (relaunch name-drift) until the user pushed.
3. The user had to explicitly request splitting Analytics/Recommendations and hiding non-actionable recs — you didn't proactively notice the noise ("$0 spent — need ≥$20 to judge" cards).
4. Console rendering/verification — the user had to ask you to verify the console actually shows the data after each change.

## Next step (exact)
Everything requested is merged and live. If continuing:
1. **Verify tomorrow's daily run** (after ~13:00 UTC, once `daily_feedback.yml` completes) kept all channels at 30d and repopulated cleanly. You CANNOT schedule a cloud agent for this — GitHub isn't connected in the cloud env and there's no doppler/venv there. Verify in-session with: `gh run list --workflow=daily_feedback.yml -L 3`, then `gh run view <id> --log | grep -iE "funnel\[|refresh_metrics done"`, then the Postgres check via `doppler run -- venv/bin/python -c "from src.ui_decisions import list_all_campaign_data; ..."`.
2. **Address issue #75 (relaunch write-back root cause)** — the durable fix: on relaunch, write new platform campaign/creative IDs back to the registry + archive the prior generation, and store the exact `utm_campaign` in a registry column at creation so joins key on a stored value instead of normalizing names. This removes the name-normalization workarounds and would fix Reddit's blank delivery.

## Watch-outs
- **Never run un-verified writeback against prod Postgres.** Dry-check the query + match logic read-only first.
- **gh account = `ppatre96`**; **Python = `venv/bin/python` under doppler**.
- **Reddit delivery is blank** — the reports API returned campaign IDs that don't match the 2 recorded Reddit registry rows (relaunch/ID-recording gap, #75). Not a bug in the fetcher; needs #75.
- **Google is billing-frozen since ~05-14** → low/zero data; joins are format-verified and will populate when spend resumes.
- **Fan-out trap:** joining `CAMPAIGN_HISTORY` (many versions/campaign) or un-deduped `CREATIVE_HISTORY` inflates SUMs. Always dedup with `ROW_NUMBER() … rn=1` and carry needed fields through that CTE.
- **Registry columns are APPEND-ONLY** (header auto-sync misreads old rows otherwise).
- **Campaign-level writes use first-representative-row** (`first_only`) to avoid console-rollup double-counting when a campaign spans multiple angle/format rows.
- **InMail:** impressions/clicks are legitimately 0; sends/opens + LANDING_PAGE_CLICKS are the real metrics; spend fields on Reddit are MICROS.
- Memory file `feedback_campaign_activations_writeback.md` has the full technical detail; `feedback_competitor_intel_not_in_targeting_experiment_dead.md` covers the experiment loop.

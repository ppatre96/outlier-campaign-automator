# Pay-rate resolver — open questions for Quintin

Context: `src/attribution_resolver.py::resolve_pay_rate(signup_flow_id)` was returning incorrect T1 rates (e.g. Math T1 = $50 instead of $30). Root-caused on 2026-05-27:

- `QUALIFICATION_PAY_RATES` is one row per **(qual_id, tier)** with tier encoded in the `QUALIFICATION_NAME` suffix (`"Mathematics overall: T3"`), not 4 rows per qual_id.
- The original `qpr_with_tier` CTE was partitioning by `QUALIFICATION_ID` (always size 1) and ranking by `PAY_RATE ASC` — every row got `tier_rank=1`, so whatever rate happened to be bound to a project's qual_id got labeled `T1_RATE_USD`.

Pivot now fixed in `queries/snowflake_pay_rate_resolver.sql` — uses `REGEXP_SUBSTR` on the name suffix to extract real tier_rank, groups by `base_name` (e.g., "Mathematics overall") to pivot the 4 tier rows correctly. Math T1 now returns $30 (matches `SCALE_PROD.VIEW.QUALIFICATION_PAY_RATES`).

But this surfaced two downstream design questions the resolver can't decide on its own:

---

## Q1 — How should the resolver pick the right qualification when a project has many?

**Repro:**
- `signup_flow_id = 66e2220635f2fca6aaa86116` ("i18n Math")
- Fallback CTE picks project `66e9debbb5e52068a84d80d9` (one of many converted projects)
- That project has 13+ qualifications bound to it
- Resolver heuristic `ORDER BY COALESCE(T1_RATE_USD, 0) DESC` → picks **Physics** (T1=$40)
- Mathematics is rank 13 in the candidate list (T1=$30, correct value, but never selected)

**Question:** what's the right way to select the qual matching the flow's intent? Candidates:

a. **`SIGNUPFLOWS.INTENDED_WORKER_SKILLS`** — does this column reliably carry the targeted skill for i18n-style flows (e.g., "Mathematics" worker_skill)? If yes, filter candidates to that set.

b. **Flow NAME → qual NAME match** — regex/substring match. Fragile for generic flows like "Multimango ToFu" but cheap.

c. **Lowest T1 instead of highest** — flip ORDER BY direction; assume the lowest-tier qual is the targeted "entry rate".

d. Other signal you know about (e.g., a SIGNUPFLOWS variant column, or a join through a `FLOW_PROJECT_ASSIGNMENTS` view)?

For the broad generalist flows (Multimango ToFu — 147 distinct project_ids, 291k conversions), is there any reasonable way to attribute a single rate, or should the resolver just return `None` / "rate-free copy" for those?

---

## Q2 — Non-determinism + dedup in the fallback path

**Two cosmetic issues in the SQL that affect reproducibility:**

a. **`ac_project` CTE uses `LIMIT 1` with no `ORDER BY`** — same signup_flow_id can return a different project_id (and hence different rate) on different runs. Should we add `ORDER BY ac.CREATED_AT DESC` so the latest converted project wins? Or `ORDER BY n_conversions DESC` (most-frequent project)? Or different signal entirely?

b. **Candidate list has 4× duplication per qual family** — for a project bound to all 4 Mathematics qual_ids (T1/T2/T3/T4), `qual_rates_pivoted` emits 4 rows (one per bound qual_id) that all map to the same family rates. Should we `SELECT DISTINCT base_name, project_id` to collapse? Or is the multiple-bound-tier signal meaningful (e.g., proj accepts T1+T2+T3 CBs)?

---

## Smoke-test current state (post-pivot-fix, pre-Q1+Q2)

| signup_flow_id | flow_name | resolved qual | T1 (correct?) |
|---|---|---|---|
| `690ea1299b09cb9f8dd4e323` | Multimango ToFu | Coding | $25 |
| `66207c94f3c74b889712febd` | Generalists T1 Greenhouse | Generalist | $15 |
| `66e2220635f2fca6aaa86116` | i18n Math | Physics ❌ should be Math | $40 ❌ should be $30 |

Files touched today:
- `queries/snowflake_pay_rate_resolver.sql` — pivot rewrite + `JOB_POST_IDS` underscore fix
- `src/attribution_resolver.py:58` — docstring underscore fix
- `~/.claude/agent-memory/outlier-data-analyst/{pay_rates.md, attribution_queries.md}` — schema gotcha + the new tier-pivot lesson

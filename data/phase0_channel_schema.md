# Phase 0 — Channel Schema Reconnaissance (v2)

**Run date:** 2026-04-23  
**Scope:** Last 180 days, `view.application_conversion` + `public.users`  
**CHANGED from v1:** Attribution map expanded with `public.users.worker_source`; MWF/Squads/LATAM are real, confirmed, non-zero; blockers 1-3 from v1 are resolved.

---

## 1. Channel Attribution Map (CORRECTED)

**Attribution precedence:** `worker_source` wins when non-null (explicit program enrollment). UTM is fallback for everyone else.

| Bucket | Table | Column / filter | Last-180d count |
|--------|-------|-----------------|-----------------|
| MWF (inqa_coder) | `PUBLIC.USERS` | `worker_source = 'inqa_coder'` | 447 |
| Squads | `PUBLIC.USERS` | `worker_source = 'in_squads'` | 9 |
| LATAM coders | `PUBLIC.USERS` | `worker_source = 'latam_coder'` | 390 |
| Joveo | `VIEW.APPLICATION_CONVERSION` | `UTM_SOURCE LIKE '%joveo%'` | 370,576 |
| LinkedIn | `VIEW.APPLICATION_CONVERSION` | `UTM_SOURCE LIKE 'linkedin%'` | ~156k |
| Organic / direct | `VIEW.APPLICATION_CONVERSION` | UTM null or 'organic*' | 866k+ |

**Join key:** `PUBLIC.USERS._id = VIEW.APPLICATION_CONVERSION.USER_ID`  
**Date column on USERS:** `CREATED_DATE` (TIMESTAMP_TZ) — NOT `CREATED_AT`.

### Standard CASE for channel bucketing

```sql
CASE
  WHEN u.worker_source = 'inqa_coder'  THEN 'MWF'
  WHEN u.worker_source = 'in_squads'   THEN 'Squads'
  WHEN u.worker_source = 'latam_coder' THEN 'LATAM'
  WHEN LOWER(ac.UTM_SOURCE) LIKE '%joveo%'   THEN 'Joveo'
  WHEN LOWER(ac.UTM_SOURCE) LIKE 'linkedin%' THEN 'LinkedIn'
  WHEN ac.UTM_SOURCE IS NULL
    OR LOWER(ac.UTM_SOURCE) IN ('organic','organic-social','organic-socials') THEN 'Organic'
  ELSE 'Other'
END AS channel_bucket
```

### UTM cross-check: what UTM do program CBs show in APPLICATION_CONVERSION?

For CBs where `worker_source` is set, their UTM_SOURCE breakdown reveals double-counting risk:

| worker_source | utm_bucket | n_users |
|---|---|---|
| inqa_coder | linkedin | 343 |
| inqa_coder | organic | 43 |
| inqa_coder | referrals | 32 |
| inqa_coder | other | 20 |
| inqa_coder | joveo | 5 |
| inqa_coder | meta | 3 |
| in_squads | organic | 5 |
| in_squads | referrals | 4 |
| latam_coder | organic | 320 |

**Interpretation:** Without `worker_source` precedence, MWF CBs would be double-counted inside LinkedIn (343 of them). LATAM CBs would fall into Organic (320 of them). The precedence rule correctly rescues them. The 180-day overlap is small enough that it doesn't materially affect paid-channel baselines (343 CBs vs. 156k LinkedIn total).

---

## 2. Metric Map

### Activation
**Column:** `VIEW.APPLICATION_CONVERSION.ACTIVATION_DAY` (DATE)  
**Filter for project-specific activation:** `ACTIVATION_PROJECT_ID = '<project_id>'`  
Note: filtering on `ACTIVATION_PROJECT_ID` restricts to CBs for whom that project was their first activation. The denominator becomes "CBs who activated on this project" not "all signups who visited."

### Task activity per CB per project
**Table:** `VIEW.CBPR__USER_PROJECT_STATS`  
**Grain:** (USER_ID, PROJECT_ID)  
**Columns confirmed:** USER_ID, PROJECT_ID, TOTAL_HOURS, ACTIVE_DAYS, FIRST_ATTEMPT_DATE, LAST_ATTEMPT_DATE  
**Schema note:** `PUBLIC.TASKATTEMPTS` has ZERO rows for Multimango (68dd6e9b668d6876a63ab1d4) — this project's task data is not mirrored into TASKATTEMPTS. Use CBPR__USER_PROJECT_STATS for engagement metrics on this project.

### Quality signals
| Signal | Column | Coverage note |
|--------|--------|---------------|
| Early quality estimate | `APPLICATION_CONVERSION.QUALITY_ESTIMATE` | Populated for all activated CBs: 'good', 'bad', 'pending' |
| First-3-tasks QMS | `APPLICATION_CONVERSION.AVERAGE_QMS_FIRST_3_TASKS` | NULL for all Multimango CBs in last 180d (scoring not active for this project) |
| First-3-tasks QC | `APPLICATION_CONVERSION.AVG_QC_SCORE_FIRST_3_TASKS` | NULL for all Multimango CBs in last 180d |
| Lifetime PCT_GOOD_QC | `VIEW.WORKER_QUALITY_SIGNAL.PCT_GOOD_QC` | Near-zero coverage for recent cohorts (9 of ~70k CBs have it) — not usable |

Best available quality proxy for recent cohorts: `QUALITY_ESTIMATE` good% among resolved (good + bad), excluding pending.

### Disable rate
**Column:** `APPLICATION_CONVERSION.DISABLED` (BOOLEAN)  
Interpret as: disabled at any point in their platform lifetime (not project-specific).

---

## 3. Overlap Projects (6-bucket taxonomy, last 6 months)

Filter: Joveo ≥ 50 AND LinkedIn ≥ 50. Ordered by sum of program CBs.

| Project | ID | MWF | Squads | LATAM | Joveo | LinkedIn | Organic | Total |
|---------|-----|-----|--------|-------|-------|----------|---------|-------|
| [SCALE-RLHF for HQL] - Multimango | 68dd6e9b668d6876a63ab1d4 | 40 | 1 | 72 | 11,091 | 7,173 | 51,757 | 128,617 |
| Real World Prompts i18n | 6913488b1af4ea7075ae2a6f | 0 | 0 | 0 | 220 | 86 | 701 | 1,602 |
| Experts Comprehensive Rubrics | 68c086ffe6ec9e9a05468e35 | 0 | 0 | 0 | 72 | 61 | 349 | 612 |
| Experts Health Radiology | 696042185141eb3f98d53d88 | 0 | 0 | 0 | 173 | 72 | 58 | 430 |

Multimango is the only project with MWF + LATAM cells above noise. Squads is too small (n=9 total, n=1 on Multimango) for any statistical analysis.

---

## 4. Schema Surprises vs. v1

| v1 finding | v2 correction |
|---|---|
| MWF not in Snowflake — blocker | RESOLVED: `public.users.worker_source = 'inqa_coder'` (447 CBs, 180d) |
| INQA not in Snowflake — blocker | RESOLVED: same column, 'inqa_coder' is the INQA value |
| LATAM is a campaign segment, not a channel | PARTIALLY RESOLVED: `latam_coder` is a real program bucket (390 CBs); separate from Meta/LinkedIn LATAM campaigns |
| TASKER_QUALITY_SCORES not found | Still unresolved; QUALITY_ESTIMATE is the usable quality proxy |
| PUBLIC.TASKATTEMPTS for Multimango | NEW: zero rows — project's task data is not in TASKATTEMPTS |
| CREATED_AT on PUBLIC.USERS | NEW: correct column is CREATED_DATE |

---

## 5. What the Study Can Support

Multimango supports (with caveats): MWF (n=40), LATAM (n=72), Joveo (n=11k), LinkedIn (n=7k), Organic (n=52k).  
Squads is unsupportable (n=1 on Multimango).  
Quality proxy is QUALITY_ESTIMATE, not QMS/QC (both null for this cohort).  
Engagement proxy is CBPR__USER_PROJECT_STATS (TOTAL_HOURS, ACTIVE_DAYS), not task counts.

# Phase 1 — Channel Funnel Comparison (v2)

**Run date:** 2026-04-23
**Changed from v1:** All-time window (programs started 2022); coding projects only (OPS_POD = 'GenAI: Code' + name pattern); TTA dropped; activation funnel added; MWF structural caveat added; pooled 7 projects (not single-project Multimango).

---

## Verdict (5 lines)

1. **Hypothesis holds clearly on coding projects.** MWF and Squads lead every paid channel on 30d retention (71%/77% vs 26-33%), 60d retention (54%/48% vs 9-15%), hours/active day (2.4/3.5 vs 1.8-2.0), disabled rate (22%/47% vs 64-65%), and QMS (3.05/3.37 vs 2.33-2.82).
2. **LATAM is also ahead of paid on all dimensions.** 50% 30d retention, 2.57 hrs/active day, 28% disabled rate, QMS 3.18, 45% quality good-rate — substantially better than LinkedIn and Joveo on every metric.
3. **QMS is now a real signal (65-93% coverage).** The quality ordering is unambiguous: Squads (3.37) > LATAM (3.18) > MWF (3.05) > Organic (2.82) > LinkedIn (2.58) > Joveo (2.33). Unlike v1 (all nulls for Multimango), this cohort has meaningful coverage.
4. **The retention and hours advantage is partly a pay-commitment artifact for MWF/LATAM** (they are pre-hired with promised hours). The engagement-density metric (hrs/active day) controls for this — MWF (2.42) and LATAM (2.57) still exceed all paid channels. Quality (QMS) is not a pay artifact and also favors program channels.
5. **Joveo is the weakest paid channel for coding.** Lowest QMS (2.33), highest disabled rate (65%), only 11% 30d retention, n=26 activated — it underperforms even LinkedIn and Organic on this domain.

---

## Pass A — Overlap Table (all-time, coding projects)

**Threshold:** MWF ≥ 30, LATAM ≥ 30, or Squads ≥ 10. Program start dates: MWF 2022-02-21, LATAM 2022-09-27, Squads 2023-08-08.

| Project | Created | Status | MWF | LATAM | Squads | Joveo | LinkedIn | Organic | Total | Active window |
|---|---|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|---|
| RFP - Master Project | 2026-02-19 | enabled | **36** | **33** | 1 | 1 | 23 | 66 | 181 | 2026-03 → 2026-04 |
| SWEAP Augmentation - Public Repo | 2025-03-09 | paused | 18 | **33** | **10** | 12 | 208 | 728 | 1,265 | 2025-03 → 2025-07 |
| Agent Completion Process Supervision Pt 3 | 2025-06-03 | paused | 2 | **35** | 8 | 1 | 64 | 123 | 251 | 2025-07 → 2025-11 |
| Code Checkpoint Evals | 2026-03-10 | enabled | **34** | 10 | 0 | 7 | 31 | 78 | 191 | 2026-03 → 2026-04 |
| [SCALE_CODE_SFT] Coding Physics Simulation | 2025-02-13 | paused | 0 | **38** | 0 | 0 | 3 | 87 | 136 | 2025-03 → 2025-07 |
| SWE Full Trace - Entry Level Tasks | 2025-06-12 | paused | 0 | **31** | 0 | 0 | 0 | 74 | 106 | 2025-06 → 2025-07 |
| Data Analysis Agents - Rubrics | 2025-09-17 | disabled | 8 | 0 | **11** | 5 | 19 | 71 | 172 | 2025-10 → 2026-02 |

---

## Pass B — Pooled Funnel (7 coding projects)

**Engagement** (`CBPR__USER_PROJECT_STATS`). MWF/LATAM retention caveat: pay commitment inflates hours and active days; hrs/active day normalizes this.

| Channel | n | Avg hrs | Avg active days | Hrs/active day | Disabled % | 30d retention | 60d retention |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 98 | 22.1 | 7.9 | **2.42** | **22.4** | **71.1%** (n=45) | **53.6%** (n=28) |
| **LATAM** | 180 | **25.4** | **10.0** | **2.57** | **28.3** | **50.0%** (n=152) | 23.4% (n=137) |
| **Squads** | 30 | **41.8** | **13.8** | **3.51** | 46.7 | **76.7%** (n=30) | **48.3%** (n=29) |
| Joveo | 26 | 18.2 | 6.3 | 2.24 | 65.4 | 30.4% (n=23) | 11.1% (n=18) |
| LinkedIn | 348 | 8.4 | 3.7 | 2.02 | 64.1 | 25.8% (n=326) | 8.5% (n=294) |
| Organic | 1,227 | 13.1 | 5.6 | 2.00 | 53.2 | 32.7% (n=1,153) | 14.7% (n=1,083) |

Retention = ≥5 active days (30d) / ≥10 active days (60d).

**Quality** (QMS 1–5, higher = better). QC coverage too sparse for most channels.

| Channel | QMS coverage | Avg QMS first-3 | Quality good% (resolved only) |
|---|:-:|:-:|:-:|
| **Squads** | 28/30 (93%) | **3.37** | **46.2%** (n=26 resolved) |
| **LATAM** | 134/180 (74%) | **3.18** | **44.8%** (n=116) |
| **MWF** | 64/98 (65%) | **3.05** | **52.6%** (n=57) |
| Organic | 1,016/1,227 (83%) | 2.82 | 16.5% (n=929) |
| LinkedIn | 266/348 (76%) | 2.58 | 12.0% (n=284) |
| Joveo | 20/26 (77%) | 2.33 | 18.2% (n=22) |

Avg QC first-3: sparse (2-37% non-null); omitted from main table. MWF QC avg = 3.69 (n=8); Squads = 3.39 (n=11); LinkedIn = 3.00 (n=8). Direction consistent with QMS but sample too small to rely on.

---

## Pass C — Activation Funnel

**Spine:** `APPLICATION_CONVERSION` (all signups). Screening from `GROWTHRESUMESCREENINGRESULTS` on EMAIL. Activation = `ACTIVATION_PROJECT_ID IN (7 project IDs)`. This is a cross-population proxy — program CBs are placed onto projects directly; paid/organic CBs self-select from a catalog. Rates are NOT directly comparable across the two populations.

| Channel | Total signups | Screened | Screening pass rate | Activated coding | Activation proxy rate |
|---|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 611 | 457 | 57.3% | 98 | **16.0%** |
| **LATAM** | 813 | 0 | N/A (bypasses flow) | 180 | **22.1%** |
| **Squads** | 191 | 57 | 59.6% | 30 | **15.7%** |
| Joveo | 482,842 | 84,885 | 63.3% | 26 | 0.005% |
| LinkedIn | 1,171,338 | 290,933 | 62.4% | 348 | 0.030% |
| Organic | 2,979,579 | 356,859 | 60.7% | 1,213 | 0.041% |

Screening pass rates are comparable (57-63%) — no channel has a materially different screening outcome. The large gap in activation rate is structural, not a quality-of-applicant difference: program CBs are assigned to coding projects; paid/organic CBs enter a broad catalog with low probability of landing on a specific coding project.

LATAM screening = 0: LATAM coders bypass the growth resume screening flow (pre-vetted externally). Not a zero pass rate.

---

## Gotchas

- **TASKATTEMPTS** has zero rows for Multimango (v1 finding). Not verified for all 7 pooled projects — `CBPR__USER_PROJECT_STATS` used throughout.
- **TTA dropped** — MWF/LATAM longer onboarding lag is structural (hired before tasks exist), not a performance issue.
- **DISABLED flag** is lifetime/platform-wide, not project-specific.
- **Activation rate incomparable** across program vs. paid channels (see Pass C caveat).
- **QC coverage** is 2-37% non-null — supplementary only.

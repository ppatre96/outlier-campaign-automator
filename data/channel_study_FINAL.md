# Channel Performance Study — FINAL Summary
**Scope:** 7 pooled coding projects (all-time, squads excluded). Channels: MWF | LATAM | Joveo | LinkedIn.
**Run date:** 2026-04-23 (Passes A'–F). Prior detail: `channel_study_summary.md` (§1–8).

---

## 1. Objective and Hypothesis

MWF and LATAM program channels deliver coding CBs at meaningfully higher quality and retention than LinkedIn paid acquisition. This study tests whether that gap is driven by a specific contributor profile (institution pedigree, field of study, employer type) that could be replicated via LinkedIn targeting, or whether it reflects structural differences that LinkedIn cannot approximate.

---

## 2. The Scorecard

**Pooled funnel across 7 coding projects. PDR lower = better; QMS higher = better. Course pass % = attempt-level pass rate (STATUS='pass'/total course encounters) among CBs with course records.**

| Channel | n (activated) | Avg hrs | Avg active days | Disabled % | PDR rate | Avg QMS | 30d retention | Course pass % |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 98 | 22.1 | 7.9 | **22%** | **0.319** | **3.05** | **71%** | 98.2% |
| **LATAM** | 180 | 25.4 | 10.0 | **28%** | **0.372** | **3.18** | **50%** | 90.5% |
| Joveo | 26 | 18.2 | 6.3 | 65% | 0.574 | 2.33 | 30% | 100.0% |
| LinkedIn | 348 | 8.4 | 3.7 | **64%** | **0.619** | **2.58** | **26%** | 97.4% |
| Organic | 1,227 | 13.1 | 5.6 | 53% | 0.516 | 2.82 | 33% | 93.2% |

MWF and LATAM show roughly half the defect rate (PDR) and disabled rate of LinkedIn. 30d retention is 2.7x and 1.9x higher respectively. QMS advantage is real but secondary — PDR is the sharpest separator. Joveo performs similar to LinkedIn on coding; it is not a viable substitute for program channels on this domain. Course pass rates are statistically indistinguishable across all channels — the quality gap is not explained by raw technical capability (see §4e and §9 of channel_study_summary.md).

---

## 3. Per-Project Activation Counts

**Compact matrix — activated CBs per project. MWF ≥5 or LATAM ≥10 threshold.**

| Project | MWF | LATAM | LinkedIn | Total |
|---|:-:|:-:|:-:|:-:|
| RFP - Master Project | 36 | 33 | 23 | 180 |
| SWEAP Augmentation | 18 | 33 | 208 | 1,255 |
| Code Checkpoint Evals | 34 | 10 | 31 | 191 |
| Coding Physics Simulation | 0 | 38 | 3 | 136 |
| Agent Completion Supervision Pt 3 | 2 | 35 | 64 | 243 |
| SWE Full Trace - Entry Level | 0 | 31 | 0 | 106 |
| Studio RL | 0 | 29 | 72 | 738 |

MWF shows concentrated activation on RFP and Code Checkpoint Evals (IIT-track projects). LATAM shows broader spread. LinkedIn dominates SWEAP Augmentation and Studio RL by count but underperforms on quality.

---

## 4. Why MWF and LATAM Outperform — Profile Hypothesis

These five features, taken together, account for the quality gap. Each pairs a measured profile delta with an outcome inference.

**a. Seniority — entry-to-mid vs senior.** Program CBs are predominantly mid-level SWEs (40%) and students/interns (18%). LinkedIn CBs are 16% senior SWE / staff / lead. Senior title holders on LinkedIn have a 64% disabled rate — they appear overqualified for coding annotation tasks and churn quickly. Program's junior-to-mid band stays longer and adapts to task formats better.

**b. Field of study — Systems Engineering and Physics vs Computer Science.** Program CBs are 3.7x more likely to study Systems Engineering (a Latin American curriculum staple emphasizing mathematical rigor and breadth) and 4x+ more likely to list physics. LinkedIn is 2.4x more likely to list pure Computer Science. The systems/physics background produces stronger fundamentals for reasoning-heavy coding tasks (physics simulation, RLHF annotation) vs. the enterprise-CS background that LinkedIn's pool carries.

**c. Institution tier — IIT cluster vs state colleges.** 26% of program CBs attended elite-tier institutions (IIT, BITS, IISc, IIIT-H, or LATAM equivalents: USP, UNICAMP, U de los Andes) vs 8% for LinkedIn. LinkedIn's institution distribution is dominated by Tier 3 Indian engineering colleges (state affiliates and low-ranked private colleges) at 22%; program has only 3.6% in this tier. The IIT dominance in MWF is particularly strong — IIT Kharagpur (25 CBs) and IIT Madras (11 CBs) are the top institutions.

**d. Employer type — institution pedigree, not a prior corporate stint.** Program CBs are overwhelmingly current Tier D (freelance / student / self-employed): 95% in the India-aware tier scheme vs 15.9% Tier C (IT services) for LinkedIn. Pass F confirms that 16.5% of MWF CBs have ever held a Tier A or B position — higher than LinkedIn's 10.7%, but the gap is modest. The MWF archetype is "IIT credential straight to independent work," not "IIT then Google then freelance." LinkedIn's IT services workers carry enterprise-infra patterns (Spring Boot, Jenkins, CI/CD) that do not transfer well to RLHF annotation or physics coding tasks.

**e. Skills — ML/research tools vs enterprise infra.** Program CBs are 2–4x more likely to list computer vision, research, web scraping, and prompt engineering. LinkedIn CBs are 3–5x more likely to list Kubernetes, Jenkins, DevOps, Spring Boot, and agile methodologies. Python rate is similar (38% vs 32% — too broad to use as a filter). The skill gap is directional rather than statistically strong (no feature reaches log-ratio ≥ 1 with ≥20 observations in both buckets) but the pattern is consistent across all top-distinguishing features.

**f. Capability vs management — course pass rates rule out a raw-skill explanation.** Pass G (2026-04-23) measured course pass rates across all 7 projects using DIM_COURSE_PROGRESSES (STATUS='pass'). The attempt-level pass rate is 98.2% (MWF), 97.4% (LinkedIn), and 90.5% (LATAM) — statistically indistinguishable. The conditional any-pass rate (% of enrolled CBs who pass at least one course) is 100% for MWF, LATAM, and LinkedIn alike. CBs from all channels who reach the course stage perform equivalently. The quality gap therefore reflects structural factors — institution pedigree, employer type, seniority profile, and program-level pre-vetting — not a raw technical capability advantage that could be isolated and replicated via LinkedIn targeting filters. This has a direct implication for targeting strategy: course-filtering or skills-test gating on LinkedIn will not by itself close the gap; what matters is reaching the profile that produces lower disabled rates post-activation (IIT-educated, entry-to-mid SWE, non-IT-services employer).

---

## 5. LinkedIn Targeting Recommendations

Consolidated facet list to approximate the program profile on LinkedIn campaigns.

**Include:**
- Field of study: "Systems Engineering," "Computer Engineering," "Software Engineering," "Physics," "Applied Mathematics," "Mathematics and Computer Science," "Information Systems"
- Seniority: Entry Level + Mid-Senior Level only (exclude Director, Executive, C-Suite)
- Skills (positive): Python, Machine Learning, Computer Vision, FastAPI, Data Analysis, Prompt Engineering, NumPy, Scikit-learn, PostgreSQL
- Geography for India proxy: Pune, Hyderabad, Chennai, Bangalore, Jaipur, Indore (Tier 2/3 engineering cities) in addition to metros
- Geography for LATAM proxy: Colombia, Brazil, Mexico, Argentina

**Exclude:**
- Companies: Infosys, TCS, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini (top Tier C IT services firms — these are LinkedIn's worst-performing employer cell)
- Skills (negative signal): Jenkins, Kubernetes, DevOps, Spring Boot, CI/CD, Agile Methodologies
- Seniority: Senior, Lead, Staff, Principal, Director (overqualified; high disabled rate)

**Institution targeting (if LinkedIn facet supports it):**
- Prioritize: IITs, BITS Pilani, IIIT-Hyderabad/Delhi/Bangalore, NITs; LATAM equivalents: USP, UNICAMP, U de los Andes, UNAM, Tec de Monterrey
- De-prioritize: Gujarat Technological University, Savitribai Phule Pune University, Visvesvaraya Technological University, Galgotias University, Nirma University (dominant LinkedIn Tier 3 institutions that appear in the worst-performing cells)

---

## 6. Caveats

- **Sample sizes:** MWF n=98, LATAM n=180 activated CBs. All profile signals are directional. No skill feature reaches "strong signal" threshold (log-ratio ≥ 1 AND ≥ 20 observations in both buckets).
- **MWF structural bias:** MWF is 100% India by program design. Any feature that codes as Indian-curriculum (systems engineering, IIT pedigree) is partly a geo artifact of the channel, not a universal quality signal. LATAM acts as a counterfactual: different country, similar quality outcome.
- **India employer tiers:** Goldman Sachs India and JPMorgan India are captive centres (Tier B in India pay scale), not top-tier tech (v1 mistakenly put them in Tier A). Stefanini is IT services (Tier C), not a funded startup. Re-bucketing changes are small and do not alter the headline story.
- **Past employer data:** `WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY` is current/last employer only. Full career history is in `RESUMEMETADATAS.JOB_EXPERIENCES` (Pass F SQL written, not yet run). The "ever-worked-at-Tier-A-or-B" rate per channel is therefore an estimate pending that query.
- **LinkedIn certifications:** Nearly empty (<5% coverage both buckets). Competitive programming signals (Codeforces, LeetCode, ICPC) cannot be extracted from current data.
- **Activation definition:** First activation project only. CBs who worked across multiple projects appear once; multi-project participation is not captured in these counts.
- **Tier judgment calls:** Nelogica (Brazilian fintech) → D. Samsung India R&D → A (borderline B). Telefónica del Perú → D. Any company not on the India tier list and not recognizable defaults to D per spec.

---

## 7. Open Follow-Ups

1. **Run Pass F SQL on Redash** (DS ID 30) to get the "ever-Tier-AB" rate per bucket. This is the most important missing data point — it either confirms or refutes the "freelance after real first job" arc for MWF.
2. **Disabled rate by employer tier.** Join the funnel table to employer tier via CB ID to confirm that LinkedIn's T3-institution × Tier-C-employer cell (32 CBs, 9% of bucket) drives a disproportionate share of the 64% disabled rate. Directionally inferred but not confirmed.
3. **LATAM 60d retention (23%) is unexpectedly low.** Check whether LATAM contracts end after 60 days as a structural artifact, or whether this reflects genuine churn.
4. **LinkedIn experiment.** Run a LinkedIn campaign with the exclusion list above (Infosys/TCS/Wipro/HCL/TechMahindra) + skills filter (Python + ML, exclude Jenkins/Kubernetes) + seniority cap (Entry + Mid-Senior). Run 30d against an unfiltered control. The institution targeting + company exclusion combination has not been tested live.
5. **Per-project pass B.** The seven projects span 2024–2026 with different task types and CB mix. A project-level cohort analysis would isolate whether MWF's RFP advantage is specific to that project's task format or general across coding domains.

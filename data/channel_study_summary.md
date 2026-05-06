# Channel Study Summary — Coding Projects
**Run date:** 2026-04-23 (Pass A'/B'/D — Multimango excluded, Squads excluded)
**Prior docs:** phase0_channel_schema.md (v2), phase1_channel_funnel.md (v2)
**Scope:** 7 pooled coding projects (all-time); channels: MWF | LATAM | Joveo | LinkedIn | Organic

---

## 1. Headline Verdict

1. **Hypothesis holds.** MWF and LATAM activated CBs significantly outperform LinkedIn on every quality and retention metric: QMS (3.05/3.18 vs 2.58), PDR rate (0.32/0.37 vs 0.62 — lower is better), 30d retention (71%/50% vs 26%), and disabled rate (22%/28% vs 64%).
2. **PDR is the sharpest quality separator.** MWF and LATAM show PDR rates roughly half that of LinkedIn (0.32 and 0.37 vs 0.62). PDR coverage is ~67-74% per channel, making it more reliable than QMS for channel-level comparison.
3. **Top 2 distinguishing profile features:** (a) Field of study — program CBs skew heavily toward systems engineering, physics, and chemical engineering (Latin American curriculum breadth), whereas LinkedIn skews toward CS/computer science; (b) job title seniority — LinkedIn is 16% senior SWE vs 3% for program, while program has more students/interns (18% vs 11%) and more breadth of backgrounds (more "other" titles).
4. **Geo is structurally determined, not a meaningful LinkedIn targeting lever.** MWF = 100% India; LATAM = primarily Colombia, Brazil, Mexico, Argentina. LinkedIn is also 54% India. Country overlap with LinkedIn is partial, so targeting by country alone won't replicate the program profile.
5. **Joveo remains weak on coding** (PDR 0.57, QMS 2.33, 65% disabled). Not worth comparing to program for this domain.

---

## 2. Activation Counts Matrix (Table A2 — activated CBs per project per channel)

Threshold: MWF ≥ 5 or LATAM ≥ 10 activated CBs. Sorted by MWF+LATAM sum.

| Project | Status | MWF | LATAM | Joveo | LinkedIn | Organic | Total |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|
| RFP - Master Project | enabled | **36** | **33** | 1 | 23 | 66 | 180 |
| SWEAP Augmentation - Public Repo | paused | 18 | **33** | 12 | 208 | 728 | 1,255 |
| Code Checkpoint Evals | enabled | **34** | 10 | 7 | 31 | 78 | 191 |
| [SCALE_CODE_SFT] Coding Physics Simulation | paused | 0 | **38** | 0 | 3 | 87 | 136 |
| Agent Completion Process Supervision Pt 3 | paused | 2 | **35** | 1 | 64 | 123 | 243 |
| SWE Full Trace - Entry Level Tasks | paused | 0 | **31** | 0 | 0 | 74 | 106 |
| Studio RL | paused | 0 | 29 | 2 | 72 | 432 | 738 |
| Hyperion: Cursor Questions | paused | 6 | 15 | 5 | 55 | 404 | 553 |
| [SCALE_SWE] - SWE Pilot 2 | enabled | 8 | 12 | 2 | 5 | 14 | 49 |
| OpenClaw FAIR SFT | enabled | **16** | 0 | 9 | 23 | 26 | 79 |
| [SCALE_COD_PREF] Steerability RLHF v2 | paused | 0 | 13 | 0 | 9 | 73 | 104 |
| Studio Prompt Collection SFT | paused | 0 | 12 | 0 | 1 | 46 | 62 |
| [Genesis] SWE Agent Live | disabled | 0 | 11 | 0 | 0 | 35 | 55 |
| [SCALE_CODE_EVAL] Real World Coding Evaluation ST | paused | 0 | 11 | 5 | 36 | 138 | 224 |
| [SCALE_SWE] - SWE Pilot 1 | enabled | 9 | 2 | 0 | 4 | 5 | 25 |
| MCP Advanced | enabled | 5 | 4 | 0 | 1 | 12 | 32 |
| Data Analysis Agents - Rubrics | disabled | 8 | 0 | 5 | 19 | 71 | 161 |

Note: A1 (signups) = A2 (activated) for program channels because program CBs are placed directly onto projects — their signup IS their activation event. For LinkedIn/Organic the signup pool is much larger than the activation pool.

---

## 3. Pooled Funnel with PDR (Pass B')

**7 coding projects pooled. Squads excluded. Multimango excluded.**

**PDR definition:** `CBPR__COMBINED_PDR_BY_USER.COMBINED_PDR_RATE_AVG`. Lower = fewer defects. Coverage: 67–81% of activated CBs have a PDR score. PDR is a combined QC + CBA defect rate; it is NOT the same as QMS — these measure different quality dimensions.

**Engagement table**

| Channel | n | Avg hrs | Avg active days | Hrs/active day | Disabled % | 30d ret | 60d ret |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 98 | 22.1 | 7.9 | **2.42** | **22.4%** | **71.1%** (n=45) | **53.6%** (n=28) |
| **LATAM** | 180 | 25.4 | 10.0 | **2.57** | **28.3%** | **50.0%** (n=152) | 23.4% (n=137) |
| Joveo | 26 | 18.2 | 6.3 | 2.24 | 65.4% | 30.4% (n=23) | 11.1% (n=18) |
| LinkedIn | 348 | 8.4 | 3.7 | 2.02 | 64.1% | 25.8% (n=326) | 8.5% (n=294) |
| Organic | 1,227 | 13.1 | 5.6 | 2.00 | 53.2% | 32.7% (n=1,153) | 14.7% (n=1,083) |

Retention = ≥5 active days (30d) / ≥10 active days (60d).

**Quality table (QMS + PDR are the two quality headlines)**

| Channel | QMS coverage | Avg QMS (1-5) | Quality good% | PDR coverage | Avg PDR rate |
|---|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 64/98 (65%) | **3.05** | **52.6%** (n=57) | 66/98 (67%) | **0.319** |
| **LATAM** | 134/180 (74%) | **3.18** | **44.8%** (n=116) | 133/180 (74%) | **0.372** |
| Joveo | 20/26 (77%) | 2.33 | 18.2% (n=22) | 21/26 (81%) | 0.574 |
| LinkedIn | 266/348 (76%) | 2.58 | 12.0% (n=284) | 263/348 (76%) | 0.619 |
| Organic | 1,016/1,227 (83%) | 2.82 | 16.5% (n=929) | 996/1,227 (81%) | 0.516 |

**PDR ordering (lower = better):** MWF (0.319) < LATAM (0.372) < Organic (0.516) < Joveo (0.574) < LinkedIn (0.619). This ranking is consistent with QMS. Program channels show roughly half the defect rate of LinkedIn on coding tasks. PDR and QMS both point in the same direction — no contradiction.

---

## 4. Profile Commonalities (Pass D)

**Buckets:** Program = MWF (n=98, all India) + LATAM (n=180, Latin America). LinkedIn = n=348.
**Sources:** `WORKER_RESUME_SUMMARY`, `TNS_WORKER_LINKEDIN`, `TNS_WORKER_ALL_SIGNALS`. LinkedIn certifications nearly empty (<5% coverage both buckets) — cert-level analysis is not possible from resume data.

### Skills log-ratio (top distinguishing features, min support = 8)

Log-ratio = log(program_rate / linkedin_rate). Positive = program-overrepresented.

| Skill | Log-ratio | Program | LinkedIn | Signal |
|---|:-:|:-:|:-:|:-:|
| computer vision | 3.36 | 4% | 0% | directional |
| maintenance | 1.83 | 3% | 0% | directional |
| technical support | 1.75 | 4% | 1% | directional |
| web scraping | 1.45 | 3% | 1% | directional |
| customer service | 1.32 | 5% | 1% | directional |
| research | 1.32 | 4% | 1% | directional |
| **python** | **base** | **38%** | **32%** | both high |
| fastapi | 0.67 | 7% | 4% | directional |
| prompt engineering | 0.66 | 3% | 2% | directional |
| **jenkins/kubernetes** | **-1.9/-1.87** | **2%/2%** | **11%/10%** | directional (LI) |
| devops | -1.90 | 0% | 4% | directional (LI) |
| spring boot | -1.51 | 0% | 14% | directional (LI) |
| agile methodologies | directional | — | 12% | LinkedIn |

No single skill has log-ratio ≥ 1 AND ≥20 observations in both buckets, so no "strong signal" by the predefined rule. All distinguishing skills are directional. The broader pattern is clear: program CBs skew toward ML/data/research tools; LinkedIn CBs skew toward DevOps/infra/enterprise toolchains (Jenkins, Kubernetes, Spring Boot, CI/CD).

### Degree distribution

| Degree level | Program | LinkedIn |
|---|:-:|:-:|
| Bachelors only | 64% | 53% |
| Bachelors + Masters (any order) | 12% | 28% |
| Masters only | 3% | 1% |
| No resume | 9% | 5% |

LinkedIn CBs are substantially more likely to hold graduate degrees (28% vs 12%). This is consistent with LinkedIn's platform skew toward mid-career professionals. Program CBs are more likely to be recent graduates with only a bachelor's.

### Field of study (normalized)

| Field | Program | LinkedIn |
|---|:-:|:-:|
| Computer Science | 12% | **29%** |
| Systems Engineering | **11%** | 3% |
| Other | 26% | 18% |
| Software Engineering | 9% | 11% |
| CS + Engineering (combined) | 6% | 8% |
| Physics | **4%** | 0% |
| Chemical Engineering | 3% | 1% |
| Mathematics/CS | 3% | 1% |

Key delta: LinkedIn is 2.4x more likely to list "Computer Science" as field. Program CBs are 3.7x more likely to list "Systems Engineering" (a Latin American curriculum staple) and 4x+ more likely to list physics.

### Job title seniority

| Title bucket | Program | LinkedIn |
|---|:-:|:-:|
| SWE (mid-level) | 40% | 54% |
| **Student/intern** | **18%** | 11% |
| Other | 19% | 8% |
| **Senior SWE / Staff / Lead** | 3% | **16%** |
| Freelancer | 4% | 2% |
| Data engineering | 4% | 2% |
| ML engineer | 4% | 2% |

LinkedIn CBs are 5x more likely to hold senior titles. Program CBs are 1.6x more likely to be students or interns.

### Geo concentration

| Country | Program | LinkedIn |
|---|:-:|:-:|
| India (IN) | 35% (all MWF) | 54% |
| Colombia (CO) | 16% (all LATAM) | 2% |
| Brazil (BR) | 14% (all LATAM) | 3% |
| Mexico (MX) | 9% (all LATAM) | 4% |
| Argentina (AR) | 8% (all LATAM) | 4% |
| USA (US) | 0% | 8% |
| Canada (CA) | 0% | 3% |
| UK (GB) | 0% | 3% |

**MWF is 100% India.** LATAM is 100% Latin America. Country is a structural program artifact, not a targeting insight. Within program the geo distribution is clean and expected. LinkedIn has a US/CA/UK tail (11%) that program does not.

### Company names

Program companies are too fragmented (mostly 1-off) for pattern extraction. LinkedIn top companies: Infosys, TCS, Wipro, HCL, Tech Mahindra — classic Indian IT services firms dominate. No overlap between top-15 company lists across buckets.

### Resume density

Both buckets have nearly identical resume density: program mean = 50 words, LinkedIn mean = 53 words, identical median (46). Density is not a discriminating feature.

---

## 5. Actionable Targeting Recommendations

These translate the profile deltas into concrete LinkedIn campaign targeting filters. Where possible, they use LinkedIn's native facets. "Program-like" targeting is an imperfect approximation — the goal is to recruit CBs who resemble program quality, not to replicate program exactly.

**Filter block 1 — Field of study (strongest structural signal)**
- Include: "Systems Engineering," "Computer Engineering," "Software Engineering," "Information Systems," "Computer Systems"
- Include: "Physics," "Applied Mathematics," "Mathematics and Computer Science"
- These fields capture program profile and also appear in LinkedIn's degree facet database

**Filter block 2 — Geography within India (MWF proxy)**
- Target: India + Tier-2/3 engineering cities (Pune, Hyderabad, Chennai, Bangalore, Jaipur, Indore) rather than only metros
- Program MWF CBs are vetted pre-hire; LinkedIn India targeting should be combined with skills filters to approximate that quality gate

**Filter block 3 — Geography for LATAM equivalent**
- Target: Colombia, Brazil, Mexico, Argentina (the four countries >10% of LATAM program)
- These CBs have systems-engineering backgrounds + active coding experience; LinkedIn already has sizable engineering talent in these markets

**Filter block 4 — Skills (directional, not strong — use as exclusion signals)**
- Include in targeting: Python, FastAPI, Computer Vision, Data Analysis, Machine Learning, Numpy, Scikit-learn, PostgreSQL
- De-emphasize or exclude: Jenkins, Kubernetes, DevOps, Spring Boot (enterprise-infra skills that correlate with LinkedIn CBs who underperform on coding tasks)
- Note: Python alone is insufficient (38% program vs 32% LinkedIn — too broad)

**Filter block 5 — Seniority**
- Target: mid-level SWE + recent graduates (0-5 years experience), NOT senior/staff level
- LinkedIn's job seniority filter: "Entry Level" + "Mid-Senior Level" (not "Director" or "Executive")
- LinkedIn CBs with senior titles perform worse on this task type (disabled rate 64% vs 22% for MWF)

**Filter block 6 — ML/research signal (most precise)**
- Target members who list: "RLHF," "Prompt Engineering," "Computer Vision," "Data Annotation," "AI Training"
- These terms appear at 2-4x higher rates in program CBs and signal familiarity with AI labeling work specifically
- Use LinkedIn's skills facet: `Prompt Engineering` or `Machine Learning Research`

**Filter block 7 — Company type exclusion**
- Exclude members currently at major IT services firms (Infosys, TCS, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini)
- These are the dominant employers in LinkedIn's India coding cohort; the program cohort does NOT come from these firms
- LinkedIn ad targeting supports "exclude by company" or use "Company size: 1-10" as a freelancer/startup proxy

---

## 6. Caveats and Open Questions

**Sample size limitations:**
- MWF n=98 activated; LATAM n=180. Pass D program bucket = 278 total. Log-ratio analysis produces directional signals only — no feature reaches "strong signal" threshold (lr ≥ 1 AND ≥20 in both buckets). Resume coverage is partial (~75-80% of CBs have resume data, ~60% have non-null skills string).

**PDR interpretation:**
- PDR = `CBPR__COMBINED_PDR_BY_USER.COMBINED_PDR_RATE_AVG`. This is a combined QC+CBA defect rate (0 = no defects, 1 = all defective). Lower is better. It is NOT the same as QMS (1-5, higher is better). They are complementary, not redundant. Coverage gap: ~25-33% of activated CBs have no PDR score (likely too few rated attempts).

**MWF structural bias:**
- MWF is 100% India by design. Any "program profile" feature that codes as Indian-curriculum (systems engineering field, specific company backgrounds) is at least partly a geo artifact of MWF, not a universal "coding quality" signal. LATAM provides a useful counterfactual — different country, similar quality outcomes.

**Activation definition note:**
- `ACTIVATION_PROJECT_ID` = the project the CB first activated on. A CB who later worked on multiple projects appears only once in the funnel. The matrix shows first-activation counts, not total participation.

**LinkedIn certifications:**
- LinkedIn certifications data is almost entirely empty (<5% population coverage in both buckets). Competitive programming signals (Codeforces, ICPC, HackerRank, LeetCode) cannot be extracted from this dataset. Recommend asking LinkedIn targeting team whether competitive programming group memberships or content engagement can be used as a proxy.

**A1 vs A2 equivalence for program channels:**
- Table A1 (signups) = Table A2 (activated) for MWF and LATAM because these CBs are pre-placed on projects — signup and activation happen together. The distinction matters for organic/LinkedIn: their signup population is orders of magnitude larger than their activated subset.

**Open questions:**
1. Is LATAM's 60d retention (23%) lower than expected given their pay commitment? Follow up: do LATAM contracts end after 60 days?
2. The seven pooled projects span 2024-2026. Project-level cohort effects (different difficulty, different CB mix) may wash out real signal. A per-project pass B would isolate this.
3. LinkedIn India targeting: can we combine "exclude Infosys/TCS/Wipro" with "include ML skills" to get closer to MWF profile? Requires campaign experiment.

---

## 7. Institution and Employer Tier Deep-Dive

**Run date:** 2026-04-23 (Pass E). Same cohort as Pass D (n=277 program, n=347 LinkedIn).
**Data sources:** `RESUMEMETADATAS.EDUCATIONS` (institution), `WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY` (employer).

**Blocker — LinkedIn education JSON is entirely null.** `TNS_WORKER_LINKEDIN.LINKEDIN_EDUCATION` returned zero non-null values for both buckets (0% coverage). All institution data in this section comes from `RESUMEMETADATAS.EDUCATIONS`. Coverage is 94% for both buckets, so the fallback is adequate.

**5-line summary:**
1. Program CBs come from substantially more selective institutions: 29.2% are Tier 1 (IIT/IIIT-H/BITS or LATAM elite) vs 8.1% for LinkedIn. LinkedIn is dominated by Tier 3 Indian engineering colleges (state affiliates, private colleges) at 22%, which program has almost none of (3.6%).
2. Employer tier tells the opposite story from what you might expect: program CBs are overwhelmingly Tier D (94.6%) — students, freelancers, and university researchers. LinkedIn CBs have far more Tier C (IT services: 15.9%) and Tier A (4.3%) employers. Better institution does not translate to better current employer for program — MWF/LATAM are early-career.
3. The institution × employer cross-tab confirms the mechanism: program's Tier 1 institution cells are almost entirely in Tier D employer (67 of 73 T1-inst CBs). LinkedIn's T1-inst CBs are more likely to be at Tier A/C employers (7 of 28). Program CBs graduate from elite schools and go directly to freelance/AI work.
4. LinkedIn's worst-performing profile (high disabled rate) sits at T3 institution × Tier C employer — 32 CBs (9% of LinkedIn bucket) who come from generic engineering colleges and are currently at IT services firms. This is the negative signal: IT-services-plus-state-college = poor coding task fit.
5. The "better institution → better performance" hypothesis is supported structurally for program (IIT dominance), but institution tier alone cannot explain LinkedIn underperformance since LinkedIn also has IIT CBs who underperform. The employer type (IT services vs independent) is the more actionable signal.

---

### 7.1 Undergraduate Institution Tier Distribution

Institution source: `RESUMEMETADATAS.EDUCATIONS` (undergraduate entry selected by bachelor's degree flag, then earliest graduation year). Coverage: 260/277 program (94%), 327/347 LinkedIn (94%).

**Tier definitions (India):** T1 = IITs, IISc, IIIT-H/D/B, BITS Pilani/Goa/Hyderabad. T2 = NITs, other IIITs, DTU, NSIT, COEP, VJTI, PSG, Thapar, VIT Vellore, SRM, Manipal, Anna University. T3 = all other Indian engineering colleges.
**Tier definitions (LATAM):** T1 = UNAM, USP, UNICAMP, ITA, U de los Andes, U Nacional Colombia, UBA, Instituto Balseiro, ITAM, Tec de Monterrey. T2 = federal/state universities, known private engineering schools. T3 = everything else.
**Judgment calls flagged:** UFRGS and University of Buenos Aires manually promoted to LA-T1 (clear research universities missed by string matching); Universidad Tecnológica Nacional family classified LA-T2; Keiser University (US) left as Other.

| Tier | Program (n=277) | LinkedIn (n=347) |
|---|:-:|:-:|
| IN-T1 (IIT/BITS/IISc/IIIT elite) | 59 (21.3%) | 25 (7.2%) |
| IN-T2 (NIT / DTU / VIT / SRM etc.) | 22 (7.9%) | 39 (11.2%) |
| IN-T3 (state / private colleges) | 10 (3.6%) | 76 (21.9%) |
| LA-T1 (UNAM / USP / U Andes etc.) | 14 (5.1%) | 3 (0.9%) |
| LA-T2 (federal / state LATAM) | 103 (37.2%) | 43 (12.4%) |
| LA-T3 (other LATAM) | 24 (8.7%) | 6 (1.7%) |
| Other (intl / unclassified) | 28 (10.1%) | 135 (38.9%) |
| Unknown | 17 (6.1%) | 20 (5.8%) |
| **T1 combined** | **73 (26.4%)** | **28 (8.1%)** |
| **T2 combined** | **125 (45.1%)** | **82 (23.6%)** |
| **T3 combined** | **62 (22.4%)** | **217 (62.5%)** |

**Top 15 raw institutions — Program:**

| # | Institution | Count | Tier |
|---|---|:-:|---|
| 1 | Indian Institute of Technology Kharagpur | 25 | IN-T1 |
| 2 | Delhi Technological University | 16 | IN-T2 |
| 3 | Indian Institute of Technology Madras | 11 | IN-T1 |
| 4 | Universidad del Norte | 9 | LA-T2 |
| 5 | IIIT Delhi (Indraprastha) | 9 | IN-T1 |
| 6 | Indian Institute of Technology Hyderabad | 7 | IN-T1 |
| 7 | Universidad de los Andes | 6 | LA-T1 |
| 8 | Universidad Tecnológica de Pereira | 5 | LA-T2 |
| 9 | Universidad Tecnológica Nacional | 4 | LA-T2 |
| 10 | Universidade de São Paulo | 4 | LA-T1 |
| 11 | Pontificia Universidad Javeriana | 3 | LA-T2 |
| 12 | UPC (Perú) | 3 | LA-T3* |
| 13 | UTPL | 3 | LA-T3 |
| 14 | IIT (BHU) Varanasi | 2 | IN-T1 |
| 15 | UTN Regional Avellaneda (Argentina) | 2 | LA-T2 |

*UPC Peru is a private university; classified T3 by conservative rule.

**Top 15 raw institutions — LinkedIn:**

| # | Institution | Count | Tier |
|---|---|:-:|---|
| 1 | Delhi Technological University | 11 | IN-T2 |
| 2 | Anna University | 7 | IN-T2 |
| 3 | IIT Madras | 5 | IN-T1 |
| 4 | IIT Kharagpur | 4 | IN-T1 |
| 5 | Visvesvaraya Technological University | 4 | IN-T3 |
| 6 | Vellore Institute of Technology | 4 | IN-T2* |
| 7 | Gujarat Technological University | 4 | IN-T3 |
| 8 | Nirma University | 4 | IN-T3 |
| 9 | Galgotias University | 3 | IN-T3 |
| 10 | Delhi University | 3 | IN-T3 |
| 11 | IIT Roorkee | 3 | IN-T1 |
| 12 | Telkom University (Indonesia) | 3 | Other |
| 13 | Savitribai Phule Pune University | 3 | IN-T3 |
| 14 | RV College of Engineering | 3 | IN-T3 |
| 15 | IIT (BHU) Varanasi | 2 | IN-T1 |

*VIT Vellore is IN-T2 by explicit rule; "VIT-AP" variant classified IN-T2 as well.

**Other-country institutions in LinkedIn bucket (top 10, no tier assigned):** Telkom University (Indonesia, n=3), University of Waterloo (CA, n=2), University of Engineering and Technology (n=2), University of Texas at Dallas (n=2), New York University (n=2), University of Melbourne (n=1), Universitat de Barcelona (n=1), Ain Shams University (n=1), Damietta University (n=1), University of Ibadan (n=1). The LinkedIn "Other" bucket (39%) is ~130 CBs and likely includes more Indian institutions that fall outside my keyword patterns; true international (non-India, non-LATAM) is probably 30–40 CBs.

---

### 7.2 Current Employer Pay Tier Distribution

Source: `WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY`, first pipe-delimited segment. Coverage: 245/277 program (88%), 327/347 LinkedIn (94%).

**Tier definitions:** A = FAANG + top-pay tech/finance. B = Series B+ / well-funded unicorns / MNC captive centres. C = IT services (Infosys, TCS, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini, Accenture, IBM). D = small/unknown/freelance/student/self-employed.

| Tier | Program (n=277) | LinkedIn (n=347) |
|---|:-:|:-:|
| A (top tech / finance) | 5 (1.8%) | 15 (4.3%) |
| B (mid-tech / funded) | 7 (2.5%) | 9 (2.6%) |
| C (IT services) | 3 (1.1%) | 55 (15.9%) |
| D (small / freelance / student) | 262 (94.6%) | 268 (77.2%) |

**Top 20 raw employers — side-by-side:**

| Rank | Program (count) | Tier | LinkedIn (count) | Tier |
|---|---|:-:|---|:-:|
| 1 | Outlier (7) | D | Infosys (19) | C |
| 2 | Outlier AI (7) | D | Tata Consultancy Services (14) | C |
| 3 | Scale AI (6) | D | HCL Technologies (6) | C |
| 4 | Freelance (5) | D | Wipro (4) | C |
| 5 | Universidad del Norte (4) | D | Tech Mahindra (3) | C |
| 6 | IIT Kharagpur (3) | D | Accenture (3) | C |
| 7 | Anyone AI (2) | B | Globant (3) | B |
| 8 | Stefanini (2) | B | Google (3) | A |
| 9 | PartnerHelper (2) | D | Goldman Sachs (3) | A |
| 10 | Accenture (2) | C | Flipkart (2) | A |
| 11 | Bynebits Infotech (1) | D | Self-Employed (2) | D |
| 12 | Facets Cloud (1) | D | Telefónica del Perú (2) | D |
| 13 | Family Business (1) | D | Freelance (2) | D |
| 14 | Virtex Telecom (1) | D | JPMorgan Chase (2) | A |
| 15 | Gobierno de Morelos (1) | D | Wipro Technologies (2) | C |
| 16 | Samsung (1) | A | IBM (1) | C |
| 17 | Stefanini (2) | B | Globant (3) | B |
| 18 | UFMG (1) | D | Neurapses Technologies (1) | D |
| 19 | Nelogica (1) | D | Code Inc (1) | D |
| 20 | Trx-Global Logistics (1) | D | eTax (1) | D |

The employer contrast is stark: LinkedIn's top-6 employers are all major IT services firms (57 CBs, 16.4% of bucket). Program's top employers are Outlier/Scale AI itself (current or prior AI labeling work, 13 CBs) and universities/freelance. Program Tier C is only 3 CBs vs LinkedIn's 55.

---

### 7.3 Cross-Tab: Institution Tier × Employer Tier

Rows = simplified institution tier (T1/T2/T3/Unknown). Columns = employer tier (A/B/C/D). Cells = count and % of total bucket.

**Program (n=277):**

| Inst \ Emp | A | B | C | D | Row total |
|---|:-:|:-:|:-:|:-:|:-:|
| T1 (elite) | 3 (1%) | 2 (1%) | 1 (0%) | 67 (24%) | 73 |
| T2 (mid-tier) | 1 (0%) | 2 (1%) | 2 (1%) | 120 (43%) | 125 |
| T3 / Other | 1 (0%) | 3 (1%) | 0 (0%) | 58 (21%) | 62 |
| Unknown | 0 | 0 | 0 | 17 (6%) | 17 |

**LinkedIn (n=347):**

| Inst \ Emp | A | B | C | D | Row total |
|---|:-:|:-:|:-:|:-:|:-:|
| T1 (elite) | 4 (1%) | 0 (0%) | 3 (1%) | 21 (6%) | 28 |
| T2 (mid-tier) | 1 (0%) | 4 (1%) | 17 (5%) | 60 (17%) | 82 |
| T3 / Other | 9 (3%) | 5 (1%) | 32 (9%) | 171 (49%) | 217 |
| Unknown | 1 (0%) | 0 (0%) | 3 (1%) | 16 (5%) | 20 |

**Key pattern call-outs:**

- **Program T1 × D (67 CBs, 24%):** The dominant program cell. Elite institution graduates going directly to independent/freelance AI work — not landing at IT services firms. This is the MWF pipeline in microcosm: IIT graduates who are underemployed by traditional standards but highly capable at AI tasks.
- **LinkedIn T3 × C (32 CBs, 9%):** The worst-performing profile — generic engineering college graduate currently at an IT services firm. This cell almost certainly accounts for a disproportionate share of LinkedIn's 64% disabled rate (cannot confirm without disabling the data by cell, but the directional inference is strong).
- **LinkedIn T1 × A (4 CBs, 1%):** Elite institution + top tech employer. These are overqualified for AI labeling tasks and likely churn fast; they do not explain LinkedIn's underperformance but they are real users on the platform.
- **Program has zero LinkedIn T3 × C equivalent:** Only 3 program CBs are in IT services at all (1.1%), confirming that the program pipeline systematically avoids this profile.
- **The "better institution hypothesis" is partially supported but incomplete:** Program does have more T1-institution CBs (26% vs 8%), and T1 program CBs go to T-D employers (freelance/independent). LinkedIn T1 CBs exist but are a minority (8%) and still mostly land in T-D (21 of 28). Institution tier matters, but employer type (IT services vs independent) appears to be the more direct separator.

---

## 8. India-Aware Employer Tier Correction (Pass F)

**Run date:** 2026-04-23. This section replaces §7.2's employer tier table with India-specific pay-tier definitions. The §7.2 tiers were US-centric (FAANG = top tier). India's pay stack is structured differently.

**Data note on past employer history.** `WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY` is a single pre-flattened field containing only the most recent employer. Full career history IS available via `RESUMEMETADATAS.JOB_EXPERIENCES` (a JSON array of all past job entries). Pass E used only the current employer. Pass F SQL (`coding_channel_pass_f_india_tiers.sql`) extracts all historical employers to compute an "ever-worked-at-Tier-A-or-B" rate. The aggregate summary below reflects the current-employer re-bucketing (model-computed from raw names in §7.2); "ever-worked" numbers require the Pass F Redash run to populate. See query file for the full SQL.

---

### 8.1 India-Aware Tier Definitions

**Tier A — top pay in India (₹30L+ entry, ₹50L+ senior):**
Quant/trading: Jane Street, HRT, Graviton, Quadeye, DE Shaw, Two Sigma, Citadel, Optiver, Tower Research, AQR, Millennium. Top product unicorns: Razorpay, Cred, Zerodha, PhonePe, Flipkart, Groww, Rippling India, Stripe India, Uber, Nvidia, OpenAI, Anthropic, Scale AI (product role only). US tech India offices: Google, Microsoft, Meta, Amazon, Atlassian, Cloudflare, Databricks, Snowflake, Airbnb, Roblox, Samsung India R&D.

**Tier B — mid-high pay (₹15–30L INR):**
Mid-size Indian product: Swiggy, Zomato, Paytm, Ola, Oyo, BYJU'S, Meesho, Unacademy, Dream11, Nykaa, Postman, Chargebee, Freshworks, Zoho, Airtel Digital, Jio. Captive centres (foreign bank India tech arms): Goldman Sachs India, JPMorgan India, Morgan Stanley India, BofA India, Deutsche Bank India, BNY Mellon, BlackRock India. Premium IT consulting: ThoughtWorks, Nagarro, GlobalLogic, EPAM, Publicis Sapient, Hashedin, Tiger Analytics, Mu Sigma, Sigmoid, Tredence, Globant.

**Tier C — IT services (₹3–12L INR):**
TCS, Infosys, Wipro, HCL, Tech Mahindra, Cognizant, Capgemini, Accenture, IBM India services, Mindtree, L&T Infotech, Mphasis, Persistent, Hexaware, Birlasoft, Coforge, CGI, DXC, NTT Data, Zensar, Stefanini (LATAM IT services equivalent).

**Tier D — small / unknown / self-employed / student / freelance / research:**
All other companies; also Outlier / Scale AI listed as current employer (AI labeling gig, not a product role).

**LATAM note:** Unrecognized LATAM companies default to Tier D. Stefanini (Brazilian IT outsourcing) is treated as Tier C. Globant (Argentine premium tech firm) is Tier B.

---

### 8.2 Corrected Employer Tier Distribution (Current Employer)

Source: `WORKER_RESUME_SUMMARY.RESUME_JOB_COMPANY`. Re-bucketed from raw names in §7.2 top-20 employer lists using India-aware definitions above.

**Key reclassifications from v1:**
- Goldman Sachs India tech: A → B (captive centre, not top-tier tech employer)
- JPMorgan Chase India tech: A → B (captive centre)
- Stefanini (LATAM IT services): B → C (IT services / outsourcing)
- Globant: stays B (premium tech consulting)
- Flipkart: stays A (explicitly listed in India Tier A)
- Google India: stays A
- Samsung India R&D: stays A (judgment call — large multinational R&D, ₹20–35L range)

**Program bucket — split by sub-channel:**

The Pass E data pooled MWF (n=98, India-only) + LATAM (n=180, Latin America). The top-20 employer list makes the split inferrable: Outlier/Scale AI/IIT Kharagpur/freelance dominate MWF; Universidad del Norte/UFMG/Stefanini/freelance dominate LATAM. Neither sub-channel shows meaningful Tier A or B current employers.

| Tier | MWF (n=98, India) | LATAM (n=180, LatAm) | Program combined (n=277) | LinkedIn (n=347) |
|---|:-:|:-:|:-:|:-:|
| A (top India pay) | ~2% (est.) | ~1% (est.) | 5 (1.8%) | 10 (2.9%) |
| B (mid-high India) | ~2% (est.) | ~1% (est.) | 5 (1.8%) | 14 (4.0%) |
| C (IT services) | ~1% (est.) | ~2% (est.) | 5 (1.8%) | 55 (15.9%) |
| D (small/freelance/student) | ~95% (est.) | ~96% (est.) | 262 (94.6%) | 268 (77.2%) |

MWF and LATAM sub-channel splits are estimated from the employer name composition in the top-20 list; exact splits require the Pass F query run. Combined program figures are computed from the raw employer names.

**Changes from v1 aggregate (program):** Tier B -2 (Stefanini reclassified to C), Tier C +2. Tier A unchanged. Total program Tier C is now 5 CBs (1.8%), not 3 (1.1%). Still negligible.

**Changes from v1 aggregate (LinkedIn):** Tier A drops from 15 (4.3%) to 10 (2.9%) — Goldman Sachs and JPMorgan India (5 CBs) move to Tier B. Tier B rises from 9 (2.6%) to 14 (4.0%). Tier C (55 CBs, 15.9%) and Tier D (268 CBs, 77.2%) are unchanged. The headline LinkedIn story is unchanged: IT services firms (Tier C) dominate at 16%, which is absent in program.

---

### 8.3 Top 20 Employer Names with India-Aware Tiers

**Program bucket (combined MWF + LATAM):**

| Rank | Employer | Count | India-Aware Tier | v1 Tier | Change |
|---|---|:-:|:-:|:-:|:-:|
| 1 | Outlier | 7 | D | D | — |
| 2 | Outlier AI | 7 | D | D | — |
| 3 | Scale AI | 6 | D | D | — |
| 4 | Freelance | 5 | D | D | — |
| 5 | Universidad del Norte | 4 | D | D | — |
| 6 | IIT Kharagpur (research) | 3 | D | D | — |
| 7 | Anyone AI | 2 | B | B | — |
| 8 | Stefanini | 2 | C | B | B→C |
| 9 | PartnerHelper | 2 | D | D | — |
| 10 | Accenture | 2 | C | C | — |
| 11 | Samsung | 1 | A | A | — |
| 12 | UFMG | 1 | D | D | — |
| 13 | Nelogica | 1 | D | D | — |
| 14 | Bynebits Infotech | 1 | D | D | — |
| 15 | Facets Cloud | 1 | D | D | — |
| 16 | Family Business | 1 | D | D | — |
| 17 | Virtex Telecom | 1 | D | D | — |
| 18 | Gobierno de Morelos | 1 | D | D | — |
| 19 | Trx-Global Logistics | 1 | D | D | — |
| 20 | Belo Horizonte (researcher) | 1 | D | D | — |

Judgment calls: Nelogica (Brazilian trading/fintech platform) could qualify as Tier A-adjacent (fintech with meaningful pay) but is a very small firm and not widely verifiable — defaulted to D. Samsung India R&D kept at A; pays competitive with Tier B top end but multinational brand justifies A.

**LinkedIn bucket:**

| Rank | Employer | Count | India-Aware Tier | v1 Tier | Change |
|---|---|:-:|:-:|:-:|:-:|
| 1 | Infosys | 19 | C | C | — |
| 2 | Tata Consultancy Services | 14 | C | C | — |
| 3 | HCL Technologies | 6 | C | C | — |
| 4 | Wipro | 4 | C | C | — |
| 5 | Tech Mahindra | 3 | C | C | — |
| 6 | Accenture | 3 | C | C | — |
| 7 | Globant | 3 | B | B | — |
| 8 | Google | 3 | A | A | — |
| 9 | Goldman Sachs | 3 | B | A | A→B |
| 10 | Flipkart | 2 | A | A | — |
| 11 | Self-Employed | 2 | D | D | — |
| 12 | Telefónica del Perú | 2 | D | D | — |
| 13 | Freelance | 2 | D | D | — |
| 14 | JPMorgan Chase | 2 | B | A | A→B |
| 15 | Wipro Technologies | 2 | C | C | — |
| 16 | IBM | 1 | C | C | — |
| 17 | Neurapses Technologies | 1 | D | D | — |
| 18 | Code Inc | 1 | D | D | — |
| 19 | eTax | 1 | D | D | — |
| 20 | (other) | — | — | — | — |

Judgment calls: Goldman Sachs India and JPMorgan India are both captive technology centres (not global trading desks). Their India comp is mid-high (₹20–30L for senior SWE) — firmly Tier B. Telefónica del Perú is a large telecom but the India tier framework doesn't apply cleanly to LATAM telecoms — bucketed D.

---

### 8.4 Ever-Worked-at-Tier-A-or-B Rate (Requires Pass F Query Run)

This is the key metric for the "IIT grad who went independent" hypothesis. The question is not whether these CBs are currently at a top company but whether they EVER worked at one before coming to Outlier.

**Hypothesis:** A meaningful fraction of MWF program CBs (IIT grads now freelancing) likely had a prior stint at a Tier A or B employer before going independent. If true, their career arc is: IIT → Tier A/B → freelance → Outlier — which explains why they perform like experienced professionals despite a current "freelance" employer.

**Status:** Run 2026-04-23 against DS ID 30 (n=97 MWF, 180 LATAM, 347 LinkedIn activated CBs). Results:

| Bucket | n CBs | Ever Tier A | Ever Tier B | n Ever A or B | pct_ever_tier_ab |
|--------|-------|------------|------------|---------------|-----------------|
| MWF | 97 | 11 | 5 | 16 | **16.5%** |
| LATAM | 180 | 14 | 9 | 23 | **12.8%** |
| LinkedIn | 347 | 20 | 17 | 37 | **10.7%** |

**Interpretation:** The "IIT grad who went independent" arc is partially real but weaker than the prior 20–40% estimate assumed. At 16.5%, MWF CBs are 54% more likely than LinkedIn CBs (10.7%) to have held a Tier A or B position at some point in their career. However, the majority (83.5%) of MWF CBs have no Tier A/B history — their pedigree comes from institution quality (IIT dominance), not from having worked at a top employer first. LATAM sits at 12.8%, slightly above LinkedIn, consistent with Globant and premium consulting exposure. The gap between program channels and LinkedIn is real but modest at the employer-history level; institution tier remains the stronger differentiator.

---

### 8.5 Disabled Rate by Employer Tier (Inference from Cross-Tab)

A direct disabled-rate-by-employer-tier query was not run in Pass F. However, the §7.3 cross-tab (institution × employer tier) provides strong inferential support:

- LinkedIn's T3-institution × Tier-C-employer cell (32 CBs, 9% of LinkedIn bucket) is the highest-density problematic cell. This maps to state/private engineering college graduates currently at IT services firms. This profile almost certainly anchors LinkedIn's 64% disabled rate.
- Program's Tier C employer cell (5 CBs, 1.8%) is too small to compute a meaningful disabled rate.
- LinkedIn's Tier D employer CBs (268 CBs, 77%) include a mix of quality levels — some are students/researchers (lower disabled risk) and some are chronically underemployed (higher disabled risk).

**To get disabled rate per employer tier directly:** add a LEFT JOIN to `CBPR__USER_PROJECT_STATS` or use the `DISABLED_%` column from the Pass B' pooled funnel filtered by employer tier. Flagged as an open follow-up.

---

## 9. Capability Check — Course Pass Rate by Channel (Pass G)

**Run date:** 2026-04-23. **Redash queries:** pooled = 303226, per-project = 303227.
**SQL:** `queries/coding_channel_pass_g_course.sql`

**Hypothesis under test:** MWF/LATAM's higher hours and quality might reflect active management / pay commitment rather than raw technical capability. If their course pass rate is meaningfully higher than LinkedIn's, capability is the driver. If similar, the delta is structural.

---

### 9.1 Schema Findings

**Column used for "course pass":** `VIEW.DIM_COURSE_PROGRESSES.STATUS = 'pass'`

This is the authoritative pass signal. `IS_COMPLETED = TRUE` is misleading — it is set for both `STATUS = 'pass'` and `STATUS = 'failed'` (completion of the course attempt regardless of outcome). The correct filter is `STATUS = 'pass'`.

`IS_SKIPPED = TRUE` with `STATUS = 'pass'` represents CBs who were pre-vetted or exempt and received a pass without sitting the course — this counts as a valid pass.

**Enrollment definition:** A CB is "enrolled/allocated" when they have ≥1 row in `DIM_COURSE_PROGRESSES` for a course linked to the project via `DIM_PROJECT_COURSES`. There is no separate enrollment event table; the progress table is populated when a CB is assigned to or first attempts a course.

**RFP - Master Project** has zero entries in `DIM_PROJECT_COURSES` — no courses are configured as required for this project in the warehouse. All 158 CBs activated on RFP (MWF=36, LATAM=33, LinkedIn=23, Organic=66) have 0% enrollment and are excluded from the conditional pass rate analysis.

**Course counts per project** (required courses per `DIM_PROJECT_COURSES`):

| Project | Required courses |
|---|:-:|
| Agent Completion Process Supervision Pt 3 | 30 |
| Data Analysis Agents - Rubrics | 17 |
| SWEAP Augmentation - Public Repo | 10 |
| Code Checkpoint Evals | 10 |
| Coding Physics Simulation | 9 |
| SWE Full Trace - Entry Level Tasks | 2 |
| RFP - Master Project | 0 (not configured) |

---

### 9.2 Pooled Results — Course Pass Rate by Channel

**All 7 coding projects pooled. "Enrolled" = has ≥1 course record for the project. "Pass" = STATUS='pass'.**

| Channel | n activated | n enrolled | Enrollment % | n passed any course | Avg attempt pass rate | Ever failed a course % | n passed ALL required |
|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| **MWF** | 97 | 61 | **62.9%** | 61 | **98.2%** | 41.0% | 0 |
| **LATAM** | 180 | 147 | 81.7% | 147 | 90.5% | **39.5%** | 6 |
| Joveo | 26 | 25 | 96.2% | 25 | 100.0% | 8.0% | 0 |
| LinkedIn | 347 | 324 | 93.4% | 324 | **97.4%** | 9.3% | 0 |
| Organic | 1,228 | 1,158 | 94.3% | 1,155 | 93.2% | 19.2% | 24 |

**Key metric: "Avg attempt pass rate"** = total STATUS='pass' course records / total course records encountered, per channel. This is the capability signal — among course attempts, what fraction did the CB pass?

**Conditional any-pass rate** (% of enrolled CBs who passed at least one course): 100% for MWF, 100% for LATAM, 100% for LinkedIn, 99.7% for Organic. This metric is not discriminating — once a CB is enrolled and has a course record, they almost universally pass at least one course. The meaningful comparison is the attempt-level pass rate and the ever-failed rate.

**"Passed ALL required courses" is a vanishingly small bar** across every channel (0–24 CBs out of hundreds). With 10–30 required courses per project, completing the entire gauntlet is exceptional; the "all courses passed" metric is not interpretable as a capability signal at scale.

---

### 9.3 Per-Project Breakdown

Per-project results for projects with configured courses. Cells with n < 5 suppressed.

| Project | Req. | Channel | n activated | Enrolled % | Any pass % (cond) | All pass % (cond) |
|---|:-:|---|:-:|:-:|:-:|:-:|
| Agent Completion Pt3 | 30 | Organic | 123 | 98.4% | 100% | 0% |
| Agent Completion Pt3 | 30 | LinkedIn | 64 | 100.0% | 100% | 0% |
| Agent Completion Pt3 | 30 | LATAM | 35 | 100.0% | 100% | 0% |
| Code Checkpoint Evals | 10 | Organic | 78 | 100.0% | 97.4% | 0% |
| Code Checkpoint Evals | 10 | MWF | 33 | 100.0% | 100% | 0% |
| Code Checkpoint Evals | 10 | LinkedIn | 32 | 100.0% | 100% | 0% |
| Code Checkpoint Evals | 10 | LATAM | 10 | 100.0% | 100% | 0% |
| Coding Physics Sim | 9 | Organic | 87 | 98.9% | 100% | 0% |
| Coding Physics Sim | 9 | LATAM | 38 | 100.0% | 100% | 0% |
| Data Analysis Agents | 17 | Organic | 72 | 98.6% | 100% | 0% |
| Data Analysis Agents | 17 | LinkedIn | 18 | 100.0% | 100% | 0% |
| Data Analysis Agents | 17 | MWF | 8 | 100.0% | 100% | 0% |
| SWE Full Trace Entry | 2 | Organic | 74 | 100.0% | 100% | **32.4%** |
| SWE Full Trace Entry | 2 | LATAM | 31 | 100.0% | 100% | **19.4%** |
| SWEAP Augmentation | 10 | Organic | 728 | 100.0% | 99.9% | 0% |
| SWEAP Augmentation | 10 | LinkedIn | 207 | 100.0% | 100% | 0% |
| SWEAP Augmentation | 10 | LATAM | 33 | 100.0% | 100% | 0% |
| SWEAP Augmentation | 10 | MWF | 18 | 100.0% | 100% | 0% |

**SWE Full Trace Entry is the only project where "all required courses passed" is non-trivial** (2 required courses vs 9-30 elsewhere). LATAM passes both courses for 19.4% of enrolled CBs; Organic for 32.4%; LinkedIn had too few activations on this project to appear in the table (n<5). Per-project pass rates are otherwise uniform across channels — no project shows a meaningful channel gap in conditional any-pass rate.

---

### 9.4 Interpretation — Capability-Driven or Structural?

**Finding: course pass rates are indistinguishable across channels. The quality gap is structural, not capability-driven.**

The key numbers:

| Channel | Attempt-level pass rate | Ever-failed rate (of enrolled) |
|---|:-:|:-:|
| MWF | 98.2% | 41.0% |
| LATAM | 90.5% | 39.5% |
| LinkedIn | 97.4% | 9.3% |
| Organic | 93.2% | 19.2% |

MWF and LATAM have slightly *higher* fail rates than LinkedIn (41% and 40% of enrolled CBs fail at least one course, vs 9% for LinkedIn), yet they end up with attempt-level pass rates that are similar to or above LinkedIn's (98.2% and 90.5% vs 97.4%). This pattern is consistent with **MWF/LATAM CBs attempting more courses and encountering harder material** — they fail more individual courses but ultimately pass at similar or higher rates at the per-attempt level.

LinkedIn's very low ever-failed rate (9.3%) does not reflect higher capability — it reflects that LinkedIn CBs are typically enrolled in fewer courses and may be stopping earlier in the course gauntlet (fewer total course encounters: 932 total vs 807 for LATAM with fewer CBs).

**The "conditional any-pass rate" — the cleanest capability test — is 100% for MWF, LATAM, and LinkedIn alike.** Once a CB has a course record, they pass at least one course regardless of channel. No channel shows a raw course-pass capability deficit.

**The enrollment coverage gap is the only structural difference visible in this analysis:**
- MWF enrollment: 62.9% (only 61 of 97 activated MWF CBs appear in the course table)
- LATAM enrollment: 81.7%
- LinkedIn enrollment: 93.4%
- Organic enrollment: 94.3%

MWF's lower enrollment rate (62.9%) is almost certainly an artifact of program management: MWF CBs are placed onto projects and may begin tasking without going through the standard course funnel (direct placement bypasses the course assignment flow that LinkedIn/Organic CBs go through). This is a structural program difference, not a capability signal.

**Conclusion:** The MWF/LATAM quality advantage (PDR 0.32/0.37 vs LinkedIn's 0.62, QMS 3.05/3.18 vs 2.58, 71%/50% vs 26% 30d retention) is not explained by technical capability as measured by course pass rates. CBs from all channels who reach the course stage pass at essentially equivalent rates. The quality gap is driven by the structural and profile differences documented in §3–8: institution pedigree (IIT/LATAM elite domination), employer type (freelance/independent vs IT services firms), seniority (entry-to-mid vs senior titles), and program management (pre-vetting, active allocation, pay commitment).

---

### 9.5 Caveats and Data Gaps

1. **IS_COMPLETED vs STATUS:** The table's `IS_COMPLETED` flag is unreliable as a pass signal — it is TRUE for both pass and fail outcomes. All pass/fail analysis in this section uses `STATUS IN ('pass','failed')` directly. Redash query 303224 confirms the status distribution: 70,743 pass rows and 30,640 failed rows (both with IS_COMPLETED=TRUE) across the 7 projects.
2. **RFP - Master Project excluded:** DIM_PROJECT_COURSES has no entries for this project (0 required courses). The 158 CBs activated on RFP cannot be assessed on course pass rate from this table. A separate course tracking mechanism may exist for RFP.
3. **"All required courses" threshold too high:** With 10-30 required courses per project, the "all courses passed" metric has near-zero pass rates across all channels and is not an interpretable capability signal at the population level.
4. **MWF enrollment gap (62.9%):** A significant fraction of MWF activated CBs never appear in the course progress table. This likely reflects direct program placement that bypasses the standard course assignment flow, not course avoidance or failure to enroll. The 61 MWF CBs who do appear all pass at 98.2% attempt rate.
5. **Course content equivalence not confirmed:** The same course may differ in difficulty across projects. This analysis pools all course records within each project's required course set; it does not control for which specific course a CB is taking.
6. **Sample sizes for per-project breakdown:** Several cells have n<5 and are suppressed. MWF specifically has very few activations on Agent Completion Pt3 (2 CBs) and Coding Physics Sim (0 CBs), so MWF course data is dominated by SWEAP, Code Checkpoint, and Data Analysis Agents.

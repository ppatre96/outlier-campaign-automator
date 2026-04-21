# Phase 3: Campaign Expansion - Research

**Researched:** 2026-04-20
**Domain:** LinkedIn InMail regeneration, TG classifier extension, pipeline integration
**Confidence:** HIGH

---

## Summary

Phase 3 has two tightly scoped tasks: (1) regenerate InMail copy for three existing STEM campaign IDs using angle F (financial), and (2) review and extend the `classify_tg()` function in `src/figma_creative.py` to cover cohort types that currently fall through to the GENERAL bucket.

The financial angle (F) already exists as the fully implemented control angle in `build_inmail_variants()` with a complete prompt template and proven subject-line format rules. The regeneration task is primarily an operational exercise: call `build_inmail_variants()` with `angle_keys=["F"]` for each of the three STEM campaign IDs, then attach the resulting creative URN back to the existing campaigns via `li_client.create_inmail_ad()`, and write the URN to Sheets.

The TG classifier has 6 buckets today. The scheduling queue and reanalysis results reveal cohort types like `DATA_SCIENTIST`, `MATH`, and `GENERAL` as active labels that fall outside the existing regex patterns. Adding `MATH` (and potentially `FINANCE`, `LEGAL`, `DESIGN`) requires parallel updates to both `classify_tg()` regex patterns and the `build_inmail_variants()` prompt — since copy quality depends on TG-specific framing.

**Primary recommendation:** Write a targeted regen script (`scripts/regen_stem_inmail.py`) for EXP-01 rather than overloading `generate_experiment_creatives.py`, and extend `classify_tg()` for MATH as the highest-priority gap (based on PROJECT.md performance data showing Math at $14.14 CPA with a distinct professional audience).

---

<phase_requirements>
## Phase Requirements

| ID | Description | Research Support |
|----|-------------|------------------|
| EXP-01 | STEM InMail variants regenerated with financial angle (F/A/C) for campaigns 633412886, 635201096, 634012966 | Angle F fully implemented in `build_inmail_variants()`. Campaigns exist in Triggers 2. LINKEDIN_INMAIL_SENDER_URN is set to `urn:li:person:vYrY4QMQH0`. Flow is: call `build_inmail_variants(tg_cat, cohort, claude_key, angle_keys=["F"])` → `li_client.create_inmail_ad()` → `sheets.write_creative()`. |
| EXP-02 | `classify_tg` extended with any new TG buckets needed for additional cohort types | Scheduling queue reveals `DATA_SCIENTIST`, `MATH`, `GENERAL` as active labels. `MATH` has no regex match today. Both `classify_tg()` and `build_inmail_variants()` need updates for each new bucket. |

</phase_requirements>

---

## Project Constraints (from CLAUDE.md)

All copy generated must follow Outlier's approved vocabulary. The copy rules in CLAUDE.md are already enforced at the prompt level via `VOCAB_RULES` string in `src/inmail_copy_writer.py`. No additional enforcement needed for Phase 3, but any new TG-specific prompt additions must also respect:

- Never "job" → "opportunity" or "task"
- Never "compensation" → "payment"
- Never "training"/"learning" → "become familiar with project guidelines"
- Never "role/position" → "opportunity"
- Never "required" → "strongly encouraged"

---

## Current State of STEM Campaigns

### Campaign IDs and Their Context

The three STEM campaign IDs referenced in EXP-01 are tracked exclusively in `.planning/PROJECT.md` and `.planning/ROADMAP.md`. No campaign metadata (angle, URN, creative ID) is stored in the codebase — that data lives in the `Triggers 2` Google Sheet and LinkedIn's campaign manager UI.

| Campaign ID | Label (from PROJECT.md) | Status per ROADMAP |
|-------------|--------------------------|-------------------|
| 633412886 | A | Pending financial angle regen |
| 635201096 | B | Pending financial angle regen |
| 634012966 | C | Pending financial angle regen |

**What "angles" the STEM campaigns currently use:** Not determinable from code inspection alone. The angle a campaign was launched with is stored in Sheets column metadata. Based on ROADMAP.md: these campaigns were created but the financial angle (F) was not applied — they were likely launched with angles A, B, or C (Expertise, Earnings, Flexibility) per the rotate-by-index logic in `_process_inmail_campaigns()`.

**Current angle rotation logic (from `_process_inmail_campaigns()`, `main.py` lines 421–428):**
```python
angle_idx   = i % 3          # 0, 1, 2 across cohorts
angle_label = ["A", "B", "C"][angle_idx]
variants    = build_inmail_variants(tg_cat, cohort, claude_key)
variant     = variants[angle_idx]
```
This never selects angle F (index 0 in ANGLE_CONFIGS dict). Default `angle_keys=None` runs all keys (F, A, B, C) but then picks by index 0→A, 1→B, 2→C — meaning F is generated but never selected. This is the gap EXP-01 fixes.

### Financial Angle (F) — Current Implementation

Angle F is fully implemented. Key facts from `src/inmail_copy_writer.py`:

- `is_control: True` — F is the proven control; all other angles carry a "challenger note" in their prompt
- Subject format rules are explicit: `"[Role] | Flexible Hours & $X/hr"`, `"[Skill] + AI = Flexible $X/hr"`, `"Earn $X/hr with [Skill] + AI"` — never "up to", never "Side Income"
- Body structure: opens with rate → explains tasks → weekly payment → no commitment → TG-specific task example → CTA
- `hourly_rate` parameter defaults to `"$50"` — can be overridden per-call

**To select angle F only:**
```python
variants = build_inmail_variants(tg_cat, cohort, claude_key, angle_keys=["F"])
variant  = variants[0]
```

**Confidence:** HIGH — code is unambiguous.

### How New InMail URNs Are Logged to Sheets

After `li_client.create_inmail_ad()` returns a `creative_urn`, `_process_inmail_campaigns()` calls:
```python
sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
```
`write_creative()` in `src/sheets.py` (line 181) appends a row to the `Creatives` tab with columns: `stg_id`, `creative_name`, `li_creative_id`, `timestamp`. The method does NOT update the `Triggers 2` row's `li_status` column — that is done separately by `sheets.update_li_campaign_id()`.

For the regen path: the STEM campaigns already have existing campaign URNs. The regen script needs to:
1. Construct the `campaign_urn` from the known campaign ID: `f"urn:li:sponsoredCampaign:{campaign_id}"`
2. Call `create_inmail_ad()` with the existing `campaign_urn` — this attaches a new creative to an already-existing campaign
3. Call `sheets.write_creative()` to log the new creative URN
4. Optionally update the Triggers 2 row `li_status` via `sheets.update_li_campaign_id()`

**Confidence:** HIGH — code is unambiguous.

---

## TG Classifier Inventory

### `classify_tg()` — Full Bucket Documentation

Location: `src/figma_creative.py`, line 57.

**Input:** `cohort_name: str` + `rules: list` (list of `(feature, value)` tuples)
**Output:** One of 6 string literals

**Text construction:** The function first concatenates `cohort_name.lower()` + all feature names from `rules`, replaces `__` separators with spaces so `\b` word-boundary regexes match correctly (e.g., `"skills__diagnosis"` → `"skills  diagnosis"`).

| Bucket | Priority | Regex Pattern |
|--------|----------|--------------|
| `DATA_ANALYST` | 1st | `\b(data\|sql\|analyst\|analytics\|tableau\|snowflake\|bigquery\|looker\|power.?bi\|excel\|dashboard\|spreadsheet)\b` |
| `ML_ENGINEER` | 2nd | `\b(ml\|machine.?learning\|deep.?learning\|pytorch\|tensorflow\|llm\|nlp\|neural\|ai.?model\|research.?scientist)\b` |
| `MEDICAL` | 3rd | Complex two-regex pattern covering `\b(doctor\|physician\|clinical\|nurse\|dentist\|surgeon\|orthopedic\|diagnosis\|medicine\|anatomy\|physiology\|surgery\|emergency\|pharmacol\|therapeut\|patient\|hospital\|healthcare)\b` plus specialty suffixes without word boundaries: `(radiolog\|cardiolog\|oncolog\|patholog\|neurolog\|psychiatr\|pediatr\|dermatol\|urolog\|nephrolog\|gastroenterol\|endocrinol\|immunolog\|pulmonol\|ophthal\|anesthesiol\|internal.?med\|medical\|health\|pharma\|biotech\|med.?grad)` |
| `LANGUAGE` | 4th | `\b(hindi\|urdu\|lingui\|translat\|spanish\|french\|german\|arabic\|japanese\|korean\|chinese\|portuguese\|italian\|language)\b` |
| `SOFTWARE_ENGINEER` | 5th | `\b(software\|engineer\|developer\|backend\|frontend\|fullstack\|swe\|devops\|cloud\|aws\|python\|java\|react\|node)\b` |
| `GENERAL` | Fallback | All cohorts that match no pattern above |

**TG_PALETTES** and **TG_ILLUS_VARIANTS** are defined at module level. Any new bucket added to `classify_tg()` that returns a new string value must also have entries in both dicts — otherwise `customizeDesign()` in the Figma plugin JS template will silently skip color and illustration customization.

**Confidence:** HIGH — code read directly.

---

## Gap Analysis: Cohort Types That Fall to GENERAL

### Evidence from `data/scheduling_queue.json`

The scheduling queue (from Phase 2.5 reanalysis stubs) contains:
- `"tg_category": "DATA_SCIENTIST"` — no regex match → falls to GENERAL (not the same as DATA_ANALYST or ML_ENGINEER)
- `"tg_category": "MATH"` — no regex match → falls to GENERAL
- `"tg_category": "GENERAL"` — explicitly labeled GENERAL by reanalysis stub

### Evidence from PROJECT.md Performance Data

PROJECT.md lists Math as a distinct TG with measurable CPA ($14.14). Languages TG has $2.83 CPA. This confirms Math is an active campaign TG that the pipeline encounters — and it currently falls to GENERAL, getting generic copy rather than math-specific framing.

### Analysis of Missing Keywords

**MATH cohorts:** A cohort named something like `fields_of_study__mathematics` or `job_titles_norm__statistician` contains terms like: `math`, `mathematics`, `statistics`, `statistician`, `actuary`, `quantitative`, `algebra`, `calculus`, `physics`, `physic`. None of these appear in any current bucket regex.

The SOFTWARE_ENGINEER regex catches `python` — which a statistician or data scientist might also have — but would not catch pure math signals.

**DATA_SCIENTIST vs ML_ENGINEER:** `research.?scientist` is in ML_ENGINEER but plain `data.?scientist` is not explicitly listed (only caught if the cohort name contains `data` → DATA_ANALYST). A cohort named `job_titles_norm__data_scientist` where title is the primary feature would likely match DATA_ANALYST via `\bdata\b`. But `DATA_SCIENTIST` as a tg_category label from reanalysis implies the reanalysis loop uses its own classification that doesn't align with `classify_tg()`.

**FINANCE cohorts:** Not yet surfaced in scheduling queue, but PROJECT.md's expansion notes mention Finance as a potential new TG. Keywords: `finance`, `accountant`, `banking`, `investment`, `trader`, `economist`, `cfa`, `cpa`, `auditor`. None match current buckets.

**LEGAL cohorts:** Keywords: `lawyer`, `attorney`, `legal`, `paralegal`, `counsel`, `juris`. Not matched.

**DESIGN cohorts:** Keywords: `design`, `ux`, `figma`, `product.?design`, `graphic`. Not matched (but low priority given Outlier's AI task focus).

### Summary: Gaps by Priority

| Cohort Type | Current Bucket | Priority | Rationale |
|-------------|---------------|----------|-----------|
| MATH / Statistics | GENERAL | HIGH | Active TG in PROJECT.md with $14.14 CPA. Needs specific copy (financial urgency matters differently for PhD mathematicians). |
| FINANCE | GENERAL | MEDIUM | Likely to appear in screening data for financial AI tasks. Distinct professional audience with specific pain points. |
| LEGAL | GENERAL | LOW | Possible but no evidence in current screening data. |
| DESIGN | GENERAL | LOW | UX/design TGs less common for AI training tasks. |
| DATA_SCIENTIST | Likely DATA_ANALYST (via `\bdata\b`) | MEDIUM | Subtle misclassification risk. DATA_SCIENTIST and DATA_ANALYST have different professional identities. |

**Confidence:** MEDIUM — MATH gap is HIGH confidence. FINANCE/LEGAL are MEDIUM (plausible but not observed in live data yet).

---

## Recommended New Buckets

### MATH (HIGH priority — add in Phase 3)

**Rationale:** Math is named explicitly in PROJECT.md performance data. The financial angle behaves differently for PhD-level mathematicians vs. software engineers — `inmail_copy_writer.py` even documents this: "These professionals may have higher average earnings and face less financial urgency." MATH-specific copy should acknowledge academic/research context.

**Proposed regex:**
```python
if re.search(r'\b(math|mathematics|statistics|statistician|actuary|actuarial|quantitative|physicist|physics|algebra|calculus|probability|stochastic|mathematician|econometrics|biostatistics)\b', text):
    return "MATH"
```

**Must also add to `TG_PALETTES` and `TG_ILLUS_VARIANTS`** — suggested palette matches DATA_ANALYST (blue tones, analytical feel), illustration variant `["chart", "neural", "chart"]`.

**Insertion point:** Between ML_ENGINEER (2nd) and MEDICAL (3rd) — before SOFTWARE_ENGINEER to avoid `python` catching statisticians first.

### FINANCE (MEDIUM priority — add in Phase 3 if capacity allows)

**Rationale:** Financial domain professionals are an Outlier target. Specific copy for them would reference financial modeling, investment analysis, and the need for domain expertise in AI financial tasks.

**Proposed regex:**
```python
if re.search(r'\b(finance|financial|accounting|accountant|banking|investment|trader|trading|economist|economics|cfa|auditor|audit|wealth.?management|portfolio|equity|hedge|brokerage|fintech)\b', text):
    return "FINANCE"
```

**Note:** `cpa` (Certified Public Accountant) is intentionally excluded from this regex because `cpa` is Outlier's internal term for Cost Per Acquisition — a collision risk in log text that might accidentally feed into the classifier. Use `accountant` or `accounting` instead.

---

## Integration Diagram

```
EXP-01: STEM InMail Regen
─────────────────────────
scripts/regen_stem_inmail.py
  │
  ├─ (1) Build minimal cohort stub from known campaign_id + TG label
  │       tg_cat = "ML_ENGINEER" | "SOFTWARE_ENGINEER" | "MEDICAL" (STEM TGs)
  │
  ├─ (2) build_inmail_variants(tg_cat, cohort, claude_key, angle_keys=["F"])
  │       → src/inmail_copy_writer.py: angle F prompt
  │       → LiteLLM proxy → claude-sonnet-4-6
  │       → InMailVariant(angle="F", subject=..., body=..., cta_label=...)
  │
  ├─ (3) li_client.create_inmail_ad(
  │           campaign_urn=f"urn:li:sponsoredCampaign:{campaign_id}",
  │           sender_urn=LINKEDIN_INMAIL_SENDER_URN,
  │           subject=variant.subject, body=variant.body, cta_label=variant.cta_label
  │       )
  │       → POST https://api.linkedin.com/v2/adInMailContents  (v202502)
  │       → POST https://api.linkedin.com/rest/adCreatives     (v202510)
  │       → returns creative_urn
  │
  └─ (4) sheets.write_creative(stg_id, campaign_name, creative_urn)
          → append to "Creatives" tab in Triggers sheet
          → also update Triggers 2 li_status = "Created:INMAIL_F"

EXP-02: TG Classifier Extension
────────────────────────────────
src/figma_creative.py  classify_tg()
  └─ Add new regex branches (MATH, optionally FINANCE)
  └─ Add to TG_PALETTES dict
  └─ Add to TG_ILLUS_VARIANTS dict

No changes needed to main.py or _process_inmail_campaigns() — classify_tg()
is called inline; returning a new bucket name flows automatically into
build_inmail_variants(tg_cat, ...) which uses tg_cat as a label in the prompt.

experiment_scientist_agent interaction (from Phase 2.5):
  ExperimentScientistAgent.ingest_feedback() → backlog
  → generate_test_directives() → ad-creative-brief-generator
  This is INDEPENDENT of Phase 3. Phase 3 does not read or write the
  experiment backlog. The financial angle is the baseline (is_control=True),
  not a test directive.
```

---

## `_process_inmail_campaigns()` — What It Expects

From `main.py` lines 388–451:

**Inputs required:**
- `selected`: list of cohort objects with `.name`, `.rules`, `.lift_pp`, `._stg_id`, `._stg_name` attributes
- `inmail_sender`: non-empty string (fails fast with `log.error()` and `return` if empty)
- `claude_key`: passed to `build_inmail_variants()` — used to construct the LiteLLM OpenAI client

**Key behavior:**
- Does NOT use `angle_keys` parameter — always calls `build_inmail_variants(tg_cat, cohort, claude_key)` with no angle override, then selects `variants[i % 3]`
- This means angle F (index 0 in ANGLE_CONFIGS) is generated but only selected for cohort index 0 (`variants[0]` when `angle_idx=0`)
- The regen script must call `build_inmail_variants(..., angle_keys=["F"])` directly, bypassing `_process_inmail_campaigns()`

**For the regen:** EXP-01 does NOT need to re-run the full pipeline for these campaigns. It needs a targeted script that:
1. Takes campaign IDs as input
2. Reconstructs a minimal cohort object (name + rules — can be hardcoded for STEM TGs)
3. Calls `build_inmail_variants()` with `angle_keys=["F"]`
4. Calls `create_inmail_ad()` on the existing campaign URN
5. Writes creative URN to Sheets

---

## experiment_scientist_agent Interaction with Phase 3

From Phase 2.5 CONTEXT.md and `src/experiment_scientist_agent.py`:

The `ExperimentScientistAgent` manages an experiment backlog of creative hypotheses (angle/photo_subject combinations). It **does not** know about or manage InMail campaign regeneration directly.

Phase 3's InMail regen is an operational fix, not an experiment. The financial angle is `is_control=True` in `ANGLE_CONFIGS` — it's the baseline, not a test variant. The experiment scientist handles challenger angles (A, B, C and custom). There is no overlap.

The reanalysis loop (`src/reanalysis_loop.py`) stages new cohorts with `angle=A` (financial angle) by default (decision logged in STATE.md). This is correct — new cohorts should start with the proven control. Phase 3 aligns with this: STEM campaign regen also uses angle F.

---

## Known Blockers and Prerequisites

### Hard Prerequisites

| Prerequisite | Status | Impact on Phase 3 |
|-------------|--------|-------------------|
| `LINKEDIN_INMAIL_SENDER_URN` | SET — `urn:li:person:vYrY4QMQH0` (confirmed in `.env`) | EXP-01 is unblocked |
| `LITELLM_API_KEY` | Must be set in `.env` for LiteLLM calls | Required for `build_inmail_variants()` |
| `LINKEDIN_ACCESS_TOKEN` | Must be valid (expires ~June 2026) | Required for `create_inmail_ad()` API calls |
| Google Sheets credentials | `credentials.json` must exist | Required for `sheets.write_creative()` |

### Soft Prerequisites

| Item | Status | Notes |
|------|--------|-------|
| STEM campaign IDs (633412886, 635201096, 634012966) | Exist in Triggers 2 sheet | Need to read stg_id/stg_name from sheet to properly log creative URN |
| TG labels for STEM campaigns | Unknown without sheet read | Caller must know if campaigns are MEDICAL, SOFTWARE_ENGINEER, etc. |
| Stage A recent output for EXP-02 | Stub data only (scheduling_queue.json has TEST_COHORT stubs) | Real assessment of new TG buckets requires a live pipeline run post-Phase 1 fixes |

### Not Blocked

- `LINKEDIN_MEMBER_URN` is NOT required for InMail — only needed for image ad DSC posts
- LinkedIn MDP approval is NOT required — InMail campaigns use existing approved endpoints
- Google Drive NOT required — InMail regen writes to Sheets only

---

## Common Pitfalls

### Pitfall 1: Using `_process_inmail_campaigns()` for Regen

**What goes wrong:** `_process_inmail_campaigns()` creates a new campaign group AND new campaigns, then creates a creative. For STEM regen we want to attach a new creative to an existing campaign, not create new campaigns.
**Why it happens:** The function's contract is "cohorts → new campaign group → new campaigns → new creatives."
**How to avoid:** Call `li_client.create_inmail_ad(campaign_urn=f"urn:li:sponsoredCampaign:{existing_id}", ...)` directly. Do NOT call `create_campaign_group()` or `create_inmail_campaign()`.

### Pitfall 2: Angle F Is Generated But Never Selected by Default

**What goes wrong:** `build_inmail_variants()` with no `angle_keys` generates F, A, B, C. But `_process_inmail_campaigns()` selects `variants[i % 3]` which maps to index 0→A, 1→B, 2→C (the variant at position index, not by angle key).
**Why it happens:** `variants` list order matches iteration order of `ANGLE_CONFIGS` dict (Python 3.7+ insertion order: F, A, B, C). `variants[0]` is the F variant, BUT `angle_label = ["A", "B", "C"][0]` = "A" — a labeling mismatch. The variant at `variants[0]` IS angle F (financial), but the selection logic doesn't know that.
**How to avoid:** Pass `angle_keys=["F"]` explicitly so `variants` has exactly 1 element and `variants[0]` unambiguously returns angle F.

### Pitfall 3: classify_tg() Insertion Order Matters

**What goes wrong:** Adding MATH after SOFTWARE_ENGINEER means cohorts with `python` in their name get classified as SOFTWARE_ENGINEER before the MATH check runs.
**Why it happens:** `classify_tg()` uses early returns — first match wins.
**How to avoid:** Insert MATH before SOFTWARE_ENGINEER (between ML_ENGINEER and MEDICAL in priority order). Math cohorts may have `python` as a secondary feature but their primary identity is quantitative/statistical.

### Pitfall 4: Missing TG_PALETTES / TG_ILLUS_VARIANTS Entry for New Bucket

**What goes wrong:** `customizeDesign()` in the Figma JS template silently skips color and illustration if `tgCategory` is not in `TG_PALETTES`. The image ad is generated but with GENERAL styling.
**Why it happens:** `figma_creative.py` defines these dicts at module level; `classify_tg()` can return a new value not in the dicts.
**How to avoid:** Whenever a new string is returned by `classify_tg()`, add matching entries to both `TG_PALETTES` and `TG_ILLUS_VARIANTS`.
**Note for Phase 3:** InMail campaigns do NOT use Figma palettes or illustration variants — those are image ad features. For InMail-only expansion, `TG_PALETTES`/`TG_ILLUS_VARIANTS` additions are not strictly required until image ads use the same bucket.

### Pitfall 5: Cohort Stub for Regen Missing Required Attributes

**What goes wrong:** `build_inmail_variants()` calls `_cohort_summary(cohort, tg_category)` which accesses `cohort.rules[:4]`, `cohort.name`, and `getattr(cohort, 'lift_pp', 0)`. A stub missing `rules` raises an AttributeError.
**Why it happens:** Regen script constructs a minimal stub, not a full cohort object.
**How to avoid:** Use a dataclass or namedtuple stub with at minimum: `name` (str), `rules` (list, can be empty), `lift_pp` (float, defaults to 0.0).

---

## Architecture Patterns

### Recommended Structure for `scripts/regen_stem_inmail.py`

```python
STEM_CAMPAIGNS = [
    {"id": 633412886, "tg_cat": "ML_ENGINEER",     "name": "STEM Campaign A"},
    {"id": 635201096, "tg_cat": "SOFTWARE_ENGINEER","name": "STEM Campaign B"},
    {"id": 634012966, "tg_cat": "MEDICAL",          "name": "STEM Campaign C"},
]

@dataclass
class StubCohort:
    name: str
    rules: list
    lift_pp: float = 0.0

for cfg in STEM_CAMPAIGNS:
    cohort = StubCohort(name=cfg["name"], rules=[])
    variants = build_inmail_variants(cfg["tg_cat"], cohort, claude_key, angle_keys=["F"])
    variant = variants[0]
    campaign_urn = f"urn:li:sponsoredCampaign:{cfg['id']}"
    creative_urn = li_client.create_inmail_ad(
        campaign_urn=campaign_urn,
        sender_urn=inmail_sender,
        subject=variant.subject,
        body=variant.body,
        cta_label=variant.cta_label,
    )
    sheets.write_creative(cfg["name"], cfg["name"], creative_urn)
    log.info("Regen creative %s → campaign %s", creative_urn, campaign_urn)
```

**Note on TG labels for STEM campaigns:** The actual TG classification of campaigns 633412886, 635201096, 634012966 is not in the codebase — these are identified as "STEM" in PROJECT.md. The planner should either (a) read the stg_name from the Triggers 2 sheet and run `classify_tg()` on it, or (b) hardcode the STEM-appropriate TG labels. Option (b) is acceptable for a targeted regen script.

### EXP-02 Change Pattern

```python
# In src/figma_creative.py, classify_tg() — insert before SOFTWARE_ENGINEER check

if re.search(r'\b(math|mathematics|statistics|statistician|actuary|actuarial|quantitative|physicist|physics|algebra|calculus|probability|mathematician|econometrics|biostatistics)\b', text):
    return "MATH"

# Also add to module-level dicts:
TG_PALETTES["MATH"] = [{"r": 0.78, "g": 0.88, "b": 1.00}, {"r": 0.88, "g": 0.94, "b": 1.00}]
TG_ILLUS_VARIANTS["MATH"] = ["chart", "neural", "chart"]
```

No changes needed to `build_inmail_variants()` itself — it uses `tg_category` as a plain string label passed into the prompt's `{tg_category}` interpolation. The prompt already says "Target audience category: {tg_category}" so returning "MATH" produces "Target audience category: MATH" in the LLM prompt, which is meaningful enough. However, the fallback subjects in `_fallback_subject()` and `_fallback_body()` only handle angles A, B, C — angle F has no fallback and relies on LiteLLM. This is acceptable since F is the primary path.

---

## State of the Art

| Old Approach | Current Approach | When Changed | Impact |
|--------------|------------------|--------------|--------|
| Angle rotation A→B→C always (no financial angle selected) | Financial angle F now designated as `is_control=True`; should be primary | Phase 2.5 data analysis | EXP-01 is fixing the selection logic gap |
| `classify_tg` with 5 buckets | 6 buckets (DATA_ANALYST, ML_ENGINEER, MEDICAL, LANGUAGE, SOFTWARE_ENGINEER, GENERAL) | Original implementation | MATH, FINANCE still missing |
| `generate_experiment_creatives.py` for ad creative experiments | Handles image ad experiments only; InMail regen needs its own script | Phase 2.5 | Need new `regen_stem_inmail.py` |

---

## Open Questions

1. **What are the actual TG labels for campaigns 633412886, 635201096, 634012966?**
   - What we know: They are called "STEM" but STEM is not a `classify_tg()` bucket
   - What's unclear: Whether they are ML_ENGINEER, SOFTWARE_ENGINEER, MEDICAL, or some mix
   - Recommendation: Planner should read `stg_name` from the Triggers 2 sheet for these three rows and run `classify_tg()` on the name to determine the correct TG label. Alternatively, hardcode a reasonable STEM TG (ML_ENGINEER or SOFTWARE_ENGINEER) as a known-good choice.

2. **Should angle F be added to `_fallback_subject()` and `_fallback_body()`?**
   - What we know: Angle F has no fallback in `inmail_copy_writer.py`; fallback dict only covers A, B, C
   - What's unclear: Whether LiteLLM failures during regen are likely enough to need a fallback
   - Recommendation: Add a minimal fallback for F using the proven subject format `"[TG] + AI = Flexible $50/hr"`. Low effort, avoids silent empty subject if LiteLLM is unavailable.

3. **Is `DATA_SCIENTIST` a separate tg_category that needs its own bucket, or is it well-served by DATA_ANALYST?**
   - What we know: `scheduling_queue.json` uses `DATA_SCIENTIST` as a label; the `\bdata\b` regex in DATA_ANALYST would catch it if the cohort name contains "data"
   - What's unclear: Whether cohort names for data scientists consistently contain "data" vs. being driven by job title features like `job_titles_norm__data_scientist`
   - Recommendation: Add `data.?scientist` to the DATA_ANALYST regex as a conservative extension, OR create a separate DATA_SCIENTIST bucket if copy differentiation is desired (data scientists vs. pure business analysts differ in technical depth).

---

## Environment Availability

| Dependency | Required By | Available | Notes |
|------------|-------------|-----------|-------|
| `LINKEDIN_INMAIL_SENDER_URN` | EXP-01 InMail creation | SET — `urn:li:person:vYrY4QMQH0` | Confirmed in `.env` |
| `LITELLM_API_KEY` | `build_inmail_variants()` | Must be set | Not readable from `.env` directly but required |
| `LINKEDIN_ACCESS_TOKEN` | `li_client.create_inmail_ad()` | Present, expires ~June 2026 | Auto-refresh available |
| Google Sheets credentials | `sheets.write_creative()` | `credentials.json` exists | Confirmed in project root |
| `LINKEDIN_MEMBER_URN` | Image ad only | Not required | EXP-01 is InMail only |
| LinkedIn MDP | audienceCounts Stage C | Blocked / bypassed | Not required for Phase 3 |

**No blocking missing dependencies for Phase 3.**

---

## Validation Architecture

`nyquist_validation: false` in `.planning/config.json` — validation section omitted per protocol.

---

## Sources

### Primary (HIGH confidence)
- `src/inmail_copy_writer.py` — direct code read; `build_inmail_variants()`, `ANGLE_CONFIGS`, `VOCAB_RULES`, angle F implementation
- `src/figma_creative.py` lines 57–77 — direct code read; `classify_tg()` function with all 6 bucket regexes
- `main.py` lines 388–451 — direct code read; `_process_inmail_campaigns()` full implementation
- `config.py` lines 40, 56–58 — confirmed `LINKEDIN_INMAIL_SENDER_URN` env var, `LITELLM_*` settings
- `.env` — confirmed `LINKEDIN_INMAIL_SENDER_URN=urn:li:person:vYrY4QMQH0`

### Secondary (MEDIUM confidence)
- `.planning/PROJECT.md` — performance data, Math TG $14.14 CPA, Languages $2.83 CPA
- `.planning/ROADMAP.md` — STEM campaign IDs, Phase 3 task descriptions, success criteria
- `data/scheduling_queue.json` — evidence of `DATA_SCIENTIST`, `MATH`, `GENERAL` as active tg_category labels from reanalysis
- `.planning/STATE.md` — confirmed Phase 2.5 complete, `LINKEDIN_INMAIL_SENDER_URN` set decision (D-06)

### Tertiary (LOW confidence)
- Inference about STEM campaign TG labels (ML_ENGINEER/SOFTWARE_ENGINEER) — not directly verifiable without reading Triggers 2 sheet data

---

## Metadata

**Confidence breakdown:**
- Standard stack: HIGH — all code read directly from source
- Architecture patterns: HIGH — InMail flow is fully implemented; regen pattern is a straightforward extension
- Pitfalls: HIGH — angle selection bug (F generated but not selected) directly observed in code
- TG bucket gaps: MEDIUM — MATH confirmed by PROJECT.md data; FINANCE/LEGAL inferred from domain knowledge
- STEM campaign TG labels: LOW — requires sheet read to confirm

**Research date:** 2026-04-20
**Valid until:** 2026-06-01 (stable codebase; LinkedIn API version 202510 in use)

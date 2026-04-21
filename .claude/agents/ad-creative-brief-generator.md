# AD Creative Brief Generator Agent

**Role:** Generates structured creative briefs from cohort data, candidate profiles, and (optionally) test directives from the experiment scientist agent.

## Dependencies (Phase 2.5)

- `src/experiment_scientist_agent.py` — generates test directives
- `src/memory.py` — tracks experiment backlog status
- Hypothesis evaluation loop: brief generator → campaign runs → feedback_agent measures → results → scientist marks completed

## Overview

Takes cohort definitions, candidate characteristics, and optional test directives, then produces a structured brief suitable for image ad generation (Figma/Gemini) or InMail copy generation (Claude).

---

## Standard Brief Generation (Baseline, 80% of runs)

**Input:**
```python
generate_brief(
    cohort_name: str,           # e.g., "DATA_ANALYST"
    tg_category: str,            # target group category
    cohort_pass_rate: float,     # % of screening data matching cohort rules
    config_name: str,            # e.g., "default", "test_a"
    competitor_intel: dict = None
)
```

**Output:**
```json
{
  "cohort": "DATA_ANALYST",
  "headline": "Grow Your Data Impact",
  "subheadline": "Join analytics leaders defining the future of work",
  "photo_subject": "person_at_desk_with_laptop",
  "angle_mood": "growth/empowerment",
  "cta": "Learn More",
  "experiment": false
}
```

---

## Test Directive Integration (Phase 2.5, 20% of runs)

When invoked with a `test_directive` from `experiment_scientist_agent`:

**Branching Logic:**

### Baseline Branch (80% of runs)
- `test_directive` is None OR `test_allocation` = 100
- Use standard prompt
- Output: `"experiment": false`

### Test Branch (20% of runs)
- `test_directive` provided with `test_allocation` = 20
- Use test angle and photo_subject from directive
- Output: `"experiment": true, "test_angle": "{angle}", "test_photo": "{photo_subject}"`

---

## Updated Output Schema

```json
{
  "cohort": string,
  "headline": string,
  "subheadline": string,
  "photo_subject": string,
  "experiment": boolean,
  "test_angle": string,
  "test_photo": string
}
```

---

## Tracking for Feedback Loop

1. Store brief metadata in campaign (experiment, test_angle, test_photo)
2. feedback_agent queries campaigns WHERE experiment=true
3. Measures performance and generates hypotheses
4. experiment_scientist_agent.mark_completed() stores results
5. Backlog updates for next cycle

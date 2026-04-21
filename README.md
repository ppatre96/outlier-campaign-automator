# Outlier Campaign Agent

Automated LinkedIn campaign pipeline for Outlier (Scale AI). Discovers high-signal audience cohorts from screening data, generates ad copy and image creatives via LLM, and publishes LinkedIn InMail and Static Ad campaigns — all from a single trigger.

## Quick Start

```bash
python main.py --dry-run
```

See `WORKFLOW.md` for full pipeline walkthrough and `AGENT-PIPELINE.md` for sub-agent architecture.

## TG Classifier Buckets

`classify_tg()` in `src/figma_creative.py` maps a cohort (name + rules) to one of these buckets. First regex match wins, so order matters.

| Priority | Bucket | Example signals |
|----------|--------|-----------------|
| 1 | DATA_ANALYST | data, sql, analyst, tableau, snowflake, bigquery, excel |
| 2 | ML_ENGINEER | ml, machine learning, pytorch, tensorflow, llm, nlp |
| 3 | MATH | math, statistics, actuary, quantitative, probability, econometrics |
| 4 | MEDICAL | doctor, clinical, cardiology, oncology, healthcare |
| 5 | LANGUAGE | hindi, spanish, translator, linguist |
| 6 | SOFTWARE_ENGINEER | software, developer, devops, python, java, react |
| 7 | GENERAL | (fallback — no match) |

Each bucket also has a matching entry in `TG_PALETTES` and `TG_ILLUS_VARIANTS` (same file) driving Figma illustration selection.

For exact regex patterns, see `src/figma_creative.py::classify_tg`.

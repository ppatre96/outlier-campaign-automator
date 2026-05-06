"""
Verify the LinkedIn exclude block end-to-end by simulating the 4-source merge
that main.py builds at campaign-creation time:

  1. config.DEFAULT_EXCLUDE_FACETS           — generic (recruiters/sales/BDRs)
  2. family_exclusions_for(...)              — role-adjacent per matched family
  3. stage_a_negative(...)                   — features over-represented in non-activators
  4. per-cohort exclude_add / exclude_remove — overrides on the merged set

Doesn't hit LinkedIn API — just runs the resolver + builder end-to-end so we
can eyeball the final targetingCriteria payload.
"""
import json
import logging

from dotenv import load_dotenv
load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import config
from src.sheets import SheetsClient
from src.linkedin_urn import UrnResolver, feature_col_to_exclude_pair
from src.linkedin_api import _build_targeting_criteria
from src.icp_from_jobpost import family_exclusions_for
from main import _merge_urn_dicts, _subtract_urn_dicts

PROJECT_ID = "69b18ff232af92d076e66165"  # Generalist Checkpoint Evals

# ── Fake meta for the family match (skips the Redash round-trip) ──
job_post_meta = {
    "job_name": "Analyst for AI Training (Data Analytics & Modeling)",
    "domain":   "Data Analytics",
}
project_meta = {"project_name": "Generalist Checkpoint Evals"}

sheets = SheetsClient()
urn_res = UrnResolver(sheets)

# 1. Defaults
print("\n── (1) Defaults: config.DEFAULT_EXCLUDE_FACETS ──")
default_pairs = list(config.DEFAULT_EXCLUDE_FACETS)
for p in default_pairs:
    print(f"  {p}")
default_urns = urn_res.resolve_default_excludes()

# 2. Family-specific
print("\n── (2) Family-specific: family_exclusions_for(...) ──")
family_pairs = family_exclusions_for(
    job_post_meta=job_post_meta, project_meta=project_meta,
    signup_flow_name="Data Analyst Screening T1",
)
for p in family_pairs:
    print(f"  {p}")
family_urns = urn_res.resolve_facet_pairs(family_pairs)

# 3. Data-driven (simulate — the real hit in the pilot was highest_degree_level__Other)
print("\n── (3) Data-driven: stage_a_negative — simulated with the real pilot 1 hit ──")
sim_data_driven_feature_cols = ["highest_degree_level__Other"]
dd_pairs: list[tuple[str, str]] = []
for col in sim_data_driven_feature_cols:
    p = feature_col_to_exclude_pair(col)
    if p:
        dd_pairs.append(p)
        print(f"  {col}  →  {p}")
    else:
        print(f"  {col}  →  (no LinkedIn facet mapping — dropped)")
dd_urns = urn_res.resolve_facet_pairs(dd_pairs)

# 4. Per-cohort overrides (just show the mechanic — synthetic pair)
print("\n── (4) Per-cohort overrides: Cohort.exclude_add / exclude_remove ──")
cohort_add    = [("skills", "Marketing Strategy")]     # pretend a specific cohort wants this extra exclusion
cohort_remove = [("titles", "Sales Representative")]   # pretend another legitimately wants SalesReps back
print(f"  add:    {cohort_add}")
print(f"  remove: {cohort_remove}")
cohort_add_urns    = urn_res.resolve_facet_pairs(cohort_add)
cohort_remove_urns = urn_res.resolve_facet_pairs(cohort_remove)

# Merge all four
shared = _merge_urn_dicts(default_urns, family_urns, dd_urns)
per_cohort = _subtract_urn_dicts(_merge_urn_dicts(shared, cohort_add_urns), cohort_remove_urns)

print("\n── Final merged exclude dict (after all 4 layers) ──")
for facet, urns in per_cohort.items():
    print(f"  {facet}: {urns}")

# Build final payload
include_urns = urn_res.resolve_cohort_rules(
    [("job_titles_norm__Data_Analyst", 1), ("skills__SQL", 1)]
)
payload = _build_targeting_criteria(include_urns, per_cohort)

print("\n── Final targetingCriteria JSON ──")
print(json.dumps(payload, indent=2))

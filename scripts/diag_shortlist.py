"""
Diagnostic: show the new Stage A shortlist for a project after the combo-bias +
broad-skill blocklist + base-role anchor changes.

Runs through engineer_features → pick_target_tier → stage_a and prints the
top cohorts with their rules, n, lift, and final score.
"""
import sys
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import pandas as pd

from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import pick_target_tier, stage_a
from src.icp_from_jobpost import (
    extract_base_role_candidates,
    base_role_feature_columns,
    required_skill_feature_columns,
    derive_icp_from_job_post,
)

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "69b18ff232af92d076e66165"

client = RedashClient()
df_raw = client.fetch_stage1_contributors(PROJECT_ID)
print(f"\nRaw rows: {len(df_raw)}")

df      = engineer_features(df_raw)
freqs   = build_frequency_maps(df, min_freq=5)
df_bin  = binary_features(df, freqs)
bin_cols = [
    c for c in df_bin.columns
    if c.startswith((
        "skills__", "job_titles_norm__", "fields_of_study__",
        "highest_degree_level__", "accreditations_norm__", "experience_band__",
    ))
]
print(f"Binary features: {len(bin_cols)}")

# Meta + base role — resolve project → primary signup flow the same way main.py does
job_post_meta = {}
project_meta  = {}
resolved_flow, resolved_config = None, None
try:
    project_meta = client.fetch_project_meta(PROJECT_ID) or {}
    r = client.resolve_project_to_flow(PROJECT_ID)
    if r:
        resolved_flow, resolved_config = r
        job_post_meta = client.fetch_job_post_meta(resolved_flow) or {}
        print(f"Resolved primary signup flow for project: {resolved_flow}  config={resolved_config!r}")
except Exception as exc:
    print(f"meta fetch error: {exc}")

print(f"\nproject_meta.name:      {project_meta.get('project_name')!r}")
print(f"project_meta.tasker:    {project_meta.get('tasker_name')!r}")
print(f"job_post_meta.job_name: {job_post_meta.get('job_name')!r}")
print(f"job_post_meta.domain:   {job_post_meta.get('domain')!r}")

derived_icp = {}
description = (job_post_meta.get("description") or project_meta.get("description") or "").strip()
if description:
    try:
        derived_icp = derive_icp_from_job_post(description) or {}
    except Exception as exc:
        print(f"derive_icp_from_job_post error: {exc}")

print(f"\nderived_icp.derived_tg_label: {derived_icp.get('derived_tg_label')!r}")
print(f"derived_icp.required_skills:  {derived_icp.get('required_skills')}")
print(f"derived_icp.required_fields:  {derived_icp.get('required_fields')}")

base_role_titles = extract_base_role_candidates(
    job_post_meta=job_post_meta,
    project_meta=project_meta,
    signup_flow_name=resolved_config,
    derived_tg_label=derived_icp.get("derived_tg_label"),
)
print(f"\nbase_role_titles: {base_role_titles}")

title_anchor_cols = base_role_feature_columns(base_role_titles, list(df_bin.columns))
skill_anchor_cols = required_skill_feature_columns(
    derived_icp.get("required_skills", []), list(df_bin.columns),
)
base_role_cols = title_anchor_cols + [c for c in skill_anchor_cols if c not in title_anchor_cols]
print(f"title_anchor_cols: {title_anchor_cols}")
print(f"skill_anchor_cols: {skill_anchor_cols}")
print(f"base_role_cols (final): {base_role_cols}")

tier, target_col, n_icp = pick_target_tier(df_bin)
print(f"\nTier: {tier}  col={target_col}  n_icp={n_icp}\n")

cohorts = stage_a(df_bin, bin_cols, target_col=target_col, base_role_cols=base_role_cols)
print(f"Stage A returned {len(cohorts)} cohorts\n")

rows = []
for i, c in enumerate(cohorts, 1):
    has_base = any(r[0] in set(base_role_cols) for r in c.rules)
    rows.append({
        "#": i,
        "cohort": c.name[:80],
        "rules": len(c.rules),
        "n": c.n,
        "passes": c.passes,
        "pass_rate": round(c.pass_rate, 2),
        "lift_pp": round(c.lift_pp, 2),
        "has_base_role": "Y" if has_base else "",
        "score": round(c.score, 2),
    })

pd.set_option("display.max_colwidth", 80)
pd.set_option("display.width", 170)
print(pd.DataFrame(rows).to_string(index=False))

print("\n── Top 3 cohorts (the shortlist we'd ship to LinkedIn) ──")
for i, c in enumerate(cohorts[:3], 1):
    rules_str = " + ".join(r[0] for r in c.rules)
    has_base = any(r[0] in set(base_role_cols) for r in c.rules)
    tag = "(anchor ✓)" if has_base else "(no anchor)"
    print(f"  {i}. {rules_str}  n={c.n}  lift=+{c.lift_pp:.1f}pp  {tag}")

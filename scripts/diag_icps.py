"""Pull 5 ICP profiles for a project — respecting the same path main.py uses:
stats mode → exemplars from activators; cold_start → worker skills + job post."""
import sys
import json

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import pick_target_tier
from src.icp_exemplars import build_exemplars, format_exemplars_for_slack
from src.icp_from_jobpost import derive_icp_from_job_post, resolve_job_post

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "698318a45989d90bf44b9b53"
c = RedashClient()

# ── 1. Pull frame + engineer features ──
df_raw = c.fetch_stage1_contributors(PROJECT_ID)
print(f"\nRaw rows: {len(df_raw)}")

# ── 2. Always pull metadata (project, job post, worker skills) ──
project_meta  = c.fetch_project_meta(PROJECT_ID) or {}
worker_skills = c.fetch_project_worker_skills(PROJECT_ID) or []
resolved = c.resolve_project_to_flow(PROJECT_ID)
resolved_flow, resolved_config = (resolved if resolved else (None, None))
job_post_meta = c.fetch_job_post_meta(resolved_flow) if resolved_flow else {}

print(f"\nProject name: {project_meta.get('project_name')!r}")
print(f"Worker skills (hard gates): {worker_skills}")
if job_post_meta.get("job_name"):
    print(f"Job post: {job_post_meta['job_name']!r}  domain={job_post_meta.get('domain')!r}")

# ── 3. If frame has rows → exemplar path (stats / sparse). Else → cold_start. ──
if df_raw.empty:
    print("\n── No activator data — falling back to worker-skill + job-post ICP ──")
    raw_post = resolve_job_post(c, project_id=PROJECT_ID, signup_flow_id=resolved_flow)
    derived_icp = derive_icp_from_job_post(raw_post) if raw_post else {}
    print(f"derived_tg_label:  {derived_icp.get('derived_tg_label')!r}")
    print(f"required_skills:   {derived_icp.get('required_skills')}")
    print(f"required_fields:   {derived_icp.get('required_fields')}")
    print(f"required_degrees:  {derived_icp.get('required_degrees')}")
    print(f"geography:         {derived_icp.get('geography')!r}")
    print("\n(No individual CB profiles — no activator data to surface.)")
    sys.exit(0)

df      = engineer_features(df_raw)
freqs   = build_frequency_maps(df, min_freq=5)
df_bin  = binary_features(df, freqs)

tier, target_col, n_icp = pick_target_tier(df_bin)
print(f"\nTier: {tier}  col={target_col}  n_icp={n_icp}")

exemplars = build_exemplars(df_bin, target_col, tier, max_count=5)
print(f"Exemplars built: {len(exemplars)}\n")

print("=" * 120)
print(f"5 ICP PROFILES — {project_meta.get('project_name')}  ({PROJECT_ID})")
print("=" * 120)
print(format_exemplars_for_slack(exemplars))

print("\n" + "=" * 120)
print("RAW DICT (for downstream use) — PII-stripped, cb_id only")
print("=" * 120)
print(json.dumps(exemplars, indent=2, default=str))

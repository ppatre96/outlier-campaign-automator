"""
Support-based cohort mining for Stage 1, ICP class ONLY.

Replaces the "lift vs. baseline" framing that's used by stage_a/stage_b. When
the frame is already post-funnel (every row is a screened CB who made it to the
activator funnel) and baseline activation sits near ceiling, lift stops being
informative — everything interesting lives in *what the activators look like*,
not in *what separates them from the small non-activator pool*.

This script answers, for a given project:

  1. Among our N activators, what are the most common individual profile
     facets? (Univariate support.)
  2. What are the most common 2-way overlaps? 3-way? (Frequent itemsets.)
  3. When we anchor on the project's base role (detected via the LLM +
     `_BASE_ROLE_FAMILIES`), what do the top anchored combos look like?

No statistical gating. No lift calculation. Just frequency + coverage, with a
minimum support floor so we don't surface noise.
"""
import itertools
import sys
from collections import Counter

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import pandas as pd

from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import pick_target_tier
from src.icp_from_jobpost import (
    extract_base_role_candidates,
    base_role_feature_columns,
    required_skill_feature_columns,
    derive_icp_from_job_post,
)

# ── Tunables ──────────────────────────────────────────────────────────────────
MIN_SUPPORT     = 8        # at least this many ICPs share the cohort — below is noise
MAX_COVERAGE    = 0.50     # cohort covering >50% of ICPs is too broad to target on LinkedIn
TOP_N_DISPLAY   = 15
TOP_FEATURES_FOR_COMBOS = 25   # beam width for combo enumeration

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "698318a45989d90bf44b9b53"

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

# Resolve meta + derived ICP to get base-role anchors (same path as main.py stats mode).
project_meta = client.fetch_project_meta(PROJECT_ID) or {}
r = client.resolve_project_to_flow(PROJECT_ID)
job_post_meta, resolved_config = {}, None
if r:
    resolved_flow, resolved_config = r
    job_post_meta = client.fetch_job_post_meta(resolved_flow) or {}

derived_icp = {}
description = (job_post_meta.get("description") or project_meta.get("description") or "").strip()
if description:
    try:
        derived_icp = derive_icp_from_job_post(description) or {}
    except Exception as exc:
        print(f"LLM ICP extraction failed: {exc}")

base_role_titles = extract_base_role_candidates(
    job_post_meta=job_post_meta, project_meta=project_meta,
    signup_flow_name=resolved_config, derived_tg_label=derived_icp.get("derived_tg_label"),
)
title_anchor_cols = base_role_feature_columns(base_role_titles, list(df_bin.columns))
skill_anchor_cols = required_skill_feature_columns(
    derived_icp.get("required_skills", []), list(df_bin.columns),
)
anchor_cols = title_anchor_cols + [c for c in skill_anchor_cols if c not in title_anchor_cols]
print(f"\nBase-role anchors: titles={title_anchor_cols}  skills={skill_anchor_cols}")

tier, target_col, n_icp = pick_target_tier(df_bin)
print(f"Tier: {tier}  col={target_col}  n_icp={n_icp}\n")

# ── The core shift: restrict to ICPs only, mine frequent itemsets ──
icp_mask = df_bin[target_col].fillna(False).astype(bool)
icp_df = df_bin.loc[icp_mask]
n_icp = len(icp_df)
print(f"Working with {n_icp} ICPs. min_support={MIN_SUPPORT}  max_coverage={MAX_COVERAGE:.0%}\n")

# ── 1. Univariate support ──
uni_rows = []
for col in bin_cols:
    support = int(icp_df[col].fillna(False).astype(bool).sum())
    if support < MIN_SUPPORT:
        continue
    coverage = support / n_icp
    if coverage > MAX_COVERAGE:
        continue
    uni_rows.append({
        "feature":   col,
        "support":   support,
        "coverage":  round(coverage, 3),
        "is_anchor": col in anchor_cols,
    })

uni_rows.sort(key=lambda r: (-r["support"]))
pd.set_option("display.max_colwidth", 80); pd.set_option("display.width", 170)

print("=" * 100)
print(f"TOP {TOP_N_DISPLAY} UNIVARIATES BY SUPPORT — ranked by how many ICPs share this facet")
print("=" * 100)
print(pd.DataFrame(uni_rows[:TOP_N_DISPLAY]).to_string(index=False))

# ── 2. 2-way combos ──
top_features = [r["feature"] for r in uni_rows[:TOP_FEATURES_FOR_COMBOS]]
# Include anchors in the combo pool even if they didn't crack the top 25 by support.
for a in anchor_cols:
    if a in icp_df.columns and a not in top_features:
        top_features.append(a)

two_rows = []
for f1, f2 in itertools.combinations(top_features, 2):
    mask = (icp_df[f1] == 1) & (icp_df[f2] == 1)
    support = int(mask.sum())
    if support < MIN_SUPPORT:
        continue
    coverage = support / n_icp
    if coverage > MAX_COVERAGE:
        continue
    anchored = (f1 in anchor_cols) or (f2 in anchor_cols)
    two_rows.append({
        "cohort":   f"{f1} + {f2}",
        "support":  support,
        "coverage": round(coverage, 3),
        "anchored": "Y" if anchored else "",
    })
two_rows.sort(key=lambda r: (-r["support"]))

print("\n" + "=" * 100)
print(f"TOP {TOP_N_DISPLAY} 2-WAY COMBOS BY SUPPORT")
print("=" * 100)
print(pd.DataFrame(two_rows[:TOP_N_DISPLAY]).to_string(index=False))

# ── 3. Anchored-only cohorts (what we'd actually ship) ──
print("\n" + "=" * 100)
print("ANCHORED COHORTS ONLY — these are what we'd ship to LinkedIn (must contain a base-role feature)")
print("=" * 100)
anchored = [r for r in uni_rows if r["is_anchor"]] + [r for r in two_rows if r["anchored"] == "Y"]
# Keep the columns consistent
rows: list[dict] = []
for r in anchored:
    if "cohort" in r:
        rows.append({"cohort": r["cohort"], "support": r["support"], "coverage": r["coverage"]})
    else:
        rows.append({"cohort": r["feature"], "support": r["support"], "coverage": r["coverage"]})
rows.sort(key=lambda r: -r["support"])
print(pd.DataFrame(rows[:TOP_N_DISPLAY]).to_string(index=False))

# ── 4. "What we'd shortlist at cap=3" ──
print("\n" + "=" * 100)
print("PROPOSED SHORTLIST (top 3 anchored cohorts, support-ranked)")
print("=" * 100)
for i, r in enumerate(rows[:3], 1):
    print(f"  {i}. {r['cohort']}")
    print(f"     support={r['support']}  coverage={r['coverage']:.1%} of {n_icp} ICPs")

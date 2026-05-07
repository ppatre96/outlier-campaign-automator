"""
One-off diagnostic: re-pull the Stage 1 contributor frame for project
69b18ff232af92d076e66165 (Generalist Checkpoint Evals) and print every
univariate feature's uplift stats exactly as stage_a() would compute them.

This does NOT modify any pipeline code — it just reimplements the univariate
loop from src/analysis.py:stage_a() so we can see the per-feature numbers that
the INFO logs don't emit.

Usage:
    source venv/bin/activate && python3 scripts/diag_univariates.py
"""
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s %(name)s — %(message)s")

import pandas as pd

from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import pick_target_tier, segment_stats, passes_thresholds

import sys
PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "69b18ff232af92d076e66165"

client = RedashClient()
df_raw = client.fetch_stage1_contributors(PROJECT_ID)
print(f"\nRaw rows: {len(df_raw)}")

df       = engineer_features(df_raw)
freqs    = build_frequency_maps(df, min_freq=5)
df_bin   = binary_features(df, freqs)
bin_cols = [
    c for c in df_bin.columns
    if c.startswith((
        "skills__", "job_titles_norm__", "fields_of_study__",
        "highest_degree_level__", "accreditations_norm__", "experience_band__",
    ))
]
print(f"Binary features: {len(bin_cols)}")

target_tier, target_col, n_icp = pick_target_tier(df_bin)
print(f"Target tier: {target_tier}  col={target_col}  n_icp={n_icp}")

df_bin["_is_pass"] = df_bin[target_col].fillna(False).astype(bool)
total    = len(df_bin)
n_pass   = int(df_bin["_is_pass"].sum())
baseline = n_pass / total
print(f"Baseline: {baseline*100:.2f}%  ({n_pass}/{total})\n")

records = []
for col in bin_cols:
    mask = df_bin[col] == 1
    if mask.sum() == 0:
        continue
    s   = segment_stats(df_bin, mask, baseline)
    ok, reason = passes_thresholds(s)
    records.append({
        "feature":     col,
        "n":           s["n"],
        "passes":      s["passes"],
        "pass_rate":   round(s["pass_rate"], 2),
        "lift_pp":     round(s["lift_pp"], 2),
        "p_value":     f"{s['p_value']:.2e}",
        "accepted":    ok,
        "reject":      reason,
    })

uni = pd.DataFrame(records)
accepted = uni[uni["accepted"]].sort_values("lift_pp", ascending=False)

print(f"Univariates tested: {len(uni)}")
print(f"Univariates accepted: {len(accepted)}\n")

# Print accepted univariates with full stats
pd.set_option("display.max_colwidth", 80)
pd.set_option("display.width", 160)
print("=" * 110)
print("ACCEPTED UNIVARIATES — sorted by lift_pp desc")
print("=" * 110)
print(accepted[["feature", "n", "passes", "pass_rate", "lift_pp", "p_value"]].to_string(index=False))

# Also show the rejected-but-close ones so we can see the edge
edge = uni[~uni["accepted"] & (uni["n"] >= 30)].sort_values("lift_pp", ascending=False).head(10)
print("\n" + "=" * 110)
print("TOP 10 NEAR-MISS UNIVARIATES (n ≥ 30 but rejected) — sorted by lift_pp desc")
print("=" * 110)
print(edge[["feature", "n", "passes", "pass_rate", "lift_pp", "p_value", "reject"]].to_string(index=False))

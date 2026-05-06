"""
One-off: inspect the raw shape of linkedin_certifications / accreditations
for the Generalist Checkpoint cohort to find where the literal "[]" feature
is coming from.
"""
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING)

from collections import Counter

import pandas as pd
from src.redash_db import RedashClient

PROJECT_ID = "69b18ff232af92d076e66165"

client = RedashClient()
df_raw = client.fetch_stage1_contributors(PROJECT_ID)
print(f"Raw rows: {len(df_raw)}")
print(f"Columns containing 'cert' or 'accred': "
      f"{[c for c in df_raw.columns if 'cert' in c.lower() or 'accred' in c.lower()]}\n")

s = df_raw["linkedin_certifications"]
print(f"linkedin_certifications dtype: {s.dtype}")
print(f"NaN count: {s.isna().sum()} / {len(s)}")

# Most common raw values
print("\nTop 10 raw values (repr-shown so '[]' vs NaN vs '' is unambiguous):")
for val, cnt in Counter(repr(v)[:80] for v in s.head(2000)).most_common(10):
    print(f"  {cnt:>5}  {val}")

# Show 5 non-null non-empty examples
non_empty = [v for v in s if v and str(v) not in ("nan", "None", "[]")][:5]
print("\n5 non-empty examples:")
for v in non_empty:
    print(f"  type={type(v).__name__}  value={v!r}"[:200])

"""Quick sanity check: are the other resume_* fields actually pipe-separated text,
or are they JSON arrays like linkedin_certifications turned out to be?"""
from dotenv import load_dotenv
load_dotenv()
from collections import Counter
from src.redash_db import RedashClient
import logging
logging.basicConfig(level=logging.WARNING)

df = RedashClient().fetch_stage1_contributors("69b18ff232af92d076e66165")

cols = [
    "resume_job_title", "resume_job_company", "resume_degree",
    "resume_field", "resume_job_skills", "linkedin_education",
]
for c in cols:
    if c not in df.columns:
        print(f"{c}: MISSING"); continue
    s = df[c]
    sample = s.dropna().head(3).tolist()
    print(f"\n{c}  dtype={s.dtype}  nonnull={s.notna().sum()}/{len(s)}")
    for v in sample:
        print(f"  {repr(v)[:200]}")

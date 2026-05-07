"""Targeted: worker-skill entries for project 698318a4..."""
from dotenv import load_dotenv
load_dotenv()
import logging
logging.basicConfig(level=logging.WARNING)

from src.redash_db import RedashClient
import pandas as pd

PROJECT_ID = "698318a45989d90bf44b9b53"
c = RedashClient()

# Slim query — just the useful fields
df = c._run_query(
    f"""
    SELECT DISTINCT
        WORKER_SKILL_NAME,
        QUALIFICATION_NAME,
        QUALIFICATION_TYPE,
        IS_ASSESSMENT,
        IS_PAY_MULTIPLIER,
        CATEGORY
    FROM SCALE_PROD.VIEW.PROJECT_QUALIFICATIONS_LONG
    WHERE PROJECT_ID = '{PROJECT_ID}'
    ORDER BY CATEGORY, WORKER_SKILL_NAME
    """,
    label="ws-for-project",
)
print(f"Worker-skill rows for project {PROJECT_ID}: {len(df)}\n")
pd.set_option("display.max_colwidth", 80)
pd.set_option("display.width", 180)
print(df.to_string(index=False) if not df.empty else "(no rows)")

# Also show PROJECT_WORKER_SKILL_ENTRY if present
print("\n── PROJECT_WORKER_SKILL_ENTRY (if exists) ──")
df2 = c._run_query(
    f"""
    SELECT DISTINCT WORKER_SKILL_NAME, QUALIFICATION_NAME, CATEGORY
    FROM SCALE_PROD.VIEW.PROJECT_WORKER_SKILL_ENTRY
    WHERE PROJECT_ID = '{PROJECT_ID}'
    LIMIT 50
    """,
    label="pwse-for-project",
)
print(df2.to_string(index=False) if not df2.empty else "(no rows)")

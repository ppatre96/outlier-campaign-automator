"""Dump PUBLIC.JOBPOSTS columns + sample row to see if there's a project_id link."""
from dotenv import load_dotenv
load_dotenv()
import logging
logging.basicConfig(level=logging.WARNING)

from src.redash_db import RedashClient

c = RedashClient()

print("── All columns in PUBLIC.JOBPOSTS ─────────────────────────")
df_cols = c._run_query(
    "SELECT COLUMN_NAME, DATA_TYPE FROM SCALE_PROD.INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_CATALOG='SCALE_PROD' AND TABLE_SCHEMA='PUBLIC' AND TABLE_NAME='JOBPOSTS' "
    "ORDER BY ORDINAL_POSITION",
    label="jobposts-cols",
)
print(df_cols.to_string(index=False) if not df_cols.empty else "(no rows)")

print("\n── Sample JOBPOSTS row for Generalist Checkpoint flow ─────")
df_row = c._run_query(
    "SELECT * FROM PUBLIC.JOBPOSTS WHERE SIGNUP_FLOW_ID = '68cc916d03332450c703573e' LIMIT 1",
    label="jobposts-sample",
)
if df_row.empty:
    print("(no row found)")
else:
    for col in df_row.columns:
        val = df_row.iloc[0][col]
        s = repr(val)[:180]
        print(f"  {col:40s} = {s}")

print("\n── Are there PROJECTS views/tables with 'description'? ────")
df_proj = c._run_query(
    "SELECT TABLE_SCHEMA, TABLE_NAME FROM SCALE_PROD.INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_CATALOG='SCALE_PROD' AND COLUMN_NAME ILIKE '%description%' "
    "AND TABLE_NAME ILIKE '%project%' "
    "ORDER BY 1, 2 LIMIT 20",
    label="projects-with-desc",
)
print(df_proj.to_string(index=False) if not df_proj.empty else "(no rows)")

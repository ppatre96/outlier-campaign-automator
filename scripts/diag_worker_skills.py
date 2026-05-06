"""Find the worker-skill tables that gate CB eligibility for a project."""
from dotenv import load_dotenv
load_dotenv()
import logging
logging.basicConfig(level=logging.WARNING)

from src.redash_db import RedashClient

PROJECT_ID = "698318a45989d90bf44b9b53"

c = RedashClient()

print("── Candidate views containing 'worker_skill' or 'qualification' ──")
df = c._run_query(
    "SELECT TABLE_SCHEMA, TABLE_NAME FROM SCALE_PROD.INFORMATION_SCHEMA.TABLES "
    "WHERE TABLE_CATALOG='SCALE_PROD' "
    "AND (TABLE_NAME ILIKE '%worker_skill%' "
    "     OR TABLE_NAME ILIKE '%qualification%' "
    "     OR TABLE_NAME ILIKE '%project_skill%') "
    "ORDER BY 1, 2",
    label="ws-tables",
)
print(df.to_string(index=False))

print("\n── Columns on PROJECT_WORKER_SKILL_ENTRY ──")
df = c._run_query(
    "SELECT COLUMN_NAME, DATA_TYPE FROM SCALE_PROD.INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_CATALOG='SCALE_PROD' AND TABLE_NAME='PROJECT_WORKER_SKILL_ENTRY' "
    "ORDER BY ORDINAL_POSITION",
    label="pwse-cols",
)
print(df.to_string(index=False))

print("\n── Columns on PROJECT_QUALIFICATIONS_LONG ──")
df = c._run_query(
    "SELECT COLUMN_NAME, DATA_TYPE FROM SCALE_PROD.INFORMATION_SCHEMA.COLUMNS "
    "WHERE TABLE_CATALOG='SCALE_PROD' AND TABLE_NAME='PROJECT_QUALIFICATIONS_LONG' "
    "ORDER BY ORDINAL_POSITION",
    label="pql-cols",
)
print(df.to_string(index=False))

print(f"\n── Worker-skill entries for project {PROJECT_ID} (top 3 candidate queries) ──")
# Try each plausible candidate — whichever returns rows wins.
for sql_label, sql in [
    ("pwse-via-project_id",
     f"SELECT * FROM SCALE_PROD.VIEW.PROJECT_WORKER_SKILL_ENTRY "
     f"WHERE PROJECT_ID = '{PROJECT_ID}' LIMIT 10"),
    ("pql-via-project_id",
     f"SELECT * FROM SCALE_PROD.VIEW.PROJECT_QUALIFICATIONS_LONG "
     f"WHERE PROJECT_ID = '{PROJECT_ID}' LIMIT 10"),
]:
    print(f"\n>> {sql_label}")
    try:
        df = c._run_query(sql, label=sql_label)
        if df.empty:
            print("  (no rows)")
        else:
            print(df.to_string(index=False))
    except Exception as exc:
        print(f"  ERROR: {exc}")

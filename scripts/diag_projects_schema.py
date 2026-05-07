"""Find description/name columns on PUBLIC.PROJECTS and DIM_PROJECTS_LONG."""
from dotenv import load_dotenv
load_dotenv()
import logging
logging.basicConfig(level=logging.WARNING)

from src.redash_db import RedashClient

c = RedashClient()

for schema, table in [
    ("PUBLIC", "PROJECTS"),
    ("VIEW",   "DIM_PROJECTS_LONG"),
]:
    print(f"\n── {schema}.{table} — columns with 'name', 'description', 'domain', 'title', 'flow' ──")
    df = c._run_query(
        f"SELECT COLUMN_NAME, DATA_TYPE FROM SCALE_PROD.INFORMATION_SCHEMA.COLUMNS "
        f"WHERE TABLE_CATALOG='SCALE_PROD' AND TABLE_SCHEMA='{schema}' AND TABLE_NAME='{table}' "
        f"AND (COLUMN_NAME ILIKE '%name%' OR COLUMN_NAME ILIKE '%desc%' "
        f"     OR COLUMN_NAME ILIKE '%domain%' OR COLUMN_NAME ILIKE '%title%' "
        f"     OR COLUMN_NAME ILIKE '%flow%' OR COLUMN_NAME ILIKE '%_id') "
        f"ORDER BY ORDINAL_POSITION",
        label=f"{schema}-{table}-cols",
    )
    print(df.to_string(index=False))

"""Quick lookup: find projects matching a name substring on PUBLIC.PROJECTS."""
import sys
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING)

from src.redash_db import RedashClient

needle = sys.argv[1] if len(sys.argv) > 1 else "openclaw"

c = RedashClient()

sql = f"""
SELECT
    PROJECT_ID              AS project_id,
    PROJECT_NAME            AS project_name,
    COLLOQUIAL_PROJECT_NAME_FROM_PROJECT_TIER AS colloquial_name,
    IS_PAUSED,
    IS_DISABLED,
    RECENT_ACTIVITY_TIME    AS last_activity,
    LEFT(COALESCE(DESCRIPTION, ''), 220)      AS description_excerpt
FROM SCALE_PROD.VIEW.DIM_PROJECTS_LONG
WHERE LOWER(PROJECT_NAME)        LIKE '%{needle.lower()}%'
   OR LOWER(DESCRIPTION)         LIKE '%{needle.lower()}%'
   OR LOWER(COALESCE(COLLOQUIAL_PROJECT_NAME_FROM_PROJECT_TIER, '')) LIKE '%{needle.lower()}%'
QUALIFY ROW_NUMBER() OVER (PARTITION BY PROJECT_ID ORDER BY RECENT_ACTIVITY_TIME DESC NULLS LAST) = 1
ORDER BY is_disabled ASC, is_paused ASC, last_activity DESC NULLS LAST
LIMIT 25
"""

df = c._run_query(sql, label=f"find-project-{needle}")
if df.empty:
    print(f"No projects match {needle!r} on name/tasker_name/description.")
else:
    print(f"Found {len(df)} project(s) matching {needle!r}:\n")
    for _, row in df.iterrows():
        flags = []
        if row.get("is_paused"):    flags.append("PAUSED")
        if row.get("is_disabled"):  flags.append("DISABLED")
        if not flags:               flags.append("ACTIVE")
        print(f"  id:          {row['project_id']}")
        print(f"  name:        {row['project_name']}")
        coll = row.get("colloquial_name")
        if coll:
            print(f"  colloquial:  {coll}")
        print(f"  status:      {' '.join(flags)}")
        print(f"  last activity: {row.get('last_activity') or '—'}")
        excerpt = (row.get("description_excerpt") or "").strip()
        if excerpt:
            print(f"  excerpt:     {excerpt}")
        print()

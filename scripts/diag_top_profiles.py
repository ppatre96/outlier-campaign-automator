"""
Pull the 5 top paid-activator profiles from a project frame, filtered for
homogeneity (same archetype — Software Engineer / coding background). Different
from `build_exemplars()` which intentionally diversifies across degrees /
countries / companies. Here we WANT the pattern.

Criteria:
  1. T3 activators only (did paid work on the project).
  2. Archetype filter — profile must have at least one of: a software-dev
     title, or one of the project's required skills (Python / JavaScript /
     Java / TypeScript / Go / etc.).
  3. Sort by TOTAL_PAYOUT_ATTEMPTS desc → "high amount of work".
  4. Tiebreak: recency (LAST_TASK_SUBMITTED).
  5. Top 5.
"""
import sys
from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING)

import pandas as pd

from src.redash_db import RedashClient
from src.icp_exemplars import _top_skills, _shorten_pipe_list

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "698318a45989d90bf44b9b53"
c = RedashClient()
df = c.fetch_stage1_contributors(PROJECT_ID)
print(f"\nRaw rows: {len(df)}")

# T3 activators
icp = df[df["t3_activated"].fillna(False).astype(bool)].copy()
print(f"T3 activators: {len(icp)}")

# Archetype filter — must look like a software engineer on their resume.
# Check: (a) has a software-dev keyword in resume_job_title OR
#        (b) lists Python/JavaScript/Java/TypeScript/Go/React as a skill.
TITLE_KEYWORDS = [
    "software engineer", "software developer", "backend engineer", "frontend engineer",
    "full stack", "full-stack", "fullstack", "web developer", "full stack developer",
]
SKILL_KEYWORDS = ["python", "javascript", "typescript", "java", "react", "node.js", "go "]

def _has_archetype(row) -> bool:
    title = str(row.get("resume_job_title") or "").lower()
    if any(kw in title for kw in TITLE_KEYWORDS):
        return True
    skills = str(row.get("resume_job_skills") or "").lower()
    if any(kw in skills for kw in SKILL_KEYWORDS):
        return True
    return False

icp["_archetype"] = icp.apply(_has_archetype, axis=1)
archetype_matched = icp[icp["_archetype"]].copy()
print(f"Archetype-matched (software dev + required skill): {len(archetype_matched)}")

# Sort by paid attempts, then recency
archetype_matched["_attempts"] = pd.to_numeric(
    archetype_matched["total_payout_attempts"], errors="coerce",
).fillna(0).astype(int)
archetype_matched["_last_task"] = pd.to_datetime(
    archetype_matched["last_task_date"], errors="coerce", utc=True,
)
archetype_matched = archetype_matched.sort_values(
    ["_attempts", "_last_task"], ascending=[False, False],
).head(5)

print(f"\n── Top 5 homogeneous, high-volume, high-quality profiles ──")
pd.set_option("display.max_colwidth", 60)
pd.set_option("display.width", 200)

rows = []
for _, r in archetype_matched.iterrows():
    title = _shorten_pipe_list(str(r.get("resume_job_title") or ""), max_items=2, max_chars=70)
    company = _shorten_pipe_list(str(r.get("resume_job_company") or ""), max_items=2, max_chars=50)
    skills = _top_skills(r.get("resume_job_skills"), limit=5)
    degree = _shorten_pipe_list(str(r.get("resume_degree") or ""), max_items=1, max_chars=30)
    field = _shorten_pipe_list(str(r.get("resume_field") or ""), max_items=1, max_chars=50)
    rows.append({
        "cb_id":          str(r["user_id"]),
        "linkedin_url":   str(r.get("linkedin_url") or ""),
        "title":          title,
        "company":        company,
        "degree":         degree,
        "field":          field,
        "top_skills":     ", ".join(skills[:5]),
        "paid_tasks":     int(r["_attempts"]),
        "last_activity":  (str(r["_last_task"])[:10] if pd.notna(r["_last_task"]) else "—"),
    })

out = pd.DataFrame(rows)
print(out.to_string(index=False))

# Markdown-friendly version too
print("\n── Markdown table ──\n")
cols = ["cb_id", "title", "company", "degree", "field", "top_skills", "paid_tasks", "last_activity"]
print("| " + " | ".join(cols) + " |")
print("|" + "|".join(["---"] * len(cols)) + "|")
for r in rows:
    print("| " + " | ".join(str(r[c]) for c in cols) + " |")

print("\nLinkedIn URLs:")
for r in rows:
    print(f"  {r['cb_id']}: {r['linkedin_url']}")

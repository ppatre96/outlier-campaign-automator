"""
Build a standalone ICP summary doc for a project:
  1. What's broadly common among activators (univariate support % by facet)
  2. Top multi-facet patterns (2-way combos with support ≥ 8)
  3. 5 example profiles (homogeneous, high-volume, high-quality)

Writes a Markdown doc to data/icp_summary_<project_short>.md that's ready to
hand off to an agency or share in Slack.
"""
import itertools
import sys
from collections import Counter
from pathlib import Path

from dotenv import load_dotenv
load_dotenv()

import logging
logging.basicConfig(level=logging.WARNING)

import pandas as pd

from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import pick_target_tier
from src.icp_exemplars import _top_skills, _shorten_pipe_list
from src.profile_tiering import (
    row_tier_labels,
    extract_schools_from_linkedin_education,
    extract_companies_from_resume,
    classify_school,
    classify_company,
)

PROJECT_ID = sys.argv[1] if len(sys.argv) > 1 else "698318a45989d90bf44b9b53"
MIN_SUPPORT = 8
MAX_COVERAGE = 0.50
TOP_FEATURES_FOR_COMBOS = 25

c = RedashClient()
df_raw = c.fetch_stage1_contributors(PROJECT_ID)
project_meta = c.fetch_project_meta(PROJECT_ID) or {}
worker_skills = c.fetch_project_worker_skills(PROJECT_ID) or []

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

tier, target_col, n_icp = pick_target_tier(df_bin)
icp_mask = df_bin[target_col].fillna(False).astype(bool)
icp_df = df_bin.loc[icp_mask]
n_icp = len(icp_df)

# ── Univariate support, grouped by facet ──
#
# Two different coverage policies serve different purposes:
#   - Tautology filter (coverage ≥ 0.99) — drops `experience_band__Other`
#     which is True for every row when the STAGE1 pipeline doesn't compute
#     years-of-experience. Applied to the univariate summary so tautologies
#     don't clutter the ICP description.
#   - Breadth filter (coverage > MAX_COVERAGE=0.50) — drops features too
#     broad to be useful LinkedIn cohorts when combined. Applied ONLY to
#     the combo section below, so the univariate summary can still show
#     legitimately modal facts like "Bachelors covers 64% of activators".
uni: list[tuple[str, int, float]] = []
for col in bin_cols:
    support = int(icp_df[col].fillna(False).astype(bool).sum())
    if support < MIN_SUPPORT:
        continue
    coverage = support / n_icp
    if coverage >= 0.99:
        continue
    uni.append((col, support, coverage))
uni.sort(key=lambda t: -t[1])

def _facet_of(col: str) -> str:
    if col.startswith("skills__"): return "Skills"
    if col.startswith("job_titles_norm__"): return "Job titles"
    if col.startswith("fields_of_study__"): return "Fields of study"
    if col.startswith("highest_degree_level__"): return "Degree level"
    if col.startswith("accreditations_norm__"): return "Accreditations"
    if col.startswith("experience_band__"): return "Experience band"
    return "Other"

def _value_of(col: str) -> str:
    return col.split("__", 1)[1].replace("_", " ") if "__" in col else col

# ── Top-5 values per facet group for the summary table ──
groups: dict[str, list[dict]] = {}
for col, support, coverage in uni:
    g = _facet_of(col)
    groups.setdefault(g, []).append({
        "value": _value_of(col),
        "support": support,
        "coverage": coverage,
    })

# ── 2-way combos (Apriori-style) ──
# For the combo pool, we ALSO drop MAX_COVERAGE features (anything > 50%)
# because a combo anchored on a too-broad parent ends up too broad itself.
combo_features = [col for col, _, coverage in uni[:TOP_FEATURES_FOR_COMBOS]
                  if coverage <= MAX_COVERAGE]
combos: list[dict] = []
for f1, f2 in itertools.combinations(combo_features, 2):
    mask = (icp_df[f1] == 1) & (icp_df[f2] == 1)
    support = int(mask.sum())
    if support < MIN_SUPPORT:
        continue
    coverage = support / n_icp
    if coverage > MAX_COVERAGE:
        continue
    combos.append({
        "cohort": f"{_value_of(f1)} + {_value_of(f2)}",
        "support": support,
        "coverage": coverage,
    })
combos.sort(key=lambda r: -r["support"])

# ── Prestige tiering — universities + companies across all activators ──
activators_full = df_raw[df_raw["t3_activated"].fillna(False).astype(bool)].copy()

# Approximate country hint from school names (cheap proxy — most Indian CBs
# list "India" or an Indian institute name). Used to bias the country-specific
# company/university matchers.
_country_hint = "india"
try:
    hits = 0
    for _, r in activators_full.iterrows():
        schools = extract_schools_from_linkedin_education(r.get("linkedin_education"))
        text = " | ".join(schools).lower()
        if "india" in text or "iit" in text or "nit" in text or "iim" in text:
            hits += 1
    if hits < 0.1 * len(activators_full):
        _country_hint = None
except Exception:
    pass

uni_counter: Counter = Counter()
uni_top_hits: Counter = Counter()
company_class_counter: Counter = Counter()
company_top_hits: Counter = Counter()
company_lower_hits: Counter = Counter()

for _, r in activators_full.iterrows():
    tiers = row_tier_labels(r, country_hint=_country_hint)
    for s in extract_schools_from_linkedin_education(r.get("linkedin_education")):
        if s:
            uni_counter[s] += 1
    for s in tiers["top_schools"]:
        uni_top_hits[s] += 1
    company_class_counter[tiers["company_class"]] += 1
    if tiers["top_global_company"]:
        company_top_hits[tiers["top_global_company"]] += 1
    if tiers["top_country_company"]:
        company_top_hits[tiers["top_country_company"]] += 1
    if tiers["lower_country_company"]:
        company_lower_hits[tiers["lower_country_company"]] += 1

n_act = len(activators_full)

# ── Top 5 homogeneous high-volume profiles ──
TITLE_KEYWORDS = [
    "software engineer", "software developer", "backend engineer", "frontend engineer",
    "full stack", "full-stack", "fullstack", "web developer",
    "machine learning", "data scientist", "ai engineer", "ml engineer",
]
SKILL_KEYWORDS = ["python", "javascript", "typescript", "java", "react", "node.js"]

activators = df_raw[df_raw["t3_activated"].fillna(False).astype(bool)].copy()

def _has_archetype(row) -> bool:
    t = str(row.get("resume_job_title") or "").lower()
    if any(kw in t for kw in TITLE_KEYWORDS):
        return True
    s = str(row.get("resume_job_skills") or "").lower()
    return any(kw in s for kw in SKILL_KEYWORDS)

matched = activators[activators.apply(_has_archetype, axis=1)].copy()
matched["_attempts"] = pd.to_numeric(matched["total_payout_attempts"], errors="coerce").fillna(0).astype(int)
matched["_last_task"] = pd.to_datetime(matched["last_task_date"], errors="coerce", utc=True)
top5 = matched.sort_values(["_attempts", "_last_task"], ascending=[False, False]).head(5)

# ── Build Markdown ──
project_name = project_meta.get("project_name") or PROJECT_ID
doc_path = Path(f"data/icp_summary_{PROJECT_ID[:12]}.md")
doc_path.parent.mkdir(parents=True, exist_ok=True)

lines: list[str] = []
lines.append(f"# ICP summary — {project_name}")
lines.append("")
lines.append(f"**Project ID:** `{PROJECT_ID}`")
lines.append(f"**Tier:** {tier}   |   **ICPs (activators):** {n_icp} of {len(df_raw)} contributors in frame")
if worker_skills:
    lines.append(f"**Worker-skill gates (platform eligibility, not LinkedIn filter):** {', '.join(worker_skills)}")
lines.append("")

# ── 1. What's broadly common ──
lines.append("## 1. What's broadly common among activated profiles")
lines.append("")
lines.append("_Support-mined on the ICP class only — for each facet, the values shared by the most activators. Coverage = % of ICPs that have the value._")
lines.append("")
facet_order = ["Skills", "Job titles", "Fields of study", "Degree level", "Accreditations", "Experience band"]
for facet in facet_order:
    entries = groups.get(facet, [])
    if not entries:
        continue
    lines.append(f"### {facet}")
    lines.append("")
    lines.append("| Value | Activators | Coverage |")
    lines.append("|---|---:|---:|")
    for e in entries[:8]:
        lines.append(f"| {e['value']} | {e['support']} | {e['coverage']:.0%} |")
    lines.append("")

# ── 2. Top multi-facet patterns ──
lines.append("## 2. Top multi-facet patterns")
lines.append("")
lines.append("_How facets co-occur inside the ICP class. Useful for LinkedIn targeting — a joint combo narrows the audience vs a single skill alone._")
lines.append("")
lines.append("| Pattern | Activators | Coverage |")
lines.append("|---|---:|---:|")
for r in combos[:10]:
    lines.append(f"| {r['cohort']} | {r['support']} | {r['coverage']:.0%} |")
lines.append("")

# ── 3. Prestige tiering ──
lines.append("## 3. University + company prestige distribution")
lines.append("")
lines.append(
    f"_Each activator's schools (from `linkedin_education`) and most recent non-Outlier "
    f"company (from `resume_job_company`) classified against curated tier lists. Country "
    f"hint = `{_country_hint or 'none'}`; country-specific matchers run when this is set._"
)
lines.append("")

lines.append("### Top-tier universities present among activators")
lines.append("")
if uni_top_hits:
    lines.append("| University | Activators | Coverage |")
    lines.append("|---|---:|---:|")
    for uni, cnt in uni_top_hits.most_common(10):
        lines.append(f"| {uni} | {cnt} | {cnt / n_act:.0%} |")
    total_top_uni = sum(uni_top_hits.values())
    unique_acts_with_top = sum(
        1 for _, r in activators_full.iterrows()
        if row_tier_labels(r, country_hint=_country_hint)["any_top_school"]
    )
    lines.append("")
    lines.append(
        f"**Signal:** {unique_acts_with_top} of {n_act} activators ({unique_acts_with_top / n_act:.0%}) "
        f"attended at least one top-tier university."
    )
else:
    lines.append("_No top-tier universities surfaced among activators (either thin LinkedIn data or none attended listed schools)._")
lines.append("")

lines.append("### Company tier mix (most recent non-Outlier employer)")
lines.append("")
lines.append("| Classification | Activators | Share | Meaning |")
lines.append("|---|---:|---:|---|")
_class_labels = {
    "top_global":    ("Top-tier global",     "FAANG+ / elite consulting+banking / top AI labs / unicorns"),
    "top_country":   ("Top-tier in-country", "Product-engineering cos / multinational offices in the CB's country"),
    "lower_country": ("Lower-tier in-country", "Typically IT-services firms — large employer base but lower comp per role"),
    "regular":       ("Regular",             "Company doesn't match either tier list — unclassified"),
    "unknown":       ("Unknown",             "No usable company field on resume"),
}
for cls in ("top_global", "top_country", "lower_country", "regular", "unknown"):
    cnt = company_class_counter.get(cls, 0)
    label, meaning = _class_labels[cls]
    lines.append(f"| {label} | {cnt} | {cnt / n_act:.0%} | {meaning} |")
lines.append("")

if company_top_hits:
    lines.append("**Top companies seen among activators:**")
    lines.append("")
    for company, cnt in company_top_hits.most_common(12):
        lines.append(f"- `{company}` — {cnt} activator(s)")
    lines.append("")

if company_lower_hits:
    lines.append("**Lower-tier in-country companies seen (for agency awareness):**")
    lines.append("")
    for company, cnt in company_lower_hits.most_common(10):
        lines.append(f"- `{company}` — {cnt} activator(s)")
    lines.append("")

# ── 4. Example profiles ──
lines.append("## 4. Example profiles (5 activators fitting the archetype)")
lines.append("")
lines.append("_Filtered for the dominant archetype (software-dev title or required skill), ranked by paid-task volume + recency. Shows the shape of a converted contributor._")
lines.append("")
lines.append("| # | cb_id | Prestige | Title (recent) | Company | Degree / Field | Top skills | Paid tasks | Last activity | LinkedIn |")
lines.append("|---|---|---|---|---|---|---|---:|---|---|")
for i, (_, r) in enumerate(top5.iterrows(), 1):
    cb_id = str(r["user_id"])
    title = _shorten_pipe_list(str(r.get("resume_job_title") or ""), max_items=2, max_chars=55) or "—"
    company = _shorten_pipe_list(str(r.get("resume_job_company") or ""), max_items=2, max_chars=45) or "—"
    degree = _shorten_pipe_list(str(r.get("resume_degree") or ""), max_items=1, max_chars=20) or "—"
    field = _shorten_pipe_list(str(r.get("resume_field") or ""), max_items=1, max_chars=35) or "—"
    skills = ", ".join(_top_skills(r.get("resume_job_skills"), limit=5))
    attempts = int(r["_attempts"])
    last = str(r["_last_task"])[:10] if pd.notna(r["_last_task"]) else "—"
    linkedin = r.get("linkedin_url") or "—"
    linkedin_md = f"[link]({linkedin})" if linkedin and linkedin != "—" else "—"
    # Prestige badges
    tiers_i = row_tier_labels(r, country_hint=_country_hint)
    badges: list[str] = []
    if tiers_i["any_top_school"]:
        badges.append("🎓 " + ", ".join(tiers_i["top_schools"][:2]))
    if tiers_i["company_class"] == "top_global":
        badges.append(f"💎 {tiers_i['top_global_company']}")
    elif tiers_i["company_class"] == "top_country":
        badges.append(f"⭐ {tiers_i['top_country_company']}")
    elif tiers_i["company_class"] == "lower_country":
        badges.append(f"⚠️ IT-services: {tiers_i['lower_country_company']}")
    badges_md = " / ".join(badges) if badges else "—"
    lines.append(
        f"| {i} | `{cb_id}` | {badges_md} | {title} | {company} | {degree} / {field} | {skills} | {attempts:,} | {last} | {linkedin_md} |"
    )
lines.append("")
lines.append("_Prestige badge legend: 🎓 top-tier university, 💎 top-tier global company, ⭐ top-tier in-country company, ⚠️ IT-services / lower-tier employer in country._")
lines.append("")

# ── 5. Targeting bullets ──
lines.append("## 5. Targeting takeaways for an agency")
lines.append("")
top_skill_names = [g["value"] for g in groups.get("Skills", [])[:3]]
top_degree = groups.get("Degree level", [{}])[0].get("value", "?")
top_field = groups.get("Fields of study", [{}])[0].get("value", "?")
lines.append(f"- **Primary skill anchors:** {', '.join(top_skill_names)} — require LinkedIn targeting to include at least one.")
lines.append(f"- **Degree floor:** {top_degree} (and above).")
lines.append(f"- **Most represented field:** {top_field}.")
if combos:
    lines.append(f"- **Strongest combo to A/B test:** `{combos[0]['cohort']}` ({combos[0]['support']} activators / {combos[0]['coverage']:.0%}).")
lines.append("- **Geography:** Global, minus platform-blocked countries (per `PROJECT_QUALIFICATIONS_LONG`).")
lines.append("- **Exclusions:** Sales / Staffing / Recruiting titles and skills (hurt conversion).")
# Prestige-aware bullets — low thresholds because any non-zero signal here
# is informative for an agency. The tier lists are conservative, so if even
# 3-5% of activators show up in them, the presence is meaningful.
_top_global_pct  = company_class_counter.get("top_global",    0) / n_act if n_act else 0
_top_country_pct = company_class_counter.get("top_country",   0) / n_act if n_act else 0
_lower_pct       = company_class_counter.get("lower_country", 0) / n_act if n_act else 0
if _top_global_pct + _top_country_pct > 0:
    lines.append(
        f"- **Employer prestige:** {_top_global_pct:.0%} at top-tier global cos, "
        f"{_top_country_pct:.0%} at top-tier in-country cos. "
        f"LinkedIn `currentCompanies` targeting using these names as a positive-signal "
        f"boost is a cheap win; most activators are at mid-tier / smaller firms so keep "
        f"this as a bonus layer, not a hard filter."
    )
if _lower_pct > 0.03:
    lines.append(
        f"- **Underperforming employer segment:** {_lower_pct:.0%} at lower-tier IT-services firms "
        f"(TCS / Infosys / Wipro / Cognizant / etc.) — "
        f"consider excluding these via LinkedIn `currentCompanies` exclude facet. "
        f"Activators from these companies exist but are under-represented relative to "
        f"their employer share of the Indian software workforce."
    )
unique_top_uni_acts = sum(
    1 for _, r in activators_full.iterrows()
    if row_tier_labels(r, country_hint=_country_hint)["any_top_school"]
)
if n_act and unique_top_uni_acts / n_act >= 0.10:
    top_uni_names = [u for u, _ in uni_top_hits.most_common(3)]
    lines.append(
        f"- **University prestige:** {unique_top_uni_acts / n_act:.0%} of activators attended a "
        f"top-tier university (strongest: {', '.join(top_uni_names)}). Use as an `schools` "
        f"facet signal on LinkedIn for an anchored-combos cohort."
    )
lines.append("")

doc_path.write_text("\n".join(lines))
print(f"\nWrote {doc_path} ({len(lines)} lines, {doc_path.stat().st_size:,} bytes)")
print("─" * 80)
print("\n".join(lines))

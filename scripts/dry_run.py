"""
End-to-end dry run for a given flow_id or project_id.

Runs all analysis + creative generation stages but does NOT:
  - Write cohorts to Google Sheets
  - Create LinkedIn campaigns or creatives

Stages covered:
  0   Discover available screening config names via Redash
  1   Fetch screening data (Redash → Snowflake)
  2   Feature engineering (binary features)
  3   Stage A — cohort discovery
  4   Stage B — beam-search refinement
  5+6 Stage C — URN resolution + LinkedIn audience validation
       NOTE: audienceCounts API requires MDP approval → returns 400.
             Falls back to top-N Stage B cohorts.
  7   [dry-run] Print cohorts (no sheet write)
  8   Generate ad creatives (Gemini) + upload to Google Drive

Usage:
  # by signup_flow_id (original pipeline entry point)
  PYTHONPATH=. python3 scripts/dry_run.py --flow-id 69a7a186d91acccdf955b912
  PYTHONPATH=. python3 scripts/dry_run.py --flow-id 69a7a186d91acccdf955b912 --config-name "Clinical Medicine - Cardiology"

  # by project_id (Outlier activation/starting project) — auto-resolves flow + config
  PYTHONPATH=. python3 scripts/dry_run.py --project-id 698a172324c01532c2f92a0d
  PYTHONPATH=. python3 scripts/dry_run.py --project-id 698a172324c01532c2f92a0d --skip-creatives
"""
import argparse
import logging
import os
import shutil
import sys
from pathlib import Path

from dotenv import load_dotenv

load_dotenv(Path(__file__).parent.parent / ".env")

import config
from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import stage_a, stage_b
from src.figma_creative import build_copy_variants
from src.midjourney_creative import generate_midjourney_creative
from src.gdrive import upload_creative
from src.figma_upload import prepare_for_figma

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("dry_run")


# ── Discovery SQL — find config names for a given flow_id ─────────────────────
_DISCOVER_SQL = """
SELECT DISTINCT r.NAME AS config_name, COUNT(*) AS n
FROM VIEW.APPLICATION_CONVERSION ac
INNER JOIN PUBLIC.GROWTHRESUMESCREENINGRESULTS g
  ON ac.EMAIL = g.CANDIDATE_EMAIL
JOIN PUBLIC.RESUMESCREENINGCONFIGS r
  ON g.RESUME_SCREENING_CONFIG_ID = r._ID
WHERE ac.SIGNUP_FLOW_ID = '{flow_id}'
  AND g.CREATED_AT >= '2024-01-01'
GROUP BY 1
ORDER BY n DESC
LIMIT 20
"""


def _discover_configs(snowflake: RedashClient, flow_id: str) -> list[tuple[str, int]]:
    sql = _DISCOVER_SQL.format(flow_id=flow_id.replace("'", "''"))
    df  = snowflake._run_query(sql, label=f"discover-{flow_id[:12]}")
    if df.empty:
        return []
    return list(zip(df["config_name"].tolist(), df["n"].tolist()))


def _sep(char: str = "=", width: int = 70) -> str:
    return char * width


def _try_stage_c(cohorts_b, li_token: str) -> list:
    """
    Attempt Stage C (URN resolution + audience counts).
    Returns selected cohorts, or empty list if blocked (MDP not approved).
    """
    try:
        from src.sheets import SheetsClient
        from src.linkedin_urn import UrnResolver
        from src.linkedin_api import LinkedInClient
        from src.stage_c import stage_c

        sheets    = SheetsClient()
        li_client = LinkedInClient(li_token)
        urn_res   = UrnResolver(sheets)
        return stage_c(cohorts_b, urn_res, li_client)
    except Exception as exc:
        log.warning("Stage C unavailable (%s) — will use Stage B top cohorts", exc)
        return []


def run(
    flow_id: str | None = None,
    project_id: str | None = None,
    config_name: str | None = None,
    skip_creatives: bool = False,
) -> None:
    if not flow_id and not project_id:
        print("ERROR: Provide either --flow-id or --project-id")
        sys.exit(1)

    entry_label = f"project_id={project_id}" if project_id else f"flow_id={flow_id}"
    print(_sep())
    print(f"  DRY RUN — {entry_label}")
    print(_sep())

    li_token = (
        os.getenv("LINKEDIN_ACCESS_TOKEN") or
        os.getenv("LINKEDIN_TOKEN") or
        config.LINKEDIN_TOKEN
    )

    snowflake = RedashClient()

    # ── Stage 0: resolve project_id → flow_id + config_name if needed ────────
    print(f"\n[Stage 0] Resolving entry point ...")
    if project_id:
        print(f"  Input: project_id = {project_id}")
        result = snowflake.resolve_project_to_flow(project_id)
        if not result:
            print("  ERROR: No signup flow found for this project_id.")
            print("         The project may not have any screenings since 2024-01-01.")
            sys.exit(1)
        resolved_flow_id, resolved_config = result
        flow_id     = resolved_flow_id
        config_name = config_name or resolved_config
        print(f"  Resolved → signup_flow_id : {flow_id}")
        print(f"             config_name    : {config_name}")
    elif config_name:
        print(f"  Using explicit flow_id='{flow_id}' config='{config_name}'")
    else:
        configs = _discover_configs(snowflake, flow_id)
        if not configs:
            print("  ERROR: No screening results found for this flow_id in Redash.")
            sys.exit(1)
        print(f"  Found {len(configs)} config(s):")
        for name, n in configs:
            print(f"    '{name}'  ({n:,} rows)")
        config_name = configs[0][0]
        print(f"  → Using: '{config_name}'")

    # ── Stage 1: Fetch screening data ─────────────────────────────────────────
    print(f"\n[Stage 1] Fetching screening data ...")
    from datetime import date
    end_date = date.today().isoformat()   # always use today — avoids stale SCREENING_END_DATE
    df_raw = snowflake.fetch_screenings(flow_id, config_name, end_date=end_date)
    if df_raw.empty:
        print("  ERROR: No rows returned. Screening data may not exist yet.")
        sys.exit(1)

    total = len(df_raw)
    if "resume_screening_result" in df_raw.columns:
        passes = int(df_raw["resume_screening_result"].astype(str).str.upper().eq("PASS").sum())
        fails  = int(df_raw["resume_screening_result"].astype(str).str.upper().eq("FAIL").sum())
        print(f"  Total rows: {total:,}  |  PASS: {passes:,}  |  FAIL: {fails:,}")
    else:
        print(f"  Total rows: {total:,}")

    # ── Stage 2: Feature engineering ─────────────────────────────────────────
    print(f"\n[Stage 2] Feature engineering ...")
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
    print(f"  Candidates   : {len(df_bin):,}")
    print(f"  Binary cols  : {len(bin_cols)}")
    # Show top features by frequency
    top_feats = sorted(
        [(c, freqs.get(c, freqs.get(c.split("__", 1)[-1], 0))) for c in bin_cols],
        key=lambda x: x[1], reverse=True,
    )[:8]
    if top_feats:
        print(f"  Top features : " + ", ".join(f"{c.split('__',1)[-1]}({n})" for c,n in top_feats))

    # ── Stage 3: Stage A ──────────────────────────────────────────────────────
    print(f"\n[Stage 3] Stage A — cohort discovery ...")
    cohorts_a = stage_a(df_bin, bin_cols)
    if not cohorts_a:
        print("  No cohorts found — check pass rate distribution and sample size.")
        sys.exit(1)
    print(f"  Cohorts found: {len(cohorts_a)}")
    for i, c in enumerate(cohorts_a[:5]):
        pr = getattr(c, "pass_rate", None)
        pr_str = f"  pass_rate={pr:.1f}%" if isinstance(pr, float) else ""
        print(f"    [{i+1}] {c.name}{pr_str}")
    if len(cohorts_a) > 5:
        print(f"    ... and {len(cohorts_a) - 5} more")

    # ── Stage 4: Stage B ──────────────────────────────────────────────────────
    print(f"\n[Stage 4] Stage B — beam-search refinement ...")
    cohorts_b = stage_b(df_bin, cohorts_a)
    print(f"  Cohorts after B: {len(cohorts_b)}")
    for i, c in enumerate(cohorts_b[:8]):
        pr = getattr(c, "pass_rate", None)
        pr_str = f"  pass_rate={pr:.1f}%" if isinstance(pr, float) else ""
        n_rules = len(getattr(c, "rules", []))
        print(f"    [{i+1}] {c.name}  ({n_rules} rules){pr_str}")
    if len(cohorts_b) > 8:
        print(f"    ... and {len(cohorts_b) - 8} more")

    # ── Stage 5+6: Stage C ────────────────────────────────────────────────────
    print(f"\n[Stage 5+6] Stage C — URN resolution + audience validation ...")
    selected = []

    if li_token:
        selected = _try_stage_c(cohorts_b, li_token)

    if selected:
        print(f"  Stage C selected {len(selected)} cohort(s)")
    else:
        # Fallback: top N from Stage B
        n = min(config.MAX_CAMPAIGNS, len(cohorts_b))
        selected = cohorts_b[:n]
        print(f"  Stage C bypassed (audienceCounts API requires MDP approval).")
        print(f"  Using top {n} Stage B cohort(s) as proxy for selected campaigns.")

    # ── Stage 7: Cohort summary ────────────────────────────────────────────────
    from src.sheets import make_stg_id
    print(f"\n[Stage 7] [dry-run] {len(selected)} cohort(s) — would write to Triggers sheet:")
    print(_sep("-"))

    for i, cohort in enumerate(selected):
        stg_id   = make_stg_id()
        stg_name = f"{flow_id[:16]} | {cohort.name[:40]}"
        cohort._stg_id   = stg_id
        cohort._stg_name = stg_name

        pr       = getattr(cohort, "pass_rate", None)
        audience = getattr(cohort, "audience_size", None)
        unique   = getattr(cohort, "unique_pct", None)
        rules    = getattr(cohort, "rules", [])

        print(f"\n  [{i+1}] {cohort.name}")
        print(f"        stg_id    : {stg_id}")
        print(f"        stg_name  : {stg_name}")
        if audience is not None:
            print(f"        audience  : {audience:,}")
        if unique is not None:
            print(f"        unique_pct: {unique:.0f}%")
        if pr is not None:
            print(f"        pass_rate : {pr:.1f}%" if isinstance(pr, float) else f"        pass_rate : {pr}")
        rules_preview = [str(r) for r in rules[:4]]
        print(f"        rules ({len(rules)}): {rules_preview}" + (" ..." if len(rules) > 4 else ""))
    print(_sep("-"))

    # ── Stage 8: Creative generation ─────────────────────────────────────────
    if skip_creatives:
        print("\n[Stage 8] Skipped (--skip-creatives)")
    else:
        print(f"\n[Stage 8] Creative generation ({len(selected)} cohort(s)) ...")
        output_dir = Path("data/dry_run_outputs")
        output_dir.mkdir(parents=True, exist_ok=True)

        for i, cohort in enumerate(selected):
            angle_idx   = i % 3
            angle_label = ["A", "B", "C"][angle_idx]

            print(f"\n  ── Cohort {i+1}: {cohort.name} ──")

            # Copy variants — fully derived from cohort signals, no fixed categories
            variants: list[dict] = []
            try:
                # Agent trace logging — shows which sub-agents would be spawned in Claude session
                print(f"     [Agent 8b] ad-creative-brief-generator → tg=unknown, angle={angle_label}, cohort={cohort.name[:40]}")
                print(f"     [Agent 8c] outlier-copy-writer → receives brief with photo_subject")
                print(f"     [Agent 8d] outlier-creative-generator → builds Gemini prompt per variant")
                variants = build_copy_variants(cohort, {})
                log.info("Copy variants generated for cohort %d", i + 1)
            except Exception as exc:
                log.warning("Copy generation failed for cohort %d: %s — skipping creative", i + 1, exc)
                print(f"     Copy gen    : FAILED — {exc}")
                continue

            if not variants:
                print(f"     Copy gen    : returned 0 variants — skipping creative")
                continue

            # TG label is derived by the LLM from the signals — no pre-defined buckets
            tg_label = variants[0].get("tg_label") or cohort.name.replace("__", " ").replace("_", " ")
            print(f"     TG (derived): {tg_label}")

            variant     = variants[angle_idx] if angle_idx < len(variants) else variants[0]
            photo_subject = variant.get("photo_subject") or ""
            if not photo_subject:
                print(f"     Copy gen    : missing photo_subject — skipping creative")
                continue

            print(f"     Angle       : {variant.get('angle', angle_label)}")
            print(f"     Headline    : {variant.get('headline', '')}")
            print(f"     Subheadline : {variant.get('subheadline', '')}")
            print(f"     Photo subj  : {photo_subject}")

            # Generate image
            try:
                tmp_path = generate_midjourney_creative(variant=variant, photo_subject=photo_subject)
                slug     = cohort._stg_id.replace("STG-", "").replace("-", "")
                out_path = output_dir / f"dry_{slug}_{angle_label}.png"
                shutil.copy2(tmp_path, out_path)
                tmp_path.unlink(missing_ok=True)
                size_kb  = out_path.stat().st_size // 1024
                print(f"     Image saved : {out_path} ({size_kb} KB)")
            except Exception as exc:
                print(f"     Image gen   : FAILED — {exc}")
                continue

            # Figma frame name (upload via Claude MCP after the run)
            try:
                proj_label = project_id or flow_id[:16]
                figma_info = prepare_for_figma(out_path, project_id=proj_label, angle=angle_label)
                print(f"     Figma frame : {figma_info['frame_name']}  ({figma_info['bytes']:,} B compressed)")
            except Exception as exc:
                print(f"     Figma prep  : FAILED — {exc}")

            # Drive upload (only when GDRIVE_ENABLED=true in .env)
            if config.GDRIVE_ENABLED:
                try:
                    drive_url = upload_creative(out_path)
                    print(f"     Drive URL   : {drive_url}")
                except Exception as exc:
                    print(f"     Drive upload: FAILED — {exc}")

    # ── Summary ───────────────────────────────────────────────────────────────
    print("\n" + _sep())
    print("DRY RUN COMPLETE")
    if project_id:
        print(f"  project_id  : {project_id}")
    print(f"  flow_id     : {flow_id}")
    print(f"  config_name : {config_name}")
    print(f"  cohorts     : {len(selected)}")
    if not skip_creatives:
        print(f"  creatives   : data/dry_run_outputs/")
    print("  No sheet writes. No LinkedIn campaigns created.")
    print(_sep())


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="End-to-end dry run: fetch → analyse → cohorts → creatives → Drive upload"
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--flow-id",    help="Outlier signup_flow_id (24-char hex)")
    group.add_argument("--project-id", help="Outlier project_id (activation/starting_project_id)")
    parser.add_argument("--config-name", default=None, help="Override screening config name")
    parser.add_argument("--skip-creatives", action="store_true",
                        help="Stop after Stage 7 — skip image generation and Drive upload")
    args = parser.parse_args()

    run(
        flow_id=args.flow_id,
        project_id=args.project_id,
        config_name=args.config_name,
        skip_creatives=args.skip_creatives,
    )

"""
Outlier Campaign Agent — end-to-end orchestrator.

Modes:
  launch  (default) — read PENDING rows → run analysis → generate creatives → create LinkedIn campaigns
  monitor           — check learning phase → score pass rates → pause underperformers → discover new ICPs
"""
import argparse
import json
import logging
import os
import sys
import tempfile
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

import config
from src.sheets import SheetsClient, make_stg_id
from src.redash_db import RedashClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import stage_a, stage_b
from src.linkedin_urn import UrnResolver
from src.linkedin_api import LinkedInClient
from src.stage_c import stage_c
from src.figma_creative import (
    FigmaCreativeClient,
    classify_tg,
    build_copy_variants,
    apply_plugin_logic,
)
from src.midjourney_creative import generate_midjourney_creative
from src.campaign_monitor import (
    check_learning_phase,
    get_pass_rates_from_snowflake,
    score_campaigns,
    pause_campaign,
    discover_new_icps,
    write_monitor_results,
    read_active_campaigns,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


# ── Launch mode ───────────────────────────────────────────────────────────────

def run_launch(dry_run: bool = False) -> None:
    sheets    = SheetsClient()
    sheet_cfg = sheets.read_config()

    li_token   = sheet_cfg.get("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_TOKEN", "")
    claude_key = sheet_cfg.get("CLAUDE_API_KEY") or os.getenv("ANTHROPIC_API_KEY", "")
    mj_token   = sheet_cfg.get("MIDJOURNEY_API_TOKEN") or os.getenv("MIDJOURNEY_API_TOKEN", "")

    if not li_token:
        log.error("LINKEDIN_TOKEN not found in Config tab or environment — aborting")
        sys.exit(1)

    li_client = LinkedInClient(li_token)
    urn_res   = UrnResolver(sheets)
    snowflake = RedashClient()

    pending = sheets.read_pending_rows()
    if not pending:
        log.info("No PENDING rows found — nothing to do")
        return

    log.info("Found %d PENDING rows", len(pending))

    for row in pending:
        flow_id    = row["flow_id"]
        location   = row.get("location", "")
        figma_file = row.get("figma_file", "").strip()
        figma_node = row.get("figma_node", "").strip()

        log.info("=" * 60)
        log.info("Processing flow_id=%s location=%s", flow_id, location)

        config_name = sheet_cfg.get("SCREENING_CONFIG_NAME", "") or flow_id

        try:
            _process_row(
                row=row,
                flow_id=flow_id,
                config_name=config_name,
                location=location,
                figma_file=figma_file,
                figma_node=figma_node,
                sheets=sheets,
                snowflake=snowflake,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                mj_token=mj_token,
                dry_run=dry_run,
            )
        except RuntimeError as exc:
            log.error("HARD STOP for flow %s: %s", flow_id, exc)
            raise
        except Exception as exc:
            log.exception("Unexpected error for flow %s: %s", flow_id, exc)

    log.info("Launch run complete")


def _process_row(
    row, flow_id, config_name, location, figma_file, figma_node,
    sheets, snowflake, li_client, urn_res, claude_key, mj_token, dry_run,
):
    # 1. Snowflake
    df_raw = snowflake.fetch_screenings(flow_id, config_name)
    if df_raw.empty:
        log.warning("No screening data for flow=%s config=%s — skipping", flow_id, config_name)
        return

    log.info("Raw data: %d rows", len(df_raw))

    # 2. Feature engineering
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
    log.info("Binary features: %d", len(bin_cols))

    # 3. Stage A
    cohorts_a = stage_a(df_bin, bin_cols)
    if not cohorts_a:
        log.warning("Stage A found no valid cohorts for flow=%s", flow_id)
        return

    # 4. Stage B
    cohorts_b = stage_b(df_bin, cohorts_a)

    # 5+6. URN resolution + Stage C
    selected = stage_c(cohorts_b, urn_res, li_client)
    if not selected:
        log.warning("No cohorts survived Stage C for flow=%s — skipping", flow_id)
        return

    log.info("Final selected cohorts: %d", len(selected))

    # 7. Write cohorts to sheet
    for cohort in selected:
        stg_id   = make_stg_id()
        stg_name = _cohort_display_name(cohort, flow_id, location)
        facet, criteria = _cohort_to_targeting_json(cohort)
        cohort._stg_id   = stg_id
        cohort._stg_name = stg_name
        cohort._facet    = facet
        cohort._criteria = criteria

    cohort_sheet_rows = [
        {
            "stg_id":                 c._stg_id,
            "stg_name":               c._stg_name,
            "targeting_facet":        c._facet,
            "targeting_criteria_json": c._criteria,
        }
        for c in selected
    ]

    if not dry_run:
        sheets.write_cohorts(row, cohort_sheet_rows)
        log.info("Wrote %d cohorts to sheet row %d", len(cohort_sheet_rows), row["sheet_row"])
    else:
        log.info("[dry-run] Would write %d cohorts", len(cohort_sheet_rows))

    # 8. Generate creatives (Figma clone + Midjourney from-scratch, with fallback)
    has_figma = bool(figma_file and figma_node and claude_key)
    has_mj    = bool(mj_token and claude_key)
    figma_client = FigmaCreativeClient() if has_figma else None

    # One PNG per cohort — rotate variant angle A→B→C across campaigns
    creative_paths: list[Path | None] = []
    all_variants_per_cohort: list[list[dict]] = []

    for i, cohort in enumerate(selected):
        angle_idx    = i % 3
        angle_label  = ["A", "B", "C"][angle_idx]
        tg_cat       = classify_tg(cohort.name, cohort.rules)
        variants: list[dict] = []
        png_path: Path | None = None

        # ── Step 8a: generate copy variants (needed for both paths) ──
        if claude_key:
            try:
                layer_map = (
                    figma_client.get_text_layer_map(figma_file, figma_node)
                    if has_figma else {}
                )
                variants = build_copy_variants(tg_cat, cohort, layer_map, claude_key)
            except Exception as exc:
                log.warning("Copy generation failed for '%s': %s", cohort.name, exc)

        all_variants_per_cohort.append(variants)
        selected_variant = variants[angle_idx] if angle_idx < len(variants) else {}

        if dry_run:
            creative_paths.append(None)
            continue

        # ── Step 8b: Figma clone path ──
        if has_figma and variants:
            try:
                clone_ids = apply_plugin_logic(
                    figma_file, figma_node, variants, tg_cat, claude_key
                )
                if clone_ids:
                    selected_id = clone_ids[angle_idx % len(clone_ids)]
                    pngs = figma_client.export_clone_pngs(figma_file, [selected_id])
                    png_path = pngs[0] if pngs else None
                    log.info(
                        "Figma clone: cohort %d '%s' → angle %s clone %s",
                        i, cohort.name, angle_label, selected_id,
                    )
            except Exception as exc:
                log.warning("Figma creative failed for '%s': %s — will try Midjourney", cohort.name, exc)

        # ── Step 8c: Midjourney from-scratch path (primary if no Figma, fallback otherwise) ──
        if png_path is None and has_mj and selected_variant:
            try:
                png_path = generate_midjourney_creative(
                    tg_category=tg_cat,
                    variant=selected_variant,
                    mj_token=mj_token,
                    claude_key=claude_key,
                )
                log.info(
                    "MJ creative: cohort %d '%s' → angle %s → %s",
                    i, cohort.name, angle_label, png_path,
                )
            except Exception as exc:
                log.warning("Midjourney creative failed for '%s': %s", cohort.name, exc)

        creative_paths.append(png_path)

    # 9+10. LinkedIn campaigns + creative upload
    if dry_run:
        log.info("[dry-run] Skipping LinkedIn campaign creation")
        return

    group_name = f"Outlier {flow_id} {location}".strip()
    group_urn  = li_client.create_campaign_group(group_name)

    for i, cohort in enumerate(selected):
        facet_urns   = urn_res.resolve_cohort_rules(cohort.rules)
        campaign_urn = li_client.create_campaign(
            name=cohort._stg_name,
            campaign_group_urn=group_urn,
            facet_urns=facet_urns,
        )
        campaign_id = campaign_urn.rsplit(":", 1)[-1]
        sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
        log.info("Created campaign %s", campaign_urn)

        png_path = creative_paths[i]
        if png_path and png_path.exists():
            # Use headline/subheadline from the selected variant angle
            variants = all_variants_per_cohort[i] if i < len(all_variants_per_cohort) else []
            angle_idx = i % 3
            variant   = variants[angle_idx] if angle_idx < len(variants) else {}
            headline  = variant.get("headline") or f"Your {_cohort_headline(cohort)} expertise is in demand."
            subhead   = variant.get("subheadline") or "Earn payment doing remote AI tasks on your schedule."

            image_urn    = li_client.upload_image(png_path)
            creative_urn = li_client.create_image_ad(
                campaign_urn=campaign_urn,
                image_urn=image_urn,
                headline=headline,
                description=subhead,
            )
            sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
            log.info("Attached creative %s to campaign %s", creative_urn, campaign_urn)
        else:
            log.info("No creative image for cohort '%s' (index %d) — campaign created without creative", cohort.name, i)


# ── Monitor mode ───────────────────────────────────────────────────────────────

def run_monitor(dry_run: bool = False) -> None:
    sheets    = SheetsClient()
    sheet_cfg = sheets.read_config()

    li_token   = sheet_cfg.get("LINKEDIN_TOKEN") or os.getenv("LINKEDIN_TOKEN", "")
    if not li_token:
        log.error("LINKEDIN_TOKEN not found — aborting")
        sys.exit(1)

    li_client = LinkedInClient(li_token)
    snowflake = RedashClient()

    # Read all active campaigns from sheet
    active = read_active_campaigns(sheets)
    if not active:
        log.info("No active campaigns to monitor")
        return

    log.info("Monitoring %d active campaigns", len(active))

    # 1. Check learning phase
    campaign_ids = [c["campaign_id"] for c in active if c.get("campaign_id")]
    learning_done = check_learning_phase(li_client, campaign_ids)

    graduated = [c for c in active if learning_done.get(c["campaign_id"], False)]
    still_learning = [c for c in active if not learning_done.get(c["campaign_id"], False)]

    log.info("%d campaigns learning complete, %d still in learning phase",
             len(graduated), len(still_learning))

    if not graduated:
        log.info("No campaigns have exited learning phase yet — nothing to do")
        snowflake.close()
        return

    # 2. Get pass rates from Snowflake
    flow_ids   = list({c["flow_id"] for c in graduated if c.get("flow_id")})
    since_date = _earliest_launch_date(graduated)
    pass_rates = get_pass_rates_from_snowflake(snowflake, flow_ids, since_date)

    # 3. Score campaigns
    scored = score_campaigns(graduated, pass_rates)

    # 4. Pause underperformers
    to_pause = [c for c in scored if c["verdict"] == "PAUSE"]
    for c in to_pause:
        log.info("Pausing campaign %s (flow=%s pass_rate=%.2f%% < cohort_avg=%.2f%%)",
                 c["campaign_id"], c["flow_id"], c["pass_rate"], c["cohort_avg"])
        if not dry_run:
            try:
                pause_campaign(li_client, c["campaign_id"])
            except Exception as exc:
                log.error("Failed to pause campaign %s: %s", c["campaign_id"], exc)

    # 5. Discover new ICPs for flows with paused campaigns
    flows_with_pauses = list({c["flow_id"] for c in to_pause})
    config_name = sheet_cfg.get("SCREENING_CONFIG_NAME", "")

    for flow_id in flows_with_pauses:
        existing_rules = [
            _parse_rules(c.get("targeting_criteria_json", ""))
            for c in active if c["flow_id"] == flow_id
        ]
        new_icps = discover_new_icps(snowflake, flow_id, config_name or flow_id, existing_rules)
        if new_icps:
            log.info("Discovered %d new ICPs for flow=%s — queuing for next launch run", len(new_icps), flow_id)
            # Write as new PENDING rows in sheet for the next launch run
            if not dry_run:
                _queue_new_icps(sheets, flow_id, active, new_icps)

    # 6. Write monitor results
    if not dry_run:
        write_monitor_results(sheets, scored)
    else:
        log.info("[dry-run] Monitor results: %s",
                 [(c["campaign_id"], c["verdict"]) for c in scored])

    log.info("Monitor run complete — %d paused, %d kept", len(to_pause), len(scored) - len(to_pause))


def _earliest_launch_date(campaigns: list[dict]) -> str:
    """Return the earliest launch_date across campaigns, or 30 days ago."""
    from datetime import datetime, timedelta, timezone
    dates = []
    for c in campaigns:
        raw = c.get("launch_date", "")
        if raw:
            try:
                dates.append(raw[:10])  # YYYY-MM-DD
            except Exception:
                pass
    if dates:
        return min(dates)
    fallback = (datetime.now(timezone.utc) - timedelta(days=30)).strftime("%Y-%m-%d")
    return fallback


def _parse_rules(json_str: str) -> list[tuple]:
    """Parse targeting_criteria_json back to [(feature, value), ...]."""
    try:
        items = json.loads(json_str)
        return [(item["feature"], item["value"]) for item in items]
    except Exception:
        return []


def _queue_new_icps(sheets, flow_id: str, active: list[dict], new_icps: list) -> None:
    """Write new ICP cohorts as PENDING rows in Triggers 2 for next launch run."""
    # Find an existing row for this flow_id to copy A-G columns from
    ref_row = next((c for c in active if c["flow_id"] == flow_id), None)
    if not ref_row:
        return

    ws       = sheets._triggers.worksheet("Triggers 2")
    all_rows = ws.get_all_values()
    base_row = all_rows[ref_row["sheet_row"] - 1] if ref_row["sheet_row"] <= len(all_rows) else []
    while len(base_row) < 12:
        base_row.append("")

    from datetime import datetime, timezone
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    for cohort in new_icps:
        new_row = list(base_row[:7])  # copy A-G
        new_row[0] = today            # update date
        new_row[2] = "PENDING"        # C = PENDING
        # Clear H-L
        while len(new_row) < 12:
            new_row.append("")
        for j in range(7, 12):
            new_row[j] = ""
        ws.append_row(new_row, value_input_option="RAW")
        log.info("Queued new ICP '%s' as PENDING row for flow=%s", cohort.name, flow_id)


# ── Formatting helpers ─────────────────────────────────────────────────────────

def _cohort_display_name(cohort, flow_id: str, location: str) -> str:
    short = cohort.name[:40].replace("__", " ").replace("_", " ").title()
    parts = [p for p in [flow_id, location, short] if p]
    return " | ".join(parts)


def _cohort_headline(cohort) -> str:
    from src.linkedin_urn import _col_to_human
    if cohort.rules:
        return _col_to_human(cohort.rules[0][0]).title()
    return "Technical"


def _cohort_to_targeting_json(cohort) -> tuple[str, str]:
    from src.analysis import _feature_to_facet
    primary_facet = _feature_to_facet(cohort.rules[0][0]) if cohort.rules else "unknown"
    criteria = [
        {"feature": r[0], "value": r[1], "lift_pp": round(cohort.lift_pp, 2)}
        for r in cohort.rules
    ]
    return primary_facet, json.dumps(criteria)


# ── Entry point ────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Outlier Campaign Agent")
    parser.add_argument(
        "--mode", default="launch", choices=["launch", "monitor"],
        help="launch: full pipeline for PENDING rows | monitor: check active campaigns",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Run analysis/checks but do not write to sheet or LinkedIn",
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)

    if args.mode == "monitor":
        run_monitor(dry_run=args.dry_run)
    else:
        run_launch(dry_run=args.dry_run)


if __name__ == "__main__":
    main()

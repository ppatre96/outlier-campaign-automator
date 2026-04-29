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
import shutil
import sys
import tempfile
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus

from dotenv import load_dotenv

load_dotenv()

import config
from src.sheets import SheetsClient, make_stg_id
from src.redash_db import RedashClient
from src.smart_ramp_client import SmartRampClient
from src.linear_client import LinearClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import stage_a, stage_b   # stage_a is now a dispatcher (support vs lift)
from src.linkedin_urn import UrnResolver
from src.linkedin_api import LinkedInClient, ImageAdResult
from src.stage_c import stage_c
from src.figma_creative import (
    FigmaCreativeClient,
    build_copy_variants,
    apply_plugin_logic,
    classify_tg,
)
from src.gemini_creative import generate_imagen_creative, generate_imagen_creative_with_qc
from src.inmail_copy_writer import build_inmail_variants
from src.campaign_monitor import (
    check_learning_phase,
    get_pass_rates_from_snowflake,
    score_campaigns,
    pause_campaign,
    discover_new_icps,
    write_monitor_results,
    read_active_campaigns,
)
from src.brand_voice_validator import BrandVoiceValidator

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-7s %(name)s — %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("main")


# ── Launch mode ───────────────────────────────────────────────────────────────

def run_launch(dry_run: bool = False, flow_id: str | None = None, project_id: str | None = None, ramp_id: str | None = None, post_to_linear: bool = False) -> None:
    sheets    = SheetsClient()
    sheet_cfg = sheets.read_config()

    li_token      = (
        sheet_cfg.get("LINKEDIN_TOKEN") or
        os.getenv("LINKEDIN_ACCESS_TOKEN") or
        os.getenv("LINKEDIN_TOKEN") or
        config.LINKEDIN_TOKEN
    )
    claude_key    = os.getenv("ANTHROPIC_API_KEY") or os.getenv("CLAUDE_API_KEY", "")
    mj_token      = sheet_cfg.get("MIDJOURNEY_API_TOKEN") or os.getenv("MIDJOURNEY_API_TOKEN", "")
    inmail_sender = sheet_cfg.get("LINKEDIN_INMAIL_SENDER_URN") or os.getenv("LINKEDIN_INMAIL_SENDER_URN", config.LINKEDIN_INMAIL_SENDER_URN)

    # Initialize Linear client if posting to Linear
    linear_client = None
    if post_to_linear:
        try:
            linear_client = LinearClient()
        except ValueError as e:
            log.warning("Linear client init failed (--post-to-linear skipped): %s", e)

    if not li_token:
        log.error("LINKEDIN_TOKEN not found in Config tab or environment — aborting")
        sys.exit(1)

    li_client = LinkedInClient(li_token)
    urn_res   = UrnResolver(sheets)
    snowflake = RedashClient()

    # Initialize brand voice validator
    brand_voice_validator = BrandVoiceValidator()

    # --flow-id / --project-id: build a synthetic row, skip sheet read
    resolved_config = ""
    if project_id:
        log.info("Resolving project_id=%s to flow_id ...", project_id)
        result = snowflake.resolve_project_to_flow(project_id)
        if result:
            flow_id, resolved_config = result
            log.info("Resolved → flow_id=%s config=%s", flow_id, resolved_config)
        else:
            # Cold-start case: project has no public signup flow (e.g. internal projects).
            # Continue with flow_id="" — fetch_stage1_contributors works from project_id
            # alone, and if it too returns empty, _run_cold_start kicks in.
            log.warning(
                "No signup flow found for project_id=%s — continuing in cold-start mode "
                "(fetch_stage1_contributors will drive Stage 1 off project_id directly).",
                project_id,
            )
            flow_id = flow_id or ""
    # --ramp-id: fetch from Smart Ramp and process each cohort
    if ramp_id:
        try:
            ramp_client = SmartRampClient()
            ramp = ramp_client.fetch_ramp(ramp_id)
            if not ramp:
                log.error("Ramp %s not found", ramp_id)
                return

            project_id = ramp.project_id
            pending = []
            for cohort in ramp.cohorts:
                pending.append({
                    "flow_id": cohort.signup_flow_id or "",
                    "location": "",
                    "ad_type": "",
                    "figma_file": "",
                    "figma_node": "",
                    "config_name": ramp.project_name or "",
                    "ramp_id": ramp.id,
                    "cohort_id": cohort.id,
                    "cohort_description": cohort.cohort_description,
                    "selected_lp_url": cohort.selected_lp_url,
                    "included_geos": cohort.included_geos,
                    "matched_locales": cohort.matched_locales,
                    "target_activations": cohort.target_activations,
                    "linear_issue_id": ramp.linear_issue_id,
                })
            retry = []
            log.info("Smart Ramp %s → %d cohorts", ramp_id, len(pending))
        except Exception as exc:
            log.error("Failed to fetch ramp %s: %s", ramp_id, exc)
            return
    elif project_id or flow_id:
        pending = [{"flow_id": flow_id, "location": "", "ad_type": "", "figma_file": "", "figma_node": "", "config_name": resolved_config}]
        retry   = []
    else:
        pending = sheets.read_pending_rows()
        retry   = sheets.read_li_retry_rows()

    if not pending and not retry:
        log.info("No PENDING rows and no retry rows found — nothing to do")
        return

    log.info("Found %d PENDING rows, %d retry rows", len(pending), len(retry))

    for row in pending:
        flow_id    = row["flow_id"]
        location   = row.get("location", "")
        figma_file = row.get("figma_file", "").strip()
        figma_node = row.get("figma_node", "").strip()
        ad_type    = row.get("ad_type", "").strip().upper()

        log.info("=" * 60)
        if row.get("cohort_id"):
            log.info("Processing ramp=%s cohort=%s flow_id=%s", row.get("ramp_id"), row.get("cohort_id"), flow_id)
        else:
            log.info("Processing flow_id=%s location=%s ad_type=%s",
                     flow_id, location, ad_type or "SPONSORED_UPDATE")

        config_name = row.get("config_name") or sheet_cfg.get("SCREENING_CONFIG_NAME", "") or flow_id

        try:
            _process_row(
                row=row,
                flow_id=flow_id,
                config_name=config_name,
                project_id=project_id,   # scope tier CTEs (T3/T2) to the project
                location=location,
                figma_file=figma_file,
                figma_node=figma_node,
                ad_type=ad_type,
                inmail_sender=inmail_sender,
                sheets=sheets,
                snowflake=snowflake,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                mj_token=mj_token,
                dry_run=dry_run,
                linear_client=linear_client,
                post_to_linear=post_to_linear,
            )
        except RuntimeError as exc:
            log.error("HARD STOP for flow %s: %s", flow_id, exc)
            raise
        except Exception as exc:
            log.exception("Unexpected error for flow %s: %s", flow_id, exc)

    for row in retry:
        log.info("=" * 60)
        log.info("Retrying LI campaign for stg_id=%s name=%s", row["stg_id"], row["stg_name"])
        try:
            _retry_li_campaign(
                row=row,
                inmail_sender=inmail_sender,
                sheets=sheets,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                figma_file=row.get("figma_file", ""),
                figma_node=row.get("figma_node", ""),
                mj_token=mj_token,
                dry_run=dry_run,
            )
        except Exception as exc:
            log.exception("Retry failed for stg_id=%s: %s", row["stg_id"], exc)

    log.info("Launch run complete")


def _process_row(
    row, flow_id, config_name, location, figma_file, figma_node,
    ad_type, inmail_sender,
    sheets, snowflake, li_client, urn_res, claude_key, mj_token, dry_run,
    project_id: str | None = None,
    linear_client = None,
    post_to_linear: bool = False,
):
    # Extract Smart Ramp cohort overrides if present
    ramp_id = row.get("ramp_id")
    cohort_id = row.get("cohort_id")
    destination_url_override = row.get("selected_lp_url")
    included_geos = row.get("included_geos", [])
    cohort_description_supplement = row.get("cohort_description", "")
    matched_locales = row.get("matched_locales")
    ramp_linear_issue_id = row.get("linear_issue_id")

    # 1. Stage 1 data pull.
    # Primary path: project-scoped STAGE1_SQL via CESF + CBPR (works regardless of
    # signup flow / screening config — captures every activator on the project).
    # Fallback path: legacy flow-scoped RESUME_SQL when no project_id is available.
    if project_id:
        df_raw = snowflake.fetch_stage1_contributors(project_id)
        if df_raw.empty:
            log.info(
                "No activators found for project=%s — branching to cold_start mode",
                project_id,
            )
            _run_cold_start(
                row=row,
                project_id=project_id,
                flow_id=flow_id,
                snowflake=snowflake,
                reason="no_activators",
            )
            return
    else:
        df_raw = snowflake.fetch_screenings(
            flow_id, config_name,
            project_id=project_id,
            end_date=date.today().isoformat(),
        )
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

    # 2b. Tiered ICP target selection
    # Picks T3 (activation) > T2 (course-pass) > T1 (resume-pass) based on positive
    # count vs MIN_POSITIVES_FOR_STATS (=30). If no tier has ≥30 positives we still
    # get the strongest available tier back — Stage A/B just fall back to legacy
    # resume-pass behaviour via target_col=None.
    from src.analysis import (
        pick_target_tier,
        small_sample_signals,
        stage_a_negative,
        MIN_POSITIVES_FOR_STATS,
        MIN_POSITIVES_FOR_SIGNALS,
    )
    from src.icp_exemplars import build_exemplars
    from src.linkedin_urn import feature_col_to_exclude_pair

    # Resolve project/job-post metadata once — used for base-role anchoring in
    # stats mode AND for the cold_start / sparse-mode summaries.
    from src.icp_from_jobpost import (
        extract_base_role_candidates,
        base_role_feature_columns,
        required_skill_feature_columns,
        family_exclusions_for,
        derive_icp_from_job_post,
    )
    job_post_meta  = {}
    project_meta   = {}
    worker_skills: list[str] = []
    try:
        if flow_id:
            job_post_meta = snowflake.fetch_job_post_meta(flow_id) or {}
        if project_id:
            project_meta = snowflake.fetch_project_meta(project_id) or {}
            worker_skills = snowflake.fetch_project_worker_skills(project_id) or []
            if worker_skills:
                log.info("Project worker skills (eligibility gates): %s", worker_skills)
    except Exception as exc:
        log.warning("Meta fetch failed (non-fatal): %s", exc)

    # Parse the job-post description with the LLM so we get a richer base-role
    # signal than just keyword-matching `job_name` / `domain`. This used to run
    # only in cold_start / sparse paths; with it active in stats mode too, the
    # `derived_tg_label` feeds the family matcher and `required_skills` become
    # direct base-role anchors for Stage A's combo synthesis.
    derived_icp: dict = {}
    description = (job_post_meta.get("description") or project_meta.get("description") or "").strip()
    if description:
        try:
            derived_icp = derive_icp_from_job_post(description) or {}
        except Exception as exc:
            log.warning("derive_icp_from_job_post failed (non-fatal): %s", exc)

    # Base-role family matching — intentionally does NOT take worker_skills
    # as an input. WS are capability-bucket labels ("Coding", "Biology") that
    # could wrongly trigger biologist/chemist families on projects where CBs
    # aren't actually biologists (e.g. OpenClaw's WS include Biology T3 as a
    # domain-expertise gate, but the CBs are software engineers). Family
    # detection stays tied to the human-facing project text + LLM derived
    # label; WS flows to the summary as a suggestion only.
    base_role_titles = extract_base_role_candidates(
        job_post_meta=job_post_meta,
        project_meta=project_meta,
        signup_flow_name=config_name or job_post_meta.get("job_name"),
        derived_tg_label=derived_icp.get("derived_tg_label"),
    )

    family_exclude_pairs = family_exclusions_for(
        job_post_meta=job_post_meta,
        project_meta=project_meta,
        signup_flow_name=config_name or job_post_meta.get("job_name"),
        derived_tg_label=derived_icp.get("derived_tg_label"),
    )

    target_tier, target_col, n_icp = pick_target_tier(df_bin)
    log.info("Stage 1 target: %s (col=%s, %d ICPs)", target_tier, target_col, n_icp)

    # n_icp == 0 → cold_start: no cohort to analyze, derive ICP from job post instead.
    if n_icp == 0:
        log.info("Zero positives across all tiers — branching to cold_start mode")
        _run_cold_start(
            row=row,
            project_id=project_id,
            flow_id=flow_id,
            snowflake=snowflake,
            reason="zero_positives",
        )
        return

    # Build exemplars in every mode — they're a summary-step artifact (used to
    # show BPO partners like Joveo sample ICP profiles), NOT an analysis tier.
    icp_exemplars = build_exemplars(df_bin, target_col, target_tier, max_count=5)

    # Analysis mode: stats (real cohort mining) vs sparse (too few ICPs).
    # cold_start (n_icp == 0) was already handled above and returned early.
    strong_signals: list[dict] = []
    if n_icp >= MIN_POSITIVES_FOR_STATS:
        analysis_mode = "stats"
    else:
        analysis_mode = "sparse"
        # Still try small_sample_signals — with n_pos < MIN_POSITIVES_FOR_SIGNALS
        # it will typically return empty, but if any feature is ≥50% of ICPs AND
        # ≤10% of non-ICPs it's still a strong signal worth surfacing.
        strong_signals = small_sample_signals(df_bin, bin_cols, target_col)
        if n_icp < MIN_POSITIVES_FOR_SIGNALS:
            log.warning(
                "Only %d ICPs (< %d signals floor) — Stage A skipped. Slack "
                "summary will rely on %d exemplar profile(s) + best-effort "
                "job-post ICP.",
                n_icp, MIN_POSITIVES_FOR_SIGNALS, len(icp_exemplars),
            )

    # Stash these on the row so Stage 11 Slack summary can surface them
    row.setdefault("_stage1_meta", {}).update({
        "target_tier": target_tier,
        "target_col": target_col,
        "icp_count": n_icp,
        "analysis_mode": analysis_mode,
        "icp_exemplars": icp_exemplars,
        "strong_signals": strong_signals,
    })

    # Data-driven exclusions: features over-represented in NON-activators.
    # Only runs in stats mode (needs ≥100 non-activators to be trustworthy).
    data_driven_exclude_pairs: list[tuple[str, str]] = []
    if analysis_mode == "stats":
        neg_hits = stage_a_negative(df_bin, bin_cols, target_col)
        for h in neg_hits:
            pair = feature_col_to_exclude_pair(h["feature"])
            if pair:
                data_driven_exclude_pairs.append(pair)

    # 3. Stage A — tier target + base-role anchor (when we know the project's base role)
    # Two HARD anchor sources:
    #   (a) family-matched job titles (Data_Analyst, Software_Developer, …)
    #   (b) LLM-derived required skills from the job post (Python, Java, …)
    #
    # Worker-skill gates (WS) from PROJECT_QUALIFICATIONS_LONG are intentionally
    # NOT hard anchors. They're capability-bucket labels like "Coding" / "Biology"
    # that gate platform eligibility internally, but contributors don't
    # necessarily list them verbatim on LinkedIn (an OpenClaw contributor
    # passing the Biology T3 gate is likely a software engineer, not someone
    # who'd tag "Biology" on their LinkedIn profile). So WS flows to the
    # summary as a suggestion for the copy-writer but doesn't narrow the
    # LinkedIn audience.
    title_anchor_cols = base_role_feature_columns(base_role_titles, list(df_bin.columns))
    skill_anchor_cols = required_skill_feature_columns(
        derived_icp.get("required_skills", []), list(df_bin.columns),
    )
    base_role_cols = title_anchor_cols + [c for c in skill_anchor_cols if c not in title_anchor_cols]
    if base_role_cols:
        log.info(
            "Base-role anchor active: %d candidate(s) — titles=%s skills=%s",
            len(base_role_cols), title_anchor_cols, skill_anchor_cols,
        )
    cohorts_a = (
        stage_a(df_bin, bin_cols, target_col=target_col, base_role_cols=base_role_cols)
        if analysis_mode == "stats" else []
    )
    if not cohorts_a and analysis_mode == "stats":
        log.warning("Stage A found no valid cohorts for flow=%s tier=%s", flow_id, target_tier)
        return
    if analysis_mode != "stats":
        log.info("Skipping Stage A/B (analysis_mode=%s). Strong signals=%d, exemplars=%d.",
                 analysis_mode, len(strong_signals), len(icp_exemplars))
        # Sparse mode persists a run summary so Slack gets a record of what was
        # found — exemplars + any strong signals + best-effort job-post ICP.
        # (cold_start is handled separately earlier and returns its own summary.)
        _persist_sparse_summary(
            row=row,
            project_id=project_id,
            flow_id=flow_id,
            snowflake=snowflake,
            target_tier=target_tier,
            target_col=target_col,
            n_icp=n_icp,
            analysis_mode=analysis_mode,
            icp_exemplars=icp_exemplars,
            strong_signals=strong_signals,
            rows_screened=len(df_bin),
        )
        return

    # Stage B — per-country lift validation. Meaningful only when Stage A ran
    # in lift mode (flow-scoped frame with real baseline). In support mode the
    # lift column is repurposed as coverage% and country validation doesn't
    # apply, so we pass cohorts through unchanged. Heuristic: if any selected
    # cohort has non-zero `support`, we came from support mining.
    from_support_mode = any(getattr(c, "support", 0) > 0 for c in cohorts_a)
    if from_support_mode:
        cohorts_b = cohorts_a
    else:
        cohorts_b = stage_b(df_bin, cohorts_a, target_col=target_col)

    # 5+6. URN resolution + Stage C
    try:
        selected = stage_c(cohorts_b, urn_res, li_client)
    except Exception as exc:
        log.warning("Stage C unavailable (%s) — falling back to Stage B top cohorts", exc)
        selected = []

    if not selected:
        log.warning("Stage C returned no cohorts for flow=%s — falling back to Stage B top cohorts", flow_id)
        selected = cohorts_b[:config.MAX_CAMPAIGNS]

    if not selected:
        log.warning("No cohorts survived Stage A/B for flow=%s — skipping", flow_id)
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

    if not dry_run and row.get("sheet_row"):
        sheets.write_cohorts(row, cohort_sheet_rows)
        log.info("Wrote %d cohorts to sheet row %d", len(cohort_sheet_rows), row["sheet_row"])
    else:
        log.info("[dry-run/direct] Would write %d cohorts to sheet", len(cohort_sheet_rows))

    # 8. Branch: InMail vs. Image Ad
    is_inmail = (ad_type == "INMAIL")

    if is_inmail:
        _process_inmail_campaigns(
            selected=selected,
            flow_id=flow_id,
            location=location,
            sheets=sheets,
            li_client=li_client,
            urn_res=urn_res,
            claude_key=claude_key,
            inmail_sender=inmail_sender,
            brand_voice_validator=brand_voice_validator,
            dry_run=dry_run,
            family_exclude_pairs=family_exclude_pairs,
            data_driven_exclude_pairs=data_driven_exclude_pairs,
            destination_url_override=destination_url_override,
            included_geos=included_geos,
        )
        return

    # 8. Generate creatives (Figma clone + Midjourney from-scratch, with fallback)
    has_figma = bool(figma_file and figma_node and claude_key)
    figma_client = FigmaCreativeClient() if has_figma else None

    # One PNG per cohort — rotate variant angle A→B→C across campaigns
    creative_paths: list[Path | None] = []
    all_variants_per_cohort: list[list[dict]] = []

    for i, cohort in enumerate(selected):
        angle_idx    = i % 3
        angle_label  = ["A", "B", "C"][angle_idx]
        variants: list[dict] = []
        png_path: Path | None = None

        # ── Step 8a: generate copy variants — fully derived from cohort signals ──
        try:
            layer_map = (
                figma_client.get_text_layer_map(figma_file, figma_node)
                if has_figma else {}
            )
            # geos drives photo_subject ethnicity choice — see _GEO_ETHNICITY_HINTS
            # in src/figma_creative.py. v1 _process_row doesn't have included_geos
            # in scope, so falls back to None (LLM picks global mix). The Phase 2.6
            # path (_process_static_campaigns) DOES pass it through — see line ~1910.
            variants = build_copy_variants(cohort, layer_map, geos=None)
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
                tg_label = variants[0].get("tg_label", cohort.name) if variants else cohort.name
                clone_ids = apply_plugin_logic(
                    figma_file, figma_node, variants, tg_label, claude_key
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

        # ── Step 8c: Gemini generation + auto QC retry loop ──
        if png_path is None and selected_variant:
            try:
                from src.figma_creative import rewrite_variant_copy
                png_path, qc_report = generate_imagen_creative_with_qc(
                    variant=selected_variant,
                    max_retries=2,
                    copy_rewriter=rewrite_variant_copy,
                )
                log.info(
                    "Creative: cohort %d '%s' → angle %s → %s (QC: %s)",
                    i, cohort.name, angle_label, png_path, qc_report.get("verdict", "UNKNOWN"),
                )
                # Stash per-cohort QC report for the end-of-run Slack summary
                if not hasattr(cohort, "_qc_reports"):
                    cohort._qc_reports = {}
                cohort._qc_reports[angle_label] = {
                    "verdict": qc_report.get("verdict", "UNKNOWN"),
                    "attempts": qc_report.get("attempts", 1),
                    "violations": qc_report.get("violations", []),
                    "retry_target": qc_report.get("retry_target", "none"),
                }
                if qc_report.get("verdict") == "FAIL":
                    # Hard-reject. Previously logged a warning and shipped anyway, which
                    # produced creatives with duplicate logos / em dashes / banned tokens
                    # in production (GMR-0005, 2026-04-28). Better to ship a campaign with
                    # no creative attached and a loud Slack note than to ship a bad one.
                    log.error(
                        "Cohort '%s' angle %s — QC FAIL after %d attempts; REJECTING creative. "
                        "Campaign will be created without an image. Violations: %s",
                        cohort.name, angle_label,
                        qc_report.get("attempts", 1),
                        qc_report.get("violations", []),
                    )
                    png_path = None  # downstream `if png_path and png_path.exists()` skips upload
            except Exception as exc:
                log.warning("Gemini creative failed for '%s': %s", cohort.name, exc)

        creative_paths.append(png_path)

    # 9+10. LinkedIn campaigns + creative upload
    if dry_run:
        log.info("[dry-run] Skipping LinkedIn campaign creation")
        return

    group_name = f"Outlier {flow_id} {location}".strip()
    group_urn  = li_client.create_campaign_group(group_name)

    # Resolve negation facets once per group. Four sources compose here:
    #   1. config.DEFAULT_EXCLUDE_FACETS         — generic (recruiters, sales, BDRs)
    #   2. family_exclude_pairs                  — role-adjacent traps per base-role family
    #   3. data_driven_exclude_pairs             — stage_a_negative hits for THIS frame
    #   4. cohort.exclude_add / exclude_remove   — per-cohort overrides
    # Steps 1-3 are shared across every campaign in the group; step 4 varies
    # per cohort and is applied inside the loop.
    default_exclude_urns = urn_res.resolve_default_excludes()
    family_exclude_urns  = urn_res.resolve_facet_pairs(family_exclude_pairs)
    data_driven_exclude_urns = urn_res.resolve_facet_pairs(data_driven_exclude_pairs)
    shared_exclude_urns  = _merge_urn_dicts(
        default_exclude_urns, family_exclude_urns, data_driven_exclude_urns,
    )
    log.info(
        "Shared exclusion set for group: %d facet(s), %d total URN(s) "
        "(defaults=%d, family=%d, data-driven=%d)",
        len(shared_exclude_urns),
        sum(len(v) for v in shared_exclude_urns.values()),
        sum(len(v) for v in default_exclude_urns.values()),
        sum(len(v) for v in family_exclude_urns.values()),
        sum(len(v) for v in data_driven_exclude_urns.values()),
    )

    # Track run-level state for the Slack end-step summary
    run_summary_contexts: list[dict] = []
    run_blockers: set[str] = set()

    for i, cohort in enumerate(selected):
        facet_urns   = urn_res.resolve_cohort_rules(cohort.rules)
        # Apply Smart Ramp geo overrides if present
        if included_geos:
            facet_urns = _apply_geo_overrides(facet_urns, included_geos, urn_res)

        # Layer per-cohort overrides on top of the shared set.
        cohort_add_urns    = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_add", []) or [])
        cohort_remove_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_remove", []) or [])
        cohort_exclude_urns = _subtract_urn_dicts(
            _merge_urn_dicts(shared_exclude_urns, cohort_add_urns),
            cohort_remove_urns,
        )

        campaign_urn = li_client.create_campaign(
            name=cohort._stg_name,
            campaign_group_urn=group_urn,
            facet_urns=facet_urns,
            exclude_facet_urns=cohort_exclude_urns,
        )
        campaign_id = campaign_urn.rsplit(":", 1)[-1]
        sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
        log.info("Created campaign %s", campaign_urn)

        cohort_creative_urns: dict[str, str | None] = {"A": None, "B": None, "C": None}

        png_path = creative_paths[i]
        if png_path and png_path.exists():
            # Use headline/subheadline from the selected variant angle
            variants = all_variants_per_cohort[i] if i < len(all_variants_per_cohort) else []
            angle_idx = i % 3
            variant   = variants[angle_idx] if angle_idx < len(variants) else {}
            angle_label = variant.get("angle", ["A","B","C"][angle_idx])
            headline  = variant.get("headline") or f"Your {_cohort_headline(cohort)} expertise is in demand."
            subhead   = variant.get("subheadline") or "Earn payment doing remote AI tasks on your schedule."

            # ── Drive upload (only when GDRIVE_ENABLED=true in .env) ───────────
            drive_url = None
            if config.GDRIVE_ENABLED:
                try:
                    from src.gdrive import upload_creative
                    drive_url = upload_creative(png_path)
                    log.info("Drive upload: %s → %s", png_path.name, drive_url)
                except Exception as exc:
                    log.warning("Drive upload failed for '%s': %s", cohort.name, exc)
            else:
                run_blockers.add("Google Drive upload: GDRIVE_ENABLED=false — creatives saved locally only.")

            # ── LinkedIn image attach (best-effort) ────────────────────────────
            # create_image_ad now returns an ImageAdResult sentinel (Phase 2.6 SR-04):
            # 403 / LINKEDIN_MEMBER_URN translate to status="local_fallback" so we can
            # save the PNG locally and continue rather than aborting. Belt-and-suspenders
            # try/except remains for unexpected exceptions outside the sentinel contract.
            try:
                image_urn = li_client.upload_image(png_path)
                ad_result = li_client.create_image_ad(
                    campaign_urn=campaign_urn,
                    image_urn=image_urn,
                    headline=headline,
                    description=subhead,
                    intro_text=variant.get("intro_text", "") if variant else "",
                    ad_headline=variant.get("ad_headline", "") if variant else "",
                    ad_description=variant.get("ad_description", "") if variant else "",
                    cta_button=variant.get("cta_button", "APPLY") if variant else "APPLY",
                    destination_url=destination_url_override,
                )
                if ad_result.status == "ok":
                    creative_urn = ad_result.creative_urn
                    sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
                    log.info("Attached creative %s to campaign %s", creative_urn, campaign_urn)
                    cohort_creative_urns[angle_label] = creative_urn
                elif ad_result.status == "local_fallback":
                    # SR-04: LinkedIn upload blocked → save PNG locally + continue.
                    # _save_creative_locally is wired via _process_static_campaigns
                    # in the Smart Ramp dual-arm flow (Task 2). For the legacy
                    # single-arm CLI path here, we record the blocker only — the
                    # PNG already lives on disk at png_path; manual upload uses it.
                    if "LINKEDIN_MEMBER_URN" in (ad_result.error_message or ""):
                        log.warning(
                            "Image ad creative skipped for '%s' — LINKEDIN_MEMBER_URN not set.",
                            cohort.name,
                        )
                        run_blockers.add("Creative attachment: LINKEDIN_MEMBER_URN not set.")
                    else:
                        log.warning(
                            "LinkedIn creative attach blocked for '%s' (DSC 403 / MDP gate): %s",
                            cohort.name, ad_result.error_message,
                        )
                        run_blockers.add(
                            "Creative attachment (MDP): DSC post creation returns 403 — "
                            "LinkedIn app needs Marketing Developer Platform approval."
                        )
                else:  # status == "error"
                    log.warning(
                        "LinkedIn creative attach failed for '%s': %s — %s",
                        cohort.name, ad_result.error_class, ad_result.error_message,
                    )
                    run_blockers.add(
                        f"Creative attachment: {(ad_result.error_message or '')[:180]}"
                    )
            except RuntimeError as exc:
                # Belt-and-suspenders: upload_image still raises RuntimeError on
                # auth/scope failures. create_image_ad's sentinel covers the
                # LINKEDIN_MEMBER_URN / 403 cases above.
                log.warning("LinkedIn creative attach failed for '%s': %s", cohort.name, exc)
                run_blockers.add(f"Creative attachment: {str(exc)[:180]}")
            except Exception as exc:
                log.warning("LinkedIn creative attach failed for '%s': %s", cohort.name, exc)
                run_blockers.add(f"Creative attachment: {str(exc)[:180]}")
        else:
            log.info("No creative image for cohort '%s' (index %d) — campaign created without creative", cohort.name, i)

        # Capture per-cohort context for the Slack summary
        variants_used = all_variants_per_cohort[i] if i < len(all_variants_per_cohort) else []
        creative_paths_map: dict[str, str] = {}
        if png_path and png_path.exists():
            # Map the flat path to the angle we shipped this run
            creative_paths_map[variant.get("angle", "A") if variant else "A"] = str(png_path)

        stage1_meta = row.get("_stage1_meta", {})
        run_summary_contexts.append({
            "project_id": project_id or "—",
            "flow_id": flow_id,
            "config_name": sheet_cfg.get("CONFIG_NAME") if isinstance(sheet_cfg, dict) else None,
            "cohort_name": cohort.name,
            "tg_label": getattr(cohort, "_tg_label", None),
            "pass_rate": getattr(cohort, "pass_rate", None),
            "lift_pp": getattr(cohort, "lift_pp", None),
            "rows_screened": getattr(cohort, "total_rows", None),
            "rows_passed": getattr(cohort, "pass_count", None),
            "stage_used": getattr(cohort, "stage", "A"),
            # Stage 1 tier info (from pick_target_tier + build_exemplars)
            "target_tier": stage1_meta.get("target_tier"),
            "target_col": stage1_meta.get("target_col"),
            "icp_count": stage1_meta.get("icp_count"),
            "analysis_mode": stage1_meta.get("analysis_mode"),
            "icp_exemplars": stage1_meta.get("icp_exemplars", []),
            "strong_signals": stage1_meta.get("strong_signals", []),
            "variants": variants_used,
            "creative_paths": creative_paths_map,
            "qc_reports": getattr(cohort, "_qc_reports", {}),
            "campaign_group_urn": group_urn,
            "campaign_group_name": f"agent_{group_name}",
            "campaign_urn": campaign_urn,
            "campaign_name": f"agent_{cohort._stg_name}",
            "creative_urns": cohort_creative_urns,
        })

        # ── Linear issue comment (per-cohort summary) ──────────────────────
        if post_to_linear and linear_client and ramp_linear_issue_id:
            try:
                # Extract top 3 rules from cohort.rules (feature, value pairs)
                top_rules = []
                if hasattr(cohort, "rules") and cohort.rules:
                    for rule in cohort.rules[:3]:
                        feature = rule[0] if isinstance(rule, (list, tuple)) else str(rule)
                        value = rule[1] if isinstance(rule, (list, tuple)) and len(rule) > 1 else ""
                        top_rules.append(f"{feature}: {value}" if value else str(feature))
                rules_text = "\n".join(f"- {r}" for r in top_rules) if top_rules else "- (no rules)"

                # Extract ICP doc path if available
                icp_doc_path = getattr(cohort, "_icp_doc_path", None) or "—"

                # Format comment body with cohort summary
                comment_body = f"""**Cohort {i+1}: {cohort.name}**
- Project: {project_id or "—"}
- Tier: {stage1_meta.get("target_tier", "—")} ({stage1_meta.get("icp_count", 0)} ICPs)
- Top targeting rules:
{rules_text}
- Campaign URN: `{campaign_urn}`
- ICP doc: {icp_doc_path}"""

                linear_client.post_comment(ramp_linear_issue_id, comment_body)
                log.info("Posted cohort %d summary to Linear issue %s", i+1, ramp_linear_issue_id)
            except Exception as exc:
                log.warning("Linear comment post failed for cohort %d '%s': %s", i+1, cohort.name, exc)

    # ── Stage 11: Persist end-of-run summary for campaign-manager agent ─────
    # We don't post to Slack directly here — the campaign-manager agent owns
    # that step and posts via the Slack MCP plugin after reading the persisted
    # summary. This separation keeps the Python pipeline headless-runnable and
    # avoids dependency on a Slack bot token that can expire.
    try:
        from datetime import datetime
        from src.campaign_summary_slack import persist_run_summary
        run_ts = datetime.now().strftime("%Y-%m-%d %H:%M")
        for ctx in run_summary_contexts:
            ctx["run_started_at"] = run_ts
            ctx["blockers"] = sorted(run_blockers)
            ctx["next_steps"] = _derive_next_steps(ctx, run_blockers)
            persist_run_summary(ctx)
        log.info("Run summary persisted. Campaign-manager agent should now post it to "
                 "Slack via mcp__plugin_slack_slack__slack_send_message.")
    except Exception as exc:
        log.warning("End-of-run summary persist failed (non-fatal): %s", exc)


def _persist_sparse_summary(
    row: dict,
    project_id: str | None,
    flow_id: str | None,
    snowflake,
    target_tier: str,
    target_col: str,
    n_icp: int,
    analysis_mode: str,
    icp_exemplars: list,
    strong_signals: list,
    rows_screened: int,
) -> None:
    """
    Persist a run summary for the sparse mode (fewer than MIN_POSITIVES_FOR_STATS
    ICPs at the chosen tier).

    Sparse mode skips Stage A/B because there are too few ICPs to mine, but we
    still want Slack to see the exemplars we found (they're the BPO-partner-
    facing summary artifact), any strong univariate signals, and — when
    available — a job-post-derived ICP to augment the scarce cohort signal for
    the copy-writer. If no job post exists (e.g. internal projects), the summary
    records that as a blocker so the user knows there's nothing more to pull.
    """
    from datetime import datetime as _dt

    from src.icp_from_jobpost import derive_icp_from_job_post, resolve_job_post
    from src.campaign_summary_slack import persist_run_summary

    raw_post = resolve_job_post(
        snowflake,
        project_id=project_id or "",
        signup_flow_id=flow_id,
        override_text=row.get("job_post_override") if isinstance(row, dict) else None,
    )
    job_post_icp = derive_icp_from_job_post(raw_post) if raw_post else {}

    worker_skills: list[str] = []
    try:
        if project_id:
            worker_skills = snowflake.fetch_project_worker_skills(project_id) or []
    except Exception as exc:
        log.warning("Sparse: fetch_project_worker_skills failed: %s", exc)

    blockers: list[str] = []
    next_steps: list[str] = [
        f"Review the {len(icp_exemplars)} exemplar profile(s) and decide whether to "
        "build a manual cohort before launching.",
    ]
    if not raw_post and not worker_skills:
        blockers.append(
            f"{analysis_mode}: only {n_icp} ICP(s), no job post, no worker skills — no "
            "statistical cohort and no supplementary signal. Consider deferring "
            "campaign launch until more activators accumulate."
        )
    elif not job_post_icp.get("derived_tg_label") and not worker_skills:
        blockers.append(
            f"{analysis_mode}: job post was too sparse for the LLM to derive a usable ICP "
            "and no worker skills are defined."
        )
    else:
        if worker_skills:
            next_steps.append(
                f"Use the worker-skill gates ({', '.join(worker_skills)}) + "
                "job-post-derived ICP as the primary TG spec for the copy-writer."
            )
        else:
            next_steps.append(
                "Use the job-post-derived ICP below as the primary TG spec for the copy-writer."
            )

    context: dict = {
        "project_id": project_id or "—",
        "flow_id": flow_id or "—",
        "run_started_at": _dt.now().strftime("%Y-%m-%d %H:%M"),
        "analysis_mode": analysis_mode,
        "target_tier": target_tier,
        "target_col": target_col,
        "icp_count": n_icp,
        "rows_screened": rows_screened,
        "icp_exemplars": icp_exemplars,
        "strong_signals": strong_signals,
        "variants": [],
        "creative_paths": {},
        "qc_reports": {},
        "creative_urns": {"A": None, "B": None, "C": None},
        "job_post_icp": job_post_icp,
        "worker_skills": worker_skills,
        "cohort_name": job_post_icp.get("derived_tg_label") or f"[{analysis_mode}] {project_id or flow_id or 'unknown'}",
        "tg_label": job_post_icp.get("derived_tg_label") or "",
        "blockers": blockers,
        "next_steps": next_steps,
    }
    try:
        persist_run_summary(context)
        log.info(
            "Sparse-mode summary persisted (mode=%s, n_icp=%d, has_icp=%s).",
            analysis_mode, n_icp, bool(job_post_icp.get("derived_tg_label")),
        )
    except Exception as exc:
        log.warning("Sparse-mode summary persist failed: %s", exc)


def _run_cold_start(
    row: dict,
    project_id: str | None,
    flow_id: str | None,
    snowflake,
    reason: str,
) -> None:
    """
    Cold-start branch — called when a project has zero activators/positives at every
    tier, so there is no statistical cohort and no exemplars to analyse.

    Strategy: pull the project's public job post (via PUBLIC.JOBPOSTS keyed on
    signup_flow_id) and let LiteLLM extract a structured ICP spec. That spec is
    persisted into the run summary as the primary signal so the campaign-manager
    agent + copy-writer have something to target.

    If NO job post is available either (e.g. Valkyrie Internal — never had a public
    signup flow), we persist an explicit "no data" summary instead of fabricating copy.
    """
    from datetime import datetime as _dt

    from src.icp_from_jobpost import derive_icp_from_job_post, resolve_job_post
    from src.campaign_summary_slack import persist_run_summary

    log.info(
        "Cold-start: project=%s flow=%s reason=%s", project_id or "—", flow_id or "—", reason,
    )

    raw_post = resolve_job_post(
        snowflake,
        project_id=project_id or "",
        signup_flow_id=flow_id,
        override_text=row.get("job_post_override") if isinstance(row, dict) else None,
    )
    job_post_icp = derive_icp_from_job_post(raw_post) if raw_post else {}

    # Pull worker-skill eligibility gates too — they're independent of the job
    # post and often present for projects that have no public signup flow. WS
    # + job post together give the copy-writer a targeting spec even when the
    # contributor frame is empty.
    worker_skills: list[str] = []
    try:
        if project_id:
            worker_skills = snowflake.fetch_project_worker_skills(project_id) or []
    except Exception as exc:
        log.warning("Cold-start: fetch_project_worker_skills failed: %s", exc)

    has_icp = bool(job_post_icp.get("derived_tg_label"))
    blockers: list[str] = []
    if not raw_post and not worker_skills:
        blockers.append(
            "Cold-start: no job post AND no worker-skill gates found for this "
            "project. Supply a job description via `--job-post-file` or create a "
            "signup flow before retrying."
        )
    elif not has_icp and not worker_skills:
        blockers.append(
            "Cold-start: job post was too sparse for the LLM to derive a targetable ICP "
            "and no worker skills are defined. Review data/last_run_summary.json."
        )

    next_steps: list[str] = []
    if has_icp:
        next_steps.append(
            "Review derived ICP (job_post_icp.derived_tg_label); if accurate, run the "
            "copy-writer with this spec as the TG input and create DRAFT campaigns manually."
        )
    else:
        next_steps.append(
            "Collect a richer job description or project brief for this project, then re-run."
        )

    context: dict = {
        "project_id": project_id or "—",
        "flow_id": flow_id or "—",
        "run_started_at": _dt.now().strftime("%Y-%m-%d %H:%M"),
        "analysis_mode": "cold_start",
        "cold_start_reason": reason,
        "target_tier": "—",
        "target_col": "—",
        "icp_count": 0,
        "icp_exemplars": [],
        "strong_signals": [],
        "variants": [],
        "creative_paths": {},
        "qc_reports": {},
        "creative_urns": {"A": None, "B": None, "C": None},
        "job_post_icp": job_post_icp,
        "worker_skills": worker_skills,
        "cohort_name": job_post_icp.get("derived_tg_label") or f"[cold-start] {project_id or flow_id or 'unknown'}",
        "tg_label": job_post_icp.get("derived_tg_label") or "",
        "blockers": blockers,
        "next_steps": next_steps,
    }
    try:
        persist_run_summary(context)
        log.info(
            "Cold-start run summary persisted (has_icp=%s, blockers=%d).",
            has_icp, len(blockers),
        )
    except Exception as exc:
        log.warning("Cold-start summary persist failed: %s", exc)


def _derive_next_steps(ctx: dict, blockers: set[str]) -> list[str]:
    """Derive actionable next steps from the run context + blockers."""
    steps: list[str] = []
    if any("MDP" in b or "403" in b for b in blockers):
        creative_count = sum(1 for v in (ctx.get("creative_paths") or {}).values() if v)
        if creative_count:
            steps.append(
                f"Manually upload {creative_count} QC-approved PNG(s) via Campaign Manager UI"
            )
    if ctx.get("campaign_urn"):
        camp_id = ctx["campaign_urn"].rsplit(":", 1)[-1]
        steps.append(
            f"Review DRAFT campaign in Campaign Manager → "
            f"https://www.linkedin.com/campaignmanager/accounts/{config.LINKEDIN_AD_ACCOUNT_ID}/campaigns/{camp_id}"
        )
    if any("GDRIVE" in b or "Drive" in b for b in blockers):
        steps.append("Enable GDRIVE_ENABLED=true + add service account to Shared Drive to archive creatives.")
    return steps


# ── InMail campaign sub-pipeline ──────────────────────────────────────────────

def _process_inmail_campaigns(
    selected, flow_id, location,
    sheets, li_client, urn_res,
    claude_key, inmail_sender, brand_voice_validator, dry_run,
    family_exclude_pairs: list[tuple[str, str]] | None = None,
    data_driven_exclude_pairs: list[tuple[str, str]] | None = None,
    destination_url_override: str | None = None,
    included_geos: list[str] | None = None,
) -> None:
    """
    InMail (Message Ad) path — no creative generation.
    For each cohort: generate InMail copy → create InMail campaign → create InMail creative.
    Angle rotates A→B→C across cohorts (same as image ad path).
    """
    if not inmail_sender:
        log.error(
            "LINKEDIN_INMAIL_SENDER_URN is not set — required for InMail ads. "
            "Add it to the Config tab or .env and retry."
        )
        return

    group_name = f"Outlier {flow_id} {location} InMail".strip()

    if dry_run:
        for i, cohort in enumerate(selected):
            angle_label = ["A", "B", "C"][i % 3]
            tg_cat = classify_tg(cohort.name, cohort.rules)
            log.info("[dry-run] InMail cohort %d '%s' tg=%s angle=%s", i, cohort.name, tg_cat, angle_label)
            variants = build_inmail_variants(tg_cat, cohort, claude_key)
            v = variants[i % 3]
            log.info("[dry-run] Subject: %s", v.subject)
            log.info("[dry-run] Body (first 100): %s…", v.body[:100])
            log.info("[dry-run] CTA: %s", v.cta_label)

            # Validate InMail copy against brand voice
            full_copy = f"{v.subject}\n\n{v.body}"
            report = brand_voice_validator.validate_copy(full_copy)

            if not report.is_compliant:
                log.warning(f"InMail angle {angle_label}: {len(report.violations)} brand voice violations")
                log.warning(f"  Must fix: {len(report.must_violations)}")
                log.warning(f"  Should fix: {len(report.should_violations)}")

                if report.must_violations:
                    log.error(f"InMail angle {angle_label} has MUST-FIX violations")
                    for v_item in report.must_violations[:3]:  # Show first 3
                        log.error(f"    {v_item.rule_name}: {v_item.found_text!r} → {v_item.suggestion}")
                else:
                    log.warning(f"InMail angle {angle_label} has SHOULD-FIX violations (allowed):")
                    for v_item in report.should_violations[:2]:  # Show first 2
                        log.warning(f"    {v_item.rule_name}: {v_item.found_text!r}")
            else:
                log.info(f"InMail angle {angle_label} passes brand voice check (confidence: {report.confidence_score:.0%})")
        return

    group_urn = li_client.create_campaign_group(group_name)

    # Same 4-source composition as the Sponsored Content path.
    default_exclude_urns = urn_res.resolve_default_excludes()
    family_exclude_urns  = urn_res.resolve_facet_pairs(family_exclude_pairs or [])
    data_driven_exclude_urns = urn_res.resolve_facet_pairs(data_driven_exclude_pairs or [])
    shared_exclude_urns  = _merge_urn_dicts(
        default_exclude_urns, family_exclude_urns, data_driven_exclude_urns,
    )

    for i, cohort in enumerate(selected):
        angle_idx   = i % 3
        angle_label = ["A", "B", "C"][angle_idx]
        tg_cat      = classify_tg(cohort.name, cohort.rules)

        # Generate InMail copy for this cohort
        variants = build_inmail_variants(tg_cat, cohort, claude_key)
        variant  = variants[angle_idx]

        # Validate InMail copy against brand voice
        full_copy = f"{variant.subject}\n\n{variant.body}"
        report = brand_voice_validator.validate_copy(full_copy)

        if not report.is_compliant:
            log.warning(f"InMail angle {angle_label}: {len(report.violations)} brand voice violations")
            log.warning(f"  Must fix: {len(report.must_violations)}")
            log.warning(f"  Should fix: {len(report.should_violations)}")

            if report.must_violations:
                log.error(f"InMail angle {angle_label} has MUST-FIX violations — blocking submission")
                for v_item in report.must_violations[:3]:
                    log.error(f"    {v_item.rule_name}: {v_item.found_text!r} → {v_item.suggestion}")
                raise RuntimeError(f"Brand voice violation in InMail angle {angle_label}: {report.must_violations[0].rule_name}")
            else:
                log.warning(f"InMail angle {angle_label} has SHOULD-FIX violations (allowed):")
                for v_item in report.should_violations[:2]:
                    log.warning(f"    {v_item.rule_name}: {v_item.found_text!r}")
        else:
            log.info(f"InMail angle {angle_label} passes brand voice check (confidence: {report.confidence_score:.0%})")

        facet_urns   = urn_res.resolve_cohort_rules(cohort.rules)
        # Apply Smart Ramp geo overrides if present
        if included_geos:
            facet_urns = _apply_geo_overrides(facet_urns, included_geos, urn_res)
        # Per-cohort override layer
        cohort_add_urns    = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_add", []) or [])
        cohort_remove_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_remove", []) or [])
        cohort_exclude_urns = _subtract_urn_dicts(
            _merge_urn_dicts(shared_exclude_urns, cohort_add_urns),
            cohort_remove_urns,
        )
        campaign_urn = li_client.create_inmail_campaign(
            name=cohort._stg_name,
            campaign_group_urn=group_urn,
            facet_urns=facet_urns,
            exclude_facet_urns=cohort_exclude_urns,
        )
        campaign_id = campaign_urn.rsplit(":", 1)[-1]
        sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
        log.info("Created InMail campaign %s angle=%s", campaign_urn, angle_label)

        creative_urn = li_client.create_inmail_ad(
            campaign_urn=campaign_urn,
            sender_urn=inmail_sender,
            subject=variant.subject,
            body=variant.body,
            cta_label=variant.cta_label,
            destination_url=destination_url_override,
        )
        sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)

        # Store validation report metadata
        validation_metadata = {
            "is_compliant": report.is_compliant,
            "must_violations": len(report.must_violations),
            "should_violations": len(report.should_violations),
            "confidence_score": report.confidence_score,
        }
        log.info(f"Creative validation: {json.dumps(validation_metadata)}")

        log.info(
            "InMail creative %s — cohort '%s' angle %s subject: %s",
            creative_urn, cohort.name, angle_label, variant.subject,
        )


def _retry_li_campaign(
    row, inmail_sender, sheets, li_client, urn_res,
    claude_key, figma_file, figma_node, mj_token, dry_run,
):
    """
    Re-attempt LinkedIn campaign + creative creation for a row that already
    has cohort data (stg_id, stg_name, targeting_criteria) but li_status=Failed/Pending.
    Skips Snowflake and analysis entirely.
    """
    import json as _json
    from dataclasses import dataclass, field as dc_field

    ad_type  = row.get("ad_type", "").strip().upper()
    location = row.get("location", "")
    flow_id  = row["flow_id"]

    # Reconstruct a minimal cohort-like object from sheet data
    @dataclass
    class SheetCohort:
        name: str
        rules: list
        lift_pp: float = 0.0
        _stg_id: str = ""
        _stg_name: str = ""
        _facet: str = ""
        _criteria: str = ""

    criteria_json = row["targeting_criteria"]
    try:
        criteria_parsed = _json.loads(criteria_json)
    except Exception:
        criteria_parsed = {}

    # Extract rules from targeting criteria for TG classification
    rules = []
    for group in criteria_parsed.get("include", []):
        for item in group.get("criteria", []):
            facet = item.get("facet", "")
            for val in item.get("values", []):
                rules.append((facet.lower(), val))

    cohort = SheetCohort(
        name=row["stg_name"],
        rules=rules,
        _stg_id=row["stg_id"],
        _stg_name=row["stg_name"],
        _facet=row["targeting_facet"],
        _criteria=criteria_json,
    )

    # Parse facet URNs from targeting criteria
    facet_urns: dict[str, list[str]] = {}
    for group in criteria_parsed.get("include", []):
        for item in group.get("criteria", []):
            facet_key = item.get("facetUrn", "")
            if facet_key and item.get("values"):
                facet_urns.setdefault(facet_key, []).extend(item["values"])

    # Apply Smart Ramp geo overrides if present (in retry context)
    included_geos = row.get("included_geos", [])
    if included_geos:
        facet_urns = _apply_geo_overrides(facet_urns, included_geos, urn_res)

    tg_cat = classify_tg(cohort.name, cohort.rules)

    if ad_type == "INMAIL":
        if not inmail_sender:
            log.error("LINKEDIN_INMAIL_SENDER_URN not set — cannot retry InMail for %s", cohort._stg_id)
            return

        variants = build_inmail_variants(tg_cat, cohort, claude_key)
        variant  = variants[0]  # default to Angle A for retries

        if dry_run:
            log.info("[dry-run] Would create InMail campaign for '%s'", cohort.name)
            log.info("[dry-run] Subject: %s", variant.subject)
            log.info("[dry-run] Body (first 100): %s…", variant.body[:100])
            return

        group_name  = f"Outlier {flow_id} {location} InMail".strip()
        group_urn   = li_client.create_campaign_group(group_name)
        campaign_urn = li_client.create_inmail_campaign(
            name=cohort._stg_name,
            campaign_group_urn=group_urn,
            facet_urns=facet_urns,
            exclude_facet_urns=urn_res.resolve_default_excludes(),
        )
        campaign_id = campaign_urn.rsplit(":", 1)[-1]
        sheets.update_li_campaign_id(cohort._stg_id, campaign_id)

        creative_urn = li_client.create_inmail_ad(
            campaign_urn=campaign_urn,
            sender_urn=inmail_sender,
            subject=variant.subject,
            body=variant.body,
            cta_label=variant.cta_label,
            destination_url=destination_url_override,
        )
        sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
        log.info("Retry InMail campaign %s creative %s", campaign_urn, creative_urn)
    else:
        # Image ad retry — skip creative generation, just recreate campaign
        if dry_run:
            log.info("[dry-run] Would create image ad campaign for '%s'", cohort.name)
            return

        if not row.get("master_campaign"):
            log.warning(
                "Skipping retry for stg_id=%s — master_campaign is empty. "
                "Cannot build campaign group URN without it.",
                row.get("stg_id", "?"),
            )
            return
        master_urn  = f"urn:li:sponsoredCampaignGroup:{row['master_campaign']}"
        campaign_urn = li_client.create_campaign(
            name=cohort._stg_name,
            campaign_group_urn=master_urn,
            facet_urns=facet_urns,
            exclude_facet_urns=urn_res.resolve_default_excludes(),
        )
        campaign_id = campaign_urn.rsplit(":", 1)[-1]
        sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
        log.info("Retry image ad campaign %s (no creative — re-run full launch to regenerate)", campaign_urn)


# ── Monitor mode ───────────────────────────────────────────────────────────────

def run_monitor(dry_run: bool = False) -> None:
    sheets    = SheetsClient()
    sheet_cfg = sheets.read_config()

    li_token   = (
        sheet_cfg.get("LINKEDIN_TOKEN") or
        os.getenv("LINKEDIN_ACCESS_TOKEN") or
        os.getenv("LINKEDIN_TOKEN") or
        config.LINKEDIN_TOKEN
    )
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

def _merge_urn_dicts(*dicts: dict[str, list[str]]) -> dict[str, list[str]]:
    """Union merge of {facet: [urns]} dicts. Dedupes within each facet,
    preserves insertion order so the first-source URNs come first."""
    out: dict[str, list[str]] = {}
    for d in dicts:
        for facet, urns in (d or {}).items():
            existing = out.setdefault(facet, [])
            for u in urns:
                if u not in existing:
                    existing.append(u)
    return out


def _subtract_urn_dicts(
    base: dict[str, list[str]],
    remove: dict[str, list[str]],
) -> dict[str, list[str]]:
    """Return `base` with every URN in `remove` stripped out. Empty facets get
    dropped so `_build_targeting_criteria` doesn't emit an empty `or` block."""
    if not remove:
        return {k: list(v) for k, v in (base or {}).items()}
    out: dict[str, list[str]] = {}
    remove_sets = {k: set(v) for k, v in remove.items()}
    for facet, urns in (base or {}).items():
        kept = [u for u in urns if u not in remove_sets.get(facet, set())]
        if kept:
            out[facet] = kept
    return out


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


def _apply_geo_overrides(
    facet_urns: dict[str, list[str]],
    included_geos: list[str],
    urn_resolver,
) -> dict[str, list[str]]:
    """
    Override profileLocations in facet_urns with Smart Ramp included_geos.
    Resolves country names to LinkedIn geo URNs via fuzzy matching.
    """
    if not included_geos:
        return facet_urns

    geo_urns = []
    for country in included_geos:
        urn = urn_resolver.resolve("profileLocations", country)
        if urn:
            geo_urns.append(urn)
            log.debug("Resolved geo '%s' → %s", country, urn)
        else:
            log.warning("Could not resolve geo '%s' to LinkedIn URN", country)

    if geo_urns:
        out = dict(facet_urns)
        out["profileLocations"] = geo_urns
        log.info("Applied geo override: %d countries → %d LinkedIn geo URNs", len(included_geos), len(geo_urns))
        return out

    return facet_urns


# ── Phase 2.6: Smart Ramp dual-arm pipeline ──────────────────────────────────
#
# These helpers expose run_launch_for_ramp(...) — the programmatic entry point
# the Smart Ramp poller (scripts/smart_ramp_poller.py) calls in-process. They
# are ADDITIVE — the legacy `_process_row` flow (used by the manual
# `python main.py --ramp-id <id>` CLI) keeps working unchanged.
#
# Locked design (CONTEXT.md):
#   - Stage A/B/C cohort discovery runs ONCE per row (Pitfall 1) via
#     _resolve_cohorts(...).
#   - For every cohort, BOTH _process_inmail_campaigns(...) AND
#     _process_static_campaigns(...) fire (SR-03).
#   - LinkedIn create_image_ad 403 / LINKEDIN_MEMBER_URN errors translate to
#     ImageAdResult(status="local_fallback") and the PNG is copied to
#     data/ramp_creatives/<ramp_id>/<cohort_id>_<mode>_<angle>__<safe_name>.png
#     (SR-04). One cohort's failure NEVER aborts other cohorts.


@dataclass
class ResolvedCohorts:
    """Output of _resolve_cohorts() — Stage A/B/C done; ready for both arms."""
    selected: list = field(default_factory=list)
    group_name: str = ""
    exclude_pairs: list = field(default_factory=list)
    project_id: str | None = None
    tg_cat: str = ""
    ad_type_hint: str = ""  # "INMAIL" or "STATIC" or "" — for compat with legacy callers
    geo_overrides_applied: bool = False
    # Pass-through context the campaign-creation arms need that we already
    # resolved (so we don't re-fetch the project meta or rebuild exclude URNs):
    family_exclude_pairs: list = field(default_factory=list)
    data_driven_exclude_pairs: list = field(default_factory=list)
    flow_id: str = ""
    location: str = ""


def _resolve_cohorts(
    row: dict,
    *,
    sheets,
    snowflake,
    li_client,
    urn_res,
    claude_key: str,
    flow_id: str = "",
    config_name: str = "",
    project_id: str | None = None,
    location: str = "",
    dry_run: bool = False,
) -> ResolvedCohorts:
    """Run Stage 1 → Stage A → Stage B → Stage C cohort discovery for a single
    row. Returns a ResolvedCohorts dataclass with `selected` populated.

    CRITICAL invariant (Pitfall 1): this MUST be called ONCE per row, even when
    both InMail and Static arms run for the same row. Calling it twice would
    double the ~30s of Snowflake/Stage work per ramp.

    Mirrors the cohort-discovery block in `_process_row` (lines 240-490). Kept
    independent from `_process_row` so the dual-arm `_process_row_both_modes`
    flow can call it once and dispatch BOTH arms with the same result.
    """
    from src.analysis import (
        pick_target_tier,
        small_sample_signals,
        stage_a_negative,
        MIN_POSITIVES_FOR_STATS,
        MIN_POSITIVES_FOR_SIGNALS,
    )
    from src.icp_exemplars import build_exemplars
    from src.linkedin_urn import feature_col_to_exclude_pair
    from src.icp_from_jobpost import (
        extract_base_role_candidates,
        base_role_feature_columns,
        required_skill_feature_columns,
        family_exclusions_for,
        derive_icp_from_job_post,
    )

    # 1. Stage 1 data pull (mirrors _process_row).
    # NOTE 2026-04-28: `fetch_stage1_contributors` was referenced but never implemented
    # (planned as project-scoped CESF+CBPR query — see _process_row docstring). Stopgap:
    # delegate to `fetch_screenings_by_project` which auto-resolves flow_id+config_name
    # via `resolve_project_to_flow` and returns the legacy RESUME_SQL screening data.
    # TODO: implement STAGE1_SQL properly to capture activators not in screening data.
    if project_id:
        df_raw, resolved_flow_id, resolved_config = snowflake.fetch_screenings_by_project(
            project_id,
            end_date=date.today().isoformat(),  # PIPE-03: don't trust stale .env SCREENING_END_DATE
        )
        flow_id = flow_id or resolved_flow_id
        config_name = config_name or resolved_config
    else:
        df_raw = snowflake.fetch_screenings(
            flow_id, config_name,
            end_date=date.today().isoformat(),
        )
    if df_raw.empty:
        log.warning(
            "_resolve_cohorts: no Stage 1 data for project=%s flow=%s — empty result",
            project_id, flow_id,
        )
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    log.info("_resolve_cohorts: raw data %d rows", len(df_raw))

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
    log.info("_resolve_cohorts: %d binary features", len(bin_cols))

    # 2b. Tiered ICP target selection
    job_post_meta: dict = {}
    project_meta: dict = {}
    try:
        if flow_id:
            job_post_meta = snowflake.fetch_job_post_meta(flow_id) or {}
        if project_id:
            project_meta = snowflake.fetch_project_meta(project_id) or {}
    except Exception as exc:
        log.warning("_resolve_cohorts: meta fetch failed (non-fatal): %s", exc)

    derived_icp: dict = {}
    description = (job_post_meta.get("description") or project_meta.get("description") or "").strip()
    if description:
        try:
            derived_icp = derive_icp_from_job_post(description) or {}
        except Exception as exc:
            log.warning("_resolve_cohorts: derive_icp_from_job_post failed: %s", exc)

    base_role_titles = extract_base_role_candidates(
        job_post_meta=job_post_meta,
        project_meta=project_meta,
        signup_flow_name=config_name or job_post_meta.get("job_name"),
        derived_tg_label=derived_icp.get("derived_tg_label"),
    )
    family_exclude_pairs = family_exclusions_for(
        job_post_meta=job_post_meta,
        project_meta=project_meta,
        signup_flow_name=config_name or job_post_meta.get("job_name"),
        derived_tg_label=derived_icp.get("derived_tg_label"),
    )

    target_tier, target_col, n_icp = pick_target_tier(df_bin)
    log.info("_resolve_cohorts: target tier=%s col=%s n_icp=%d", target_tier, target_col, n_icp)

    if n_icp == 0:
        log.warning("_resolve_cohorts: zero positives — no cohorts to resolve")
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    # Mode = stats vs sparse
    if n_icp < MIN_POSITIVES_FOR_STATS:
        log.warning(
            "_resolve_cohorts: only %d positives (< %d) — sparse mode; no cohorts mined.",
            n_icp, MIN_POSITIVES_FOR_STATS,
        )
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    # Data-driven exclusions (negative signal)
    data_driven_exclude_pairs: list = []
    neg_hits = stage_a_negative(df_bin, bin_cols, target_col)
    for h in neg_hits:
        pair = feature_col_to_exclude_pair(h["feature"])
        if pair:
            data_driven_exclude_pairs.append(pair)

    # Stage A
    title_anchor_cols = base_role_feature_columns(base_role_titles, list(df_bin.columns))
    skill_anchor_cols = required_skill_feature_columns(
        derived_icp.get("required_skills", []), list(df_bin.columns),
    )
    base_role_cols = title_anchor_cols + [c for c in skill_anchor_cols if c not in title_anchor_cols]
    cohorts_a = stage_a(df_bin, bin_cols, target_col=target_col, base_role_cols=base_role_cols)
    if not cohorts_a:
        log.warning("_resolve_cohorts: Stage A returned no cohorts")
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    # Stage B (skip when Stage A came from support mode)
    from_support_mode = any(getattr(c, "support", 0) > 0 for c in cohorts_a)
    cohorts_b = cohorts_a if from_support_mode else stage_b(df_bin, cohorts_a, target_col=target_col)

    # Stage C with graceful bypass
    try:
        selected = stage_c(cohorts_b, urn_res, li_client)
    except Exception as exc:
        log.warning("_resolve_cohorts: Stage C unavailable (%s) — falling back to Stage B top cohorts", exc)
        selected = []
    if not selected:
        selected = cohorts_b[:config.MAX_CAMPAIGNS]

    if not selected:
        log.warning("_resolve_cohorts: no cohorts survived Stage A/B")
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    log.info("_resolve_cohorts: %d cohorts selected", len(selected))

    # Persist stg_id / display name on each cohort so downstream arms can use them
    for cohort in selected:
        if not getattr(cohort, "_stg_id", None):
            cohort._stg_id = make_stg_id()
        if not getattr(cohort, "_stg_name", None):
            cohort._stg_name = _cohort_display_name(cohort, flow_id, location)
        if not getattr(cohort, "_facet", None):
            facet, criteria = _cohort_to_targeting_json(cohort)
            cohort._facet = facet
            cohort._criteria = criteria

    group_name = f"Outlier {flow_id} {location}".strip()
    return ResolvedCohorts(
        selected=list(selected),
        group_name=group_name,
        exclude_pairs=family_exclude_pairs + data_driven_exclude_pairs,
        project_id=project_id,
        tg_cat="",  # cohort-level, not row-level — set per-arm via classify_tg
        ad_type_hint="",
        geo_overrides_applied=False,
        family_exclude_pairs=family_exclude_pairs,
        data_driven_exclude_pairs=data_driven_exclude_pairs,
        flow_id=flow_id,
        location=location,
    )


def _save_creative_locally(
    png_path,
    ramp_id: str,
    cohort_id: str,
    mode: str,            # "inmail" or "static"
    angle: str,           # "A", "B", "C", "F", etc.
    campaign_name: str,
) -> str:
    """Copy a generated PNG to data/ramp_creatives/<ramp_id>/<cohort>_<mode>_<angle>__<safe_name>.png.

    SR-04: LinkedIn upload-blocked fallback. The PNG already exists at png_path
    (created by gemini_creative.py); we just copy it under a deterministic name
    so manual upload knows which campaign it belongs to. Original PNG is
    preserved (shutil.copy2, NOT shutil.move).

    The campaign_name is URL-encoded via urllib.parse.quote_plus to make it
    filesystem-safe. Group/campaign names are auto-prefixed with `agent_`
    upstream (LinkedInClient._prefixed) and are vocabulary-clean per CLAUDE.md.

    Returns the absolute target path as a string.
    """
    target_dir = Path("data/ramp_creatives") / ramp_id
    target_dir.mkdir(parents=True, exist_ok=True)
    safe_name = quote_plus(campaign_name or "unnamed")
    target = target_dir / f"{cohort_id}_{mode}_{angle}__{safe_name}.png"
    shutil.copy2(str(png_path), str(target))
    log.info("Creative saved locally: %s", target)
    return str(target)


def _process_static_campaigns(
    selected,
    *,
    flow_id: str,
    location: str,
    sheets,
    li_client,
    urn_res,
    claude_key: str,
    figma_file: str = "",
    figma_node: str = "",
    mj_token: str = "",
    dry_run: bool = False,
    family_exclude_pairs: list | None = None,
    data_driven_exclude_pairs: list | None = None,
    destination_url_override: str | None = None,
    included_geos: list[str] | None = None,
    ramp_id: str | None = None,
    cohort_id_override: str | None = None,
) -> dict:
    """Static-ad arm — symmetric counterpart to _process_inmail_campaigns.

    For each cohort: generate copy variants → render image (Figma or Gemini) →
    create LinkedIn campaign → attach image creative (with SR-04 local-fallback
    if create_image_ad returns ImageAdResult(status="local_fallback")).

    Per-cohort isolation: each cohort's loop body wrapped in try/except so one
    cohort's failure NEVER aborts the others.

    Returns a dict with the shape Plan 03 / scripts/smart_ramp_poller.py expects:
      {
        "campaigns": [campaign_urn, ...],
        "campaigns_by_cohort": {cohort_id: campaign_urn, ...},
        "creative_paths": {cohort_id: <urn or local path>, ...},
      }
    """
    family_exclude_pairs = family_exclude_pairs or []
    data_driven_exclude_pairs = data_driven_exclude_pairs or []
    included_geos = included_geos or []

    out_campaigns: list[str] = []
    by_cohort: dict[str, str] = {}
    creative_paths: dict[str, str] = {}

    if not selected:
        return {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}}

    # Generate creatives per cohort (mirrors _process_row lines 540-619)
    has_figma = bool(figma_file and figma_node and claude_key)
    figma_client = FigmaCreativeClient() if has_figma else None
    creative_pngs: list[Path | None] = []
    all_variants_per_cohort: list[list[dict]] = []

    for i, cohort in enumerate(selected):
        angle_idx = i % 3
        angle_label = ["A", "B", "C"][angle_idx]
        variants: list[dict] = []
        png_path: Path | None = None

        try:
            layer_map = (
                figma_client.get_text_layer_map(figma_file, figma_node)
                if has_figma else {}
            )
            # Phase 2.6: thread Smart Ramp included_geos into copy gen so the LLM
            # picks photo_subject ethnicities plausible for the targeted geo.
            # See _GEO_ETHNICITY_HINTS in src/figma_creative.py for the lookup.
            variants = build_copy_variants(cohort, layer_map, geos=included_geos)
        except Exception as exc:
            log.warning("Static copy generation failed for '%s': %s", cohort.name, exc)

        all_variants_per_cohort.append(variants)
        selected_variant = variants[angle_idx] if angle_idx < len(variants) else {}

        # Honor WITH_IMAGES=1 to run image gen even in dry-run mode (LinkedIn calls
        # still skipped at the lower gate). Default dry-run skips Gemini for cost.
        skip_image_gen = dry_run and not os.environ.get("WITH_IMAGES")
        if skip_image_gen:
            creative_pngs.append(None)
            continue

        # Figma clone path
        if has_figma and variants:
            try:
                tg_label = variants[0].get("tg_label", cohort.name) if variants else cohort.name
                clone_ids = apply_plugin_logic(figma_file, figma_node, variants, tg_label, claude_key)
                if clone_ids:
                    selected_id = clone_ids[angle_idx % len(clone_ids)]
                    pngs = figma_client.export_clone_pngs(figma_file, [selected_id])
                    png_path = pngs[0] if pngs else None
            except Exception as exc:
                log.warning("Static Figma path failed for '%s': %s — falling back to Gemini", cohort.name, exc)

        # Gemini path
        if png_path is None and selected_variant:
            try:
                from src.figma_creative import rewrite_variant_copy
                png_path, qc_report = generate_imagen_creative_with_qc(
                    variant=selected_variant,
                    max_retries=2,
                    copy_rewriter=rewrite_variant_copy,
                )
                # Hard-reject on QC FAIL — Phase 2.6 path was silently shipping bad
                # creatives until 2026-04-28. The campaign is still created, just with
                # no image attached. Diego sees the empty draft + the loud Slack alert
                # and can intervene manually.
                if qc_report and qc_report.get("verdict") == "FAIL":
                    log.error(
                        "Static QC FAIL for '%s' after %d attempts; REJECTING creative. "
                        "Campaign will be created without an image. Violations: %s",
                        cohort.name,
                        qc_report.get("attempts", 1),
                        qc_report.get("violations", []),
                    )
                    png_path = None
            except Exception as exc:
                log.warning("Static Gemini path failed for '%s': %s", cohort.name, exc)
                png_path = None

        creative_pngs.append(png_path)

    if dry_run:
        log.info("[dry-run] _process_static_campaigns: skipping LinkedIn calls (%d cohorts)", len(selected))
        return {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}}

    # Create campaign group + campaigns
    group_name = f"Outlier {flow_id} {location} Static".strip()
    group_urn = li_client.create_campaign_group(group_name)
    out_groups = [group_urn]
    log.debug("_process_static_campaigns: group=%s", group_urn)

    default_exclude_urns = urn_res.resolve_default_excludes()
    family_exclude_urns = urn_res.resolve_facet_pairs(family_exclude_pairs)
    data_driven_exclude_urns = urn_res.resolve_facet_pairs(data_driven_exclude_pairs)
    shared_exclude_urns = _merge_urn_dicts(
        default_exclude_urns, family_exclude_urns, data_driven_exclude_urns,
    )

    for i, cohort in enumerate(selected):
        # Per-cohort isolation: wrap each cohort's body in try/except so one
        # cohort's failure NEVER aborts the others (SR-04 contract).
        try:
            facet_urns = urn_res.resolve_cohort_rules(cohort.rules)
            if included_geos:
                facet_urns = _apply_geo_overrides(facet_urns, included_geos, urn_res)

            cohort_add_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_add", []) or [])
            cohort_remove_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_remove", []) or [])
            cohort_exclude_urns = _subtract_urn_dicts(
                _merge_urn_dicts(shared_exclude_urns, cohort_add_urns),
                cohort_remove_urns,
            )

            campaign_urn = li_client.create_campaign(
                name=cohort._stg_name,
                campaign_group_urn=group_urn,
                facet_urns=facet_urns,
                exclude_facet_urns=cohort_exclude_urns,
            )
            campaign_id = campaign_urn.rsplit(":", 1)[-1]
            sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
            out_campaigns.append(campaign_urn)
            by_cohort_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id
            by_cohort[by_cohort_id] = campaign_urn
            log.info("_process_static_campaigns: campaign %s for cohort %s", campaign_urn, by_cohort_id)

            # Image attach — sentinel-aware (SR-04).
            png_path = creative_pngs[i] if i < len(creative_pngs) else None
            if not (png_path and Path(str(png_path)).exists()):
                log.info("_process_static_campaigns: no PNG for cohort '%s' — skipping creative attach", cohort.name)
                continue

            angle_idx = i % 3
            angle_label = ["A", "B", "C"][angle_idx]
            variants = all_variants_per_cohort[i] if i < len(all_variants_per_cohort) else []
            variant = variants[angle_idx] if angle_idx < len(variants) else {}
            headline = variant.get("headline") or f"Your {_cohort_headline(cohort)} expertise is in demand."
            subhead = variant.get("subheadline") or "Earn payment doing remote AI tasks on your schedule."

            try:
                image_urn = li_client.upload_image(png_path)
                ad_result = li_client.create_image_ad(
                    campaign_urn=campaign_urn,
                    image_urn=image_urn,
                    headline=headline,
                    description=subhead,
                    intro_text=variant.get("intro_text", "") if variant else "",
                    ad_headline=variant.get("ad_headline", "") if variant else "",
                    ad_description=variant.get("ad_description", "") if variant else "",
                    cta_button=variant.get("cta_button", "APPLY") if variant else "APPLY",
                    destination_url=destination_url_override,
                )
            except Exception as exc:
                # upload_image (or other unexpected error) → record as fallback.
                log.warning("_process_static_campaigns: upload/attach raised for '%s': %s", cohort.name, exc)
                creative_paths[by_cohort_id] = ""
                continue

            if ad_result.status == "ok":
                creative_urn = ad_result.creative_urn
                sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
                creative_paths[by_cohort_id] = creative_urn
                log.info("_process_static_campaigns: creative %s attached", creative_urn)
            elif ad_result.status == "local_fallback":
                # SR-04: copy the PNG locally + continue.
                local_path = _save_creative_locally(
                    png_path=png_path,
                    ramp_id=ramp_id or "manual",
                    cohort_id=by_cohort_id,
                    mode="static",
                    angle=angle_label,
                    campaign_name=cohort._stg_name,
                )
                creative_paths[by_cohort_id] = local_path
                log.warning(
                    "_process_static_campaigns: creative for cohort %s angle %s saved locally at %s "
                    "(reason: %s — %s)",
                    by_cohort_id, angle_label, local_path,
                    ad_result.error_class, ad_result.error_message,
                )
            else:  # status == "error"
                log.error(
                    "_process_static_campaigns: create_image_ad hard error for cohort %s: %s — %s",
                    by_cohort_id, ad_result.error_class, ad_result.error_message,
                )
                creative_paths[by_cohort_id] = ""
        except Exception as exc:
            # Per-cohort isolation: log + continue with next cohort.
            log.exception(
                "_process_static_campaigns: cohort %d '%s' failed — continuing with next cohort: %s",
                i, getattr(cohort, "name", "?"), exc,
            )
            continue

    return {
        "campaigns": out_campaigns,
        "campaigns_by_cohort": by_cohort,
        "creative_paths": creative_paths,
        "campaign_groups": out_groups,
        "group_name": group_name,
    }


def _process_row_both_modes(
    row: dict,
    *,
    ramp_id: str,
    dry_run: bool = False,
    modes: tuple[str, ...] = ("inmail", "static"),
    sheets=None,
    snowflake=None,
    li_client=None,
    urn_res=None,
    claude_key: str = "",
    inmail_sender: str = "",
    brand_voice_validator=None,
    mj_token: str = "",
    figma_file: str = "",
    figma_node: str = "",
) -> dict:
    """Phase 2.6: run cohort discovery ONCE per row, then dispatch BOTH InMail
    + Static arms.

    Per-arm isolation: each arm wrapped in try/except so one arm's crash never
    aborts the other. Per-cohort isolation lives inside each arm.
    """
    flow_id = row.get("flow_id", "")
    location = row.get("location", "")
    config_name = row.get("config_name") or flow_id
    project_id = row.get("project_id")
    cohort_id_override = row.get("cohort_id")
    destination_url_override = row.get("selected_lp_url")
    included_geos = row.get("included_geos", []) or []

    resolved = _resolve_cohorts(
        row,
        sheets=sheets, snowflake=snowflake, li_client=li_client, urn_res=urn_res,
        claude_key=claude_key,
        flow_id=flow_id, config_name=config_name, project_id=project_id,
        location=location, dry_run=dry_run,
    )

    if not resolved.selected:
        log.warning("_process_row_both_modes: no cohorts resolved for row %s — skipping both arms",
                    cohort_id_override or flow_id or "?")
        return {
            "ok": True,
            "campaign_groups": [],
            "inmail_campaigns": [],
            "static_campaigns": [],
            "creative_paths": {},
            "per_cohort": [],
        }

    inmail_result: dict = {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": []}
    static_result: dict = {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": []}

    if "inmail" in modes:
        try:
            r = _process_inmail_campaigns(
                selected=resolved.selected,
                flow_id=flow_id,
                location=location,
                sheets=sheets,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                inmail_sender=inmail_sender,
                brand_voice_validator=brand_voice_validator,
                dry_run=dry_run,
                family_exclude_pairs=resolved.family_exclude_pairs,
                data_driven_exclude_pairs=resolved.data_driven_exclude_pairs,
                destination_url_override=destination_url_override,
                included_geos=included_geos,
            )
            # Legacy _process_inmail_campaigns returns None and writes to sheets directly;
            # to keep backwards compat we synthesize an empty dict if r is None.
            if isinstance(r, dict):
                inmail_result.update(r)
        except Exception:
            log.exception("_process_row_both_modes: InMail arm aborted — Static arm will still run")

    if "static" in modes:
        try:
            r = _process_static_campaigns(
                selected=resolved.selected,
                flow_id=flow_id,
                location=location,
                sheets=sheets,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                figma_file=figma_file,
                figma_node=figma_node,
                mj_token=mj_token,
                dry_run=dry_run,
                family_exclude_pairs=resolved.family_exclude_pairs,
                data_driven_exclude_pairs=resolved.data_driven_exclude_pairs,
                destination_url_override=destination_url_override,
                included_geos=included_geos,
                ramp_id=ramp_id,
                cohort_id_override=cohort_id_override,
            )
            if isinstance(r, dict):
                static_result.update(r)
        except Exception:
            log.exception("_process_row_both_modes: Static arm aborted — InMail arm result preserved")

    # Aggregate the per-cohort view.
    per_cohort = []
    for c in resolved.selected:
        cid = cohort_id_override or getattr(c, "id", None) or getattr(c, "_stg_id", "")
        per_cohort.append({
            "cohort_id": cid,
            "cohort_description": getattr(c, "cohort_description", "") or getattr(c, "name", ""),
            "inmail_urn": inmail_result.get("campaigns_by_cohort", {}).get(cid),
            "static_urn": static_result.get("campaigns_by_cohort", {}).get(cid),
            "inmail_creative": inmail_result.get("creative_paths", {}).get(cid),
            "static_creative": static_result.get("creative_paths", {}).get(cid),
        })

    return {
        "ok": True,
        "campaign_groups": (
            list(inmail_result.get("campaign_groups") or [])
            + list(static_result.get("campaign_groups") or [])
        ),
        "inmail_campaigns": list(inmail_result.get("campaigns", [])),
        "static_campaigns": list(static_result.get("campaigns", [])),
        "creative_paths": {
            **{f"{k}_inmail": v for k, v in inmail_result.get("creative_paths", {}).items()},
            **{f"{k}_static": v for k, v in static_result.get("creative_paths", {}).items()},
        },
        "per_cohort": per_cohort,
    }


def _ramp_to_rows(ramp) -> list[dict]:
    """Convert a RampRecord into the row-dict shape `_process_row_both_modes`
    consumes. Mirrors the logic in run_launch() lines ~120-138 — kept in one
    place so the CLI and run_launch_for_ramp don't drift.
    """
    rows = []
    for cohort in ramp.cohorts:
        rows.append({
            "flow_id": cohort.signup_flow_id or "",
            "location": "",
            "ad_type": "",  # Phase 2.6 ignores ad_type — both arms always run
            "figma_file": "",
            "figma_node": "",
            "config_name": ramp.project_name or "",
            "ramp_id": ramp.id,
            "cohort_id": cohort.id,
            "cohort_description": cohort.cohort_description,
            "selected_lp_url": cohort.selected_lp_url,
            "included_geos": cohort.included_geos,
            "matched_locales": cohort.matched_locales,
            "target_activations": cohort.target_activations,
            "linear_issue_id": ramp.linear_issue_id,
            "project_id": ramp.project_id,
        })
    return rows


def run_launch_for_ramp(
    ramp_id: str,
    modes: tuple[str, ...] = ("inmail", "static"),
    dry_run: bool = False,
) -> dict:
    """Programmatic entry point for the Smart Ramp poller (Plan 01).

    Fetches the ramp from Smart Ramp, iterates cohort rows, dispatches BOTH
    InMail + Static arms per cohort. Returns the aggregated result dict the
    poller's state file needs.

    Per-row isolation: a single row raising never aborts the rest. Per-cohort
    isolation lives inside _process_static_campaigns / _process_inmail_campaigns.

    Backwards compat note: this is ADDITIVE. The legacy `python main.py
    --ramp-id <id>` CLI continues to use the original `_process_row` flow via
    `run_launch()` — both paths coexist.
    """
    client = SmartRampClient()
    ramp = client.fetch_ramp(ramp_id)
    if not ramp:
        return {
            "ok": False,
            "error": f"Could not fetch ramp {ramp_id}",
            "campaign_groups": [],
            "inmail_campaigns": [],
            "static_campaigns": [],
            "creative_paths": {},
            "per_cohort": [],
        }

    # Resolve shared dependencies once (sheets / snowflake / li_client / etc.)
    sheets = SheetsClient()
    sheet_cfg = sheets.read_config()
    li_token = (
        sheet_cfg.get("LINKEDIN_TOKEN")
        or os.getenv("LINKEDIN_ACCESS_TOKEN")
        or os.getenv("LINKEDIN_TOKEN")
        or config.LINKEDIN_TOKEN
    )
    if not li_token:
        return {
            "ok": False,
            "error": "LINKEDIN_TOKEN not set",
            "campaign_groups": [],
            "inmail_campaigns": [],
            "static_campaigns": [],
            "creative_paths": {},
            "per_cohort": [],
        }
    li_client = LinkedInClient(li_token)
    urn_res = UrnResolver(sheets)
    snowflake = RedashClient()
    claude_key = os.getenv("ANTHROPIC_API_KEY") or os.getenv("CLAUDE_API_KEY", "")
    mj_token = sheet_cfg.get("MIDJOURNEY_API_TOKEN") or os.getenv("MIDJOURNEY_API_TOKEN", "")
    inmail_sender = (
        sheet_cfg.get("LINKEDIN_INMAIL_SENDER_URN")
        or os.getenv("LINKEDIN_INMAIL_SENDER_URN", config.LINKEDIN_INMAIL_SENDER_URN)
    )
    brand_voice_validator = BrandVoiceValidator()

    aggregated = {
        "ok": True,
        "campaign_groups": [],
        "inmail_campaigns": [],
        "static_campaigns": [],
        "creative_paths": {},
        "per_cohort": [],
    }

    for row in _ramp_to_rows(ramp):
        try:
            outcome = _process_row_both_modes(
                row,
                ramp_id=ramp_id,
                dry_run=dry_run,
                modes=modes,
                sheets=sheets,
                snowflake=snowflake,
                li_client=li_client,
                urn_res=urn_res,
                claude_key=claude_key,
                inmail_sender=inmail_sender,
                brand_voice_validator=brand_voice_validator,
                mj_token=mj_token,
                figma_file=row.get("figma_file", ""),
                figma_node=row.get("figma_node", ""),
            )
            aggregated["campaign_groups"].extend(outcome.get("campaign_groups", []) or [])
            aggregated["inmail_campaigns"].extend(outcome.get("inmail_campaigns", []) or [])
            aggregated["static_campaigns"].extend(outcome.get("static_campaigns", []) or [])
            aggregated["creative_paths"].update(outcome.get("creative_paths", {}) or {})
            aggregated["per_cohort"].extend(outcome.get("per_cohort", []) or [])
        except Exception:
            log.exception(
                "run_launch_for_ramp: row failed for ramp=%s cohort=%s — continuing with next row",
                ramp_id, row.get("cohort_id"),
            )
            continue

    return aggregated


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
        "--flow-id",
        help="Target a specific signup flow directly (bypasses Triggers sheet)",
    )
    parser.add_argument(
        "--project-id",
        help="Target a specific Outlier project (resolved to flow_id via Snowflake)",
    )
    parser.add_argument(
        "--ramp-id",
        help="Target a Smart Ramp by ID (fetches cohorts, runs one campaign per cohort)",
    )
    parser.add_argument(
        "--post-to-linear", action="store_true",
        help="Post cohort summaries to the Smart Ramp's Linear issue (requires --ramp-id)",
    )
    parser.add_argument(
        "--modes", nargs="+", default=None, choices=["inmail", "static"],
        help=(
            "Phase 2.6 dual-arm dispatch (with --ramp-id). Default = both arms via "
            "run_launch_for_ramp. Pass `--modes inmail` or `--modes static` for a "
            "single arm. Without this flag, the legacy single-arm `_process_row` "
            "flow runs (backwards-compatible)."
        ),
    )
    parser.add_argument(
        "--log-level", default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()
    logging.getLogger().setLevel(args.log_level)

    if args.mode == "monitor":
        run_monitor(dry_run=args.dry_run)
    elif args.ramp_id and args.modes:
        # Phase 2.6 programmatic dual-arm path — same code the poller uses.
        modes_tuple = tuple(args.modes)
        result = run_launch_for_ramp(args.ramp_id, modes=modes_tuple, dry_run=args.dry_run)
        log.info(
            "Ramp %s processed via run_launch_for_ramp: ok=%s inmail=%d static=%d cohorts=%d",
            args.ramp_id, result.get("ok"),
            len(result.get("inmail_campaigns", [])),
            len(result.get("static_campaigns", [])),
            len(result.get("per_cohort", [])),
        )
        sys.exit(0 if result.get("ok") else 1)
    else:
        run_launch(dry_run=args.dry_run, flow_id=args.flow_id, project_id=args.project_id, ramp_id=args.ramp_id)


if __name__ == "__main__":
    main()

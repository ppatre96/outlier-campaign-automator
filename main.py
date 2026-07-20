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
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from datetime import date
from pathlib import Path
from urllib.parse import quote_plus

import requests
from dotenv import load_dotenv

load_dotenv()

import config

# Set of campaign-group URNs we've discovered to no longer exist on LinkedIn
# during this process run. Used by `_retry_li_campaign` to short-circuit
# subsequent retries that reference the same dead group instead of spamming
# LinkedIn with N copies of the same 400. Cleared per-process (no persistence).
_DEAD_CAMPAIGN_GROUPS: set[str] = set()
from src.sheets import SheetsClient, make_stg_id
from src.redash_db import RedashClient
from src.smart_ramp_client import SmartRampClient
from src.linear_client import LinearClient
from src.features import engineer_features, build_frequency_maps, binary_features
from src.analysis import stage_a, stage_b   # stage_a is now a dispatcher (support vs lift)
from src.linkedin_urn import UrnResolver
from src.linkedin_api import LinkedInClient, ImageAdResult
from src.stage_c import stage_c
from src.linkedin_targeting_guard import linkedin_targeting_collapsed
from src.figma_creative import (
    FigmaCreativeClient,
    build_copy_variants,
    apply_plugin_logic,
    classify_tg,
)
from src.gemini_creative import generate_imagen_creative, generate_imagen_creative_with_qc
from src.inmail_copy_writer import build_inmail_variants
from src.task_card import cached_card
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
    urn_res   = UrnResolver(sheets, linkedin_client=li_client)
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
                    # Top-level Smart Ramp brief — applies across all cohorts in
                    # this Ramp. Distinct from per-cohort `cohort_description`.
                    # Folded into ICP derivation alongside cohort_description so
                    # both ramp-wide intent ("we're hiring for Project X") AND
                    # per-cohort specifics reach Stage A + the copy LLM.
                    "ramp_summary": ramp.summary or "",
                    "selected_lp_url": cohort.selected_lp_url,
                    "included_geos": cohort.included_geos,
                    "matched_locales": cohort.matched_locales,
                    "target_activations": cohort.target_activations,
                    "job_post_pay_rates": getattr(cohort, "job_post_pay_rates", None),
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

    def _run_one(row: dict) -> None:
        """Process a single pending row. Extracted so it can be submitted to
        a ThreadPoolExecutor when RAMP_CONCURRENCY > 1.
        """
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

    ramp_workers = max(1, int(getattr(config, "RAMP_CONCURRENCY", 1) or 1))
    if ramp_workers > 1 and len(pending) > 1:
        # Phase 3.4 — process multiple pending rows (cohorts within a Smart
        # Ramp, or independent flows from the Triggers sheet) concurrently.
        # Each _process_row runs Stage 1+2+C, creative gen (already pooled
        # inside via IMAGE_GEN_CONCURRENCY / COPY_GEN_CONCURRENCY), and
        # campaign creation across LinkedIn/Meta/Google. The shared clients
        # (LinkedInClient, SheetsClient, UrnResolver, gdrive cache) all have
        # internal locks; see Phase 3.3 + Phase 3.4 docstrings.
        log.info("Phase 3.4: running %d rows in parallel (RAMP_CONCURRENCY=%d)",
                 len(pending), ramp_workers)
        hard_error: Exception | None = None
        with ThreadPoolExecutor(max_workers=ramp_workers, thread_name_prefix="ramp") as ex:
            future_to_row = {ex.submit(_run_one, row): row for row in pending}
            for fut in as_completed(future_to_row):
                row = future_to_row[fut]
                fid = row.get("flow_id", "?")
                try:
                    fut.result()
                except RuntimeError as exc:
                    # HARD STOP — preserve current semantics but only after
                    # already-running rows finish; submitting more rows is
                    # safe because all `pending` were submitted up-front.
                    log.error("HARD STOP for flow %s: %s", fid, exc)
                    hard_error = hard_error or exc
                except Exception as exc:
                    log.exception("Unexpected error for flow %s: %s", fid, exc)
        if hard_error is not None:
            raise hard_error
    else:
        # Sequential fallback (RAMP_CONCURRENCY=1 or only one row) — preserves
        # the historical single-threaded behavior and stack-trace surfaces.
        for row in pending:
            try:
                _run_one(row)
            except RuntimeError as exc:
                log.error("HARD STOP for flow %s: %s", row.get("flow_id", "?"), exc)
                raise
            except Exception as exc:
                log.exception("Unexpected error for flow %s: %s", row.get("flow_id", "?"), exc)

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
    unique_id = row.get("unique_id", f"ROW_{row.get('sheet_row', 'UNKNOWN')}")
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
        df_raw, resolved_flow_id, resolved_config = snowflake.fetch_screenings_by_project(
            project_id,
            end_date=date.today().isoformat(),
        )
        flow_id     = flow_id     or resolved_flow_id
        config_name = config_name or resolved_config
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
            variants = build_copy_variants(
                cohort, layer_map, geos=None,
                icp=getattr(cohort, "_icp", None),
            )
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
                from src.figma_creative import rewrite_variant_copy, repair_photo_subject
                # max_retries comes from the function default (env-var
                # QC_MAX_RETRIES, default 9 = 10 attempts) — Pranav rule
                # 2026-04-29, we always want to ship a creative.
                png_path, qc_report = generate_imagen_creative_with_qc(
                    variant=selected_variant,
                    copy_rewriter=rewrite_variant_copy,
                    subject_repairer=repair_photo_subject,
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

        # Cold-start cohorts bypass Stage C's no_urns_resolved reject. If this
        # cohort meant to facet-target but nothing resolved, _apply_geo_overrides
        # above left us with geo-only targeting — shipping now would buy the
        # whole country (the GMR-0024 ~290M class). Skip + flag for a human.
        if linkedin_targeting_collapsed(cohort, facet_urns):
            msg = (
                f"LinkedIn cohort '{cohort.name}' targeting collapsed to geo-only "
                f"(no skill/title facet resolved) — skipped to avoid a country-wide "
                f"spend. Needs human targeting."
            )
            log.warning(msg)
            run_blockers.add(msg)
            continue

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
            campaign_state=getattr(cohort, "campaign_state", None),
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
                    ad_name=f"{cohort._stg_name} | {angle_label}",
                )
                if ad_result.status == "ok":
                    creative_urn = ad_result.creative_urn
                    sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
                    log.info("Attached creative %s to campaign %s", creative_urn, campaign_urn)
                    cohort_creative_urns[angle_label] = creative_urn
                elif ad_result.status == "local_fallback":
                    # SR-04: LinkedIn creative attach blocked (DSC 403 — MDP
                    # entitlement gate). Drive-only policy (no local PNG):
                    # PNGs from Outlier designer (Gemini) live exclusively in
                    # Shared Drive at <ramp_id>/<channel>/<cohort_geo>/<angle>.png;
                    # this CLI legacy single-arm path just records the blocker
                    # since it doesn't carry the cohort×geo metadata needed to
                    # build the Drive hierarchy from here.
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
            "config_name": config_name or None,
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

def _fmt_advertised_rate(base_rate_usd: float | None) -> str:
    """Format the resolved Smart Ramp pay rate for ad copy, PRESERVING cents.

    `int()` truncation was dropping the cents (e.g. $22.50/hr → "$22/hr"). Show
    two decimals only when there is a fractional part, so "$22.50/hr" but
    "$29/hr" (not "$29.00/hr"). Returns "" when no rate resolved — never a
    default; see [[feedback_smart_ramp_authoritative_data]].
    """
    if base_rate_usd is None:
        return ""
    if base_rate_usd % 1:
        return f"${base_rate_usd:.2f}/hr"
    return f"${int(base_rate_usd)}/hr"


def _process_inmail_campaigns(
    selected, flow_id, location,
    sheets, li_client, urn_res,
    claude_key, inmail_sender, brand_voice_validator, dry_run,
    family_exclude_pairs: list[tuple[str, str]] | None = None,
    data_driven_exclude_pairs: list[tuple[str, str]] | None = None,
    destination_url_override: str | None = None,
    included_geos: list[str] | None = None,
    base_rate_usd: float | None = None,
    rate_geo_specific: bool = False,
    ramp_id: str | None = None,
    cohort_id_override: str | None = None,
    cohort_description: str = "",
    unique_id: str | None = None,
    naming_meta: dict | None = None,
    seen_keys: set | None = None,
    daily_budget_cents: int | None = None,
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

    # InMail localization (opt-in per ramp via the console Review tab): when the
    # decision flag is set and the ramp has a non-English target locale, InMail
    # subject/body are translated into that language before validation/upload.
    _inmail_localize = False
    _inmail_locale = None
    if ramp_id:
        try:
            from src.ui_decisions import get_decision
            _inmail_localize = bool(getattr(get_decision(ramp_id), "localize_inmail", False))
        except Exception as _exc:  # noqa: BLE001 — best-effort; default English
            log.debug("InMail localize flag lookup failed (%s) — staying English", _exc)
    if _inmail_localize and naming_meta and naming_meta.get("locale"):
        from src.locales import get_locale
        _inmail_locale = get_locale(naming_meta.get("locale"))
    if _inmail_localize:
        log.info("_process_inmail_campaigns: InMail localization ON (locale=%s)",
                 getattr(_inmail_locale, "display_language", None))

    def _maybe_localize(variant):
        """Translate an InMailVariant's subject+body when localization is on and
        a non-English locale resolved. No-op otherwise. Returns a (possibly new)
        variant."""
        if not (_inmail_localize and _inmail_locale):
            return variant
        from src.copy_adapter import localize_inmail as _loc
        s, b = _loc(variant.subject, variant.body, _inmail_locale)
        if s == variant.subject and b == variant.body:
            return variant
        import dataclasses
        return dataclasses.replace(variant, subject=s, body=b)

    # Group-level name: Smart Ramp v2 spec without the per-leaf angle/geo
    # detail (those live on the child campaigns). Falls back to the legacy
    # "Outlier <flow_id> <location> InMail" pattern when naming_meta isn't
    # provided (CLI / dry-run / tests). Format segment is "InMail Group"
    # to distinguish from leaf "Inmail" format.
    if naming_meta is not None:
        from src.campaign_name import build_campaign_name as _build_grp_name
        group_name = _build_grp_name(
            ramp_id=ramp_id or "",
            submitted_at=naming_meta.get("submitted_at", ""),
            cohort=None,
            platform="linkedin",
            campaign_type="inmail",
            format_override="InMail Group",
            pod=naming_meta.get("pod"),
            domain=naming_meta.get("domain"),
            locale=naming_meta.get("locale"),
            included_geos=naming_meta.get("included_geos"),
            campaign_state=naming_meta.get("campaign_state"),
        )
    else:
        group_name = f"Outlier {flow_id} {location} InMail".strip()

    from src.geo_tiers import group_geos_for_campaigns, GeoCampaignGroup
    from src.campaign_registry import log_campaign as _reg_log_inmail
    from src.ui_decisions import upsert_launch_progress as _lp
    from src.ui_decisions import next_generation as _next_gen

    raw_geos = included_geos or []
    geo_groups = group_geos_for_campaigns(raw_geos, base_rate_usd, apply_geo_multiplier=not rate_geo_specific)
    if not geo_groups:
        geo_groups = [GeoCampaignGroup(
            cluster="global_mix", cluster_label="Global", geos=[],
            median_multiplier=1.0, advertised_rate=_fmt_advertised_rate(base_rate_usd),
            campaign_suffix="global",
        )]
    # Experimentation caps: max 3 cohorts per geo cluster, all angles tested
    capped_cohorts_inmail = selected[:config.MAX_COHORTS_PER_GEO_CLUSTER]
    inmail_angle_keys = ["A", "B", "C"][:config.ANGLES_PER_COHORT]
    log.info(
        "_process_inmail_campaigns: %d cohort(s) × %d geo group(s) × %d angle(s) = %d campaigns",
        len(capped_cohorts_inmail), len(geo_groups), len(inmail_angle_keys),
        len(capped_cohorts_inmail) * len(geo_groups) * len(inmail_angle_keys),
    )

    if dry_run:
        for i, cohort in enumerate(capped_cohorts_inmail):
            tg_cat = classify_tg(cohort.name, cohort.rules)
            for geo_group in geo_groups:
                for angle_label in inmail_angle_keys:
                    log.info("[dry-run] InMail cohort %d '%s' tg=%s angle=%s geo=%s rate=%s",
                             i, cohort.name, tg_cat, angle_label,
                             geo_group.cluster_label, geo_group.advertised_rate)
                    variants = build_inmail_variants(
                        tg_cat, cohort, claude_key,
                        hourly_rate=geo_group.advertised_rate,
                        geo_icp_hint=geo_group.icp_hint,
                        task_card=cached_card(ramp_id, cohort_id_override),
                    )
                    v = variants[["A","B","C"].index(angle_label) % len(variants)]
                    v = _maybe_localize(v)
                log.info("[dry-run] Subject: %s", v.subject)
                log.info("[dry-run] Body:\n%s", v.body)
                log.info("[dry-run] CTA: %s", v.cta_label)

                full_copy = f"{v.subject}\n\n{v.body}"
                report = brand_voice_validator.validate_copy(full_copy)
                if not report.is_compliant:
                    log.warning(f"InMail angle {angle_label}: {len(report.violations)} brand voice violations")
                    log.warning(f"  Must fix: {len(report.must_violations)}")
                    log.warning(f"  Should fix: {len(report.should_violations)}")
                    if report.must_violations:
                        log.error(f"InMail angle {angle_label} has MUST-FIX violations")
                        for v_item in report.must_violations[:3]:
                            log.error(f"    {v_item.rule_name}: {v_item.found_text!r} → {v_item.suggestion}")
                    else:
                        log.warning(f"InMail angle {angle_label} has SHOULD-FIX violations (allowed):")
                        for v_item in report.should_violations[:2]:
                            log.warning(f"    {v_item.rule_name}: {v_item.found_text!r}")
                else:
                    log.info(f"InMail angle {angle_label} passes brand voice check (confidence: {report.confidence_score:.0%})")
        return

    # Reviewer feedback (GMR-0024, 2026-06-11): land every agent-built campaign
    # as a DRAFT inside ONE shared "agent" staging group rather than a fresh
    # group per ramp. `group_name` is retained for registry/logging only.
    group_urn = li_client.get_or_create_staging_group()

    # Same 4-source composition as the Sponsored Content path.
    default_exclude_urns = urn_res.resolve_default_excludes()
    family_exclude_urns  = urn_res.resolve_facet_pairs(family_exclude_pairs or [])
    data_driven_exclude_urns = urn_res.resolve_facet_pairs(data_driven_exclude_pairs or [])
    shared_exclude_urns  = _merge_urn_dicts(
        default_exclude_urns, family_exclude_urns, data_driven_exclude_urns,
    )

    # NEW structure (2026-05-13): one InMail campaign per (cohort × geo),
    # with multiple InMail ads attached (one per angle). Previous structure
    # was 1 campaign per (cohort × geo × angle) which produced 3× the
    # campaign count and didn't match the LinkedIn campaign-group / campaign
    # / creative hierarchy the marketing team is standardizing on.
    from src import launch_verify
    healed_empties: list[dict] = []
    for cohort in capped_cohorts_inmail:
        for geo_group in geo_groups:
            # Cross-row dedup (run_launch_for_ramp path only): if a prior row
            # in the same ramp already produced an InMail campaign for this
            # (cohort × geo_cluster), skip — see _cohort_geo_dedup_key for the
            # motivation. Default seen_keys=None (legacy _process_row CLI
            # caller) → check is a no-op and behavior is unchanged.
            if seen_keys is not None:
                _dedup_key = _cohort_geo_dedup_key(cohort.name, geo_group.cluster)
                if _dedup_key in seen_keys:
                    log.info(
                        "_process_inmail_campaigns: skipping (cohort=%r geo=%r) — "
                        "already produced by an earlier row in this ramp (cohort_id=%s)",
                        cohort.name, geo_group.cluster, cohort_id_override or "?",
                    )
                    continue
                seen_keys.add(_dedup_key)
            # Per-cohort idempotency: skip a (cohort × geo) that already has a
            # live LinkedIn InMail campaign (bypassed on replace).
            if _cohort_channel_already_live(ramp_id, "linkedin", "inmail", cohort, geo_group):
                log.info(
                    "_process_inmail_campaigns: skipping (cohort=%r geo=%r) — already has a "
                    "live LinkedIn InMail campaign (idempotent re-launch)",
                    cohort.name, geo_group.cluster,
                )
                continue
            tg_cat      = classify_tg(cohort.name, cohort.rules)
            group_geos  = geo_group.geos or raw_geos

            # Generate all InMail variants for this (cohort × geo) — one call
            # produces variants for all angles in `inmail_angle_keys`.
            variants = build_inmail_variants(
                tg_cat, cohort, claude_key,
                hourly_rate=geo_group.advertised_rate,
                geo_icp_hint=geo_group.icp_hint,
                task_card=cached_card(ramp_id, cohort_id_override),
            )
            if not variants:
                log.warning(
                    "No InMail variants for cohort '%s' geo=%s — skipping all angles",
                    cohort.name, geo_group.cluster_label,
                )
                continue
            # Phase 5 — persist per-angle rationale for the console.
            if ramp_id:
                _persist_cohort_rationales(
                    ramp_id=ramp_id,
                    cohort=cohort,
                    geo_cluster=geo_group.cluster,
                    channel="linkedin_inmail",
                    variants=variants,
                )

            # Per-angle brand voice validation. Aggregate failures so MUST
            # violations on one angle don't kill the whole (cohort × geo) —
            # we drop the offending angle, keep the rest.
            valid_pairs: list[tuple[str, object]] = []  # (angle_label, variant)
            for angle_label in inmail_angle_keys:
                angle_idx = inmail_angle_keys.index(angle_label)
                variant = variants[angle_idx % len(variants)]
                variant = _maybe_localize(variant)
                full_copy = f"{variant.subject}\n\n{variant.body}"
                report = brand_voice_validator.validate_copy(full_copy)
                if report.is_compliant:
                    log.info(
                        "InMail angle %s passes brand voice check (confidence: %.0f%%)",
                        angle_label, report.confidence_score * 100,
                    )
                    valid_pairs.append((angle_label, variant))
                elif report.must_violations:
                    log.error(
                        "InMail angle %s has MUST-FIX brand voice violations — DROPPING this angle",
                        angle_label,
                    )
                    for v_item in report.must_violations[:3]:
                        log.error(
                            "    %s: %r → %s",
                            v_item.rule_name, v_item.found_text, v_item.suggestion,
                        )
                else:
                    log.warning(
                        "InMail angle %s has SHOULD-FIX violations (allowed): %d",
                        angle_label, len(report.should_violations),
                    )
                    for v_item in report.should_violations[:2]:
                        log.warning("    %s: %r", v_item.rule_name, v_item.found_text)
                    valid_pairs.append((angle_label, variant))

            if not valid_pairs:
                log.warning(
                    "All InMail angles failed brand voice for cohort '%s' geo=%s — skipping",
                    cohort.name, geo_group.cluster_label,
                )
                continue

            # Per-(cohort × geo) isolation: failure in one combo (URN resolution,
            # LinkedIn validation, network) must NOT abort the remaining combos
            # in the outer loop. Mirrors _process_static_campaigns. The 2026-05-16
            # incident — LinkedIn returning 400 FAILED_TO_PROCESS_CAMPAIGN_FOR_
            # AUDIENCE_SIZE_ESTIMATION on a single NW-European cohort — aborted
            # the entire row's InMail arm before this try block existed.
            # Launch-progress telemetry (console "Launch status" view).
            _lp_locale = (naming_meta.get("locale") if naming_meta else "") or ""
            _lp_cohort_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id
            _lp_kw = dict(
                ramp_id=ramp_id or "", channel="linkedin_inmail", locale=_lp_locale,
                cohort_id=str(_lp_cohort_id or ""), cohort_signature=getattr(cohort, "name", ""),
                geo_cluster=geo_group.cluster,
            )
            _lp(**_lp_kw, status="queued")
            # Additive launch → fresh generation (v2/v3…); default stays gen 1.
            _gen = (
                _next_gen(ramp_id=ramp_id or "", platform="linkedin", campaign_type="inmail",
                          cohort_signature=getattr(cohort, "name", ""), geo_cluster=geo_group.cluster)
                if getattr(config, "ADDITIVE_LAUNCH", False) else 1
            )
            try:
                # Targeting resolution — once per (cohort × geo).
                facet_urns = urn_res.resolve_cohort_rules(cohort.rules)
                if group_geos:
                    facet_urns = _apply_geo_overrides(facet_urns, group_geos, urn_res)
                facet_urns = _apply_generalist_language_skill(facet_urns, cohort)

                # Cold-start cohorts bypass Stage C — guard against shipping a
                # geo-only (country-wide) InMail campaign when no skill/title
                # facet resolved. See _process_static_campaigns for the rationale.
                if linkedin_targeting_collapsed(cohort, facet_urns):
                    log.warning(
                        "InMail cohort '%s' geo=%s targeting collapsed to geo-only "
                        "(no skill/title facet resolved) — skipped to avoid a "
                        "country-wide spend. Needs human targeting.",
                        cohort.name, geo_group.cluster_label,
                    )
                    continue

                # Per-geo audience recheck (2026-05-20). See matching block in
                # _process_static_campaigns. InMail audience reach is gated by
                # the same audienceCounts API; recording per (cohort × geo)
                # lets the console flag sub-50k clusters for manual override.
                geo_audience: int | None
                try:
                    geo_audience = li_client.get_audience_count(facet_urns)
                    log.info(
                        "Per-geo InMail audience: cohort=%s geo=%s → %d",
                        cohort.name, geo_group.cluster_label, geo_audience,
                    )
                except Exception as _aud_exc:
                    log.warning(
                        "Per-geo InMail audience count failed for cohort=%s geo=%s: %s",
                        cohort.name, geo_group.cluster_label, _aud_exc,
                    )
                    geo_audience = None

                cohort_add_urns    = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_add", []) or [])
                cohort_remove_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_remove", []) or [])
                cohort_exclude_urns = _subtract_urn_dicts(
                    _merge_urn_dicts(shared_exclude_urns, cohort_add_urns),
                    cohort_remove_urns,
                )

                # Campaign name — Smart Ramp v2 spec, ONE name per (cohort × geo)
                # (no angle suffix; angles are now the multi-creative dimension).
                if naming_meta is not None:
                    from src.campaign_name import build_campaign_name
                    campaign_name = build_campaign_name(
                        ramp_id=ramp_id or "",
                        submitted_at=naming_meta.get("submitted_at", ""),
                        cohort=cohort,
                        geo_group=geo_group,
                        platform="linkedin",
                        campaign_type="inmail",
                        pod=naming_meta.get("pod"),
                        domain=naming_meta.get("domain"),
                        locale=naming_meta.get("locale"),
                        included_geos=naming_meta.get("included_geos"),
                        campaign_state=naming_meta.get("campaign_state"),
                    )
                else:
                    geo_suffix = f" [{geo_group.cluster_label}]" if geo_group.cluster != "global_mix" else ""
                    campaign_name = f"{cohort._stg_name}{geo_suffix} InMail"

                # ONE campaign per (cohort × geo)
                _li_inmail_budget_kwargs = (
                    {"daily_budget_cents": daily_budget_cents}
                    if daily_budget_cents is not None else {}
                )
                # Optimize on the per-pod WS Grant conversion (pod from Smart
                # Ramp). When the pod is known this REPLACES the default OCP
                # conversion so LinkedIn optimizes on worker_skill_grant only;
                # unknown pod → None → falls back to LINKEDIN_CONVERSION_ID.
                _pod_conv = _linkedin_pod_conversion_id(naming_meta.get("pod") if naming_meta else None)
                _lp(**_lp_kw, status="creating")
                campaign_urn = li_client.create_inmail_campaign(
                    name=campaign_name,
                    campaign_group_urn=group_urn,
                    facet_urns=facet_urns,
                    exclude_facet_urns=cohort_exclude_urns,
                    conversion_id=_pod_conv,
                    **_li_inmail_budget_kwargs,
                )
                campaign_id = campaign_urn.rsplit(":", 1)[-1]
                sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
                log.info(
                    "Created InMail campaign %s cohort=%s geo=%s rate=%s (%d angles to attach)",
                    campaign_urn, cohort.name, geo_group.cluster_label, geo_group.advertised_rate, len(valid_pairs),
                )
                _lp(**_lp_kw, status="created")
            except Exception as exc:
                log.exception(
                    "_process_inmail_campaigns: cohort '%s' geo=%s campaign creation failed — skipping all angles: %s",
                    getattr(cohort, "name", "?"), geo_group.cluster_label, exc,
                )
                _lp(**_lp_kw, status="failed", error=str(exc))
                continue

            # Build the UTM destination URL once per (cohort × geo). Each
            # angle's ad shares the same target URL but gets a distinct
            # utm_content slug so attribution can split angle performance.
            from src.utm_builder import build_utm_url, resolve_base_lp_url
            base_lp = resolve_base_lp_url(
                campaign_state=(naming_meta or {}).get("campaign_state"),
                platform="linkedin",
                fallback=destination_url_override or config.LINKEDIN_DESTINATION,
                matched_domain=(naming_meta or {}).get("domain"),
                sheets_client=sheets,
                ramp_id=ramp_id,
                cohort_id=cohort_id_override or getattr(cohort, "id", None) or "",
            )

            # Attach one InMail ad per (valid) angle to the same campaign.
            base_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id

            def _attach_inmail(angle_label, variant) -> bool:
                # Per-angle UTM URL: shared campaign_name + distinct utm_content
                utm_url = build_utm_url(
                    base_url=base_lp, platform="linkedin",
                    campaign_name=campaign_name,
                    pod=(naming_meta or {}).get("pod"),
                    domain=(naming_meta or {}).get("domain"),
                    locale=(naming_meta or {}).get("locale"),
                    language=((naming_meta or {}).get("campaign_state") or {}).get("linkedin", {}).get("liAdLanguage") or "EN",
                    utm_content=f"{cohort._stg_id}-inmail-{angle_label}",
                ) if base_lp else (destination_url_override or "")

                try:
                    creative_urn = li_client.create_inmail_ad(
                        campaign_urn=campaign_urn,
                        sender_urn=inmail_sender,
                        subject=variant.subject,
                        body=variant.body,
                        cta_label=variant.cta_label,
                        destination_url=utm_url,
                        # Readable ad name = campaign-spec name + angle, so the
                        # InMail ad is legible in Campaign Manager (vs inmail_<ts>).
                        ad_name=f"{campaign_name} | Angle {angle_label}",
                    )
                except Exception as _exc:
                    log.warning(
                        "_process_inmail_campaigns: create_inmail_ad raised cohort=%s angle=%s: %s",
                        cohort.name, angle_label, _exc,
                    )
                    _im_errors.append(f"{type(_exc).__name__}: {str(_exc)[:200]}")
                    return False
                if not creative_urn:
                    _im_errors.append("create_inmail_ad returned no creative URN")
                    return False
                sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)

                try:
                    _reg_log_inmail(
                        smart_ramp_id=ramp_id or flow_id or "",
                        cohort_id=str(base_id),
                        cohort_signature=cohort.name,
                        geo_cluster=geo_group.cluster,
                        geo_cluster_label=geo_group.cluster_label,
                        geos=group_geos,
                        angle=angle_label,
                        campaign_type="inmail",
                        advertised_rate=geo_group.advertised_rate,
                        audience_size=geo_audience,
                        linkedin_campaign_urn=campaign_urn,
                        creative_urn=creative_urn,
                        inmail_subject=variant.subject,
                        inmail_body=variant.body,
                        campaign_name=campaign_name,
                        generation=_gen,
                    )
                except Exception as _exc:
                    log.warning("Registry log failed (non-fatal): %s", _exc)

                log.info(
                    "InMail creative %s — cohort '%s' angle %s geo=%s subject: %s",
                    creative_urn, cohort.name, angle_label, geo_group.cluster_label, variant.subject,
                )
                return True

            _im_ok = 0
            _im_failed: list[tuple] = []
            _im_errors: list[str] = []  # failure reasons for verify-and-heal surfacing
            for angle_label, variant in valid_pairs:
                if _attach_inmail(angle_label, variant):
                    _im_ok += 1
                else:
                    _im_failed.append((angle_label, variant))

            # ── Verify-and-heal (piece C). If no InMail content attached to
            # this campaign, retry the failed angle(s) once; if still empty,
            # archive the campaign + flag so no empty shell survives launch.
            if config.LAUNCH_VERIFY_ENABLED and _im_ok == 0 and _im_failed:
                log.warning(
                    "_process_inmail_campaigns: 0/%d InMail ads attached to %s — "
                    "retrying %d angle(s) once before heal",
                    len(valid_pairs), campaign_urn, len(_im_failed),
                )
                for angle_label, variant in _im_failed:
                    if _attach_inmail(angle_label, variant):
                        _im_ok += 1
                if _im_ok == 0:
                    _reason = (
                        "; ".join(dict.fromkeys(_im_errors))[:400] if _im_errors
                        else "no InMail content attached after retry"
                    )
                    _summ = launch_verify.heal_empty(
                        platform="linkedin",
                        container_id=campaign_urn,
                        ramp_id=ramp_id or "",
                        campaign_name=campaign_name,
                        reason=_reason,
                        li_client=li_client,
                    )
                    if _summ:
                        healed_empties.append(_summ)

    if config.LAUNCH_VERIFY_ENABLED:
        launch_verify.notify_healed(ramp_id or "", healed_empties)


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

        # Pay rate is authoritative from Smart Ramp (row.job_post_pay_rates),
        # never a hardcoded default. Resolve it here so retried InMail copy
        # carries the correct $/hr (or stays rate-free) instead of the old $50.
        from src.attribution_resolver import parse_job_post_pay_rate
        _retry_rate = parse_job_post_pay_rate(row.get("job_post_pay_rates"))
        variants = build_inmail_variants(
            tg_cat, cohort, claude_key,
            hourly_rate=_fmt_advertised_rate(_retry_rate),
        )
        variant  = variants[0]  # default to Angle A for retries

        if dry_run:
            log.info("[dry-run] Would create InMail campaign for '%s'", cohort.name)
            log.info("[dry-run] Subject: %s", variant.subject)
            log.info("[dry-run] Body:\n%s", variant.body)
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
            # Readable ad name (retry path defaults to Angle A — see variant above).
            ad_name=f"{cohort._stg_name} | Angle A",
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
        # Skip rows pointing at a group URN we've already proven dead in this
        # run — avoids spamming LinkedIn with N copies of the same 400 when
        # multiple sheet rows share an archived master_campaign.
        if master_urn in _DEAD_CAMPAIGN_GROUPS:
            log.warning(
                "Skipping retry for stg_id=%s — campaign group %s was already "
                "shown to not exist earlier this run. Clear master_campaign "
                "in the sheet to retry under a fresh group.",
                row.get("stg_id", "?"), row["master_campaign"],
            )
            return
        try:
            campaign_urn = li_client.create_campaign(
                name=cohort._stg_name,
                campaign_group_urn=master_urn,
                facet_urns=facet_urns,
                exclude_facet_urns=urn_res.resolve_default_excludes(),
                campaign_state=getattr(cohort, "campaign_state", None),
            )
        except requests.exceptions.HTTPError as exc:
            resp = exc.response
            body = (resp.text or "") if resp is not None else ""
            if (
                resp is not None
                and resp.status_code == 400
                and "FIELD_VALUE_DOES_NOT_EXIST" in body
                and "campaignGroup" in body
            ):
                _DEAD_CAMPAIGN_GROUPS.add(master_urn)
                log.error(
                    "Retry skipped for stg_id=%s — campaign group %s no longer "
                    "exists on LinkedIn (archived or deleted). Clear the "
                    "master_campaign column for this row in the Triggers sheet "
                    "so the next run creates a fresh group.",
                    row.get("stg_id", "?"), row["master_campaign"],
                )
                return
            raise
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
    # Generalist/i18n locale cohort (Bug 2) → LinkedIn targets GEO ONLY in v1.
    # Emit empty résumé-facet criteria so the synthetic ("interface_locale", …)
    # rule never leaks into URN resolution; included_geos are applied downstream
    # via _apply_geo_overrides. (Interface-locale facet targeting is a planned
    # fast-follow — see data/plan_generalist_locale_targeting.md.)
    if (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale"):
        return "generalist_locale", json.dumps([])
    primary_facet = _feature_to_facet(cohort.rules[0][0]) if cohort.rules else "unknown"
    criteria = [
        {"feature": r[0], "value": r[1], "lift_pp": round(cohort.lift_pp, 2)}
        for r in cohort.rules
    ]
    return primary_facet, json.dumps(criteria)


def _cohort_geo_dedup_key(cohort_name: str, geo_cluster: str) -> tuple[str, str]:
    """Normalized (cohort × geo_cluster) key for cross-row dedup inside one ramp.

    A ramp with N cohort rows that share project_id pulls identical Snowflake
    data into Stage A (see _resolve_cohorts), so multiple rows commonly mine the
    same cohort name. Combined with overlapping included_geos, this produced
    duplicate LinkedIn campaigns (e.g. "Finance × Anglo / south_asian" 3×).

    run_launch_for_ramp seeds one set per arm (InMail and Static dedup
    independently — both should produce one campaign per unique tuple) and
    passes them into the arm functions. First row to hit a tuple wins; later
    rows skip with a structured log line.

    Normalization: case-insensitive + stripped to absorb cosmetic differences.
    Returns a 2-tuple to keep the key hashable and cheap to print.
    """
    return ((cohort_name or "").strip().lower(), (geo_cluster or "").strip().lower())


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


def _apply_generalist_language_skill(facet_urns: dict[str, list[str]], cohort) -> dict[str, list[str]]:
    """For a generalist locale cohort, add the LinkedIn language SKILL facet
    (Diego 2026-06-04: "on skills you can look for languages"). LinkedIn has no
    interface-locale facet for most of these languages, so we target the
    language as a skill — people with e.g. the Bengali skill, within the geo —
    instead of everyone in the geo. No-op for specialist cohorts or locales
    without a known skill URN (falls back to geo-only)."""
    locale = (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale")
    if not locale:
        return facet_urns
    from src.locales import linkedin_skill_urn
    urn = linkedin_skill_urn(locale)
    if not urn:
        return facet_urns
    out = dict(facet_urns)
    skills = list(out.get("skills") or [])
    if urn not in skills:
        skills.append(urn)
    out["skills"] = skills
    log.info("Applied generalist language skill: locale=%s → %s", locale, urn)
    return out


def _cohort_channel_already_live(ramp_id, platform: str, campaign_type: str, cohort, geo_group) -> bool:
    """Per-cohort launch idempotency: True when this (cohort × geo × channel ×
    type) already has a live campaign and should be skipped on a re-launch — so
    a forced re-run creates campaigns ONLY for cohorts that don't have them yet
    (surgically adds a newly-added cohort instead of duplicating the rest).

    Off when SKIP_EXISTING_COHORT_CAMPAIGNS is false, on a REPLACE_EXISTING run
    (replace archives + recreates on purpose), or when there's no ramp_id."""
    if not ramp_id:
        return False
    from src.ui_decisions import campaign_exists_for_cohort_channel
    if config.REPLACE_EXISTING or config.ADDITIVE_LAUNCH or not config.SKIP_EXISTING_COHORT_CAMPAIGNS:
        # Guard disabled. REPLACE_EXISTING archives + recreates on purpose;
        # ADDITIVE_LAUNCH creates a NEW generation alongside prior ones on purpose
        # (each keeps a distinct campaigns-table key via `generation`), so it's
        # safe. But SKIP_EXISTING=false with REPLACE=false AND ADDITIVE=false
        # silently creates a NEW campaign ALONGSIDE any existing one with no
        # archival — the failure mode behind GMR-0023's 2026-07-03 ko-KR/vi-VN
        # duplicates (a scoped re-run of the same locales dispatched with
        # skip_existing=false). Warn loudly for THAT case only (not the
        # intentional additive path), so an accidental re-run is visible.
        if not config.REPLACE_EXISTING and not config.ADDITIVE_LAUNCH and campaign_exists_for_cohort_channel(
            ramp_id, platform, campaign_type,
            getattr(cohort, "name", ""), getattr(geo_group, "cluster", ""),
        ):
            log.warning(
                "DUPLICATE RISK: %s/%s cohort=%r geo=%r already has a live campaign, "
                "but SKIP_EXISTING_COHORT_CAMPAIGNS=false and REPLACE_EXISTING=false — "
                "creating a NEW campaign ALONGSIDE it (no archival). If this is an "
                "accidental re-run of the same locales, cancel and re-dispatch with "
                "skip_existing=true.",
                platform, campaign_type, getattr(cohort, "name", ""),
                getattr(geo_group, "cluster", ""),
            )
        return False
    return campaign_exists_for_cohort_channel(
        ramp_id, platform, campaign_type,
        getattr(cohort, "name", ""), getattr(geo_group, "cluster", ""),
    )


def _linkedin_pod_conversion_id(pod) -> int | None:
    """Map a Smart Ramp pod (job_post_pod) → its per-pod LinkedIn WS Grant
    conversion rule id. Passed as create_campaign(conversion_id=...) so it
    REPLACES the default LINKEDIN_CONVERSION_ID and LinkedIn optimizes on
    worker_skill_grant only. Returns None when the pod is missing/unrecognized
    (campaign falls back to LINKEDIN_CONVERSION_ID). See
    config.LINKEDIN_POD_CONVERSION_IDS."""
    return config.LINKEDIN_POD_CONVERSION_IDS.get((pod or "").strip().lower())


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
    # Prestige tiering signal across the chosen tier's positives (Pranav rule
    # 2026-04-29: conditional graft — fold prestige cues into copy/targeting
    # only when ≥50% of positives skew top-tier). See compute_prestige_signal
    # in src/profile_tiering.py. {} when fetch is skipped or no data.
    prestige_signal: dict = field(default_factory=dict)
    # Combined Smart Ramp brief — top-level `ramp_summary` + per-cohort
    # `cohort_description`, joined with newlines. The requester's free-form
    # audience description (e.g., "We want cardiologists with 5+ yrs exp").
    # Surfaced 2026-04-29 (GMR-0016): the per-cohort field was being read in
    # main.py and dropped on the floor — Stage A and the copy LLM never saw
    # "cardiologist" so creatives went out with generic PhD photo subjects.
    # Threaded through build_copy_variants as description_hint.
    smart_ramp_brief: str = ""
    # Requirement-commonality signal across the chosen tier's positives.
    # List of dicts: [{requirement, n_hits, n_total, hit_rate, recommended_action}].
    # action ∈ {"hard_filter", "soft_hint", "drop"} — see compute_requirement_commonality.
    # Phase 1 (2026-04-29): surfaced + logged. Stage A consumption is Phase 2.
    requirement_commonality: list = field(default_factory=list)


def is_generalist_cohort(row: dict) -> str | None:
    """Return the BCP-47 locale to target if this Smart Ramp cohort is a
    per-locale generalist cohort, else None.

    Generalist requires BOTH (a) a known locale in matched_locales, and (b) a
    "generalist"/"i18n" signal in the cohort description — so a specialist ramp
    that merely carries a locale is NOT hijacked. See
    data/plan_generalist_locale_targeting.md.
    """
    if not row:
        return None
    desc = (row.get("cohort_description") or "").lower()
    if "generalist" not in desc and "i18n" not in desc:
        return None
    from src.locales import get_locale
    for loc in (row.get("matched_locales") or []):
        lt = get_locale(loc)
        if lt:
            return lt.locale
    return None


def _build_locale_cohort(row: dict, locale: str):
    """Construct a single Cohort that targets a generalist locale (language +
    geo) instead of résumé facets. Marked with facet_strength['generalist_locale']
    so the channel resolvers use locale targeting."""
    from src.analysis import Cohort
    from src.locales import get_locale
    lt = get_locale(locale)
    name = (row.get("cohort_description") or "").strip() or f"{lt.display_language} generalist contributors"
    cohort = Cohort(name=name, rules=[("interface_locale", locale)])
    cohort.facet_strength = {"generalist_locale": locale}
    return cohort


def _resolve_locale_cohort(
    row: dict, locale: str, *, flow_id: str, location: str, project_id: str | None,
    li_client=None, urn_res=None,
) -> "ResolvedCohorts":
    """Self-contained generalist path: build one locale cohort, persist its
    per-channel audience + targeting (idempotent, no wipe so sibling locales of
    the same ramp coexist), and return. Skips Stage A/B/C beam discovery."""
    from src.sheets import make_stg_id
    cohort = _build_locale_cohort(row, locale)
    cohort._stg_id = make_stg_id()
    cohort._stg_name = _cohort_display_name(cohort, flow_id, location)
    facet, criteria = _cohort_to_targeting_json(cohort)
    cohort._facet = facet
    cohort._criteria = criteria

    ramp_id = (row or {}).get("ramp_id") or ""
    included_geos = list((row or {}).get("included_geos") or [])
    log.info(
        "_resolve_cohorts: generalist locale path ramp=%s locale=%s cohort=%r geos=%s (beam skipped)",
        ramp_id, locale, cohort.name, included_geos,
    )

    if ramp_id:
        try:
            from src.prep_audience import measure_audience_for_cohort
            from src.ui_decisions import upsert_cohort_audience, upsert_cohort_targeting
            enabled = [p.strip().lower() for p in (config.ENABLED_PLATFORMS or "").split(",") if p.strip()]
            if not enabled:
                enabled = ["linkedin", "meta", "google"]
            rows = measure_audience_for_cohort(
                cohort, included_geos=included_geos, enabled_platforms=enabled,
                li_client=li_client, urn_resolver=urn_res,
            )
            for ca in rows:
                upsert_cohort_audience(
                    ramp_id=ramp_id, cohort_id=cohort._stg_id,
                    cohort_signature=cohort.name, platform=ca.platform,
                    audience_size=ca.audience_size, status=ca.status,
                    geos_used=ca.geos_used, rules_dropped=ca.rules_dropped,
                    forecast=ca.forecast,
                )
                upsert_cohort_targeting(
                    ramp_id=ramp_id, cohort_id=cohort._stg_id,
                    cohort_signature=cohort.name, platform=ca.platform,
                    facets=ca.facets,
                )
            log.info(
                "_resolve_cohorts: locale audience persisted ramp=%s cohort=%s %s",
                ramp_id, cohort.name,
                " ".join(f"{r.platform}={r.audience_size}({r.status})" for r in rows),
            )
        except Exception as exc:
            log.warning("_resolve_cohorts: locale audience block failed (non-fatal): %s", exc)

        # Persist a lightweight localized ICP so the locale cohort shows in the
        # console's combined ICP card (the beam path's LLM ICP needs a résumé
        # sample, which generalist cohorts don't have — use a template instead).
        try:
            from src.ui_decisions import upsert_cohort_icp
            from src.locales import get_locale
            _lt = get_locale(locale)
            _lang = _lt.display_language if _lt else locale
            upsert_cohort_icp(
                ramp_id=ramp_id, cohort_id=cohort._stg_id, cohort_signature=cohort.name,
                icp_dict={
                    "cohort_description": cohort.name,
                    "top_motivations": [
                        "Flexible, fully remote schedule",
                        f"Earn working in {_lang}",
                        "Generalist tasks — no specialized degree required",
                    ],
                    "decision_drivers": [
                        "Current tasking rate",
                        f"Work in {_lang}",
                        "Flexible hours",
                        "Reputable platform (Outlier / Scale)",
                    ],
                    "content_prefs": [f"Localized copy in {_lang}", "Authentic local imagery"],
                    "creative_liberty": "medium",
                    "language_pref": _lang,
                    "skill_priorities": [],
                    "sample_size_n": None,
                    "model_version": "generalist-locale-template-v1",
                },
            )
        except Exception as exc:
            log.warning("_resolve_cohorts: locale ICP persist failed (non-fatal): %s", exc)

    group_name = f"Outlier {flow_id} {location}".strip()
    return ResolvedCohorts(
        selected=[cohort],
        group_name=group_name,
        project_id=project_id,
        flow_id=flow_id,
        location=location,
        smart_ramp_brief=(row or {}).get("cohort_description") or (row or {}).get("ramp_summary") or "",
    )


def _resolve_cold_start_cohort(
    row: dict,
    *,
    snowflake,
    li_client,
    urn_res,
    project_id: str | None,
    flow_id: str,
    location: str,
) -> ResolvedCohorts:
    """Frame-independent cold start — the last resort across the whole
    cold-start regime (n_icp < MIN_POSITIVES_FOR_STATS, i.e. < 30 qualified CBs,
    INCLUDING the extreme empty-screening-frame case like GMR-0024's
    BLV-accessibility ramp).

    The in-frame ICP-fallback (`_try_icp_fallback`) is tried first; it anchors
    the synthetic cohort on skills that matched *columns in the data frame*. It
    returns nothing when the frame is empty (no columns) OR when the LLM's ICP
    skills don't match any corpus column. In both cases we land here: read the
    job post / Smart Ramp brief and (when COLD_START_MULTI_COHORT) derive 1..N
    TARGETED cohorts — each carrying rules across every channel-usable prefix
    (skills__, job_titles_norm__, fields_of_study__, highest_degree_level__) plus
    lookalike-title exclusions — so each channel resolver produces focused
    targeting instead of broad geo-only. Multiple cohorts are produced only when
    the request explicitly names distinct sub-groups. Targeting is all rule-based
    so it works with no warehouse rows behind it.

    Cascade: multi-cohort LLM specs → single-ICP fallback (skills-only when the
    flag is off) → empty when there's no job post / brief / targetable attribute.
    Returns a ResolvedCohorts with the synthesized cohort(s); each gets its ICP +
    reach-per-channel + (downstream) test-angle cards in the console.
    """
    import re as _re
    from src.analysis import Cohort
    from src.icp_from_jobpost import (
        resolve_job_post, derive_icp_from_job_post, derive_cohorts_from_job_post,
        extract_base_role_candidates, family_exclusions_for, _normalize_degrees,
    )
    from src.locales import country_name_to_iso2

    ramp_summary = (row.get("ramp_summary") or "").strip()
    cohort_description = (row.get("cohort_description") or "").strip()
    brief = "\n".join(filter(None, [ramp_summary, cohort_description])).strip()
    try:
        raw_post = resolve_job_post(
            snowflake, project_id=project_id or "", signup_flow_id=flow_id,
            override_text=(row.get("job_post_override") if isinstance(row, dict) else None),
        )
    except Exception as exc:
        log.warning("cold_start: resolve_job_post failed (%s)", exc)
        raw_post = ""
    # Same description shape the stats path feeds the LLM: Smart Ramp brief first,
    # then the Snowflake job-post HTML.
    description = "\n\n".join(filter(None, [brief, raw_post or ""]))

    # ── Build cohort specs ────────────────────────────────────────────────────
    # Multi-cohort (default): one richer LLM call returns 1..N targeted specs.
    # Falls back to the single-ICP extraction (and, with the flag off, to the
    # legacy skills-only spec) so a niche/new ramp still gets targeting, not
    # nothing. A "spec" = {label, required_skills, job_titles, fields_of_study,
    # degrees, geos}.
    fallback_geo = ""
    specs: list[dict] = []
    # Manual per-ramp override wins over LLM derivation (e.g. GMR-0024 BLV →
    # accessibility-professional skill facets; see config.COHORT_SPEC_OVERRIDES).
    _override_specs = config.COHORT_SPEC_OVERRIDES.get((row or {}).get("ramp_id") or "")
    if _override_specs:
        specs = [dict(s) for s in _override_specs]
        log.warning(
            "cold_start: ramp=%s using COHORT_SPEC_OVERRIDES (%d spec(s)) — bypassing LLM derivation",
            (row or {}).get("ramp_id"), len(specs),
        )
    if not specs and config.COLD_START_MULTI_COHORT and description:
        specs = derive_cohorts_from_job_post(description, max_cohorts=config.MAX_COHORTS_PER_GEO_CLUSTER)
    if not specs:
        icp = derive_icp_from_job_post(description) if description else {}
        label0 = (icp.get("derived_tg_label") or (brief.splitlines()[0][:60] if brief else "")).strip()
        skills0 = [s for s in (icp.get("required_skills") or []) if (s or "").strip()][:8]
        fallback_geo = country_name_to_iso2(icp.get("geography")) or ""
        if config.COLD_START_MULTI_COHORT:
            # Richer single cohort (titles come from the label via base-role match).
            specs = [{
                "label": label0, "required_skills": skills0, "job_titles": [],
                "fields_of_study": [f for f in (icp.get("required_fields") or [])][:5],
                "degrees": _normalize_degrees(icp.get("required_degrees") or []),
                "geos": [],
            }] if (skills0 or label0) else []
        else:
            # Legacy: skills-only single cohort (exact prior behavior).
            specs = [{"label": label0, "required_skills": skills0, "job_titles": [],
                      "fields_of_study": [], "degrees": [], "geos": []}] if (skills0 or label0) else []

    if not specs:
        log.warning(
            "cold_start: no job post / brief / targetable spec for project=%s flow=%s — empty result",
            project_id, flow_id,
        )
        return ResolvedCohorts(flow_id=flow_id, location=location, project_id=project_id)

    def _slug(v: str) -> str:
        return _re.sub(r"[^a-z0-9]+", "_", (v or "").strip().lower()).strip("_")

    # ── Spec → Cohort (rules across every channel-usable prefix) ──────────────
    cohorts: list = []
    seen_names: set[str] = set()
    for spec in specs:
        label = (spec.get("label") or "ICP cold-start cohort").strip()
        rules: list[tuple[str, int]] = []
        seen_rules: set[str] = set()

        def _add(prefix: str, value: str):
            slug = _slug(value)
            key = f"{prefix}__{slug}"
            if slug and key not in seen_rules:
                seen_rules.add(key)
                rules.append((key, 1))

        for s in spec.get("required_skills", [])[:5]:
            _add("skills", s)
        # Titles: explicit spec titles + base-role family matched off the label.
        # `skills_only` specs (manual overrides) skip the base-role fold so the
        # cohort stays single-facet — titles AND skills would AND down to a tiny
        # intersection (e.g. GMR-0024: 4.1M skills-only vs 5.4k titles∩skills).
        titles = list(spec.get("job_titles", []))
        if not spec.get("skills_only"):
            titles += extract_base_role_candidates(derived_tg_label=label)
        for t in titles:
            _add("job_titles_norm", t)
        for f in spec.get("fields_of_study", [])[:3]:
            _add("fields_of_study", f)
        for d in spec.get("degrees", [])[:2]:
            _add("highest_degree_level", d)

        # Disambiguate duplicate labels so cohort_signature stays unique
        # (cohort_icp/cohort_audience/cohort_targeting key on it).
        name = label
        if name in seen_names:
            disc = (spec.get("required_skills") or spec.get("geos") or [""])[0]
            name = f"{label} ({disc})".strip()[:80] if disc else f"{label} #{len(seen_names)+1}"
        seen_names.add(name)

        cohort = Cohort(
            name=name or "ICP cold-start cohort", rules=rules,
            n=0, passes=0, pass_rate=0.0, lift_pp=0.0, p_value=1.0,
            score=1.0, support=0, coverage=0.0,
        )
        cohort.exclude_add = family_exclusions_for(derived_tg_label=label)
        cohort._stg_id = make_stg_id()
        cohort._stg_name = _cohort_display_name(cohort, flow_id, location)
        # Stash the cohort's geo preference for the audience estimate (spec.geos
        # → ramp included_geos → ICP geography). NOTE: only the cold-start reach
        # estimate honors per-cohort geos; the launched arms use row-level
        # included_geos (geo is row-level downstream — documented constraint).
        cohort._cold_start_geos = list(spec.get("geos") or [])
        cohorts.append(cohort)
        log.warning(
            "cold_start: synthesized cohort %r with %d rule(s) %s exclude=%d — "
            "job-post-derived targeting (looser than Stage A)",
            name, len(rules), [r[0] for r in rules], len(cohort.exclude_add or []),
        )

    # ── Per-cohort persistence (ICP + reach-per-channel cards) ────────────────
    ramp_id = (row or {}).get("ramp_id") or ""
    if ramp_id:
        from src.icp_enrichment import enrich as enrich_icp
        from src.prep_audience import measure_audience_for_cohort
        from src.ui_decisions import (
            upsert_cohort_icp, upsert_cohort_audience, upsert_cohort_targeting,
        )
        # Wipe prior-run cohort rows for this ramp before re-persisting. The
        # normal `_resolve_cohorts` path does this, but cold start returns early
        # (before that block), so without this a re-run whose labels shifted
        # left orphan cohorts in the console. Mirror that cleanup (+ targeting).
        current_sigs = sorted({c.name for c in cohorts if getattr(c, "name", None)})
        # ONLY_COHORT (feature 010): a scoped per-cohort run must be purely
        # ADDITIVE — skip the ramp-wide wipe entirely so existing cohorts' rows
        # survive. The upserts below are ON-CONFLICT keyed, so only the target
        # cohort's rows are added/updated.
        if config.ONLY_COHORT:
            log.info("cold_start: ONLY_COHORT=%s — skipping ramp-wide cohort-row wipe (additive)", config.ONLY_COHORT)
        else:
            try:
                from src.ui_decisions import _connect
                with _connect() as conn, conn.cursor() as cur:
                    cur.execute("DELETE FROM cohort_icp WHERE ramp_id = %s", (ramp_id,))
                    cur.execute("DELETE FROM cohort_audience WHERE ramp_id = %s", (ramp_id,))
                    cur.execute("DELETE FROM cohort_targeting WHERE ramp_id = %s", (ramp_id,))
                    if current_sigs:
                        # psycopg3 (not psycopg2) does NOT expand a tuple into a
                        # SQL `IN (...)` list — `IN %s` with a tuple renders the
                        # whole tuple as one param ("syntax error at or near $2").
                        # Use `<> ALL(array)`: psycopg3 adapts a Python list to a
                        # Postgres array, and `x <> ALL(arr)` == NOT IN for the
                        # non-null cohort_signature column.
                        cur.execute(
                            "DELETE FROM cohort_brief_rationale WHERE ramp_id = %s AND cohort_signature <> ALL(%s)",
                            (ramp_id, list(current_sigs)),
                        )
                    else:
                        cur.execute("DELETE FROM cohort_brief_rationale WHERE ramp_id = %s", (ramp_id,))
                    conn.commit()
                log.info("cold_start: cleared prior cohort rows for ramp=%s (keeping %s)", ramp_id, current_sigs)
            except Exception as exc:
                log.warning("cold_start: prior-row cleanup skipped (non-fatal): %s", exc)
        enabled = [p.strip().lower() for p in (config.ENABLED_PLATFORMS or "").split(",") if p.strip()] \
            or ["linkedin", "meta", "google"]
        row_geos = list((row or {}).get("included_geos") or [])
        locale_hint = (f"en-{location.upper()[:2]}" if location and len(location) >= 2 else None)
        for cohort in cohorts:
            try:
                icp_obj = enrich_icp(cohort, resume_sample=[], locale_hint=locale_hint)
                cohort._icp = icp_obj
                upsert_cohort_icp(
                    ramp_id=ramp_id, cohort_id=cohort._stg_id,
                    cohort_signature=cohort.name, icp_dict=icp_obj.to_dict(),
                )
            except Exception as exc:
                log.warning("cold_start: ICP enrich/persist failed for %r (non-fatal): %s", cohort.name, exc)
            try:
                geos = list(getattr(cohort, "_cold_start_geos", []) or []) or row_geos \
                    or ([fallback_geo] if fallback_geo else [])
                for ca in measure_audience_for_cohort(
                    cohort, included_geos=geos, enabled_platforms=enabled,
                    li_client=li_client, urn_resolver=urn_res,
                ):
                    upsert_cohort_audience(
                        ramp_id=ramp_id, cohort_id=cohort._stg_id, cohort_signature=cohort.name,
                        platform=ca.platform, audience_size=ca.audience_size, status=ca.status,
                        geos_used=ca.geos_used, rules_dropped=ca.rules_dropped, forecast=ca.forecast,
                    )
                    upsert_cohort_targeting(
                        ramp_id=ramp_id, cohort_id=cohort._stg_id, cohort_signature=cohort.name,
                        platform=ca.platform, facets=ca.facets,
                    )
            except Exception as exc:
                log.warning("cold_start: audience persist failed for %r (non-fatal): %s", cohort.name, exc)

    return ResolvedCohorts(
        selected=cohorts,
        group_name=f"Outlier {flow_id} {location}".strip(),
        project_id=project_id, flow_id=flow_id, location=location,
        smart_ramp_brief=brief,
    )


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

    # 0. Generalist/i18n locale targeting (Bug 2). For a per-locale generalist
    # cohort, target by language + geo instead of running Stage A beam discovery
    # over résumé features (which produced noise cohorts — environmental
    # engineering, adsorption+dna — for generalist ramps). Self-contained early
    # return; the specialist beam path below is untouched.
    if config.GENERALIST_LOCALE_TARGETING:
        _locale = is_generalist_cohort(row)
        if _locale:
            return _resolve_locale_cohort(
                row, _locale, flow_id=flow_id, location=location, project_id=project_id,
                li_client=li_client, urn_res=urn_res,
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
        # Empty screening frame (brand-new / ultra-niche project, no contributor
        # history). The in-frame ICP-fallback below can't run — it anchors on
        # skills matched against frame columns, and there's no frame. Route to a
        # frame-independent cold start that derives an ICP + 1 cohort from the
        # job post / brief, so the console still shows ICP + cohort + angles
        # instead of empty cards. Returns empty only when there's no job post.
        log.warning(
            "_resolve_cohorts: no Stage 1 data for project=%s flow=%s — "
            "routing to job-post cold start",
            project_id, flow_id,
        )
        return _resolve_cold_start_cohort(
            row, snowflake=snowflake, li_client=li_client, urn_res=urn_res,
            project_id=project_id, flow_id=flow_id, location=location,
        )

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

    # Smart Ramp briefs — the requester's free-form asks. Two levels:
    #   - ramp_summary:       top-level, applies to all cohorts in this Ramp
    #   - cohort_description: per-cohort specifics (e.g., "cardiologists in IN+SG")
    # Both prepended to the Snowflake job_post description so the LLM ICP
    # derivation sees Smart Ramp intent FIRST, then the broader project blurb.
    # Surfaced 2026-04-29 (GMR-0016 cardiologist drift).
    ramp_summary = (row.get("ramp_summary") or "").strip()
    cohort_description = (row.get("cohort_description") or "").strip()
    smart_ramp_brief = "\n".join(filter(None, [ramp_summary, cohort_description])).strip()
    derived_icp: dict = {}
    snowflake_description = (job_post_meta.get("description") or project_meta.get("description") or "").strip()
    description = "\n\n".join(filter(None, [smart_ramp_brief, snowflake_description]))
    if smart_ramp_brief:
        log.info(
            "_resolve_cohorts: Smart Ramp brief = %r (will fold into ICP derivation + copy gen)",
            smart_ramp_brief[:200],
        )
    # Build the description_hint that flows to the copy LLM via build_copy_variants.
    # Prefer Smart Ramp brief (authoritative requester intent). If empty (e.g.,
    # GMR-0016 came in via launchd without a fresh Smart Ramp form), fall back
    # to a compact summary of Snowflake-side context — flow_name + job_name +
    # domain + project_description — which still beats the cohort signature alone.
    if smart_ramp_brief:
        copy_description_hint = smart_ramp_brief
    else:
        rich_parts = [
            job_post_meta.get("flow_name", ""),
            job_post_meta.get("job_name", ""),
            job_post_meta.get("domain", ""),
            project_meta.get("name", ""),
            (project_meta.get("description") or "")[:400],
        ]
        copy_description_hint = "\n".join(p for p in rich_parts if p and p.strip()).strip()
        if copy_description_hint:
            log.info(
                "_resolve_cohorts: no Smart Ramp brief — using Snowflake-derived hint for copy gen: %r",
                copy_description_hint[:200],
            )
    if description:
        try:
            derived_icp = derive_icp_from_job_post(description) or {}
        except Exception as exc:
            log.warning("_resolve_cohorts: derive_icp_from_job_post failed: %s", exc)

    # ── Stage 1 brief-pool filter ────────────────────────────────────────────
    # Constrain df_bin (and the matching df_raw rows) to contributors whose
    # resume free-text overlaps with the brief's keywords. Without this,
    # Stage A mines features that predict screening pass across the ENTIRE
    # T1 pool — surfacing off-brief cohorts (e.g. "phd_student" for a
    # video-creator ramp because phds pass screening at high rates).
    #
    # Keywords come from:
    #   1. derived_icp.required_skills          (LLM-extracted must-haves)
    #   2. derived_icp.derived_tg_label tokens  (the LLM's role label)
    #   3. smart_ramp_brief tokens              (requester's verbatim ask)
    #
    # Matched against resume_job_skills, resume_job_title, resume_field,
    # and resume_job_company on a case-insensitive substring basis.
    #
    # Soft-fail: if filtered pool shrinks below MIN_POSITIVES_FOR_STATS, log
    # and revert to the unfiltered df_bin so Stage A still has signal to work
    # with. Disable entirely via STAGE1_BRIEF_FILTER_ENABLED=false.
    if os.getenv("STAGE1_BRIEF_FILTER_ENABLED", "true").lower() == "true":
        # Build keyword set from the brief: LLM-derived required_skills,
        # derived_tg_label tokens, and Smart Ramp brief tokens.
        keywords: list[str] = []
        for s in (derived_icp.get("required_skills") or []):
            if s and len(s.strip()) >= 3:
                keywords.append(s.strip().lower())
        tg_label = (derived_icp.get("derived_tg_label") or "").strip()
        for tok in tg_label.replace("(", " ").replace(")", " ").replace(",", " ").split():
            t = tok.strip().lower()
            if len(t) >= 4 and t not in {"with", "from", "have", "their", "they", "this", "that", "want", "and", "the"}:
                keywords.append(t)
        for tok in smart_ramp_brief.replace(",", " ").split():
            t = tok.strip().strip(".:;").lower()
            if len(t) >= 5 and t not in {"with", "from", "have", "their", "they", "this", "that", "want", "those", "looking"}:
                keywords.append(t)
        # Dedupe while preserving order.
        _seen: set[str] = set()
        keywords = [k for k in keywords if not (k in _seen or _seen.add(k))]

        # df_raw is the Redash screening query result — has identity columns
        # only, NOT free-text resume fields. The free-text was already encoded
        # into df_bin's binary features (skills__video_editing,
        # job_titles_norm__editor, fields_of_study__media, etc.) by
        # binary_features() above. Match keywords against the suffix of each
        # binary column and OR-join the masks.
        if keywords:
            anchor_cols: list[str] = []
            for col in bin_cols:
                # Column shape: <facet>__<value>, e.g. "skills__video_editing".
                suffix = col.split("__", 1)[-1].lower().replace("_", " ")
                if len(suffix) < 3:
                    continue  # avoid 1-char suffixes spuriously matching
                # Match if ANY keyword is a substring of the value suffix.
                # Direction is one-way (keyword ⊆ suffix) so "video" matches
                # "skills__video_editing" but the reverse direction is dropped
                # to prevent tiny tokens swallowing every column.
                if any(k in suffix for k in keywords if len(k) >= 3):
                    anchor_cols.append(col)
            log.info(
                "_resolve_cohorts: Stage 1 brief filter — keywords=%s matched %d binary anchors: %s",
                keywords[:6], len(anchor_cols), anchor_cols[:8],
            )
            if anchor_cols:
                # Row keeps if ANY anchor binary feature is True.
                mask = df_bin[anchor_cols].fillna(False).astype(bool).any(axis=1)
                n_match = int(mask.sum())
                log.info(
                    "_resolve_cohorts: Stage 1 brief filter — %d/%d rows match at least one anchor",
                    n_match, len(df_bin),
                )
                if n_match >= MIN_POSITIVES_FOR_STATS:
                    matched_idx = df_bin.index[mask]
                    df_bin = df_bin.loc[matched_idx]
                    df_raw = df_raw.loc[df_raw.index.intersection(matched_idx)]
                    log.info(
                        "_resolve_cohorts: brief filter APPLIED — df_bin=%d rows (Stage A mines within this pool)",
                        len(df_bin),
                    )
                else:
                    log.warning(
                        "_resolve_cohorts: brief filter would shrink pool to %d (<%d) — keeping unfiltered pool",
                        n_match, MIN_POSITIVES_FOR_STATS,
                    )
            else:
                log.info(
                    "_resolve_cohorts: brief filter — no binary anchor columns matched keywords; keeping unfiltered pool"
                )
        else:
            log.info("_resolve_cohorts: no usable brief keywords — skipping Stage 1 filter")

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

    # Compute LLM-derived anchor columns up front so the ICP-fallback path
    # (zero-positives, sparse mode, post-Stage-A-empty) can synthesize a
    # cohort even when statistical Stage A can't run.
    title_anchor_cols = base_role_feature_columns(base_role_titles, list(df_bin.columns))
    skill_anchor_cols = required_skill_feature_columns(
        derived_icp.get("required_skills", []), list(df_bin.columns),
    )
    base_role_cols = title_anchor_cols + [c for c in skill_anchor_cols if c not in title_anchor_cols]

    def _try_icp_fallback(reason: str) -> list:
        """Synthesize a single Cohort from the LLM-derived ICP's anchor
        columns. Used when statistical Stage A is unavailable (zero positives,
        sparse mode, or Stage A produced no cohorts). Returns [Cohort] when
        the LLM gave us anchors that exist in the corpus; [] otherwise.

        Originally added 2026-05-20 for the GMR-0021 post-Stage-A-empty path.
        Extended 2026-05-22 to also fire on the zero-positives and sparse-mode
        branches — previously those paths returned empty without ever calling
        the fallback, leaving ramps stuck at 0 cohorts whenever 1-29
        contributors had passed screening."""
        if not base_role_cols:
            log.warning(
                "_resolve_cohorts: ICP-fallback unavailable (no LLM-derived "
                "base-role / skill anchors matched the corpus) — reason=%s",
                reason,
            )
            return []
        from src.analysis import Cohort
        n_pos = (
            int(df_bin[target_col].sum())
            if target_col in df_bin.columns else 0
        )
        n_total = len(df_bin)
        anchor_cols = base_role_cols[:3]
        synthetic_rules = [(col, 1) for col in anchor_cols]
        synthetic_label = (
            derived_icp.get("derived_tg_label") or
            derived_icp.get("derived_label") or
            "ICP fallback cohort"
        )
        log.warning(
            "_resolve_cohorts: ICP-fallback synthesized 1 cohort %r with "
            "rules=%s — reason=%s. Targeting will be looser than a Stage-A-"
            "derived cohort.",
            synthetic_label, [r[0] for r in synthetic_rules], reason,
        )
        return [Cohort(
            name=synthetic_label,
            rules=synthetic_rules,
            n=n_total,
            passes=n_pos,
            pass_rate=(n_pos / n_total) if n_total else 0.0,
            lift_pp=0.0,
            p_value=1.0,
            score=1.0,
            support=n_pos,
            coverage=(n_pos / n_total) if n_total else 0.0,
        )]

    # ── Branch: stats mode vs sub-threshold ICP fallback ──────────────────
    # Pre-2026-05-22: n_icp==0 returned empty; 0<n_icp<30 returned empty.
    # Both branches now call the ICP-fallback so the LLM-derived cohort
    # carries the ramp through Stage C → copy gen → campaign creation.
    # When the fallback also can't help (no LLM anchors), still return empty.
    data_driven_exclude_pairs: list = []
    skip_stage_b = False

    if n_icp < MIN_POSITIVES_FOR_STATS:
        # Covers BOTH n_icp == 0 (cold start) and 0 < n_icp < 30 (sparse).
        # Stats Stage A wouldn't find statistically valid signal in either
        # case, so jump straight to the fallback.
        reason = "zero_positives" if n_icp == 0 else "sparse_mode"
        log.warning(
            "_resolve_cohorts: n_icp=%d (<%d) — %s; attempting ICP-fallback",
            n_icp, MIN_POSITIVES_FOR_STATS, reason,
        )
        cohorts_a = _try_icp_fallback(reason=reason)
        if not cohorts_a:
            # n_icp < 30 IS the cold-start regime (not just exact-zero). The
            # in-frame fallback found no LLM skill anchors in the corpus, so
            # fall back to the frame-independent job-post cold start instead of
            # returning 0 cohorts (which left the console ICP/cohorts/angles
            # cards empty).
            return _resolve_cold_start_cohort(
                row, snowflake=snowflake, li_client=li_client, urn_res=urn_res,
                project_id=project_id, flow_id=flow_id, location=location,
            )
        # The synthetic cohort has no statistical lift signal; Stage B (which
        # computes additional lift facets) would produce nonsense. Skip it.
        skip_stage_b = True
    else:
        # Stats mode — original path.
        neg_hits = stage_a_negative(df_bin, bin_cols, target_col)
        for h in neg_hits:
            pair = feature_col_to_exclude_pair(h["feature"])
            if pair:
                data_driven_exclude_pairs.append(pair)
        cohorts_a = stage_a(df_bin, bin_cols, target_col=target_col, base_role_cols=base_role_cols)
    if not cohorts_a:
        # Stage A produced nothing — try the same LLM-derived ICP-fallback
        # the sub-threshold branches use. Originally surfaced 2026-05-20 for
        # GMR-0021 (96% pass-rate → univariate analysis finds no further lift).
        # Skip Stage B too, since the synthetic cohort has no statistical
        # lift signal for stage_b to enrich.
        log.warning("_resolve_cohorts: Stage A returned no cohorts — attempting ICP-fallback")
        cohorts_a = _try_icp_fallback(reason="stage_a_empty")
        if not cohorts_a:
            # In-frame fallback also empty → job-post cold start (last resort).
            return _resolve_cold_start_cohort(
                row, snowflake=snowflake, li_client=li_client, urn_res=urn_res,
                project_id=project_id, flow_id=flow_id, location=location,
            )
        skip_stage_b = True

    # Stage B (skip when Stage A came from support mode OR was bypassed via
    # the ICP-fallback path for zero-positives / sparse / Stage-A-empty).
    from_support_mode = any(getattr(c, "support", 0) > 0 for c in cohorts_a)
    cohorts_b = (
        cohorts_a if (from_support_mode or skip_stage_b)
        else stage_b(df_bin, cohorts_a, target_col=target_col)
    )

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

    # ── Prestige tiering signal (Pranav rule 2026-04-29) ──
    # If ≥50% of the chosen tier's positives studied at top-tier universities or
    # worked at top-tier companies, surface a "prestige applies" signal so
    # downstream consumers (copy gen, photo subject, future LinkedIn school-facet
    # targeting) can lean into prestige cues. Otherwise ignore. Best-effort:
    # missing prestige columns / fetch failures degrade silently.
    prestige_signal: dict = {}
    try:
        from src.profile_tiering import compute_prestige_signal
        positives_mask = df_bin[target_col].fillna(False).astype(bool)
        positive_cb_ids = df_bin.loc[positives_mask, "user_id"].dropna().astype(str).tolist() \
            if "user_id" in df_bin.columns else []
        if positive_cb_ids and hasattr(snowflake, "fetch_prestige_columns"):
            prestige_df = snowflake.fetch_prestige_columns(positive_cb_ids)
            if not prestige_df.empty:
                prestige_signal = compute_prestige_signal(
                    prestige_df,
                    country_hint=(location or "").lower() or None,
                )
                log.info(
                    "_resolve_cohorts: prestige signal — %s",
                    prestige_signal.get("summary", "(no summary)"),
                )
        else:
            log.debug(
                "_resolve_cohorts: prestige signal skipped (no positives or client lacks fetch_prestige_columns)",
            )
    except Exception as exc:
        log.warning("_resolve_cohorts: prestige signal computation failed (non-fatal): %s", exc)

    # ── Requirement commonality (Pranav rule 2026-04-29) ──
    # For each requirement extracted from the Smart Ramp brief / job post,
    # check what % of the chosen tier's positives have it in their resume.
    # Surfaces hard_filter / soft_hint / drop recommendations so downstream
    # can promote dominant signals to Stage A facet anchors and keep rare
    # ones as copy-only hints. Phase 1 ships the signal; Stage A actually
    # CONSUMING it is Phase 2 (separate work).
    requirement_commonality: list[dict] = []
    try:
        from src.profile_tiering import compute_requirement_commonality
        # Pull requirements from derived_icp + Smart Ramp brief words.
        candidate_reqs: list[str] = []
        candidate_reqs.extend(derived_icp.get("required_skills", []) or [])
        tg_label = (derived_icp.get("derived_tg_label") or "").strip()
        if tg_label:
            # Split label like "Pediatric Cardiologists (India)" → ["Pediatric Cardiologists", "India"]
            for token in tg_label.replace("(", " ").replace(")", " ").replace(",", " ").split():
                t = token.strip()
                if len(t) >= 4:
                    candidate_reqs.append(t)
        # Also seed from Smart Ramp brief (each meaningful word ≥4 chars)
        for token in smart_ramp_brief.replace(",", " ").split():
            t = token.strip().strip(".:;")
            if len(t) >= 5 and t.lower() not in {"with", "from", "have", "their", "they", "this", "that", "want"}:
                candidate_reqs.append(t)
        # Dedupe while preserving order
        seen: set[str] = set()
        uniq_reqs: list[str] = []
        for r in candidate_reqs:
            k = r.lower()
            if k not in seen:
                seen.add(k)
                uniq_reqs.append(r)
        # Cap at 12 — too many spammed requirements would noise the signal.
        uniq_reqs = uniq_reqs[:12]

        if uniq_reqs and positive_cb_ids and hasattr(snowflake, "fetch_signal_columns"):
            signal_df = snowflake.fetch_signal_columns(positive_cb_ids)
            if not signal_df.empty:
                requirement_commonality = compute_requirement_commonality(signal_df, uniq_reqs)
                # Log a one-line summary per requirement — show 1 decimal so
                # borderline rates (e.g. 9.7%) aren't visually rounded to 10%
                # which falsely looks like the soft_hint threshold was met.
                for rec in requirement_commonality:
                    log.info(
                        "_resolve_cohorts: requirement %r (stem=%r) → %d/%d (%.1f%%) — %s",
                        rec["requirement"], rec.get("stem", ""),
                        rec["n_hits"], rec["n_total"],
                        rec["hit_rate"] * 100, rec["recommended_action"],
                    )
        else:
            log.debug(
                "_resolve_cohorts: requirement commonality skipped (no requirements / positives / fetch method)"
            )
    except Exception as exc:
        log.warning("_resolve_cohorts: requirement commonality computation failed (non-fatal): %s", exc)

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

    # ── ICP enrichment (LLM step) ────────────────────────────────────────────
    # For each selected cohort, derive a structured ICP (motivations, content
    # prefs, creative_liberty, decision_drivers) from the cohort rules + a
    # resume sample matching those rules. Persisted to Postgres `cohort_icp`
    # so the brief agent + console can consume it. Best-effort: any failure
    # logs and continues — never blocks cohort selection.
    ramp_id_for_icp = (row or {}).get("ramp_id") or ""

    # Wipe ghost rows from prior prep/launch runs whose cohort_signature is
    # no longer in the current selection. Without this, stale signatures
    # accumulate (the ICP card shows cohorts that aren't part of today's
    # selection; AnglesCard renders duplicate cards per ghost signature).
    #
    # Strategy:
    #   - cohort_icp + cohort_audience: ALWAYS re-persisted later in this
    #     function, so we can wipe-all and rebuild.
    #   - cohort_brief_rationale: only re-persisted during the LAUNCH flow
    #     (inside _process_static_campaigns → _persist_cohort_rationales),
    #     NOT during prep_only. Wiping it here in prep_only would orphan
    #     valid rationale rows from a prior launch. So we wipe ONLY orphan
    #     signatures (those NOT in the current selection) — same-cohort
    #     rationale survives across prep runs.
    current_sigs = sorted({getattr(c, "name", "") for c in selected if getattr(c, "name", None)})
    # ONLY_COHORT (feature 010): scoped per-cohort run is purely ADDITIVE — never
    # wipe the ramp's other cohorts' icp/audience/rationale. The upserts below
    # are ON-CONFLICT keyed, so only the target cohort's rows are touched.
    if ramp_id_for_icp and config.ONLY_COHORT:
        log.info("_resolve_cohorts: ONLY_COHORT=%s — skipping ramp-wide cohort-row wipe (additive)", config.ONLY_COHORT)
    elif ramp_id_for_icp:
        try:
            from src.ui_decisions import _connect, UIDecisionsUnavailable
            with _connect() as conn, conn.cursor() as cur:
                cur.execute("DELETE FROM cohort_icp WHERE ramp_id = %s", (ramp_id_for_icp,))
                cur.execute("DELETE FROM cohort_audience WHERE ramp_id = %s", (ramp_id_for_icp,))
                if current_sigs:
                    # psycopg3: `IN %s` + tuple renders as one param ($2 syntax
                    # error). `<> ALL(%s)` + list adapts to a Postgres array and
                    # is equivalent to NOT IN for non-null cohort_signature.
                    cur.execute(
                        "DELETE FROM cohort_brief_rationale "
                        "WHERE ramp_id = %s AND cohort_signature <> ALL(%s)",
                        (ramp_id_for_icp, list(current_sigs)),
                    )
                else:
                    # No cohorts mined this run → wipe everything for the ramp.
                    cur.execute(
                        "DELETE FROM cohort_brief_rationale WHERE ramp_id = %s",
                        (ramp_id_for_icp,),
                    )
                conn.commit()
                log.info(
                    "_resolve_cohorts: cleared prior icp + audience for ramp=%s, "
                    "wiped orphan rationale (kept signatures: %s)",
                    ramp_id_for_icp, current_sigs or "(none — all wiped)",
                )
        except Exception as exc:
            log.warning("_resolve_cohorts: prior-row cleanup skipped (non-fatal): %s", exc)

    if ramp_id_for_icp and selected:
        try:
            from src.icp_enrichment import enrich as enrich_icp
            from src.ui_decisions import upsert_cohort_icp
            for cohort in selected:
                # Sample df_bin rows matching this cohort's rules. df_bin has
                # one column per (feature__value) pair set to True/False; the
                # rules list is the same shape so filtering is a row-wise AND.
                mask = None
                for feat, val in (getattr(cohort, "rules", []) or []):
                    if feat in df_bin.columns:
                        col_mask = df_bin[feat].fillna(False).astype(bool) == bool(val)
                        mask = col_mask if mask is None else (mask & col_mask)
                sample_rows: list[dict] = []
                if mask is not None and "user_id" in df_bin.columns:
                    matched_ids = df_bin.loc[mask, "user_id"].dropna().astype(str).tolist()[:10]
                    if matched_ids and hasattr(snowflake, "fetch_resume_summary"):
                        try:
                            sample_df = snowflake.fetch_resume_summary(matched_ids)
                            if sample_df is not None and not sample_df.empty:
                                sample_rows = sample_df.to_dict(orient="records")
                        except Exception as exc:
                            log.debug("ICP resume sample fetch failed: %s", exc)
                locale_hint = None
                if location:
                    locale_hint = f"en-{location.upper()[:2]}" if len(location) >= 2 else None
                icp = enrich_icp(cohort, resume_sample=sample_rows, locale_hint=locale_hint)
                cohort._icp = icp  # stash on the cohort for downstream consumers (brief agent)
                upsert_cohort_icp(
                    ramp_id=ramp_id_for_icp,
                    cohort_id=getattr(cohort, "_stg_id", "") or "",
                    cohort_signature=getattr(cohort, "name", "") or "",
                    icp_dict=icp.to_dict(),
                )
                log.info(
                    "_resolve_cohorts: ICP persisted ramp=%s cohort=%s liberty=%s lang=%s motivations=%d",
                    ramp_id_for_icp,
                    getattr(cohort, "name", "?"),
                    icp.creative_liberty, icp.language_pref,
                    len(icp.top_motivations),
                )
        except Exception as exc:
            log.warning("_resolve_cohorts: ICP enrichment block failed (non-fatal): %s", exc)

    # ── Per-channel audience measurement (prep-time) ─────────────────────────
    # Measures LinkedIn (already done by Stage C), Meta, and Google audience
    # size for each selected cohort and persists to Postgres cohort_audience.
    # Lets the console show per-channel AudienceBadge BEFORE approval. Without
    # this, only LinkedIn's number survives prep (in memory, then discarded);
    # Meta + Google audiences only get measured during launch.
    if ramp_id_for_icp and selected:
        try:
            from src.prep_audience import measure_audience_for_cohort
            from src.ui_decisions import upsert_cohort_audience, upsert_cohort_targeting
            enabled = [p.strip().lower() for p in (config.ENABLED_PLATFORMS or "").split(",") if p.strip()]
            if not enabled:
                enabled = ["linkedin", "meta", "google"]
            included_geos = list((row or {}).get("included_geos") or [])
            for cohort in selected:
                rows = measure_audience_for_cohort(
                    cohort,
                    included_geos=included_geos,
                    enabled_platforms=enabled,
                    li_audience_size=getattr(cohort, "audience_size", None),
                )
                for ca in rows:
                    upsert_cohort_audience(
                        ramp_id=ramp_id_for_icp,
                        cohort_id=getattr(cohort, "_stg_id", "") or "",
                        cohort_signature=getattr(cohort, "name", "") or "",
                        platform=ca.platform,
                        audience_size=ca.audience_size,
                        status=ca.status,
                        geos_used=ca.geos_used,
                        rules_dropped=ca.rules_dropped,
                        forecast=ca.forecast,
                    )
                    upsert_cohort_targeting(
                        ramp_id=ramp_id_for_icp,
                        cohort_id=getattr(cohort, "_stg_id", "") or "",
                        cohort_signature=getattr(cohort, "name", "") or "",
                        platform=ca.platform,
                        facets=ca.facets,
                    )
                log.info(
                    "_resolve_cohorts: audience persisted ramp=%s cohort=%s %s",
                    ramp_id_for_icp,
                    getattr(cohort, "name", "?"),
                    " ".join(f"{r.platform}={r.audience_size}({r.status})" for r in rows),
                )
        except Exception as exc:
            log.warning("_resolve_cohorts: per-channel audience block failed (non-fatal): %s", exc)

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
        prestige_signal=prestige_signal,
        smart_ramp_brief=copy_description_hint,
        requirement_commonality=requirement_commonality,
    )


def _save_creative_to_drive(
    png_path,
    ramp_id: str,
    unique_id: str,
    channel: str,         # "linkedin", "meta", "google"
    angle: str,           # "A", "B", "C", ...
    cohort_geo: str = "", # "<cohort_stg_id>__<geo_cluster>"
) -> str:
    """Upload PNG to the Shared Drive hierarchy
    `<ramp_id>/<channel>/<cohort_geo>/<angle>.png`. The folders are find-or-
    created on each run. Falls back to legacy flat upload when cohort_geo is
    empty (callers that don't have cohort context, e.g. registry image embed).

    Returns the Drive web view URL on success, empty string on failure.
    """
    from src.gdrive import upload_creative_in_hierarchy, upload_creative

    if not config.GDRIVE_ENABLED:
        log.warning("GDRIVE_ENABLED=false — skipping Drive upload for %s", unique_id)
        return ""

    try:
        if cohort_geo:
            url = upload_creative_in_hierarchy(
                file_path=Path(str(png_path)),
                ramp_id=ramp_id or "no_ramp",
                channel=channel or "linkedin",
                cohort_geo=cohort_geo,
                angle=angle or "creative",
            )
            log.info("Creative uploaded to Drive: %s/%s/%s/%s.png → %s",
                     ramp_id, channel, cohort_geo, angle, url)
            return url
        # Legacy flat path (no cohort context — keep filename unique).
        target_name = f"{unique_id}_{angle}.png"
        temp_path = Path(tempfile.mktemp(suffix=f"_{target_name}"))
        shutil.copy2(str(png_path), str(temp_path))
        url = upload_creative(temp_path)
        log.info("Creative uploaded to Drive (flat): %s → %s", target_name, url)
        return url
    except Exception as e:
        log.error("Failed to upload creative to Drive: %s", e)
        return ""


def _cohort_geo_label(cohort, geo_group) -> str:
    """Stable, filesystem-safe folder name for a (cohort × geo cluster).

    Format: `<stg_id>__<cluster>` — uses the Stage cohort id (short, unique
    per run) plus the geo cluster slug (anglo / south_asian / etc). Both
    are alphanumeric-friendly so no extra sanitisation needed.
    """
    stg = getattr(cohort, "_stg_id", None) or getattr(cohort, "id", "cohort")
    cluster = getattr(geo_group, "cluster", "global")
    return f"{stg}__{cluster}"


def _persist_cohort_rationales(
    *,
    ramp_id: str,
    cohort,
    geo_cluster: str,
    channel: str,
    variants: list[dict],
) -> None:
    """Phase 5 — best-effort persistence of per-angle rationale + a snapshot
    of the produced copy into Postgres. Each variant becomes one row in
    `cohort_brief_rationale` keyed by (ramp_id, cohort_id, channel, angle,
    geo_cluster). Silent on UIDecisionsUnavailable so a Postgres outage
    never blocks copy gen.

    `variants` come from build_copy_variants / build_inmail_variants —
    accepts either dicts (static) or InMailVariant dataclasses by
    duck-typing the field reads.
    """
    if not (ramp_id and variants):
        return
    try:
        from src.ui_decisions import upsert_cohort_brief_rationale
    except ImportError:
        return
    cohort_id = getattr(cohort, "_stg_id", "") or getattr(cohort, "id", "") or ""
    cohort_signature = getattr(cohort, "name", "") or ""
    for v in variants:
        # Static variants are dicts; InMail variants are dataclasses.
        def _g(k: str) -> str:
            if isinstance(v, dict):
                return str(v.get(k, "") or "")
            return str(getattr(v, k, "") or "")
        angle = _g("angle")
        if not angle:
            continue
        # Optional expected_uplift_pp parsed as float when present.
        uplift_raw = _g("expected_uplift_pp")
        try:
            uplift_val = float(uplift_raw) if uplift_raw not in ("", None) else None
        except (TypeError, ValueError):
            uplift_val = None
        try:
            upsert_cohort_brief_rationale(
                ramp_id=ramp_id,
                cohort_id=cohort_id,
                cohort_signature=cohort_signature,
                geo_cluster=geo_cluster or None,
                channel=channel,
                angle=angle,
                angle_label=_g("angleLabel") or _g("angle_label") or None,
                headline=_g("headline") or None,
                subheadline=_g("subheadline") or _g("subject") or None,
                photo_subject=_g("photo_subject") or None,
                rationale=_g("rationale") or None,
                competitor_signal=_g("competitor_signal") or None,
                expected_uplift_pp=uplift_val,
            )
        except Exception as _exc:
            log.debug(
                "_persist_cohort_rationales: row write failed (non-fatal) ramp=%s cohort=%s angle=%s: %s",
                ramp_id, cohort_id, angle, _exc,
            )


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
    cohort_description: str = "",
    base_rate_usd: float | None = None,
    rate_geo_specific: bool = False,
    unique_id: str | None = None,
    naming_meta: dict | None = None,
    seen_keys: set | None = None,
    daily_budget_cents: int | None = None,
    create_linkedin_campaigns: bool = True,
) -> dict:
    """Static-ad arm — symmetric counterpart to _process_inmail_campaigns.

    Architecturally has two distinct phases:
      (A) spec generation — image gen + copy variants + geo clusters; produces
          `campaign_specs[]`. Platform-agnostic; Meta + Google arms consume it.
      (B) LinkedIn API calls — campaign group, campaigns, DSC posts, creatives.

    `create_linkedin_campaigns` (default True) gates Phase B. Pass False to run
    Phase A only — used when channels excludes LinkedIn but Meta/Google still
    need the specs. Previously a hidden env-var (LINKEDIN_CAMPAIGN_CREATE_DISABLED);
    now explicit so callers can see the dependency in the signature.

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
    from src.geo_tiers import group_geos_for_campaigns, filter_blocked_geos

    family_exclude_pairs = family_exclude_pairs or []
    data_driven_exclude_pairs = data_driven_exclude_pairs or []
    raw_geos = included_geos or []

    # ── Per-geo group splitting (2026-05-04) ──────────────────────────────────
    # Split included_geos into ethnic creative clusters, each getting its own
    # LinkedIn campaign with geo-appropriate photo_subject + computed rate.
    # G4 blocked countries are strictly skipped here.
    geo_groups = group_geos_for_campaigns(raw_geos, base_rate_usd, apply_geo_multiplier=not rate_geo_specific)
    if not geo_groups and raw_geos:
        # All geos were G4 — fall back to single group with empty geos (global)
        log.warning("_process_static_campaigns: all included_geos are G4 blocked — creating global campaign")
        from src.geo_tiers import GeoCampaignGroup
        geo_groups = [GeoCampaignGroup(
            cluster="global_mix", cluster_label="Global", geos=[],
            median_multiplier=1.0, advertised_rate=_fmt_advertised_rate(base_rate_usd),
            campaign_suffix="global",
        )]
    if not geo_groups:
        # No geos at all — single global campaign, existing behavior
        from src.geo_tiers import GeoCampaignGroup
        geo_groups = [GeoCampaignGroup(
            cluster="global_mix", cluster_label="Global", geos=[],
            median_multiplier=1.0, advertised_rate=_fmt_advertised_rate(base_rate_usd),
            campaign_suffix="global",
        )]

    # ── Experimentation caps (Pranav rule 2026-05-05) ─────────────────────────
    # Max 3 cohorts per geo cluster × 3 angles each.
    # Feedback agent surfaces winners/losers; losing angles get deprecated.
    capped_cohorts = selected[:config.MAX_COHORTS_PER_GEO_CLUSTER]
    angle_labels   = ["A", "B", "C"][:config.ANGLES_PER_COHORT]
    log.info(
        "_process_static_campaigns: %d cohort(s) × %d geo group(s) × %d angle(s) = %d campaigns",
        len(capped_cohorts), len(geo_groups), len(angle_labels),
        len(capped_cohorts) * len(geo_groups) * len(angle_labels),
    )

    out_campaigns: list[str] = []
    by_cohort: dict[str, str] = {}
    creative_paths: dict[str, str] = {}

    if not capped_cohorts:
        return {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_specs": []}

    # Generate creatives per cohort × geo group × angle.
    # Phase 3.1 (2026-05-08): image gen is parallelized across angles via
    # ThreadPoolExecutor. Copy gen stays sequential per (cohort × geo) — it's
    # cheap (~5s) compared to image gen (~30-300s with QC reroll).
    import concurrent.futures as _cf

    has_figma = bool(figma_file and figma_node and claude_key)
    figma_client = FigmaCreativeClient() if has_figma else None
    # Flat list of specs — one entry per (cohort × geo_group × angle) = one LinkedIn campaign
    campaign_specs: list[dict] = []
    skip_image_gen = dry_run and not os.environ.get("WITH_IMAGES")

    # ── Phase A1: enumerate (cohort × geo) jobs in stable order (cohort-outer,
    # geo-inner). This order is the contract downstream consumers (the angle
    # fan-out in A3, the grouping at line ~2378, and _process_extra_platform_arm)
    # rely on, so it must not be re-shuffled by A2's parallel completion order.
    #
    # Cross-row dedup (run_launch_for_ramp path only): skip a (cohort ×
    # geo_cluster) tuple if a prior row in this ramp already enqueued it for
    # Static. See _cohort_geo_dedup_key. Skipping HERE (pre-copy-job) saves the
    # downstream Anthropic copy-gen, Gemini image-gen, QC, Drive upload, and
    # LinkedIn campaign creation for the duplicate. Legacy _process_row CLI
    # caller passes seen_keys=None → no-op.
    copy_jobs: list[dict] = []
    for cohort in capped_cohorts:
        for geo_group in geo_groups:
            if seen_keys is not None:
                _dedup_key = _cohort_geo_dedup_key(cohort.name, geo_group.cluster)
                if _dedup_key in seen_keys:
                    log.info(
                        "_process_static_campaigns: skipping (cohort=%r geo=%r) — "
                        "already produced by an earlier row in this ramp (cohort_id=%s)",
                        cohort.name, geo_group.cluster, cohort_id_override or "?",
                    )
                    continue
                seen_keys.add(_dedup_key)
            # Per-cohort idempotency: skip a (cohort × geo) that already has a
            # live LinkedIn static campaign so a re-launch only creates the new
            # cohorts (bypassed on replace). Saves the downstream copy/image gen.
            if _cohort_channel_already_live(ramp_id, "linkedin", "static", cohort, geo_group):
                log.info(
                    "_process_static_campaigns: skipping (cohort=%r geo=%r) — already has a "
                    "live LinkedIn campaign (idempotent re-launch; set "
                    "SKIP_EXISTING_COHORT_CAMPAIGNS=false or use replace to recreate)",
                    cohort.name, geo_group.cluster,
                )
                continue
            geo_label  = geo_group.cluster_label
            group_geos = geo_group.geos or raw_geos
            log.info(
                "_process_static_campaigns: cohort '%s' × geo_group '%s' (%s) rate=%s",
                cohort.name, geo_label, group_geos, geo_group.advertised_rate,
            )
            copy_jobs.append({
                "cohort":     cohort,
                "geo_group":  geo_group,
                "geo_label":  geo_label,
                "group_geos": group_geos,
            })

    # ── Phase A2: parallelize build_copy_variants across (cohort × geo).
    # build_copy_variants is one Anthropic call per job (~5 s) and is read-only
    # — confirmed no shared mutable state, file writes, or cohort mutation. The
    # shared anthropic.Anthropic client (src/claude_client.get_client) is
    # constructed under threading.Lock so concurrent first calls don't race.
    # Per-job exceptions are isolated: a single failed copy gen leaves that
    # job's variants=[] and the downstream angle loop falls through with
    # empty selected_variant (same path as today's single-threaded except).
    def _copy_one(job: dict) -> list[dict]:
        cohort     = job["cohort"]
        geo_group  = job["geo_group"]
        group_geos = job["group_geos"]
        try:
            layer_map = (
                figma_client.get_text_layer_map(figma_file, figma_node)
                if has_figma else {}
            )
            variants: list[dict] = []
            # 2026-05-22 brief-review path: if Postgres has cohort_briefs rows
            # for this (ramp × cohort × geo_cluster × linkedin), use the
            # Phase-2 builder so reviewer comments get honored. Falls through
            # to the legacy single-phase build_copy_variants when no briefs
            # exist (older ramps, dry-runs, prep_only=False CLI path).
            if ramp_id:
                try:
                    from src.ui_decisions import list_briefs_for_ramp
                    from src.brief_generator import build_copy_from_brief
                    all_briefs = list_briefs_for_ramp(ramp_id, channel="linkedin")
                    matching = [
                        b for b in all_briefs
                        if b.cohort_signature == cohort.name
                        and b.geo_cluster == (geo_group.cluster or "global_mix")
                    ]
                    if matching:
                        log.info(
                            "Static copy: %d brief(s) found in Postgres for "
                            "cohort=%s geo=%s — running Phase-2 (build_copy_from_brief)",
                            len(matching), cohort.name, geo_group.cluster,
                        )
                        for b in matching:
                            v = build_copy_from_brief(
                                b.brief,
                                layer_map=layer_map,
                                cohort=cohort,
                                geos=group_geos,
                                hourly_rate=geo_group.advertised_rate,
                                reviewer_comment=b.reviewer_comment or "",
                                task_card=cached_card(ramp_id, cohort_id_override),
                            )
                            if v:
                                variants.append(v)
                except Exception as exc:
                    log.warning(
                        "Brief-based copy gen failed for cohort=%s geo=%s: %s — "
                        "falling back to legacy build_copy_variants",
                        cohort.name, geo_group.cluster, exc,
                    )
                    variants = []
            if not variants:
                variants = build_copy_variants(
                    cohort, layer_map,
                    geos=group_geos,
                    description_hint=cohort_description,
                    hourly_rate=geo_group.advertised_rate,
                    geo_icp_hint=geo_group.icp_hint,
                    icp=getattr(cohort, "_icp", None),
                )
            # Phase 5 — persist per-angle rationale so the console can render
            # "Angles we'd test" above the timeline. Best-effort: failures
            # never abort copy gen.
            if ramp_id and variants:
                _persist_cohort_rationales(
                    ramp_id=ramp_id,
                    cohort=cohort,
                    geo_cluster=geo_group.cluster,
                    channel="linkedin",
                    variants=variants,
                )
            return variants
        except Exception as exc:
            log.warning("Static copy generation failed for '%s' / '%s': %s (%s)",
                        cohort.name, job["geo_label"], exc, type(exc).__name__,
                        exc_info=True)
            return []

    variants_by_idx: dict[int, list[dict]] = {}
    if not copy_jobs:
        pass
    elif config.COPY_GEN_CONCURRENCY <= 1:
        # Sequential fallback (also avoids thread overhead for tiny ramps).
        for i, job in enumerate(copy_jobs):
            variants_by_idx[i] = _copy_one(job)
    else:
        log.info(
            "_process_static_campaigns: dispatching %d copy-gen jobs "
            "with concurrency=%d",
            len(copy_jobs), config.COPY_GEN_CONCURRENCY,
        )
        with _cf.ThreadPoolExecutor(
            max_workers=config.COPY_GEN_CONCURRENCY,
            thread_name_prefix="static-copygen",
        ) as ex:
            fut_to_idx = {ex.submit(_copy_one, j): i for i, j in enumerate(copy_jobs)}
            for fut in _cf.as_completed(fut_to_idx):
                idx = fut_to_idx[fut]
                try:
                    variants_by_idx[idx] = fut.result()
                except Exception as exc:
                    # _copy_one already swallows, but a future raised by other
                    # paths (executor shutdown, etc.) shouldn't abort the arm.
                    log.exception(
                        "_process_static_campaigns: unexpected copy-gen exception "
                        "idx=%d: %s", idx, exc,
                    )
                    variants_by_idx[idx] = []

    # ── Phase A3: fan out to (cohort × geo × angle) image-gen tasks. The
    # cohort-outer/geo-inner/angle-innermost order matches the pre-3.2 layout
    # so Phase B (image-gen executor below) and downstream campaign_specs
    # consumers see the identical structure.
    tasks: list[dict] = []
    for i, job in enumerate(copy_jobs):
        variants = variants_by_idx.get(i, [])
        for angle_label in angle_labels:
            tasks.append({
                "cohort":      job["cohort"],
                "geo_group":   job["geo_group"],
                "group_geos":  job["group_geos"],
                "geo_label":   job["geo_label"],
                "angle_idx":   angle_labels.index(angle_label),
                "angle_label": angle_label,
                "variants":    variants,
            })

    # ── Phase B: parallelize image gen across all (cohort × geo × angle)
    # tasks via ThreadPoolExecutor. Gemini calls are I/O-bound (HTTP), so
    # threads suffice (no asyncio rewrite needed). Concurrency is bounded
    # by config.IMAGE_GEN_CONCURRENCY (default 4) to keep below Gemini's
    # rate limits. Each task is independent — exceptions are isolated.
    def _gen_one(task: dict) -> dict:
        cohort       = task["cohort"]
        geo_group    = task["geo_group"]
        geo_label    = task["geo_label"]
        angle_idx    = task["angle_idx"]
        angle_label  = task["angle_label"]
        variants     = task["variants"]
        group_geos   = task["group_geos"]
        selected_variant = variants[angle_idx] if angle_idx < len(variants) else {}
        png_path: Path | None = None
        qc_report: dict = {}

        if not skip_image_gen:
            if has_figma and variants:
                try:
                    tg_label = variants[0].get("tg_label", cohort.name) if variants else cohort.name
                    clone_ids = apply_plugin_logic(figma_file, figma_node, variants, tg_label, claude_key)
                    if clone_ids:
                        selected_id = clone_ids[angle_idx % len(clone_ids)]
                        pngs = figma_client.export_clone_pngs(figma_file, [selected_id])
                        png_path = pngs[0] if pngs else None
                except Exception as exc:
                    log.warning("Static Figma path failed for '%s': %s — falling back to Gemini",
                                cohort.name, exc)

            if png_path is None and selected_variant:
                try:
                    from src.figma_creative import rewrite_variant_copy, repair_photo_subject
                    # Carry the resolved rate so the creative's bottom band shows
                    # the real figure (not a hardcoded range) on angles whose
                    # subheadline leads with a non-rate hook.
                    selected_variant["advertised_rate"] = geo_group.advertised_rate
                    png_path, qc_report = generate_imagen_creative_with_qc(
                        variant=selected_variant,
                        copy_rewriter=rewrite_variant_copy,
                        subject_repairer=repair_photo_subject,
                    )
                    if qc_report and qc_report.get("verdict") == "FAIL":
                        log.error(
                            "Static QC FAIL for '%s' / '%s' after %d attempts; REJECTING creative. "
                            "Violations: %s",
                            cohort.name, geo_label,
                            qc_report.get("attempts", 1),
                            qc_report.get("violations", []),
                        )
                        png_path = None
                except Exception as exc:
                    log.warning("Static Gemini path failed for '%s' / '%s': %s",
                                cohort.name, geo_label, exc)
                    png_path = None
                    qc_report = {}

        return {
            "cohort":      cohort,
            "geo_group":   geo_group,
            "group_geos":  group_geos,
            "angle_idx":   angle_idx,
            "angle_label": angle_label,
            "variants":    variants,
            "png_path":    png_path,
            "qc_report":   qc_report,
        }

    if not tasks:
        pass  # nothing to gen; campaign_specs stays empty
    elif config.IMAGE_GEN_CONCURRENCY <= 1 or skip_image_gen:
        # Sequential fallback (also avoids thread overhead in dry-run mode
        # where image gen is a no-op).
        for t in tasks:
            campaign_specs.append(_gen_one(t))
    else:
        log.info(
            "_process_static_campaigns: dispatching %d image-gen tasks "
            "with concurrency=%d",
            len(tasks), config.IMAGE_GEN_CONCURRENCY,
        )
        # Preserve task order in campaign_specs by indexing futures.
        results: dict[int, dict] = {}
        with _cf.ThreadPoolExecutor(
            max_workers=config.IMAGE_GEN_CONCURRENCY,
            thread_name_prefix="static-imggen",
        ) as ex:
            future_to_idx = {ex.submit(_gen_one, t): i for i, t in enumerate(tasks)}
            for fut in _cf.as_completed(future_to_idx):
                idx = future_to_idx[fut]
                try:
                    results[idx] = fut.result()
                except Exception as exc:
                    # Should not happen — _gen_one already swallows errors —
                    # but if it does, fall back to a spec with no image so the
                    # caller can decide to skip the angle rather than abort
                    # the whole arm.
                    t = tasks[idx]
                    log.exception(
                        "_process_static_campaigns: unexpected exception in image-gen "
                        "task cohort=%s geo=%s angle=%s — %s",
                        t["cohort"].name, t["geo_label"], t["angle_label"], exc,
                    )
                    results[idx] = {
                        "cohort":      t["cohort"],
                        "geo_group":   t["geo_group"],
                        "group_geos":  t["group_geos"],
                        "angle_idx":   t["angle_idx"],
                        "angle_label": t["angle_label"],
                        "variants":    t["variants"],
                        "png_path":    None,
                        "qc_report":   {},
                    }
        for i in range(len(tasks)):
            campaign_specs.append(results[i])

    if dry_run:
        log.info(
            "[dry-run] _process_static_campaigns: skipping LinkedIn calls "
            "(%d cohorts × %d geo groups × %d angles = %d specs)",
            len(capped_cohorts), len(geo_groups), len(angle_labels), len(campaign_specs),
        )
        return {
            "campaigns":            [],
            "campaigns_by_cohort":  {},
            "creative_paths":       {},
            "campaign_specs":       campaign_specs,  # multi-platform arms reuse these
        }

    # Phase B gate (explicit) — when create_linkedin_campaigns is False the
    # caller wants spec generation only (Meta/Google consume the same
    # campaign_specs without us shipping LinkedIn drafts). Previously this
    # path was triggered by LINKEDIN_CAMPAIGN_CREATE_DISABLED env; now
    # explicit so the dependency is visible in the function signature.
    if not create_linkedin_campaigns:
        log.info(
            "_process_static_campaigns: create_linkedin_campaigns=False — "
            "skipping LinkedIn API calls. campaign_specs (%d) still produced "
            "for Meta + Google arms.",
            len(campaign_specs),
        )
        return {
            "campaigns":            [],
            "campaigns_by_cohort":  {},
            "creative_paths":       {},
            "campaign_specs":       campaign_specs,
            "campaign_groups":      [],
        }

    # Create campaign group + campaigns — one per (cohort × geo_group) spec.
    # Group-level name: Smart Ramp v2 spec when naming_meta is available;
    # legacy "Outlier <flow_id> <location> Static" otherwise.
    if naming_meta is not None:
        from src.campaign_name import build_campaign_name as _build_grp_name
        group_name = _build_grp_name(
            ramp_id=ramp_id or "",
            submitted_at=naming_meta.get("submitted_at", ""),
            cohort=None,
            platform="linkedin",
            campaign_type="static",
            format_override="Single Image Group",
            pod=naming_meta.get("pod"),
            domain=naming_meta.get("domain"),
            locale=naming_meta.get("locale"),
            included_geos=naming_meta.get("included_geos"),
            campaign_state=naming_meta.get("campaign_state"),
        )
    else:
        group_name = f"Outlier {flow_id} {location} Static".strip()
    # Shared "agent" staging group (see _process_inmail_campaigns / GMR-0024
    # reviewer feedback). `group_name` retained for registry/logging only.
    group_urn = li_client.get_or_create_staging_group()
    out_groups = [group_urn]
    log.debug("_process_static_campaigns: group=%s", group_urn)

    default_exclude_urns = urn_res.resolve_default_excludes()
    family_exclude_urns = urn_res.resolve_facet_pairs(family_exclude_pairs)
    data_driven_exclude_urns = urn_res.resolve_facet_pairs(data_driven_exclude_pairs)
    shared_exclude_urns = _merge_urn_dicts(
        default_exclude_urns, family_exclude_urns, data_driven_exclude_urns,
    )

    from src.campaign_registry import log_campaign as _reg_log
    from src.ui_decisions import upsert_launch_progress as _lp
    from src.ui_decisions import next_generation as _next_gen

    # ── Group specs by (cohort × geo_group) so each (cohort × geo) becomes
    # ONE LinkedIn campaign with multiple creatives attached (one per angle).
    # Hierarchy: CampaignGroup → Campaign (per cohort × geo) → 3 Creatives
    grouped_specs: dict[tuple[str, str], list[dict]] = {}
    group_meta: dict[tuple[str, str], dict] = {}
    for spec in campaign_specs:
        key = (spec["cohort"]._stg_id, spec["geo_group"].cluster)
        grouped_specs.setdefault(key, []).append(spec)
        if key not in group_meta:
            group_meta[key] = {
                "cohort": spec["cohort"],
                "geo_group": spec["geo_group"],
                "group_geos": spec["group_geos"],
                "variants": spec["variants"],
            }

    log.info(
        "_process_static_campaigns: %d cohort × geo group(s) → 1 LinkedIn campaign each, "
        "with up to %d creatives per campaign (one per angle)",
        len(grouped_specs), config.ANGLES_PER_COHORT,
    )

    from src import launch_verify
    healed_empties: list[dict] = []
    for (stg_id, cluster), specs in grouped_specs.items():
        meta = group_meta[(stg_id, cluster)]
        cohort = meta["cohort"]
        geo_group = meta["geo_group"]
        group_geos = meta["group_geos"]
        variants = meta["variants"]
        geo_label = geo_group.cluster_label

        # Launch-progress telemetry (console "Launch status" view). Key fields
        # for this (channel × locale × cohort) unit; best-effort, never breaks.
        _lp_locale = (naming_meta.get("locale") if naming_meta else "") or ""
        _lp_cohort_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id
        _lp_kw = dict(
            ramp_id=ramp_id or "", channel="linkedin", locale=_lp_locale,
            cohort_id=str(_lp_cohort_id or ""), cohort_signature=getattr(cohort, "name", ""),
            geo_cluster=geo_group.cluster,
        )
        _lp(**_lp_kw, status="queued")
        # Additive launch → a fresh generation (v2/v3…) that coexists with prior
        # ones; default launch stays generation 1 (unchanged behavior).
        _gen = (
            _next_gen(ramp_id=ramp_id or "", platform="linkedin", campaign_type="static",
                      cohort_signature=getattr(cohort, "name", ""), geo_cluster=geo_group.cluster)
            if getattr(config, "ADDITIVE_LAUNCH", False) else 1
        )

        # Per-(cohort × geo_group) isolation: failure in one combo never
        # aborts another. Each angle inside the combo is also isolated below.
        try:
            facet_urns = urn_res.resolve_cohort_rules(cohort.rules)
            if group_geos:
                facet_urns = _apply_geo_overrides(facet_urns, group_geos, urn_res)
            facet_urns = _apply_generalist_language_skill(facet_urns, cohort)

            # Cold-start cohorts bypass Stage C — guard against shipping a
            # geo-only (country-wide) static campaign when no skill/title facet
            # resolved (the GMR-0024 ~290M class). Mirrors the InMail arm guard.
            if linkedin_targeting_collapsed(cohort, facet_urns):
                log.warning(
                    "_process_static_campaigns: cohort '%s' geo=%s targeting collapsed to "
                    "geo-only (no skill/title facet resolved) — skipped to avoid a "
                    "country-wide spend. Needs human targeting.",
                    cohort.name, geo_group.cluster_label,
                )
                continue

            # Per-geo audience recheck (2026-05-20). Stage C's audience check
            # used the cohort's facet URNs without geo intersection. A cohort
            # that passes 50k globally may yield far less in a single-country
            # cluster (e.g. brazilian). Recheck with the geo-applied facets so
            # the registry captures the audience the campaign will actually be
            # served to. None on failure — UI renders an amber/unknown badge.
            geo_audience: int | None
            try:
                geo_audience = li_client.get_audience_count(facet_urns)
                log.info(
                    "Per-geo audience: cohort=%s geo=%s → %d",
                    cohort.name, geo_label, geo_audience,
                )
            except Exception as _aud_exc:
                log.warning(
                    "Per-geo audience count failed for cohort=%s geo=%s: %s",
                    cohort.name, geo_label, _aud_exc,
                )
                geo_audience = None

            # Smart Ramp v2 naming convention — see src/campaign_name.py.
            # Static path: one campaign per (cohort×geo) with 3 creatives, so
            # no angle suffix needed. Falls back to legacy form when naming_meta
            # is unavailable.
            if naming_meta is not None:
                from src.campaign_name import build_campaign_name
                campaign_name = build_campaign_name(
                    ramp_id=ramp_id or "",
                    submitted_at=naming_meta.get("submitted_at", ""),
                    cohort=cohort,
                    geo_group=geo_group,
                    platform="linkedin",
                    campaign_type="static",
                    pod=naming_meta.get("pod"),
                    domain=naming_meta.get("domain"),
                    locale=naming_meta.get("locale"),
                    included_geos=naming_meta.get("included_geos"),
                    campaign_state=naming_meta.get("campaign_state"),
                )
            else:
                geo_suffix = f" [{geo_group.cluster_label}]" if geo_group.cluster != "global_mix" else ""
                campaign_name = f"{cohort._stg_name}{geo_suffix}"

            cohort_add_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_add", []) or [])
            cohort_remove_urns = urn_res.resolve_facet_pairs(getattr(cohort, "exclude_remove", []) or [])
            cohort_exclude_urns = _subtract_urn_dicts(
                _merge_urn_dicts(shared_exclude_urns, cohort_add_urns),
                cohort_remove_urns,
            )

            _li_static_budget_kwargs = (
                {"daily_budget_cents": daily_budget_cents}
                if daily_budget_cents is not None else {}
            )
            # Optimize on the per-pod WS Grant conversion (pod from Smart Ramp).
            # Known pod REPLACES the default OCP conversion so LinkedIn optimizes
            # on worker_skill_grant only; unknown pod → None → LINKEDIN_CONVERSION_ID.
            _pod_conv = _linkedin_pod_conversion_id(naming_meta.get("pod") if naming_meta else None)
            _lp(**_lp_kw, status="creating")
            campaign_urn = li_client.create_campaign(
                name=campaign_name,
                campaign_group_urn=group_urn,
                facet_urns=facet_urns,
                exclude_facet_urns=cohort_exclude_urns,
                campaign_state=getattr(cohort, "campaign_state", None),
                conversion_id=_pod_conv,
                **_li_static_budget_kwargs,
            )
            campaign_id = campaign_urn.rsplit(":", 1)[-1]
            sheets.update_li_campaign_id(cohort._stg_id, campaign_id)
            out_campaigns.append(campaign_urn)
            base_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id
            by_cohort_key = f"{base_id}_{geo_group.campaign_suffix}"
            by_cohort[by_cohort_key] = campaign_urn
            log.info(
                "_process_static_campaigns: campaign %s cohort=%s geo=%s (%d angles to attach)",
                campaign_urn, base_id, geo_group.cluster_label, len(specs),
            )
            _lp(**_lp_kw, status="created")
        except Exception as exc:
            log.exception(
                "_process_static_campaigns: cohort '%s' geo=%s campaign creation failed — skipping all angles: %s",
                getattr(cohort, "name", "?"), geo_label, exc,
            )
            _lp(**_lp_kw, status="failed", error=str(exc))
            continue

        # Verify-and-heal (piece C) trackers for THIS campaign: row_ids that
        # got a real creative attached, and the retry kwargs for the ones that
        # didn't (captured once per spec just before the attach attempt).
        _li_attached: set[str] = set()
        _li_payloads: dict[str, dict] = {}
        _li_errors: list[str] = []  # failure reasons for verify-and-heal surfacing

        # ── Attach one creative per angle to the just-created campaign. ─────
        for spec in specs:
            angle_idx = spec["angle_idx"]
            angle_label = spec.get("angle_label", ["A", "B", "C"][angle_idx])
            png_path = spec["png_path"]
            qc_report = spec.get("qc_report", {})
            variant = variants[angle_idx] if angle_idx < len(variants) else {}
            row_id = f"{by_cohort_key}_{angle_label}"

            # Upload to Drive FIRST so the URL can be logged into the registry
            # as `creative_image_path` — gives the Triggers sheet a clickable
            # link in the Creative Image Path column. Falls back to the local
            # path if Drive is disabled / upload fails.
            drive_url = ""
            if config.GDRIVE_ENABLED and png_path and Path(str(png_path)).exists():
                drive_url = _save_creative_to_drive(
                    png_path=png_path,
                    ramp_id=ramp_id or "manual",
                    unique_id=unique_id or row_id,
                    channel="linkedin",
                    angle=angle_label,
                    cohort_geo=_cohort_geo_label(cohort, geo_group),
                )

            # One registry row per (cohort × geo × angle) — shared
            # platform_campaign_id (the LinkedIn campaign URN) + distinct
            # platform_creative_id once the creative attaches.
            try:
                _reg_log(
                    smart_ramp_id=ramp_id or "",
                    cohort_id=base_id,
                    cohort_signature=cohort.name,
                    geo_cluster=geo_group.cluster,
                    geo_cluster_label=geo_group.cluster_label,
                    geos=group_geos,
                    angle=angle_label,
                    campaign_type="static",
                    advertised_rate=geo_group.advertised_rate,
                    audience_size=geo_audience,
                    linkedin_campaign_urn=campaign_urn,
                    headline=variant.get("headline", ""),
                    subheadline=variant.get("subheadline", ""),
                    photo_subject=variant.get("photo_subject", ""),
                    creative_image_path=drive_url or (str(png_path) if png_path else ""),
                    cohort_geo=_cohort_geo_label(cohort, geo_group),
                    gemini_prompt=qc_report.get("gemini_prompt", ""),
                    qc_verdict=qc_report.get("verdict", "") if qc_report else "",
                    qc_attempts=qc_report.get("attempts") if qc_report else None,
                    qc_violations=qc_report.get("violations") if qc_report else None,
                    campaign_name=campaign_name,
                    generation=_gen,
                )
            except Exception as _exc:
                log.warning("Registry log failed (non-fatal): %s", _exc)

            if not (png_path and Path(str(png_path)).exists()):
                log.info("_process_static_campaigns: no PNG for angle %s — skipping creative attach", angle_label)
                continue

            headline = variant.get("headline") or f"Your {_cohort_headline(cohort)} expertise is in demand."
            subhead = variant.get("subheadline") or "Earn payment doing remote AI tasks on your schedule."

            # Build the UTM destination URL per (cohort × geo × angle). Base
            # LP resolved from Smart Ramp's campaign_state.utm_linkedin or
            # config.LP_URL_BY_DOMAIN → LP sheet; falls back to LINKEDIN_DESTINATION.
            from src.utm_builder import build_utm_url, resolve_base_lp_url
            base_lp = resolve_base_lp_url(
                campaign_state=(naming_meta or {}).get("campaign_state"),
                platform="linkedin",
                fallback=destination_url_override or config.LINKEDIN_DESTINATION,
                matched_domain=(naming_meta or {}).get("domain"),
                sheets_client=sheets,
                ramp_id=ramp_id,
                cohort_id=cohort_id_override or getattr(cohort, "id", None) or "",
            )
            utm_url = build_utm_url(
                base_url=base_lp, platform="linkedin",
                campaign_name=campaign_name,
                pod=(naming_meta or {}).get("pod"),
                domain=(naming_meta or {}).get("domain"),
                locale=(naming_meta or {}).get("locale"),
                language=((naming_meta or {}).get("campaign_state") or {}).get("linkedin", {}).get("liAdLanguage") or "EN",
                utm_content=f"{cohort._stg_id}-static-{angle_label}",
            ) if base_lp else (destination_url_override or "")

            # Capture the attach kwargs so the verify-and-heal retry (piece C)
            # can re-attempt this creative without recomputing UTM/copy.
            _attach_kwargs = dict(
                campaign_urn=campaign_urn,
                headline=headline,
                description=subhead,
                intro_text=variant.get("intro_text", "") if variant else "",
                ad_headline=variant.get("ad_headline", "") if variant else "",
                ad_description=variant.get("ad_description", "") if variant else "",
                cta_button=variant.get("cta_button", "APPLY") if variant else "APPLY",
                destination_url=utm_url,
                # Per-ad creative name "<campaign name> | <angle>" so A/B/C is
                # identifiable in Campaign Manager (mirrors the Meta arm's ad_name).
                ad_name=f"{campaign_name} | {angle_label}",
            )
            _li_payloads[row_id] = {"png_path": png_path, "kwargs": _attach_kwargs}

            try:
                image_urn = li_client.upload_image(png_path)
                ad_result = li_client.create_image_ad(image_urn=image_urn, **_attach_kwargs)
            except Exception as exc:
                log.warning("_process_static_campaigns: upload/attach raised for angle %s: %s", angle_label, exc)
                creative_paths[row_id] = ""
                _li_errors.append(f"{type(exc).__name__}: {str(exc)[:200]}")
                continue

            if ad_result.status == "ok":
                creative_urn = ad_result.creative_urn
                _li_attached.add(row_id)
                sheets.write_creative(cohort._stg_id, cohort._stg_name, creative_urn)
                creative_paths[row_id] = creative_urn
                log.info("_process_static_campaigns: angle=%s creative %s attached to %s",
                         angle_label, creative_urn, campaign_urn)
                # Update the matching registry row with its creative URN.
                # Atomic under Phase 3.3's concurrent-arms model — the
                # registry_critical_section context manager re-entrant-locks
                # the file across load → mutate → save so InMail arm can't
                # interleave a write between our load and save.
                try:
                    from src.campaign_registry import (
                        _load as _reg_load,
                        _save as _reg_save,
                        registry_critical_section,
                    )
                    with registry_critical_section():
                        recs = _reg_load()
                        for _r in recs:
                            if (
                                _r.get("linkedin_campaign_urn") == campaign_urn
                                and _r.get("angle") == angle_label
                                and not _r.get("creative_urn")
                            ):
                                _r["creative_urn"] = creative_urn
                                _r["platform_creative_id"] = creative_urn
                                break
                        _reg_save(recs)
                except Exception:
                    pass
            elif ad_result.status == "local_fallback":
                # SR-04: LinkedIn creative attach blocked (typically DSC 403
                # gated by MDP entitlement). The PNG was already uploaded to
                # Shared Drive at the top of this loop (line ~2482) — that
                # `drive_url` variable holds the canonical Drive URL.
                # Pranav rule: never write creatives locally; Drive is the
                # source of truth. If the prior Drive upload failed, log and
                # skip without falling back to local disk.
                if drive_url:
                    creative_paths[row_id] = drive_url
                else:
                    creative_paths[row_id] = ""
                    log.error(
                        "_process_static_campaigns: angle=%s creative attach blocked AND "
                        "Drive upload had failed earlier — no creative path recorded "
                        "(no local fallback per Drive-only policy). reason=%s — %s",
                        angle_label, ad_result.error_class, ad_result.error_message,
                    )
                log.warning(
                    "_process_static_campaigns: angle=%s creative attach blocked "
                    "(reason: %s — %s); PNG lives at Drive URL=%s",
                    angle_label, ad_result.error_class, ad_result.error_message,
                    drive_url or "(upload failed)",
                )
                _li_errors.append(
                    f"{ad_result.error_class or 'blocked'}: {ad_result.error_message or 'creative attach blocked'}"[:200]
                )
            else:  # status == "error"
                log.error(
                    "_process_static_campaigns: create_image_ad hard error angle=%s: %s — %s",
                    angle_label, ad_result.error_class, ad_result.error_message,
                )
                creative_paths[row_id] = ""
                _li_errors.append(
                    f"{ad_result.error_class or 'error'}: {ad_result.error_message or 'unknown'}"[:200]
                )

        # ── Verify-and-heal (piece C). If no creative attached to this
        # campaign, retry each captured spec once; if still none, archive the
        # campaign + flag so no empty shell survives the launch. (A DSC/MDP
        # entitlement block is deterministic so retry won't change it — but the
        # heal still guarantees no empty LinkedIn campaign is left behind.)
        if config.LAUNCH_VERIFY_ENABLED and not _li_attached and _li_payloads:
            log.warning(
                "_process_static_campaigns: 0 creatives attached to %s — "
                "retrying %d angle(s) once before heal",
                campaign_urn, len(_li_payloads),
            )
            for _rid, _pl in _li_payloads.items():
                _pp = _pl["png_path"]
                if not (_pp and Path(str(_pp)).exists()):
                    continue
                try:
                    _iu = li_client.upload_image(_pp)
                    _rr = li_client.create_image_ad(image_urn=_iu, **_pl["kwargs"])
                    if _rr.status == "ok":
                        _li_attached.add(_rid)
                        creative_paths[_rid] = _rr.creative_urn
                    else:
                        _li_errors.append(
                            f"{_rr.error_class or 'error'}: {_rr.error_message or 'unknown'}"[:200]
                        )
                except Exception as _exc:
                    log.warning("_process_static_campaigns: heal retry raised for %s: %s", _rid, _exc)
                    _li_errors.append(f"{type(_exc).__name__}: {str(_exc)[:200]}")
            if not _li_attached:
                _reason = (
                    "; ".join(dict.fromkeys(_li_errors))[:400] if _li_errors
                    else "no creative attached after retry"
                )
                _summ = launch_verify.heal_empty(
                    platform="linkedin",
                    container_id=campaign_urn,
                    ramp_id=ramp_id or "",
                    campaign_name=campaign_name,
                    reason=_reason,
                    li_client=li_client,
                )
                if _summ:
                    healed_empties.append(_summ)

    if config.LAUNCH_VERIFY_ENABLED:
        launch_verify.notify_healed(ramp_id or "", healed_empties)

    # Reconciliation pass: registry rows are logged at campaign-creation time,
    # but PNG renders + Drive uploads may complete later (retries, async). Walk
    # Drive at the canonical hierarchy and patch any rows for this ramp that
    # ended up with an empty creative_image_path.
    if ramp_id and config.GDRIVE_ENABLED:
        try:
            from src.campaign_registry import reconcile_creative_paths
            stats = reconcile_creative_paths(ramp_id, "linkedin")
            if stats.get("patched"):
                log.info(
                    "_process_static_campaigns: reconciled creative_image_path "
                    "for %d row(s) (unmatched=%d, ambiguous_legacy=%d)",
                    stats["patched"], stats["unmatched"], stats["ambiguous_legacy"],
                )
        except Exception as _exc:
            log.warning("creative-path reconciliation failed (non-fatal): %s", _exc)

    return {
        "campaigns": out_campaigns,
        "campaigns_by_cohort": by_cohort,
        "creative_paths": creative_paths,
        "campaign_groups": out_groups,
        "group_name": group_name,
        # campaign_specs is consumed by _process_extra_platform_arm so Meta +
        # Google can reuse the same cohort × geo × angle plan + PNGs without
        # re-running Gemini.
        "campaign_specs": campaign_specs,
    }


def _process_extra_platform_arm(
    *,
    platform: str,
    client,
    resolver,
    campaign_specs: list[dict],
    flow_id: str,
    location: str,
    ramp_id: str | None,
    cohort_id_override: str | None,
    destination_url_override: str | None,
    unique_id: str | None = None,
    naming_meta: dict | None = None,
    sheets=None,
    daily_budget_cents: int | None = None,
) -> dict:
    """Per-platform static-ad arm for non-LinkedIn platforms (Meta + Google).

    Reuses the (cohort × geo × angle) plan + LinkedIn-rendered PNGs from
    `_process_static_campaigns`; calls the platform-specific copy adapter
    + targeting resolver + AdPlatformClient. Per (cohort × geo × angle)
    failures are isolated — one bad combo doesn't abort the rest.
    """
    from src.ad_platform import CreateAdResult
    from src.campaign_registry import log_campaign as _reg_log
    from src.ui_decisions import upsert_launch_progress as _lp
    from src.ui_decisions import next_generation as _next_gen
    from src.ui_decisions import resolve_live_container_id as _resolve_container
    from src.copy_adapter import adapt_copy_for_platform, localize_variant
    from src.locales import resolve_copy_locale
    from src import launch_verify

    out: dict = {
        "campaigns": [],
        "campaigns_by_cohort": {},
        "creative_paths": {},
        "campaign_groups": [],
        # Graceful-degradation: even when the parent campaign-group create
        # 4xxs (Meta SAC, Google PERMISSION_DENIED), we still want Diego
        # (Meta) / Bryan (Google) to have the PNGs + cohort/copy details to
        # build the campaign by hand. `manual_handoff_url` is the Drive link
        # to a JSON manifest with everything they need.
        "manual_handoff_url": "",
    }
    if not campaign_specs:
        return out

    # ── Phase 1: Always preserve creatives + cohort/copy to Drive ───────────
    # Runs BEFORE the platform-side campaign create, so even if the parent
    # group create fails (Meta SAC geo mismatch, Google permission denied),
    # the artifacts a human needs to build the campaign manually are in
    # Drive at <ramp_id>/<platform>/_manual_handoff.json + the per-spec PNGs.
    drive_urls_by_spec: dict[str, str] = {}
    handoff_entries: list[dict] = []
    for spec in campaign_specs:
        cohort_e = spec.get("cohort")
        geo_group_e = spec.get("geo_group")
        angle_label_e = spec.get("angle_label", "A")
        angle_idx_e = spec.get("angle_idx", 0)
        png_path_e = spec.get("png_path")
        variants_e = spec.get("variants") or []
        variant_e = variants_e[angle_idx_e] if angle_idx_e < len(variants_e) else {}
        group_geos_e = spec.get("group_geos") or []

        base_id_e = cohort_id_override or getattr(cohort_e, "id", None) or getattr(cohort_e, "_stg_id", "")
        cluster_suffix = getattr(geo_group_e, "campaign_suffix", "") or "geo"
        spec_key = f"{base_id_e}_{cluster_suffix}_{angle_label_e}"

        # 2026-05-23 — Per-channel brief consumption. If prep wrote a Meta or
        # Google brief for this (cohort × geo × angle), run Phase-2 against
        # that brief (with reviewer comment honored) and use the resulting
        # variant for both PNG composition + ad fields. Falls back to the
        # LinkedIn variant + adapt_copy_for_platform when no brief exists
        # (legacy ramps pre-2026-05-23, sparse-mode skips, etc.).
        if ramp_id and platform in ("meta", "google") and cohort_e is not None:
            try:
                from src.ui_decisions import list_briefs_for_ramp
                from src.brief_generator import build_copy_from_brief
                _platform_briefs = list_briefs_for_ramp(ramp_id, channel=platform)
                _matching = [
                    _b for _b in _platform_briefs
                    if _b.cohort_signature == getattr(cohort_e, "name", "")
                    and _b.geo_cluster == (getattr(geo_group_e, "cluster", "") or "global_mix")
                    and _b.angle == angle_label_e
                ]
                if _matching:
                    log.info(
                        "_process_extra_platform_arm[%s]: brief match for cohort=%s "
                        "geo=%s angle=%s (reviewer_comment=%dch) — regen variant from brief",
                        platform, getattr(cohort_e, "name", "")[:40],
                        getattr(geo_group_e, "cluster", ""), angle_label_e,
                        len(_matching[0].reviewer_comment or ""),
                    )
                    _v = build_copy_from_brief(
                        _matching[0].brief,
                        layer_map={},  # no Figma overlay outside LinkedIn
                        cohort=cohort_e,
                        geos=group_geos_e,
                        hourly_rate=getattr(geo_group_e, "advertised_rate", "") or "",
                        reviewer_comment=_matching[0].reviewer_comment or "",
                        channel=platform,
                        task_card=cached_card(ramp_id, cohort_id_override),
                    )
                    if _v:
                        variant_e = _v
                        # mutate the spec so downstream PNG composition + Drive
                        # manifest see the channel-tuned variant.
                        if isinstance(variants_e, list) and angle_idx_e < len(variants_e):
                            variants_e[angle_idx_e] = _v
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[%s]: per-channel brief consumption "
                    "failed (%s) — falling back to LinkedIn-adapted variant",
                    platform, _exc,
                )

        # ── Localize the variant for non-LinkedIn channels + locale-defined
        # cohorts (2026-06-17). Translates headline/subheadline (→ the image
        # overlay) + body fields into the cohort's language, keeping $/USD/
        # numerals + "Outlier" in English. No-op for English cohorts or when
        # LOCALIZE_PLATFORM_COPY is off. Done BEFORE PNG composition so the
        # overlay renders in-language (image_adapter passes text= so the
        # script-aware font + RAQM shaping engage — no tofu). LinkedIn is
        # excluded by design; this arm only runs meta/google/google_search/reddit.
        copy_locale_e = resolve_copy_locale(cohort_e, getattr(cohort_e, "_icp", None))
        if config.LOCALIZE_PLATFORM_COPY and copy_locale_e and variant_e:
            _english_e = dict(variant_e)   # fallback if the localized overlay can't render
            variant_e = localize_variant(variant_e, copy_locale_e)
            # TOFU GUARD (brand-critical): if no font resolves for the localized
            # overlay's script at render time, the image ships boxes — invisible
            # to every string check and never vision-QC'd on this arm. Fail CLOSED:
            # revert to the English overlay (legible + on-brand) and log LOUD so
            # the missing script font gets installed. A language mismatch is far
            # less damaging than tofu.
            from src.copy_design_qc import check_overlay_renderable
            _ok_e, _viol_e = check_overlay_renderable(variant_e)
            if not _ok_e:
                # SKIP — do NOT ship this creative. ensure_script_font already
                # tried fontconfig + retries to obtain the correct font and
                # failed, so we can neither render the localized overlay nor mix
                # in English (unprofessional). Better no ad than a broken one.
                log.error(
                    "_process_extra_platform_arm[%s]: SKIPPING creative — no font for the "
                    "localized overlay after retries; refusing to ship tofu OR mixed-language "
                    "copy. cohort=%s geo=%s angle=%s. INSTALL the script font (CI: "
                    "fonts-noto-core/fonts-noto-extra) + confirm `fc-list :lang=<lang>`. %s",
                    platform, getattr(cohort_e, "name", "")[:40], cluster_suffix, angle_label_e,
                    "; ".join(_viol_e),
                )
                continue
            # Strip emoji/symbols the copy LLM may have slipped into the overlay
            # — text fonts don't carry those glyphs, so they render as tofu boxes
            # (seen on a live Bengali creative). Off-brand regardless.
            from src.gemini_creative import strip_overlay_symbols as _strip_sym
            for _f in ("headline", "subheadline"):
                if isinstance(variant_e, dict) and variant_e.get(_f):
                    variant_e[_f] = _strip_sym(variant_e[_f])
            if isinstance(variants_e, list) and angle_idx_e < len(variants_e):
                variants_e[angle_idx_e] = variant_e
            log.info(
                "_process_extra_platform_arm[%s]: localized variant → %s for cohort=%s geo=%s angle=%s (overlay_ok=%s)",
                platform, copy_locale_e.display_language,
                getattr(cohort_e, "name", "")[:40], cluster_suffix, angle_label_e, _ok_e,
            )

        # Carry the resolved rate so the Meta/Google/Reddit compositor's bottom
        # band shows the real figure, never a hardcoded range (see derive_bottom_text).
        if isinstance(variant_e, dict):
            variant_e["advertised_rate"] = getattr(geo_group_e, "advertised_rate", "") or ""

        # 2026-05-20: Meta arm regenerates a fresh 4:5 (1080×1350) photo
        # instead of reusing the LinkedIn 1:1 composite. Per Meta Help Center
        # + 2025/2026 ad-performance benchmarks, 4:5 is the highest-converting
        # static Feed ratio on FB + IG (~33% more vertical screen than 1:1).
        # The Meta-native compositor (compose_ad_for_platform) renders only
        # the headline on the image — Meta surfaces description / primary_text
        # as separate ad fields, so leaving them off the photo avoids
        # duplication and visual noise.
        #
        # Done in Phase 1 (before the Drive upload) so the same 4:5 PNG flows
        # to BOTH Drive at <ramp>/meta/<cohort_geo>/<angle>.png AND to
        # client.upload_image() in Phase 2. spec["png_path"] is rewritten so
        # the Phase 2 loop picks up the new path automatically.
        #
        # Cost: +1 Gemini call per (cohort × geo × angle) when meta is in
        # ENABLED_PLATFORMS. Falls back to the LinkedIn 1:1 PNG on any
        # Gemini / compose failure so the Meta arm keeps producing campaigns
        # in a degraded state.
        if platform == "meta" and variant_e:
            try:
                from src.gemini_creative import generate_imagen_photo
                from src.image_adapter import compose_ad_for_platform, primary_aspect
                meta_aspect = primary_aspect("meta")  # (4, 5) post 2026-05-20
                log.info(
                    "_process_extra_platform_arm[meta]: regenerating photo at "
                    "aspect=%s for cohort=%s geo=%s angle=%s",
                    meta_aspect,
                    str(base_id_e)[:12], cluster_suffix, angle_label_e,
                )
                meta_bg = generate_imagen_photo(variant_e, aspect=meta_aspect)
                meta_png_path = compose_ad_for_platform(
                    bg_image=meta_bg,
                    copy_variant=variant_e,
                    platform="meta",
                    angle=angle_label_e,
                    aspect=meta_aspect,
                )
                log.info(
                    "_process_extra_platform_arm[meta]: 4:5 PNG ready %s "
                    "(replacing LinkedIn 1:1 PNG for Drive + upload_image)",
                    meta_png_path,
                )
                png_path_e = meta_png_path
                spec["png_path"] = meta_png_path
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[meta]: 4:5 photo gen FAILED "
                    "for cohort=%s geo=%s angle=%s — falling back to LinkedIn "
                    "1:1 PNG: %s",
                    str(base_id_e)[:12], cluster_suffix, angle_label_e, _exc,
                )

        # Google Display (RDA) needs a 1:1 SQUARE source — create_image_ad
        # derives the required 1.91:1 landscape from it. The Meta block above
        # rewrites the SHARED spec["png_path"] to a 4:5 photo; without our own
        # 1:1 the Google arm inherits that 4:5 and every RDA create fails the
        # square asset spec ("aspect ratio does not match" → 0 ads). Compose a
        # fresh 1:1 so Display ads actually attach. (google_search is text-only.)
        if platform == "google" and variant_e:
            try:
                from src.gemini_creative import generate_imagen_photo
                from src.image_adapter import compose_ad_for_platform
                g_bg = generate_imagen_photo(variant_e, aspect=(1, 1))
                g_png = compose_ad_for_platform(
                    bg_image=g_bg, copy_variant=variant_e,
                    platform="google", angle=angle_label_e, aspect=(1, 1),
                )
                png_path_e = g_png
                spec["png_path"] = g_png
                log.info(
                    "_process_extra_platform_arm[google]: 1:1 square PNG ready %s "
                    "(RDA square source; 1.91:1 landscape derived at upload)", g_png,
                )
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[google]: 1:1 photo gen FAILED "
                    "cohort=%s geo=%s angle=%s — falling back to existing PNG: %s",
                    str(base_id_e)[:12], cluster_suffix, angle_label_e, _exc,
                )

        # Reddit feed promoted image is 1:1 (1200×1200). Compose a fresh square
        # so the Reddit creative isn't inherited from a 4:5 (Meta) rewrite of the
        # shared spec. (The free-form/native text post carries no image.)
        if platform == "reddit" and variant_e:
            try:
                from src.gemini_creative import generate_imagen_photo
                from src.image_adapter import compose_ad_for_platform, primary_aspect
                r_aspect = primary_aspect("reddit")  # (1, 1)
                r_bg = generate_imagen_photo(variant_e, aspect=r_aspect)
                r_png = compose_ad_for_platform(
                    bg_image=r_bg, copy_variant=variant_e,
                    platform="reddit", angle=angle_label_e, aspect=r_aspect,
                )
                png_path_e = r_png
                spec["png_path"] = r_png
                log.info(
                    "_process_extra_platform_arm[reddit]: 1:1 PNG ready %s", r_png,
                )
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[reddit]: 1:1 photo gen FAILED "
                    "cohort=%s geo=%s angle=%s — falling back to existing PNG: %s",
                    str(base_id_e)[:12], cluster_suffix, angle_label_e, _exc,
                )

        # TikTok in-feed is a vertical 9:16 (1080×1920) surface. Compose a fresh
        # 9:16 with the TikTok safe-zones (headline below the 140px top pill,
        # subject above the ~400px bottom CTA strip) so the creative isn't a
        # 4:5/1:1 crop inherited from the shared spec.
        if platform == "tiktok" and variant_e:
            try:
                from src.gemini_creative import generate_imagen_photo
                from src.image_adapter import compose_ad_for_platform, primary_aspect
                tt_aspect = primary_aspect("tiktok")  # (9, 16)
                tt_bg = generate_imagen_photo(variant_e, aspect=tt_aspect)
                tt_png = compose_ad_for_platform(
                    bg_image=tt_bg, copy_variant=variant_e,
                    platform="tiktok", angle=angle_label_e, aspect=tt_aspect,
                )
                png_path_e = tt_png
                spec["png_path"] = tt_png
                log.info(
                    "_process_extra_platform_arm[tiktok]: 9:16 PNG ready %s", tt_png,
                )
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[tiktok]: 9:16 photo gen FAILED "
                    "cohort=%s geo=%s angle=%s — falling back to existing PNG: %s",
                    str(base_id_e)[:12], cluster_suffix, angle_label_e, _exc,
                )

        # Full QC (copy + vision/design) on EVERY localized creative — this arm
        # previously ran none, so baked-in photo text ("85%") + overlay tofu
        # shipped uncaught. On a regen-fixable FAIL, REGENERATE the photo and
        # re-QC up to QC_MAX_RETRIES times — ideally we never skip; skipping is a
        # last resort only after every retry still fails. Copy-length notes are
        # surfaced but don't gate here (regen can't fix copy, and localized
        # length quirks would false-skip).
        if config.EXTRA_ARM_VISION_QC and png_path_e and Path(str(png_path_e)).exists():
            from src.copy_design_qc import qc_creative
            _hl = (variant_e or {}).get("headline", "")
            _sub = (variant_e or {}).get("subheadline", "")
            # Default capped 10→5 (2026-07-20): the extra arm has its OWN QC loop
            # separate from gemini_creative's; a Thai cohort whose creatives kept
            # failing QC burned ~25s × 10 × 3 angles (~23-min run). 5 still recovers
            # transient misses. Env QC_MAX_RETRIES overrides (set to 5 in Doppler prd).
            _max_qc = max(1, int(os.getenv("QC_MAX_RETRIES", "5")))

            def _regen_extra_png():
                from src.gemini_creative import generate_imagen_photo
                from src.image_adapter import compose_ad_for_platform
                _asp = (4, 5) if platform == "meta" else (9, 16) if platform == "tiktok" else (1, 1)
                _bg = generate_imagen_photo(variant_e, aspect=_asp)
                return compose_ad_for_platform(
                    bg_image=_bg, copy_variant=variant_e, platform=platform,
                    angle=angle_label_e, aspect=_asp,
                )

            _passed = False
            for _att in range(_max_qc):
                try:
                    _qcr = qc_creative(creative_path=png_path_e, reference_path=None,
                                       headline=_hl, subheadline=_sub, attempt_index=_att)
                except Exception as _exc:  # noqa: BLE001 — QC infra hiccup must not gate
                    log.warning("_process_extra_platform_arm[%s]: QC errored (%s) — proceeding ungated", platform, _exc)
                    _passed = True
                    break
                if _qcr.copy_violations:
                    log.warning("_process_extra_platform_arm[%s]: QC copy notes (not gated): %s", platform, _qcr.copy_violations)
                _crit = [v for v in _qcr.violations if any(k in v.lower() for k in (
                    "rendered text", "text in photo", "tofu", "legible", "logo",
                    "subject", "overlap", "contrast", "photo fills"))]
                if not _crit:
                    _passed = True
                    break
                log.warning("_process_extra_platform_arm[%s]: QC FAIL attempt %d/%d (%s) — regenerating creative",
                            platform, _att + 1, _max_qc, "; ".join(_crit))
                try:
                    _new = _regen_extra_png()
                except Exception as _exc:  # noqa: BLE001
                    log.warning("_process_extra_platform_arm[%s]: regen failed (%s) — stopping retries", platform, _exc)
                    break
                if _new and Path(str(_new)).exists():
                    png_path_e = _new
                    spec["png_path"] = _new
                else:
                    break
            if not _passed:
                log.error(
                    "_process_extra_platform_arm[%s]: QC still failing after %d attempts — SKIPPING as "
                    "LAST RESORT (never ship a broken creative). cohort=%s geo=%s angle=%s",
                    platform, _max_qc, str(base_id_e)[:12], cluster_suffix, angle_label_e,
                )
                continue

        drive_url_e = ""
        if config.GDRIVE_ENABLED and png_path_e and Path(str(png_path_e)).exists():
            try:
                drive_url_e = _save_creative_to_drive(
                    png_path=png_path_e,
                    ramp_id=ramp_id or "manual",
                    unique_id=spec_key,
                    channel=platform,
                    angle=angle_label_e,
                    cohort_geo=_cohort_geo_label(cohort_e, geo_group_e),
                )
            except Exception as _exc:
                log.warning(
                    "_process_extra_platform_arm[%s]: Drive PNG upload failed for %s: %s",
                    platform, spec_key, _exc,
                )
        drive_urls_by_spec[spec_key] = drive_url_e

        platform_copy_e = adapt_copy_for_platform(variant_e, platform, icp=getattr(cohort_e, "_icp", None), locale=copy_locale_e) if variant_e else {}
        # Reddit: the manual-upload manifest needs the targeting + conversion an
        # operator would otherwise have to reconstruct — per-pod subreddits +
        # interests/keywords (from the resolver), the intended worker_skill_grant
        # conversion event for this pod, the pixel id, suggested budget, and
        # which ad formats to build (image + free-form). platform_copy already
        # carries the image title/cta + free-form title/body.
        reddit_extra: dict = {}
        if platform == "reddit":
            try:
                _rt = resolver.resolve_cohort(cohort_e, geos=list(group_geos_e))
            except Exception as _exc:
                log.warning("_process_extra_platform_arm[reddit]: resolver failed for %s: %s", spec_key, _exc)
                _rt = {}
            from src.reddit_api import reddit_pod_conversion_event
            _pod = _rt.get("pod", "")
            reddit_extra = {
                "reddit_pod":                 _pod,
                "reddit_subreddits":          _rt.get("subreddits", []),
                "reddit_interests":           _rt.get("interests", []),
                "reddit_keywords":            _rt.get("keywords", []),
                "reddit_conversion_event":    reddit_pod_conversion_event(_pod) or "(pending Tuan: Reddit pixel id + per-pod WS event names)",
                "reddit_pixel_id":            config.REDDIT_PIXEL_ID or "(pending Tuan)",
                "reddit_suggested_daily_usd": config.REDDIT_DEFAULT_DAILY_USD,
                "reddit_ad_formats":          ["image", "free_form"],
            }
        handoff_entries.append({
            "cohort_name":      getattr(cohort_e, "name", ""),
            "cohort_stg_id":    getattr(cohort_e, "_stg_id", ""),
            "cohort_stg_name":  getattr(cohort_e, "_stg_name", ""),
            "geo_cluster":      getattr(geo_group_e, "cluster", ""),
            "geo_cluster_label": getattr(geo_group_e, "cluster_label", ""),
            "geos":             list(group_geos_e),
            "advertised_rate":  getattr(geo_group_e, "advertised_rate", ""),
            "angle":            angle_label_e,
            "image_drive_url":  drive_url_e,
            "headline":         variant_e.get("headline", "") if variant_e else "",
            "subheadline":      variant_e.get("subheadline", "") if variant_e else "",
            "photo_subject":    variant_e.get("photo_subject", "") if variant_e else "",
            "platform_copy":    platform_copy_e,
            "destination_url":  destination_url_override or "",
            "rules":            list(getattr(cohort_e, "rules", []) or []),
            **reddit_extra,
        })

    manual_handoff_url = ""
    if handoff_entries:
        import json as _json
        from datetime import datetime as _dt, timezone as _tz
        manifest = {
            "ramp_id":      ramp_id or "",
            "platform":     platform,
            "generated_at": _dt.now(_tz.utc).isoformat(),
            "purpose": (
                f"Manual-creation handoff for {platform.title()}. If the "
                f"agent failed to create the {platform.title()} campaign, "
                "use these entries to build it manually in the platform UI. "
                "Each entry has cohort + geo targeting, copy variant, and "
                "the rendered creative image."
            ),
            "entries_count": len(handoff_entries),
            "entries":       handoff_entries,
        }
        try:
            from src.gdrive import upload_text_in_hierarchy
            manual_handoff_url = upload_text_in_hierarchy(
                text=_json.dumps(manifest, indent=2, default=str),
                ramp_id=ramp_id or "manual",
                channel=platform,
                filename="_manual_handoff.json",
            )
            log.info(
                "_process_extra_platform_arm[%s]: manual-handoff manifest written → %s",
                platform, manual_handoff_url or "(local-only, Drive disabled)",
            )
        except Exception as _exc:
            log.warning(
                "_process_extra_platform_arm[%s]: manifest upload failed (non-fatal): %s",
                platform, _exc,
            )
    out["manual_handoff_url"] = manual_handoff_url

    # ── Phase 2: Best-effort platform-side campaign create ──────────────────
    # One platform-level container ("Campaign" on Meta/Google) per ramp run.
    # Cohort × geo × angle become Ad Sets / Ad Groups under it.
    # Group-level name uses the Smart Ramp v2 spec when naming_meta is
    # available; legacy "Outlier <flow> <loc> <Platform>" otherwise.

    # Google keyword override read-back (Phase 2 of the keywords-card flow).
    # Before generating fresh keyword_ideas via the Google Ads keyword planner,
    # check the Campaign Registry sheet for prior keywords on this (ramp ×
    # cohort × geo × angle) combo. If present (either pipeline-written from a
    # previous run OR user-edited via outlier-campaign-console keywords-card),
    # those win. Empty list ([]) is treated as "user intentionally cleared all"
    # and respected. Missing key → caller regenerates as before.
    google_keyword_overrides: dict[tuple[str, str, str], list[str]] = {}
    if platform == "google" and ramp_id and sheets is not None:
        try:
            google_keyword_overrides = sheets.read_registry_keywords_for_ramp(ramp_id)
        except Exception as _exc:
            log.warning(
                "Google keyword override read failed (%s) — falling back to "
                "fresh keyword generation",
                _exc,
            )

    if naming_meta is not None:
        from src.campaign_name import build_campaign_name as _build_grp_name
        group_name = _build_grp_name(
            ramp_id=ramp_id or "",
            submitted_at=naming_meta.get("submitted_at", ""),
            cohort=None,
            platform=platform,
            campaign_type="static",
            format_override=f"{platform.title()} Parent",
            pod=naming_meta.get("pod"),
            domain=naming_meta.get("domain"),
            locale=naming_meta.get("locale"),
            included_geos=naming_meta.get("included_geos"),
            campaign_state=naming_meta.get("campaign_state"),
        )
    else:
        group_name = f"Outlier {flow_id} {location} {platform.title()}".strip()
    # Union of all targeted geos across child ad-set specs — Meta needs this at
    # the campaign level for special_ad_category_country under EMPLOYMENT SAC,
    # else child ad-set creation fails with a geo-mismatch 400.
    union_geos: list[str] = sorted({
        g.upper()
        for spec in campaign_specs
        for g in (spec.get("group_geos") or [])
        if g
    })
    # Empty-geo generalist locale cohorts (e.g. ko-KR/vi-VN — ramp left
    # included_geos empty): the ad-set resolver falls back to the locale region
    # for targeting, so the parent campaign's special_ad_category_country must
    # carry that region too — otherwise the ad set is "outside" the (empty) SAC
    # countries and Meta rejects it (subcode 2909035). Mirror the resolver's
    # fallback here so both levels agree.
    if not union_geos:
        from src.locales import region_for_locale
        union_geos = sorted({
            r for spec in campaign_specs
            for r in [region_for_locale(
                (getattr(spec.get("cohort"), "facet_strength", None) or {}).get("generalist_locale")
            )]
            if r
        })
    # Additive launch = attach NEW ad creatives to the ad set/campaign that's
    # ALREADY live for each (cohort × geo) — one campaign accumulates fresh
    # creatives as older ones fatigue. It must NOT create a new campaign/ad set
    # per launch (the old, wrong "generation = new campaign" model). So in
    # additive mode we do NOT create the parent campaign-group up front; it's
    # created lazily via _ensure_group() only for a (cohort × geo) that has
    # nothing live to extend (a genuine first-launch fallback).
    _additive = bool(getattr(config, "ADDITIVE_LAUNCH", False))
    _group_state: dict = {"id": None}

    def _ensure_group():
        """Create the platform campaign-group (Meta campaign / Google campaign)
        and log its parent registry row, once. Returns the group id, or None on
        creation failure. Idempotent within this arm invocation."""
        if _group_state["id"] is not None:
            return _group_state["id"]
        try:
            gid = client.create_campaign_group(group_name, geos=union_geos)
        except Exception as exc:
            log.error(
                "_process_extra_platform_arm[%s]: create_campaign_group failed (%s) — "
                "platform-side skipped, but Phase 1 already wrote %d creatives + manifest to Drive (%s)",
                platform, exc, len(handoff_entries), manual_handoff_url or "drive disabled",
            )
            return None
        _group_state["id"] = gid
        out["campaign_groups"].append(gid)
        # Log the platform-level campaign-group/parent immediately. This row gets
        # written to the registry even when downstream ad-set creation fails (e.g.
        # Meta Special Ad Category geo mismatch, Google permission-denied on a
        # specific campaign), so the parent_id is always traceable post-mortem.
        try:
            from src.campaign_registry import log_campaign as _reg_log_parent
            # A parent row is only ever logged for a fresh create (first launch /
            # replace, or additive's first-launch fallback), so it always maps to
            # a real new group. generation follows the same batch-tag scheme.
            _parent_gen = (
                _next_gen(ramp_id=ramp_id or "", platform=platform, campaign_type="parent",
                          cohort_signature=f"{platform}_root", geo_cluster="")
                if _additive else 1
            )
            _reg_log_parent(
                smart_ramp_id=ramp_id or flow_id or "",
                cohort_id=cohort_id_override or "",
                cohort_signature=f"{platform}_root",
                geo_cluster="",
                geo_cluster_label="",
                geos=[],
                angle="",
                campaign_type="parent",
                advertised_rate="",
                platform=platform,
                platform_campaign_id=str(gid),
                platform_creative_id="",
                campaign_name=group_name,
                generation=_parent_gen,
            )
            log.info(
                "_process_extra_platform_arm[%s]: parent group %s logged to registry (campaign_type=parent)",
                platform, gid,
            )
        except Exception as _exc:
            log.warning(
                "_process_extra_platform_arm[%s]: registry log for parent group failed (non-fatal): %s",
                platform, _exc,
            )
        return gid

    if not _additive:
        # First-launch / replace: create the parent up front (existing behavior).
        # A failure here means the whole arm can't proceed.
        if _ensure_group() is None:
            return out
    else:
        log.info(
            "_process_extra_platform_arm[%s]: ADDITIVE mode — will attach new creatives to "
            "existing live ad sets; parent group created lazily only for cohorts with nothing live.",
            platform,
        )

    # ── Group specs by (cohort × geo_group) so each becomes ONE Meta Ad Set
    # (or Google Ad Group), with multiple ads attached (one per angle).
    grouped: dict[tuple[str, str], list[dict]] = {}
    g_meta: dict[tuple[str, str], dict] = {}
    for spec in campaign_specs:
        key = (spec["cohort"]._stg_id, spec["geo_group"].cluster)
        grouped.setdefault(key, []).append(spec)
        if key not in g_meta:
            g_meta[key] = {
                "cohort":     spec["cohort"],
                "geo_group":  spec["geo_group"],
                "group_geos": spec.get("group_geos") or [],
                "variants":   spec.get("variants") or [],
            }

    log.info(
        "_process_extra_platform_arm[%s]: %d cohort × geo group(s) → 1 ad set/group each, "
        "with up to %d ads per group",
        platform, len(grouped),
        max((len(v) for v in grouped.values()), default=0),
    )

    healed_empties: list[dict] = []
    keyword_drops: list[dict] = []  # Search keywords Google rejected (needs review)
    manual_location_adds: list[dict] = []  # young-market countries Tuan must add manually (Meta 1870249)
    for (stg_id, cluster), specs in grouped.items():
        meta = g_meta[(stg_id, cluster)]
        cohort     = meta["cohort"]
        geo_group  = meta["geo_group"]
        group_geos = meta["group_geos"]
        variants   = meta["variants"]
        base_id = cohort_id_override or getattr(cohort, "id", None) or cohort._stg_id
        by_cohort_key = f"{base_id}_{geo_group.campaign_suffix}"

        # Per-cohort idempotency: skip a (cohort × geo) that already has a live
        # campaign on this platform (bypassed on replace).
        if _cohort_channel_already_live(ramp_id, platform, "static", cohort, geo_group):
            log.info(
                "_process_extra_platform_arm[%s]: skipping (cohort=%r geo=%r) — already has a "
                "live campaign (idempotent re-launch)", platform, cohort.name, geo_group.cluster,
            )
            continue

        # Launch-progress telemetry (console "Launch status" view). channel is
        # the platform key (meta | google | google_search | reddit | tiktok).
        _lp_locale = (naming_meta.get("locale") if naming_meta else "") or ""
        _lp_kw = dict(
            ramp_id=ramp_id or "", channel=platform, locale=_lp_locale,
            cohort_id=str(base_id or ""), cohort_signature=getattr(cohort, "name", ""),
            geo_cluster=geo_group.cluster,
        )
        _lp(**_lp_kw, status="queued")
        # Additive launch → fresh generation (v2/v3…); default stays gen 1.
        _gen = (
            _next_gen(ramp_id=ramp_id or "", platform=platform, campaign_type="static",
                      cohort_signature=getattr(cohort, "name", ""), geo_cluster=geo_group.cluster)
            if getattr(config, "ADDITIVE_LAUNCH", False) else 1
        )

        # Per (cohort × geo) isolation: failure in one combo never aborts another.
        try:
            # Smart Ramp v2 naming for the ad-set (Meta) / ad-group (Google);
            # falls back to legacy <stg_name>[geo] when naming_meta is absent.
            if naming_meta is not None:
                from src.campaign_name import build_campaign_name
                campaign_name = build_campaign_name(
                    ramp_id=ramp_id or "",
                    submitted_at=naming_meta.get("submitted_at", ""),
                    cohort=cohort,
                    geo_group=geo_group,
                    platform=platform,
                    campaign_type="static",
                    pod=naming_meta.get("pod"),
                    domain=naming_meta.get("domain"),
                    locale=naming_meta.get("locale"),
                    included_geos=naming_meta.get("included_geos"),
                    campaign_state=naming_meta.get("campaign_state"),
                )
            else:
                geo_suffix = (
                    f" [{geo_group.cluster_label}]" if geo_group.cluster != "global_mix" else ""
                )
                campaign_name = f"{cohort._stg_name}{geo_suffix}"
            # Google enforces ad_group_name uniqueness within a parent
            # campaign. The Smart Ramp v2 spec (build_campaign_name) has no
            # per-cohort or per-geo segment that distinguishes within one
            # row — "main country" is shared across all geo clusters AND
            # the cohort signature is omitted entirely. With 3 cohorts × 3
            # geos = 9 combos, the spec produces just 1 unique name → the
            # first ad group lands and the other 8 fail DUPLICATE_ADGROUP_NAME.
            # Append cohort._stg_id + geo_group.cluster_label as a 13th/14th
            # segment ONLY for Google; LinkedIn + Meta tolerate duplicates so
            # we leave their canonical names untouched.
            if platform == "google":
                stg_suffix = getattr(cohort, "_stg_id", "") or ""
                geo_suffix = (
                    getattr(geo_group, "cluster_label", "")
                    if geo_group is not None
                    and getattr(geo_group, "cluster", "") != "global_mix"
                    else ""
                )
                # Both segments together guarantee uniqueness across (cohort
                # × geo_group) under a single parent campaign.
                extras = " | ".join(s for s in (stg_suffix, geo_suffix) if s)
                if extras:
                    campaign_name = f"{campaign_name} | {extras}"
            targeting = resolver.resolve_cohort(cohort, geos=group_geos)

            # Google Search keyword override (Phase 2 of keywords-card flow).
            # Keywords attach to the AD-GROUP (1:1 with cohort × geo), not the
            # individual ad — so all 3 angle rows in the registry share a
            # single keyword set. We pick angle A's override as canonical
            # (fall back to B then C if only those are populated). Empty list
            # ([]) is honored — user cleared all keywords intentionally.
            if platform == "google" and google_keyword_overrides:
                for _try_angle in ("A", "B", "C"):
                    _ovr_key = (str(base_id), geo_group.cluster, _try_angle)
                    if _ovr_key in google_keyword_overrides:
                        targeting["keyword_ideas"] = google_keyword_overrides[_ovr_key]
                        log.info(
                            "Google keyword override applied: ramp=%s cohort=%s "
                            "geo=%s (using angle %s) → %d keyword(s)",
                            ramp_id, base_id, geo_group.cluster, _try_angle,
                            len(targeting["keyword_ideas"]),
                        )
                        break

            # ── Pre-campaign audience check (parity with LinkedIn Stage C). ─
            # Same 50k floor; same de-narrow loop. On below_floor we skip THIS
            # (cohort × geo) for THIS platform only — other platforms run
            # independently. On skipped (API failure / unsupported account)
            # we ship without gating, with a null audience_size in the sheet.
            from src.audience_check import (
                denarrow_for_platform,
                drop_rule_for_google,
                drop_rule_for_meta,
            )

            audience_count: int | None = None
            audience_status = "skipped"
            if platform == "meta" and hasattr(client, "get_reach_estimate"):
                audience_count, targeting, audience_status = denarrow_for_platform(
                    platform="meta",
                    targeting=targeting,
                    get_reach_fn=lambda t: client.get_reach_estimate(t),
                    drop_rule_fn=drop_rule_for_meta,
                    cohort_label=f"{getattr(cohort, '_stg_id', '?')}|{geo_group.cluster_label}",
                )
            elif platform == "google" and hasattr(client, "get_reach_estimate"):
                audience_count, targeting, audience_status = denarrow_for_platform(
                    platform="google",
                    targeting=targeting,
                    get_reach_fn=lambda t: client.get_reach_estimate(t),
                    drop_rule_fn=drop_rule_for_google,
                    cohort_label=f"{getattr(cohort, '_stg_id', '?')}|{geo_group.cluster_label}",
                )

            _is_generalist_locale = bool(
                (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale")
            )
            if audience_status == "below_floor" and _additive:
                # Additive attach re-targets nothing — it adds creatives to an ad
                # set/campaign that's ALREADY live (and already passed the floor at
                # creation). An audience recheck must never gate a creative refresh.
                log.info(
                    "_process_extra_platform_arm[%s]: audience=%s below floor but ADDITIVE — "
                    "not gating a creative refresh on an existing campaign (cohort=%s geo=%s).",
                    platform, audience_count,
                    getattr(cohort, "name", "?"),
                    getattr(geo_group, "cluster_label", "?"),
                )
            elif audience_status == "below_floor" and _is_generalist_locale:
                # Generalist/i18n locale cohort (Bug 2): de-narrowing already ran
                # above; we must NOT skip even below floor — the ramp needs these
                # users. Launch anyway. ⚠️ RELOOK: how small-but-required locale
                # audiences should be handled (data/plan_generalist_locale_targeting.md).
                log.warning(
                    "_process_extra_platform_arm[%s]: audience=%s below floor for GENERALIST "
                    "locale cohort=%s geo=%s — NOT skipping (ramp needs these users); launching "
                    "anyway. RELOOK item: revisit small-but-required locale audience handling.",
                    platform, audience_count,
                    getattr(cohort, "name", "?"),
                    getattr(geo_group, "cluster_label", "?"),
                )
            elif audience_status == "below_floor":
                log.info(
                    "_process_extra_platform_arm[%s]: audience=%s below floor — skipping "
                    "cohort=%s geo=%s for this channel (other channels unaffected)",
                    platform, audience_count,
                    getattr(cohort, "name", "?"),
                    getattr(geo_group, "cluster_label", "?"),
                )
                # Log a registry row so the reviewer sees WHY this slot is empty.
                try:
                    _reg_log(
                        smart_ramp_id=ramp_id or "",
                        cohort_id=base_id,
                        cohort_signature=cohort.name,
                        geo_cluster=geo_group.cluster,
                        geo_cluster_label=geo_group.cluster_label,
                        geos=group_geos,
                        angle="",
                        campaign_type="static",
                        advertised_rate=geo_group.advertised_rate,
                        cohort_geo=_cohort_geo_label(cohort, geo_group),
                        platform=platform,
                        meta_audience_size=audience_count if platform == "meta" else None,
                        google_audience_size=audience_count if platform == "google" else None,
                        audience_check_status=audience_status,
                        campaign_name=campaign_name,
                        generation=_gen,
                    )
                except Exception as exc:
                    log.warning(
                        "_process_extra_platform_arm[%s]: registry log of below-floor skip failed: %s",
                        platform, exc,
                    )
                continue

            _extra_budget_kwargs = (
                {"daily_budget_cents": daily_budget_cents}
                if daily_budget_cents is not None else {}
            )
            # Google Search: fold in the negative keywords Bryan approved on the
            # console for this ramp (on top of the confident config defaults that
            # _apply_negative_keywords always adds).
            if platform == "google_search" and ramp_id:
                try:
                    from src.console_db import list_approved_negative_keywords
                    _approved_neg = list_approved_negative_keywords(ramp_id)
                    if _approved_neg:
                        targeting = dict(targeting or {})
                        targeting["negative_keywords"] = (
                            list((targeting or {}).get("negative_keywords") or []) + _approved_neg
                        )
                except Exception as _exc:
                    log.warning("extra-arm: approved-negative-keyword merge failed (non-fatal): %s", _exc)
            # ── Additive attach: reuse the ad set/campaign already LIVE for this
            #    (cohort × geo) and attach new creatives to it, instead of creating
            #    a fresh container. Only when a live container exists; otherwise
            #    fall through to the normal first-launch create below. ──────────
            _attach_to_existing = False
            sub_id = None
            if _additive:
                _resolved = _resolve_container(
                    ramp_id=ramp_id or "", platform=platform, campaign_type="static",
                    cohort_signature=getattr(cohort, "name", ""), geo_cluster=geo_group.cluster,
                )
                _cid = (_resolved or {}).get("container_id") or ""
                if _cid:
                    # Meta: confirm the ad set is still live on-platform before
                    # attaching (a DELETED/ARCHIVED one can't take new ads).
                    _live = True
                    if platform == "meta" and hasattr(client, "is_live"):
                        try:
                            _live = client.is_live(_cid, level="adset")
                        except Exception as _exc:
                            log.warning("additive: is_live check failed for %s (%s) — treating as live", _cid, _exc)
                            _live = True
                    if _live:
                        _attach_to_existing = True
                        sub_id = _cid
                        _lp(**_lp_kw, status="creating")
                        out["campaigns"].append(sub_id)
                        out["campaigns_by_cohort"][by_cohort_key] = sub_id
                        log.info(
                            "_process_extra_platform_arm[%s]: ADDITIVE — attaching new creatives to "
                            "existing ad set %s cohort=%s geo=%s (%d angles); no new campaign/ad set created.",
                            platform, sub_id, base_id, geo_group.cluster_label, len(specs),
                        )
                        _lp(**_lp_kw, status="created")
                    else:
                        log.warning(
                            "_process_extra_platform_arm[%s]: ADDITIVE — existing container %s for cohort=%s "
                            "geo=%s is DELETED/ARCHIVED on-platform; creating a fresh one (first-launch fallback).",
                            platform, _cid, getattr(cohort, "name", ""), geo_group.cluster,
                        )
                else:
                    log.info(
                        "_process_extra_platform_arm[%s]: ADDITIVE — no existing live container for cohort=%s "
                        "geo=%s; creating first launch for it.",
                        platform, getattr(cohort, "name", ""), geo_group.cluster,
                    )

            if not _attach_to_existing:
                # ── First launch / replace / additive first-launch-fallback:
                #    create a fresh campaign group (lazily) + ad set. ───────────
                # Young-market workaround (Meta): drop countries Meta won't accept for
                # a young-eligible audience under EMPLOYMENT SAC (subcode 1870249, e.g.
                # Thailand) so the rest of the ad set still creates; Tuan adds them
                # manually in Ads Manager. If the ad set is young-market-ONLY, skip the
                # programmatic create entirely and flag it for manual creation.
                _manual_note = None
                if platform == "meta":
                    _geo = (targeting or {}).get("geo_locations") or {}
                    _countries = list(_geo.get("countries") or [])
                    _young = [c for c in _countries if c in config.META_YOUNG_MARKET_COUNTRIES]
                    if _young:
                        _remaining = [c for c in _countries if c not in config.META_YOUNG_MARKET_COUNTRIES]
                        _base_note = {
                            "platform": platform,
                            "cohort_signature": getattr(cohort, "name", ""),
                            "geo_cluster": geo_group.cluster,
                            "geo_cluster_label": getattr(geo_group, "cluster_label", ""),
                            "dropped_countries": _young,
                            "campaign_name": campaign_name,
                        }
                        if _remaining:
                            targeting = dict(targeting or {})
                            targeting["geo_locations"] = {**_geo, "countries": _remaining}
                            log.warning(
                                "_process_extra_platform_arm[meta]: dropping young-market %s from ad set "
                                "(cohort=%s geo=%s) — creating for %s; Tuan to add %s manually (Meta 1870249)",
                                _young, getattr(cohort, "name", ""), geo_group.cluster, _remaining, _young,
                            )
                            _manual_note = {**_base_note, "reason": "add these countries manually in Ads Manager (Meta young-market age restriction 1870249)"}
                        else:
                            log.warning(
                                "_process_extra_platform_arm[meta]: ad set is young-market-only %s "
                                "(cohort=%s geo=%s) — skipping programmatic create; Tuan to create manually",
                                _young, getattr(cohort, "name", ""), geo_group.cluster,
                            )
                            manual_location_adds.append({**_base_note, "whole_adset": True, "reason": "entire ad set is young-market-only — create manually in Ads Manager (Meta 1870249)"})
                            _lp(**_lp_kw, status="failed", error=f"young-market {_young}: manual creation needed (Meta 1870249)")
                            continue

                # Parent campaign-group is created lazily (up front for non-additive;
                # here for an additive first-launch fallback).
                group_id = _ensure_group()
                if group_id is None:
                    _lp(**_lp_kw, status="failed", error="parent campaign-group creation failed")
                    continue

                _lp(**_lp_kw, status="creating")
                sub_id = client.create_campaign(
                    name=campaign_name,
                    campaign_group_id=group_id,
                    targeting=targeting,
                    **_extra_budget_kwargs,
                )
                if _manual_note is not None:
                    _manual_note["platform_campaign_id"] = sub_id
                    manual_location_adds.append(_manual_note)
                out["campaigns"].append(sub_id)
                out["campaigns_by_cohort"][by_cohort_key] = sub_id
                log.info(
                    "_process_extra_platform_arm[%s]: ad set/group %s cohort=%s geo=%s (%d angles)",
                    platform, sub_id, base_id, geo_group.cluster_label, len(specs),
                )
                _lp(**_lp_kw, status="created")
        except Exception as exc:
            log.exception(
                "_process_extra_platform_arm[%s]: ad set creation failed cohort=%s geo=%s — skipping all angles: %s",
                platform, getattr(cohort, "name", "?"),
                getattr(geo_group, "cluster_label", "?"), exc,
            )
            _lp(**_lp_kw, status="failed", error=str(exc))
            continue

        # ── Attach one ad/RDA per angle to the just-created ad set/group. ───
        # Body is an inner function so the verify-and-heal retry (piece C) can
        # re-invoke a failed spec without duplicating the upload/UTM/dispatch.
        # Returns True iff an ad attached (ad_result.status == "ok").
        def _attempt_and_record(spec) -> bool:
            angle_label = spec.get("angle_label", "A")
            angle_idx   = spec.get("angle_idx", 0)
            png_path    = spec.get("png_path")
            qc_report_e = spec.get("qc_report") or {}
            variant     = variants[angle_idx] if angle_idx < len(variants) else {}
            row_id = f"{by_cohort_key}_{angle_label}"

            platform_copy = adapt_copy_for_platform(variant, platform, icp=getattr(cohort, "_icp", None), locale=resolve_copy_locale(cohort, getattr(cohort, "_icp", None))) if variant else {}

            # Drive URL was uploaded in Phase 1; reuse the cached URL to avoid
            # a duplicate Drive write (idempotent at the filename level — the
            # API would create a 2nd file with same name otherwise).
            spec_key = f"{base_id}_{geo_group.campaign_suffix}_{angle_label}"
            drive_url = drive_urls_by_spec.get(spec_key, "")
            if not drive_url and config.GDRIVE_ENABLED and png_path and Path(str(png_path)).exists():
                drive_url = _save_creative_to_drive(
                    png_path=png_path,
                    ramp_id=ramp_id or "manual",
                    unique_id=row_id,
                    channel=platform,
                    angle=angle_label,
                    cohort_geo=_cohort_geo_label(cohort, geo_group),
                )

            ad_result: CreateAdResult
            # 2026-05-24: google_search is text-only (Responsive Search Ad).
            # No image_id, no upload_image, no PNG required — keyword targeting
            # + ad text are everything the RSA needs.
            if platform == "google_search":
                image_id = ""  # unused by create_search_ad
                # Build UTM destination URL inline (mirrors the else-branch below).
                from src.utm_builder import build_utm_url, resolve_base_lp_url
                base_lp = resolve_base_lp_url(
                    campaign_state=(naming_meta or {}).get("campaign_state"),
                    platform=platform,
                    fallback=destination_url_override or config.LINKEDIN_DESTINATION,
                    matched_domain=(naming_meta or {}).get("domain"),
                    sheets_client=sheets,
                    ramp_id=ramp_id,
                    cohort_id=cohort_id_override or getattr(cohort, "id", None) or "",
                )
                utm_url = build_utm_url(
                    base_url=base_lp, platform=platform,
                    campaign_name=campaign_name,
                    pod=(naming_meta or {}).get("pod"),
                    domain=(naming_meta or {}).get("domain"),
                    locale=(naming_meta or {}).get("locale"),
                    language="EN",
                    utm_content=f"{cohort._stg_id}-{platform}-{angle_label}",
                ) if base_lp else (destination_url_override or "")
                # Fall through to the create_*_ad dispatch below — it has
                # the google_search branch.
                _ad_dispatch_ready = True
            elif not png_path or not Path(str(png_path)).exists():
                ad_result = CreateAdResult(
                    status="local_fallback",
                    error_class="MissingPNG",
                    error_message="static-arm produced no PNG for this spec",
                )
                _ad_dispatch_ready = False
            else:
                _ad_dispatch_ready = False
                try:
                    image_id = client.upload_image(png_path)
                except Exception as exc:
                    log.warning(
                        "_process_extra_platform_arm[%s]: upload_image failed angle=%s — %s",
                        platform, angle_label, exc,
                    )
                    ad_result = CreateAdResult(
                        status="error",
                        error_class=type(exc).__name__,
                        error_message=str(exc)[:300],
                    )
                else:
                    _ad_dispatch_ready = True
                    # Build the platform-specific UTM destination URL.
                    from src.utm_builder import build_utm_url, resolve_base_lp_url
                    base_lp = resolve_base_lp_url(
                        campaign_state=(naming_meta or {}).get("campaign_state"),
                        platform=platform,
                        fallback=destination_url_override or config.LINKEDIN_DESTINATION,
                        matched_domain=(naming_meta or {}).get("domain"),
                        sheets_client=sheets,
                        ramp_id=ramp_id,
                        cohort_id=cohort_id_override or getattr(cohort, "id", None) or "",
                    )
                    utm_url = build_utm_url(
                        base_url=base_lp, platform=platform,
                        campaign_name=campaign_name,
                        pod=(naming_meta or {}).get("pod"),
                        domain=(naming_meta or {}).get("domain"),
                        locale=(naming_meta or {}).get("locale"),
                        language="EN",
                        utm_content=f"{cohort._stg_id}-{platform}-{angle_label}",
                    ) if base_lp else (destination_url_override or "")

            # Ad-creation dispatch (lifted out of the upload_image else-branch
            # 2026-05-24 so google_search short-circuit reaches it without
            # needing an image_id).
            if _ad_dispatch_ready:
                if platform == "google_search":
                    # Responsive Search Ad. No image. RSA needs ≥3 short
                    # headlines (≤30c each) + ≥2 descriptions (≤90c).
                    # Synthesise from existing variant fields; create_search_ad
                    # truncates each to RSA limits.
                    _rsa_headlines = [
                        h for h in (
                            variant.get("headline", "") if variant else "",
                            variant.get("subheadline", "") if variant else "",
                            variant.get("ad_headline", "") if variant else "",
                        ) if h
                    ]
                    for extra in (platform_copy.get("headlines") or []):
                        if extra and extra not in _rsa_headlines:
                            _rsa_headlines.append(extra)
                    _rsa_descs = [
                        d for d in (
                            variant.get("intro_text", "") if variant else "",
                            variant.get("ad_description", "") if variant else "",
                        ) if d
                    ]
                    for extra in (platform_copy.get("descriptions") or []):
                        if extra and extra not in _rsa_descs:
                            _rsa_descs.append(extra)
                    ad_result = client.create_search_ad(
                        ad_group_resource=sub_id,
                        headlines=_rsa_headlines,
                        descriptions=_rsa_descs,
                        destination_url=utm_url,
                        ad_name=f"{campaign_name} | {angle_label}",
                    )
                elif platform == "google":
                    ad_result = client.create_image_ad(
                        campaign_id=sub_id,
                        image_id=image_id,
                        headline=(platform_copy.get("headlines") or [""])[0],
                        description=(platform_copy.get("descriptions") or [""])[0],
                        destination_url=utm_url,
                        headlines=platform_copy.get("headlines") or [],
                        long_headline=platform_copy.get("long_headline") or "",
                        descriptions=platform_copy.get("descriptions") or [],
                        # 2026-05-18: pass the local PNG path so
                        # create_image_ad can generate the 1.91:1
                        # landscape variant Google RDA requires.
                        local_png_path=str(png_path) if png_path else None,
                        ad_name=f"{campaign_name} | {angle_label}",
                    )
                elif platform == "reddit":
                    # Reddit image ad: title=headline, free-form body/title carried
                    # for the native text-post variant. Returns local_fallback while
                    # REDDIT_API_ENABLED is off (creative-only); raises when on-but-
                    # unimplemented (caught per-cohort) — Phase 1 already exported.
                    ad_result = client.create_image_ad(
                        campaign_id=sub_id,
                        image_id=image_id,
                        headline=platform_copy.get("title", ""),
                        description=platform_copy.get("freeform_body", ""),
                        intro_text=platform_copy.get("freeform_title", ""),
                        cta=platform_copy.get("cta"),
                        destination_url=utm_url,
                        ad_name=f"{campaign_name} | {angle_label}",
                    )
                else:  # meta
                    ad_result = client.create_image_ad(
                        campaign_id=sub_id,
                        image_id=image_id,
                        headline=platform_copy.get("headline", ""),
                        description=platform_copy.get("description", ""),
                        primary_text=platform_copy.get("primary_text"),
                        cta=platform_copy.get("cta"),
                        destination_url=utm_url,
                        ad_name=f"{campaign_name} | {angle_label}",
                    )

            # Registry: one row per (cohort × geo × angle), shared
            # platform_campaign_id, distinct platform_creative_id.
            try:
                _reg_log(
                    smart_ramp_id=ramp_id or "",
                    cohort_id=base_id,
                    cohort_signature=cohort.name,
                    geo_cluster=geo_group.cluster,
                    geo_cluster_label=geo_group.cluster_label,
                    geos=group_geos,
                    angle=angle_label,
                    campaign_type="static",
                    advertised_rate=geo_group.advertised_rate,
                    headline=variant.get("headline", "") if variant else "",
                    subheadline=variant.get("subheadline", "") if variant else "",
                    photo_subject=variant.get("photo_subject", "") if variant else "",
                    creative_image_path=drive_url or (str(png_path) if png_path else ""),
                    cohort_geo=_cohort_geo_label(cohort, geo_group),
                    platform=platform,
                    platform_campaign_id=sub_id,
                    platform_creative_id=ad_result.creative_id or "",
                    qc_verdict=qc_report_e.get("verdict", ""),
                    qc_attempts=qc_report_e.get("attempts"),
                    qc_violations=qc_report_e.get("violations"),
                    meta_audience_size=audience_count if platform == "meta" else None,
                    google_audience_size=audience_count if platform == "google" else None,
                    audience_check_status=audience_status,
                    campaign_name=campaign_name,
                    generation=_gen,
                    # Google search keywords (review surface for Diego/Bryan in the
                    # outlier-campaign-console keywords-card). Captured from the same
                    # targeting dict that's about to be applied to the Search ad-group
                    # via _apply_keyword_criteria. Null for Meta rows.
                    google_keywords=(
                        list((targeting or {}).get("keyword_ideas") or [])[:30]
                        if platform in ("google", "google_search") else None
                    ),
                )
            except Exception as exc:
                log.warning(
                    "_process_extra_platform_arm[%s]: registry log failed angle=%s (non-fatal): %s",
                    platform, angle_label, exc,
                )

            out["creative_paths"][row_id] = (
                ad_result.creative_id if ad_result.status == "ok"
                else (str(png_path) if png_path else "")
            )
            if ad_result.status != "ok":
                # Stash the real failure so verify-and-heal can surface WHY this
                # ad couldn't be created (console + Slack), not just "0 ads".
                spec["_last_error"] = (
                    f"{ad_result.error_class or 'error'}: "
                    f"{ad_result.error_message or 'unknown'}"
                )[:300]
            return ad_result.status == "ok"

        # First pass: one ad per angle.
        ads_ok = 0
        failed_specs: list[dict] = []
        for spec in specs:
            if _attempt_and_record(spec):
                ads_ok += 1
            else:
                failed_specs.append(spec)

        # ── Additive attach with zero new ads: NEVER touch the existing ad set.
        # It already has its prior (live) creatives — a failed creative refresh
        # must be a safe no-op, not a heal that archives/deletes the container.
        if _attach_to_existing and ads_ok == 0:
            log.warning(
                "_process_extra_platform_arm[%s]: ADDITIVE — 0 new creatives attached to existing "
                "ad set %s (cohort=%s geo=%s); leaving it untouched (no heal). Reasons: %s",
                platform, sub_id, getattr(cohort, "name", "?"), geo_group.cluster_label,
                "; ".join(dict.fromkeys(s.get("_last_error", "") for s in specs if s.get("_last_error"))) or "unknown",
            )
            continue

        # ── Verify-and-heal (piece C). If the ad set/group ended the run with
        # zero ads, retry the failed specs once; if still empty, archive the
        # container + flag so no empty shell survives the launch. (Skipped for
        # additive attach above — there's no fresh shell to heal.)
        if config.LAUNCH_VERIFY_ENABLED and ads_ok == 0 and failed_specs:
            log.warning(
                "_process_extra_platform_arm[%s]: 0/%d ads attached to %s — "
                "retrying %d spec(s) once before heal",
                platform, len(specs), sub_id, len(failed_specs),
            )
            for spec in failed_specs:
                if _attempt_and_record(spec):
                    ads_ok += 1
            if ads_ok == 0:
                _errs = list(dict.fromkeys(
                    s["_last_error"] for s in specs if s.get("_last_error")
                ))
                _reason = (
                    "; ".join(_errs)[:400] if _errs
                    else f"0/{len(specs)} ads attached after retry"
                )
                _summ = launch_verify.heal_empty(
                    platform=platform,
                    container_id=sub_id,
                    ramp_id=ramp_id or "",
                    campaign_name=campaign_name,
                    reason=_reason,
                )
                if _summ:
                    healed_empties.append(_summ)

        # Surface keywords Google rejected on an otherwise-healthy Search
        # campaign (live with the survivors — not an empty heal, a "needs
        # review"). dropped_keywords is keyed by ad_group_resource (= sub_id).
        if config.LAUNCH_VERIFY_ENABLED and platform == "google_search" and ads_ok > 0:
            _dropped_kw = (getattr(client, "dropped_keywords", {}) or {}).get(sub_id) or []
            if _dropped_kw:
                _note = launch_verify.record_keywords_dropped(
                    ramp_id=ramp_id or "",
                    container_id=sub_id,
                    campaign_name=campaign_name,
                    dropped=_dropped_kw,
                )
                if _note:
                    keyword_drops.append(_note)

    if config.LAUNCH_VERIFY_ENABLED:
        launch_verify.notify_healed(ramp_id or "", healed_empties)
        launch_verify.notify_keywords_dropped(ramp_id or "", keyword_drops)
    # Independent of verify-and-heal: flag young-market countries Tuan must add
    # manually (Meta 1870249). Persists to the audit log + DMs Tuan.
    launch_verify.notify_manual_geo_add(ramp_id or "", manual_location_adds)

    return out


def _build_extra_platform_clients(enabled: list[str]) -> dict:
    """Lazy-construct the non-LinkedIn AdPlatformClient + TargetingResolver pairs.

    Returns dict[str, dict] keyed by platform name with shape
    `{platform: {"client": <client>, "resolver": <resolver>}}`. Skips
    platforms whose required env vars are missing (logged at info level).
    Always returns an empty dict for "linkedin" since that arm runs through
    the existing `_process_static_campaigns` code path.
    """
    out: dict[str, dict] = {}
    if "meta" in enabled:
        if config.META_ACCESS_TOKEN and config.META_AD_ACCOUNT_ID:
            from src.meta_api import MetaClient
            from src.meta_targeting import MetaInterestResolver
            try:
                out["meta"] = {
                    "client":   MetaClient(),
                    "resolver": MetaInterestResolver(),
                }
            except Exception as exc:
                log.warning("Skipping Meta arm — init failed: %s", exc)
        else:
            log.info("Skipping Meta arm — META_ACCESS_TOKEN / META_AD_ACCOUNT_ID not set")
    google_creds_ok = (
        config.GOOGLE_ADS_DEVELOPER_TOKEN
        and config.GOOGLE_ADS_REFRESH_TOKEN
        and config.GOOGLE_ADS_CUSTOMER_ID
    )
    if "google" in enabled:
        if google_creds_ok:
            from src.google_ads_api import GoogleAdsClient
            from src.google_targeting import GoogleSegmentResolver
            try:
                out["google"] = {
                    "client":   GoogleAdsClient(channel="display"),
                    "resolver": GoogleSegmentResolver(),
                }
            except Exception as exc:
                log.warning("Skipping Google arm — init failed: %s", exc)
        else:
            log.info(
                "Skipping Google arm — GOOGLE_ADS_DEVELOPER_TOKEN / GOOGLE_ADS_REFRESH_TOKEN / "
                "GOOGLE_ADS_CUSTOMER_ID not all set"
            )
    # 2026-05-24 — google_search is the sibling Search arm. Mirrors Diego's
    # manual Search campaigns: SEARCH channel + RSA + keyword criteria via
    # KeywordPlanIdeaService. Can run alongside or instead of "google"
    # (Display). Shares Google Ads creds with the Display arm.
    if "google_search" in enabled:
        if google_creds_ok:
            from src.google_ads_api import GoogleAdsClient
            from src.google_targeting import GoogleSegmentResolver
            try:
                out["google_search"] = {
                    "client":   GoogleAdsClient(channel="search"),
                    "resolver": GoogleSegmentResolver(),
                }
            except Exception as exc:
                log.warning("Skipping Google Search arm — init failed: %s", exc)
        else:
            log.info(
                "Skipping Google Search arm — GOOGLE_ADS_DEVELOPER_TOKEN / "
                "GOOGLE_ADS_REFRESH_TOKEN / GOOGLE_ADS_CUSTOMER_ID not all set"
            )
    # 2026-06-11 — Reddit. Built UNCONDITIONALLY (unlike Meta/Google cred-gating):
    # v1 is creative-only (export image + free-form creatives + manifest to Drive
    # for manual upload), which needs no API creds. RedditClient self-gates its
    # programmatic create methods on config.REDDIT_API_ENABLED, so when the
    # allow-list API access lands, flipping the flag upgrades this same arm to
    # full programmatic create with no dispatch change.
    if "reddit" in enabled:
        from src.reddit_api import RedditClient
        from src.reddit_targeting import RedditSubredditResolver
        try:
            out["reddit"] = {
                "client":   RedditClient(),
                "resolver": RedditSubredditResolver(),
            }
        except Exception as exc:
            log.warning("Skipping Reddit arm — init failed: %s", exc)
    # 2026-07-09 — TikTok. Built UNCONDITIONALLY like Reddit: v1 is creative-only
    # (export 9:16/1:1 images + manifest to Drive for manual upload), needing no
    # API creds. TikTokClient self-gates its programmatic create methods on
    # config.TIKTOK_API_ENABLED, so flipping the flag (once the Marketing API
    # token lands) upgrades this same arm to full programmatic create with no
    # dispatch change.
    if "tiktok" in enabled:
        from src.tiktok_api import TikTokClient
        from src.tiktok_targeting import TikTokTargetingResolver
        try:
            out["tiktok"] = {
                "client":   TikTokClient(),
                "resolver": TikTokTargetingResolver(),
            }
        except Exception as exc:
            log.warning("Skipping TikTok arm — init failed: %s", exc)
    return out


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
    seen_inmail_keys: set | None = None,
    seen_static_keys: set | None = None,
    prep_only: bool = False,
    channels: list[str] | None = None,
    budgets: dict[str, int] | None = None,
) -> dict:
    # Naming metadata bundle — read once from the row dict so the arm
    # functions can forward to src.campaign_name.build_campaign_name without
    # parsing the row shape themselves. See _ramp_to_rows for fields.
    naming_meta = {
        "submitted_at":  row.get("ramp_submitted_at", "") or "",
        "pod":           row.get("job_post_pod"),
        # Name "domain" segment = Smart Ramp tool's job_post_domain (e.g. "bn-IN"),
        # falling back to matched_domain for ramps that don't carry it. When the
        # Smart Ramp domain matcher FAILED (domain_not_found), job_post_domain is a
        # junk guess (e.g. a BLV ramp tagged "Media & Communications") — prefer
        # matched_domain instead so the name isn't actively misleading.
        "domain": (
            (row.get("matched_domain") or row.get("job_post_domain"))
            if row.get("domain_match_failed")
            else (row.get("job_post_domain") or row.get("matched_domain"))
        ),
        "locale":        row.get("job_post_language_code"),
        "included_geos": row.get("included_geos") or [],
        "campaign_state": row.get("campaign_state"),
    }

    # Ground copy in the real task: build the task card ONCE per row (LP scrape +
    # Smart Ramp fields → what they do / device / artifact) and cache it by
    # (ramp_id, cohort_id). Every copy generator (InMail + Phase-2 → which all
    # channels reshape) reads it via task_card.cached_card and grounds the copy
    # in these facts instead of inventing them. No-op when TASK_GROUNDING_ENABLED
    # is off; stays general (never fabricates) when grounding is thin.
    if config.TASK_GROUNDING_ENABLED:
        try:
            from src.task_card import warm_task_card
            warm_task_card(
                ramp_id, row.get("cohort_id"),
                lp_url=row.get("selected_lp_url"),
                ramp_summary=row.get("ramp_summary", "") or "",
                cohort_description=row.get("cohort_description", "") or "",
            )
        except Exception as _exc:
            log.warning("task-card warm failed (non-fatal, copy stays ungrounded): %s", _exc)

    """Phase 2.6: run cohort discovery ONCE per row, then dispatch BOTH InMail
    + Static arms.

    Per-arm isolation: each arm wrapped in try/except so one arm's crash never
    aborts the other. Per-cohort isolation lives inside each arm.

    OUTLIER_ARMS env override (2026-05-13): comma-separated subset of
    {"inmail","static"}. Lets a manual rerun process only a specific arm
    without rebuilding Static (e.g. when fixing an InMail bug and the prior
    Static campaigns are still valid DRAFTs). Empty/unset → use the `modes`
    arg as-is.
    """
    import os as _os
    _arms_env = (_os.environ.get("OUTLIER_ARMS") or "").strip()
    if _arms_env:
        _override = tuple(a.strip().lower() for a in _arms_env.split(",") if a.strip())
        _override = tuple(a for a in _override if a in ("inmail", "static"))
        if _override:
            log.info(
                "_process_row_both_modes: OUTLIER_ARMS env override active — modes=%s (default was %s)",
                _override, modes,
            )
            modes = _override

    # OUTLIER_CHANNELS env override (2026-05-20): comma-separated subset of
    # {"linkedin","meta","google"}. Mirrors OUTLIER_ARMS but for the platform
    # axis — lets a manual rerun create only Meta campaigns (no LinkedIn /
    # Google touch). Used for GMR-0021's Meta-only end-to-end trigger.
    # When set, overrides the `channels` arg (which normally comes from the
    # console approval decision); the env var wins so a one-off CLI rerun
    # doesn't need to fake a console decision.
    _channels_env = (_os.environ.get("OUTLIER_CHANNELS") or "").strip()
    if _channels_env:
        _channels_override = [c.strip().lower() for c in _channels_env.split(",") if c.strip()]
        _channels_override = [c for c in _channels_override if c in ("linkedin", "meta", "google")]
        if _channels_override:
            log.info(
                "_process_row_both_modes: OUTLIER_CHANNELS env override active — "
                "channels=%s (default was %s)",
                _channels_override, channels,
            )
            channels = _channels_override

    # Phase 2 — per-ramp channel override (from console approval decision).
    # `channels` is a subset of {'linkedin','meta','google'}. Wins over both
    # the OUTLIER_ARMS env var and config.ENABLED_PLATFORMS for this row.
    # 'linkedin' implies both InMail + Static; absence → drop both LI arms
    # in name but the static path still runs in spec-only mode (Phase A only)
    # so Meta + Google arms downstream get campaign_specs + PNGs to consume.
    # Phase A vs Phase B is gated explicitly via create_linkedin_campaigns on
    # the _process_static_campaigns call below.
    linkedin_enabled = channels is None or "linkedin" in channels
    if not linkedin_enabled and modes:
        # Channels excludes LinkedIn → drop InMail entirely (it has no
        # spec-only fallback) but KEEP "static" so Meta/Google can consume
        # the specs. Phase B inside the static arm is gated separately by
        # passing create_linkedin_campaigns=False to _process_static_campaigns.
        kept = tuple(m for m in modes if m == "static")
        log.info(
            "_process_row_both_modes: channels=%s excludes linkedin — "
            "keeping static arm in spec-only mode (no LinkedIn API calls) "
            "so Meta/Google can reuse specs (was %s, now %s)",
            channels, modes, kept,
        )
        modes = kept

    # Per-channel budget cents (None → platform client uses its PLACEHOLDER).
    linkedin_budget_cents = (budgets or {}).get("linkedin")
    flow_id = row.get("flow_id", "")
    location = row.get("location", "")
    config_name = row.get("config_name") or flow_id
    project_id = row.get("project_id")
    cohort_id_override = row.get("cohort_id")
    destination_url_override = row.get("selected_lp_url")
    included_geos = row.get("included_geos", []) or []
    unique_id = row.get("unique_id", f"ROW_{row.get('sheet_row', 'UNKNOWN')}")

    # Pay-rate resolution. Priority chain (per Pranav 2026-06-09: pull from
    # Smart Ramp first, never guess — see feedback_smart_ramp_authoritative_data):
    #   1. OUTLIER_BASE_RATE_USD env var (manual override for one-off runs)
    #   2. Smart Ramp `job_post_pay_rates` (e.g. "up to $35 /hr") on the row
    #   3. (manual/agent step) canonical pay-rate file — not runtime-readable
    # When nothing resolves: base_rate_usd stays None and downstream callers
    # (group_geos_for_campaigns, copy gen) ship rate-free copy. NEVER hardcode
    # a $50 default — wrong rate in ads is a critical risk.
    import os as _os
    base_rate_usd: float | None = None
    # True when base_rate_usd is the Smart Ramp job_post_pay_rates value — an
    # authoritative, locale/geo-specific advertised rate that must NOT be passed
    # through the country pay-multiplier or $5 rounding (that mangled $22.50→$20
    # for he-IL, $7.50→$5 for kn-IN). Only a US-baseline OUTLIER_BASE_RATE_USD
    # gets the geo multiplier. See [[feedback_smart_ramp_authoritative_data]].
    rate_geo_specific = False
    _env_rate = (_os.environ.get("OUTLIER_BASE_RATE_USD") or "").strip()
    if _env_rate:
        try:
            base_rate_usd = float(_env_rate)
            log.info(
                "_process_row_both_modes: OUTLIER_BASE_RATE_USD env override → "
                "base_rate_usd=$%.2f/hr",
                base_rate_usd,
            )
        except ValueError:
            log.warning(
                "_process_row_both_modes: OUTLIER_BASE_RATE_USD=%r is not a valid "
                "float — falling back to None (rate-free copy)",
                _env_rate,
            )
    if base_rate_usd is None:
        # Smart Ramp job-post rate (authoritative). Carried on the row from
        # CohortSpec.job_post_pay_rates. parse_job_post_pay_rate takes the
        # headline (max) figure from "up to $X /hr" / "$X-$Y" / "$X/hr".
        from src.attribution_resolver import parse_job_post_pay_rate
        _sr_rate = parse_job_post_pay_rate(row.get("job_post_pay_rates"))
        if _sr_rate is not None:
            base_rate_usd = _sr_rate
            rate_geo_specific = True
            log.info(
                "_process_row_both_modes: pay rate from Smart Ramp job_post_pay_rates "
                "%r → base_rate_usd=$%.2f/hr (geo-specific, no multiplier)",
                row.get("job_post_pay_rates"), base_rate_usd,
            )
    if base_rate_usd is None:
        log.warning(
            "_process_row_both_modes: base_rate_usd unresolved (no OUTLIER_BASE_RATE_USD, "
            "no Smart Ramp job_post_pay_rates). Copy gen will skip $/hr mentions — "
            "supply the rate from the canonical pay-rate file if needed."
        )

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

    # Phase 1.4 — prep_only short-circuit. Cohorts have been mined + written to
    # the Triggers Sheet by _resolve_cohorts; that's enough for the UI to show
    # the ramp as "ready for review". The arms (which generate copy, images,
    # and call platform create_campaign) are gated until Diego/Bryan approve
    # via outlier-campaign-console.
    #
    # 2026-05-22 — Brief-review gate: ALSO generate Phase-1 briefs here for
    # LinkedIn so the console can show them in the BriefReviewCard. The poller
    # then transitions the ramp to 'awaiting_brief_review' when briefs were
    # persisted (or falls back to 'awaiting_approval' on failure). The actual
    # ad copy / image generation still runs only after launch fires — Phase 2
    # (build_copy_from_brief) reads the (possibly reviewer-edited) brief at
    # launch time.
    if prep_only:
        briefs_generated = 0
        try:
            from src.brief_generator import build_briefs, _DEFAULT_ANGLE_LABELS
            from src.ui_decisions import upsert_brief, upsert_cohort_brief_rationale
            from src.geo_tiers import group_geos_for_campaigns, GeoCampaignGroup
            import config as _cfg

            raw_geos = included_geos or []
            geo_groups = group_geos_for_campaigns(
                raw_geos, base_rate_usd, apply_geo_multiplier=not rate_geo_specific,
            )
            if not geo_groups:
                # Mirror the static arm's fallback so brief gen still happens
                # even when the geo grouper returns empty.
                geo_groups = [GeoCampaignGroup(
                    cluster="global_mix", cluster_label="Global", geos=raw_geos,
                    median_multiplier=1.0,
                    advertised_rate=_fmt_advertised_rate(base_rate_usd),
                    campaign_suffix="global",
                )]

            capped_cohorts = resolved.selected[:_cfg.MAX_COHORTS_PER_GEO_CLUSTER]
            # 2026-05-23 — Phase-1 briefs are now generated PER CHANNEL so the
            # reviewer sees LinkedIn vs Meta vs Google variants side-by-side.
            # Channels gated by config.ENABLED_PLATFORMS; LinkedIn always runs
            # (the static + InMail arms remain the canonical Outlier surface).
            _platforms_csv = (getattr(_cfg, "ENABLED_PLATFORMS", "") or "").lower()
            _platforms_list = [p.strip() for p in _platforms_csv.split(",") if p.strip()]
            enabled_channels = ["linkedin"]
            # Meta/Google/Reddit/TikTok get their own Phase-1 briefs when enabled,
            # so the console can review each channel's variants. The launch arm
            # reads per-channel briefs (list_briefs_for_ramp(channel=platform)).
            for ch in ("meta", "google", "reddit", "tiktok"):
                if ch in _platforms_list:
                    enabled_channels.append(ch)

            log.info(
                "_process_row_both_modes[prep_only]: generating briefs for "
                "%d cohort(s) × %d geo cluster(s) × %d channel(s) × 3 angles = %d briefs",
                len(capped_cohorts), len(geo_groups), len(enabled_channels),
                len(capped_cohorts) * len(geo_groups) * len(enabled_channels) * 3,
            )

            for cohort in capped_cohorts:
                for geo_group in geo_groups:
                    for channel in enabled_channels:
                        try:
                            briefs = build_briefs(
                                cohort,
                                geos=geo_group.geos or raw_geos,
                                description_hint=row.get("description_hint", "") or "",
                                hourly_rate=geo_group.advertised_rate or "",
                                geo_icp_hint="",  # geo ICP hints not yet wired into prep path
                                icp=getattr(cohort, "_icp", None),
                                channel=channel,
                                task_card=cached_card(ramp_id, cohort_id_override),
                            )
                        except Exception as exc:
                            log.warning(
                                "Brief gen failed for cohort=%s geo=%s channel=%s: %s — skipping",
                                cohort.name, geo_group.cluster, channel, exc,
                            )
                            continue
                        for brief in briefs:
                            _bid = cohort_id_override or getattr(cohort, "_stg_id", "") or cohort.name
                            _angle = brief.get("angle", "A")
                            upsert_brief(
                                ramp_id=ramp_id,
                                cohort_id=_bid,
                                cohort_signature=cohort.name,
                                geo_cluster=geo_group.cluster or "global_mix",
                                channel=channel,
                                angle=_angle,
                                brief=brief,
                            )
                            # Also persist the per-angle rationale so the
                            # console's "Angles we'd test" card (AnglesCard reads
                            # cohort_brief_rationale) fills at PREP time. Without
                            # this it only populated at launch via
                            # _persist_cohort_rationales, so pre-launch ramps
                            # showed an empty card. Maps the Phase-1 brief's
                            # directional fields; launch later overwrites the
                            # same (ramp,cohort,channel,angle,geo) row with final
                            # Phase-2 copy.
                            try:
                                upsert_cohort_brief_rationale(
                                    ramp_id=ramp_id,
                                    cohort_id=_bid,
                                    cohort_signature=cohort.name,
                                    channel=channel,
                                    angle=_angle,
                                    geo_cluster=geo_group.cluster or "global_mix",
                                    angle_label=brief.get("angle_label")
                                        or _DEFAULT_ANGLE_LABELS.get(_angle, ""),
                                    headline=brief.get("headline_direction", "") or "",
                                    subheadline=brief.get("subheadline_direction", "") or "",
                                    photo_subject=brief.get("photo_direction", "") or "",
                                    rationale=brief.get("angle_hook", "") or "",
                                    competitor_signal=brief.get("competitor_signal", "") or "",
                                )
                            except Exception as _exc:
                                log.debug("prep rationale persist skipped (%s)", _exc)
                            briefs_generated += 1
            log.info(
                "_process_row_both_modes[prep_only]: %d brief(s) persisted to cohort_briefs",
                briefs_generated,
            )
        except Exception as exc:
            # Best-effort: brief gen failure must NOT block the prep return —
            # the poller will fall back to upsert_awaiting_approval (no brief
            # gate) if briefs_generated == 0.
            log.warning(
                "_process_row_both_modes[prep_only]: brief generation surface — "
                "continuing without briefs (%s)", exc,
            )

        log.info(
            "_process_row_both_modes[prep_only]: %d cohort(s) mined for row %s — "
            "skipping arms; awaiting approval (briefs_generated=%d)",
            len(resolved.selected), cohort_id_override or flow_id or "?",
            briefs_generated,
        )
        return {
            "ok": True,
            "prep_only": True,
            "briefs_generated": briefs_generated,
            "cohorts_mined": [
                {
                    "name": c.name,
                    "stg_id": getattr(c, "_stg_id", ""),
                    "stg_name": getattr(c, "_stg_name", ""),
                }
                for c in resolved.selected
            ],
            "campaign_groups": [],
            "inmail_campaigns": [],
            "static_campaigns": [],
            "creative_paths": {},
            "per_cohort": [],
        }

    inmail_result: dict = {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": []}
    static_result: dict = {"campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": []}

    # ── Phase 3.3 — InMail + Static arms run concurrently ──────────────────
    # Pre-3.3 the two arms ran sequentially (~25 min + ~25 min = ~50 min);
    # they now overlap into ~25 min wall-clock per ramp. Shared mutable
    # state is gated by locks added in the same release:
    #   - SheetsClient._write_lock: serializes update_li_campaign_id /
    #     write_creative / write_registry_row across the two arms
    #   - campaign_registry._registry_lock (RLock): serializes the registry
    #     load → mutate → save windows in both log_campaign and the inline
    #     patch in _process_static_campaigns
    #   - LinkedInClient._refresh_lock: serializes token refresh + session
    #     header update so two arms can't double-refresh on a 401
    # The Meta + Google fan-out stays sequential after Static completes
    # because it reuses Static's PNGs.
    import concurrent.futures as _cf

    def _run_inmail() -> dict:
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
                ramp_id=ramp_id,
                cohort_id_override=cohort_id_override,
                cohort_description=resolved.smart_ramp_brief,
                unique_id=unique_id,
                naming_meta=naming_meta,
                seen_keys=seen_inmail_keys,
                daily_budget_cents=linkedin_budget_cents,
                base_rate_usd=base_rate_usd,
                rate_geo_specific=rate_geo_specific,
            )
            return r if isinstance(r, dict) else {}
        except Exception:
            log.exception(
                "_process_row_both_modes: InMail arm aborted — Static arm preserved"
            )
            return {}

    def _run_static() -> dict:
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
                cohort_description=resolved.smart_ramp_brief,
                unique_id=unique_id,
                naming_meta=naming_meta,
                seen_keys=seen_static_keys,
                daily_budget_cents=linkedin_budget_cents,
                base_rate_usd=base_rate_usd,
                rate_geo_specific=rate_geo_specific,
                create_linkedin_campaigns=linkedin_enabled,
            )
            return r if isinstance(r, dict) else {}
        except Exception:
            log.exception(
                "_process_row_both_modes: Static arm aborted — InMail arm preserved"
            )
            return {}

    arm_funcs: dict[str, "callable"] = {}
    if "inmail" in modes:
        arm_funcs["inmail"] = _run_inmail
    if "static" in modes:
        arm_funcs["static"] = _run_static

    if len(arm_funcs) <= 1 or dry_run:
        # Single-arm or dry-run: skip the executor overhead and run inline.
        # dry_run keeps sequential ordering to make logs deterministic for
        # test fixtures that compare log output across runs.
        for name, fn in arm_funcs.items():
            r = fn()
            if name == "inmail":
                inmail_result.update(r)
            else:
                static_result.update(r)
    else:
        log.info(
            "_process_row_both_modes: running InMail + Static arms concurrently (Phase 3.3)"
        )
        with _cf.ThreadPoolExecutor(
            max_workers=len(arm_funcs),
            thread_name_prefix="ramp-arm",
        ) as ex:
            futures = {ex.submit(fn): name for name, fn in arm_funcs.items()}
            for fut in _cf.as_completed(futures):
                name = futures[fut]
                try:
                    r = fut.result()
                except Exception:
                    # Each arm's wrapper already swallows; defensive only.
                    log.exception("_process_row_both_modes: %s arm executor surface", name)
                    r = {}
                if name == "inmail":
                    inmail_result.update(r)
                else:
                    static_result.update(r)

    # ── Multi-platform fan-out (Meta + Google) ───────────────────────────────
    # Reuse the LinkedIn static arm's (cohort × geo × angle) plan + PNGs.
    # Each platform arm is independent — Meta failures don't affect Google,
    # and neither affects LinkedIn (already done above).
    extra_platform_results: dict[str, dict] = {}
    if "static" in modes and not dry_run:
        from src.ad_platform import enabled_platforms
        platforms = [p for p in enabled_platforms() if p != "linkedin"]
        # Phase 2 — `channels` decision-row override filters the extras list.
        if channels is not None:
            allowed_extras = {c for c in channels if c != "linkedin"}
            # A bare "google" selection covers BOTH Google arms — Display
            # ("google") and Search ("google_search") — for the full/approve-all
            # path. But a SCOPED per-channel launch (ONLY_CHANNEL set) means the
            # console picked exactly one Google arm: "google" = Display only,
            # "google_search" = Search only. So only auto-pair the two when this
            # is NOT a scoped single-channel run.
            if "google" in allowed_extras and not (getattr(config, "ONLY_CHANNEL", "") or "").strip():
                allowed_extras.add("google_search")
            filtered = [p for p in platforms if p in allowed_extras]
            if filtered != platforms:
                log.info(
                    "_process_row_both_modes: channels=%s → extras platforms %s → %s",
                    channels, platforms, filtered,
                )
            platforms = filtered
        specs = (static_result or {}).get("campaign_specs") or []
        if platforms and specs:
            extras = _build_extra_platform_clients(platforms)
            for platform_name, parts in extras.items():
                try:
                    r = _process_extra_platform_arm(
                        platform=platform_name,
                        client=parts["client"],
                        resolver=parts["resolver"],
                        campaign_specs=specs,
                        flow_id=flow_id,
                        location=location,
                        ramp_id=ramp_id,
                        cohort_id_override=cohort_id_override,
                        destination_url_override=destination_url_override,
                        unique_id=unique_id,
                        naming_meta=naming_meta,
                        sheets=sheets,
                        daily_budget_cents=(budgets or {}).get(platform_name),
                    )
                    extra_platform_results[platform_name] = r
                except Exception:
                    log.exception(
                        "_process_row_both_modes: %s arm aborted — other arms preserved",
                        platform_name,
                    )

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

    extra_groups: list[str] = []
    extra_campaigns: dict[str, list[str]] = {}
    extra_creative_paths: dict[str, str] = {}
    manual_handoff_urls: dict[str, str] = {}
    for plat, r in extra_platform_results.items():
        extra_groups.extend(r.get("campaign_groups") or [])
        extra_campaigns[plat] = list(r.get("campaigns") or [])
        for k, v in (r.get("creative_paths") or {}).items():
            extra_creative_paths[f"{k}_{plat}"] = v
        # Surface the Meta/Google manual-handoff Drive URL so the Slack notifier
        # can ping Diego/Bryan when platform-side creation failed but the
        # creatives + cohort/copy details are still preserved in Drive.
        if r.get("manual_handoff_url"):
            manual_handoff_urls[plat] = r["manual_handoff_url"]

    return {
        "ok": True,
        "campaign_groups": (
            list(inmail_result.get("campaign_groups") or [])
            + list(static_result.get("campaign_groups") or [])
            + extra_groups
        ),
        "inmail_campaigns": list(inmail_result.get("campaigns", [])),
        "static_campaigns": list(static_result.get("campaigns", [])),
        "extra_platform_campaigns": extra_campaigns,
        "manual_handoff_urls": manual_handoff_urls,
        "creative_paths": {
            **{f"{k}_inmail": v for k, v in inmail_result.get("creative_paths", {}).items()},
            **{f"{k}_static": v for k, v in static_result.get("creative_paths", {}).items()},
            **extra_creative_paths,
        },
        "per_cohort": per_cohort,
    }


def _ramp_to_rows(ramp) -> list[dict]:
    """Convert a RampRecord into the row-dict shape `_process_row_both_modes`
    consumes. Mirrors the logic in run_launch() lines ~120-138 — kept in one
    place so the CLI and run_launch_for_ramp don't drift.

    Includes Smart Ramp v2 campaign-naming metadata (`job_post_pod`,
    `matched_domain`, `job_post_language_code`, `campaign_state`,
    `ramp_submitted_at`) so the leaf-campaign builders can emit names per the
    pipe-delimited spec at /ramps/<id>/campaigns.
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
            "ramp_submitted_at": ramp.submitted_at or "",
            "cohort_id": cohort.id,
            "cohort_description": cohort.cohort_description,
            # Ramp-wide brief — feeds task-card grounding alongside cohort_description.
            "ramp_summary": ramp.summary or "",
            "selected_lp_url": cohort.selected_lp_url,
            "included_geos": cohort.included_geos,
            "matched_locales": cohort.matched_locales,
            "target_activations": cohort.target_activations,
            "linear_issue_id": ramp.linear_issue_id,
            "project_id": ramp.project_id,
            # Smart Ramp v2 campaign-naming metadata
            "job_post_pod": getattr(cohort, "job_post_pod", None),
            "matched_domain": getattr(cohort, "matched_domain", None),
            "job_post_domain": getattr(cohort, "job_post_domain", None),
            "domain_match_failed": getattr(cohort, "domain_match_failed", False),
            "job_post_language_code": getattr(cohort, "job_post_language_code", None),
            "campaign_state": getattr(cohort, "campaign_state", None),
            "job_post_pay_rates": getattr(cohort, "job_post_pay_rates", None),
        })
    # ONLY_COHORT scope (new-cohort feature 010): restrict the run to a single
    # Smart Ramp cohort id. This is the isolation core — prep + every launch arm
    # only ever see this one cohort's row, so existing cohorts' campaigns are
    # never iterated, let alone touched (the ramp-wide DELETEs are also guarded
    # by config.ONLY_COHORT in _resolve_cohorts / cold start).
    _only_cohort = (getattr(config, "ONLY_COHORT", "") or "").strip()
    if _only_cohort:
        _before = len(rows)
        rows = [r for r in rows if str(r.get("cohort_id") or "") == _only_cohort]
        log.info("ONLY_COHORT=%s → %d of %d cohort rows kept", _only_cohort, len(rows), _before)

    # ONLY_LOCALES env filter (manual one-off reruns): comma-separated BCP-47
    # locales (e.g. "th-th,ko-kr,vi-vn"). When set, restrict to cohorts whose
    # matched_locales intersect the list, so a targeted rerun materializes just
    # the named locale cohorts without re-touching the others. Mirrors the
    # OUTLIER_CHANNELS / OUTLIER_ARMS env overrides.
    _only_locales = (os.environ.get("ONLY_LOCALES") or "").strip()
    if _only_locales:
        _want = {l.strip().lower().replace("_", "-")
                 for l in _only_locales.split(",") if l.strip()}
        _before = len(rows)
        rows = [
            r for r in rows
            if _want & {str(m).lower().replace("_", "-")
                        for m in (r.get("matched_locales") or [])}
        ]
        log.info("ONLY_LOCALES=%s → %d of %d cohorts kept", sorted(_want), len(rows), _before)
    return rows


def run_launch_for_ramp(
    ramp_id: str,
    modes: tuple[str, ...] = ("inmail", "static"),
    dry_run: bool = False,
    prep_only: bool = False,
    channels: list[str] | None = None,
    budgets: dict[str, int] | None = None,
) -> dict:
    """Programmatic entry point for the Smart Ramp poller (Plan 01).

    Fetches the ramp from Smart Ramp, iterates cohort rows, dispatches BOTH
    InMail + Static arms per cohort. Returns the aggregated result dict the
    poller's state file needs.

    Per-row isolation: a single row raising never aborts the rest. Per-cohort
    isolation lives inside _process_static_campaigns / _process_inmail_campaigns.

    `prep_only=True` (Phase 1.4): mine cohorts + write them to the Triggers
    Sheet, then STOP before any platform API call. Used by the outlier-
    campaign-console approval gate so Diego/Bryan can see what the pipeline
    *would* launch before clicking Approve. See `_prep_ramp` /
    `_launch_ramp` for the intent-named wrappers.

    `channels` + `budgets` (Phase 2): per-ramp overrides supplied by the
    console's approval decision. When None, the legacy global defaults
    (config.ENABLED_PLATFORMS + each platform client's PLACEHOLDER budget)
    apply. When set:
      - channels: subset of {'linkedin','meta','google'} — drives which
        arms execute. 'linkedin' covers both Static + InMail.
      - budgets: {'linkedin'|'meta'|'google': int_cents_per_day}. A missing
        key → use the platform client's PLACEHOLDER default.

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
    urn_res = UrnResolver(sheets, linkedin_client=li_client)
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
    if prep_only:
        aggregated["prep_only"] = True
        aggregated["cohorts_mined"] = []
        aggregated["briefs_generated"] = 0

    # Phase 5 — refresh + snapshot competitor intel per ramp.
    # Pre-2026-05-22 this only SNAPSHOTTED the shared latest.json file, which
    # was refreshed manually via scripts/refresh_competitor_intel.py. That
    # meant every ramp got the same generic intel regardless of the actual
    # role being recruited. Now we ALSO refresh in-line, scoped to this
    # ramp's brief, so the snapshot carries role-specific listings + Reddit
    # signals. Disable via COMPETITOR_INTEL_AUTO_REFRESH=false when running
    # batch reruns to avoid duplicate scrapes per (tg_label, day).
    try:
        from pathlib import Path as _Path
        from src.ui_decisions import upsert_competitor_intel_snapshot
        _intel_path = _Path("data/competitor_intel/latest.json")
        _auto_refresh = os.getenv("COMPETITOR_INTEL_AUTO_REFRESH", "true").lower() in ("1", "true", "yes")
        # Pull the ramp's brief once — it's the cohort-shared tg_label seed.
        # ramp.summary is the verbatim requester ask (e.g. "Short-Form Video
        # Creators"). Fall back to project_name if summary is empty. We
        # reuse the `client` + `ramp` already fetched at the top of
        # run_launch_for_ramp via the outer scope rather than re-instantiating
        # SmartRampClient locally (a local import here would scope-shadow the
        # top-of-function `client = SmartRampClient()` and raise
        # UnboundLocalError before this block ever runs).
        _ramp_record = ramp
        _tg_label = ""
        if _ramp_record is not None:
            _tg_label = (
                (getattr(_ramp_record, "summary", "") or "").strip()
                or (getattr(_ramp_record, "project_name", "") or "").strip()
            )
        if _auto_refresh and _tg_label:
            try:
                from src.competitor_intel import run_competitor_intel, save_intel_json
                log.info(
                    "Phase5: auto-refreshing competitor intel for ramp=%s tg_label=%r",
                    ramp_id, _tg_label[:80],
                )
                _intel = run_competitor_intel(
                    tg_label=_tg_label,
                    include_reddit=True,
                    include_trustpilot=False,  # ~1 min slowest source; skip in pipeline
                    include_seo=True,
                    include_task_listings=True,
                )
                save_intel_json(_intel, tg_label=_tg_label)
            except Exception as _exc:
                log.warning(
                    "Phase5: competitor intel auto-refresh failed (%s) — "
                    "falling back to the existing latest.json snapshot",
                    _exc,
                )
        elif not _auto_refresh:
            log.info("Phase5: COMPETITOR_INTEL_AUTO_REFRESH=false — using existing latest.json")
        else:
            log.info("Phase5: no tg_label resolved for ramp=%s — skipping auto-refresh", ramp_id)

        if _intel_path.exists():
            import json as _json_mod
            _intel_data = _json_mod.loads(_intel_path.read_text())
            upsert_competitor_intel_snapshot(ramp_id, _intel_data)
            log.info(
                "Phase5: snapshotted competitor_intel for ramp=%s tg_label=%r "
                "(%d top-level keys, listings=%d, role_hits=%d)",
                ramp_id, (_intel_data.get("tg_label") or "")[:60],
                len(_intel_data),
                len(_intel_data.get("task_listings") or []),
                (_intel_data.get("role_demand_signals") or {}).get("match_count") or 0,
            )
    except Exception as _exc:
        log.warning("Phase5: competitor_intel snapshot skipped: %s", _exc)

    # 2026-05-20 — role-based Meta Ad Library lookup. For each unique
    # (matched_domain or cohort role) on this ramp, query Meta's public
    # ads_archive endpoint for competitors running ads for the SAME role.
    # Best-effort; skips silently when META_ACCESS_TOKEN is missing or the
    # Graph API errors out. Surfaces in the console's competitor card so
    # reviewers see competitor messaging / pay rate / TG context before
    # approving channels.
    try:
        from src.competitor_intel import fetch_role_based_meta_ads
        from src.ui_decisions import upsert_competitor_role_ads
        seen_role_queries: set[str] = set()
        for _cohort in getattr(ramp, "cohorts", []) or []:
            # Prefer matched_domain (concise, role-aligned) then fall back to
            # the first 5 words of cohort_description (paragraph form).
            role_q = (
                (getattr(_cohort, "matched_domain", "") or "").strip()
                or " ".join((getattr(_cohort, "cohort_description", "") or "").split()[:5])
            ).strip()
            if not role_q or role_q in seen_role_queries:
                continue
            seen_role_queries.add(role_q)
            try:
                _role_ads = fetch_role_based_meta_ads(role_q, max_results=30)
                if _role_ads:
                    upsert_competitor_role_ads(ramp_id, role_q, _role_ads)
                    log.info(
                        "Meta role-ads: persisted %d ads for ramp=%s role=%r",
                        len(_role_ads), ramp_id, role_q,
                    )
            except Exception as _r_exc:
                log.warning(
                    "Meta role-ads lookup failed for role=%r ramp=%s: %s",
                    role_q, ramp_id, _r_exc,
                )
    except Exception as _exc:
        log.warning("Meta role-ads lookup skipped (import / setup error): %s", _exc)

    # Cross-row (cohort × geo_cluster) dedup state, scoped to this single ramp.
    # Two independent sets so the InMail and Static arms — which run
    # concurrently on the same row inside _process_row_both_modes — don't race
    # each other into skipping the other arm's first-claim. First row in
    # _ramp_to_rows order wins; later rows skip with a structured log line.
    seen_inmail_keys: set = set()
    seen_static_keys: set = set()

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
                seen_inmail_keys=seen_inmail_keys,
                seen_static_keys=seen_static_keys,
                prep_only=prep_only,
                channels=channels,
                budgets=budgets,
            )
            aggregated["campaign_groups"].extend(outcome.get("campaign_groups", []) or [])
            aggregated["inmail_campaigns"].extend(outcome.get("inmail_campaigns", []) or [])
            aggregated["static_campaigns"].extend(outcome.get("static_campaigns", []) or [])
            aggregated["creative_paths"].update(outcome.get("creative_paths", {}) or {})
            aggregated["per_cohort"].extend(outcome.get("per_cohort", []) or [])
            if prep_only:
                aggregated["cohorts_mined"].extend(outcome.get("cohorts_mined", []) or [])
                aggregated["briefs_generated"] += int(outcome.get("briefs_generated", 0) or 0)
        except Exception:
            log.exception(
                "run_launch_for_ramp: row failed for ramp=%s cohort=%s — continuing with next row",
                ramp_id, row.get("cohort_id"),
            )
            continue

    # Last step of every LAUNCH: consolidated per-ramp audit — recursively
    # check + auto-fix the campaigns just created for this ramp (creative
    # resolution today; extensible) before the run summary goes back to the
    # poller. Skipped for prep_only / dry_run. Best-effort, never aborts.
    if not prep_only and not dry_run:
        try:
            from src.ramp_audit import audit_ramp
            aggregated["audit"] = audit_ramp(ramp_id)
        except Exception as exc:
            log.warning("run_launch_for_ramp: per-ramp audit failed (non-fatal): %s", exc)

    return aggregated


def _prep_ramp(ramp_id: str) -> dict:
    """Run pipeline prep stages only — mine cohorts + write them to the
    Triggers Sheet. No platform API calls. Used by the outlier-campaign-
    console approval gate to populate UI state before Diego/Bryan choose
    channels + budgets. Idempotent: re-running re-reads cohorts and
    re-upserts the same Triggers rows (Phase 5 will add Postgres rationale
    persistence).
    """
    return run_launch_for_ramp(ramp_id, dry_run=False, prep_only=True)


def _launch_ramp(ramp_id: str, decision=None) -> dict:
    """Run the full pipeline (prep + platform creates) for a ramp that has
    already been approved in the console. `decision` is the
    `src.ui_decisions.Decision` row carrying channel selection + budget
    overrides. When `decision` is None, falls back to global defaults
    (config.ENABLED_PLATFORMS + per-client PLACEHOLDER budgets).
    """
    channels = list(decision.channels) if decision and decision.channels else None
    budgets = dict(decision.budgets) if decision and decision.budgets else None

    # Per-channel manual launch (feature #3): ONLY_CHANNEL restricts this run to
    # one channel and the console's channel_locks guards concurrency. Release
    # the lock when done (the run is the canonical signal); the console TTL is
    # only a backstop for crashed runs.
    only_channel = (getattr(config, "ONLY_CHANNEL", "") or "").strip().lower()
    if only_channel:
        channels = [only_channel]
        log.info("_launch_ramp: ONLY_CHANNEL=%s — restricting run to channels=%s", only_channel, channels)

    # Relaunch (replace): archive this channel's existing campaigns before
    # creating fresh ones, so a re-launch doesn't pile up duplicates. Requires
    # ONLY_CHANNEL (we archive one channel at a time). Best-effort — never
    # blocks the fresh launch.
    if only_channel and getattr(config, "REPLACE_EXISTING", False):
        try:
            from src.relaunch import archive_channel_campaigns
            # Honor ONLY_LOCALES: a locale-scoped relaunch must archive ONLY the
            # targeted locales, else replace wipes the ramp's other-language
            # campaigns (which the scoped launch never recreates).
            _ol = (os.environ.get("ONLY_LOCALES") or "").strip()
            _replace_locales = [l for l in _ol.split(",") if l.strip()] if _ol else None
            summary = archive_channel_campaigns(ramp_id, only_channel, _replace_locales)
            log.info("_launch_ramp: relaunch-replace archived %s (locales=%s)",
                     summary, _replace_locales or "all")
            try:
                from src.ui_decisions import log_event
                log_event(ramp_id, "relaunch_replace_archived", summary, None)
            except Exception:
                pass
        except Exception as exc:
            log.warning("_launch_ramp: relaunch-replace archive failed (%s) — continuing to fresh launch", exc)

    # Lock key: the console locks per GRANULAR channel. LinkedIn splits into
    # Sponsored (static arm) and InMail (inmail arm); both dispatch
    # only_channel=linkedin but the console locks/releases them under distinct
    # keys ("linkedin" vs "linkedin_inmail") so the two don't block each other.
    _arms = (os.environ.get("OUTLIER_ARMS") or "").strip().lower()
    lock_channel = "linkedin_inmail" if (only_channel == "linkedin" and _arms == "inmail") else only_channel

    # Unified launcher: prep the scoped cohort BEFORE launching, so a
    # not-yet-prepped cohort launches in one action. Additive/idempotent
    # (ON_CONFLICT); scoped by ONLY_COHORT/ONLY_LOCALES like the launch itself.
    if only_channel and getattr(config, "PREP_THEN_LAUNCH", False):
        try:
            log.info("_launch_ramp: PREP_THEN_LAUNCH — prepping scoped cohort=%s before launch",
                     getattr(config, "ONLY_COHORT", "") or "?")
            run_launch_for_ramp(ramp_id, dry_run=False, prep_only=True, channels=channels, budgets=budgets)
        except Exception as exc:
            log.warning("_launch_ramp: prep-then-launch prep phase failed (%s) — attempting launch anyway", exc)

    try:
        return run_launch_for_ramp(
            ramp_id, dry_run=False, prep_only=False,
            channels=channels, budgets=budgets,
        )
    finally:
        if only_channel:
            try:
                from src.ui_decisions import release_channel_lock
                # Release exactly the (locale × channel) locks this scoped run
                # held, under the granular lock key. ONLY_LOCALES is the
                # console's acquired-locale set; empty → release only the
                # whole-channel (locale='') lock (see release_channel_lock).
                _rl = (os.environ.get("ONLY_LOCALES") or "").strip()
                _rl_locales = [l for l in _rl.split(",") if l.strip()] if _rl else None
                release_channel_lock(ramp_id=ramp_id, channel=lock_channel, locales=_rl_locales)
                # Crash-safety: flip any unit still 'creating' (run died mid-API
                # call) to 'failed' so the console doesn't show it stuck forever.
                from src.ui_decisions import mark_launch_progress_failed
                mark_launch_progress_failed(ramp_id, lock_channel, _rl_locales)
            except Exception as exc:
                log.warning("_launch_ramp: channel lock release failed (%s/%s): %s — TTL will clear it",
                            ramp_id, lock_channel, exc)


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

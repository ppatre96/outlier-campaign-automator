#!/usr/bin/env python3
"""
Smart Ramp Poller — Phase 2.6 (SR-01, SR-02, SR-05, SR-08, SR-10)

Runs every 15 minutes via launchd (com.outlier.smart-ramp-poller). Sequentially:
  1. Acquire filelock (data/smart_ramp_poller.lock, timeout=5) — exit 0 on contention
  2. Fetch submitted ramps from Smart Ramp API
  3. Filter test ramps (requester_name matching r"\btest\b" — Pitfall 10)
  4. Compute sha256 signature over (sorted cohort dicts + summary + updated_at)
  5. Classify each as new / edit / noop vs data/processed_ramps.json
  6. Skip ramps with consecutive_failures >= SMART_RAMP_FAILURE_THRESHOLD (escalation gate)
  7. Dispatch to run_ramp_pipeline(record) — STUB in Plan 01; Plan 02 replaces with main.run_launch_for_ramp
  8. Update state file atomically (write-to-tmp + os.replace — Pitfall 5)

CLI:
  --once                  Run a single poll then exit (default behavior under launchd)
  --ramp-id <id>          Force-process exactly one ramp ID (debugging; bypasses signature noop)
  --dry-run               All steps run except state write + Slack notify

Vocabulary (CLAUDE.md): log lines and any user-facing strings use approved Outlier
vocabulary. NEVER emit banned tokens (payment, screening, opportunity, task, match,
reward, progress — substitutions for the project's vocabulary table).
"""
from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import tempfile
import traceback
from dataclasses import asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# CRITICAL: load_dotenv MUST precede the config-module import below
# (Phase 2.5 V2 Pitfall 3 / RESEARCH §Pitfall 3 — config.py reads env at import time)
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from filelock import FileLock, Timeout  # noqa: E402

import config  # noqa: E402  load_dotenv above must run first
from src.smart_ramp_client import SmartRampClient, RampRecord, CohortSpec  # noqa: E402

log = logging.getLogger("smart_ramp_poller")

STATE_PATH = _PROJECT_ROOT / "data" / "processed_ramps.json"
LOCK_PATH = _PROJECT_ROOT / "data" / "smart_ramp_poller.lock"
LOG_DIR = _PROJECT_ROOT / "logs" / "smart_ramp_poller"


# ─────────────────────────────────────────────────────────────────────────────
# Logging + state helpers
# ─────────────────────────────────────────────────────────────────────────────


def _setup_logging() -> Path:
    """Configure root + module loggers with date-stamped file + stdout handlers.

    Returns the path to the log file written for this run.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    log_path = LOG_DIR / f"{today}.log"

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(name)s %(message)s")
    file_handler = logging.FileHandler(log_path)
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)

    root = logging.getLogger()
    root.setLevel(logging.INFO)
    # Avoid duplicate handlers on repeated invocations within a single process
    for h in list(root.handlers):
        root.removeHandler(h)
    root.addHandler(file_handler)
    root.addHandler(stream_handler)
    return log_path


def _read_state() -> dict:
    """Read state JSON; on any failure, return an empty dict (safe default).

    NEVER raises — corruption / missing file / permission errors all yield the
    empty default so a single bad state file does not halt the polling loop.
    """
    try:
        if not STATE_PATH.exists():
            return {"ramps": {}, "ramp_versions": {}}
        data = json.loads(STATE_PATH.read_text())
        if not isinstance(data, dict):
            log.warning("State file %s is not a dict; resetting to empty", STATE_PATH)
            return {"ramps": {}, "ramp_versions": {}}
        data.setdefault("ramps", {})
        data.setdefault("ramp_versions", {})
        return data
    except Exception as e:  # corrupted JSON, permission error, etc.
        log.warning("Failed to read state file %s: %s — using empty default", STATE_PATH, e)
        return {"ramps": {}, "ramp_versions": {}}


def _write_state_atomic(state: dict) -> None:
    """Atomic state write — write-to-tmp + os.replace. Caller MUST hold filelock.

    Pattern 2 / Pitfall 5: if the process is killed between fdopen.write and
    os.replace, the destination file is either unchanged (pre-write contents)
    or fully written (post-write contents) — never a partial JSON document.
    """
    STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(state, indent=2, sort_keys=True, default=str)
    fd, tmp_path_str = tempfile.mkstemp(
        prefix=".processed_ramps.", suffix=".json.tmp", dir=str(STATE_PATH.parent)
    )
    try:
        with os.fdopen(fd, "w") as f:
            f.write(payload)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp_path_str, STATE_PATH)
    except Exception:
        try:
            os.unlink(tmp_path_str)
        except OSError:
            pass
        raise


# ─────────────────────────────────────────────────────────────────────────────
# Signature + classification helpers
# ─────────────────────────────────────────────────────────────────────────────


def compute_signature(ramp: RampRecord) -> str:
    """SHA-256 over (sorted-cohort dicts + summary + updated_at). Deterministic.

    Pitfall 2: signature MUST hash CONTENT (cohorts + summary), not just timestamps,
    so a Vercel-side updated_at refresh without real edits doesn't trigger a false v2.
    Cohort list is sorted by .id before serialize so list-order changes do not
    invalidate the signature.
    """
    cohorts_serialized = json.dumps(
        [asdict(c) for c in sorted(ramp.cohorts, key=lambda c: c.id)],
        sort_keys=True,
    )
    payload = f"{cohorts_serialized}\n{ramp.summary or ''}\n{ramp.updated_at or ''}"
    return "sha256:" + hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _should_skip_test_ramp(record: RampRecord) -> bool:
    """Filter test ramps (e.g., 'Quintin Au Test'). SR-08.

    Uses word-boundary regex from config.SMART_RAMP_TEST_REQUESTER_PATTERN.
    Substring match (`.lower().contains("test")`) is FORBIDDEN — Pitfall 10:
    "Christopher Testov" must NOT be filtered (substring matches but no word boundary).
    """
    name = (record.requester_name or "").strip()
    if not name:
        return False
    pattern = re.compile(config.SMART_RAMP_TEST_REQUESTER_PATTERN, re.IGNORECASE)
    return bool(pattern.search(name))


def _classify_action(prior: Optional[dict], current_sig: str) -> str:
    """Return one of: "new" | "edit" | "noop"."""
    if prior is None:
        return "new"
    if prior.get("last_signature") == current_sig:
        return "noop"
    return "edit"


def _mark_superseded(state: dict, ramp_id: str) -> int:
    """Archive the live ramps[ramp_id] entry into ramp_versions and bump version.

    Returns the NEW version number (prior.version + 1, or 1 if no prior).
    The archived snapshot has superseded=True; the live entry is left intact
    for process_ramp() to overwrite with the new version.
    """
    cur = state.setdefault("ramps", {}).get(ramp_id)
    if not cur:
        return 1
    prior_version = int(cur.get("version", 1))
    state.setdefault("ramp_versions", {})
    history_key = f"{ramp_id}_v{prior_version}"
    snapshot = dict(cur)
    snapshot["superseded"] = True
    state["ramp_versions"][history_key] = snapshot
    return prior_version + 1


def _should_block_for_escalation(prior: Optional[dict]) -> bool:
    """Gate that prevents retrying ramps already at the escalation threshold.

    CONTEXT.md: stop retrying that ramp until consecutive_failures resets to 0
    in the state file (manual reset or future reaction-handler trigger).
    """
    if not prior:
        return False
    return (
        bool(prior.get("escalation_dm_sent"))
        and int(prior.get("consecutive_failures", 0)) >= config.SMART_RAMP_FAILURE_THRESHOLD
    )


# ─────────────────────────────────────────────────────────────────────────────
# Pipeline call — Plan 03 wired the real impl in 02.6-03
# ─────────────────────────────────────────────────────────────────────────────


def run_ramp_pipeline(
    record: RampRecord,
    dry_run: bool = False,
    version: int = 1,
    force: bool = False,
) -> dict:
    """Run the real pipeline for a single ramp. Calls main.run_launch_for_ramp.

    `force=True` (set by `--force` CLI flag, only usable with `--ramp-id`)
    bypasses the existing-decision gate-skip check. Use to recover from
    stuck ramps whose first prep run failed mid-flow and left a stale
    `awaiting_approval` row in Postgres (e.g. GMR-0022 OOM 2026-05-22 —
    poller subsequently skipped every tick because the decision row
    existed). When `force=True` and a decision row is present, the
    pre-existing `cohort_briefs` + `cohort_icp` rows are NOT deleted, but
    `_prep_ramp` re-runs and overwrites them via the existing
    upsert_*_ON_CONFLICT logic.

    Returns the dict shape Plan 02 produces:
      {
        "ok": bool,
        "campaign_groups": [urn, ...],
        "inmail_campaigns": [urn, ...],
        "static_campaigns": [urn, ...],
        "creative_paths": {<key>: <urn-or-local-path>, ...},
        "per_cohort": [ {cohort_id, cohort_description, inmail_urn, static_urn,
                          inmail_creative, static_creative}, ... ],
      }

    Phase 1.5 — outlier-campaign-console approval gate. When
    `config.UI_GATE_ENABLED=true`, this function consults Postgres before
    dispatching:

      no row in ramp_decisions  → call _prep_ramp(), upsert awaiting_approval,
                                  return prep result. UI takes over.
      status prep_running       → defensive — same as awaiting_approval.
      status awaiting_approval  → log + return early; nothing to do.
      status approved / yolo    → atomic claim_ramp() → _launch_ramp().
                                  On success: update_status('completed').
                                  On failure: update_status('failed').
      status launching          → another worker holds the claim; skip.
      status completed / failed → terminal; the user-facing decision is in
                                  the past. Skip to avoid double-launch.

    On UIDecisionsUnavailable (DB down / DATABASE_URL unset) with gate ON,
    we fail-closed: log + return ok=False with a clear error class. The
    poller's existing failure-counter + escalation gate then surfaces the
    outage to Slack. When gate is OFF, behavior is unchanged.
    """
    from main import run_launch_for_ramp, _prep_ramp, _launch_ramp, _ramp_to_rows
    log.info(
        "Running pipeline for ramp=%s version=%s dry_run=%s",
        record.id, version, dry_run,
    )

    # Legacy path — gate disabled. No Postgres lookups; behaves as before.
    if not getattr(config, "UI_GATE_ENABLED", False):
        return run_launch_for_ramp(
            record.id, modes=("inmail", "static"), dry_run=dry_run,
        )

    # Dry-run never gates. Always behave as before.
    if dry_run:
        return run_launch_for_ramp(
            record.id, modes=("inmail", "static"), dry_run=True,
        )

    # Gated path — consult Postgres.
    try:
        from src.ui_decisions import (
            UIDecisionsUnavailable,
            get_decision,
            claim_ramp,
            upsert_awaiting_approval,
            upsert_awaiting_brief_review,
            update_status,
        )
    except ImportError as exc:
        log.error("UI gate ON but src.ui_decisions import failed: %s", exc)
        return {"ok": False, "error": f"ui_decisions import failed: {exc}",
                "campaign_groups": [], "inmail_campaigns": [],
                "static_campaigns": [], "creative_paths": {}, "per_cohort": []}

    try:
        decision = get_decision(record.id)
    except UIDecisionsUnavailable as exc:
        log.warning("UI gate ON but Postgres unreachable (%s) — skipping ramp %s",
                    exc, record.id)
        return {"ok": False, "error": f"ui_decisions unreachable: {exc}",
                "campaign_groups": [], "inmail_campaigns": [],
                "static_campaigns": [], "creative_paths": {}, "per_cohort": []}

    # Per-channel manual launch (feature #3). When ONLY_CHANNEL is set (console
    # per-channel trigger), bypass the ramp-level status machine: concurrency is
    # guarded by the console's channel_locks (per ramp × channel), and we must
    # NOT claim 'launching' or flip to 'completed' (other channels still need
    # launching). Requires a prior approval. _launch_ramp restricts to the one
    # channel and releases the lock when done.
    only_channel = (getattr(config, "ONLY_CHANNEL", "") or "").strip().lower()
    if only_channel:
        ok_states = {"approved", "yolo", "completed", "launching"}
        if not decision or decision.status not in ok_states:
            log.warning(
                "Per-channel launch ramp=%s channel=%s but status=%s (needs prior approval) — skipping",
                record.id, only_channel, getattr(decision, "status", None),
            )
            return {"ok": False, "error": "per_channel_requires_approval",
                    "campaign_groups": [], "inmail_campaigns": [],
                    "static_campaigns": [], "creative_paths": {}, "per_cohort": []}
        log.info(
            "Per-channel launch ramp=%s channel=%s (channel_locks-guarded; ramp status %s unchanged)",
            record.id, only_channel, decision.status,
        )
        return _launch_ramp(record.id, decision)

    # No prior decision → first time we've seen this ramp post-submission.
    # Run prep (cohort mining + Triggers Sheet rows + LinkedIn briefs).
    # If briefs were generated, transition to awaiting_brief_review (new gate);
    # otherwise fall back to awaiting_approval (legacy gate, no brief review).
    #
    # `force=True` short-circuits the gate-skip block below and falls into
    # this path even when a decision row exists. Treated as "treat the
    # existing decision as if it were null" — re-run prep, let upsert_*
    # ON_CONFLICT overwrite stale rows.
    if decision is None or force:
        if decision is not None and force:
            log.info(
                "UI gate: ramp %s status=%s — forcing prep re-run (--force)",
                record.id, decision.status,
            )
        else:
            log.info("UI gate: ramp %s has no decision — running prep", record.id)

        # Slack ping #1 — fired BEFORE prep starts so Diego/Bryan know a new
        # ramp is in flight. Best-effort; failures never block the pipeline.
        try:
            from src.smart_ramp_notifier import notify_new_ramp
            notify_new_ramp(record)
        except Exception as exc:
            log.warning("notify_new_ramp failed (non-fatal): %s", exc)

        prep_result = _prep_ramp(record.id)
        briefs_n = int(prep_result.get("briefs_generated", 0) or 0)
        cohorts_n = len(prep_result.get("cohorts_mined", []) or [])
        prep_summary = {
            "cohorts_mined": prep_result.get("cohorts_mined", []),
            "rows_processed": len(_ramp_to_rows(record)),
            "briefs_generated": briefs_n,
        }
        try:
            if briefs_n > 0:
                log.info("Prep wrote %d brief(s) — transitioning to awaiting_brief_review", briefs_n)
                upsert_awaiting_brief_review(
                    record.id,
                    matched_domain=(record.cohorts[0].matched_domain
                                    if record.cohorts else "") or "",
                    requester_name=getattr(record, "requester_name", "") or "",
                    summary=getattr(record, "summary", "") or "",
                    submitted_at=getattr(record, "submitted_at", "") or None,
                    prep_summary=prep_summary,
                )
                fell_back_to_legacy = False
            else:
                log.info("Prep wrote 0 briefs — falling back to awaiting_approval (legacy gate)")
                upsert_awaiting_approval(
                    record.id,
                    matched_domain=(record.cohorts[0].matched_domain
                                    if record.cohorts else "") or "",
                    requester_name=getattr(record, "requester_name", "") or "",
                    summary=getattr(record, "summary", "") or "",
                    submitted_at=getattr(record, "submitted_at", "") or None,
                    prep_summary=prep_summary,
                )
                fell_back_to_legacy = True
        except UIDecisionsUnavailable as exc:
            log.warning("Prep finished for %s but upsert failed: %s — UI won't see it until next poll", record.id, exc)
            return prep_result

        # Slack ping #2 — action-required: prep done, asks Diego/Bryan to
        # open the console, review briefs, then approve + launch. Sent
        # AFTER the decision row is written so the link in the message
        # lands the reviewer on an interactive page.
        try:
            from src.smart_ramp_notifier import notify_briefs_ready
            notify_briefs_ready(
                record,
                briefs_generated=briefs_n,
                cohorts_count=cohorts_n,
                fell_back_to_legacy=fell_back_to_legacy,
            )
        except Exception as exc:
            log.warning("notify_briefs_ready failed (non-fatal): %s", exc)

        return prep_result

    # Terminal / in-flight states — nothing to do this tick. awaiting_brief_review
    # is included so the poller doesn't repeatedly re-run prep on ramps still
    # waiting for reviewer comments / auto-confirm.
    if decision.status in ("awaiting_brief_review", "awaiting_approval",
                            "prep_running", "launching",
                            "completed", "failed"):
        log.info("UI gate: ramp %s status=%s — skipping (UI controls)",
                 record.id, decision.status)
        return {"ok": True, "ui_gated": True, "status": decision.status,
                "campaign_groups": [], "inmail_campaigns": [],
                "static_campaigns": [], "creative_paths": {}, "per_cohort": []}

    # Approved or YOLO — try to atomically claim and launch.
    if decision.status in ("approved", "yolo"):
        # Per-channel launch model (feature #3): approval is the GATE only.
        # Unless AUTO_LAUNCH_APPROVED is set, the scheduled poller does NOT
        # auto-launch approved ramps — launching is explicit + per-channel via
        # the console (which dispatches with only_channel, handled by the
        # per-channel branch above). This leaves the ramp 'approved' so each
        # channel can be fired independently.
        if not getattr(config, "AUTO_LAUNCH_APPROVED", False):
            log.info(
                "UI gate: ramp %s is %s — auto-launch disabled "
                "(AUTO_LAUNCH_APPROVED=false); awaiting explicit per-channel "
                "launch from the console.", record.id, decision.status,
            )
            return {"ok": True, "ui_gated": True, "status": "awaiting_manual_launch",
                    "campaign_groups": [], "inmail_campaigns": [],
                    "static_campaigns": [], "creative_paths": {}, "per_cohort": []}
        claimed = claim_ramp(record.id)
        if claimed is None:
            log.info("UI gate: ramp %s claim raced (likely concurrent poller) — skipping",
                     record.id)
            return {"ok": True, "ui_gated": True, "status": "claim_lost",
                    "campaign_groups": [], "inmail_campaigns": [],
                    "static_campaigns": [], "creative_paths": {}, "per_cohort": []}
        try:
            result = _launch_ramp(record.id, decision=claimed)
            update_status(record.id,
                          "completed" if result.get("ok") else "failed",
                          payload={"campaign_count":
                                   len(result.get("static_campaigns", []) or []) +
                                   len(result.get("inmail_campaigns", []) or [])})
            return result
        except Exception as exc:
            log.exception("UI gate: ramp %s launch raised", record.id)
            try:
                update_status(record.id, "failed",
                              payload={"error_class": type(exc).__name__,
                                       "error_message": str(exc)[:300]})
            except UIDecisionsUnavailable:
                pass
            raise

    # Unknown status — defensively skip.
    log.warning("UI gate: ramp %s has unknown status %r — skipping",
                record.id, decision.status)
    return {"ok": False, "error": f"unknown status {decision.status!r}",
            "campaign_groups": [], "inmail_campaigns": [],
            "static_campaigns": [], "creative_paths": {}, "per_cohort": []}


# ─────────────────────────────────────────────────────────────────────────────
# Per-ramp processor + orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def process_ramp(
    record: RampRecord, action: str, state: dict, dry_run: bool = False,
    force: bool = False,
) -> dict:
    """Process a single ramp; update state[ramps][record.id]; return pipeline result.

    Honors the 5-failure escalation gate: when consecutive_failures hits the
    SMART_RAMP_FAILURE_THRESHOLD, escalation_dm_sent flips to True. The actual
    Slack DM send happens OUTSIDE process_ramp (driven by `notify_kind` in the
    return dict) so Slack errors stay isolated from state-write paths.
    """
    now = datetime.now(timezone.utc).isoformat()
    ramps = state.setdefault("ramps", {})
    prior = ramps.get(record.id)
    prior_escalation = bool((prior or {}).get("escalation_dm_sent", False))

    # Compute version for this run
    if action == "edit":
        version = _mark_superseded(state, record.id)
    elif action == "new":
        version = 1
    else:  # action == "retry" or "noop" — same version
        version = int(prior.get("version", 1)) if prior else 1

    sig = compute_signature(record)

    try:
        result = run_ramp_pipeline(record, dry_run=dry_run, version=version, force=force)
        ok = bool(result.get("ok"))
        err_class = None
        tb_text = None
    except Exception as e:
        log.exception("Ramp %s pipeline raised", record.id)
        result = {"ok": False, "error": str(e)}
        ok = False
        err_class = type(e).__name__
        tb_text = traceback.format_exc()

    # Update state entry (atomic-write deferred to caller)
    consecutive = 0 if ok else int((prior or {}).get("consecutive_failures", 0)) + 1
    entry = {
        "first_seen_at": (prior or {}).get("first_seen_at", now),
        "last_processed_at": now,
        "last_signature": sig,
        "consecutive_failures": consecutive,
        "last_failure_class": err_class,
        "last_failure_traceback": (tb_text[:2000] if tb_text else None),
        "version": version,
        "campaign_groups": result.get("campaign_groups", []),
        "inmail_campaigns": result.get("inmail_campaigns", []),
        "static_campaigns": result.get("static_campaigns", []),
        "creative_paths": result.get("creative_paths", {}),
        "superseded": False,
        "escalation_dm_sent": (prior or {}).get("escalation_dm_sent", False),
        "per_cohort": result.get("per_cohort", []),
        "project_name": getattr(record, "project_name", None),
        "requester_name": getattr(record, "requester_name", None),
    }
    # Trip the escalation flag once we hit the threshold
    if consecutive >= config.SMART_RAMP_FAILURE_THRESHOLD and not entry["escalation_dm_sent"]:
        entry["escalation_dm_sent"] = True
        log.warning(
            "Ramp %s reached escalation threshold (%d consecutive failures) — "
            "escalation_dm_sent flipped to True; notifier will fire.",
            record.id, consecutive,
        )
    ramps[record.id] = entry

    # Decide whether (and how) to notify. Caller drives the actual Slack send so
    # state-write paths stay isolated from Slack errors. Escalation fires ONCE
    # per threshold trip (gated on prior_escalation transition).
    notify_kind: Optional[str] = None
    if ok and not dry_run:
        notify_kind = "success"
    elif (
        not ok
        and entry["escalation_dm_sent"]
        and not prior_escalation
        and not dry_run
    ):
        notify_kind = "escalation"

    return {
        "ok": ok,
        "result": result,
        "err_class": err_class,
        "tb": tb_text,
        "version": version,
        "notify_kind": notify_kind,
        "record": record,
    }


def _drive_notifier(outcome: dict, dry_run: bool) -> None:
    """Fire the success or escalation Slack DM based on `outcome["notify_kind"]`.

    Slack failures are caught + logged; they NEVER break the poll. In --dry-run
    mode no Slack call is issued (logs "would notify" instead).
    """
    kind = outcome.get("notify_kind")
    record = outcome.get("record")
    if dry_run:
        log.info(
            "[DRY-RUN] would notify (kind=%s) for ramp=%s",
            kind, getattr(record, "id", "?"),
        )
        return
    if kind == "success":
        try:
            from src.smart_ramp_notifier import notify_success
            notify_success(record, outcome["result"], version=outcome["version"])
        except Exception:
            log.exception(
                "notify_success failed for ramp=%s — continuing",
                getattr(record, "id", "?"),
            )
    elif kind == "escalation":
        try:
            from src.smart_ramp_notifier import notify_escalation
            notify_escalation(
                record,
                error_class=outcome.get("err_class") or "UnknownError",
                traceback_text=outcome.get("tb"),
            )
        except Exception:
            log.exception(
                "notify_escalation failed for ramp=%s — continuing",
                getattr(record, "id", "?"),
            )


def _regen_failed_creatives(ramp_id: str, *, dry_run: bool = False) -> int:
    """Regen-only path: re-run image gen for registry rows where the original
    pipeline run produced no creative.

    Flow:
    1. Read qc_rule_overrides from console Postgres (ramp_id-scoped). Empty
       set when no overrides — regen runs with default QC; the same rules
       that failed last time will fail again unless reviewer toggled them.
    2. Load data/campaign_registry.json. Filter to rows matching ramp_id
       with empty creative_image_path AND empty creative_urn. These are the
       genuine-fail rows (the original gen + 10 retries hit QC FAIL → png
       set to None → no Drive + no LinkedIn attach).
    3. For each failed row:
       a. Rebuild a minimal variant dict (angle, headline, subheadline,
          photo_subject, tg_label) from the row's columns
       b. Call generate_imagen_creative_with_qc(..., skip_rules=overrides).
          When skip_rules is set, the QC loop drops matching violations and
          can promote a FAIL to PASS if every remaining violation was skipped.
       c. On verdict ∈ {PASS, UNKNOWN}:
          - upload PNG to Drive at <ramp>/<platform>/<cohort_geo>/<angle>.png
          - upload PNG to LinkedIn → image_urn
          - create image_ad attached to the row's EXISTING campaign URN
          - patch the registry row: creative_image_path + creative_urn +
            qc_verdict + qc_attempts + qc_violations (post-skip-rule filter)
       d. On verdict == FAIL: leave the row untouched; log the violations.
    4. Reports a per-row tally + total recovered.

    Non-LinkedIn rows (Meta, Google) are skipped in this v1 — their
    creative-attach path differs from LinkedIn's create_image_ad and would
    need separate platform-specific handling. Tracked as a follow-up.

    Idempotent — re-running after a successful regen is a no-op (rows
    already have a creative_image_path).
    """
    import json
    log.info("=" * 70)
    log.info("REGEN MODE — ramp_id=%s dry_run=%s", ramp_id, dry_run)
    log.info("=" * 70)

    from src.console_db import list_qc_rule_overrides
    skip_rules = list_qc_rule_overrides(ramp_id)
    log.info("Skip rules: %s", sorted(skip_rules) if skip_rules else "(none)")

    reg_path = Path(__file__).resolve().parent.parent / "data" / "campaign_registry.json"
    if not reg_path.exists():
        log.error("Registry not found at %s", reg_path)
        return 1
    records = json.loads(reg_path.read_text())
    # v3: LinkedIn + Meta + Google all supported. Google reuses the QC'd 1:1
    # PNG and lets google_ads_api.create_image_ad auto-generate the 1.91:1
    # pillarboxed variant via the local_png_path passthrough.
    supported_platforms = {"linkedin", "meta", "google"}
    failed = [
        r for r in records
        if r.get("smart_ramp_id") == ramp_id
        and not r.get("creative_image_path")
        and not r.get("creative_urn")
        and (r.get("angle") or r.get("geo_cluster_label"))
        and (r.get("platform") or "").lower() in supported_platforms
    ]
    log.info("Failed rows for %s: %d (linkedin + meta + google)", ramp_id, len(failed))
    if not failed:
        log.info("Nothing to regen — every supported-platform row has a creative.")
        return 0

    if dry_run:
        log.info("dry-run: skipping actual regen. Would attempt %d row(s).", len(failed))
        return 0

    # Lazy imports — keep the poller's import cost down for non-regen runs.
    from src.gemini_creative import generate_imagen_creative_with_qc, generate_imagen_photo
    from src.figma_creative import rewrite_variant_copy
    from src.campaign_registry import update_row as _reg_update_row
    from src.linkedin_api import LinkedInClient
    from src.image_adapter import compose_ad_for_platform, primary_aspect

    li_client: LinkedInClient | None = None  # lazy-init on first need
    meta_client = None  # lazy-init on first Meta row
    google_client = None  # lazy-init on first Google row

    succeeded = 0
    failed_again = 0
    for row in failed:
        platform = (row.get("platform") or "").lower()
        angle = row.get("angle", "")
        cohort_geo = row.get("cohort_geo", "")
        campaign_id = (
            row.get("platform_campaign_id") or row.get("linkedin_campaign_urn") or ""
        )
        if not campaign_id:
            log.warning(
                "Row missing platform_campaign_id — cannot attach creative; skipping. "
                "platform=%s cohort_geo=%s angle=%s",
                platform, cohort_geo, angle,
            )
            failed_again += 1
            continue

        variant = {
            "angle":          angle,
            "headline":       row.get("headline", ""),
            "subheadline":    row.get("subheadline", ""),
            "photo_subject":  row.get("photo_subject", ""),
            "tg_label":       row.get("cohort_signature", ""),
            "intro_text":     "",
            "ad_headline":    row.get("headline", "")[:70],
            "ad_description": row.get("subheadline", "")[:100],
            "cta_button":     "APPLY",
        }

        log.info(
            "Regen %s · %s · angle=%s · headline=%r",
            platform, cohort_geo, angle, variant["headline"][:50],
        )
        try:
            # Always run QC against the 1:1 composite — that gates the photo's
            # subject + composition + brand quality. Meta then re-renders the
            # underlying photo at 4:5 (different aspect, same subject) for
            # final upload. Without the 1:1 QC gate Meta would skip QC entirely.
            png_path, qc_report = generate_imagen_creative_with_qc(
                variant=variant,
                copy_rewriter=rewrite_variant_copy,
                skip_rules=skip_rules,
            )
        except Exception as exc:
            log.exception("Gen raised — skipping. platform=%s cohort_geo=%s angle=%s: %s",
                          platform, cohort_geo, angle, exc)
            failed_again += 1
            continue

        verdict = (qc_report or {}).get("verdict", "UNKNOWN")
        if verdict == "FAIL":
            log.warning(
                "  QC still FAIL after regen — violations=%s",
                (qc_report or {}).get("violations", []),
            )
            _reg_update_row(
                smart_ramp_id=ramp_id,
                cohort_geo=cohort_geo,
                angle=angle,
                platform=platform,
                fields={
                    "qc_verdict": "FAIL",
                    "qc_attempts": (qc_report or {}).get("attempts"),
                    "qc_violations": json.dumps((qc_report or {}).get("violations") or []),
                },
            )
            failed_again += 1
            continue

        # PASS or UNKNOWN — ship the creative. Per-platform path below.
        if platform == "meta":
            # Re-render the QC'd photo at Meta's preferred 4:5 aspect. Reuses
            # the SAME subject prompt but at the new aspect override so the
            # subject doesn't get center-cropped out of frame.
            try:
                meta_bg = generate_imagen_photo(variant, aspect=(4, 5))
                meta_png_path = compose_ad_for_platform(
                    bg_image=meta_bg,
                    copy_variant=variant,
                    platform="meta",
                    angle=angle,
                    aspect=(4, 5),
                )
                final_png = meta_png_path
            except Exception as exc:
                log.warning(
                    "Meta 4:5 re-render failed — falling back to 1:1 PNG. %s/%s: %s",
                    cohort_geo, angle, exc,
                )
                final_png = png_path
        else:
            final_png = png_path

        try:
            from src.gdrive import upload_creative_in_hierarchy
            drive_url = upload_creative_in_hierarchy(
                file_path=Path(str(final_png)),
                ramp_id=ramp_id,
                channel=platform,
                cohort_geo=cohort_geo,
                angle=angle,
            ) or ""
        except Exception as exc:
            log.warning("Drive upload failed for %s/%s/%s: %s", platform, cohort_geo, angle, exc)
            drive_url = ""

        creative_id = ""
        try:
            if platform == "linkedin":
                if li_client is None:
                    li_client = LinkedInClient()
                image_urn = li_client.upload_image(final_png)
                ad_result = li_client.create_image_ad(
                    campaign_urn=campaign_id,
                    image_urn=image_urn,
                    headline=variant["headline"] or "Your expertise is in demand.",
                    description=variant["subheadline"]
                        or "Earn payment doing remote AI tasks on your schedule.",
                    intro_text=variant["intro_text"],
                    ad_headline=variant["ad_headline"],
                    ad_description=variant["ad_description"],
                    cta_button=variant["cta_button"],
                    destination_url="",  # use campaign's default
                )
                creative_id = ad_result.creative_id or "" if ad_result.status == "ok" else ""
            elif platform == "meta":
                if meta_client is None:
                    from src.meta_api import MetaClient
                    meta_client = MetaClient()
                from src.copy_adapter import adapt_copy_for_platform
                meta_copy = adapt_copy_for_platform(variant, "meta")
                image_id = meta_client.upload_image(final_png)
                ad_result = meta_client.create_image_ad(
                    campaign_id=campaign_id,
                    image_id=image_id,
                    headline=meta_copy.get("headline", variant["headline"]),
                    description=meta_copy.get("description", variant["subheadline"]),
                    primary_text=meta_copy.get("primary_text"),
                    cta=meta_copy.get("cta"),
                    destination_url=None,
                )
                creative_id = ad_result.creative_id or "" if ad_result.status == "ok" else ""
            elif platform == "google":
                if google_client is None:
                    from src.google_ads_api import GoogleAdsClient
                    google_client = GoogleAdsClient()
                from src.copy_adapter import adapt_copy_for_platform
                google_copy = adapt_copy_for_platform(variant, "google")
                # Google RDA wants the 1:1 square; create_image_ad auto-builds
                # the 1.91:1 pillarboxed variant from local_png_path. Pass the
                # QC'd 1:1 we already have.
                image_id = google_client.upload_image(png_path)
                ad_result = google_client.create_image_ad(
                    campaign_id=campaign_id,
                    image_id=image_id,
                    headline=(google_copy.get("headlines") or [variant["headline"]])[0],
                    description=(google_copy.get("descriptions") or [variant["subheadline"]])[0],
                    destination_url=None,
                    headlines=google_copy.get("headlines") or [variant["headline"]],
                    long_headline=google_copy.get("long_headline") or variant["headline"],
                    descriptions=google_copy.get("descriptions") or [variant["subheadline"]],
                    local_png_path=str(png_path),  # auto-generates 1.91:1 variant
                )
                creative_id = ad_result.creative_id or "" if ad_result.status == "ok" else ""
        except Exception as exc:
            log.exception("%s attach failed for %s/%s: %s", platform, cohort_geo, angle, exc)

        _reg_update_row(
            smart_ramp_id=ramp_id,
            cohort_geo=cohort_geo,
            angle=angle,
            platform=platform,
            fields={
                "creative_image_path": drive_url or str(final_png),
                "creative_urn":         creative_id if platform == "linkedin" else "",
                "platform_creative_id": creative_id,
                "qc_verdict":           verdict,
                "qc_attempts":          (qc_report or {}).get("attempts"),
                "qc_violations":        json.dumps((qc_report or {}).get("violations") or []),
            },
        )
        if creative_id:
            succeeded += 1
            log.info("  ✓ regen success — %s creative=%s", platform, creative_id)
        else:
            failed_again += 1
            log.warning("  ✗ %s attach failed; registry updated with drive_url only", platform)

    log.info("=" * 70)
    log.info(
        "REGEN COMPLETE — recovered=%d still_failing=%d total_attempted=%d",
        succeeded, failed_again, len(failed),
    )
    log.info("=" * 70)
    return 0


def run_once(args: argparse.Namespace) -> int:
    """Single poll cycle. Returns process exit code (0 == success)."""
    state = _read_state()
    state.setdefault("ramps", {})
    state.setdefault("ramp_versions", {})

    # 2026-05-22 — brief-review auto-confirm sweep. Before the main poll loop,
    # check for ramps stuck in 'awaiting_brief_review' longer than
    # config.BRIEF_REVIEW_AUTO_CONFIRM_HOURS and flip them to
    # 'awaiting_approval'. Best-effort: a DB outage doesn't block the rest of
    # the tick (the next tick will retry).
    try:
        from src.ui_decisions import auto_confirm_stale_brief_reviews
        threshold_h = int(getattr(config, "BRIEF_REVIEW_AUTO_CONFIRM_HOURS", 4))
        confirmed = auto_confirm_stale_brief_reviews(threshold_hours=threshold_h)
        if confirmed:
            log.info("Auto-confirmed %d stale brief-review ramp(s): %s",
                     len(confirmed), confirmed)
    except Exception as exc:
        log.warning("brief-review auto-confirm sweep skipped: %s", exc)

    client = SmartRampClient()

    # --ramp-id force-process path: skip the list fetch, fetch the one and process it
    if getattr(args, "ramp_id", None):
        if getattr(args, "regen_mode", False):
            # Regen-only path: bypass Stage A/B/C entirely and just re-run gen
            # for empty-creative registry rows of this ramp.
            return _regen_failed_creatives(args.ramp_id, dry_run=args.dry_run)

        full = client.fetch_ramp(args.ramp_id)
        if not full:
            log.error("Could not fetch ramp %s", args.ramp_id)
            return 1
        outcome = process_ramp(
            full, action="retry", state=state, dry_run=args.dry_run,
            force=bool(getattr(args, "force", False)),
        )
        _drive_notifier(outcome, dry_run=args.dry_run)
        if not args.dry_run:
            _write_state_atomic(state)
        return 0 if outcome["ok"] else 1

    # Normal poll path
    summaries = client.fetch_ramp_list() or []
    # Smart Ramp API quirk (2026-05-12): the list endpoint omits the `status`
    # field, so `_parse_ramp` defaults to "draft" for every summary. Detail
    # endpoint (`fetch_ramp(id)`) returns the real status. Fall back to a
    # detail check for any list item whose status looks like a default;
    # promote to `submitted` only if the detail endpoint confirms it.
    #
    # Recency guard: only promote ramps with submittedAt within the last
    # RAMP_RECENCY_GUARD_DAYS days. Without this, a fresh poller deploy
    # would auto-process every old `submitted` ramp the GitHub Actions
    # cache doesn't yet know about (spend + spurious DRAFT campaigns).
    # Older legitimate ramps can still be force-processed via --ramp-id.
    from datetime import datetime, timezone, timedelta
    recency_days = int(os.environ.get("RAMP_RECENCY_GUARD_DAYS", "7"))
    now_utc = datetime.now(timezone.utc)
    submitted: list[RampRecord] = []
    promoted_count = 0
    aged_out_count = 0
    for r in summaries:
        if (r.status or "").lower() == "submitted":
            submitted.append(r)
            continue
        # Status missing or unexpected on list → verify via detail.
        detail = client.fetch_ramp(r.id)
        if not detail or detail.status.lower() != "submitted":
            continue
        # Recency guard.
        sub_at = detail.submitted_at or ""
        try:
            sub_dt = datetime.fromisoformat(sub_at.replace("Z", "+00:00"))
        except (ValueError, TypeError):
            log.warning(
                "Ramp %s has unparseable submittedAt=%r; promoting anyway",
                detail.id, sub_at,
            )
            submitted.append(detail)
            promoted_count += 1
            continue
        age_days = (now_utc - sub_dt).days
        if age_days > recency_days:
            log.info(
                "Ramp %s aged out of recency guard (submittedAt=%s, age=%dd > %dd) "
                "— skipping. Use --ramp-id to force-process.",
                detail.id, sub_at[:10], age_days, recency_days,
            )
            aged_out_count += 1
            continue
        submitted.append(detail)
        promoted_count += 1
    if promoted_count:
        log.info(
            "Promoted %d ramp(s) from list-default 'draft' to detail-confirmed "
            "'submitted' (Smart Ramp list-endpoint status workaround)",
            promoted_count,
        )
    if aged_out_count:
        log.info("Aged-out (skipped by recency guard): %d ramp(s)", aged_out_count)
    log.info("Fetched %d ramps; %d submitted", len(summaries), len(submitted))

    for summary in submitted:
        try:
            if _should_skip_test_ramp(summary):
                log.info(
                    "Skipping test ramp: %s (requester=%s)",
                    summary.id, summary.requester_name,
                )
                continue
            full = client.fetch_ramp(summary.id)
            if not full:
                log.warning("Could not fetch full ramp content for %s", summary.id)
                continue
            sig = compute_signature(full)
            prior = state["ramps"].get(full.id)
            if _should_block_for_escalation(prior):
                log.warning(
                    "Ramp %s blocked at escalation threshold; manual reset strongly encouraged.",
                    full.id,
                )
                continue
            action = _classify_action(prior, sig)
            if action == "noop":
                log.info("Ramp %s unchanged (sig %s) — skipping", full.id, sig[:14])
                continue
            log.info("Ramp %s action=%s sig=%s", full.id, action, sig[:14])
            outcome = process_ramp(full, action=action, state=state, dry_run=args.dry_run)
            _drive_notifier(outcome, dry_run=args.dry_run)
        except Exception:
            # Per-ramp isolation: one ramp's failure NEVER aborts the rest of the poll
            log.exception("Unhandled error processing %s — continuing", summary.id)
            continue

    if not args.dry_run:
        _write_state_atomic(state)
    return 0


# ─────────────────────────────────────────────────────────────────────────────
# CLI
# ─────────────────────────────────────────────────────────────────────────────


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Smart Ramp Poller (Phase 2.6)")
    p.add_argument(
        "--once", action="store_true",
        help="Run a single poll then exit (default under launchd)",
    )
    p.add_argument(
        "--ramp-id", dest="ramp_id", default=None,
        help="Force-process exactly one ramp ID (debugging)",
    )
    p.add_argument(
        "--dry-run", action="store_true",
        help="Run pipeline + classification but skip state write + Slack notify",
    )
    p.add_argument(
        "--regen-mode", action="store_true",
        help=(
            "Regen-only: skip Stage A/B/C. Re-runs image gen + Drive upload + "
            "LinkedIn creative attach for registry rows where creative_image_path "
            "is empty. Reads qc_rule_overrides for this ramp_id from console DB "
            "and suppresses matching QC rules during gen. Requires --ramp-id."
        ),
    )
    p.add_argument(
        "--force", action="store_true",
        help=(
            "Force prep re-run for --ramp-id even when a ramp_decisions row "
            "already exists. Use to recover ramps stuck because their first "
            "prep crashed mid-flow (e.g. GMR-0022 OOM 2026-05-22 left a stale "
            "awaiting_approval row that the poller respected as 'UI controls'). "
            "Requires --ramp-id. Re-running upserts cohort_briefs + cohort_icp "
            "via ON_CONFLICT, overwriting stale rows."
        ),
    )
    return p.parse_args(argv)


def main(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    log_path = _setup_logging()
    log.info(
        "Smart Ramp Poller starting (once=%s ramp_id=%s dry_run=%s) -> %s",
        args.once, args.ramp_id, args.dry_run, log_path,
    )
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(LOCK_PATH), timeout=5):
            return run_once(args)
    except Timeout:
        log.warning("previous poll still running — exiting cleanly")
        return 0


if __name__ == "__main__":
    sys.exit(main())

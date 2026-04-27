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
# Pipeline call — STUB
# ─────────────────────────────────────────────────────────────────────────────


def run_ramp_pipeline(record: RampRecord, dry_run: bool = False, version: int = 1) -> dict:
    """STUB: replaced by Plan 02 with real implementation from main.run_launch_for_ramp.

    Returns the same dict shape Plan 02 will produce so downstream code is stable:
      {
        "ok": bool,
        "campaign_groups": [urn, ...],
        "inmail_campaigns": [urn, ...],
        "static_campaigns": [urn, ...],
        "creative_paths": {<cohort>_<mode>_<angle>: <urn or local path>, ...},
        "per_cohort": [ {cohort_id, cohort_description, inmail_urn, static_urn,
                          inmail_creative, static_creative}, ... ],
      }
    """
    log.warning(
        "run_ramp_pipeline STUB called for ramp=%s version=%s dry_run=%s — "
        "Plan 02 will replace this with main.run_launch_for_ramp",
        record.id, version, dry_run,
    )
    return {
        "ok": True,
        "campaign_groups": [],
        "inmail_campaigns": [],
        "static_campaigns": [],
        "creative_paths": {},
        "per_cohort": [
            {
                "cohort_id": c.id,
                "cohort_description": c.cohort_description,
                "inmail_urn": None,
                "static_urn": None,
                "inmail_creative": None,
                "static_creative": None,
            }
            for c in record.cohorts
        ],
    }


# ─────────────────────────────────────────────────────────────────────────────
# Per-ramp processor + orchestrator
# ─────────────────────────────────────────────────────────────────────────────


def process_ramp(
    record: RampRecord, action: str, state: dict, dry_run: bool = False
) -> dict:
    """Process a single ramp; update state[ramps][record.id]; return pipeline result.

    Honors the 5-failure escalation gate: when consecutive_failures hits the
    SMART_RAMP_FAILURE_THRESHOLD, escalation_dm_sent flips to True (Plan 03's
    notifier sends the actual escalation DM; this plan only flips the flag).
    """
    now = datetime.now(timezone.utc).isoformat()
    ramps = state.setdefault("ramps", {})
    prior = ramps.get(record.id)

    # Compute version for this run
    if action == "edit":
        version = _mark_superseded(state, record.id)
    elif action == "new":
        version = 1
    else:  # action == "retry" or "noop" — same version
        version = int(prior.get("version", 1)) if prior else 1

    sig = compute_signature(record)

    try:
        result = run_ramp_pipeline(record, dry_run=dry_run, version=version)
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
            "escalation_dm_sent flipped to True; Plan 03 notifier will fire.",
            record.id, consecutive,
        )
    ramps[record.id] = entry
    return {"ok": ok, "result": result, "err_class": err_class, "tb": tb_text, "version": version}


def run_once(args: argparse.Namespace) -> int:
    """Single poll cycle. Returns process exit code (0 == success)."""
    state = _read_state()
    state.setdefault("ramps", {})
    state.setdefault("ramp_versions", {})

    client = SmartRampClient()

    # --ramp-id force-process path: skip the list fetch, fetch the one and process it
    if getattr(args, "ramp_id", None):
        full = client.fetch_ramp(args.ramp_id)
        if not full:
            log.error("Could not fetch ramp %s", args.ramp_id)
            return 1
        outcome = process_ramp(full, action="retry", state=state, dry_run=args.dry_run)
        if not args.dry_run:
            _write_state_atomic(state)
        return 0 if outcome["ok"] else 1

    # Normal poll path
    summaries = client.fetch_ramp_list() or []
    submitted = [r for r in summaries if (r.status or "").lower() == "submitted"]
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
            process_ramp(full, action=action, state=state, dry_run=args.dry_run)
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

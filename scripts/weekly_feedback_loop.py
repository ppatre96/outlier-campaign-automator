#!/usr/bin/env python3
"""
Weekly Feedback Loop Orchestrator — Phase 2.5 V2 (FEED-22, FEED-23)

Runs every Monday 09:00 IST via launchd. Sequentially:
  Step A — v1 alerts      (scripts.post_weekly_reports.main)
  Step B — funnel analysis (src.feedback_agent.analyze_funnel_by_cohort)
  Step C — sentiment mining (src.sentiment_miner.run)
  Step D — ICP drift check (src.icp_drift_monitor.check_and_trigger per project)
  Step E — consolidated Slack post (single message, four sections)

Idempotent: filelock mutual exclusion + 6-day last_success_ts skip (unless --force).
Step-isolated: one step failing does NOT abort the others.
Loud failure: on any step error, still posts a minimal failure message to Slack.

CLI:
  --dry-run   All steps run except Slack post + reanalysis trigger
  --force     Bypass 6-day idempotency skip
  --only      Run only one of: v1 | funnel | sentiment | drift  (debugging)

Vocabulary rules (CLAUDE.md): every Slack-facing string in this file uses approved
Outlier vocabulary. NEVER emit any banned token from the substitution table below.
  NEVER "compensation"  -> say "payment"
  NEVER "performance"   -> say "progress"
  NEVER "project rate"  -> say "current tasking rate"
  NEVER "job"           -> say "task" or "opportunity"
  NEVER "role"          -> say "opportunity"
  NEVER "interview"     -> say "screening"
  NEVER "bonus"         -> say "reward"
  NEVER "assign"        -> say "match"
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

# CRITICAL: load_dotenv MUST precede `import config` (RESEARCH-V2 Pitfall 6)
from dotenv import load_dotenv

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(_PROJECT_ROOT / ".env")

# Make project root importable regardless of cwd
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from filelock import FileLock, Timeout  # noqa: E402

import config  # noqa: E402  (load_dotenv must run first)

log = logging.getLogger("weekly_feedback_loop")

STATE_PATH = _PROJECT_ROOT / "data" / "weekly_feedback_loop_state.json"
LOCK_PATH = _PROJECT_ROOT / "data" / "weekly_feedback_loop_state.lock"
LOG_DIR = _PROJECT_ROOT / "logs" / "weekly_feedback_loop"
SKIP_WINDOW = timedelta(days=6)
ACTIVE_PROJECTS_PATH = _PROJECT_ROOT / "data" / "active_projects.json"


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
    """Read state JSON; on any failure, return an empty dict (safe default)."""
    try:
        if not STATE_PATH.exists():
            return {}
        return json.loads(STATE_PATH.read_text())
    except Exception as e:  # corrupted JSON, permission error, etc.
        log.warning("Failed to read state file %s: %s", STATE_PATH, e)
        return {}


def _write_state(state: dict) -> None:
    """Persist state JSON. MUST be called inside the FileLock."""
    try:
        STATE_PATH.parent.mkdir(parents=True, exist_ok=True)
        STATE_PATH.write_text(json.dumps(state, indent=2, default=str))
    except Exception as e:
        log.warning("Failed to write state file %s: %s", STATE_PATH, e)


def _should_skip(state: dict, force: bool) -> bool:
    """Return True if a successful run occurred within SKIP_WINDOW (6 days).

    --force always bypasses the skip. A missing/malformed last_success_ts is
    treated as "never ran" (do not skip).
    """
    if force:
        return False
    last_success = state.get("last_success_ts")
    if not last_success:
        return False
    try:
        last_ts = datetime.fromisoformat(last_success)
        if last_ts.tzinfo is None:
            last_ts = last_ts.replace(tzinfo=timezone.utc)
        return datetime.now(timezone.utc) - last_ts < SKIP_WINDOW
    except Exception:
        return False


def _active_projects() -> list[str]:
    """Resolve the list of project_ids to drift-check.

    Order:
      1. data/active_projects.json (JSON list of strings) if present
      2. OUTLIER_TRACKING_PROJECT_ID env var (single project)
      3. Empty list with a warning
    """
    if ACTIVE_PROJECTS_PATH.exists():
        try:
            payload = json.loads(ACTIVE_PROJECTS_PATH.read_text())
            if isinstance(payload, list) and all(isinstance(p, str) for p in payload):
                return payload
            log.warning(
                "%s exists but is not a JSON list of strings; ignoring",
                ACTIVE_PROJECTS_PATH,
            )
        except Exception as e:
            log.warning("Failed to parse %s: %s", ACTIVE_PROJECTS_PATH, e)

    env_pid = os.getenv("OUTLIER_TRACKING_PROJECT_ID", "").strip()
    if env_pid:
        return [env_pid]

    log.warning(
        "No active projects configured; drift step will no-op. "
        "Create %s or set OUTLIER_TRACKING_PROJECT_ID in .env.",
        ACTIVE_PROJECTS_PATH,
    )
    return []


# ─────────────────────────────────────────────────────────────────────────────
# Per-step runners (each isolated in try/except; one failure NEVER aborts others)
# ─────────────────────────────────────────────────────────────────────────────


def _step_v1(dry_run: bool) -> dict:
    """Step A — run v1 weekly Slack reports in-process."""
    log.info("Step A: v1 weekly alerts")
    if dry_run:
        log.info("  [DRY-RUN] skipping v1 Slack post")
        return {"ok": True, "dry_run": True}
    try:
        # Import inside function so --only=funnel etc. don't pull v1 deps unnecessarily
        from scripts import post_weekly_reports

        post_weekly_reports.main()
        return {"ok": True}
    except SystemExit:
        # argparse in post_weekly_reports may SystemExit(0) — treat as success
        return {"ok": True, "note": "post_weekly_reports SystemExit"}
    except Exception as e:
        log.exception("Step A failed")
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _step_funnel(dry_run: bool) -> dict:
    """Step B — full-funnel analysis + drop classification (Plan 05)."""
    log.info("Step B: funnel analysis")
    try:
        from src.feedback_agent import FeedbackAgent
        from src.redash_db import RedashClient

        agent = FeedbackAgent(RedashClient())
        rows = agent.analyze_funnel_by_cohort(days_back=7)
        diagnosis = agent.identify_funnel_drop_stage(rows) if rows else {}
        return {"ok": True, "n_rows": len(rows), "diagnosis": diagnosis}
    except Exception as e:
        log.exception("Step B failed")
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "diagnosis": {}}


def _step_sentiment(dry_run: bool) -> dict:
    """Step C — sentiment miner (Plan 06)."""
    log.info("Step C: sentiment miner")
    try:
        from src import sentiment_miner

        result = sentiment_miner.run()
        return {"ok": True, **result}
    except Exception as e:
        log.exception("Step C failed")
        return {"ok": False, "error": f"{type(e).__name__}: {e}"}


def _step_drift(dry_run: bool, projects: list[str]) -> dict:
    """Step D — ICP drift + conditional reanalysis per project (Plan 07).

    In --dry-run mode, runs snapshot + compute_drift but SKIPS check_and_trigger's
    reanalysis trigger (no outlier-data-analyst invocation).
    """
    log.info("Step D: ICP drift (%d projects)", len(projects))
    results: dict[str, dict] = {}
    if not projects:
        return {"ok": True, "note": "no_active_projects", "projects": {}}
    try:
        from src import icp_drift_monitor as idm

        for pid in projects:
            try:
                if dry_run:
                    idm.snapshot(pid)
                    drift = idm.compute_drift(pid)
                    results[pid] = {
                        "drift": drift,
                        "triggered": False,
                        "trigger_reason": "dry_run",
                        "rate_limited": False,
                        "dry_run": True,
                    }
                else:
                    results[pid] = idm.check_and_trigger(pid)
            except Exception as e:
                log.exception("drift failed for project %s", pid)
                results[pid] = {"ok": False, "error": f"{type(e).__name__}: {e}"}
        return {"ok": True, "projects": results}
    except Exception as e:
        log.exception("Step D failed")
        return {"ok": False, "error": f"{type(e).__name__}: {e}", "projects": {}}


# ─────────────────────────────────────────────────────────────────────────────
# Slack message builders (vocabulary-clean per CLAUDE.md)
# ─────────────────────────────────────────────────────────────────────────────


def _format_funnel_section(funnel: dict) -> list[str]:
    """Build the Funnel Drop Diagnosis section lines."""
    if not funnel or not funnel.get("ok"):
        err = (funnel or {}).get("error", "unknown error")
        return [f"Funnel Drop Diagnosis", f"  • [step funnel failed: {err}]"]

    diagnosis = funnel.get("diagnosis") or {}
    flagged = [
        (cohort, info)
        for cohort, info in diagnosis.items()
        if isinstance(info, dict) and info.get("drop_stage") and info.get("drop_stage") != "none"
    ]
    out = ["Funnel Drop Diagnosis"]
    if not flagged:
        out.append("  • No funnel drops flagged this week.")
        return out
    for cohort, info in flagged:
        stage = info.get("drop_stage", "unknown")
        rate = info.get("drop_rate")
        baseline = info.get("baseline_rate")
        rate_s = f"{rate:.1%}" if isinstance(rate, (int, float)) and rate is not None else "n/a"
        base_s = (
            f"{baseline:.1%}"
            if isinstance(baseline, (int, float)) and baseline is not None
            else "n/a"
        )
        out.append(
            f"  • {cohort} — drop at {stage} stage (rate {rate_s} vs baseline {base_s})"
        )
    return out


def _format_sentiment_section(sentiment: dict) -> list[str]:
    """Build the Sentiment Themes section by reading callouts JSON from disk."""
    if not sentiment or not sentiment.get("ok"):
        err = (sentiment or {}).get("error", "unknown error")
        return [f"Sentiment Themes", f"  • [step sentiment failed: {err}]"]

    callouts_path = sentiment.get("callouts_path")
    themes: list[dict] = []
    if callouts_path:
        try:
            data = json.loads(Path(callouts_path).read_text())
            themes = data.get("themes", []) or []
        except Exception as e:
            log.warning("Could not read callouts file %s: %s", callouts_path, e)

    out = [f"Sentiment Themes (top {min(5, len(themes))})"]
    if not themes:
        out.append("  • No new themes surfaced.")
        return out
    for theme in themes[:5]:
        label = theme.get("theme", "unlabeled")
        sent = theme.get("sentiment", "neutral")
        evidence = theme.get("evidence_count", 0)
        out.append(f"  • {label} — {sent} ({evidence} mentions)")
    return out


def _format_drift_section(drift: dict) -> list[str]:
    """Build the ICP Drift section."""
    if not drift or not drift.get("ok"):
        err = (drift or {}).get("error", "unknown error")
        return [f"ICP Drift", f"  • [step drift failed: {err}]"]

    projects = drift.get("projects") or {}
    out = ["ICP Drift"]
    if not projects:
        out.append("  • No active projects configured for drift tracking.")
        return out
    for pid, pres in projects.items():
        if pres.get("ok") is False:
            out.append(f"  • Project {pid}: error — {pres.get('error', 'unknown')}")
            continue
        d = pres.get("drift") or {}
        score = d.get("drift_score")
        cold = d.get("cold_start", False)
        if cold:
            out.append(f"  • Project {pid}: insufficient history — skipping drift")
            continue
        triggered = pres.get("triggered", False)
        rate_limited = pres.get("rate_limited", False)
        score_s = f"{score:.3f}" if isinstance(score, (int, float)) else "n/a"
        if triggered:
            status = "reanalysis triggered"
        elif rate_limited:
            status = "rate-limited"
        else:
            status = "within-threshold"
        out.append(f"  • Project {pid}: drift {score_s} — {status}")
    return out


def _format_v1_section(v1_status: dict) -> list[str]:
    """Build the v1 creative progress alerts section.

    Note: v1 alerts are posted in-process by Step A via the existing
    post_weekly_feedback_alert() flow. This section ONLY summarizes whether
    Step A ran cleanly — the full v1 alert text is its own Slack message.
    """
    if not v1_status:
        return ["Creative Progress Alerts (v1)", "  • [no v1 status reported]"]

    if not v1_status.get("ok"):
        err = v1_status.get("error", "unknown error")
        return ["Creative Progress Alerts (v1)", f"  • [step v1 failed: {err}]"]

    if v1_status.get("dry_run"):
        return [
            "Creative Progress Alerts (v1)",
            "  • [DRY-RUN] v1 alerts skipped — no Slack post issued",
        ]

    note = v1_status.get("note")
    if note:
        return ["Creative Progress Alerts (v1)", f"  • v1 alerts posted ({note})"]
    return ["Creative Progress Alerts (v1)", "  • v1 alerts posted to Slack."]


def _build_consolidated_message(
    v1_status: dict, funnel: dict, sentiment: dict, drift: dict
) -> str:
    """Build the single multi-section Slack message.

    Section order is locked (CONTEXT-V2):
      1. Creative Progress Alerts (v1)
      2. Funnel Drop Diagnosis
      3. Sentiment Themes
      4. ICP Drift

    All copy uses approved Outlier vocabulary (CLAUDE.md). NEVER emit banned
    tokens from the substitution table — see module docstring for the full list.
    """
    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    lines: list[str] = [f"Weekly Feedback Loop — {today}", ""]
    lines.extend(_format_v1_section(v1_status))
    lines.append("")
    lines.extend(_format_funnel_section(funnel))
    lines.append("")
    lines.extend(_format_sentiment_section(sentiment))
    lines.append("")
    lines.extend(_format_drift_section(drift))
    return "\n".join(lines)


def _build_failure_message(failures: dict) -> str:
    """Build a minimal Slack message naming failed steps (loud failure)."""
    lines = ["Weekly Feedback Loop — partial failure"]
    for step, err in failures.items():
        lines.append(f"  • Step {step}: {err}")
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Orchestration entrypoints
# ─────────────────────────────────────────────────────────────────────────────


def run_once(dry_run: bool = False, only: str | None = None) -> dict:
    """Run the four steps sequentially (or one, if --only).

    Returns:
        {
          "results":  {step_name: step_result_dict, ...},
          "failures": {step_name: error_message, ...},  # only failed steps
        }
    """
    projects = _active_projects()

    results: dict[str, dict] = {}
    step_map = {
        "v1": lambda: _step_v1(dry_run),
        "funnel": lambda: _step_funnel(dry_run),
        "sentiment": lambda: _step_sentiment(dry_run),
        "drift": lambda: _step_drift(dry_run, projects),
    }
    steps_to_run = [only] if only else list(step_map.keys())
    for name in steps_to_run:
        if name not in step_map:
            log.error("Unknown step: %s", name)
            continue
        results[name] = step_map[name]()

    failures = {
        k: v.get("error", "unknown error")
        for k, v in results.items()
        if v and not v.get("ok")
    }

    # Always attempt the consolidated Slack post — loud failure beats silent failure.
    try:
        if failures:
            msg = _build_failure_message(failures)
        else:
            msg = _build_consolidated_message(
                results.get("v1", {}),
                results.get("funnel", {}),
                results.get("sentiment", {}),
                results.get("drift", {}),
            )
        if dry_run:
            log.info("[DRY-RUN] would post to Slack:\n%s", msg)
        else:
            from scripts.post_weekly_reports import _post_to_slack

            _post_to_slack(msg)
    except Exception as e:
        log.exception("Consolidated Slack post failed: %s", e)

    return {"results": results, "failures": failures}


def main() -> int:
    parser = argparse.ArgumentParser(description="Weekly Feedback Loop Orchestrator")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Run all steps WITHOUT posting to Slack or triggering reanalysis",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Bypass 6-day idempotency skip (manual reruns)",
    )
    parser.add_argument(
        "--only",
        choices=["v1", "funnel", "sentiment", "drift"],
        help="Run only one step (debugging)",
    )
    args = parser.parse_args()

    log_path = _setup_logging()
    log.info(
        "Weekly Feedback Loop starting (dry_run=%s, force=%s, only=%s) -> %s",
        args.dry_run,
        args.force,
        args.only,
        log_path,
    )

    # Acquire mutex lock (avoid Pitfall 5 race)
    LOCK_PATH.parent.mkdir(parents=True, exist_ok=True)
    try:
        with FileLock(str(LOCK_PATH), timeout=10):
            state = _read_state()
            if _should_skip(state, args.force):
                log.info("Skipping — last_success_ts within %s", SKIP_WINDOW)
                return 0
            try:
                outcome = run_once(dry_run=args.dry_run, only=args.only)
            except Exception as e:
                log.exception("Unhandled error: %s", e)
                state["last_failure_ts"] = datetime.now(timezone.utc).isoformat()
                state["last_failure_reason"] = f"{type(e).__name__}: {e}"
                state["last_failure_traceback"] = traceback.format_exc()
                _write_state(state)
                return 2

            if outcome["failures"]:
                state["last_failure_ts"] = datetime.now(timezone.utc).isoformat()
                state["last_failure_reason"] = json.dumps(outcome["failures"])
                state["runs_this_week"] = int(state.get("runs_this_week", 0)) + 1
                _write_state(state)
                log.warning(
                    "Completed with %d failed steps: %s",
                    len(outcome["failures"]),
                    list(outcome["failures"]),
                )
                return 1
            if not args.dry_run:
                state["last_success_ts"] = datetime.now(timezone.utc).isoformat()
                state["runs_this_week"] = int(state.get("runs_this_week", 0)) + 1
                _write_state(state)
            log.info("Weekly Feedback Loop completed successfully")
            return 0
    except Timeout:
        log.warning(
            "Another weekly_feedback_loop is already running — exiting cleanly"
        )
        return 0


if __name__ == "__main__":
    sys.exit(main())

"""
ICP Drift Monitor — Phase 2.5 V2 (FEED-20, FEED-21)

Weekly snapshots Stage 1 ICP output, computes per-feature KL divergence vs a
trailing 4-week median, and auto-triggers outlier-data-analyst reanalysis when
drift > ICP_DRIFT_THRESHOLD AND n_rows >= ICP_DRIFT_MIN_ROWS AND no reanalysis
was triggered in the past 7 days.

Public functions:
  snapshot(project_id)          → Path  (writes this week's parquet snapshot)
  compute_drift(project_id)     → dict  (drift_score, per_feature, cold_start, n_rows)
  check_and_trigger(project_id) → dict  (runs full pipeline + conditionally invokes reanalysis)
  categorical_kl(p, q)          → float (scipy.stats.entropy KL with EPSILON for zero bins)

Uses scipy.stats.entropy for KL (NOT a hand-rolled implementation).
"""
from __future__ import annotations

import asyncio
import inspect
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd
from scipy.stats import entropy

import config

log = logging.getLogger(__name__)

EPSILON = 1e-10  # avoids inf on zero bins per RESEARCH-V2 Pitfall 4

_SNAPSHOT_DIR  = Path("data/icp_snapshots")
_DRIFT_STATE   = Path("data/icp_drift_state.json")
_MIN_SNAPSHOTS = 2   # cold-start cutoff (per user plan-breakdown note)
_BASELINE_WEEKS = 4  # trailing-N-week median for stable periods

# Features to drift-check. Keys must match RedashClient.fetch_stage1_contributors columns.
_CATEGORICAL_FEATURES = [
    "worker_source", "resume_degree", "resume_field",
    "resume_job_title", "experience_band",
]
_NUMERIC_FEATURES = ["total_payout_attempts", "task_count_30d"]


def categorical_kl(this_week: pd.Series, baseline: pd.Series) -> float:
    """KL divergence of this_week vs baseline over categorical value counts.

    Uses scipy.stats.entropy (relative entropy = KL when q is provided).
    Adds EPSILON to both vectors before normalizing to avoid inf on zero bins
    (Pitfall 4 in RESEARCH-V2). Always returns a finite float — even when
    this_week contains categorical values absent from baseline.
    """
    if this_week is None or baseline is None:
        return 0.0
    if this_week.empty or baseline.empty:
        return 0.0
    p = this_week.value_counts(normalize=True)
    q = baseline.value_counts(normalize=True)
    union = p.index.union(q.index)
    p_vec = p.reindex(union, fill_value=0.0).values + EPSILON
    q_vec = q.reindex(union, fill_value=0.0).values + EPSILON
    p_vec = p_vec / p_vec.sum()
    q_vec = q_vec / q_vec.sum()
    return float(entropy(p_vec, q_vec))  # KL(p || q) using scipy relative entropy


def _numeric_shift(this_week: pd.Series, baseline: pd.Series) -> float:
    """Absolute standardized mean shift: |this_mean - baseline_mean| / baseline_std."""
    tw = pd.to_numeric(this_week, errors="coerce").dropna()
    bl = pd.to_numeric(baseline, errors="coerce").dropna()
    if tw.empty or bl.empty:
        return 0.0
    bl_std = float(bl.std(ddof=1))
    if bl_std == 0 or np.isnan(bl_std):
        return 0.0
    return float(abs(tw.mean() - bl.mean()) / bl_std)


def snapshot(project_id: str, today: Optional[datetime] = None) -> Path:
    """Fetch Stage 1 ICP for project_id, save as parquet. Returns written path.

    Path: data/icp_snapshots/<project_id>/<yyyy-mm-dd>.parquet
    Empty Stage 1 output is logged + an empty parquet still written
    (so cold-start counters advance and one bad week does not break the loop).
    """
    from src.redash_db import RedashClient

    today_iso = (today or datetime.now(timezone.utc)).date().isoformat()
    out_dir = _SNAPSHOT_DIR / str(project_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"{today_iso}.parquet"

    try:
        df = RedashClient().fetch_stage1_contributors(project_id)
    except Exception as e:
        log.exception("Stage1 fetch failed for project %s: %s", project_id, e)
        df = pd.DataFrame()

    if df is None or df.empty:
        log.warning("Stage1 snapshot empty for project %s — writing empty parquet", project_id)
        df = pd.DataFrame()
    df.to_parquet(out_path, index=False)  # pyarrow engine
    log.info("Wrote Stage1 snapshot: %s (%d rows)", out_path, len(df))
    return out_path


def _load_snapshots(project_id: str) -> list[tuple[datetime, pd.DataFrame]]:
    """Load all parquet snapshots for project_id, sorted ascending by date."""
    out_dir = _SNAPSHOT_DIR / str(project_id)
    if not out_dir.exists():
        return []
    items: list[tuple[datetime, pd.DataFrame]] = []
    for f in sorted(out_dir.glob("*.parquet")):
        try:
            date = datetime.fromisoformat(f.stem).replace(tzinfo=timezone.utc)
            items.append((date, pd.read_parquet(f)))
        except Exception as e:
            log.warning("Skipping malformed snapshot %s: %s", f, e)
    return items


def compute_drift(project_id: str) -> dict:
    """Compute drift score for project_id's Stage 1 ICP.

    Returns: {drift_score: float|None, per_feature: dict, cold_start: bool, n_rows: int}

    drift_score = max(categorical KL across features) + sum(numeric abs-mean-shifts).
    When fewer than 2 snapshots exist: returns cold_start=True, drift_score=None
    and logs "insufficient history, skipping drift" (per plan contract).
    """
    snaps = _load_snapshots(project_id)
    if len(snaps) < _MIN_SNAPSHOTS:
        log.info(
            "ICP drift: insufficient history for project %s (%d snapshots) — skipping drift",
            project_id, len(snaps),
        )
        return {
            "drift_score": None,
            "per_feature": {},
            "cold_start": True,
            "n_rows": len(snaps[-1][1]) if snaps else 0,
        }

    _this_date, this_df = snaps[-1]
    # Baseline = last N prior snapshots (up to _BASELINE_WEEKS), excluding this week.
    baseline_frames = [df for (_, df) in snaps[-(_BASELINE_WEEKS + 1):-1]]
    if not baseline_frames:
        return {
            "drift_score": None,
            "per_feature": {},
            "cold_start": True,
            "n_rows": len(this_df),
        }
    baseline_df = pd.concat(baseline_frames, ignore_index=True)

    per_feature: dict[str, float] = {}
    # Categorical KL
    for feat in _CATEGORICAL_FEATURES:
        if feat not in this_df.columns or feat not in baseline_df.columns:
            continue
        kl = categorical_kl(this_df[feat].dropna(), baseline_df[feat].dropna())
        per_feature[feat] = kl
    # Numeric abs-mean-shift (standardized by baseline std)
    for feat in _NUMERIC_FEATURES:
        if feat not in this_df.columns or feat not in baseline_df.columns:
            continue
        shift = _numeric_shift(this_df[feat], baseline_df[feat])
        per_feature[feat] = shift

    if not per_feature:
        return {
            "drift_score": 0.0,
            "per_feature": {},
            "cold_start": False,
            "n_rows": len(this_df),
        }

    cat_scores = [per_feature[f] for f in _CATEGORICAL_FEATURES if f in per_feature]
    num_scores = [per_feature[f] for f in _NUMERIC_FEATURES if f in per_feature]
    drift_score = (max(cat_scores) if cat_scores else 0.0) + sum(num_scores)
    return {
        "drift_score": float(drift_score),
        "per_feature": per_feature,
        "cold_start": False,
        "n_rows": len(this_df),
    }


def _load_drift_state() -> dict:
    if not _DRIFT_STATE.exists():
        return {}
    try:
        return json.loads(_DRIFT_STATE.read_text())
    except Exception:
        log.warning("drift state file corrupt — resetting")
        return {}


def _save_drift_state(state: dict) -> None:
    _DRIFT_STATE.parent.mkdir(parents=True, exist_ok=True)
    _DRIFT_STATE.write_text(json.dumps(state, indent=2, default=str))


def _invoke_trigger(orchestrator, **kwargs):
    """Call ReanalysisOrchestrator.trigger_reanalysis whether sync or async.

    The current contract (src/reanalysis_loop.py:16) is sync; v1 SUMMARY notes
    a sync wrapper exists. Handle both shapes so this monitor stays robust if
    the orchestrator's signature changes.
    """
    result = orchestrator.trigger_reanalysis(**kwargs)
    if inspect.iscoroutine(result):
        return asyncio.run(result)
    return result


def check_and_trigger(project_id: str, force_snapshot: bool = True) -> dict:
    """Full pipeline: snapshot → compute_drift → conditional reanalysis trigger.

    Trigger fires IFF all of:
      - drift_score is not None (not cold-start)
      - drift_score > config.ICP_DRIFT_THRESHOLD
      - n_rows >= config.ICP_DRIFT_MIN_ROWS
      - No reanalysis triggered for this project in the past 7 days (rate limit)

    Returns dict: {snapshot_path, drift, triggered, trigger_reason, rate_limited}.

    Note: rate-limit uses strict `<` against timedelta(days=7) — a trigger
    exactly 7 days after the last one IS allowed to re-fire.
    """
    snap_path = snapshot(project_id) if force_snapshot else None
    drift = compute_drift(project_id)

    result = {
        "snapshot_path": str(snap_path) if snap_path else None,
        "drift": drift,
        "triggered": False,
        "trigger_reason": None,
        "rate_limited": False,
    }

    score = drift.get("drift_score")
    n_rows = drift.get("n_rows", 0)

    # Short-circuit gates
    if drift.get("cold_start"):
        result["trigger_reason"] = "cold_start"
        return result
    if score is None:
        result["trigger_reason"] = "no_score"
        return result
    if n_rows < config.ICP_DRIFT_MIN_ROWS:
        result["trigger_reason"] = (
            f"below_noise_floor ({n_rows} < {config.ICP_DRIFT_MIN_ROWS})"
        )
        return result
    if score <= config.ICP_DRIFT_THRESHOLD:
        result["trigger_reason"] = (
            f"within_threshold ({score:.4f} <= {config.ICP_DRIFT_THRESHOLD})"
        )
        return result

    # Rate-limit: at most one reanalysis per project per 7 days
    state = _load_drift_state()
    last_ts_raw = state.get(str(project_id), {}).get("last_reanalysis_ts")
    if last_ts_raw:
        try:
            last_ts = datetime.fromisoformat(last_ts_raw)
            if last_ts.tzinfo is None:
                last_ts = last_ts.replace(tzinfo=timezone.utc)
            if datetime.now(timezone.utc) - last_ts < timedelta(days=7):
                log.info(
                    "Drift trigger rate-limited for project %s (last %s)",
                    project_id, last_ts_raw,
                )
                result["rate_limited"] = True
                result["trigger_reason"] = "rate_limited_7d"
                return result
        except Exception:
            pass  # malformed timestamp — proceed to fire the trigger

    # Fire the trigger — handle sync or async signature
    try:
        from src.reanalysis_loop import ReanalysisOrchestrator
        orchestrator = ReanalysisOrchestrator()
        _invoke_trigger(orchestrator, reason="icp_drift")
        result["triggered"] = True
        result["trigger_reason"] = (
            f"drift_{score:.4f}_exceeds_{config.ICP_DRIFT_THRESHOLD}"
        )
        # Persist last-trigger timestamp + score per project
        state.setdefault(str(project_id), {})
        state[str(project_id)]["last_reanalysis_ts"] = datetime.now(timezone.utc).isoformat()
        state[str(project_id)]["last_drift_score"] = score
        _save_drift_state(state)
        log.info(
            "ICP drift triggered reanalysis for project %s (score=%.4f)",
            project_id, score,
        )
    except Exception as e:
        log.exception(
            "Reanalysis trigger failed for project %s: %s", project_id, e,
        )
        result["triggered"] = False
        result["trigger_reason"] = f"trigger_error: {e.__class__.__name__}"

    return result

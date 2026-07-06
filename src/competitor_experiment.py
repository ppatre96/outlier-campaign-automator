"""Competitor-insight experiment loop.

Closes the loop between the weekly competitor intelligence and the campaigns we
launch:

  1. refresh_backlog()  — turn competitor `experiment_ideas` into a prioritized,
     persistent ExperimentBacklog (via the previously-dead ExperimentScientistAgent)
     and pin the top-priority one to `data/experiment_directive.json`.
  2. directive_prompt_block() — read by brief_generator + figma_creative so the
     challenger arm (angle C) is deterministically built around the pinned insight.
  3. read_results() — attribute per-(cohort, angle) LinkedIn creative metrics to the
     challenger arm, compare angle C vs the cohort's baseline angles, and record the
     outcome back on the backlog (winner/loser).
  4. format_slack_section() — one "Experiment Results" block for the weekly post.

Auto-readback is LinkedIn-only today: VIEW.LINKEDIN_CREATIVE_COSTS is the only
per-angle metrics source wired into the feedback agent. On Meta/Google/TikTok/Reddit
the challenger creative still ships and is tagged, but results must be read manually.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path
from typing import Optional

from src.memory import ExperimentBacklog
from src.experiment_scientist_agent import ExperimentScientistAgent

log = logging.getLogger(__name__)

INTEL_PATH = "data/competitor_intel/latest.json"
BACKLOG_PATH = "data/experiment_backlog.json"
DIRECTIVE_PATH = "data/experiment_directive.json"

# The challenger arm. Angles A/B stay business-as-usual; C tests the pinned insight.
CHALLENGER_ANGLE = "C"

# Minimum impressions on BOTH the challenger and the baseline before we call a
# result — below this the CTR delta is noise, so we report "still gathering".
_MIN_IMPRESSIONS = 500


def _normalize_ideas(intel_data: dict) -> list[dict]:
    """Coerce `experiment_ideas` (string-list or dict-list) into the dict shape
    ExperimentScientistAgent expects, pinned to the challenger angle."""
    ideas = intel_data.get("experiment_ideas") or []
    out: list[dict] = []
    for it in ideas:
        if isinstance(it, str):
            desc = it.strip()
        elif isinstance(it, dict):
            desc = (it.get("description") or it.get("idea") or "").strip()
        else:
            desc = ""
        if not desc:
            continue
        out.append({
            # Slug the cohort so each idea is a distinct backlog candidate. The
            # scientist matches/dedups on (cohort, angle) only, so a constant
            # cohort would collapse every competitor idea into one entry and
            # defeat the priority queue. Competitor ideas are generic (no real
            # cohort), so this bucket id is purely a backlog key.
            "cohort": _slug(desc),
            "angle": CHALLENGER_ANGLE,
            "photo_subject": "baseline",
            "description": desc,
            "expected_impact": it.get("expected_impact", "3%") if isinstance(it, dict) else "3%",
        })
    return out


def _slug(text: str, words: int = 5) -> str:
    """Short kebab slug from the first few words of an idea (for stable keys)."""
    toks = [t for t in "".join(c if c.isalnum() else " " for c in text.lower()).split() if t]
    return "-".join(toks[:words]) or "idea"


def refresh_backlog(
    intel_path: str = INTEL_PATH,
    backlog_path: str = BACKLOG_PATH,
    directive_path: str = DIRECTIVE_PATH,
    feedback_hypotheses: Optional[list[dict]] = None,
) -> dict:
    """Ingest competitor ideas into the persistent backlog and pin the top one.

    Returns a summary dict; best-effort (never raises)."""
    p = Path(intel_path)
    if not p.exists():
        log.info("refresh_backlog: no competitor intel at %s — skipping", intel_path)
        return {"ok": False, "reason": "no_intel", "backlog_size": 0}

    try:
        intel_data = json.loads(p.read_text())
    except Exception as exc:  # noqa: BLE001 — never let a bad file break the loop
        log.warning("refresh_backlog: failed to read intel: %s", exc)
        return {"ok": False, "reason": f"bad_intel:{exc}", "backlog_size": 0}

    # The scientist expects dict-shaped experiment_ideas; latest.json stores
    # them as plain strings. Normalize before ingest (shape bridge).
    normalized_intel = {"experiment_ideas": _normalize_ideas(intel_data)}

    backlog = ExperimentBacklog(backlog_path)
    scientist = ExperimentScientistAgent(backlog=backlog)
    scientist.ingest_feedback(feedback_hypotheses or [], normalized_intel)
    backlog.save()

    pending = backlog.peek_next(1)
    directive = None
    if pending:
        top = pending[0]
        directive = {
            "experiment_id": _experiment_id(top),
            "signal": (top.get("reason") or top.get("description") or "").strip(),
            "description": top.get("reason") or top.get("description") or "",
            "cohort": top.get("cohort", "GENERAL"),
            "angle": top.get("angle", CHALLENGER_ANGLE),
            "photo_subject": top.get("photo_subject", "baseline"),
            "priority_score": top.get("priority_score", 0.0),
        }
        Path(directive_path).parent.mkdir(parents=True, exist_ok=True)
        Path(directive_path).write_text(json.dumps(directive, indent=2))
        log.info("refresh_backlog: pinned experiment %s (priority=%.1f)",
                 directive["experiment_id"], directive["priority_score"])

    return {
        "ok": True,
        "backlog_size": len(backlog.backlog),
        "directive": directive,
    }


def _experiment_id(hyp: dict) -> str:
    """Stable id for a hypothesis: EXP-<cohort>-<angle>."""
    return f"EXP-{hyp.get('cohort', 'GENERAL')}-{hyp.get('angle', CHALLENGER_ANGLE)}"


def active_directive(directive_path: str = DIRECTIVE_PATH) -> Optional[dict]:
    """The insight currently under test, or None."""
    p = Path(directive_path)
    if not p.exists():
        return None
    try:
        return json.loads(p.read_text())
    except Exception:  # noqa: BLE001
        return None


def directive_prompt_block(directive_path: str = DIRECTIVE_PATH) -> str:
    """Prompt fragment pinning the challenger arm to the active insight.

    Read by brief_generator._load_competitor_context and figma_creative so angle
    C is deterministically built around the tested insight (A/B stay baseline).
    Empty string when no experiment is active."""
    d = active_directive(directive_path)
    if not d or not (d.get("description") or "").strip():
        return ""
    return (
        f"\n\nPRIORITY EXPERIMENT — Angle {d.get('angle', CHALLENGER_ANGLE)} MUST be built "
        f"around this competitor insight (angles A and B stay baseline):\n"
        f"- {d['description'].strip()}\n"
    )


def read_results(days_back: int = 7, backlog_path: str = BACKLOG_PATH) -> dict:
    """Compare the challenger angle vs baseline angles on LinkedIn CTR, per cohort.

    Returns {ok, linkedin_only: True, cohorts: [{cohort, challenger_ctr,
    baseline_ctr, lift_pct, n_impressions, verdict}], directive}. Best-effort."""
    directive = active_directive()
    try:
        from src.redash_db import RedashClient

        df = RedashClient().query_creative_performance(days_back=days_back)
    except Exception as exc:  # noqa: BLE001
        log.warning("read_results: creative performance query failed: %s", exc)
        return {"ok": False, "reason": f"query_failed:{exc}", "cohorts": [], "directive": directive}

    if df is None or df.empty:
        return {"ok": True, "linkedin_only": True, "cohorts": [], "directive": directive,
                "note": "no LinkedIn creative rows in window"}

    import pandas as pd  # local import — pandas is already a pipeline dep

    for col in ("ctr", "impressions"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Per-insight attribution: if the pinned experiment has an id AND any
    # creative rows are tagged with it, the challenger is ONLY those tagged
    # angle-C creatives (they actually tested this insight). Otherwise fall
    # back to the angle-C-vs-baseline proxy (e.g. before any tagged creative
    # has run). `attribution` reports which mode was used — honesty over
    # implying we measured the specific insight when we didn't.
    exp_id = (directive or {}).get("experiment_id") or ""
    has_tagged = (
        exp_id and "experiment_id" in df.columns
        and (df["experiment_id"].astype(str) == exp_id).any()
    )
    attribution = "per-insight" if has_tagged else "angle-proxy"

    cohorts: list[dict] = []
    for cohort_name, grp in df.groupby("cohort_name"):
        is_c = grp["angle"].astype(str).str.upper() == CHALLENGER_ANGLE
        if has_tagged:
            chal = grp[is_c & (grp["experiment_id"].astype(str) == exp_id)]
        else:
            chal = grp[is_c]
        base = grp[~is_c]
        if chal.empty or base.empty:
            continue

        chal_imp = float(chal["impressions"].sum())
        base_imp = float(base["impressions"].sum())
        # Impression-weighted CTR so a tiny creative can't swing the arm.
        # The query returns ctr in percentage-points (clicks/impressions*100);
        # divide by 100 so these are true fractions and format cleanly as %.
        chal_ctr = _weighted_ctr(chal)
        base_ctr = _weighted_ctr(base)
        if chal_ctr is None or base_ctr is None or base_ctr == 0:
            continue
        chal_ctr /= 100.0
        base_ctr /= 100.0

        lift_pct = (chal_ctr - base_ctr) / base_ctr * 100.0
        underpowered = chal_imp < _MIN_IMPRESSIONS or base_imp < _MIN_IMPRESSIONS
        if underpowered:
            verdict = "gathering"
        elif lift_pct >= 5.0:
            verdict = "winner"
        elif lift_pct <= -5.0:
            verdict = "loser"
        else:
            verdict = "flat"

        cohorts.append({
            "cohort": str(cohort_name),
            "challenger_ctr": round(chal_ctr, 6),
            "baseline_ctr": round(base_ctr, 6),
            "lift_pct": round(lift_pct, 1),
            "n_impressions": int(chal_imp + base_imp),
            "verdict": verdict,
        })

    _record_outcomes(cohorts, directive, backlog_path)
    return {"ok": True, "linkedin_only": True, "cohorts": cohorts,
            "directive": directive, "attribution": attribution}


def _weighted_ctr(grp) -> Optional[float]:
    """Impression-weighted mean CTR for a set of creative rows."""
    imp = grp["impressions"].fillna(0)
    ctr = grp["ctr"].fillna(0)
    total_imp = float(imp.sum())
    if total_imp <= 0:
        return None
    return float((ctr * imp).sum() / total_imp)


def _record_outcomes(cohorts: list[dict], directive: Optional[dict], backlog_path: str) -> None:
    """Write conclusive verdicts back onto the pinned experiment's backlog entry."""
    if not directive:
        return
    decided = [c for c in cohorts if c["verdict"] in ("winner", "loser")]
    if not decided:
        return
    try:
        backlog = ExperimentBacklog(backlog_path)
        key = (directive.get("cohort", "GENERAL"), directive.get("angle", CHALLENGER_ANGLE),
               directive.get("photo_subject", "baseline"))
        winners = sum(1 for c in decided if c["verdict"] == "winner")
        backlog.mark_completed(key, {
            "winners": winners,
            "losers": len(decided) - winners,
            "cohorts": decided,
        })
        backlog.save()
        log.info("read_results: recorded outcome for %s (%d winners / %d decided)",
                 directive.get("experiment_id"), winners, len(decided))
    except Exception as exc:  # noqa: BLE001
        log.warning("read_results: failed to record outcome: %s", exc)


def format_slack_section(results: dict) -> list[str]:
    """Render the 'Experiment Results' block for the weekly consolidated message.

    Outlier vocabulary: no banned tokens (avoids 'performance' etc.)."""
    directive = (results or {}).get("directive")
    lines = ["🧪 Experiment Results"]

    if not directive:
        lines.append("  • No competitor insight is currently under test.")
        return lines

    lines.append(f"  Testing: {directive.get('description', '(unknown)')[:160]}")
    attribution = (results or {}).get("attribution", "angle-proxy")
    mode = ("tagged creatives only" if attribution == "per-insight"
            else "angle C proxy — no tagged creatives live yet")
    lines.append(f"  Arm: angle {directive.get('angle', CHALLENGER_ANGLE)} vs baseline "
                 f"(LinkedIn CTR only; {mode})")

    cohorts = (results or {}).get("cohorts") or []
    if not cohorts:
        lines.append("  • Still gathering — no eligible LinkedIn creatives in the window yet.")
        return lines

    symbol = {"winner": "✅", "loser": "❌", "flat": "➖", "gathering": "⏳"}
    for c in cohorts:
        sfx = " (low volume)" if c["verdict"] == "gathering" else ""
        lines.append(
            f"  {symbol.get(c['verdict'], '•')} {c['cohort']}: "
            f"challenger {c['challenger_ctr']:.2%} vs baseline {c['baseline_ctr']:.2%} "
            f"→ {c['lift_pct']:+.1f}% (n={c['n_impressions']:,}){sfx}"
        )
    lines.append("  Meta/Google/TikTok/Reddit: challenger ships + tagged; read results manually.")
    return lines

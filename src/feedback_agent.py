"""
Feedback Agent — Creative & Cohort Performance Analysis

Analyzes LinkedIn campaign creative and cohort performance metrics from Redash,
generates data-driven hypotheses, and produces a weekly Slack alert flagging
underperforming cohorts.

Phase 2.5 feedback loop — FEED-01, FEED-02, FEED-05, FEED-06, FEED-07.

Usage:
    from src.feedback_agent import FeedbackAgent
    from src.redash_db import RedashClient

    agent = FeedbackAgent(RedashClient())
    creatives = agent.analyze_creative_performance(days_back=7)
    underperformers = agent.identify_underperforming_cohorts(days_back=7)
    alert = agent.generate_slack_alert(underperformers, creatives)
"""

import logging
from typing import Any

import numpy as np
import pandas as pd

from src.redash_db import RedashClient

log = logging.getLogger(__name__)

# ── Thresholds ─────────────────────────────────────────────────────────────────

# Underperformance thresholds (locked in CONTEXT.md)
CPA_Z_SCORE_PAUSE     = 3.0   # PAUSE recommendation if z-score exceeds this
CPA_Z_SCORE_ALERT     = 2.0   # flag as underperforming if z-score exceeds this
CTR_DECLINE_THRESHOLD = -0.10  # 10% week-over-week CTR decline


class FeedbackAgent:
    """
    Analyzes creative and cohort performance metrics from Redash.

    Provides:
    - analyze_creative_performance() — ranks creatives within each cohort,
      generates reason_hypothesis per creative
    - identify_underperforming_cohorts() — flags cohorts with CPA > baseline+2σ
      or CTR declining > 10% week-over-week
    - generate_slack_alert() — formats a weekly Slack message per Outlier vocabulary
    """

    def __init__(self, redash_client: RedashClient) -> None:
        self.redash_client = redash_client

    # ─────────────────────────────────────────────────────────────────────────
    # Public methods
    # ─────────────────────────────────────────────────────────────────────────

    def analyze_creative_performance(self, days_back: int = 7) -> list[dict]:
        """
        Analyze creative performance for the last `days_back` days.

        Returns a list of dicts with schema:
            {angle, photo_subject, cohort_name, ctr, cpa, impression_count,
             rank_in_cohort, reason_hypothesis}

        Creatives are sorted by cohort_name, then CPA descending (most
        expensive first for review).

        Returns [] if Redash returns no data.
        """
        df = self.redash_client.query_creative_performance(days_back=days_back)

        if df.empty:
            log.warning("analyze_creative_performance: empty query result, returning []")
            return []

        cohort_count = df["cohort_name"].nunique() if "cohort_name" in df.columns else 0
        log.info(
            "Analyzed %d creatives across %d cohorts",
            len(df),
            cohort_count,
        )

        results: list[dict] = []

        for cohort_name, cohort_df in df.groupby("cohort_name"):
            cohort_df = cohort_df.copy().reset_index(drop=True)
            cohort_df = _ensure_numeric(cohort_df, ["ctr", "cpa", "impressions"])

            # Compute cohort-level statistics for hypothesis generation
            median_cpa = cohort_df["cpa"].dropna().median() if not cohort_df["cpa"].dropna().empty else 0.0
            std_cpa    = cohort_df["cpa"].dropna().std()    if len(cohort_df["cpa"].dropna()) > 1 else 0.0
            median_ctr = cohort_df["ctr"].median()
            std_ctr    = cohort_df["ctr"].std() if len(cohort_df) > 1 else 0.0

            # Unique photo_subjects in this cohort (for differentiation detection)
            photo_subject_counts = cohort_df["photo_subject"].value_counts()

            # Rank by CTR ascending (worst first) within each cohort
            cohort_df = cohort_df.sort_values("ctr", ascending=True).reset_index(drop=True)
            cohort_df["rank_ctr_asc"] = range(1, len(cohort_df) + 1)

            # Select top 3 (best CTR) and bottom 3 (worst CTR) for review
            n = len(cohort_df)
            if n <= 6:
                selected_idx = list(range(n))
            else:
                selected_idx = list(range(3)) + list(range(n - 3, n))

            for idx in selected_idx:
                row = cohort_df.iloc[idx]
                cpa_val = float(row["cpa"]) if pd.notna(row.get("cpa")) else None
                ctr_val = float(row["ctr"]) if pd.notna(row.get("ctr")) else 0.0
                photo   = str(row.get("photo_subject", "unknown"))

                hypothesis = _generate_hypothesis(
                    cpa_val=cpa_val,
                    ctr_val=ctr_val,
                    median_cpa=float(median_cpa) if pd.notna(median_cpa) else 0.0,
                    std_cpa=float(std_cpa) if pd.notna(std_cpa) else 0.0,
                    median_ctr=float(median_ctr) if pd.notna(median_ctr) else 0.0,
                    std_ctr=float(std_ctr) if pd.notna(std_ctr) else 0.0,
                    photo_subject=photo,
                    photo_subject_counts=photo_subject_counts,
                )

                results.append({
                    "angle":            str(row.get("angle", "unknown")),
                    "photo_subject":    photo,
                    "cohort_name":      str(cohort_name),
                    "ctr":              ctr_val,
                    "cpa":              cpa_val,
                    "impression_count": int(row.get("impressions", 0)),
                    "rank_in_cohort":   int(row["rank_ctr_asc"]),
                    "reason_hypothesis": hypothesis,
                })

        # Sort by cohort_name, then CPA descending (None → treated as large)
        results.sort(
            key=lambda x: (x["cohort_name"], -(x["cpa"] or 1e9))
        )
        return results

    def identify_underperforming_cohorts(self, days_back: int = 7) -> list[dict]:
        """
        Identify cohorts underperforming on CPA (2σ rule) or CTR trend (10% decline).

        Returns a list of dicts with schema:
            {cohort_name, current_cpa, baseline_cpa, sigma, cpa_z_score,
             ctr_trend, recommendation}

        Sorted by cpa_z_score descending (worst first).
        Returns [] if Redash returns no data.
        """
        df = self.redash_client.query_cohort_metrics(days_back=days_back)

        if df.empty:
            log.warning("identify_underperforming_cohorts: empty query result, returning []")
            return []

        df = _ensure_numeric(df, ["ctr", "cpa", "n_impressions", "n_conversions"])

        # Get the most recent week and prior week for each cohort
        # week_of may be a string or datetime; sort lexicographically (ISO dates sort correctly)
        df = df.copy()
        df["week_of_str"] = df["week_of"].astype(str)
        df_sorted = df.sort_values(["cohort_name", "week_of_str"], ascending=[True, False])

        latest_rows: list[dict] = []
        for cohort_name, grp in df_sorted.groupby("cohort_name"):
            grp = grp.reset_index(drop=True)
            current_row = grp.iloc[0]
            prior_row   = grp.iloc[1] if len(grp) > 1 else None

            current_cpa = float(current_row["cpa"]) if pd.notna(current_row.get("cpa")) else None
            current_ctr = float(current_row["ctr"]) if pd.notna(current_row.get("ctr")) else 0.0
            prior_ctr   = float(prior_row["ctr"])   if prior_row is not None and pd.notna(prior_row.get("ctr")) else None

            # CTR trend (week-over-week delta as fraction)
            if prior_ctr is not None and prior_ctr > 0:
                ctr_trend = (current_ctr - prior_ctr) / prior_ctr
            else:
                ctr_trend = 0.0

            latest_rows.append({
                "cohort_name": str(cohort_name),
                "current_cpa": current_cpa,
                "current_ctr": current_ctr,
                "ctr_trend":   ctr_trend,
            })

        if not latest_rows:
            return []

        # Compute baseline (median) and sigma from current-week CPAs
        cpa_values = [r["current_cpa"] for r in latest_rows if r["current_cpa"] is not None]
        if len(cpa_values) >= 2:
            baseline_cpa = float(np.median(cpa_values))
            sigma        = float(np.std(cpa_values, ddof=1))
        elif len(cpa_values) == 1:
            baseline_cpa = cpa_values[0]
            sigma        = 0.0
        else:
            baseline_cpa = 0.0
            sigma        = 0.0

        underperformers: list[dict] = []

        for r in latest_rows:
            current_cpa = r["current_cpa"]
            ctr_trend   = r["ctr_trend"]

            # Compute z-score; guard against zero sigma
            if sigma > 0 and current_cpa is not None:
                cpa_z_score = (current_cpa - baseline_cpa) / sigma
            else:
                cpa_z_score = 0.0

            # Determine if underperforming
            is_high_cpa = cpa_z_score > CPA_Z_SCORE_ALERT
            is_ctr_down = ctr_trend < CTR_DECLINE_THRESHOLD

            if not (is_high_cpa or is_ctr_down):
                continue

            # Recommendation
            if cpa_z_score > CPA_Z_SCORE_PAUSE:
                recommendation = "PAUSE"
            elif is_ctr_down:
                recommendation = "TEST_NEW_ANGLES"
            else:
                recommendation = "MONITOR"

            underperformers.append({
                "cohort_name":   r["cohort_name"],
                "current_cpa":   current_cpa,
                "baseline_cpa":  baseline_cpa,
                "sigma":         sigma,
                "cpa_z_score":   round(cpa_z_score, 2),
                "ctr_trend":     round(ctr_trend, 4),
                "recommendation": recommendation,
            })

        underperformers.sort(key=lambda x: -(x["cpa_z_score"] or 0.0))

        if underperformers:
            log.warning(
                "Underperformers detected: %s",
                [c["cohort_name"] for c in underperformers],
            )

        return underperformers

    # ── V2: Full-funnel decomposition (FEED-15 / FEED-16) ────────────────────

    def analyze_funnel_by_cohort(self, days_back: int = 7) -> list[dict]:
        """
        FEED-15: Full-funnel decomposition per (cohort_name, creative_id).

        Returns a list of dicts — one per (cohort_name, creative_id) row — with keys:
          cohort_name, creative_id,
          impressions, clicks, applications, screening_passes, activations,
          ctr, click_to_signup, signup_to_screen, screen_to_activate.

        Empty list if no campaign data in the window.
        """
        df = self.redash_client.query_funnel_metrics(days_back=days_back)
        if df is None or df.empty:
            log.warning("No funnel rows returned for window=%d days", days_back)
            return []
        cohort_count = (
            df["cohort_name"].nunique() if "cohort_name" in df.columns else 0
        )
        log.info(
            "Analyzed funnel for %d creatives across %d cohorts",
            len(df), cohort_count,
        )
        return df.to_dict(orient="records")

    def identify_funnel_drop_stage(self, funnel_rows: list[dict]) -> dict:
        """
        FEED-16: For each cohort, identify the funnel stage where drop occurs.

        Input: funnel_rows from analyze_funnel_by_cohort().
        Output: dict mapping cohort_name -> {
            drop_stage: 'top_of_funnel' | 'signup' | 'screening' | 'activation' | 'none',
            drop_rate: float (the observed rate at the drop stage),
            baseline_rate: float (cohort-median rate at that stage),
            delta_pct: float (how far below baseline, signed fraction),
        }

        Classification logic: for each cohort, compute the cohort-median rate at
        each of four stages (ctr, click_to_signup, signup_to_screen,
        screen_to_activate). Pick the EARLIEST stage whose worst observed rate
        is <= baseline * (1 - FUNNEL_DROP_ALERT_THRESHOLD). If no stage drops
        far enough, drop_stage = 'none'.
        """
        import config as _config
        import pandas as _pd

        if not funnel_rows:
            return {}

        df = _pd.DataFrame(funnel_rows)
        threshold = 1.0 - _config.FUNNEL_DROP_ALERT_THRESHOLD  # 0.70 default

        stage_cols = [
            ("top_of_funnel", "ctr"),
            ("signup",        "click_to_signup"),
            ("screening",     "signup_to_screen"),
            ("activation",    "screen_to_activate"),
        ]

        result: dict[str, dict] = {}
        for cohort, grp in df.groupby("cohort_name"):
            diagnosis = {
                "drop_stage":    "none",
                "drop_rate":     None,
                "baseline_rate": None,
                "delta_pct":     None,
            }
            for stage_label, col in stage_cols:
                if col not in grp.columns:
                    continue
                series = _pd.to_numeric(grp[col], errors="coerce").dropna()
                if series.empty:
                    continue
                baseline = float(series.median())
                if baseline == 0:
                    continue
                worst = float(series.min())
                if worst <= baseline * threshold:
                    diagnosis = {
                        "drop_stage":    stage_label,
                        "drop_rate":     round(worst, 4),
                        "baseline_rate": round(baseline, 4),
                        "delta_pct":     round((worst - baseline) / baseline, 4),
                    }
                    break  # earliest stage wins
            result[str(cohort)] = diagnosis

        n_drops = sum(1 for d in result.values() if d.get("drop_stage") != "none")
        log.info(
            "Funnel drop diagnosis: %d cohorts analyzed, %d with drops",
            len(result), n_drops,
        )
        return result

    def generate_slack_alert(
        self,
        underperformers: list[dict],
        hypothesis_summary: list[dict] | dict,
        funnel_diagnosis: dict | None = None,
    ) -> str:
        """
        Format a weekly Slack alert message per Outlier vocabulary.

        Args:
            underperformers: output of identify_underperforming_cohorts()
            hypothesis_summary: output of analyze_creative_performance() (list),
                                or a dict (legacy; treated as single-item list)

        Returns formatted Slack message string.
        """
        if not underperformers:
            return "No underperforming cohorts this week."

        # Normalise hypothesis_summary to list
        if isinstance(hypothesis_summary, dict):
            hypothesis_summary = [hypothesis_summary] if hypothesis_summary else []

        lines: list[str] = [
            "Weekly Feedback: Creative Analysis Update",
            "",
            "Top Underperforming Cohorts:",
        ]

        for item in underperformers[:3]:
            cohort      = item.get("cohort_name", "unknown")
            current_cpa = item.get("current_cpa")
            baseline    = item.get("baseline_cpa", 0)
            z_score     = item.get("cpa_z_score", 0.0)
            ctr_trend   = item.get("ctr_trend", 0.0)
            rec         = item.get("recommendation", "MONITOR")

            cpa_str      = f"${current_cpa:.0f}" if current_cpa is not None else "N/A"
            baseline_str = f"${baseline:.0f}"

            lines.append(
                f"- {cohort} — CPA {cpa_str}"
                f" (baseline {baseline_str}, +{z_score:.1f}σ)"
            )
            lines.append(f"  Trend: CTR {ctr_trend:+.1%}")
            lines.append(f"  Recommendation: {rec}")
            lines.append("")

        # FEED-16 (V2): Funnel-drop diagnosis block (only if provided AND
        # at least one cohort has a non-"none" drop_stage). Section header is
        # "Funnel Drop Diagnosis:" — neutral wording per CLAUDE.md vocabulary.
        if funnel_diagnosis:
            drop_lines: list[str] = []
            for cohort, d in funnel_diagnosis.items():
                if d.get("drop_stage") in (None, "none"):
                    continue
                drop_rate     = d.get("drop_rate")     or 0.0
                baseline_rate = d.get("baseline_rate") or 0.0
                delta_pct     = d.get("delta_pct")     or 0.0
                drop_lines.append(
                    f"- {cohort}: funnel drop at {d['drop_stage']} stage "
                    f"(rate {drop_rate:.1%} vs cohort baseline "
                    f"{baseline_rate:.1%}, delta {delta_pct:+.0%})"
                )
            if drop_lines:
                lines.append("Funnel Drop Diagnosis:")
                lines.extend(drop_lines)
                lines.append("")

        lines.append("Creative Insights:")

        for item in hypothesis_summary[:5]:
            angle   = item.get("angle", "unknown")
            photo   = item.get("photo_subject", "unknown")
            cohort  = item.get("cohort_name", "unknown")
            hypo    = item.get("reason_hypothesis", "No hypothesis available")
            lines.append(f"- Angle {angle} with {photo} in {cohort}: {hypo}")

        lines += [
            "",
            "React with:",
            "\U0001f44d to pause a cohort",
            "\U0001f9ea to request A/B analysis of new angles",
        ]

        return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Internal helpers
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_numeric(df: pd.DataFrame, cols: list[str]) -> pd.DataFrame:
    """Coerce listed columns to float; ignore errors (leaves NaN for bad values)."""
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def _generate_hypothesis(
    cpa_val: float | None,
    ctr_val: float,
    median_cpa: float,
    std_cpa: float,
    median_ctr: float,
    std_ctr: float,
    photo_subject: str,
    photo_subject_counts: "pd.Series",
) -> str:
    """
    Simple pattern-matching hypothesis generator.

    Priority:
    1. High CPA (> median + 2σ) → cost signal
    2. High CTR (> median + 2σ) → engagement signal
    3. Unique photo_subject in cohort → differentiation signal
    4. Default → A/B test suggestion
    """
    if cpa_val is not None and std_cpa > 0 and cpa_val > (median_cpa + 2 * std_cpa):
        return (
            "High-cost creative; lower engagement rate in target cohort"
        )

    if std_ctr > 0 and ctr_val > (median_ctr + 2 * std_ctr):
        return (
            "Strong engagement hook; consider testing similar angle"
        )

    if photo_subject_counts.get(photo_subject, 0) == 1:
        return (
            "Unique visual treatment drives differentiation"
        )

    return "Audience resonance varies; test A/B variants"

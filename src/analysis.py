"""
Stage A — Global statistical modeling (univariate + beam search intersections).
Stage B — Country directional validation.

Tiered ICP targeting (new canonical columns from STAGE1_SQL / CESF):
  - T3:   t3_activated          (= cesf.ACTIVATED)
  - T2.5: t25_project_started   (= cesf.ACP_STARTED)
  - T2:   t2_course_pass        (= cesf.FIRST_OCP_ENDED_IN_SUCCESS)
  - T1:   t1_resume_pass        (= cesf.EVER_PASSED_SKILL_SCREENING)

Legacy columns (from the old RESUME_SQL path) are still accepted as fallbacks:
  - T3 ← tasked_on_project
  - T2 ← eligible_for_project
  - T1 ← resume_screening_result = PASS (via _is_pass)

pick_target_tier() auto-selects the strongest tier with ≥MIN_POSITIVES_FOR_STATS
positives. Below that, main.py routes to sparse mode (small_sample_signals +
exemplars + best-effort job-post ICP in the summary). At 0 positives it routes
to cold_start. Exemplars are always built regardless of mode — they're a summary
artifact for BPO partners (Joveo, etc.), not an analysis tier.
"""
import itertools
import logging
import math
from dataclasses import dataclass, field
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

import config

log = logging.getLogger(__name__)

# ── Tiered ICP targeting ──────────────────────────────────────────────────────

MIN_POSITIVES_FOR_STATS = 30  # Below this, skip Stage A/B and use small_sample_signals
MIN_POSITIVES_FOR_SIGNALS = 10  # Below this, skip signal extraction too — just exemplars


# ── Shortlist controls (prevent LinkedIn-broad univariates from topping the list) ─

# Features on this list are too broad on LinkedIn to work as a solo cohort — they
# pull in millions of unrelated users. They're still allowed inside 2-way / 3-way
# combos (e.g. Strategy + Data Analyst), just never as a standalone shortlist item.
# Stored as the lowercased *tail* of the feature column (after `facet__` prefix).
BROAD_SOLO_FEATURES: set[str] = {
    # Generic professional skills
    "strategy", "strategic_planning",
    "leadership", "team_leadership",
    "management", "project_management", "change_management", "program_management",
    "communication", "collaboration", "teamwork", "team_management",
    "mentoring", "teaching", "training", "coaching",
    "problem_solving", "critical_thinking", "analytical_skills",
    "research", "writing", "editing",
    "microsoft_office", "excel", "powerpoint", "word",
    "administration", "operations",
    "consulting", "customer_service", "sales",
    "presentation", "public_speaking",
    "time_management", "organization",
    # Misc artifacts we've seen at the top of rankings historically
    "[]", "{}", "none", "null",
}

# Relative score boost applied to 2-way / 3-way combos in the final sort. Set to
# 1.5 so a combo at +12pp / n=8 outranks a singleton at +12pp / n=31 — which
# matches LinkedIn-targeting reality (more rules = narrower audience).
COMBO_SCORE_BIAS = 1.5

# Extra boost for cohorts that contain a base-role feature (e.g. job_titles_norm__Data_Analyst
# when the project is a Data Analyst evals screening). Set to 2.0 so a combo
# anchored on the right title easily outranks a combo without it.
BASE_ROLE_SCORE_BIAS = 2.0


def _feature_tail(col: str) -> str:
    """Return the part of a binary feature column after `facet__` — the value side.
    Example: `skills__Strategy` → `strategy`.
    """
    if "__" not in col:
        return col.lower()
    return col.split("__", 1)[1].strip().lower()


def _is_broad_solo(col: str) -> bool:
    """Is this feature too broad on LinkedIn to ship as a solo cohort?"""
    return _feature_tail(col) in BROAD_SOLO_FEATURES


def pick_target_tier(df: pd.DataFrame) -> tuple[str, str, int]:
    """
    Walk T3 → T2.5 → T2 → T1 and return the FIRST tier that clears
    MIN_POSITIVES_FOR_STATS (30). Genuine tier fallback — if T3 has only 5
    activators but T1 has 250 resume-passers, we return T1 so downstream can
    actually run a meaningful analysis.

    Returns (tier_label, target_col, positive_count).

    Tie-breaking when NO tier clears the threshold: return the tier with the
    MOST positives, not the first-checked. Previously returned T3-always which
    was a bug (T3 is weakest in a flow-scoped frame — we'd miss the much larger
    T1 pool).

    Column resolution: prefers the new STAGE1_SQL columns (t3_activated,
    t25_project_started, t2_course_pass, t1_resume_pass) and falls back to the
    legacy RESUME_SQL columns (tasked_on_project, eligible_for_project,
    resume_screening_result) when the new ones aren't present. Returns the
    first available name per tier.

    Note on frames:
      - CBPR-scoped (fetch_stage1_contributors): every row did paid work, so
        all four tiers are correlated and near-True. T3 is always the strongest,
        fallback to T1 essentially never happens — which is correct.
      - Flow-scoped (fetch_screenings): rows include non-passers at every
        stage. T3 rates are low; T1 rates are high. The fallback here is what
        routes sparse activator pools onto the larger resume-pass class.
    """
    # Ordered tier → preferred column list. First existing column wins for each tier.
    tier_candidates: list[tuple[str, list[str]]] = [
        ("T3",   ["t3_activated", "tasked_on_project"]),
        ("T2.5", ["t25_project_started"]),
        ("T2",   ["t2_course_pass", "eligible_for_project"]),
        ("T1",   ["t1_resume_pass", "_is_pass"]),
    ]

    # Make sure _is_pass exists for the legacy T1 fallback path.
    if "_is_pass" not in df.columns and "resume_screening_result" in df.columns:
        df["_is_pass"] = df["resume_screening_result"].astype(str).str.upper() == "PASS"

    # First pass: walk tiers and return the first one that clears the threshold.
    # Track every tier's positive count so we can pick the max as a safety net.
    tier_counts: list[tuple[str, str, int]] = []
    for tier, cols in tier_candidates:
        col = next((c for c in cols if c in df.columns), None)
        if col is None:
            continue
        n_pos = int(df[col].fillna(False).astype(bool).sum())
        log.info("Tier %s (%s): %d positives", tier, col, n_pos)
        tier_counts.append((tier, col, n_pos))
        if n_pos >= MIN_POSITIVES_FOR_STATS:
            if tier != "T3":
                log.info(
                    "Tier fallback: stronger tiers were thin — dropped down to %s (%d ≥ %d). "
                    "Cohort analysis will target this tier's positive class.",
                    tier, n_pos, MIN_POSITIVES_FOR_STATS,
                )
            else:
                log.info("Picked target tier %s (%d ≥ %d threshold)", tier, n_pos, MIN_POSITIVES_FOR_STATS)
            return tier, col, n_pos

    # No tier cleared the bar. Return the tier with the MOST positives so
    # downstream branches (sparse / cold_start) act
    # on the richest pool available, not an arbitrary default.
    if not tier_counts:
        log.warning("pick_target_tier: no tier columns found in df — defaulting to T1/_is_pass")
        return "T1", "_is_pass", 0
    tier_counts.sort(key=lambda t: -t[2])
    best_tier, best_col, best_n = tier_counts[0]
    log.warning(
        "No tier cleared the %d threshold. Best available: %s with %d positive(s). "
        "Downstream will branch to sparse / cold_start.",
        MIN_POSITIVES_FOR_STATS, best_tier, best_n,
    )
    return best_tier, best_col, best_n


def stage_a_negative(
    df: pd.DataFrame,
    binary_cols: list[str],
    target_col: str,
    min_non_activators: int = 100,
    min_non_icp_share: float = 0.20,
    max_ratio_vs_non_icp: float = 0.40,
    top_k: int = 5,
) -> list[dict]:
    """
    Mirror of Stage A univariates on the NON-activator class — find features
    over-represented in the people who didn't activate. Output is a shortlist
    of features we should EXCLUDE from LinkedIn targeting (layered on top of
    `config.DEFAULT_EXCLUDE_FACETS` and `_BASE_ROLE_EXCLUSIONS`).

    Guardrails (small non-activator pools are noisy, so we're conservative):
      - `min_non_activators`   — bail out entirely if the negative class is
                                 smaller than this. For pilot 1's Generalist
                                 Checkpoint cohort (262 non-activators of 2,134)
                                 we're comfortably above. For an 80% activation
                                 cohort on a 50-person screen, we're not.
      - `min_non_icp_share`    — feature must appear in ≥20% of non-activators.
                                 Rare signals are too noisy to exclude on.
      - `max_ratio_vs_non_icp` — activators with the feature / non-activators
                                 with the feature must be ≤40%. I.e. this
                                 feature is at least 2.5× more common in the
                                 people who DIDN'T activate.
      - `top_k`                — cap output at the 5 strongest negative
                                 signals so we don't over-exclude the audience.

    Returns a list of dicts, strongest negative signal first:
        [{"feature", "non_icp_share", "icp_share", "ratio", "icp_count",
          "non_icp_count"}, ...]

    Callers should convert the feature column names to LinkedIn (facet, value)
    exclusion pairs before passing to UrnResolver — see main.py for the glue.
    """
    if target_col not in df.columns:
        log.warning("stage_a_negative: target col %r missing", target_col)
        return []
    pos = df[target_col].fillna(False).astype(bool)
    n_pos = int(pos.sum())
    n_neg = int((~pos).sum())
    if n_neg < min_non_activators:
        log.info(
            "stage_a_negative: only %d non-activators (< %d threshold) — skipping "
            "data-driven exclusions. The negative class is too thin to trust.",
            n_neg, min_non_activators,
        )
        return []

    hits: list[dict] = []
    for feat in binary_cols:
        if feat not in df.columns:
            continue
        flags = df[feat].fillna(False).astype(bool)
        non_icp_count = int((flags & ~pos).sum())
        icp_count     = int((flags & pos).sum())
        non_icp_share = non_icp_count / n_neg
        icp_share     = icp_count / n_pos if n_pos else 0.0
        if non_icp_share < min_non_icp_share:
            continue
        # Ratio = activator_share / non_activator_share. Low ratio = feature is
        # much more common among non-activators than activators.
        ratio = icp_share / non_icp_share if non_icp_share > 0 else 1.0
        if ratio > max_ratio_vs_non_icp:
            continue
        hits.append({
            "feature":       feat,
            "non_icp_share": round(non_icp_share, 3),
            "icp_share":     round(icp_share, 3),
            "ratio":         round(ratio, 3),
            "icp_count":     icp_count,
            "non_icp_count": non_icp_count,
        })

    # Strongest negative signal first: highest non_icp_share AND lowest ratio.
    hits.sort(key=lambda h: (-h["non_icp_share"], h["ratio"]))
    log.info(
        "stage_a_negative: %d negative signal(s) pass filters (n_neg=%d, n_pos=%d). Top %d:",
        len(hits), n_neg, n_pos, min(top_k, len(hits)),
    )
    for h in hits[:top_k]:
        log.info(
            "  EXCLUDE %s  non_icp_share=%.1f%%  icp_share=%.1f%%  ratio=%.2f",
            h["feature"], h["non_icp_share"] * 100, h["icp_share"] * 100, h["ratio"],
        )
    return hits[:top_k]


def small_sample_signals(
    df: pd.DataFrame,
    binary_cols: list[str],
    target_col: str,
    icp_min_share: float = 0.50,
    non_icp_max_share: float = 0.10,
) -> list[dict]:
    """
    For cohorts with 10–29 positives where Stage A's p-value filter is unreliable,
    surface features that are strongly over-represented among ICPs.

    A feature passes if:
      - it appears in ≥icp_min_share (default 50%) of ICP rows AND
      - it appears in ≤non_icp_max_share (default 10%) of non-ICP rows.

    Returns a list of dicts sorted by ICP share desc:
        {"feature", "icp_share", "non_icp_share", "icp_count", "non_icp_count"}
    """
    if target_col not in df.columns:
        log.warning("small_sample_signals: target col %r missing", target_col)
        return []
    pos = df[target_col].fillna(False).astype(bool)
    n_pos = int(pos.sum())
    n_neg = int((~pos).sum())
    if n_pos == 0 or n_neg == 0:
        return []

    hits: list[dict] = []
    for feat in binary_cols:
        if feat not in df.columns:
            continue
        flags = df[feat].fillna(False).astype(bool)
        icp_hits = int((flags & pos).sum())
        non_icp_hits = int((flags & ~pos).sum())
        icp_share = icp_hits / n_pos
        non_icp_share = non_icp_hits / n_neg
        if icp_share >= icp_min_share and non_icp_share <= non_icp_max_share:
            hits.append({
                "feature": feat,
                "icp_share": round(icp_share, 3),
                "non_icp_share": round(non_icp_share, 3),
                "icp_count": icp_hits,
                "non_icp_count": non_icp_hits,
            })

    hits.sort(key=lambda h: (-h["icp_share"], h["non_icp_share"]))
    log.info("small_sample_signals: %d features pass (n_pos=%d, n_neg=%d)", len(hits), n_pos, n_neg)
    return hits


# ── Data structures ───────────────────────────────────────────────────────────

@dataclass
class SignalResult:
    feature:   str          # e.g. "skill__python"
    value:     Any = True   # the value that defines the segment
    n:         int = 0
    passes:    int = 0
    pass_rate: float = 0.0
    lift_pp:   float = 0.0
    p_value:   float = 1.0
    accepted:  bool = False
    reject_reason: str = ""


@dataclass
class Cohort:
    name:         str
    rules:        list[tuple]       # [(feature, value), ...]
    n:            int = 0
    passes:       int = 0
    pass_rate:    float = 0.0
    lift_pp:      float = 0.0
    p_value:      float = 1.0
    score:        float = 0.0       # support (× bias multipliers) in production stage_a_support
    # Support-mining fields (set by stage_a_support). Populated in the
    # lift-based legacy path too as display proxies.
    support:      int   = 0         # # of ICPs this rule-set covers
    coverage:     float = 0.0       # support / |ICP|, in [0, 1]
    facet_strength: dict = field(default_factory=dict)
    country_results: dict = field(default_factory=dict)
    # Per-cohort negation overrides. Added on top of the (defaults + family +
    # data-driven) exclusion set at campaign-creation time.
    #   exclude_add    — extra (facet, value) pairs to exclude for this cohort only
    #   exclude_remove — pairs to drop from the merged set (e.g. a medical cohort
    #                    that legitimately includes sales-adjacent work)
    exclude_add:     list[tuple[str, str]] = field(default_factory=list)
    exclude_remove:  list[tuple[str, str]] = field(default_factory=list)
    # filled by Stage C
    audience_size:   int = 0
    intersection_score: float = 0.0
    unique_pct:      float = 100.0


# ── Statistics helpers ────────────────────────────────────────────────────────

def two_prop_z_test(pass_a: int, n_a: int, pass_b: int, n_b: int) -> float:
    """Two-proportion z-test. Returns p-value (two-tailed)."""
    if n_a == 0 or n_b == 0:
        return 1.0
    p_a = pass_a / n_a
    p_b = pass_b / n_b
    p_pool = (pass_a + pass_b) / (n_a + n_b)
    denom  = math.sqrt(p_pool * (1 - p_pool) * (1 / n_a + 1 / n_b))
    if denom == 0:
        return 1.0
    z = (p_a - p_b) / denom
    return float(2 * (1 - stats.norm.cdf(abs(z))))


def segment_stats(df: pd.DataFrame, mask: pd.Series, baseline_rate: float) -> dict:
    # Use pre-computed boolean column if available (set once in stage_a); fallback for safety
    if "_is_pass" in df.columns:
        is_pass = df["_is_pass"]
    else:
        is_pass = df["resume_screening_result"].str.upper() == "PASS"
    n      = int(mask.sum())
    passes = int(is_pass[mask].sum())
    rate   = passes / n if n > 0 else 0.0
    lift   = (rate - baseline_rate) * 100
    n_rest = len(df) - n
    p_rest = int(is_pass[~mask].sum())
    pval   = two_prop_z_test(passes, n, p_rest, n_rest)
    return {"n": n, "passes": passes, "pass_rate": rate * 100, "lift_pp": lift, "p_value": pval}


def passes_thresholds(stats_dict: dict) -> tuple[bool, str]:
    if stats_dict["n"] < config.MIN_SAMPLE_INTERNAL:
        return False, f"n={stats_dict['n']} < {config.MIN_SAMPLE_INTERNAL}"
    if stats_dict["passes"] < config.MIN_ABSOLUTE_PASSES:
        return False, f"passes={stats_dict['passes']} < {config.MIN_ABSOLUTE_PASSES}"
    if stats_dict["lift_pp"] < config.MIN_LIFT_PP:
        return False, f"lift={stats_dict['lift_pp']:.1f}pp < {config.MIN_LIFT_PP}"
    if stats_dict["pass_rate"] < config.MIN_PASS_RATE_FLOOR:
        return False, f"pass_rate={stats_dict['pass_rate']:.1f}% < {config.MIN_PASS_RATE_FLOOR}"
    if stats_dict["p_value"] >= 0.05:
        return False, f"p={stats_dict['p_value']:.3f} >= 0.05"
    return True, ""


# ── Stage A ───────────────────────────────────────────────────────────────────

STAGE_A_SUPPORT_BASELINE_THRESHOLD = 0.80  # dispatcher cutoff: baseline ≥ this → support mode

# T1 (resume-pass) columns always use the lift-based Stage A, regardless of
# the frame's baseline. Semantically T1 is a "who clears the screen" question —
# its whole point is to differentiate passers from failers — so lift-over-
# baseline is the conceptually correct framing. If these columns ever land in
# a near-ceiling CBPR frame, lift mode will return no cohorts (all baseline,
# no negatives to learn from); that's the correct fail-closed behaviour — a
# signal that T1 in that frame isn't meaningful to mine.
_T1_TARGET_COLS = {"t1_resume_pass", "_is_pass", "resume_screening_result"}


def stage_a(
    df: pd.DataFrame,
    binary_cols: list[str],
    target_col: str | None = None,
    base_role_cols: list[str] | None = None,
) -> list[Cohort]:
    """
    Top-level dispatcher. Picks between lift-based and support-based mining:

      1. If target_col is a T1 (resume-pass) column → always `stage_a_lift`.
         T1 is conceptually about "who passes the screen", a lift problem.
      2. Else, dispatch by baseline:
         - Baseline ≥ 80% → `stage_a_support` (post-funnel frame, lift is
           uninformative, mine frequent itemsets on the ICP class only).
         - Baseline < 80% → `stage_a_lift` (flow-scoped frame with real
           negatives; lift genuinely predicts positive-class membership).

    Callers who want to force a specific mode can call `stage_a_support` or
    `stage_a_lift` directly.
    """
    # Rule 1: T1 targets always use lift mode.
    if target_col in _T1_TARGET_COLS:
        log.info(
            "stage_a dispatch → lift mode (T1 / resume-pass target: %r — lift is the correct framing for pass-vs-fail)",
            target_col,
        )
        return stage_a_lift(df, binary_cols, target_col=target_col, base_role_cols=base_role_cols)

    # Rule 2: baseline-based dispatch for T3/T2.5/T2.
    if target_col and target_col in df.columns:
        baseline = float(df[target_col].fillna(False).astype(bool).mean())
    elif "_is_pass" in df.columns:
        baseline = float(df["_is_pass"].fillna(False).astype(bool).mean())
    else:
        baseline = 1.0  # unknown — default to support mining

    if baseline >= STAGE_A_SUPPORT_BASELINE_THRESHOLD:
        log.info(
            "stage_a dispatch → support mode (baseline=%.1f%% ≥ %.0f%%, target=%r)",
            baseline * 100, STAGE_A_SUPPORT_BASELINE_THRESHOLD * 100, target_col,
        )
        return stage_a_support(df, binary_cols, target_col or "_is_pass", base_role_cols)

    log.info(
        "stage_a dispatch → lift mode (baseline=%.1f%% < %.0f%%, target=%r)",
        baseline * 100, STAGE_A_SUPPORT_BASELINE_THRESHOLD * 100, target_col,
    )
    return stage_a_lift(df, binary_cols, target_col=target_col, base_role_cols=base_role_cols)


def stage_a_lift(
    df: pd.DataFrame,
    binary_cols: list[str],
    target_col: str | None = None,
    base_role_cols: list[str] | None = None,
) -> list[Cohort]:
    """
    Lift-over-baseline beam search — for frames with a meaningful negative
    class. Primary use: flow-scoped `fetch_screenings` frames targeting T1
    (resume-pass). Dispatched automatically by `stage_a` when baseline < 80%.

    1. Compute global baseline.
    2. Univariate tests on every binary column.
    3. Beam search for 2-way and 3-way intersections.
    Returns up to BEAM_CANDIDATES accepted cohorts sorted by adjusted score.

    Args:
        df: screening DataFrame
        binary_cols: list of binary feature columns
        target_col: which column to use as the positive indicator. One of:
            - "tasked_on_project" (T3 — activation)
            - "eligible_for_project" (T2 — course-pass)
            - "_is_pass" or None (T1 — legacy resume-pass, the default)
        base_role_cols: list of feature columns corresponding to the project's
            "base role" (e.g. ["job_titles_norm__Data_Analyst",
            "job_titles_norm__Data_Scientist"] when the project is a data-analyst
            evals screen). Cohorts that include any of these are given a score
            boost so LinkedIn targeting ends up anchored on the right role.
    """
    df = df.copy()

    # Normalise the positive indicator into _is_pass (kept for segment_stats backward compat).
    if target_col in (None, "_is_pass"):
        if "_is_pass" not in df.columns:
            df["_is_pass"] = df["resume_screening_result"].astype(str).str.upper() == "PASS"
        log.info("Stage A target: T1 (resume-pass)")
    else:
        if target_col not in df.columns:
            raise ValueError(
                f"stage_a: target_col {target_col!r} not in DataFrame. "
                f"Available: {list(df.columns)[:20]}..."
            )
        df["_is_pass"] = df[target_col].fillna(False).astype(bool)
        tier_map = {
            "t3_activated":          "T3 (activation)",
            "t25_project_started":   "T2.5 (project started)",
            "t2_course_pass":        "T2 (course-pass)",
            "t1_resume_pass":        "T1 (resume-pass)",
            "tasked_on_project":     "T3 (activation) — legacy col",
            "eligible_for_project":  "T2 (course-pass) — legacy col",
        }
        tier = tier_map.get(target_col, target_col)
        log.info("Stage A target: %s via column %r", tier, target_col)

    total    = len(df)
    n_pass   = int(df["_is_pass"].sum())
    baseline = n_pass / total if total > 0 else 0.0
    log.info("Global baseline: %.1f%% (%d/%d)", baseline * 100, n_pass, total)

    # ── Univariates ──────────────────────────────────────────────────────────
    univariates: list[SignalResult] = []
    for col in binary_cols:
        mask  = df[col] == 1
        if mask.sum() == 0:
            continue
        s = segment_stats(df, mask, baseline)
        ok, reason = passes_thresholds(s)
        univariates.append(SignalResult(
            feature=col, value=1,
            n=s["n"], passes=s["passes"],
            pass_rate=s["pass_rate"], lift_pp=s["lift_pp"], p_value=s["p_value"],
            accepted=ok, reject_reason=reason,
        ))

    accepted_uni = [u for u in univariates if u.accepted]
    log.info("Univariates: %d accepted / %d tested", len(accepted_uni), len(univariates))

    # ── Beam search ──────────────────────────────────────────────────────────
    # Score = lift_pp * log(n) — used for beam candidate ordering
    def raw_score(lift: float, n: int) -> float:
        return lift * math.log10(max(n, 2))

    # Sort accepted univariates by score descending
    accepted_uni.sort(key=lambda u: raw_score(u.lift_pp, u.n), reverse=True)

    # Candidate pool: start with accepted univariates as 1-rule cohorts
    candidates: list[Cohort] = []
    for u in accepted_uni:
        c = Cohort(
            name=u.feature,
            rules=[(u.feature, 1)],
            n=u.n, passes=u.passes,
            pass_rate=u.pass_rate, lift_pp=u.lift_pp, p_value=u.p_value,
        )
        c.score = raw_score(u.lift_pp, u.n)
        candidates.append(c)

    # 2-way combos
    top_features = [u.feature for u in accepted_uni[:20]]  # limit beam width
    # Make sure base-role features are in the combo pool even if they didn't top
    # the univariate ranking — they're mandatory anchors, not optional signals.
    for br in (base_role_cols or []):
        if br in df.columns and br not in top_features:
            top_features.append(br)

    # Helper: the combo must either raise lift OR narrow the audience meaningfully.
    # This replaces the old hard "+1pp over best parent" rule, which was impossible
    # to clear when both parents sat at ~100% activation (e.g. Strategy + Data_Analyst
    # inside a cohort already at 87.7% baseline — mathematically capped at +12.28pp).
    def _is_narrower_combo(n_combo: int, n_parent_min: int) -> bool:
        # ≥20% narrower than the smaller parent → genuinely more specific
        return n_combo > 0 and n_combo <= 0.8 * max(n_parent_min, 1)

    for f1, f2 in itertools.combinations(top_features, 2):
        if _facet_count([(f1, 1), (f2, 1)]) > config.MAX_INCLUDE_FACETS:
            continue
        mask = (df[f1] == 1) & (df[f2] == 1)
        if mask.sum() == 0:
            continue
        s = segment_stats(df, mask, baseline)
        ok, _ = passes_thresholds(s)
        if not ok:
            continue
        # Incremental-lift gate: keep the combo if it raises lift OR tightens the
        # audience. Previously required strict lift+1pp; that silently dropped all
        # intersections of near-ceiling singletons (the "Strategy + Data Analyst"
        # cohort was never generated, which is why solo Strategy ended up in the
        # shortlist alone — wrong for LinkedIn targeting).
        u1 = next((u for u in accepted_uni if u.feature == f1), None)
        u2 = next((u for u in accepted_uni if u.feature == f2), None)
        parent_lift = max(u1.lift_pp if u1 else 0, u2.lift_pp if u2 else 0)
        parent_n = min(u1.n if u1 else s["n"], u2.n if u2 else s["n"])
        if s["lift_pp"] < parent_lift and not _is_narrower_combo(s["n"], parent_n):
            continue
        c = Cohort(
            name=f"{f1} + {f2}",
            rules=[(f1, 1), (f2, 1)],
            n=s["n"], passes=s["passes"],
            pass_rate=s["pass_rate"], lift_pp=s["lift_pp"], p_value=s["p_value"],
        )
        c.score = raw_score(s["lift_pp"], s["n"])
        candidates.append(c)

    # 3-way combos (limited)
    top_2way = sorted(candidates, key=lambda c: c.score, reverse=True)[:10]
    for c2 in top_2way:
        if len(c2.rules) != 2:
            continue
        f1, f2 = c2.rules[0][0], c2.rules[1][0]
        for f3 in top_features:
            if f3 in (f1, f2):
                continue
            rules3 = [(f1, 1), (f2, 1), (f3, 1)]
            if _facet_count(rules3) > config.MAX_INCLUDE_FACETS:
                continue
            mask = (df[f1] == 1) & (df[f2] == 1) & (df[f3] == 1)
            if mask.sum() == 0:
                continue
            s = segment_stats(df, mask, baseline)
            ok, _ = passes_thresholds(s)
            if not ok:
                continue
            # Same relaxed gate as 2-way: raise lift OR narrow audience ≥20% vs parent.
            if s["lift_pp"] < c2.lift_pp and not _is_narrower_combo(s["n"], c2.n):
                continue
            c = Cohort(
                name=f"{f1} + {f2} + {f3}",
                rules=rules3,
                n=s["n"], passes=s["passes"],
                pass_rate=s["pass_rate"], lift_pp=s["lift_pp"], p_value=s["p_value"],
            )
            c.score = raw_score(s["lift_pp"], s["n"])
            candidates.append(c)

    # ── Synthesize base_role × signal combos (relaxed gates) ────────────────
    # When the caller passes a base role (e.g. Data_Analyst), we want every
    # shortlisted cohort to be anchored on it — LinkedIn targeting by a solo
    # skill like "Python" would pull in unrelated engineers, but "Data_Analyst
    # + Python" narrows to the right audience. The default Stage A thresholds
    # reject these intersections because the combo sample size (≤ 30 typically)
    # is smaller than MIN_SAMPLE_INTERNAL. Here we generate them with a looser
    # gate (passes ≥ 5 and lift_pp ≥ MIN_LIFT_PP; no n floor) since the
    # statistical confidence inside our labeled frame matters less than "does
    # this combo match the project's known base role".
    def _segment_stats_loose_ok(stats_dict: dict) -> bool:
        if stats_dict["passes"] < 5:
            return False
        if stats_dict["lift_pp"] < config.MIN_LIFT_PP:
            return False
        if stats_dict["pass_rate"] < config.MIN_PASS_RATE_FLOOR:
            return False
        return True

    if base_role_cols:
        # Seed pool: every accepted univariate (inc. broad-solos, they're fine
        # inside combos) + the base-role columns themselves as solo cohorts.
        seed_features = [u.feature for u in accepted_uni]
        synthesized = 0
        for br in base_role_cols:
            if br not in df.columns:
                continue
            # Solo base-role cohort — always include (so "Data_Analyst" alone
            # survives even if no Data_Analyst+X combo is tight).
            mask_solo = df[br] == 1
            if mask_solo.sum() > 0:
                s_solo = segment_stats(df, mask_solo, baseline)
                if _segment_stats_loose_ok(s_solo):
                    existing = next((c for c in candidates if c.rules == [(br, 1)]), None)
                    if existing is None:
                        c = Cohort(
                            name=br, rules=[(br, 1)],
                            n=s_solo["n"], passes=s_solo["passes"],
                            pass_rate=s_solo["pass_rate"], lift_pp=s_solo["lift_pp"],
                            p_value=s_solo["p_value"],
                        )
                        c.score = raw_score(s_solo["lift_pp"], s_solo["n"])
                        candidates.append(c)
                        synthesized += 1
            # 2-way combos: base_role × every other accepted signal.
            for sig in seed_features:
                if sig == br or sig in base_role_cols:
                    continue
                mask = (df[br] == 1) & (df[sig] == 1)
                if mask.sum() == 0:
                    continue
                s = segment_stats(df, mask, baseline)
                if not _segment_stats_loose_ok(s):
                    continue
                rules = [(br, 1), (sig, 1)]
                if _facet_count(rules) > config.MAX_INCLUDE_FACETS:
                    continue
                # Skip duplicates of combos already in candidates.
                already = any(set(c.rules) == set(rules) for c in candidates)
                if already:
                    continue
                c = Cohort(
                    name=f"{br} + {sig}", rules=rules,
                    n=s["n"], passes=s["passes"],
                    pass_rate=s["pass_rate"], lift_pp=s["lift_pp"], p_value=s["p_value"],
                )
                c.score = raw_score(s["lift_pp"], s["n"])
                candidates.append(c)
                synthesized += 1
        log.info("Synthesized %d base-role anchored candidate(s).", synthesized)

    # ── Filter out broad solo univariates BEFORE diversity / biasing ─────────
    # These features (Strategy, Leadership, Communication, …) look great inside
    # our 2,134-person labeled frame but explode on LinkedIn — a solo cohort on
    # "Strategy" would target tens of millions of unrelated MBAs/PMs. They can
    # still appear inside a combo (Strategy + Data_Analyst is fine), just not
    # alone. We drop them here so they don't steal a slot in the final shortlist.
    pre_filter = len(candidates)
    candidates = [
        c for c in candidates
        if not (len(c.rules) == 1 and _is_broad_solo(c.rules[0][0]))
    ]
    n_removed = pre_filter - len(candidates)
    if n_removed:
        log.info(
            "Filtered %d broad solo univariate(s) from shortlist (blocklist hits).",
            n_removed,
        )

    # ── Apply combo + base-role score biases ─────────────────────────────────
    base_role_set = set(base_role_cols or [])
    for c in candidates:
        if len(c.rules) >= 2:
            c.score *= COMBO_SCORE_BIAS
        if base_role_set and any(r[0] in base_role_set for r in c.rules):
            c.score *= BASE_ROLE_SCORE_BIAS

    # ── When a base role is confirmed, require every shortlisted cohort to contain one ──
    # This is the LinkedIn-reality rule: "Python" on its own targets millions of
    # unrelated engineers, so for a Data-Analyst project we refuse to ship any
    # cohort that isn't anchored on the known role.
    if base_role_set:
        pre = len(candidates)
        candidates = [
            c for c in candidates
            if any(r[0] in base_role_set for r in c.rules)
        ]
        log.info(
            "Enforced base-role anchor: %d candidate(s) kept, %d dropped (no base-role feature).",
            len(candidates), pre - len(candidates),
        )

    # ── Diversity multiplier ─────────────────────────────────────────────────
    def primary_facet(c: Cohort) -> str:
        return _feature_to_facet(c.rules[0][0])

    candidates.sort(key=lambda c: c.score, reverse=True)
    used_primary_facets: dict[str, set] = {}
    for c in candidates:
        pf   = primary_facet(c)
        vals = {r[0] for r in c.rules}
        if pf not in used_primary_facets:
            mult = 1.0
        elif not vals.intersection(used_primary_facets[pf]):
            mult = 0.7
        else:
            mult = 0.4
        c.score *= mult
        used_primary_facets.setdefault(pf, set()).update(vals)

    # ── Facet strength ───────────────────────────────────────────────────────
    for c in candidates:
        c.facet_strength = _compute_facet_strength(df, c, baseline)

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[: config.BEAM_CANDIDATES]


def stage_a_support(
    df: pd.DataFrame,
    binary_cols: list[str],
    target_col: str,
    base_role_cols: list[str] | None = None,
    min_support: int = 8,
    max_coverage: float = 0.50,
    min_facets: int = 2,
    top_features_for_combos: int = 25,
) -> list[Cohort]:
    """
    Production Stage A — frequency / support-based cohort mining on the ICP
    class only. Replaces the lift-over-baseline framing of `stage_a_lift`.

    Rationale: when the frame is already post-funnel (every row is a screened
    CB who made it to the activator flow) the baseline activation rate sits
    near ceiling and the non-activator class is tiny. "Lift over baseline"
    then measures noise rather than signal. What we actually care about for
    LinkedIn targeting is: *among our good-fit CBs, which profile facets do
    most of them share?* That's frequent-itemset mining on the ICP subset.

    Algorithm:
      1. Restrict df to rows where target_col == True (the ICP class).
      2. Score every binary_col by support (count of ICPs with feature=1).
      3. Enumerate 2-way and 3-way combos from the top features + base-role
         columns, score by joint support.
      4. Drop broad solo cohorts (BROAD_SOLO_FEATURES) and cohorts covering
         >max_coverage of ICPs — both are too broad to target on LinkedIn.
      5. When base_role_cols is non-empty, require every shortlisted cohort
         to include a base-role feature.
      6. Require ≥min_facets rules per cohort (default 2) — solos are too
         broad on LinkedIn even when they cover lots of our ICPs.
      7. Apply combo bias + base-role bias + diversity multiplier on score.
      8. Return top `config.BEAM_CANDIDATES` cohorts.
    """
    if target_col not in df.columns:
        raise ValueError(f"stage_a_support: target_col {target_col!r} not in df")

    icp_mask = df[target_col].fillna(False).astype(bool)
    icp_df = df.loc[icp_mask]
    n_icp = len(icp_df)
    if n_icp == 0:
        log.warning("stage_a_support: no ICPs in frame — returning empty")
        return []

    log.info(
        "stage_a_support: mining on %d ICPs (from %d total). "
        "min_support=%d  max_coverage=%.0f%%  min_facets=%d",
        n_icp, len(df), min_support, max_coverage * 100, min_facets,
    )

    # ── Step 2: univariate support ─────────────────────────────────────────
    uni_stats: list[tuple[str, int, float]] = []
    for col in binary_cols:
        if col not in icp_df.columns:
            continue
        support = int(icp_df[col].fillna(False).astype(bool).sum())
        if support < min_support:
            continue
        coverage = support / n_icp
        if coverage > max_coverage:
            continue
        uni_stats.append((col, support, coverage))
    uni_stats.sort(key=lambda t: -t[1])
    log.info("stage_a_support: %d univariate features pass support gate", len(uni_stats))

    # Seed pool for combos: top-N univariates + any base-role anchors.
    top_features = [t[0] for t in uni_stats[:top_features_for_combos]]
    for br in (base_role_cols or []):
        if br in icp_df.columns and br not in top_features:
            top_features.append(br)

    candidates: list[Cohort] = []

    def _add_cohort(rules: list[tuple], support: int) -> None:
        coverage = support / n_icp
        if coverage > max_coverage:
            return
        c = Cohort(
            name=" + ".join(r[0] for r in rules),
            rules=rules,
            n=support, passes=support,
            pass_rate=100.0,               # tautology inside ICP subset; kept for display compat
            lift_pp=round(coverage * 100, 2),  # repurposed as coverage% for display
            support=support,
            coverage=round(coverage, 4),
        )
        c.score = float(support)           # primary score = raw support
        candidates.append(c)

    # Solo cohorts (will get filtered by min_facets later, but keep them so
    # they can seed combos and so we can log their support).
    for col, support, _ in uni_stats:
        _add_cohort([(col, 1)], support)

    # ── Step 3: 2-way combos ───────────────────────────────────────────────
    import itertools as _itertools
    for f1, f2 in _itertools.combinations(top_features, 2):
        rules = [(f1, 1), (f2, 1)]
        if _facet_count(rules) > config.MAX_INCLUDE_FACETS:
            continue
        mask = (icp_df[f1] == 1) & (icp_df[f2] == 1)
        support = int(mask.sum())
        if support < min_support:
            continue
        _add_cohort(rules, support)

    # ── Step 3b: 3-way combos — anchored only, limited ─────────────────────
    # 3-ways proliferate fast; only bother when we have a base role to pin them.
    if base_role_cols:
        top_2way = sorted(
            (c for c in candidates if len(c.rules) == 2),
            key=lambda c: -c.support,
        )[:10]
        for c2 in top_2way:
            f1, f2 = c2.rules[0][0], c2.rules[1][0]
            for f3 in top_features:
                if f3 in (f1, f2):
                    continue
                rules = [(f1, 1), (f2, 1), (f3, 1)]
                if _facet_count(rules) > config.MAX_INCLUDE_FACETS:
                    continue
                mask = (icp_df[f1] == 1) & (icp_df[f2] == 1) & (icp_df[f3] == 1)
                support = int(mask.sum())
                if support < min_support:
                    continue
                # Only add if strictly narrower than the 2-way parent (else redundant).
                if support >= c2.support:
                    continue
                _add_cohort(rules, support)

    # ── Step 4a: drop broad solo blocklist hits ────────────────────────────
    pre = len(candidates)
    candidates = [
        c for c in candidates
        if not (len(c.rules) == 1 and _is_broad_solo(c.rules[0][0]))
    ]
    if pre - len(candidates):
        log.info("stage_a_support: dropped %d broad-solo candidate(s)", pre - len(candidates))

    # ── Step 4b: enforce min_facets ────────────────────────────────────────
    pre = len(candidates)
    candidates = [c for c in candidates if len(c.rules) >= min_facets]
    if pre - len(candidates):
        log.info(
            "stage_a_support: dropped %d candidate(s) below min_facets=%d",
            pre - len(candidates), min_facets,
        )

    # ── Step 7a: combo + base-role biases ──────────────────────────────────
    base_role_set = set(base_role_cols or [])
    for c in candidates:
        if len(c.rules) >= 2:
            c.score *= COMBO_SCORE_BIAS
        if base_role_set and any(r[0] in base_role_set for r in c.rules):
            c.score *= BASE_ROLE_SCORE_BIAS

    # ── Step 5: enforce base-role anchor when one is known ─────────────────
    if base_role_set:
        pre = len(candidates)
        candidates = [
            c for c in candidates
            if any(r[0] in base_role_set for r in c.rules)
        ]
        log.info(
            "stage_a_support: enforced base-role anchor — kept %d, dropped %d (no anchor).",
            len(candidates), pre - len(candidates),
        )

    # ── Step 7b: diversity multiplier (avoid 3 cohorts with the same primary facet) ──
    def _primary_facet(c: Cohort) -> str:
        return _feature_to_facet(c.rules[0][0])

    candidates.sort(key=lambda c: c.score, reverse=True)
    used_primary_facets: dict[str, set[str]] = {}
    for c in candidates:
        pf = _primary_facet(c)
        vals = {r[0] for r in c.rules}
        if pf not in used_primary_facets:
            mult = 1.0
        elif not vals.intersection(used_primary_facets[pf]):
            mult = 0.7
        else:
            mult = 0.4
        c.score *= mult
        used_primary_facets.setdefault(pf, set()).update(vals)

    candidates.sort(key=lambda c: c.score, reverse=True)
    return candidates[: config.BEAM_CANDIDATES]


def _facet_count(rules: list) -> int:
    """Count distinct LinkedIn facet dimensions in a rule set."""
    facets = {_feature_to_facet(r[0]) for r in rules}
    return len(facets)


def _feature_to_facet(feature_col: str) -> str:
    """Map binary column name back to a LinkedIn facet dimension."""
    if feature_col.startswith("skills__"):        return "skills"
    if feature_col.startswith("job_titles_norm__"): return "titles"
    if feature_col.startswith("fields_of_study__"): return "fieldsOfStudy"
    if feature_col.startswith("highest_degree_level__"): return "degrees"
    if feature_col.startswith("accreditations_norm__"):  return "accreditations"
    if feature_col.startswith("experience_band__"):      return "experience"
    return feature_col


def _compute_facet_strength(df: pd.DataFrame, cohort: Cohort, baseline: float) -> dict:
    strength = {}
    for i, (feat, val) in enumerate(cohort.rules):
        # lift without this facet
        other_rules = [(f, v) for j, (f, v) in enumerate(cohort.rules) if j != i]
        if other_rules:
            mask = _apply_rules(df, other_rules)
        else:
            mask = pd.Series([True] * len(df), index=df.index)
        s = segment_stats(df, mask, baseline)
        marginal = cohort.lift_pp - s["lift_pp"]
        strength[feat] = {"marginal_contribution": round(marginal, 2), "lift_without": round(s["lift_pp"], 2)}
    return strength


def _apply_rules(df: pd.DataFrame, rules: list) -> pd.Series:
    mask = pd.Series([True] * len(df), index=df.index)
    for feat, val in rules:
        if feat in df.columns:
            mask &= (df[feat] == val)
    return mask


# ── Stage B ───────────────────────────────────────────────────────────────────

def stage_b(df: pd.DataFrame, cohorts: list[Cohort], target_col: str | None = None) -> list[Cohort]:
    """
    Validate each cohort directionally per country.
    Adds cohort.country_results dict.

    target_col: same semantics as stage_a — which column is the positive indicator.
    Defaults to legacy resume-pass behavior.
    """
    df = df.copy()
    # Build positive-indicator column matching the one stage_a used
    if target_col in (None, "_is_pass"):
        if "_is_pass" not in df.columns:
            df["_is_pass"] = df["resume_screening_result"].astype(str).str.upper() == "PASS"
    else:
        if target_col not in df.columns:
            log.warning("stage_b: target_col %r not in DataFrame, falling back to T1", target_col)
            df["_is_pass"] = df["resume_screening_result"].astype(str).str.upper() == "PASS"
        else:
            df["_is_pass"] = df[target_col].fillna(False).astype(bool)

    country_groups = df.groupby("country")

    for cohort in cohorts:
        cohort.country_results = {}
        for country, cdf in country_groups:
            if country == "UNKNOWN" or len(cdf) < config.COUNTRY_VALIDATION_THRESHOLD:
                cohort.country_results[country] = "below_threshold"
                continue

            c_total = len(cdf)
            c_pass  = int(cdf["_is_pass"].sum())
            c_base  = c_pass / c_total if c_total else 0.0

            mask = _apply_rules(cdf, cohort.rules)
            s    = segment_stats(cdf, mask, c_base)

            same_sign   = s["lift_pp"] > 0 and cohort.lift_pp > 0
            no_strong_neg = s["lift_pp"] > -5.0
            at_least_half = s["lift_pp"] >= cohort.lift_pp * 0.5

            validated = same_sign and no_strong_neg and at_least_half
            cohort.country_results[country] = {
                "n": s["n"], "lift_pp": round(s["lift_pp"], 2),
                "validated": validated,
            }
            log.debug("Stage B %s / %s: lift=%.1fpp validated=%s",
                      cohort.name, country, s["lift_pp"], validated)

    return cohorts

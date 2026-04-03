"""
Stage A — Global statistical modeling (univariate + beam search intersections).
Stage B — Country directional validation.
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
    score:        float = 0.0       # lift_pp * log(n) * diversity_multiplier
    facet_strength: dict = field(default_factory=dict)
    country_results: dict = field(default_factory=dict)
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
    sub    = df[mask]
    n      = len(sub)
    passes = int((sub["resume_screening_result"].str.upper() == "PASS").sum())
    rate   = passes / n if n > 0 else 0.0
    lift   = (rate - baseline_rate) * 100
    n_rest = len(df) - n
    p_rest = int((df[~mask]["resume_screening_result"].str.upper() == "PASS").sum())
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

def stage_a(df: pd.DataFrame, binary_cols: list[str]) -> list[Cohort]:
    """
    1. Compute global baseline.
    2. Univariate tests on every binary column.
    3. Beam search for 2-way and 3-way intersections.
    Returns up to BEAM_CANDIDATES accepted cohorts sorted by adjusted score.
    """
    total    = len(df)
    n_pass   = int((df["resume_screening_result"].str.upper() == "PASS").sum())
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
        # Require incremental lift >= 1pp over best parent
        parent_lift = max(
            next((u.lift_pp for u in accepted_uni if u.feature == f1), 0),
            next((u.lift_pp for u in accepted_uni if u.feature == f2), 0),
        )
        if s["lift_pp"] < parent_lift + 1.0:
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
            if s["lift_pp"] < c2.lift_pp + 1.0:
                continue
            c = Cohort(
                name=f"{f1} + {f2} + {f3}",
                rules=rules3,
                n=s["n"], passes=s["passes"],
                pass_rate=s["pass_rate"], lift_pp=s["lift_pp"], p_value=s["p_value"],
            )
            c.score = raw_score(s["lift_pp"], s["n"])
            candidates.append(c)

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

def stage_b(df: pd.DataFrame, cohorts: list[Cohort]) -> list[Cohort]:
    """
    Validate each cohort directionally per country.
    Adds cohort.country_results dict.
    """
    country_groups = df.groupby("country")

    for cohort in cohorts:
        cohort.country_results = {}
        for country, cdf in country_groups:
            if country == "UNKNOWN" or len(cdf) < config.COUNTRY_VALIDATION_THRESHOLD:
                cohort.country_results[country] = "below_threshold"
                continue

            c_total = len(cdf)
            c_pass  = int((cdf["resume_screening_result"].str.upper() == "PASS").sum())
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

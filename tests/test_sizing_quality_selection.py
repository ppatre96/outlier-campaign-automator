"""Which contributors a quality sheet's ICP gets mined from.

The sheet's `quality_tier` column is authoritative — it is computed from
Snowflake by the tier logic the project lead documents on the sheet's own Rules
tab. These tests pin the two mistakes the earlier implementation made: gating on
a task count we invented (which dropped CBs the rules call Strong at any volume)
and re-deriving the ranking with a composite z-score instead of reading the label.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.sizing_analysis import (  # noqa: E402
    _ICP_MINE_TARGET, _rules_rank_key, _select_high_quality_ids,
)


def _row(uid, tier, **kw):
    r = {"user_id": uid, "quality_tier": tier}
    r.update(kw)
    return r


def test_takes_the_whole_top_tier_even_with_almost_no_tasks():
    """ADEL's Strong rule qualifies on WORKFORCES_MADE>=2 or promotion_pct>33 at
    ANY task count, so a volume gate must never drop a Strong CB."""
    records = [_row(f"s{i}", "Strong", total_tasks=n)
               for i, n in enumerate([4, 5, 6, 8, 17, 18, 1033])]
    picked = _select_high_quality_ids(records)
    assert len(picked) == 7, "every Strong row must survive regardless of volume"


def test_tops_up_from_the_next_positive_tier_to_the_target():
    records = [_row(f"s{i}", "Strong") for i in range(5)]
    records += [_row(f"a{i}", "Average", total_tasks=100 - i) for i in range(80)]
    picked = _select_high_quality_ids(records)
    assert len(picked) == _ICP_MINE_TARGET
    assert sum(1 for p in picked if p.startswith("s")) == 5  # all Strong kept
    # Average contributes in the Rules tab's order: total_tasks descending.
    assert [p for p in picked if p.startswith("a")][:3] == ["a0", "a1", "a2"]


def test_never_mines_the_excluded_tiers():
    records = [_row("s1", "Strong")]
    records += [_row(f"w{i}", "Weak", total_tasks=500) for i in range(10)]
    records += [_row(f"n{i}", "Not enough data", total_tasks=500) for i in range(10)]
    records += [_row(f"m{i}", "Mixed signal", total_tasks=500) for i in range(10)]
    assert _select_high_quality_ids(records) == ["s1"]


def test_recognizes_the_second_tier_vocabulary():
    """The Rules tab documents two label sets; another lead's sheet uses the
    Very good / Good / Average one."""
    records = [_row(f"v{i}", "Very good") for i in range(3)]
    records += [_row(f"g{i}", "Good") for i in range(3)]
    records += [_row(f"l{i}", "Low quality") for i in range(5)]
    records += [_row(f"x{i}", "Limited signal") for i in range(5)]
    picked = _select_high_quality_ids(records)
    assert sorted(picked) == ["g0", "g1", "g2", "v0", "v1", "v2"]


def test_falls_back_to_the_metric_composite_without_a_tier_column():
    """No quality_tier at all — the composite proxy is all that's left."""
    records = [
        _row(f"c{i}", "", total_tasks=100, QMS=q, PROMOTION_RATE=q * 10,
             WORKFORCES_MADE=0, num_high_qual_tags=0,
             QUALITY_DISABLE_RATE=0, FAILURE_RATE=0)
        for i, q in enumerate([1, 2, 3, 4, 5, 4.5])
    ]
    picked = _select_high_quality_ids(records)
    assert picked, "must still return contributors via the fallback"
    assert "c4" in picked  # highest QMS survives the above-median cut


def test_broad_skills_are_not_targetable():
    """LinkedIn ORs the skills facet, so one generic skill balloons the audience
    to everybody. Reuses Stage A's BROAD_SOLO_FEATURES."""
    from src.sizing_analysis import _is_broad_skill

    for generic in ("Research", "Training", "Leadership", "Customer Service", "Excel"):
        assert _is_broad_skill(generic), generic
    for specific in ("Tax Preparation", "Equity Research", "Financial Modeling"):
        assert not _is_broad_skill(specific), specific


def test_icp_exclusions_become_negative_title_facets():
    from src.sizing_analysis import _apply_icp_exclusions

    class _Icp:
        exclude_titles = ["Tax Attorney", "Bookkeeper", "Financial Advisor"]

    class _Cohort:
        name = "c"
        # The cohort targets Financial Advisor, so it must not also exclude it.
        rules = [("job_titles_norm__financial_advisor", 1), ("skills__tax_preparation", 1)]
        exclude_add = [("titles", "Sales Analyst")]

    cohort = _Cohort()
    _apply_icp_exclusions(cohort, _Icp())
    values = [v for f, v in cohort.exclude_add if f == "titles"]
    assert "Tax Attorney" in values and "Bookkeeper" in values
    assert "Sales Analyst" in values, "pre-existing family exclusions must survive"
    # The ICP wins over a weak-frequency positive rule: the targeted title is
    # dropped from the rules AND excluded.
    assert cohort.rules == [("skills__tax_preparation", 1)]
    assert "Financial Advisor" in values


def test_never_drops_the_last_positive_rule():
    """A cohort with no rules can't be targeted at all, so the conflicting rule
    survives and the exclusion is skipped instead."""
    from src.sizing_analysis import _apply_icp_exclusions

    class _Icp:
        exclude_titles = ["Financial Advisor"]

    class _Cohort:
        name = "c"
        rules = [("job_titles_norm__financial_advisor", 1)]
        exclude_add: list = []

    cohort = _Cohort()
    _apply_icp_exclusions(cohort, _Icp())
    assert cohort.rules == [("job_titles_norm__financial_advisor", 1)]
    assert cohort.exclude_add == []


def test_apply_icp_exclusions_is_a_noop_without_titles():
    from src.sizing_analysis import _apply_icp_exclusions

    class _Icp:
        exclude_titles: list = []

    class _Cohort:
        name = "c"
        rules = [("skills__x", 1)]
        exclude_add = [("titles", "Kept")]

    cohort = _Cohort()
    _apply_icp_exclusions(cohort, _Icp())
    assert cohort.exclude_add == [("titles", "Kept")]


def test_rules_rank_key_orders_hfc_then_activity_then_volume():
    hfc = _row("a", "Strong", is_hfc="TRUE", activity_bucket="Online >60D", total_tasks=1)
    active = _row("b", "Strong", is_hfc="FALSE", activity_bucket="Active L30D", total_tasks=1)
    busy = _row("c", "Strong", is_hfc="FALSE", activity_bucket="Active L30D", total_tasks=999)
    assert _rules_rank_key(hfc) < _rules_rank_key(active)   # HFC first
    assert _rules_rank_key(busy) < _rules_rank_key(active)  # then volume desc

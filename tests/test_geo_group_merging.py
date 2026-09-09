"""Undersized geo clusters get merged, never dropped.

Two problems this covers.

**Geos were being silently discarded.** The MAX_GEO_CLUSTERS cap used to be
`groups[:cap]`, which threw away the surplus clusters *and every country in
them*. On GMR-0029 that dropped 65 of 194 geos — including US, GB, CA, AU, DE,
FR, IN, PH, BR, SG and JP — because the cap ranks clusters by country COUNT, so
a 95-country long-tail bucket outranks the 7-country `anglo` cluster holding the
highest-value markets. A global English-language ramp was advertising in Eastern
Europe and Latin America but not the US or UK.

**A cluster too small to be worth a campaign.** Below GEO_AUDIENCE_FLOOR
(50,000) a cluster is folded into a similar one to widen the net instead of
running a campaign that cannot deliver.

Merge-target choice is culture-first on purpose: the cluster drives the
creative's photo-subject ethnicity via `icp_hint`, so ranking rate first put
India and Nigeria in a cluster labelled "Eastern European" (both happen to
advertise $30/hr) and would have shown those audiences Eastern European faces.
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.geo_tiers import GeoCampaignGroup, merge_small_geo_groups  # noqa: E402


def _g(cluster: str, geos: list[str], rate: str = "$50/hr", mult: float = 1.0):
    return GeoCampaignGroup(
        cluster=cluster,
        cluster_label=cluster.replace("_", " ").title(),
        geos=list(geos),
        median_multiplier=mult,
        advertised_rate=rate,
        campaign_suffix=cluster,
    )


# ── never drop a geo ────────────────────────────────────────────────────────


def test_cap_merges_instead_of_dropping():
    """The GMR-0029 shape: 12 clusters, cap 3, every geo must survive."""
    groups = [
        _g("global_mix", [f"X{i}" for i in range(95)]),
        _g("eastern_european", [f"E{i}" for i in range(18)], "$30/hr", 0.65),
        _g("latin_american", [f"L{i}" for i in range(16)], "$40/hr", 0.79),
        _g("middle_eastern", [f"M{i}" for i in range(13)]),
        _g("northern_european", [f"N{i}" for i in range(11)]),
        _g("african", [f"A{i}" for i in range(9)], "$30/hr", 0.61),
        _g("anglo", ["US", "GB", "CA", "AU", "NZ", "IE", "BM"]),
        _g("southeast_asian", [f"S{i}" for i in range(7)]),
        _g("southern_european", [f"P{i}" for i in range(6)]),
        _g("east_asian", [f"K{i}" for i in range(6)], "$45/hr", 0.90),
        _g("south_asian", ["IN", "PK", "BD", "LK", "NP"], "$30/hr", 0.55),
        _g("brazilian", ["BR"], "$25/hr", 0.48),
    ]
    before = {c for g in groups for c in g.geos}

    out = merge_small_geo_groups(groups, max_clusters=3)

    assert len(out) == 3
    after = {c for g in out for c in g.geos}
    assert after == before, f"lost geos: {sorted(before - after)}"


def test_high_value_markets_are_never_the_ones_dropped():
    """The specific GMR-0029 failure — US/GB/CA/AU had no campaign at all."""
    groups = [
        _g("global_mix", [f"X{i}" for i in range(95)]),
        _g("eastern_european", [f"E{i}" for i in range(18)]),
        _g("latin_american", [f"L{i}" for i in range(16)]),
        _g("anglo", ["US", "GB", "CA", "AU"]),
    ]
    out = merge_small_geo_groups(groups, max_clusters=3)
    covered = {c for g in out for c in g.geos}
    for market in ("US", "GB", "CA", "AU"):
        assert market in covered, f"{market} dropped out of the net"


def test_cap_of_zero_leaves_clusters_alone():
    groups = [_g("a", ["A"]), _g("b", ["B"]), _g("c", ["C"]), _g("d", ["D"])]
    assert len(merge_small_geo_groups(groups, max_clusters=0)) == 4


@pytest.mark.parametrize("n_groups", [0, 1])
def test_trivial_inputs_pass_through(n_groups):
    groups = [_g(f"c{i}", [f"G{i}"]) for i in range(n_groups)]
    assert merge_small_geo_groups(groups, max_clusters=3) == groups


def test_empty_geo_groups_are_discarded():
    """A group with no countries can't be a campaign."""
    out = merge_small_geo_groups([_g("a", ["A", "B"]), _g("empty", [])], max_clusters=0)
    assert [g.cluster for g in out] == ["a"]


# ── audience floor ─────────────────────────────────────────────────────────


def test_below_floor_cluster_is_merged():
    groups = [_g("global_mix", ["X1", "X2"]), _g("south_asian", ["IN"], "$30/hr", 0.55)]
    sizes = {"IN": 1_000, "X1": 400_000, "X2": 400_000}

    def audience(geos):
        return sum(sizes.get(g, 0) for g in geos)

    out = merge_small_geo_groups(
        groups, audience_floor=50_000, audience_fn=audience, max_clusters=0,
    )
    assert len(out) == 1
    assert set(out[0].geos) == {"X1", "X2", "IN"}


def test_above_floor_clusters_are_left_split():
    groups = [_g("global_mix", ["X1"]), _g("anglo", ["US"])]

    def audience(geos):
        return 500_000

    out = merge_small_geo_groups(
        groups, audience_floor=50_000, audience_fn=audience, max_clusters=0,
    )
    assert len(out) == 2


def test_floor_without_audience_fn_is_skipped():
    """Audience depends on the cohort's targeting — never guess it."""
    groups = [_g("a", ["A"]), _g("b", ["B"])]
    out = merge_small_geo_groups(groups, audience_floor=50_000, audience_fn=None,
                                max_clusters=0)
    assert len(out) == 2


def test_unmeasurable_cluster_is_not_merged():
    """audience_fn returning None means unknown, not zero."""
    groups = [_g("a", ["A"]), _g("b", ["B"])]
    out = merge_small_geo_groups(
        groups, audience_floor=50_000, audience_fn=lambda geos: None, max_clusters=0,
    )
    assert len(out) == 2


def test_audience_probe_failure_does_not_abort():
    groups = [_g("a", ["A"]), _g("b", ["B"])]

    def boom(geos):
        raise RuntimeError("LinkedIn 500")

    out = merge_small_geo_groups(
        groups, audience_floor=50_000, audience_fn=boom, max_clusters=0,
    )
    assert len(out) == 2


def test_floor_never_collapses_to_zero_groups():
    """Even if everything is below the floor, one campaign must remain."""
    groups = [_g("a", ["A"]), _g("b", ["B"]), _g("c", ["C"])]
    out = merge_small_geo_groups(
        groups, audience_floor=50_000, audience_fn=lambda geos: 10, max_clusters=0,
    )
    assert len(out) == 1
    assert set(out[0].geos) == {"A", "B", "C"}


# ── merge-target choice ────────────────────────────────────────────────────


def test_culture_beats_rate_so_india_is_not_labelled_eastern_european():
    """The regression this ordering exists to prevent.

    south_asian and eastern_european both advertise $30/hr. Ranking rate first
    merged India and Nigeria into a cluster labelled "Eastern European" — those
    audiences would have been shown Eastern European faces.
    """
    groups = [
        _g("eastern_european", [f"E{i}" for i in range(18)], "$30/hr", 0.65),
        _g("global_mix", [f"X{i}" for i in range(95)]),
        _g("south_asian", ["IN", "PK"], "$30/hr", 0.55),
    ]
    out = merge_small_geo_groups(groups, max_clusters=2)
    holder = next(g for g in out if "IN" in g.geos)
    assert holder.cluster != "eastern_european", "India must not be 'Eastern European'"
    assert holder.cluster == "global_mix"


def test_african_falls_to_global_not_middle_eastern():
    groups = [
        _g("middle_eastern", [f"M{i}" for i in range(13)]),
        _g("global_mix", [f"X{i}" for i in range(95)]),
        _g("african", ["NG", "KE"], "$30/hr", 0.61),
    ]
    out = merge_small_geo_groups(groups, max_clusters=2)
    holder = next(g for g in out if "NG" in g.geos)
    assert holder.cluster == "global_mix", "Nigeria must not be 'Middle Eastern'"


def test_affinity_is_preferred_when_available():
    """anglo has real cultural neighbours — use them before global_mix."""
    groups = [
        _g("northern_european", [f"N{i}" for i in range(11)]),
        _g("global_mix", [f"X{i}" for i in range(95)]),
        _g("anglo", ["US", "GB"]),
    ]
    out = merge_small_geo_groups(groups, max_clusters=2)
    holder = next(g for g in out if "US" in g.geos)
    assert holder.cluster == "northern_european"


# ── rate handling ─────────────────────────────────────────────────────────


def test_merged_rate_is_the_higher_of_the_two():
    """Matches how a cluster rate is derived in the first place (MAX multiplier)."""
    groups = [
        _g("northern_european", [f"N{i}" for i in range(11)], "$50/hr", 1.05),
        _g("anglo", ["US", "GB"], "$40/hr", 1.00),
    ]
    out = merge_small_geo_groups(groups, max_clusters=1)
    assert out[0].advertised_rate == "$50/hr"


def test_unresolved_rate_stays_empty_after_merge():
    """Empty rate means the base rate was unresolved and copy ships rate-free —
    don't invent one from the other side of the merge."""
    groups = [
        _g("global_mix", [f"X{i}" for i in range(10)], ""),
        _g("anglo", ["US"], ""),
    ]
    out = merge_small_geo_groups(groups, max_clusters=1)
    assert out[0].advertised_rate == ""


def test_cross_rate_band_merge_is_warned(caplog):
    """Absorbed geos change advertised rate — that must be visible in the log."""
    groups = [
        _g("global_mix", [f"X{i}" for i in range(10)], "$50/hr", 1.04),
        _g("african", ["NG"], "$30/hr", 0.61),
    ]
    with caplog.at_level(logging.WARNING):
        merge_small_geo_groups(
            groups, audience_floor=50_000,
            audience_fn=lambda geos: 10 if "NG" in geos else 900_000,
            max_clusters=0,
        )
    assert any("rate band" in r.getMessage() for r in caplog.records)


def test_merge_dedupes_overlapping_geos():
    groups = [_g("a", ["A", "B"]), _g("b", ["B", "C"])]
    out = merge_small_geo_groups(groups, max_clusters=1)
    assert sorted(out[0].geos) == ["A", "B", "C"]


# ── "up to $X/hr" ceiling wording ──────────────────────────────────────────


def test_multi_country_cluster_rate_is_phrased_as_a_ceiling():
    """A cluster's rate is its MAX country multiplier, so most countries in it
    pay less — global_mix spans $10-$50 and quotes $50 across 152 countries."""
    from src.geo_tiers import group_geos_for_campaigns

    groups = group_geos_for_campaigns(["US", "CA", "GB"], base_rate_usd=50.0)
    assert groups[0].advertised_rate.startswith("up to $")


def test_single_country_rate_is_exact_not_a_ceiling():
    """One country means max == min — hedging it would be misleading."""
    from src.geo_tiers import group_geos_for_campaigns

    groups = group_geos_for_campaigns(["US"], base_rate_usd=50.0)
    assert groups[0].advertised_rate == "$50/hr"
    assert "up to" not in groups[0].advertised_rate


def test_uniform_multiplier_cluster_is_exact():
    """AU and BM are both 1.00 — no spread, so no 'up to'."""
    from src.geo_tiers import group_geos_for_campaigns

    groups = group_geos_for_campaigns(["AU", "BM"], base_rate_usd=50.0)
    assert groups[0].advertised_rate == "$50/hr"


def test_verbatim_smart_ramp_rate_is_never_hedged():
    """apply_geo_multiplier=False means the rate is the authoritative
    locale-specific figure, exact — not a ceiling."""
    from src.geo_tiers import group_geos_for_campaigns

    groups = group_geos_for_campaigns(
        ["IL", "US"], base_rate_usd=22.50, apply_geo_multiplier=False,
    )
    assert all("up to" not in g.advertised_rate for g in groups)
    assert all("22.50" in g.advertised_rate for g in groups)


def test_inmail_prompt_keeps_the_ceiling_wording():
    """inmail_copy_writer used to lstrip('$') and rebuild as f'${rate}/hr',
    turning 'up to $50/hr' into '$up to $50/hr/hr' while also instructing the
    model never to say 'up to'."""
    import inspect
    from src import inmail_copy_writer as icw

    src = inspect.getsource(icw)
    assert 'is_ceiling' in src or '_is_ceiling' in src
    # The blanket prohibition must not apply to a genuine ceiling.
    assert 'reproduce it EXACTLY as written' in src

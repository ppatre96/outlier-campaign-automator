"""Cross-row (cohort × geo_cluster) dedup inside one Smart Ramp.

A ramp with N cohort rows that share project_id pulls identical Snowflake data
into Stage A in `_resolve_cohorts`, so multiple rows commonly mine the same
cohort name. Combined with overlapping included_geos, this produced duplicate
LinkedIn campaigns (e.g. "Finance × Anglo / south_asian" three times — once per
row in the ramp). `run_launch_for_ramp` now seeds one set per arm and threads it
into `_process_inmail_campaigns` and `_process_static_campaigns` so the first
row to hit a tuple wins and later rows skip with a structured log line.

These tests pin the dedup decision at the (cohort × geo_group) iteration in
each arm, using `build_inmail_variants` / `build_copy_variants` call counts as
the signal — a skipped tuple never reaches those calls. Legacy `_process_row`
CLI callers pass `seen_keys=None` and must behave exactly as before.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ─── shared fixtures ────────────────────────────────────────────────────────


def _build_cohort(name: str):
    """Minimal CohortSpec stand-in for the InMail/Static arms.

    Both arms read .name, .rules, ._stg_id, ._stg_name, getattr(.,'id'),
    getattr(.,'exclude_add'), getattr(.,'exclude_remove'). A SimpleNamespace
    covers all of them.
    """
    return SimpleNamespace(
        name=name,
        _stg_id=f"stg_{name}",
        _stg_name=name.title(),
        id=name,
        rules=[],
        exclude_add=[],
        exclude_remove=[],
    )


def _stub_two_geo_groups(monkeypatch, *clusters: str):
    """Replace group_geos_for_campaigns with one returning `len(clusters)`
    GeoCampaignGroups, named for the requested clusters. Keeps the test
    decoupled from real geo-tier logic."""
    from src import geo_tiers as _gt
    groups = [
        _gt.GeoCampaignGroup(
            cluster=c,
            cluster_label=c.title(),
            geos=[c.upper()[:2]],
            median_multiplier=1.0,
            advertised_rate="$50/hr",
            campaign_suffix=c,
        )
        for c in clusters
    ]
    monkeypatch.setattr(_gt, "group_geos_for_campaigns", lambda *a, **kw: list(groups))
    monkeypatch.setattr(_gt, "filter_blocked_geos", lambda geos: geos)
    return groups


def _mock_urn_resolver():
    """A MagicMock that returns empty {} for every resolver call — sufficient
    for the early portion of both arms that runs before the variant short-circuit."""
    m = MagicMock()
    m.resolve_default_excludes.return_value = {}
    m.resolve_facet_pairs.return_value = {}
    m.resolve_cohort_rules.return_value = {}
    return m


# ─── unit: the dedup key helper ──────────────────────────────────────────────


def test_cohort_geo_dedup_key_is_case_and_whitespace_insensitive():
    """Cohort names produced by Stage A occasionally drift in case or trailing
    whitespace between rows that share project_id (the cohort signature is
    rebuilt per-row even when the underlying rules are identical). The dedup
    key normalizes both halves so cosmetic differences collapse."""
    import main as M

    k1 = M._cohort_geo_dedup_key("Finance × Anglo", "south_asian")
    k2 = M._cohort_geo_dedup_key("finance × anglo  ", "  South_Asian")
    assert k1 == k2

    # Different cluster → different key.
    assert k1 != M._cohort_geo_dedup_key("Finance × Anglo", "western_european")


def test_cohort_geo_dedup_key_handles_empty_inputs():
    """Defensive: empty cohort.name or empty cluster shouldn't raise — the
    arms still iterate over GeoCampaignGroup(cluster='global_mix') when all
    geos are G4-blocked and over cohorts whose .name accessor returns ''."""
    import main as M

    assert M._cohort_geo_dedup_key("", "") == ("", "")
    assert M._cohort_geo_dedup_key(None, None) == ("", "")


# ─── _process_static_campaigns dedup ─────────────────────────────────────────


def _static_arm_kwargs(cohorts, li_client, urn_res, seen_keys=None):
    """Bare-minimum kwargs for _process_static_campaigns under test. dry_run
    is True with WITH_IMAGES unset, so the arm short-circuits image generation
    before any LinkedIn calls — but the (cohort × geo) enumeration where
    dedup lives still runs."""
    return dict(
        selected=cohorts,
        flow_id="flow",
        location="US",
        sheets=MagicMock(),
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        dry_run=True,
        seen_keys=seen_keys,
    )


def test_static_dedup_skips_when_cohort_geo_already_seen(monkeypatch):
    """Pre-seed seen_keys with one (cohort × geo) tuple → that combination's
    copy-gen call is skipped on the next arm invocation. The other (cohort ×
    geo) tuple still goes through copy-gen."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")  # one geo group

    copy_call_args: list[tuple[str, str]] = []

    def fake_copy(cohort, layer_map, **kw):
        copy_call_args.append((cohort.name, kw.get("geos", []) and kw["geos"][0] or ""))
        return []  # empty variants → loop short-circuits after copy gen

    monkeypatch.setattr(M, "build_copy_variants", fake_copy)

    cohorts = [_build_cohort("finance"), _build_cohort("ml")]
    # Pre-seed: "finance × anglo" was already produced by an earlier row.
    seen: set = {M._cohort_geo_dedup_key("finance", "anglo")}

    M._process_static_campaigns(
        **_static_arm_kwargs(cohorts, MagicMock(), _mock_urn_resolver(), seen_keys=seen),
    )

    cohort_names_called = [c for c, _ in copy_call_args]
    assert "finance" not in cohort_names_called, (
        "seen (finance × anglo) tuple should have been skipped before copy gen"
    )
    assert "ml" in cohort_names_called, "unseen cohort 'ml' must still produce copy"


def test_static_dedup_treats_different_geo_clusters_as_distinct(monkeypatch):
    """A cohort that's already been produced for geo_cluster A must still
    produce for geo_cluster B — the key is (cohort × geo_cluster), not cohort
    alone."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo", "south_asian")  # two geo groups

    copy_keys_called: list[tuple[str, str]] = []

    def fake_copy(cohort, layer_map, **kw):
        # geo_group_cluster is communicated via geos kw arg in build_copy_variants.
        # For this test we just record (cohort.name, geos) and verify both
        # geo iterations happened.
        copy_keys_called.append((cohort.name, tuple(kw.get("geos") or [])))
        return []

    monkeypatch.setattr(M, "build_copy_variants", fake_copy)

    cohorts = [_build_cohort("finance")]
    # Pre-seed only the anglo combination as seen.
    seen: set = {M._cohort_geo_dedup_key("finance", "anglo")}

    M._process_static_campaigns(
        **_static_arm_kwargs(cohorts, MagicMock(), _mock_urn_resolver(), seen_keys=seen),
    )

    # finance × south_asian must still produce; finance × anglo must not.
    cohort_geo_pairs = [(c, tuple(geos)) for c, geos in copy_keys_called]
    assert ("finance", ("SO",)) in cohort_geo_pairs, (
        f"finance × south_asian should produce — got pairs {cohort_geo_pairs}"
    )
    assert all(geos != ("AN",) for _, geos in cohort_geo_pairs), (
        "finance × anglo was pre-seeded as seen; should have been skipped"
    )


def test_static_dedup_none_seen_keys_preserves_legacy_behavior(monkeypatch):
    """seen_keys=None (the default — what the legacy _process_row CLI caller
    passes) must skip dedup entirely. Every (cohort × geo) tuple goes through
    copy gen as before."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")

    copy_call_count = {"n": 0}

    def fake_copy(cohort, layer_map, **kw):
        copy_call_count["n"] += 1
        return []

    monkeypatch.setattr(M, "build_copy_variants", fake_copy)

    cohorts = [_build_cohort("finance"), _build_cohort("ml")]

    M._process_static_campaigns(
        **_static_arm_kwargs(cohorts, MagicMock(), _mock_urn_resolver(), seen_keys=None),
    )

    assert copy_call_count["n"] == 2, (
        f"seen_keys=None → no dedup; expected 2 copy-gen calls, got {copy_call_count['n']}"
    )


def test_static_dedup_first_call_populates_set_then_second_call_dedups(monkeypatch):
    """End-to-end mini-flow: two sequential calls share one seen_keys set.
    Call 1 (cohort='finance', 1 geo) populates the set; call 2 with the same
    (cohort × geo) skips, but with a NEW cohort still produces.

    This mirrors the run_launch_for_ramp pattern where each ramp row calls
    _process_static_campaigns with the shared set."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")

    copy_call_args: list[str] = []

    def fake_copy(cohort, layer_map, **kw):
        copy_call_args.append(cohort.name)
        return []

    monkeypatch.setattr(M, "build_copy_variants", fake_copy)

    seen: set = set()

    # Row 1: finance.
    M._process_static_campaigns(
        **_static_arm_kwargs([_build_cohort("finance")], MagicMock(), _mock_urn_resolver(), seen_keys=seen),
    )
    # Row 2: finance again + new cohort 'ml'.
    M._process_static_campaigns(
        **_static_arm_kwargs(
            [_build_cohort("finance"), _build_cohort("ml")],
            MagicMock(), _mock_urn_resolver(),
            seen_keys=seen,
        ),
    )

    assert copy_call_args == ["finance", "ml"], (
        f"row 1 should produce 'finance'; row 2 should skip 'finance' and "
        f"produce 'ml'. Got {copy_call_args}"
    )
    # Both keys now in seen.
    assert M._cohort_geo_dedup_key("finance", "anglo") in seen
    assert M._cohort_geo_dedup_key("ml", "anglo") in seen


# ─── _process_inmail_campaigns dedup ─────────────────────────────────────────


def _inmail_arm_kwargs(cohorts, li_client, urn_res, seen_keys=None):
    """Bare-minimum kwargs for _process_inmail_campaigns under test.
    inmail_sender is set so the arm doesn't bail in the early sender check.
    dry_run is False — the production path is what carries the dedup logic
    (the dry_run branch returns earlier)."""
    bv = MagicMock()
    bv.validate_copy.return_value = MagicMock(is_compliant=True, confidence_score=1.0,
                                              violations=[], must_violations=[],
                                              should_violations=[])
    return dict(
        selected=cohorts,
        flow_id="flow",
        location="US",
        sheets=MagicMock(),
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        inmail_sender="urn:li:person:test",
        brand_voice_validator=bv,
        dry_run=False,
        seen_keys=seen_keys,
    )


def test_inmail_dedup_skips_when_cohort_geo_already_seen(monkeypatch):
    """Pre-seed seen_keys with one (cohort × geo) tuple → InMail variant
    generation is skipped for that combination on the next arm invocation."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")

    variant_call_args: list[str] = []

    def fake_variants(tg_cat, cohort, claude_key, **kw):
        variant_call_args.append(cohort.name)
        return []  # empty variants → loop continues to next (cohort × geo)

    monkeypatch.setattr(M, "build_inmail_variants", fake_variants)

    cohorts = [_build_cohort("finance"), _build_cohort("ml")]
    seen: set = {M._cohort_geo_dedup_key("finance", "anglo")}

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    M._process_inmail_campaigns(
        **_inmail_arm_kwargs(cohorts, li, _mock_urn_resolver(), seen_keys=seen),
    )

    assert "finance" not in variant_call_args, (
        "seen (finance × anglo) tuple should have been skipped before InMail variant gen"
    )
    assert "ml" in variant_call_args, "unseen cohort 'ml' must still produce InMail variants"


def test_inmail_dedup_none_seen_keys_preserves_legacy_behavior(monkeypatch):
    """seen_keys=None preserves the pre-PR behavior: every (cohort × geo)
    tuple gets InMail variants generated."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")

    call_count = {"n": 0}

    def fake_variants(tg_cat, cohort, claude_key, **kw):
        call_count["n"] += 1
        return []

    monkeypatch.setattr(M, "build_inmail_variants", fake_variants)

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    M._process_inmail_campaigns(
        **_inmail_arm_kwargs(
            [_build_cohort("finance"), _build_cohort("ml")],
            li, _mock_urn_resolver(), seen_keys=None,
        ),
    )

    assert call_count["n"] == 2, (
        f"seen_keys=None → no dedup; expected 2 variant-gen calls, got {call_count['n']}"
    )


def test_inmail_and_static_dedup_independently(monkeypatch):
    """The two arms have separate seen_keys sets so they don't suppress each
    other on the same row. Confirms run_launch_for_ramp's two-set design:
    InMail's seen_inmail_keys and Static's seen_static_keys are independent.

    Within a single row both arms should produce their first-row campaign even
    though they iterate the same (cohort × geo) tuples concurrently."""
    import main as M
    _stub_two_geo_groups(monkeypatch, "anglo")

    inmail_calls: list[str] = []
    static_calls: list[str] = []

    monkeypatch.setattr(M, "build_inmail_variants",
                        lambda tg, c, k, **kw: inmail_calls.append(c.name) or [])
    monkeypatch.setattr(M, "build_copy_variants",
                        lambda c, lm, **kw: static_calls.append(c.name) or [])

    # Each arm gets its own empty set.
    seen_inmail: set = set()
    seen_static: set = set()
    cohorts = [_build_cohort("finance")]

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs(cohorts, li, _mock_urn_resolver(), seen_keys=seen_inmail),
    )
    M._process_static_campaigns(
        **_static_arm_kwargs(cohorts, MagicMock(), _mock_urn_resolver(), seen_keys=seen_static),
    )

    # Both arms produced for the same (cohort × geo) — the two seen sets are
    # independent. If they were shared, Static would have skipped finance.
    assert inmail_calls == ["finance"]
    assert static_calls == ["finance"]
    # Each set contains exactly one key.
    assert seen_inmail == {M._cohort_geo_dedup_key("finance", "anglo")}
    assert seen_static == {M._cohort_geo_dedup_key("finance", "anglo")}

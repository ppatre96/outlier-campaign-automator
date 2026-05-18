"""Per-(cohort × geo) failure isolation in `_process_inmail_campaigns`.

Pinning the fix for the 2026-05-16 incident: LinkedIn returned 400
FAILED_TO_PROCESS_CAMPAIGN_FOR_AUDIENCE_SIZE_ESTIMATION on a single NW-European
cohort, and because the `create_inmail_campaign` call wasn't wrapped in a
try/except, the whole row's InMail arm aborted. 4 entries were lost. The fix
mirrors the Static arm's per-(cohort × geo) isolation.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# Lifted from test_dedup_cohort_geo.py — same fixtures, same minimal stubbing.
def _build_cohort(name: str):
    return SimpleNamespace(
        name=name,
        _stg_id=f"stg_{name}",
        _stg_name=name.title(),
        id=name,
        rules=[],
        exclude_add=[],
        exclude_remove=[],
    )


def _stub_one_geo_group(monkeypatch, cluster: str = "anglo"):
    from src import geo_tiers as _gt
    group = _gt.GeoCampaignGroup(
        cluster=cluster,
        cluster_label=cluster.title(),
        geos=[cluster.upper()[:2]],
        median_multiplier=1.0,
        advertised_rate="$50/hr",
        campaign_suffix=cluster,
    )
    monkeypatch.setattr(_gt, "group_geos_for_campaigns", lambda *a, **kw: [group])
    monkeypatch.setattr(_gt, "filter_blocked_geos", lambda geos: geos)


def _mock_urn_resolver():
    m = MagicMock()
    m.resolve_default_excludes.return_value = {}
    m.resolve_facet_pairs.return_value = {}
    m.resolve_cohort_rules.return_value = {"urn:li:adTargetingFacet:skills": ["urn:li:skill:1"]}
    return m


def _good_variant(angle: str):
    return SimpleNamespace(
        angle=angle,
        angle_label={"A": "Expertise", "B": "Earnings", "C": "Flexibility"}[angle],
        subject="Subject",
        body="Body text here.",
        cta_label="Apply",
        cta_url="https://outlier.ai/",
    )


def _inmail_arm_kwargs(cohorts, li_client, urn_res):
    bv = MagicMock()
    bv.validate_copy.return_value = MagicMock(
        is_compliant=True, confidence_score=1.0,
        violations=[], must_violations=[], should_violations=[],
    )
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
    )


def test_inmail_create_failure_does_not_abort_remaining_cohorts(monkeypatch):
    """When `create_inmail_campaign` raises on cohort A, cohort B's
    `create_inmail_campaign` must still be called. Before the fix the
    exception bubbled out of the (cohort × geo) loop and aborted the
    entire InMail arm — losing every subsequent cohort/geo on that row.
    """
    import main as M
    _stub_one_geo_group(monkeypatch)

    # Variants must be non-empty so we reach the create_inmail_campaign call.
    monkeypatch.setattr(
        M, "build_inmail_variants",
        lambda tg, cohort, key, **kw: [_good_variant("A"), _good_variant("B"), _good_variant("C")],
    )

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"

    # Track which cohorts the create call was attempted for.
    create_calls: list[str] = []

    def fake_create_inmail(name, **kw):
        create_calls.append(name)
        if "Cohort-A" in name:
            raise RuntimeError(
                "createInMailCampaign failed 400: FAILED_TO_PROCESS_CAMPAIGN_FOR_AUDIENCE_SIZE_ESTIMATION"
            )
        return "urn:li:sponsoredCampaign:42"

    li.create_inmail_campaign.side_effect = fake_create_inmail
    li.create_inmail_ad.return_value = "urn:li:sponsoredCreative:99"

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs(
            [_build_cohort("Cohort-A"), _build_cohort("Cohort-B")],
            li, _mock_urn_resolver(),
        ),
    )

    # Both cohorts must have been attempted, even though the first raised.
    assert len(create_calls) == 2, (
        f"InMail arm should attempt create_inmail_campaign for BOTH cohorts; "
        f"got {len(create_calls)}: {create_calls!r}"
    )
    # Cohort-B's angle attach loop must have run since its create succeeded.
    assert li.create_inmail_ad.call_count == 3, (
        f"Cohort-B's 3 angles must each get a create_inmail_ad call; "
        f"got {li.create_inmail_ad.call_count}"
    )


def test_inmail_create_success_path_unchanged(monkeypatch):
    """Sanity: a successful create still attaches all 3 angles. Catches
    accidental control-flow regressions in the new try/except wrapper."""
    import main as M
    _stub_one_geo_group(monkeypatch)

    monkeypatch.setattr(
        M, "build_inmail_variants",
        lambda tg, cohort, key, **kw: [_good_variant("A"), _good_variant("B"), _good_variant("C")],
    )

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    li.create_inmail_campaign.return_value = "urn:li:sponsoredCampaign:42"
    li.create_inmail_ad.return_value = "urn:li:sponsoredCreative:99"

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs([_build_cohort("solo")], li, _mock_urn_resolver()),
    )

    assert li.create_inmail_campaign.call_count == 1
    assert li.create_inmail_ad.call_count == 3

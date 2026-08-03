"""The Slack ramp summary must find the campaigns/creatives a run created.

Pranav, 2026-08-04: every ramp summary in the bot channel showed

    Cohort: Bengali generalist contributors
      • InMail draft: —
      • Static draft: —
      • Creative (InMail): —
      • Creative (Static): —

The campaigns existed. The aggregation looked them up by BARE cohort id, but the
LinkedIn arms key `campaigns_by_cohort` as `<cohort>_<geo_suffix>` and
`creative_paths` as `<cohort>_<geo_suffix>_<angle>` — one cohort fans out over geo
clusters and A/B/C angles. Every lookup missed, so every field rendered "—".

These tests pin the real key shapes on both sides of that boundary.
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

import main  # noqa: E402
from src.smart_ramp_notifier import _more, build_success_message  # noqa: E402


# ── the aggregation helper ────────────────────────────────────────────────────

def test_finds_geo_suffixed_campaign_keys():
    """`campaigns_by_cohort` is keyed <cohort>_<geo_suffix>."""
    result = {"campaigns_by_cohort": {
        "T1_us": "urn:li:sponsoredCampaign:1",
        "T1_emea": "urn:li:sponsoredCampaign:2",
    }}
    assert main._values_for_cohort(result, "campaigns_by_cohort", "T1") == [
        "urn:li:sponsoredCampaign:1", "urn:li:sponsoredCampaign:2",
    ]


def test_finds_angle_suffixed_creative_keys():
    """`creative_paths` is keyed <cohort>_<geo_suffix>_<angle>."""
    result = {"creative_paths": {
        "T1_us_A": "urn:li:dsc:a",
        "T1_us_B": "urn:li:dsc:b",
        "T1_us_C": "urn:li:dsc:c",
    }}
    got = main._values_for_cohort(result, "creative_paths", "T1")
    assert got == ["urn:li:dsc:a", "urn:li:dsc:b", "urn:li:dsc:c"]


def test_still_matches_a_bare_cohort_id():
    """Older single-geo rows keyed by the bare id must keep working."""
    result = {"campaigns_by_cohort": {"T1": "urn:li:sponsoredCampaign:1"}}
    assert main._values_for_cohort(result, "campaigns_by_cohort", "T1") == [
        "urn:li:sponsoredCampaign:1"
    ]


def test_cohort_id_is_not_a_loose_prefix():
    """T1 must not absorb T10's campaigns. The trailing underscore is what makes
    prefix matching safe."""
    result = {"campaigns_by_cohort": {
        "T1_us": "mine",
        "T10_us": "someone else's",
        "T1x_us": "also not mine",
    }}
    assert main._values_for_cohort(result, "campaigns_by_cohort", "T1") == ["mine"]
    assert main._values_for_cohort(result, "campaigns_by_cohort", "T10") == [
        "someone else's"
    ]


def test_cohort_ids_containing_underscores_are_isolated():
    """Real cohort ids can contain underscores (e.g. "cohort_0")."""
    result = {"campaigns_by_cohort": {
        "cohort_0_us": "zero",
        "cohort_01_us": "oh-one",
    }}
    assert main._values_for_cohort(result, "campaigns_by_cohort", "cohort_0") == ["zero"]


def test_empty_and_missing_inputs_are_safe():
    assert main._values_for_cohort({}, "campaigns_by_cohort", "T1") == []
    assert main._values_for_cohort({"campaigns_by_cohort": None}, "campaigns_by_cohort", "T1") == []
    assert main._values_for_cohort({"campaigns_by_cohort": {"T1_us": ""}},
                                   "campaigns_by_cohort", "T1") == []
    # No cohort id → never claim everything in the map.
    assert main._values_for_cohort({"campaigns_by_cohort": {"T1_us": "x"}},
                                   "campaigns_by_cohort", "") == []


# ── the rendered message ──────────────────────────────────────────────────────

def _summary(per_cohort):
    return build_success_message(
        ramp_id="GMR-0031", project_name="Aether", requester_name="Tuan",
        per_cohort=per_cohort,
    )


def test_summary_shows_the_urns_not_em_dashes():
    text = _summary([{
        "cohort_id": "T1",
        "cohort_description": "Bengali generalist contributors",
        "inmail_urn": "urn:li:sponsoredCampaign:11",
        "static_urn": "urn:li:sponsoredCampaign:22",
        "inmail_creative": "urn:li:dsc:in",
        "static_creative": "urn:li:dsc:st",
        "inmail_count": 1, "static_count": 1,
        "inmail_creative_count": 1, "static_creative_count": 1,
    }])
    assert "Bengali generalist contributors" in text
    for expected in ("urn:li:sponsoredCampaign:11", "urn:li:sponsoredCampaign:22",
                     "urn:li:dsc:in", "urn:li:dsc:st"):
        assert expected in text, expected
    assert "InMail draft: `—`" not in text
    assert "Creative (Static): —" not in text


def test_summary_flags_the_rest_of_the_fan_out():
    """One line per cohort can only name the first of each — say how many more."""
    text = _summary([{
        "cohort_id": "T1",
        "cohort_description": "Bengali generalist contributors",
        "inmail_urn": "urn:li:sponsoredCampaign:11",
        "static_urn": "urn:li:sponsoredCampaign:22",
        "inmail_creative": "urn:li:dsc:in",
        "static_creative": "urn:li:dsc:st",
        "inmail_count": 2, "static_count": 2,
        "inmail_creative_count": 6, "static_creative_count": 6,
    }])
    assert "(+1 more)" in text
    assert "(+5 more)" in text


def test_summary_still_shows_em_dash_when_an_arm_really_produced_nothing():
    """The em dash must keep meaning "nothing was created" — that signal is the
    reason the bug was hard to spot."""
    text = _summary([{
        "cohort_id": "T1",
        "cohort_description": "Bengali generalist contributors",
        "inmail_urn": None, "static_urn": "urn:li:sponsoredCampaign:22",
        "inmail_creative": None, "static_creative": "urn:li:dsc:st",
        "inmail_count": 0, "static_count": 1,
        "inmail_creative_count": 0, "static_creative_count": 1,
    }])
    assert "InMail draft: `—`" in text
    assert "Creative (InMail): —" in text
    assert "urn:li:sponsoredCampaign:22" in text


def test_more_helper_edge_cases():
    assert _more(0) == ""
    assert _more(1) == ""
    assert _more(None) == ""
    assert _more("nonsense") == ""
    assert _more(3) == " (+2 more)"

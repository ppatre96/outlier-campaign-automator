"""Google ad-group names stay unique within a parent campaign.

Google Ads rejects a second ad group whose name already exists under the same
campaign (DUPLICATE_ADGROUP_NAME). The Smart Ramp v2 name
(build_campaign_name) carries no per-cohort and no per-geo segment, so every
(cohort × geo_cluster) combo under one parent resolves to the SAME string —
`_google_unique_suffix` is what pulls them apart.

Regression (GMR-0029, 2026-09-04): the suffix was gated on
`platform == "google"`, but the Search arm runs as `platform == "google_search"`.
15 of 18 ad groups were rejected and the pipeline logged "skipping all angles"
for each, so the ramp launched 9 ads instead of 54. These tests pin BOTH Google
platform strings, and pin that Meta/LinkedIn names are left untouched.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from main import _google_unique_suffix  # noqa: E402


def _cohort(stg_id: str):
    return SimpleNamespace(_stg_id=stg_id)


def _geo(cluster: str, label: str):
    return SimpleNamespace(cluster=cluster, cluster_label=label)


# The name every GMR-0029 (cohort × geo) combo collapsed to before the fix.
_BASE = "Scale-GMR-0029 | Google_Search | coders | Coders | en-US | AR | 09/04/2026 | ALL"


@pytest.mark.parametrize("platform", ["google", "google_search"])
def test_both_google_platforms_get_a_suffix(platform):
    suffix = _google_unique_suffix(
        platform, _cohort("STG-20260904-89173"), _geo("latin_american", "Latin American")
    )
    assert suffix == " | STG-20260904-89173 | Latin American"


@pytest.mark.parametrize("platform", ["meta", "linkedin", "reddit"])
def test_non_google_platforms_keep_canonical_names(platform):
    assert _google_unique_suffix(
        platform, _cohort("STG-20260904-89173"), _geo("latin_american", "Latin American")
    ) == ""


def test_global_mix_omits_the_geo_label():
    """global_mix is the unsuffixed default — the cohort id alone separates it."""
    assert _google_unique_suffix(
        "google_search", _cohort("STG-20260904-89173"), _geo("global_mix", "Global")
    ) == " | STG-20260904-89173"


def test_every_cohort_geo_combo_is_unique_under_one_parent():
    """The GMR-0029 shape: 6 mined cohorts × 3 geo clusters under one campaign."""
    cohorts = [_cohort(f"STG-2026090{i}-{i}0000") for i in range(6)]
    geos = [
        _geo("global_mix", "Global"),
        _geo("eastern_european", "Eastern European"),
        _geo("latin_american", "Latin American"),
    ]
    names = [
        _BASE + _google_unique_suffix("google_search", c, g)
        for c in cohorts
        for g in geos
    ]
    assert len(names) == 18
    assert len(set(names)) == 18, "duplicate ad-group name would 400 on Google Ads"


def test_missing_cohort_and_geo_degrade_to_no_suffix():
    """Name construction must never block campaign creation."""
    assert _google_unique_suffix("google_search", SimpleNamespace(), None) == ""

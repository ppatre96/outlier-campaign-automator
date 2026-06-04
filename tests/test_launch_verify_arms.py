"""Integration: piece C wiring inside the live arm code paths.

Drives `_process_inmail_campaigns` (the easiest arm to fixture) end-to-end with
LAUNCH_VERIFY_ENABLED on, using fakes only — no live API — to pin the retry →
heal control flow:
  - all attaches fail (even on retry)  → heal_empty(campaign_urn) once + notify
  - first pass fails, retry recovers   → no heal
  - first pass succeeds                → no heal, notify gets an empty list

Reuses the minimal fixtures from test_inmail_isolation.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from tests.test_inmail_isolation import (
    _build_cohort,
    _stub_one_geo_group,
    _block_registry_writes,
    _mock_urn_resolver,
    _good_variant,
    _inmail_arm_kwargs,
)


def _patch_common(monkeypatch):
    import main as M
    import config
    import src.launch_verify as lv

    _stub_one_geo_group(monkeypatch)
    _block_registry_writes(monkeypatch)
    monkeypatch.setattr(config, "LAUNCH_VERIFY_ENABLED", True)
    monkeypatch.setattr(
        M, "build_inmail_variants",
        lambda tg, cohort, key, **kw: [_good_variant("A"), _good_variant("B"), _good_variant("C")],
    )
    heal = MagicMock(return_value={"platform": "linkedin", "archived": True})
    notify = MagicMock()
    monkeypatch.setattr(lv, "heal_empty", heal)
    monkeypatch.setattr(lv, "notify_healed", notify)
    return M, heal, notify


def test_inmail_empty_after_retry_heals(monkeypatch):
    M, heal, notify = _patch_common(monkeypatch)

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    li.create_inmail_campaign.return_value = "urn:li:sponsoredCampaign:42"
    # Every attach attempt fails (raises) — first pass AND retry.
    li.create_inmail_ad.side_effect = RuntimeError("createInMailCreative 400")

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs([_build_cohort("solo")], li, _mock_urn_resolver()),
    )

    # 3 angles first pass + 3 retried = 6 attach attempts before heal.
    assert li.create_inmail_ad.call_count == 6
    assert heal.call_count == 1
    _, kw = heal.call_args
    assert kw["platform"] == "linkedin"
    assert kw["container_id"] == "urn:li:sponsoredCampaign:42"
    notify.assert_called_once()
    assert len(notify.call_args[0][1]) == 1  # one healed empty reported


def test_inmail_retry_recovers_no_heal(monkeypatch):
    M, heal, notify = _patch_common(monkeypatch)

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    li.create_inmail_campaign.return_value = "urn:li:sponsoredCampaign:42"

    # First pass (3 calls) all fail; retry pass succeeds.
    calls = {"n": 0}
    def _attach(*a, **k):
        calls["n"] += 1
        if calls["n"] <= 3:
            raise RuntimeError("transient")
        return "urn:li:sponsoredCreative:99"
    li.create_inmail_ad.side_effect = _attach

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs([_build_cohort("solo")], li, _mock_urn_resolver()),
    )

    assert li.create_inmail_ad.call_count == 6  # 3 fail + 3 retry-succeed
    heal.assert_not_called()
    notify.assert_called_once()
    assert notify.call_args[0][1] == []  # nothing healed


def test_inmail_first_pass_success_no_heal(monkeypatch):
    M, heal, notify = _patch_common(monkeypatch)

    li = MagicMock()
    li.create_campaign_group.return_value = "urn:li:sponsoredCampaignGroup:1"
    li.create_inmail_campaign.return_value = "urn:li:sponsoredCampaign:42"
    li.create_inmail_ad.return_value = "urn:li:sponsoredCreative:99"

    M._process_inmail_campaigns(
        **_inmail_arm_kwargs([_build_cohort("solo")], li, _mock_urn_resolver()),
    )

    assert li.create_inmail_ad.call_count == 3  # no retry needed
    heal.assert_not_called()
    notify.assert_called_once()
    assert notify.call_args[0][1] == []

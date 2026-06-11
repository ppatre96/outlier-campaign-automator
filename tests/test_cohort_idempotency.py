"""Per-cohort launch idempotency gate (_cohort_channel_already_live).

A re-launch should create campaigns only for cohorts that don't already have
one — so a forced re-run surgically adds a newly-added cohort instead of
duplicating the rest. Off on replace (which archives + recreates), when the
flag is disabled, or when there's no ramp_id. Keys carry campaign_type so
LinkedIn static vs inmail don't shadow each other.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import main as M


class _C:
    name = "Accessibility & assistive-technology professionals"
class _G:
    cluster = "US"


def _set(monkeypatch, *, replace, flag):
    monkeypatch.setattr(config, "REPLACE_EXISTING", replace, raising=False)
    monkeypatch.setattr(config, "SKIP_EXISTING_COHORT_CAMPAIGNS", flag, raising=False)


def test_replace_bypasses_even_if_exists(monkeypatch):
    _set(monkeypatch, replace=True, flag=True)
    monkeypatch.setattr("src.ui_decisions.campaign_exists_for_cohort_channel", lambda *a: True)
    assert M._cohort_channel_already_live("GMR-0024", "linkedin", "static", _C(), _G()) is False


def test_flag_off(monkeypatch):
    _set(monkeypatch, replace=False, flag=False)
    monkeypatch.setattr("src.ui_decisions.campaign_exists_for_cohort_channel", lambda *a: True)
    assert M._cohort_channel_already_live("GMR-0024", "linkedin", "static", _C(), _G()) is False


def test_no_ramp_id(monkeypatch):
    _set(monkeypatch, replace=False, flag=True)
    monkeypatch.setattr("src.ui_decisions.campaign_exists_for_cohort_channel", lambda *a: True)
    assert M._cohort_channel_already_live("", "linkedin", "static", _C(), _G()) is False


def test_exists_true_passes_full_key(monkeypatch):
    _set(monkeypatch, replace=False, flag=True)
    captured = {}
    def fake(ramp, plat, ctype, sig, geo):
        captured.update(ramp=ramp, plat=plat, ctype=ctype, sig=sig, geo=geo)
        return True
    monkeypatch.setattr("src.ui_decisions.campaign_exists_for_cohort_channel", fake)
    assert M._cohort_channel_already_live("GMR-0024", "linkedin", "inmail", _C(), _G()) is True
    assert captured == {
        "ramp": "GMR-0024", "plat": "linkedin", "ctype": "inmail",
        "sig": _C.name, "geo": "US",
    }


def test_exists_false(monkeypatch):
    _set(monkeypatch, replace=False, flag=True)
    monkeypatch.setattr("src.ui_decisions.campaign_exists_for_cohort_channel", lambda *a: False)
    assert M._cohort_channel_already_live("GMR-0024", "meta", "static", _C(), _G()) is False

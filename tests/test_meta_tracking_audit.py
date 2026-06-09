"""Meta tracking audit flags + repairs ad sets not optimizing on the pixel event.

Guards the GMR-0023 archived-custom-conversion trap (0 conversions tracked).
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import config
import src.meta_tracking_audit as mta


def _rows():
    return [
        {"platform": "meta", "platform_campaign_id": "adset_OK", "smart_ramp_id": "GMR-0023",
         "campaign_name": "ok", "cohort_geo": "ko"},
        {"platform": "meta", "platform_campaign_id": "adset_BAD", "smart_ramp_id": "GMR-0023",
         "campaign_name": "bad", "cohort_geo": "ar"},
        {"platform": "google", "platform_campaign_id": "ag_x", "smart_ramp_id": "GMR-0023"},  # ignored
    ]


def _reader(adset_id):
    return {
        # correct pixel-event form
        "adset_OK": {"pixel_id": str(config.META_PIXEL_ID), "custom_event_type": "OTHER",
                     "custom_event_str": config.META_CUSTOM_EVENT_STR},
        # the archived-custom-conversion trap
        "adset_BAD": {"custom_conversion_id": "986478843749388"},
    }[adset_id]


def test_flags_and_repairs_bad_tracking(monkeypatch):
    import src.ui_decisions as ui
    events = []
    monkeypatch.setattr(ui, "log_event", lambda r, e, p, **k: events.append((e, p)))

    fixed = []
    out = mta.audit_meta_tracking(
        _rows(), autofix=True, reader=_reader,
        fixer=lambda aid: (fixed.append(aid) or True),
    )

    assert out["checked"] == 2                       # two meta ad sets, google ignored
    assert [v["container_id"] for v in out["violations"]] == ["adset_BAD"]
    assert fixed == ["adset_BAD"]                     # only the bad one repaired
    assert out["handled"] == ["adset_BAD"]
    assert events and events[0][0] == "meta_tracking_repaired"


def test_detect_only_no_repair(monkeypatch):
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda *a, **k: None)
    out = mta.audit_meta_tracking(
        _rows(), autofix=False, reader=_reader,
        fixer=lambda aid: (_ for _ in ()).throw(AssertionError("should not fix")),
    )
    assert len(out["violations"]) == 1
    assert out["handled"] == []


def test_auto_rebuild_on_published_adset(monkeypatch):
    import src.ui_decisions as ui
    events = []
    monkeypatch.setattr(ui, "log_event", lambda r, e, p, **k: events.append((e, p)))

    def _published_fixer(aid):
        raise RuntimeError('(#100) error_subcode 3260011 "Can\'t Make Edits to Published Ad Set"')

    paused_old = []
    out = mta.audit_meta_tracking(
        _rows(), autofix=True, auto_rebuild=True, reader=_reader,
        fixer=_published_fixer,
        rebuilder=lambda aid: f"new_{aid}",
        old_pauser=lambda aid: (paused_old.append(aid) or True),
    )

    v = out["detail"][0]
    assert v["rebuilt_to"] == "new_adset_BAD"      # copy created
    assert paused_old == ["adset_BAD"]             # old one paused
    assert v["needs_rebuild"] is False             # resolved via rebuild
    assert out["handled"] == ["adset_BAD"]         # won't re-process
    assert events[0][0] == "meta_tracking_rebuilt"


def test_published_flags_needs_rebuild_when_autorebuild_off(monkeypatch):
    import src.ui_decisions as ui
    events = []
    monkeypatch.setattr(ui, "log_event", lambda r, e, p, **k: events.append((e, p)))

    def _published_fixer(aid):
        raise RuntimeError("3260011 published")

    out = mta.audit_meta_tracking(
        _rows(), autofix=True, auto_rebuild=False, reader=_reader,
        fixer=_published_fixer,
        rebuilder=lambda aid: (_ for _ in ()).throw(AssertionError("rebuild disabled")),
        old_pauser=lambda aid: (_ for _ in ()).throw(AssertionError("should not pause")),
    )
    assert out["detail"][0]["needs_rebuild"] is True
    assert out["handled"] == []
    assert events[0][0] == "meta_tracking_needs_rebuild"


def test_exclude_skips_repair():
    out = mta.audit_meta_tracking(
        _rows(), autofix=True, reader=_reader,
        fixer=lambda aid: (_ for _ in ()).throw(AssertionError("excluded, should not fix")),
        exclude_containers={"adset_BAD"},
    )
    assert len(out["violations"]) == 1   # still reported
    assert out["handled"] == []          # but not repaired (excluded)

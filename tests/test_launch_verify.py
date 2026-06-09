"""Piece C — verify-and-heal. heal_empty() routes to the right per-platform
child-container archiver, flags via log_event, and never raises; notify_healed
is a no-op on an empty list.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.launch_verify as lv


def test_heal_empty_routes_meta(monkeypatch):
    calls = {}
    def _fake_meta(cid):
        calls["meta"] = cid
        return True
    monkeypatch.setattr(lv, "_archive_meta_adset", _fake_meta)
    # log_event is imported lazily inside heal_empty; patch the source.
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda *a, **k: None)

    summ = lv.heal_empty(platform="meta", container_id="6123", ramp_id="GMR-0099",
                         campaign_name="Scale-GMR-0099 | Meta | …", reason="0 ads")
    assert calls["meta"] == "6123"
    assert summ["archived"] is True
    assert summ["platform"] == "meta"
    assert summ["container_id"] == "6123"


def test_heal_empty_routes_google_search_uses_search_channel(monkeypatch):
    seen = {}
    def _fake_google(cid, channel="display"):
        seen["ch"] = channel
        return True
    monkeypatch.setattr(lv, "_archive_google_adgroup", _fake_google)
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda *a, **k: None)

    lv.heal_empty(platform="google_search", container_id="customers/1/adGroups/9",
                  ramp_id="r", campaign_name="c")
    assert seen["ch"] == "search"


def test_heal_empty_swallows_archiver_errors(monkeypatch):
    def _boom(_):
        raise RuntimeError("api down")
    monkeypatch.setattr(lv, "_archive_meta_adset", _boom)
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda *a, **k: None)

    summ = lv.heal_empty(platform="meta", container_id="x", ramp_id="r", campaign_name="c")
    assert summ["archived"] is False  # healed-flag still returned, no raise


def test_heal_empty_unknown_platform_returns_none():
    assert lv.heal_empty(platform="tiktok", container_id="x", ramp_id="r", campaign_name="c") is None


def test_notify_healed_noop_on_empty(monkeypatch):
    # Should not even import the notifier when there's nothing to report.
    called = {"n": 0}
    import src.smart_ramp_notifier as nf
    monkeypatch.setattr(nf, "_send_to_all_targets", lambda *a, **k: called.__setitem__("n", called["n"] + 1))
    lv.notify_healed("r", [])
    assert called["n"] == 0


def test_record_keywords_dropped_flags_and_returns(monkeypatch):
    events = []
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda r, e, p, **k: events.append((r, e, p)))

    summ = lv.record_keywords_dropped(
        ramp_id="GMR-0099", container_id="customers/1/adGroups/9",
        campaign_name="Scale-GMR-0099 | Search | …",
        dropped=["assistive technology daily use", "screen reader jobs"],
    )
    assert summ is not None
    assert summ["dropped"] == ["assistive technology daily use", "screen reader jobs"]
    assert "2 keyword(s) rejected" in summ["reason"]
    # One audit row written with the launch_keywords_dropped event type.
    assert len(events) == 1 and events[0][1] == "launch_keywords_dropped"


def test_record_keywords_dropped_noop_on_empty():
    assert lv.record_keywords_dropped(
        ramp_id="r", container_id="x", campaign_name="c", dropped=[],
    ) is None

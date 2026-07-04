"""A locale-scoped relaunch-replace must archive ONLY the targeted locales.

Regression for the GMR-0023 footgun (2026-07-04): the workflow `replace` flag
archives via list_campaign_platform_ids(ramp_id, platform), whose SQL filtered
only on (ramp_id, platform) — no locale scope. A relaunch scoped to a few
locales via ONLY_LOCALES would therefore archive the WHOLE ramp's campaigns
(all languages) and only recreate the targeted ones, silently wiping the rest.
The archive path must honor ONLY_LOCALES by matching the locale token embedded
in each campaign's campaign_name.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.ui_decisions as ui


class _FakeCursor:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params):
        self._sink["sql"] = sql
        self._sink["params"] = list(params)
    def fetchall(self):
        # one row per bound (ramp, platform, *patterns) so len() reflects params
        return [(f"id{i}",) for i in range(len(self._sink["params"]))]


class _FakeConn:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _FakeCursor(self._sink)


def _patch(monkeypatch):
    sink = {}
    monkeypatch.setattr(ui, "_connect", lambda: _FakeConn(sink))
    return sink


def test_no_locales_no_ilike_filter(monkeypatch):
    sink = _patch(monkeypatch)
    ui.list_campaign_platform_ids("GMR-0023", "linkedin")
    assert "ILIKE" not in sink["sql"]
    assert sink["params"] == ["GMR-0023", "linkedin"]


def test_locales_add_one_ilike_clause_each(monkeypatch):
    sink = _patch(monkeypatch)
    ui.list_campaign_platform_ids("GMR-0023", "linkedin", ["ko-KR", "bn-IN"])
    assert sink["sql"].count("ILIKE") == 2
    # delimited, lowercased locale tokens — case-insensitive vs campaign_name
    assert sink["params"] == ["GMR-0023", "linkedin", "%| ko-kr |%", "%| bn-in |%"]


def test_locales_normalized_like_only_locales_env(monkeypatch):
    sink = _patch(monkeypatch)
    # mixed case + underscore, mirroring the ONLY_LOCALES parse (lower, _->-)
    ui.list_campaign_platform_ids("GMR-0023", "meta", ["ID_id", " it-IT "])
    assert sink["params"][2:] == ["%| id-id |%", "%| it-it |%"]


def test_blank_locales_ignored(monkeypatch):
    sink = _patch(monkeypatch)
    ui.list_campaign_platform_ids("GMR-0023", "meta", ["", "  ", None])
    assert "ILIKE" not in sink["sql"]
    assert sink["params"] == ["GMR-0023", "meta"]

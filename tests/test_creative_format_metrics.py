"""Unit tests for creative-format delivery parsing + aggregation helpers."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.creative_format_metrics as cf


def test_lang_of():
    assert cf._lang_of("Scale-GMR-0023 | Meta | language | bn-IN | bn-IN | IN | 07/04") == "Bengali"
    assert cf._lang_of("agent_Scale-GMR-0023 | Meta | de-DE | DE") == "German"
    assert cf._lang_of("Scale-GMR-0023 | Meta | ko-KR") == "Korean"
    assert cf._lang_of("Scale-GMR-0023 | Meta | English | US") == ""   # not a tracked locale


def test_ramp_of():
    assert cf._ramp_of("Scale-GMR-0023 | Meta | bn-IN") == "GMR-0023"
    assert cf._ramp_of("agent_Scale-GMR-0011 | Meta") == "GMR-0011"
    assert cf._ramp_of("no ramp here") == ""


def test_upsert_batch_filters_incomplete(monkeypatch):
    """Rows missing a key are dropped; complete rows are sent."""
    import src.ui_decisions as ui
    captured = {}

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, *a): pass
        def executemany(self, sql, params): captured["params"] = params

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def cursor(self): return _Cur()
        def commit(self): pass

    monkeypatch.setattr(ui, "_connect", lambda: _Conn())
    n = ui.upsert_meta_creative_format_batch([
        {"ramp_id": "GMR-0023", "language": "Bengali", "creative_format": "video",
         "metric_date": "2026-07-08", "impressions": 100, "clicks": 5, "spend_usd": 2.5},
        {"ramp_id": "", "language": "Bengali", "creative_format": "video",   # dropped
         "metric_date": "2026-07-08"},
    ])
    assert n == 1
    assert captured["params"][0][:4] == ["GMR-0023", "Bengali", "video", "2026-07-08"]

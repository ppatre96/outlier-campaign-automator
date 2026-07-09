"""daily_metrics — registry index resolution + batched daily upsert.

The dashboard's DoD charts depend on funnel-by-day and delivery-by-day landing
on the SAME (campaign × day) row via one stable campaign_key, and on each pass
touching only its own metric columns (no clobber). These tests pin both.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.daily_metrics as dm
import src.ui_decisions as ui


def test_index_uses_canonical_utm_for_utm_channels_and_idtail_for_google(monkeypatch):
    rows = [
        {"smart_ramp_id": "GMR-0023", "platform": "meta", "campaign_type": "static",
         "utm_campaign": "Scale-GMR-0023 | Meta | id-ID | 06/09/2026",
         "platform_campaign_id": "120333"},
        {"smart_ramp_id": "GMR-0023", "platform": "google", "campaign_type": "static",
         "campaign_name": "Scale-GMR-0023 | Google | search",
         "platform_campaign_id": "customers/88/adGroups/197762789672"},
        {"smart_ramp_id": "GMR-0023", "platform": "parent", "campaign_type": "parent",
         "platform_campaign_id": "999"},  # parent must be skipped
    ]
    monkeypatch.setattr(ui, "list_all_campaign_data", lambda: rows)
    by_utm, by_id, _rep = dm._build_indexes()
    # meta → keyed by canonical utm; campaign_key is that canonical utm
    from src.campaign_registry import _canonical_utm
    k = _canonical_utm("scale-gmr-0023 | meta | id-id | 06/09/2026")
    assert k in by_utm and by_utm[k].campaign_key == k and by_utm[k].platform == "meta"
    # google → keyed by id tail; campaign_key is the id tail
    assert "197762789672" in by_id and by_id["197762789672"].campaign_key == "197762789672"
    # meta ad id also resolvable by id (for delivery), campaign_key stays the utm
    assert by_id["120333"].campaign_key == k
    # parent skipped
    assert "999" not in by_id


def test_tiktok_is_a_utm_funnel_channel(monkeypatch):
    """TikTok is a real UTM-attributed channel (not creative-only): its campaign
    key is the canonical UTM, and funnel_writeback + redash treat it as by=utm."""
    from src.redash_db import _CHANNEL_FUNNEL, CHANNEL_JOIN_MODE
    assert "tiktok" in dm._UTM_CHANNELS
    assert _CHANNEL_FUNNEL["tiktok"]["by"] == "utm"
    assert CHANNEL_JOIN_MODE["tiktok"] == "utm"
    # a tiktok registry row keys by canonical utm (like meta/reddit)
    rows = [{"smart_ramp_id": "GMR-0023", "platform": "tiktok", "campaign_type": "static",
             "utm_campaign": "Scale-GMR-0023 | TikTok | US | 07/09/2026",
             "platform_campaign_id": "ttc1"}]
    monkeypatch.setattr(ui, "list_all_campaign_data", lambda: rows)
    by_utm, by_id, _ = dm._build_indexes()
    from src.campaign_registry import _canonical_utm
    k = _canonical_utm("scale-gmr-0023 | tiktok | us | 07/09/2026")
    assert k in by_utm and by_utm[k].platform == "tiktok"
    assert by_id["ttc1"].campaign_key == k     # delivery resolvable by id → same key


def test_reddit_representative_is_highest_impressions(monkeypatch):
    """Reddit funnel attributes at ramp level to the ramp's highest-impressions
    reddit row (warehouse UTM collapses geos + CAMPAIGN_ID is null)."""
    rows = [
        {"smart_ramp_id": "GMR-0011", "platform": "reddit", "campaign_type": "static",
         "campaign_name": "Scale-GMR-0011 | Reddit | US | 05/01/2026",
         "platform_campaign_id": "111", "impressions": 150000},
        {"smart_ramp_id": "GMR-0011", "platform": "reddit", "campaign_type": "static",
         "campaign_name": "Scale-GMR-0011 | Reddit | Native Language + Geo",
         "platform_campaign_id": "222", "impressions": 26895910},
    ]
    monkeypatch.setattr(ui, "list_all_campaign_data", lambda: rows)
    _u, _i, rep = dm._build_indexes()
    assert dm._ramp_of("scale-gmr-0011 | reddit | coder | — | 05/01/2026") == "GMR-0011"
    assert rep["GMR-0011"].campaign_key.endswith("native language + geo") or "native" in rep["GMR-0011"].campaign_key
    # the 26.9M-impression row wins over the 150k one
    assert rep["GMR-0011"].campaign_name.endswith("Native Language + Geo")


def test_batch_writes_only_its_metric_columns(monkeypatch):
    """Funnel batch and delivery batch each SET only their own columns — the SQL
    must not mention the other pass's columns (that's what lets them merge)."""
    captured = {}

    class _Cur:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def execute(self, sql, *a): captured.setdefault("execute", []).append(sql)
        def executemany(self, sql, params): captured["sql"] = sql; captured["params"] = params

    class _Conn:
        def __enter__(self): return self
        def __exit__(self, *a): return False
        def cursor(self): return _Cur()
        def commit(self): captured["committed"] = True

    monkeypatch.setattr(ui, "_connect", lambda: _Conn())
    n = ui.upsert_daily_metrics_batch(
        [{"ramp_id": "GMR-0023", "platform": "meta", "campaign_key": "k",
          "metric_date": "2026-06-09", "campaign_name": "c", "signups": 10, "activations": 2}],
        ["signups", "screening_passes", "activations"],
    )
    assert n == 1
    sql = captured["sql"]
    assert "signups = excluded.signups" in sql and "activations = excluded.activations" in sql
    # delivery columns must NOT be in a funnel batch's SET clause
    assert "spend_usd = excluded" not in sql and "impressions = excluded" not in sql


def test_batch_skips_incomplete_rows(monkeypatch):
    monkeypatch.setattr(ui, "_connect", lambda: (_ for _ in ()).throw(AssertionError("should not connect")))
    # no rows with a complete key → no DB touch, returns 0
    assert ui.upsert_daily_metrics_batch([{"ramp_id": "", "platform": "meta"}], ["signups"]) == 0
    assert ui.upsert_daily_metrics_batch([], ["signups"]) == 0

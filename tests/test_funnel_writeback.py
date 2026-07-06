"""update_funnel_metrics writes FEED-15 funnel outcomes onto registry rows by
creative id — the writeback that fills the previously-always-empty
activations / skill_passes / applications columns."""

import src.campaign_registry as reg
import src.ui_decisions as ui


def _stub_registry(monkeypatch, rows):
    saved = {}
    monkeypatch.setattr(reg, "_load", lambda: rows)
    monkeypatch.setattr(reg, "_save", lambda rs: saved.__setitem__("rows", rs))
    monkeypatch.setattr(ui, "upsert_campaign", lambda rec: saved.setdefault("pg", []).append(rec))
    return saved


def test_match_by_urn_and_recompute_cpa(monkeypatch):
    rows = [{"platform_creative_id": "urn:li:sponsoredCreative:12345", "spend_usd": 200.0, "angle": "C"}]
    saved = _stub_registry(monkeypatch, rows)
    n = reg.update_funnel_metrics("12345", applications=100, skill_passes=40, activations=12)
    assert n == 1
    r = saved["rows"][0]
    assert (r["applications"], r["skill_passes"], r["activations"]) == (100, 40, 12)
    assert r["cpa_usd"] == 2.0            # 200 / 100
    assert saved["pg"]                    # mirrored to Postgres


def test_accepts_numeric_id_and_handles_no_spend(monkeypatch):
    rows = [{"platform_creative_id": "67890", "spend_usd": None, "angle": "A"}]
    saved = _stub_registry(monkeypatch, rows)
    n = reg.update_funnel_metrics("urn:li:sponsoredCreative:67890", applications=5, skill_passes=1, activations=0)
    assert n == 1
    r = saved["rows"][0]
    assert r["activations"] == 0
    assert r.get("cpa_usd") is None       # no spend → no cpa


def test_no_match_returns_zero(monkeypatch):
    saved = _stub_registry(monkeypatch, [{"platform_creative_id": "11111"}])
    assert reg.update_funnel_metrics("99999", applications=1) == 0
    assert "rows" not in saved            # nothing saved when nothing matched


def test_empty_creative_id_is_noop(monkeypatch):
    saved = _stub_registry(monkeypatch, [{"platform_creative_id": "111"}])
    assert reg.update_funnel_metrics("", applications=1) == 0
    assert "rows" not in saved

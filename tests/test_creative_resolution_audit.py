"""Auditor catches (and optionally pauses) thumbnail-resolution creatives.

GMR-0023 2026-06-09: native-language B/C variants went live at 64×64. The
weekly auditor must flag these every run and — when AUDIT_AUTOFIX_LOWRES is on —
pause the offending container.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.creative_resolution_audit as cra


def _rows():
    return [
        {"platform": "meta", "platform_campaign_id": "adset_A", "platform_creative_id": "ad_A",
         "smart_ramp_id": "GMR-0023", "cohort_geo": "ko__SEA", "campaign_name": "ko A",
         "creative_image_path": "https://drive/ko_A.png"},  # full res
        {"platform": "meta", "platform_campaign_id": "adset_A", "platform_creative_id": "ad_B",
         "smart_ramp_id": "GMR-0023", "cohort_geo": "ko__SEA", "campaign_name": "ko B",
         "creative_image_path": "https://drive/ko_B.png"},  # 64x64 → violation
        {"platform": "google", "platform_campaign_id": "customers/1/adGroups/9", "platform_creative_id": "rda_C",
         "smart_ramp_id": "GMR-0023", "cohort_geo": "it__WEU", "campaign_name": "it C",
         "creative_image_path": "/tmp/it_C.png"},  # 64x64 → violation
    ]


def _dim_reader(path):
    return {"https://drive/ko_A.png": (1080, 1350),
            "https://drive/ko_B.png": (64, 64),
            "/tmp/it_C.png": (64, 64)}.get(path)


def test_detects_and_pauses_lowres(monkeypatch):
    import src.ui_decisions as ui
    events = []
    monkeypatch.setattr(ui, "log_event", lambda r, e, p, **k: events.append((r, e, p)))

    paused_calls = []
    def _pauser(platform, container_id):
        paused_calls.append((platform, container_id))
        return True

    out = cra.audit_creative_resolution(
        _rows(), min_px=600, autofix=True, dim_reader=_dim_reader, pauser=_pauser,
    )

    assert out["checked"] == 3
    assert len(out["violations"]) == 2          # ko_B + it_C (ko_A is full res)
    # Two distinct containers paused once each.
    assert ("meta", "adset_A") in paused_calls
    assert ("google", "customers/1/adGroups/9") in paused_calls
    assert len(paused_calls) == 2
    assert all(et == "creative_lowres_paused" for _, et, _ in events)


def test_dedups_container_pauses_once(monkeypatch):
    import src.ui_decisions as ui
    monkeypatch.setattr(ui, "log_event", lambda *a, **k: None)
    rows = _rows()
    rows[0]["creative_image_path"] = "https://drive/ko_B.png"  # make ko_A also 64x64, same adset_A

    calls = []
    out = cra.audit_creative_resolution(
        rows, min_px=600, autofix=True, dim_reader=_dim_reader,
        pauser=lambda p, c: calls.append((p, c)) or True,
    )
    assert len(out["violations"]) == 3
    # adset_A appears twice in violations but is paused only once.
    assert calls.count(("meta", "adset_A")) == 1


def test_autofix_off_detects_only():
    out = cra.audit_creative_resolution(
        _rows(), min_px=600, autofix=False, dim_reader=_dim_reader,
        pauser=lambda p, c: (_ for _ in ()).throw(AssertionError("should not pause")),
    )
    assert len(out["violations"]) == 2
    assert out["paused"] == []

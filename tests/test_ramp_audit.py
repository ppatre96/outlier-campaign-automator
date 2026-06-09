"""audit_ramp loops check->fix->re-check to a fixpoint and never re-fixes a
container it already handled."""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.ramp_audit as ra
import src.creative_resolution_audit as cra


def _fake_acr(rows, *, autofix=True, exclude_containers=None, **kw):
    """Simulate one standing low-res violation on container 'adset_A'."""
    exclude_containers = exclude_containers or set()
    violations = [{
        "platform": "meta", "container_id": "adset_A", "creative_id": "ad_B",
        "ramp_id": "R", "cohort_geo": "", "campaign_name": "",
        "width": 64, "height": 64,
    }]
    paused = []
    if autofix:
        for v in violations:
            if v["container_id"] not in exclude_containers:
                paused.append(v)
    return {"checked": 1, "violations": violations, "paused": paused,
            "autofix": autofix, "min_px": 600}


def test_autofix_reaches_fixpoint_without_re_pausing(monkeypatch):
    monkeypatch.setattr(ra, "_registry_rows_for_ramp", lambda rid: [{"x": 1}])
    monkeypatch.setattr(cra, "audit_creative_resolution", _fake_acr)

    out = ra.audit_ramp("R", autofix=True, notify=False)

    # Pass 1 fixes adset_A; pass 2 sees it excluded → 0 new fixes → stop.
    assert out["iterations"] == 2
    assert len(out["fixes_applied"]) == 1
    assert out["fixes_applied"][0]["container_id"] == "adset_A"
    assert out["residual"] == []          # the only issue was fixed


def test_detect_only_reports_residual(monkeypatch):
    monkeypatch.setattr(ra, "_registry_rows_for_ramp", lambda rid: [{"x": 1}])
    monkeypatch.setattr(cra, "audit_creative_resolution", _fake_acr)

    out = ra.audit_ramp("R", autofix=False, notify=False)

    assert out["iterations"] == 1         # nothing fixed → stop after first pass
    assert out["fixes_applied"] == []
    assert len(out["residual"]) == 1
    assert out["residual"][0]["container_id"] == "adset_A"


def test_disabled_flag_skips(monkeypatch):
    monkeypatch.setattr(ra.config, "RAMP_AUDIT_ENABLED", False)
    out = ra.audit_ramp("R", notify=False)
    assert out.get("skipped")

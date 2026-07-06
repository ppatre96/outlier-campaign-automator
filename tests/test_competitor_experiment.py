"""Tests for the competitor-insight experiment loop (src/competitor_experiment.py)."""

import json

from src import competitor_experiment as ce


def _write_intel(tmp_path, ideas):
    p = tmp_path / "latest.json"
    p.write_text(json.dumps({"tg_label": "Data Analysts", "experiment_ideas": ideas}))
    return p


def test_normalize_mixed_shapes_and_drops_empty(tmp_path):
    ideas = ["Lead with same-day pay", {"description": "Show payout screenshots"}, "", 123]
    out = ce._normalize_ideas({"experiment_ideas": ideas})
    # 2 valid ideas survive; ints/empties dropped
    assert len(out) == 2
    assert all(i["angle"] == ce.CHALLENGER_ANGLE for i in out)
    # distinct backlog keys so the queue holds multiple candidates
    assert out[0]["cohort"] != out[1]["cohort"]


def test_refresh_backlog_pins_top_and_is_idempotent(tmp_path):
    intel = _write_intel(tmp_path, ["Idea one about pay", "Idea two about reviews"])
    backlog = tmp_path / "backlog.json"
    directive = tmp_path / "directive.json"

    out = ce.refresh_backlog(str(intel), str(backlog), str(directive))
    assert out["ok"] and out["backlog_size"] == 2
    assert out["directive"]["angle"] == ce.CHALLENGER_ANGLE

    # Re-running must not duplicate entries.
    out2 = ce.refresh_backlog(str(intel), str(backlog), str(directive))
    assert out2["backlog_size"] == 2


def test_directive_prompt_block(tmp_path):
    directive = tmp_path / "directive.json"
    assert ce.directive_prompt_block(str(directive)) == ""  # none active

    directive.write_text(json.dumps({"description": "Test same-day pay", "angle": "C"}))
    block = ce.directive_prompt_block(str(directive))
    assert "PRIORITY EXPERIMENT" in block and "Angle C" in block and "same-day pay" in block


def test_refresh_backlog_missing_intel(tmp_path):
    out = ce.refresh_backlog(str(tmp_path / "nope.json"), str(tmp_path / "b.json"))
    assert out["ok"] is False and out["backlog_size"] == 0


def test_format_slack_section_verdicts_and_vocab(tmp_path):
    results = {
        "directive": {"description": "Same-day pay", "angle": "C"},
        "cohorts": [
            {"cohort": "DA", "challenger_ctr": 0.021, "baseline_ctr": 0.017,
             "lift_pct": 23.5, "n_impressions": 8200, "verdict": "winner"},
            {"cohort": "SWE", "challenger_ctr": 0.009, "baseline_ctr": 0.012,
             "lift_pct": -25.0, "n_impressions": 5100, "verdict": "loser"},
            {"cohort": "LEGAL", "challenger_ctr": 0.02, "baseline_ctr": 0.019,
             "lift_pct": 5.3, "n_impressions": 300, "verdict": "gathering"},
        ],
    }
    msg = "\n".join(ce.format_slack_section(results))
    assert "✅ DA" in msg and "❌ SWE" in msg and "(low volume)" in msg
    # Outlier vocabulary guard — banned token must not leak.
    assert "performance" not in msg.lower()


def test_format_slack_section_no_directive():
    lines = ce.format_slack_section({"directive": None})
    assert any("No competitor insight" in ln for ln in lines)


def _fake_redash(df, monkeypatch):
    class FakeRC:
        def query_creative_performance(self, days_back=7):
            return df
    monkeypatch.setattr("src.redash_db.RedashClient", FakeRC)


def test_read_results_per_insight_attribution(tmp_path, monkeypatch):
    """Challenger = ONLY angle-C creatives tagged with the pinned experiment_id;
    a differently-tagged angle-C row (from a past experiment) must be excluded."""
    import pandas as pd

    monkeypatch.setattr(ce, "active_directive",
                        lambda *a, **k: {"experiment_id": "EXP-x-C", "description": "d", "angle": "C"})
    df = pd.DataFrame([
        {"cohort_name": "K1", "angle": "C", "impressions": 5000, "ctr": 0.8, "experiment_id": "EXP-x-C"},
        {"cohort_name": "K1", "angle": "C", "impressions": 5000, "ctr": 9.9, "experiment_id": "EXP-old"},
        {"cohort_name": "K1", "angle": "A", "impressions": 5000, "ctr": 0.6, "experiment_id": ""},
        {"cohort_name": "K1", "angle": "B", "impressions": 5000, "ctr": 0.6, "experiment_id": ""},
    ])
    _fake_redash(df, monkeypatch)

    r = ce.read_results(days_back=7, backlog_path=str(tmp_path / "b.json"))
    assert r["attribution"] == "per-insight"
    c = r["cohorts"][0]
    # 0.8 ctr (percentage-points) / 100 → 0.008; the EXP-old 9.9 row is excluded
    assert abs(c["challenger_ctr"] - 0.008) < 1e-6, c
    assert abs(c["baseline_ctr"] - 0.006) < 1e-6, c


def test_read_results_falls_back_to_angle_proxy(tmp_path, monkeypatch):
    """With no creatives tagged for the pinned experiment, fall back to the
    angle-C-vs-baseline proxy and label it as such."""
    import pandas as pd

    monkeypatch.setattr(ce, "active_directive",
                        lambda *a, **k: {"experiment_id": "EXP-new-C", "description": "d", "angle": "C"})
    df = pd.DataFrame([
        {"cohort_name": "K1", "angle": "C", "impressions": 5000, "ctr": 0.8, "experiment_id": ""},
        {"cohort_name": "K1", "angle": "A", "impressions": 5000, "ctr": 0.6, "experiment_id": ""},
    ])
    _fake_redash(df, monkeypatch)

    r = ce.read_results(days_back=7, backlog_path=str(tmp_path / "b.json"))
    assert r["attribution"] == "angle-proxy"
    assert r["cohorts"][0]["verdict"] == "winner"  # 0.008 vs 0.006 = +33%

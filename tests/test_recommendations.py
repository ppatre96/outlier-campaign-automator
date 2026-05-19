"""Phase 6 — per-campaign recommendation engine.

Covers:
- `_classify_campaign_row` returns the right (classification, action) per
  the handoff-locked rules (insufficient_data / failing / underperforming /
  working).
- `FeedbackAgent.recommend_actions` walks the Campaign Registry, classifies,
  and (when persist=True) upserts via `ui_decisions.upsert_recommendation`.
- `ui_decisions.upsert_recommendation` swallows UIDecisionsUnavailable.
- `ui_decisions.set_recommendation_decision` validates the decision enum.

Registry + Postgres are mocked — no live LinkedIn / no live DB.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ── _classify_campaign_row ────────────────────────────────────────────────────

def _row(**overrides) -> dict:
    base = {
        "smart_ramp_id":   "GMR-TEST",
        "platform_campaign_id": "urn:li:sponsoredCampaign:1",
        "cohort_signature": "stem-grad",
        "platform":        "linkedin",
        "angle":           "A",
        "status":          "active",
        "impressions":     0,
        "clicks":          0,
        "ctr_pct":         0,
        "spend_usd":       0,
        "cpa_usd":         None,
    }
    base.update(overrides)
    return base


def test_classify_insufficient_data_under_threshold():
    from src.feedback_agent import _classify_campaign_row
    cls, action, rationale, signal = _classify_campaign_row(
        _row(impressions=500, clicks=0, ctr_pct=0.0, spend_usd=5.0)
    )
    assert cls == "insufficient_data"
    assert action == "keep"
    assert signal["spend_usd"] == 5.0


def test_classify_failing_zero_clicks_above_spend_floor():
    from src.feedback_agent import _classify_campaign_row
    cls, action, rationale, signal = _classify_campaign_row(
        _row(impressions=5000, clicks=0, ctr_pct=0.0, spend_usd=25.0)
    )
    assert cls == "failing"
    assert action == "pause"
    assert "Zero clicks" in rationale


def test_classify_underperforming_low_ctr():
    from src.feedback_agent import _classify_campaign_row
    cls, action, rationale, signal = _classify_campaign_row(
        _row(impressions=10000, clicks=50, ctr_pct=0.5, spend_usd=40.0, cpa_usd=20.0)
    )
    assert cls == "underperforming"
    assert action == "replace"
    assert "CTR" in rationale


def test_classify_underperforming_high_cpa():
    from src.feedback_agent import _classify_campaign_row
    cls, action, rationale, signal = _classify_campaign_row(
        _row(impressions=10000, clicks=200, ctr_pct=2.0, spend_usd=300.0, cpa_usd=75.0)
    )
    assert cls == "underperforming"
    assert action == "replace"
    assert "CPA" in rationale


def test_classify_working_with_acceptable_cpa():
    from src.feedback_agent import _classify_campaign_row
    cls, action, rationale, signal = _classify_campaign_row(
        _row(impressions=10000, clicks=200, ctr_pct=2.0, spend_usd=200.0, cpa_usd=40.0)
    )
    assert cls == "working"
    assert action == "keep"


def test_classify_working_with_null_cpa():
    """Clicks above the floor, CTR above the floor, no applications yet (cpa=None)."""
    from src.feedback_agent import _classify_campaign_row
    cls, action, _rationale, _signal = _classify_campaign_row(
        _row(impressions=10000, clicks=200, ctr_pct=2.0, spend_usd=200.0, cpa_usd=None)
    )
    assert cls == "working"
    assert action == "keep"


# ── FeedbackAgent.recommend_actions ───────────────────────────────────────────

def test_recommend_actions_persists_each_row(monkeypatch):
    """recommend_actions should walk get_active_campaigns and call
    upsert_recommendation once per row when persist=True."""
    from src import campaign_registry, ui_decisions, feedback_agent

    fake_rows = [
        _row(platform_campaign_id="urn:li:sponsoredCampaign:111",
             impressions=10000, clicks=200, ctr_pct=2.0, spend_usd=200.0, cpa_usd=40.0),
        _row(platform_campaign_id="urn:li:sponsoredCampaign:222",
             impressions=5000, clicks=0, ctr_pct=0.0, spend_usd=25.0),
        _row(platform_campaign_id="urn:li:sponsoredCampaign:333",
             impressions=500, clicks=0, ctr_pct=0.0, spend_usd=5.0),
    ]
    monkeypatch.setattr(campaign_registry, "get_active_campaigns", lambda smart_ramp_id=None: fake_rows)

    captured: list[dict] = []
    def fake_upsert(**kwargs):
        captured.append(kwargs)
        return None
    monkeypatch.setattr(ui_decisions, "upsert_recommendation", fake_upsert)

    agent = feedback_agent.FeedbackAgent(redash_client=MagicMock())
    recs = agent.recommend_actions("GMR-TEST")

    assert len(recs) == 3
    classifications = sorted(r["classification"] for r in recs)
    assert classifications == ["failing", "insufficient_data", "working"]
    actions = sorted(r["action"] for r in recs)
    assert actions == ["keep", "keep", "pause"]
    # persistence was attempted for each row
    assert len(captured) == 3
    # rationale + metric_signal are always populated
    for r in recs:
        assert r["rationale"]
        assert isinstance(r["metric_signal"], dict)
        assert "spend_usd" in r["metric_signal"]


def test_recommend_actions_skips_rows_without_campaign_id(monkeypatch):
    """Rows missing both platform_campaign_id and linkedin_campaign_urn are
    skipped — no upsert attempted."""
    from src import campaign_registry, ui_decisions, feedback_agent

    fake_rows = [
        _row(platform_campaign_id="", linkedin_campaign_urn="", impressions=5000, spend_usd=20.0),
    ]
    monkeypatch.setattr(campaign_registry, "get_active_campaigns", lambda smart_ramp_id=None: fake_rows)
    upsert_calls: list = []
    monkeypatch.setattr(ui_decisions, "upsert_recommendation",
                        lambda **kw: upsert_calls.append(kw))

    agent = feedback_agent.FeedbackAgent(redash_client=MagicMock())
    recs = agent.recommend_actions("GMR-TEST")
    assert recs == []
    assert upsert_calls == []


def test_recommend_actions_no_rows(monkeypatch):
    from src import campaign_registry, feedback_agent
    monkeypatch.setattr(campaign_registry, "get_active_campaigns", lambda smart_ramp_id=None: [])
    agent = feedback_agent.FeedbackAgent(redash_client=MagicMock())
    assert agent.recommend_actions("GMR-EMPTY") == []


def test_recommend_actions_persist_failure_is_non_fatal(monkeypatch):
    """If upsert raises, recommend_actions should swallow + continue (the
    handoff explicitly requires this — Postgres outage cannot block the
    classifier loop)."""
    from src import campaign_registry, ui_decisions, feedback_agent

    fake_rows = [
        _row(platform_campaign_id="urn:li:sponsoredCampaign:1",
             impressions=10000, clicks=200, ctr_pct=2.0, spend_usd=200.0, cpa_usd=40.0),
    ]
    monkeypatch.setattr(campaign_registry, "get_active_campaigns", lambda smart_ramp_id=None: fake_rows)

    def boom(**kwargs):
        raise RuntimeError("postgres down")
    monkeypatch.setattr(ui_decisions, "upsert_recommendation", boom)

    agent = feedback_agent.FeedbackAgent(redash_client=MagicMock())
    recs = agent.recommend_actions("GMR-TEST")
    assert len(recs) == 1
    assert recs[0]["classification"] == "working"


# ── ui_decisions: enum validation + DB-down handling ─────────────────────────

def test_upsert_recommendation_rejects_invalid_classification():
    from src import ui_decisions
    import pytest
    with pytest.raises(ValueError):
        ui_decisions.upsert_recommendation(
            ramp_id="GMR-X", campaign_urn="urn:li:1",
            classification="bogus", action="keep",
        )


def test_upsert_recommendation_rejects_invalid_action():
    from src import ui_decisions
    import pytest
    with pytest.raises(ValueError):
        ui_decisions.upsert_recommendation(
            ramp_id="GMR-X", campaign_urn="urn:li:1",
            classification="working", action="bogus",
        )


def test_upsert_recommendation_swallows_db_unavailable(monkeypatch):
    """When _connect raises UIDecisionsUnavailable, upsert returns None
    (best-effort persistence — feedback agent keeps iterating)."""
    from src import ui_decisions
    def boom():
        raise ui_decisions.UIDecisionsUnavailable("no DATABASE_URL")
    monkeypatch.setattr(ui_decisions, "_connect", boom)
    result = ui_decisions.upsert_recommendation(
        ramp_id="GMR-X", campaign_urn="urn:li:1",
        classification="working", action="keep",
    )
    assert result is None


def test_set_recommendation_decision_validates_decision():
    from src import ui_decisions
    import pytest
    with pytest.raises(ValueError):
        ui_decisions.set_recommendation_decision(1, "bogus")


def test_list_recommendations_validates_decision_filter():
    from src import ui_decisions
    import pytest
    with pytest.raises(ValueError):
        ui_decisions.list_recommendations("GMR-X", decision="bogus")

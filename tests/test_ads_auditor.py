"""Unit tests for src.ads_auditor — 21d filter, per-platform grouping,
Claude call shape, error stubs, and JSON parsing edge cases."""
from __future__ import annotations

import datetime as dt
import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import ads_auditor


# ── Fixtures ────────────────────────────────────────────────────────────


@pytest.fixture
def now_utc() -> dt.datetime:
    return dt.datetime(2026, 5, 26, 17, 0, 0, tzinfo=dt.timezone.utc)


@pytest.fixture
def registry_rows() -> list[dict]:
    """Mixed registry: 2 LinkedIn + 2 Meta + 1 Google in-window, 2 stale."""
    return [
        {
            "platform": "linkedin", "platform_campaign_id": "li-001",
            "campaign_name": "agent_li_recent_1", "created_at": "2026-05-20 12:00 UTC",
            "status": "active", "cohort_signature": "skills__python",
            "geos": ["US", "CA"], "angle": "A", "campaign_type": "static",
            "impressions": 12_000, "clicks": 240, "spend_usd": 180.50,
            "ctr_pct": 2.0, "cpc_usd": 0.75, "applications": 6, "cpa_usd": 30.08,
        },
        {
            "platform": "linkedin", "platform_campaign_id": "li-002",
            "campaign_name": "agent_li_recent_2", "created_at": "2026-05-15 18:30 UTC",
            "status": "active", "impressions": 8_000, "spend_usd": 120.00,
        },
        {
            "platform": "meta", "platform_campaign_id": "120246409656140257",
            "campaign_name": "agent_meta_recent_1", "created_at": "2026-05-21 20:06 UTC",
            "status": "active", "cohort_signature": "fields_of_study__electronics_engineering",
            "geo_cluster_label": "Latin American", "angle": "C",
            "impressions": 0, "spend_usd": 0,
        },
        {
            "platform": "meta", "platform_campaign_id": "120246409656140258",
            "campaign_name": "agent_meta_recent_2", "created_at": "2026-05-22 09:00 UTC",
            "status": "paused", "impressions": 5_000, "spend_usd": 45.20,
        },
        {
            "platform": "google", "platform_campaign_id": "g-001",
            "campaign_name": "agent_google_recent_1", "created_at": "2026-05-23 10:00 UTC",
            "status": "active", "impressions": 3_000, "spend_usd": 75.00,
        },
        # Stale rows — outside 21-day window relative to now=2026-05-26
        {
            "platform": "linkedin", "platform_campaign_id": "li-stale-1",
            "campaign_name": "stale_li", "created_at": "2026-04-01 12:00 UTC",
            "status": "archived", "spend_usd": 99999.99,
        },
        {
            "platform": "meta", "platform_campaign_id": "meta-stale-1",
            "campaign_name": "stale_meta", "created_at": "2026-04-30 12:00 UTC",
        },
    ]


@pytest.fixture
def registry_path(tmp_path, registry_rows) -> Path:
    p = tmp_path / "campaign_registry.json"
    p.write_text(json.dumps(registry_rows))
    return p


@pytest.fixture
def stub_claude_fn() -> MagicMock:
    """A call_claude stub that returns valid JSON for every platform."""
    return MagicMock(return_value=json.dumps({
        "health_score": 72,
        "executive_summary": "Healthy delivery; CTR is on the lower end for new campaigns.",
        "top_issues": ["Three campaigns in learning phase still", "Frequency cap not yet hit"],
        "top_recommendations": ["Wait 7 days before adjusting bids", "Check pixel firing"],
    }))


# ── Tests ───────────────────────────────────────────────────────────────


def test_filter_recent_drops_stale_rows(registry_rows, now_utc):
    recent = ads_auditor._filter_recent(registry_rows, 21, now_utc=now_utc)
    names = [r["campaign_name"] for r in recent]
    assert "stale_li" not in names
    assert "stale_meta" not in names
    assert len(recent) == 5


def test_filter_recent_at_boundary_inclusive(now_utc):
    # 21d cutoff = 2026-05-05 17:00 UTC. Row at exactly that time stays.
    boundary_row = {"created_at": "2026-05-05 17:00 UTC", "platform": "linkedin"}
    just_before  = {"created_at": "2026-05-05 16:59 UTC", "platform": "linkedin"}
    rows = [boundary_row, just_before]
    recent = ads_auditor._filter_recent(rows, 21, now_utc=now_utc)
    assert boundary_row in recent
    assert just_before not in recent


def test_group_by_platform_includes_only_supported(registry_rows, now_utc):
    recent = ads_auditor._filter_recent(registry_rows, 21, now_utc=now_utc)
    by_platform = ads_auditor._group_by_platform(recent)
    assert set(by_platform.keys()) == {"linkedin", "meta", "google"}
    assert len(by_platform["linkedin"]) == 2
    assert len(by_platform["meta"]) == 2
    assert len(by_platform["google"]) == 1


def test_group_by_platform_infers_linkedin_from_urn(now_utc):
    rows = [{
        "platform": "", "linkedin_campaign_urn": "urn:li:sponsoredCampaign:123",
        "created_at": "2026-05-20 12:00 UTC",
    }]
    by_platform = ads_auditor._group_by_platform(rows)
    assert "linkedin" in by_platform


def test_parse_created_at_handles_known_formats():
    parse = ads_auditor._parse_created_at
    assert parse("2026-05-20 12:00 UTC") is not None
    assert parse("2026-05-20 12:00:00") is not None
    assert parse("2026-05-20T12:00:00") is not None
    assert parse("") is None
    assert parse(None) is None
    assert parse("garbage") is None


def test_run_weekly_audit_happy_path(registry_path, stub_claude_fn, now_utc):
    result = ads_auditor.run_weekly_audit(
        lookback_days=21,
        registry_path=registry_path,
        now_utc=now_utc,
        call_claude_fn=stub_claude_fn,
    )
    assert result["lookback_days"] == 21
    assert result["total_audited"] == 5
    assert set(result["platforms"].keys()) == {"linkedin", "meta", "google"}
    # Each populated platform got one Claude call
    assert stub_claude_fn.call_count == 3

    li = result["platforms"]["linkedin"]
    assert li["campaigns_audited"] == 2
    assert li["health_score"] == 72
    assert li["total_spend_usd"] == 300.50
    assert li["total_impressions"] == 20_000


def test_empty_platform_skips_claude_call(tmp_path, stub_claude_fn, now_utc):
    """If a platform has 0 in-window rows, don't burn a Claude call."""
    empty_registry = tmp_path / "empty_registry.json"
    empty_registry.write_text(json.dumps([
        {"platform": "linkedin", "created_at": "2026-05-20 12:00 UTC"},
    ]))
    result = ads_auditor.run_weekly_audit(
        lookback_days=21,
        registry_path=empty_registry,
        now_utc=now_utc,
        call_claude_fn=stub_claude_fn,
    )
    # Only LinkedIn got a Claude call; Meta + Google got empty stubs.
    assert stub_claude_fn.call_count == 1
    assert result["platforms"]["meta"]["campaigns_audited"] == 0
    assert result["platforms"]["meta"]["executive_summary"] == "No new campaigns this period."
    assert result["platforms"]["google"]["health_score"] is None


def test_claude_failure_produces_error_stub(registry_path, now_utc):
    def boom(*_a, **_kw):
        raise RuntimeError("anthropic API exploded")
    result = ads_auditor.run_weekly_audit(
        lookback_days=21,
        registry_path=registry_path,
        now_utc=now_utc,
        call_claude_fn=boom,
    )
    for platform in ("linkedin", "meta", "google"):
        finding = result["platforms"][platform]
        assert finding["health_score"] is None
        assert "Audit failed" in finding["executive_summary"]
        assert any("Audit error" in i for i in finding["top_issues"])


def test_parse_claude_response_strips_markdown_fences():
    parse = ads_auditor._parse_claude_response
    assert parse('```json\n{"health_score": 80}\n```') == {"health_score": 80}
    assert parse('```\n{"health_score": 80}\n```') == {"health_score": 80}
    assert parse('{"health_score": 80}') == {"health_score": 80}


def test_parse_claude_response_extracts_first_json_object():
    """Sometimes Claude prepends a sentence despite instructions."""
    text = 'Here is the audit: {"health_score": 50, "top_issues": ["x"]}'
    out = ads_auditor._parse_claude_response(text)
    assert out["health_score"] == 50


def test_parse_claude_response_returns_empty_on_garbage():
    assert ads_auditor._parse_claude_response("") == {}
    assert ads_auditor._parse_claude_response("not json at all") == {}


def test_missing_registry_returns_empty_audit(tmp_path, stub_claude_fn, now_utc):
    missing = tmp_path / "nope.json"
    result = ads_auditor.run_weekly_audit(
        lookback_days=21,
        registry_path=missing,
        now_utc=now_utc,
        call_claude_fn=stub_claude_fn,
    )
    assert result["total_audited"] == 0
    assert stub_claude_fn.call_count == 0
    for finding in result["platforms"].values():
        assert finding["campaigns_audited"] == 0


def test_slim_row_drops_empty_fields(registry_rows):
    slimmed = ads_auditor._slim_row(registry_rows[0])
    # Empty deprecation_reason was missing from input — should not appear
    assert "deprecation_reason" not in slimmed
    # Real values survive
    assert slimmed["platform_campaign_id"] == "li-001"
    assert slimmed["impressions"] == 12_000


def test_audit_platform_includes_prompt_in_system(registry_rows, stub_claude_fn, now_utc):
    """The vendored skill prompt MUST be in the system message — that's
    what differentiates per-platform audit logic."""
    recent = ads_auditor._filter_recent(registry_rows, 21, now_utc=now_utc)
    by_platform = ads_auditor._group_by_platform(recent)
    ads_auditor._audit_platform("meta", by_platform["meta"], stub_claude_fn)
    call = stub_claude_fn.call_args
    system_arg = call.kwargs.get("system", "")
    assert "AUDIT FRAMEWORK" in system_arg
    # The vendored ads-meta.md should contribute something distinctive
    assert "meta" in system_arg.lower()

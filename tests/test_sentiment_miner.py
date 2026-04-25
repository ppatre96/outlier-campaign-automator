"""Unit tests for src/sentiment_miner - Phase 2.5 V2 (FEED-17, FEED-18, FEED-19).

All HTTP + LLM calls are mocked. No live network access.
"""
from __future__ import annotations

import json
import re as _re
from pathlib import Path

import pytest


# ── FEED-17: fetch isolation ─────────────────────────────────────────────────

def test_fetcher_isolation(tmp_path, monkeypatch):
    """One source failing (e.g. Reddit 500) does NOT block other sources."""
    from src import sentiment_miner as sm

    def _raise(*_a, **_kw):
        raise RuntimeError("reddit 500")

    monkeypatch.setattr(sm, "fetch_reddit",     _raise)
    monkeypatch.setattr(sm, "fetch_trustpilot", lambda **kw: [
        {"source": "trustpilot", "url": "http://x", "title": "t",
         "body": "b", "ts": 1},
    ])
    monkeypatch.setattr(sm, "fetch_glassdoor", lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_discourse", lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_zendesk",   lambda: [])
    monkeypatch.setattr(sm, "fetch_intercom",  lambda: [])
    monkeypatch.setattr(sm, "extract_themes",  lambda snippets, model=None: [])
    monkeypatch.setattr(sm, "_CALLOUTS_PATH",  tmp_path / "callouts.json")
    monkeypatch.setattr(sm, "_RAW_DIR",        tmp_path / "raw")

    result = sm.run()

    skipped_sources = [s["source"] for s in result["sources_skipped"]]
    assert "reddit" in skipped_sources, (
        f"reddit should be in sources_skipped, got {result['sources_skipped']}"
    )
    assert "trustpilot" in result["sources_queried"], (
        "trustpilot should have succeeded"
    )
    # Output file still written despite one failure.
    assert (tmp_path / "callouts.json").exists()
    payload = json.loads((tmp_path / "callouts.json").read_text())
    assert payload["sources_queried"] == result["sources_queried"]
    assert payload["sources_skipped"] == result["sources_skipped"]


# ── FEED-18: credential gating ───────────────────────────────────────────────

def test_zendesk_skip_no_creds(monkeypatch):
    """Zendesk fetcher returns [] when credentials are empty."""
    from src import sentiment_miner as sm
    import config

    monkeypatch.setattr(config, "ZENDESK_SUBDOMAIN", "")
    monkeypatch.setattr(config, "ZENDESK_EMAIL", "")
    monkeypatch.setattr(config, "ZENDESK_API_TOKEN", "")

    result = sm.fetch_zendesk()
    assert result == []


def test_intercom_skip_no_creds(monkeypatch):
    """Intercom fetcher returns [] when access token is empty."""
    from src import sentiment_miner as sm
    import config

    monkeypatch.setattr(config, "INTERCOM_ACCESS_TOKEN", "")

    result = sm.fetch_intercom()
    assert result == []


# ── FEED-19: evidence threshold ──────────────────────────────────────────────

def test_evidence_threshold(tmp_path, monkeypatch):
    """Themes with < SENTIMENT_THEME_MIN_EVIDENCE quotes are NOT in callouts.json."""
    from src import sentiment_miner as sm
    import config

    monkeypatch.setattr(config, "SENTIMENT_THEME_MIN_EVIDENCE", 3)
    monkeypatch.setattr(sm, "fetch_reddit",     lambda subs=None, limit=100: [
        {"source": "reddit_outlier_ai", "url": "http://r/1", "title": "t",
         "body": "b", "ts": 1},
    ])
    monkeypatch.setattr(sm, "fetch_trustpilot", lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_glassdoor",  lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_discourse",  lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_zendesk",    lambda: [])
    monkeypatch.setattr(sm, "fetch_intercom",   lambda: [])
    monkeypatch.setattr(sm, "extract_themes",   lambda snippets, model=None: [
        {"theme": "Slow payment release", "sentiment": "negative",
         "evidence_quotes": ["q1", "q2", "q3", "q4"],
         "source_indices": [0]},
        {"theme": "Confusing project guidelines", "sentiment": "negative",
         "evidence_quotes": ["q1", "q2"],
         "source_indices": [0]},
    ])
    monkeypatch.setattr(sm, "_CALLOUTS_PATH", tmp_path / "callouts.json")
    monkeypatch.setattr(sm, "_RAW_DIR",       tmp_path / "raw")

    sm.run()

    data = json.loads((tmp_path / "callouts.json").read_text())
    surfaced_labels = [t["theme"] for t in data["themes"]]
    assert "Slow payment release" in surfaced_labels
    assert "Confusing project guidelines" not in surfaced_labels, (
        "Sub-threshold theme leaked into callouts.json"
    )

    # Sub-threshold themes should land in the raw dump for postmortem.
    raw_files = list((tmp_path / "raw").glob("*.json"))
    assert raw_files, "raw dump not written"
    raw = json.loads(raw_files[0].read_text())
    raw_labels = [t["theme"] for t in raw.get("themes_below_threshold", [])]
    assert "Confusing project guidelines" in raw_labels


# ── FEED-19 + CLAUDE.md: vocabulary scrub ────────────────────────────────────

def test_no_banned_vocab(tmp_path, monkeypatch):
    """Theme labels in callouts.json NEVER contain banned vocabulary."""
    from src import sentiment_miner as sm
    import config

    monkeypatch.setattr(config, "SENTIMENT_THEME_MIN_EVIDENCE", 1)
    monkeypatch.setattr(sm, "fetch_reddit",     lambda subs=None, limit=100: [
        {"source": "r", "url": "http://r", "title": "t", "body": "b", "ts": 1},
    ])
    monkeypatch.setattr(sm, "fetch_trustpilot", lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_glassdoor",  lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_discourse",  lambda **kw: [])
    monkeypatch.setattr(sm, "fetch_zendesk",    lambda: [])
    monkeypatch.setattr(sm, "fetch_intercom",   lambda: [])
    # LLM deliberately returns BANNED vocabulary - scrub must replace before write.
    monkeypatch.setattr(sm, "extract_themes",   lambda snippets, model=None: [
        {"theme": "Low compensation per job", "sentiment": "negative",
         "evidence_quotes": ["a", "b", "c", "d"], "source_indices": [0]},
        {"theme": "Poor performance after interview", "sentiment": "negative",
         "evidence_quotes": ["a", "b", "c", "d"], "source_indices": [0]},
    ])
    monkeypatch.setattr(sm, "_CALLOUTS_PATH", tmp_path / "callouts.json")
    monkeypatch.setattr(sm, "_RAW_DIR",       tmp_path / "raw")

    sm.run()

    data = json.loads((tmp_path / "callouts.json").read_text())
    banned = _re.compile(
        r"\b(compensation|job|role|position|interview|performance|"
        r"bonus|project rate|required)\b",
        _re.IGNORECASE,
    )
    assert data["themes"], "expected at least one surfaced theme"
    for theme in data["themes"]:
        assert not banned.search(theme["theme"]), (
            f"banned token in theme label: {theme['theme']!r}"
        )
        assert not banned.search(theme.get("directive_for_brief", "")), (
            f"banned token in directive: {theme.get('directive_for_brief')!r}"
        )

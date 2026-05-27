"""Unit tests for src.google_custom_intent — keyword cleaning, audience
creation, caching, registry I/O."""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import google_custom_intent as gci


# Patch the Google SDK call seam — we never import google.ads.googleads in tests.
@pytest.fixture(autouse=True)
def _stub_sdk_create(monkeypatch):
    calls: list = []
    def fake_create(client, cohort_signature, keywords):
        calls.append((cohort_signature, tuple(keywords)))
        return f"customers/8840244968/audiences/{abs(hash(cohort_signature)) % 10**10}"
    monkeypatch.setattr(gci, "_create_custom_intent_on_google", fake_create)
    fake_create.calls = calls   # type: ignore[attr-defined]
    return fake_create


def _client() -> MagicMock:
    c = MagicMock()
    c._ensure_client = MagicMock()
    c._customer_id_str = "8840244968"
    return c


# ── Keyword cleaning ────────────────────────────────────────────────────


def test_clean_keywords_dedupes_and_normalizes():
    out = gci._clean_keywords(["Python", "python", "  Python ", "Java"])
    assert out == ["Python", "Java"]


def test_clean_keywords_drops_short():
    out = gci._clean_keywords(["a", "ab", "abc", "data scientist"])
    # min length is 3 — "a" and "ab" dropped, "abc" kept
    assert "abc" in out
    assert "data scientist" in out
    assert "a"  not in out
    assert "ab" not in out


def test_clean_keywords_rejects_invalid_chars():
    out = gci._clean_keywords(["python <script>", "valid term", "@admin", "data-science"])
    # Only valid-chars terms survive
    assert "valid term" in out
    assert "data-science" in out
    assert "python <script>" not in out
    assert "@admin" not in out


def test_clean_keywords_truncates_to_max():
    raw = [f"keyword{i}" for i in range(500)]
    out = gci._clean_keywords(raw)
    assert len(out) <= gci._MAX_KEYWORDS


def test_clean_keywords_empty_returns_empty():
    assert gci._clean_keywords([]) == []
    assert gci._clean_keywords(None) == []        # type: ignore[arg-type]
    assert gci._clean_keywords([None, "", "  "]) == []  # type: ignore[list-item]


# ── Keyword extraction from cohort.rules ────────────────────────────────


def test_extract_keywords_from_rules_strips_prefix():
    rules = [
        ("skills__python",                "python"),
        ("job_titles_norm__data_scientist", "data scientist"),
        ("fields_of_study__computer_science", "computer science"),
    ]
    kw = gci._extract_keywords_from_rules(rules)
    assert "python" in kw
    assert "data scientist" in kw
    assert "computer science" in kw


def test_extract_keywords_from_rules_handles_plain_strings():
    """Some cohorts may pass rules as bare strings rather than tuples."""
    kw = gci._extract_keywords_from_rules(["skills__python", "skills__java"])
    assert "python" in kw
    assert "java" in kw


def test_extract_keywords_from_rules_no_prefix_passthrough():
    """If no `__` separator, treat as raw keyword."""
    kw = gci._extract_keywords_from_rules([("freelance",  "freelance")])
    assert kw == ["freelance"]


# ── get_or_create_custom_intent ─────────────────────────────────────────


def test_first_call_creates_audience(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    result = gci.get_or_create_custom_intent(
        "skills__python", ["python", "django"],
        registry_path=reg, google_client=_client(),
    )
    assert result.newly_created
    assert result.audience_resource.startswith("customers/8840244968/audiences/")
    assert result.keyword_count == 2
    assert len(_stub_sdk_create.calls) == 1


def test_second_call_returns_cached(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    a = gci.get_or_create_custom_intent("sig1", ["python"], registry_path=reg, google_client=_client())
    b = gci.get_or_create_custom_intent("sig1", ["python", "extra_kw_ignored"], registry_path=reg, google_client=_client())
    assert a.audience_resource == b.audience_resource
    assert b.newly_created is False
    # SDK called ONCE — cache hit on second call (keywords ignored on cache hit
    # because we don't have a "modify audience" path; new keywords would need
    # a new cohort_signature).
    assert len(_stub_sdk_create.calls) == 1


def test_different_signatures_different_audiences(tmp_path, _stub_sdk_create):
    """Different cohort_signatures produce different audience resources.

    Note: keywords must pass _clean_keywords' character whitelist
    (alphanumeric + space/hyphen/slash only — no underscores). Real cohort
    keywords are search-query-shaped, e.g. "data scientist", not "a_term".
    """
    reg = tmp_path / "registry.json"
    a = gci.get_or_create_custom_intent("sig_A", ["python"], registry_path=reg, google_client=_client())
    b = gci.get_or_create_custom_intent("sig_B", ["java"],   registry_path=reg, google_client=_client())
    assert a.audience_resource != b.audience_resource


def test_rejects_empty_signature(tmp_path):
    reg = tmp_path / "registry.json"
    with pytest.raises(ValueError):
        gci.get_or_create_custom_intent("", ["python"], registry_path=reg, google_client=_client())
    with pytest.raises(ValueError):
        gci.get_or_create_custom_intent("   ", ["python"], registry_path=reg, google_client=_client())


def test_rejects_no_valid_keywords(tmp_path):
    reg = tmp_path / "registry.json"
    with pytest.raises(ValueError):
        gci.get_or_create_custom_intent("sig", [], registry_path=reg, google_client=_client())
    with pytest.raises(ValueError):
        gci.get_or_create_custom_intent("sig", ["a", "b"], registry_path=reg, google_client=_client())   # all too short


def test_registry_stores_keyword_preview(tmp_path, _stub_sdk_create):
    """The registry stores a small preview of keywords for debugging — not the full list."""
    reg = tmp_path / "registry.json"
    keywords = [f"kw{i}" for i in range(20)]
    gci.get_or_create_custom_intent("sig_long", keywords, registry_path=reg, google_client=_client())
    data = json.loads(reg.read_text())
    entry = data["registry"]["sig_long"]
    assert entry["keyword_count"] == 20
    assert len(entry["keywords_preview"]) == 10   # Only first 10 stored


def test_corrupted_registry_resets(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    reg.write_text("{not valid json")
    r = gci.get_or_create_custom_intent("sig", ["python"], registry_path=reg, google_client=_client())
    assert r.newly_created


# ── resolve_custom_intent_for_cohort (high-level wrapper) ───────────────


def test_resolve_for_cohort_happy_path(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    out = gci.resolve_custom_intent_for_cohort(
        "skills__python", [("skills__python", "python"), ("job_titles_norm__data_engineer", "data engineer")],
        registry_path=reg, google_client=_client(),
    )
    assert out is not None
    assert out.startswith("customers/")


def test_resolve_for_cohort_empty_rules_returns_none(tmp_path):
    reg = tmp_path / "registry.json"
    out = gci.resolve_custom_intent_for_cohort(
        "skills__python", [],
        registry_path=reg, google_client=_client(),
    )
    assert out is None


def test_resolve_for_cohort_api_failure_returns_none_not_raises(tmp_path, monkeypatch):
    def boom(*_a, **_kw):
        raise RuntimeError("Google rejected the request")
    monkeypatch.setattr(gci, "_create_custom_intent_on_google", boom)
    reg = tmp_path / "registry.json"
    out = gci.resolve_custom_intent_for_cohort(
        "sig_fail", [("skills__python", "python")],
        registry_path=reg, google_client=_client(),
    )
    assert out is None

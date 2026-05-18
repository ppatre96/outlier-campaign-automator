"""Tests for the LinkedIn typeahead self-heal path in UrnResolver.

When the cached URN sheet doesn't include a value (UX-Engineer-style miss),
UrnResolver should fall back to LinkedIn's live typeahead endpoint and
populate the in-memory cache so subsequent resolves in the same session
hit the fast path. Without a linkedin_client, the resolver must still
return None gracefully (backwards-compat).
"""
import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.linkedin_urn import UrnResolver


def _sheets_with(tab_rows: dict[str, list[dict]]):
    """MagicMock SheetsClient whose `read_urn_tab(tab)` returns rows."""
    sheets = MagicMock()
    sheets.read_urn_tab = lambda tab: tab_rows.get(tab, [])
    return sheets


# ── 1. Cache hit short-circuits typeahead ─────────────────────────────────────

def test_cache_hit_skips_typeahead():
    """A value already present in the cached URN sheet must resolve from
    cache; the typeahead endpoint should never be called."""
    sheets = _sheets_with({"Titles": [{"name": "Data Scientist", "urn": "urn:li:title:9"}]})
    li = MagicMock()
    li.typeahead_facet = MagicMock(return_value=[])

    resolver = UrnResolver(sheets, linkedin_client=li)
    urn = resolver.resolve("titles", "Data Scientist")

    assert urn == "urn:li:title:9"
    li.typeahead_facet.assert_not_called()


# ── 2. Cache miss + no client → None, no crash ────────────────────────────────

def test_cache_miss_without_client_returns_none():
    """Existing callers that pass only `sheets` (no linkedin_client) must keep
    working — a miss returns None instead of attempting typeahead."""
    sheets = _sheets_with({"Titles": [{"name": "Cardiologist", "urn": "urn:li:title:1"}]})

    resolver = UrnResolver(sheets)  # linkedin_client=None by default
    urn = resolver.resolve("titles", "Quantum Cryptographer")

    assert urn is None


# ── 3. Cache miss + typeahead hit → return URN + self-heal cache ──────────────

def test_typeahead_fallback_populates_cache():
    """On a cache miss with a linkedin_client, the resolver must use the
    typeahead result and append it to the in-memory cache so the next
    resolve() in this session hits the fast path."""
    sheets = _sheets_with({"Titles": [{"name": "Cardiologist", "urn": "urn:li:title:1"}]})
    li = MagicMock()
    li.typeahead_facet = MagicMock(return_value=[
        {"name": "Quantum Cryptographer", "urn": "urn:li:title:42"},
    ])

    resolver = UrnResolver(sheets, linkedin_client=li)
    urn = resolver.resolve("titles", "Quantum Cryptographer")

    assert urn == "urn:li:title:42"
    li.typeahead_facet.assert_called_once_with("titles", "Quantum Cryptographer", limit=10)

    # Second resolve of the same value should hit the cache.
    li.typeahead_facet.reset_mock()
    urn2 = resolver.resolve("titles", "Quantum Cryptographer")
    assert urn2 == "urn:li:title:42"
    li.typeahead_facet.assert_not_called()


# ── 4. Cache miss + empty typeahead → None ────────────────────────────────────

def test_typeahead_returns_empty_returns_none():
    sheets = _sheets_with({"Titles": [{"name": "Cardiologist", "urn": "urn:li:title:1"}]})
    li = MagicMock()
    li.typeahead_facet = MagicMock(return_value=[])

    resolver = UrnResolver(sheets, linkedin_client=li)
    urn = resolver.resolve("titles", "Quantum Cryptographer")

    assert urn is None


# ── 5. Typeahead URL format — RAW locale tuple, encoded facet URN ─────────────

def test_typeahead_url_format():
    """LinkedIn /rest/adTargetingEntities requires the locale tuple in RAW
    Rest.li form `(language:en,country:US)` with NO percent-encoding of the
    colons or parens. The facet URN must be percent-encoded. Regressing on
    either of these causes PARAM_INVALID 400 from LinkedIn."""
    from src.linkedin_api import LinkedInClient

    client = LinkedInClient(token="TEST_TOKEN")

    captured = {}
    def fake_req(method, url, **kwargs):
        captured["url"] = url
        captured["method"] = method
        resp = MagicMock()
        resp.ok = True
        resp.json = lambda: {"elements": [{"name": "UX Engineer", "urn": "urn:li:title:42"}]}
        return resp

    client._req = fake_req
    out = client.typeahead_facet("titles", "UX Engineer", limit=5)

    assert out == [{"name": "UX Engineer", "urn": "urn:li:title:42"}]
    assert captured["method"] == "GET"

    url = captured["url"]
    assert "q=typeahead" in url
    # Facet URN must be percent-encoded.
    assert "facet=urn%3Ali%3AadTargetingFacet%3Atitles" in url
    # Query value percent-encoded (space → %20).
    assert "query=UX%20Engineer" in url
    assert "queryVersion=QUERY_USES_URNS" in url
    # Locale stays as RAW Rest.li tuple — colons + parens must NOT be encoded.
    assert "locale=(language:en,country:US)" in url


# ── 6. Unknown facet → None, no typeahead call ────────────────────────────────

def test_unknown_facet_returns_none():
    sheets = _sheets_with({})
    li = MagicMock()
    li.typeahead_facet = MagicMock()

    resolver = UrnResolver(sheets, linkedin_client=li)
    urn = resolver.resolve("not_a_real_facet", "anything")

    assert urn is None
    li.typeahead_facet.assert_not_called()

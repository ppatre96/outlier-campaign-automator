"""Unit tests for the LinkedIn audienceCounts targeting builder.

Verifies the URN-encoding contract: structural Rest.li delimiters stay raw
(parens, commas, colons separating clauses) while URN-internal characters
get percent-encoded. Mismatching this causes LinkedIn to 400 the request.
"""
import os
import sys
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.linkedin_api import (
    LinkedInClient,
    _build_restli_targeting,
    _encode_urn,
)


class TestEncodeUrn:
    def test_url_unsafe_chars_encoded(self):
        out = _encode_urn("urn:li:skill:1")
        assert out == "urn%3Ali%3Askill%3A1"

    def test_parens_in_urn_encoded(self):
        out = _encode_urn("urn:li:staffCountRange:(10001,20000)")
        # Every URN-internal `(`, `)`, `,`, `:` must be encoded.
        assert "%28" in out and "%29" in out and "%2C" in out
        assert "(" not in out and ")" not in out and "," not in out

    def test_alphanumeric_unchanged(self):
        assert _encode_urn("abc123") == "abc123"

    def test_existing_percents_preserved(self):
        out = _encode_urn("a%b")
        # `%` is encoded to `%25` first so it doesn't get double-decoded server-side.
        assert "%25" in out


class TestBuildRestliTargeting:
    def test_single_facet_single_value(self):
        out = _build_restli_targeting({"skills": ["urn:li:skill:1"]})
        # Structural pieces stay raw; URN gets encoded.
        assert out == (
            "(include:(and:List("
            "(or:(urn%3Ali%3AadTargetingFacet%3Askills:List(urn%3Ali%3Askill%3A1)))"
            ")))"
        )

    def test_multiple_values_use_raw_comma_separator(self):
        out = _build_restli_targeting({"skills": ["urn:li:skill:1", "urn:li:skill:2"]})
        # Comma between URNs is structural (separator) — NOT encoded.
        assert "List(urn%3Ali%3Askill%3A1,urn%3Ali%3Askill%3A2)" in out

    def test_empty_value_lists_are_dropped(self):
        out = _build_restli_targeting({
            "skills": ["urn:li:skill:1"],
            "titles": [],
        })
        assert "skills" in out and "titles" not in out

    def test_full_facet_urn_passes_through(self):
        out = _build_restli_targeting({
            "urn:li:adTargetingFacet:industries": ["urn:li:industry:9"],
        })
        assert "urn%3Ali%3AadTargetingFacet%3Aindustries" in out

    def test_exclude_block_is_appended(self):
        out = _build_restli_targeting(
            include={"skills": ["urn:li:skill:1"]},
            exclude={"seniorities": ["urn:li:seniority:1"]},
        )
        # Exclude uses or:(...) directly — no and:List wrapper.
        assert ",exclude:(or:(" in out
        assert "urn%3Ali%3AadTargetingFacet%3Aseniorities" in out

    def test_no_exclude_yields_no_exclude_block(self):
        out = _build_restli_targeting({"skills": ["urn:li:skill:1"]}, exclude={})
        assert "exclude:" not in out


class TestGetAudienceCountWiring:
    def test_empty_include_skips_api_call(self):
        client = LinkedInClient.__new__(LinkedInClient)   # bypass __init__
        client._req = MagicMock()
        result = client.get_audience_count({})
        assert result == 0
        client._req.assert_not_called()

    def test_empty_value_lists_skip_api_call(self):
        client = LinkedInClient.__new__(LinkedInClient)
        client._req = MagicMock()
        result = client.get_audience_count({"skills": [], "titles": []})
        assert result == 0
        client._req.assert_not_called()

    def test_url_is_built_manually_not_via_params(self):
        """The targeting string must be embedded directly in the URL — passing
        it through requests.params= would double-encode the % escapes."""
        client = LinkedInClient.__new__(LinkedInClient)
        client._token = "x"
        # Capture whatever URL the client tries to hit.
        captured = {}

        def fake_req(method, url, **kwargs):
            captured["method"] = method
            captured["url"]    = url
            captured["kwargs"] = kwargs
            resp = MagicMock()
            resp.ok = True
            resp.json.return_value = {"elements": [{"total": 12345}]}
            return resp
        client._req = fake_req
        client._raise_for_status = lambda r, ctx: None

        with patch("src.linkedin_api.config") as cfg:
            cfg.LINKEDIN_API_BASE = "https://api.linkedin.com/rest"
            n = client.get_audience_count({"skills": ["urn:li:skill:1"]})

        assert n == 12345
        assert "audienceCounts" in captured["url"]
        assert "q=targetingCriteriaV2" in captured["url"]
        assert "targetingCriteria=(include:" in captured["url"]
        # Critical: no `params=` kwarg was passed (would double-encode).
        assert "params" not in captured["kwargs"]

    def test_falls_back_to_active_when_total_missing(self):
        client = LinkedInClient.__new__(LinkedInClient)
        client._token = "x"

        def fake_req(method, url, **kwargs):
            resp = MagicMock()
            resp.ok = True
            resp.json.return_value = {"elements": [{"active": 999}]}
            return resp
        client._req = fake_req
        client._raise_for_status = lambda r, ctx: None
        with patch("src.linkedin_api.config") as cfg:
            cfg.LINKEDIN_API_BASE = "https://api.linkedin.com/rest"
            n = client.get_audience_count({"skills": ["urn:li:skill:1"]})
        assert n == 999

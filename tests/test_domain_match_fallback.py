"""Naming domain fallback: when Smart Ramp's domain matcher fails
(lp_guardrail_snapshot.error == "domain_not_found"), job_post_domain is a junk
guess and the campaign name should prefer matched_domain. GMR-0024 (BLV) hit
this: job_post_domain="Media & Communications" but matched_domain="Generalists".
"""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.smart_ramp_client import _domain_match_failed, SmartRampClient


class TestDomainMatchFailedParser:
    def test_domain_not_found_json_string(self):
        snap = '{"domain_matched": "Media & Communications", "error": "domain_not_found"}'
        assert _domain_match_failed(snap) is True

    def test_domain_not_found_dict(self):
        assert _domain_match_failed({"error": "domain_not_found"}) is True

    def test_ok_when_no_error(self):
        assert _domain_match_failed('{"error": ""}') is False
        assert _domain_match_failed('{"floor": 0}') is False

    def test_defensive_none_and_malformed(self):
        assert _domain_match_failed(None) is False
        assert _domain_match_failed("not json") is False
        assert _domain_match_failed(123) is False


class TestCohortSpecCarriesFlag:
    def test_parse_cohort_sets_domain_match_failed(self):
        client = SmartRampClient.__new__(SmartRampClient)  # no network init
        raw = {
            "id": "c1",
            "cohort_description": "Legally blind US-based Android/TalkBack users",
            "job_post_domain": "Media & Communications",
            "matched_domain": "Generalists",
            "lp_guardrail_snapshot": '{"error": "domain_not_found"}',
        }
        spec = client._parse_cohort(raw)
        assert spec.domain_match_failed is True
        assert spec.matched_domain == "Generalists"

    def test_parse_cohort_flag_false_when_match_ok(self):
        client = SmartRampClient.__new__(SmartRampClient)
        spec = client._parse_cohort({"id": "c2", "cohort_description": "x"})
        assert spec.domain_match_failed is False


def _resolve_domain(row: dict) -> str:
    """Mirror of the naming_meta domain logic in main._process_row_both_modes."""
    return (
        (row.get("matched_domain") or row.get("job_post_domain"))
        if row.get("domain_match_failed")
        else (row.get("job_post_domain") or row.get("matched_domain"))
    )


class TestNamingDomainResolution:
    def test_failed_match_prefers_matched_domain(self):
        row = {"job_post_domain": "Media & Communications", "matched_domain": "Generalists",
               "domain_match_failed": True}
        assert _resolve_domain(row) == "Generalists"

    def test_ok_match_keeps_job_post_domain(self):
        row = {"job_post_domain": "Finance & Quantitative Analysis", "matched_domain": "Finance",
               "domain_match_failed": False}
        assert _resolve_domain(row) == "Finance & Quantitative Analysis"

    def test_failed_match_with_empty_matched_falls_back_to_jpd(self):
        row = {"job_post_domain": "X", "matched_domain": None, "domain_match_failed": True}
        assert _resolve_domain(row) == "X"

"""_linkedin_pod_conversion_id — Smart Ramp pod → per-pod WS Grant rule id.

Attached IN ADDITION to LINKEDIN_CONVERSION_ID on LinkedIn campaigns. Rule ids
verified live by name 2026-06-11 (see reference_outlier_value_based_conversions).
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main as M


def test_each_pod_maps_to_its_rule():
    assert M._linkedin_pod_conversion_id("coders") == 26557044
    assert M._linkedin_pod_conversion_id("specialist") == 26557052
    assert M._linkedin_pod_conversion_id("languages") == 26557060
    assert M._linkedin_pod_conversion_id("generalist") == 26557068


def test_case_and_whitespace_insensitive():
    assert M._linkedin_pod_conversion_id("  Specialist ") == 26557052
    assert M._linkedin_pod_conversion_id("CODERS") == 26557044


def test_unknown_or_missing_pod_returns_none():
    assert M._linkedin_pod_conversion_id("") is None
    assert M._linkedin_pod_conversion_id(None) is None
    assert M._linkedin_pod_conversion_id("data-science") is None

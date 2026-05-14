"""Regression test for the _retry_li_campaign stale-master_campaign path.

In production, archive_stale_drafts.py and manual LinkedIn UI deletions
remove sponsored campaign groups whose URNs may still be referenced in the
Triggers sheet's `master_campaign` column. Before this fix, `_retry_li_campaign`
crashed with `requests.HTTPError 400 FIELD_VALUE_DOES_NOT_EXIST` for every row
referencing the dead group — once per row, even though the cause was identical
across the batch.

This test verifies:
  1. The first row referencing a dead group catches the 400 and exits cleanly
     (no exception propagates out of _retry_li_campaign).
  2. The dead group URN is added to `_DEAD_CAMPAIGN_GROUPS`.
  3. A second row referencing the same dead group is short-circuited — no
     second `create_campaign` call hits the wire.
"""
from __future__ import annotations

import json as _json
import os
import sys
from unittest.mock import MagicMock

import pytest
import requests

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import main  # noqa: E402


def _make_dead_group_response() -> requests.Response:
    resp = requests.Response()
    resp.status_code = 400
    resp._content = _json.dumps({
        "code":      "FIELD_VALUE_DOES_NOT_EXIST",
        "message":   "/Campaign/campaignGroup value urn:li:sponsoredCampaignGroup:381923536 does not exist.",
        "errorDetails": {
            "inputErrors": [{
                "description": "/Campaign/campaignGroup value urn:li:sponsoredCampaignGroup:381923536 does not exist",
                "input":       {"value": {"stringValue": "urn:li:sponsoredCampaignGroup:381923536"}},
                "inputPath":   {"fieldPath": "/Campaign/campaignGroup"},
                "code":        "FIELD_VALUE_DOES_NOT_EXIST",
            }],
        },
    }).encode("utf-8")
    resp.url = "https://api.linkedin.com/rest/adAccounts/510956407/adCampaigns"
    return resp


def _retry_kwargs(stg_id: str, master_campaign: str):
    """Build kwargs for _retry_li_campaign with a static (non-INMAIL) ad_type."""
    row = {
        "stg_id":             stg_id,
        "stg_name":           f"name-{stg_id}",
        "ad_type":            "STATIC",
        "master_campaign":    master_campaign,
        "targeting_facet":    "skills",
        "targeting_criteria": _json.dumps({"include": []}),
        "included_geos":      [],
        "flow_id":             "F1",
        "location":            "US",
    }
    li_client = MagicMock()
    li_client.create_campaign.side_effect = requests.HTTPError(
        "400 Client Error", response=_make_dead_group_response(),
    )
    urn_res = MagicMock()
    urn_res.resolve_default_excludes.return_value = []
    sheets = MagicMock()
    return {
        "row":            row,
        "inmail_sender":  "",
        "sheets":         sheets,
        "li_client":      li_client,
        "urn_res":        urn_res,
        "claude_key":     "stub",
        "figma_file":     "",
        "figma_node":     "",
        "mj_token":       "",
        "dry_run":        False,
    }


def test_dead_group_first_row_swallows_400_and_caches(monkeypatch):
    monkeypatch.setattr(main, "_DEAD_CAMPAIGN_GROUPS", set())
    kwargs = _retry_kwargs("STG-1", master_campaign="381923536")

    # Must not raise — the function logs + returns cleanly.
    main._retry_li_campaign(**kwargs)

    # The URN is cached so subsequent rows skip without an API call.
    assert "urn:li:sponsoredCampaignGroup:381923536" in main._DEAD_CAMPAIGN_GROUPS
    # We hit LinkedIn exactly once for this row.
    assert kwargs["li_client"].create_campaign.call_count == 1


def test_dead_group_second_row_skips_without_api_call(monkeypatch):
    monkeypatch.setattr(
        main, "_DEAD_CAMPAIGN_GROUPS",
        {"urn:li:sponsoredCampaignGroup:381923536"},
    )
    kwargs = _retry_kwargs("STG-2", master_campaign="381923536")

    main._retry_li_campaign(**kwargs)

    # No API call this time — short-circuited by the cache.
    assert kwargs["li_client"].create_campaign.call_count == 0


def test_dead_group_unrelated_400_still_raises(monkeypatch):
    """Other 400 errors must NOT be silently swallowed — only the specific
    dead-group case is treated as a soft skip."""
    monkeypatch.setattr(main, "_DEAD_CAMPAIGN_GROUPS", set())

    # Build a 400 that's NOT the dead-group case.
    resp = requests.Response()
    resp.status_code = 400
    resp._content = _json.dumps({
        "code":    "INVALID_FIELD_VALUE",
        "message": "Some unrelated validation error",
    }).encode("utf-8")

    kwargs = _retry_kwargs("STG-3", master_campaign="111111111")
    kwargs["li_client"].create_campaign.side_effect = requests.HTTPError(
        "400 Client Error", response=resp,
    )

    with pytest.raises(requests.HTTPError):
        main._retry_li_campaign(**kwargs)

    # The unrelated 400 didn't pollute the dead-group cache.
    assert "urn:li:sponsoredCampaignGroup:111111111" not in main._DEAD_CAMPAIGN_GROUPS

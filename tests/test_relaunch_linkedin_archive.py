"""relaunch._archive_linkedin — DRAFT-safe archive sequence.

A bare `$set status=ARCHIVED` PATCH 400s on DRAFT campaigns. The archiver must
bump `runSchedule.start` to the future AND archive the parent group BEFORE the
child campaigns (feedback_linkedin_archive_rules). Without this, replace=true
no-ops and leaves duplicates (GMR-0024, 2026-06-09).
"""
import os, sys, time
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.relaunch as relaunch


class FakeLI:
    def __init__(self, group="urn:li:sponsoredCampaignGroup:G1", fail_ids=None):
        self.calls = []  # list of (url, payload)
        self._group = group
        self._fail = set(fail_ids or [])

    def _url(self, path):
        return f"https://api/{path}"

    def get_campaign(self, cid):
        return {"campaignGroup": self._group}

    def _req(self, method, url, json=None, headers=None):
        self.calls.append((url, json))
        status = 400 if any(f in url for f in self._fail) else 200
        return SimpleNamespace(status_code=status, text="err" if status >= 300 else "")


def _install(monkeypatch, fake):
    monkeypatch.setenv("LINKEDIN_TOKEN", "x")
    monkeypatch.setattr("src.linkedin_api.LinkedInClient", lambda token: fake)


def test_archives_group_before_campaigns_with_future_start(monkeypatch):
    fake = FakeLI()
    _install(monkeypatch, fake)
    done = relaunch._archive_linkedin(
        ["urn:li:sponsoredCampaign:C1", "urn:li:sponsoredCampaign:C2"]
    )
    paths = [u for u, _ in fake.calls]
    grp_idx = next(i for i, p in enumerate(paths) if "adCampaignGroups/G1" in p)
    first_camp_idx = min(i for i, p in enumerate(paths) if "/adCampaigns/" in p)
    assert grp_idx < first_camp_idx                      # groups first
    now_ms = int(time.time() * 1000)
    for _, payload in fake.calls:
        s = payload["patch"]["$set"]
        assert s["status"] == "ARCHIVED"
        assert s["runSchedule"]["start"] > now_ms        # future start bump
    assert set(done) == {"urn:li:sponsoredCampaign:C1", "urn:li:sponsoredCampaign:C2"}


def test_group_archived_once_for_shared_parent(monkeypatch):
    fake = FakeLI()  # both campaigns share G1
    _install(monkeypatch, fake)
    relaunch._archive_linkedin(
        ["urn:li:sponsoredCampaign:C1", "urn:li:sponsoredCampaign:C2"]
    )
    group_calls = [u for u, _ in fake.calls if "adCampaignGroups/G1" in u]
    assert len(group_calls) == 1                          # deduped


def test_failed_campaign_excluded_from_done(monkeypatch):
    fake = FakeLI(fail_ids={"adCampaigns/C2"})
    _install(monkeypatch, fake)
    done = relaunch._archive_linkedin(
        ["urn:li:sponsoredCampaign:C1", "urn:li:sponsoredCampaign:C2"]
    )
    assert done == ["urn:li:sponsoredCampaign:C1"]

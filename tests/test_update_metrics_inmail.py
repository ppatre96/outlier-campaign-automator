"""update_metrics: InMail sends/opens + relaunch-tolerant name match.

LinkedIn is mostly InMail (Message Ads), which report sends/opens not
impressions/clicks, and relaunched campaigns' reporting ids aren't in the
registry — so the metrics refresh matches by normalized campaign name."""

import src.campaign_registry as reg
import src.ui_decisions as ui


def _stub(monkeypatch, rows):
    saved = {}
    monkeypatch.setattr(reg, "_load", lambda: rows)
    monkeypatch.setattr(reg, "_save", lambda rs: saved.__setitem__("rows", rs))
    monkeypatch.setattr(ui, "upsert_campaign", lambda rec: saved.setdefault("pg", []).append(rec))
    return saved


def test_name_norm_writes_sends_opens_first_only(monkeypatch):
    # two angle rows share the campaign name; relaunch date differs from the ad's
    rows = [
        {"campaign_name": "Scale-GMR-0023 | LinkedIn | ko-KR | Message ad | 06/13/2026", "angle": "A"},
        {"campaign_name": "Scale-GMR-0023 | LinkedIn | ko-KR | Message ad | 06/13/2026", "angle": "B"},
    ]
    saved = _stub(monkeypatch, rows)
    reg.update_metrics(
        "scale-gmr-0023 | linkedin | ko-kr | message ads | 06/03/2026",  # drifted date + "ads"
        impressions=0, clicks=500, spend_usd=100.0, sends=9000, opens=4000, by="name_norm",
    )
    r0 = saved["rows"][0]
    assert (r0["sends"], r0["opens"], r0["clicks"]) == (9000, 4000, 500)
    # campaign-level: only the first representative row is written (no double-count)
    assert "sends" not in saved["rows"][1]


def test_by_id_default_still_matches(monkeypatch):
    rows = [{"platform_campaign_id": "urn:li:sponsoredCampaign:123",
             "linkedin_campaign_urn": "urn:li:sponsoredCampaign:123", "angle": "A"}]
    saved = _stub(monkeypatch, rows)
    reg.update_metrics("urn:li:sponsoredCampaign:123", impressions=1000, clicks=20, spend_usd=50.0)
    assert saved["rows"][0]["impressions"] == 1000 and saved["rows"][0]["cpm_usd"] == 50.0

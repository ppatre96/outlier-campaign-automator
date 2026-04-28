"""End-to-end integration test — Phase 2.6 cross-plan replay (SR-02 + SR-06)."""
from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _load_fixture():
    """Load the recorded RampRecord fixture from Plan 01."""
    from src.smart_ramp_client import RampRecord, CohortSpec
    fixture_path = Path(__file__).resolve().parent / "fixtures" / "ramp_GMR-0010.json"
    raw = json.loads(fixture_path.read_text())
    cohorts = [CohortSpec(**c) for c in raw["cohorts"]]
    raw["cohorts"] = cohorts
    return RampRecord(**raw)


def test_recorded_ramp_replay_writes_state_and_three_slack_calls(tmp_path, monkeypatch):
    """End-to-end: poller → run_launch_for_ramp (mocked) → notifier (3 sends)."""
    fixture = _load_fixture()

    # 1) Mock SmartRampClient — return the fixture for both list + single fetch.
    fake_client = MagicMock()
    fake_client.fetch_ramp_list.return_value = [fixture]
    fake_client.fetch_ramp.return_value = fixture
    # The poller imports SmartRampClient at module-import time and calls
    # SmartRampClient() inside run_once; patch the class on the poller module
    # so its constructor returns our fake instance.
    import scripts.smart_ramp_poller as poller
    monkeypatch.setattr(poller, "SmartRampClient", lambda: fake_client)

    # 2) Mock main.run_launch_for_ramp — bypass the real Snowflake/LinkedIn pipeline.
    fake_pipeline_result = {
        "ok": True,
        "campaign_groups": ["urn:li:sponsoredCampaignGroup:9001"],
        "inmail_campaigns": [
            f"urn:li:sponsoredCampaign:in_{c.id}" for c in fixture.cohorts
        ],
        "static_campaigns": [
            f"urn:li:sponsoredCampaign:st_{c.id}" for c in fixture.cohorts
        ],
        "creative_paths": {
            "bn-in_inmail": "urn:li:dsc:111",
            "hi-in_static": "data/ramp_creatives/GMR-0010/hi-in_static_A__agent_x.png",
        },
        "per_cohort": [
            {
                "cohort_id": c.id,
                "cohort_description": c.cohort_description,
                "inmail_urn": f"urn:li:sponsoredCampaign:in_{c.id}",
                "static_urn": f"urn:li:sponsoredCampaign:st_{c.id}",
                "inmail_creative": f"urn:li:dsc:in_{c.id}",
                "static_creative": f"urn:li:dsc:st_{c.id}",
            }
            for c in fixture.cohorts
        ],
    }
    import main
    monkeypatch.setattr(
        main, "run_launch_for_ramp",
        lambda ramp_id, modes=("inmail", "static"), dry_run=False: fake_pipeline_result,
    )

    # 3) Mock slack_sdk.WebClient inside the notifier module.
    chat_calls: list[dict] = []
    open_calls: list[tuple] = []
    fake_web = MagicMock()

    def conv_open(users):
        open_calls.append(tuple(users))
        return {"channel": {"id": f"D_{users[0]}"}}

    def chat_post(channel, text):
        chat_calls.append({"channel": channel, "text": text})
        return {"ok": True}

    fake_web.conversations_open.side_effect = conv_open
    fake_web.chat_postMessage.side_effect = chat_post

    from src import smart_ramp_notifier as N
    monkeypatch.setattr(N, "WebClient", lambda token=None: fake_web)

    import config
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxb-fake-int")

    # 4) Redirect state + lock + log paths into tmp_path so we don't touch real
    #    data/ files. (LOG_DIR is also redirected so the date-stamped log file
    #    lands inside tmp_path.)
    monkeypatch.setattr(poller, "STATE_PATH", tmp_path / "processed_ramps.json")
    monkeypatch.setattr(poller, "LOCK_PATH", tmp_path / "smart_ramp_poller.lock")
    monkeypatch.setattr(poller, "LOG_DIR", tmp_path / "logs")

    # 5) Invoke the poller's main() — this is the entire Phase 2.6 wave under test.
    rc = poller.main(argv=["--once"])
    assert rc == 0, f"poller main() must exit 0; got {rc}"

    # 6) Assert state was written with sig + version=1.
    state_path = tmp_path / "processed_ramps.json"
    assert state_path.exists(), "state file must be written"
    state = json.loads(state_path.read_text())
    assert "GMR-0010" in state.get("ramps", {})
    entry = state["ramps"]["GMR-0010"]
    assert entry["version"] == 1
    assert entry["last_signature"].startswith("sha256:")
    assert entry["consecutive_failures"] == 0
    # Pipeline result reflected in state
    assert entry["inmail_campaigns"] == fake_pipeline_result["inmail_campaigns"]
    assert entry["static_campaigns"] == fake_pipeline_result["static_campaigns"]

    # 7) Assert EXACTLY 3 chat_postMessage calls (Pranav + Diego + channel).
    assert len(chat_calls) == 3, (
        f"expected exactly 3 chat_postMessage calls, got {len(chat_calls)}: "
        f"{[c['channel'] for c in chat_calls]}"
    )
    channels = [c["channel"] for c in chat_calls]
    assert "D_U095J930UEL" in channels, "Pranav DM channel missing"
    assert "D_U08AW9FCP27" in channels, "Diego DM channel missing"
    assert "C0B0NBB986L" in channels, "shared channel C0B0NBB986L missing"

    # 8) Assert two-step DM open: 2 conversations_open calls (NOT 3 — channel skips it).
    assert len(open_calls) == 2
    assert ("U095J930UEL",) in open_calls
    assert ("U08AW9FCP27",) in open_calls

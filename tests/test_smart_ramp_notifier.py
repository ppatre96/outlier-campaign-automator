"""Unit tests for src.smart_ramp_notifier — Phase 2.6 Plan 03 (SR-06, SR-07)."""
from __future__ import annotations

import re
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


@pytest.fixture
def fake_ramp():
    from src.smart_ramp_client import RampRecord, CohortSpec
    return RampRecord(
        id="GMR-0010", project_id="p", project_name="Outlier Coders India",
        requester_name="Pranav Patre", summary="ramp summary",
        submitted_at="t", updated_at="t", status="submitted",
        linear_issue_id=None, linear_url=None,
        cohorts=[
            CohortSpec(id="bn-in", cohort_description="Bengali speakers in India",
                       signup_flow_id=None, selected_lp_url=None,
                       included_geos=["IN"], matched_locales=None,
                       target_activations=None, job_post_id=None),
        ],
    )


def _fake_result(n_cohorts=1):
    return {
        "ok": True,
        "campaign_groups": ["urn:li:cg:1"],
        "inmail_campaigns": [f"urn:li:cmp:in_{i}" for i in range(n_cohorts)],
        "static_campaigns": [f"urn:li:cmp:st_{i}" for i in range(n_cohorts)],
        "creative_paths": {},
        "per_cohort": [
            {
                "cohort_id": f"cohort{i}",
                "cohort_description": f"Cohort {i}",
                "inmail_urn": f"urn:li:cmp:in_{i}",
                "static_urn": f"urn:li:cmp:st_{i}",
                "inmail_creative": f"urn:li:dsc:in_{i}",
                "static_creative": "data/ramp_creatives/GMR-0010/cohort_static_A__agent_xyz.png",
            }
            for i in range(n_cohorts)
        ],
    }


def _make_fake_webclient(open_fail_for_uid=None, post_fail_for_channel=None):
    """Returns (web_client_instance_mock, calls_list).

    `calls_list` accumulates dicts of {method, kwargs} so tests can assert
    EXACTLY 3 chat_postMessage calls, etc.
    """
    calls: list[dict] = []
    instance = MagicMock()

    def conv_open(users):
        calls.append({"method": "conversations_open", "users": users})
        uid = users[0]
        if open_fail_for_uid and uid == open_fail_for_uid:
            from slack_sdk.errors import SlackApiError
            raise SlackApiError("cannot_dm_bot",
                                response={"error": "cannot_dm_bot"})
        return {"channel": {"id": f"D_{uid}"}}

    def chat_post(channel, text):
        calls.append({"method": "chat_postMessage", "channel": channel, "text": text})
        if post_fail_for_channel and channel == post_fail_for_channel:
            from slack_sdk.errors import SlackApiError
            raise SlackApiError("not_in_channel",
                                response={"error": "not_in_channel"})
        return {"ok": True, "ts": "1234.5678"}

    instance.conversations_open.side_effect = conv_open
    instance.chat_postMessage.side_effect = chat_post
    return instance, calls


# ─────────────────────────────────────────────────────────────────────────────
# SR-06: 3 targets per ramp (Pranav DM + Diego DM + channel)
# ─────────────────────────────────────────────────────────────────────────────


def test_dm_to_pranav_diego_and_channel(monkeypatch, fake_ramp):
    """SR-06: notify_success sends EXACTLY 3 chat_postMessage calls (bot-token
    opportunistic path) — Pranav, Diego, C0B0NBB986L. After the 2026-05-13
    rewire, the primary delivery path is the Drive queue + RemoteTrigger;
    bot-token sends still run opportunistically when the token is valid."""
    import config
    from src import smart_ramp_notifier as N

    fake_client, calls = _make_fake_webclient()
    monkeypatch.setattr(N, "WebClient", lambda token=None: fake_client)
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxb-fake")
    # Stub the Drive queue write so the test doesn't touch real Drive auth.
    monkeypatch.setattr(N, "_enqueue_via_drive", lambda text, ramp_id="": True)

    outcomes = N.notify_success(fake_ramp, _fake_result(n_cohorts=2), version=1)

    # 4 outcomes: drive_queue (primary) + 3 bot targets — all True
    assert "drive_queue" in outcomes
    assert outcomes["drive_queue"] is True
    bot_outcomes = {k: v for k, v in outcomes.items() if k != "drive_queue"}
    assert len(bot_outcomes) == 3, f"expected 3 bot targets, got {len(bot_outcomes)}"
    assert all(bot_outcomes.values()), f"all 3 bot targets should succeed: {bot_outcomes}"

    # EXACTLY 3 chat_postMessage calls (bot path)
    post_calls = [c for c in calls if c["method"] == "chat_postMessage"]
    assert len(post_calls) == 3, f"expected exactly 3 chat_postMessage calls, got {len(post_calls)}"

    # Resolved channels: D_<Pranav>, D_<Diego>, literal channel C0B0NBB986L
    channels = [c["channel"] for c in post_calls]
    assert "D_U095J930UEL" in channels, "Pranav DM channel missing"
    assert "D_U08AW9FCP27" in channels, "Diego DM channel missing"
    assert "C0B0NBB986L" in channels, "shared channel C0B0NBB986L missing"

    # All 3 messages have IDENTICAL text
    texts = {c["text"] for c in post_calls}
    assert len(texts) == 1, f"all 3 sends should share the same body; got {len(texts)} distinct"


def test_two_step_conversations_open_for_dms(monkeypatch, fake_ramp):
    """SR-06: DMs use two-step conversations_open → chat_postMessage; channel uses direct post."""
    import config
    from src import smart_ramp_notifier as N

    fake_client, calls = _make_fake_webclient()
    monkeypatch.setattr(N, "WebClient", lambda token=None: fake_client)
    monkeypatch.setattr(N, "_enqueue_via_drive", lambda text, ramp_id="": True)
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxb-fake")

    N.notify_success(fake_ramp, _fake_result(), version=1)

    # conversations_open called EXACTLY 2 times (once per user target — never for channel)
    open_calls = [c for c in calls if c["method"] == "conversations_open"]
    assert len(open_calls) == 2, f"expected 2 conversations_open calls, got {len(open_calls)}"
    open_users = {tuple(c["users"]) for c in open_calls}
    assert ("U095J930UEL",) in open_users
    assert ("U08AW9FCP27",) in open_users
    # The channel target was NOT routed through conversations_open
    assert ("C0B0NBB986L",) not in open_users


# ─────────────────────────────────────────────────────────────────────────────
# CLAUDE.md vocabulary
# ─────────────────────────────────────────────────────────────────────────────


def test_dm_vocabulary(fake_ramp):
    """CLAUDE.md: success + escalation messages pass the banned-token regex."""
    from src import smart_ramp_notifier as N
    success_msg = N.build_success_message(
        ramp_id=fake_ramp.id, project_name="X", requester_name="Y",
        per_cohort=_fake_result(n_cohorts=2)["per_cohort"], version=1,
    )
    esc_msg = N.build_escalation_message(
        ramp_id=fake_ramp.id, project_name="X", requester_name="Y",
        error_class="RuntimeError",
        traceback_text="Traceback (most recent call last):\n  File 'x'\nRuntimeError: boom",
    )

    # Banned-token strict regex (CLAUDE.md Don't-Say table). Note: "required" is
    # banned per CLAUDE.md ("Required" → "Strongly encouraged"); we keep it in
    # the strict regex so the message bodies are forced to use the approved
    # alternative.
    banned_strict = re.compile(
        r"\b(compensation|project rate|interview|bonus|promote|assign|"
        r"job|role|position|team|required)\b",
        re.IGNORECASE,
    )

    for label, msg in [("success", success_msg), ("escalation", esc_msg)]:
        m = banned_strict.search(msg)
        assert not m, f"banned vocab in {label}: matched {m.group(0)!r} in: {msg!r}"


# ─────────────────────────────────────────────────────────────────────────────
# SR-07: escalation format
# ─────────────────────────────────────────────────────────────────────────────


def test_escalation_dm_format(fake_ramp):
    """SR-07: escalation message contains error class, traceback line, recovery cmd, reset snippet."""
    from src import smart_ramp_notifier as N
    esc = N.build_escalation_message(
        ramp_id="GMR-0010", project_name="Outlier Coders India",
        requester_name="Pranav Patre",
        error_class="RuntimeError",
        traceback_text=(
            "Traceback (most recent call last):\n"
            "  File 'main.py', line 521, in _process_row\n"
            "RuntimeError: 403 Forbidden — DSC post denied"
        ),
    )
    assert "GMR-0010" in esc
    assert "RuntimeError" in esc
    assert "403 Forbidden" in esc, "first traceback line should appear"
    # Manual recovery command
    assert "cd /Users/pranavpatre/outlier-campaign-agent" in esc
    assert "venv/bin/python3 main.py --ramp-id GMR-0010" in esc
    # Reset-counter snippet
    assert "Reset retry counter" in esc
    assert "consecutive_failures" in esc
    assert "escalation_dm_sent" in esc


# ─────────────────────────────────────────────────────────────────────────────
# SR-06 + Pitfall 7: per-target isolation
# ─────────────────────────────────────────────────────────────────────────────


def test_one_target_failure_does_not_block_others(monkeypatch, fake_ramp):
    """SR-06: Diego's DM open fails (cannot_dm_bot) — Pranav DM AND channel post still succeed."""
    import config
    from src import smart_ramp_notifier as N

    # Diego (U08AW9FCP27) cannot be DMed
    fake_client, calls = _make_fake_webclient(open_fail_for_uid="U08AW9FCP27")
    monkeypatch.setattr(N, "WebClient", lambda token=None: fake_client)
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxb-fake")
    monkeypatch.setattr(N, "_enqueue_via_drive", lambda text, ramp_id="": True)

    outcomes = N.notify_success(fake_ramp, _fake_result(), version=1)

    # 2 of 3 succeed; Diego is False
    assert outcomes["user:U095J930UEL"] is True, "Pranav should succeed"
    assert outcomes["user:U08AW9FCP27"] is False, "Diego should fail (cannot_dm_bot)"
    assert outcomes["channel:C0B0NBB986L"] is True, "channel should still succeed"

    # 2 chat_postMessage calls (Pranav DM + channel post; Diego skipped after open failure)
    post_calls = [c for c in calls if c["method"] == "chat_postMessage"]
    assert len(post_calls) == 2, f"expected 2 successful posts (Diego skipped), got {len(post_calls)}"


# ─────────────────────────────────────────────────────────────────────────────
# Degraded-mode webhook fallback — when ALL bot-token targets fail
# ─────────────────────────────────────────────────────────────────────────────


def test_webhook_fallback_when_all_targets_fail(monkeypatch, fake_ramp):
    """When Drive queue AND all 3 bot-token targets fail (token expired,
    no Drive auth), notifier falls back to SLACK_WEBHOOK_URL so Pranav still
    gets the message. Diego DM and C0B0NBB986L stay silent — webhook only
    covers one destination."""
    import config
    from src import smart_ramp_notifier as N
    from slack_sdk.errors import SlackApiError

    # Drive queue also fails
    monkeypatch.setattr(N, "_enqueue_via_drive", lambda text, ramp_id="": False)

    # Make every bot-token call fail with token_expired
    instance = MagicMock()
    instance.conversations_open.side_effect = SlackApiError(
        "token_expired", response={"error": "token_expired"}
    )
    instance.chat_postMessage.side_effect = SlackApiError(
        "token_expired", response={"error": "token_expired"}
    )
    monkeypatch.setattr(N, "WebClient", lambda token=None: instance)
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxe.xoxp-expired")
    monkeypatch.setattr(config, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")

    # Capture webhook POST
    webhook_calls = []

    class FakeResp:
        ok = True
        status_code = 200
        text = "ok"

    def fake_post(url, json=None, timeout=None):
        webhook_calls.append({"url": url, "json": json})
        return FakeResp()

    monkeypatch.setattr(N.requests, "post", fake_post)

    outcomes = N.notify_success(fake_ramp, _fake_result(), version=1)

    # All 3 primary targets failed
    assert outcomes["user:U095J930UEL"] is False
    assert outcomes["user:U08AW9FCP27"] is False
    assert outcomes["channel:C0B0NBB986L"] is False
    # Webhook fallback fired and succeeded
    assert outcomes["webhook_fallback"] is True
    # Webhook called exactly once with the same body
    assert len(webhook_calls) == 1
    assert webhook_calls[0]["url"] == "https://hooks.slack.com/services/T/B/X"
    assert "GMR-0010" in webhook_calls[0]["json"]["text"]


def test_webhook_fallback_skipped_when_any_target_succeeds(monkeypatch, fake_ramp):
    """Partial failure (e.g., Diego cannot_dm_bot but Pranav + channel succeed)
    must NOT trigger the webhook fallback — that would double-post to Pranav."""
    import config
    from src import smart_ramp_notifier as N

    fake_client, _calls = _make_fake_webclient(open_fail_for_uid="U08AW9FCP27")
    monkeypatch.setattr(N, "WebClient", lambda token=None: fake_client)
    monkeypatch.setattr(config, "SLACK_BOT_TOKEN", "xoxb-fake")
    monkeypatch.setattr(config, "SLACK_WEBHOOK_URL", "https://hooks.slack.com/services/T/B/X")
    monkeypatch.setattr(N, "_enqueue_via_drive", lambda text, ramp_id="": True)

    webhook_calls = []
    monkeypatch.setattr(N.requests, "post",
                        lambda *a, **kw: webhook_calls.append((a, kw)) or MagicMock(ok=True))

    outcomes = N.notify_success(fake_ramp, _fake_result(), version=1)

    # Pranav + channel succeeded → no fallback
    assert outcomes["user:U095J930UEL"] is True
    assert outcomes["channel:C0B0NBB986L"] is True
    assert "webhook_fallback" not in outcomes
    assert len(webhook_calls) == 0, "webhook must NOT fire when any primary target succeeds"

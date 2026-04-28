"""
Smart Ramp Notifier — Phase 2.6 (SR-06, SR-07).

Sends Slack messages to THREE targets per ramp:
  1. Pranav DM      (user U095J930UEL)
  2. Diego DM       (user U08AW9FCP27)
  3. Channel post   (channel C0B0NBB986L)

Targets resolved from config.SLACK_RAMP_NOTIFY_TARGETS — adding/removing targets
is a config edit. Per-target error isolation: one failed target does NOT block
the others (RESEARCH §Pattern 6, §Pitfall 7).

Vocabulary (CLAUDE.md): every user-facing string here uses approved Outlier
vocabulary. The Don't-Say list (a.k.a. banned tokens) lives in CLAUDE.md and
is never emitted at runtime — message bodies are exercised by the banned
regex in tests/test_smart_ramp_notifier.py and the CI vocabulary scan.
"""
from __future__ import annotations

import logging
from typing import Optional

import requests
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

import config

log = logging.getLogger(__name__)

# Locked deep-link to LinkedIn Campaign Manager
LINKEDIN_CAMPAIGN_MANAGER_URL = (
    "https://www.linkedin.com/campaignmanager/accounts/510956407/campaigns"
)


# ─────────────────────────────────────────────────────────────────────────────
# Message builders (vocabulary-clean per CLAUDE.md)
# ─────────────────────────────────────────────────────────────────────────────


def build_success_message(
    ramp_id: str,
    project_name: str,
    requester_name: str,
    per_cohort: list[dict],
    version: int = 1,
) -> str:
    """Build the success-path Slack body.

    Template locked in CONTEXT.md §Slack Notifier. Vocabulary-clean: uses
    "draft" (not "campaign awaiting approval"), "review and activate" (not
    "approve"), "creative" (not "ad"). Never emits banned tokens.
    """
    if version > 1:
        header = f"*Smart Ramp processed (v{version}): {ramp_id}* — {project_name}"
    else:
        header = f"*Smart Ramp processed: {ramp_id}* — {project_name}"

    lines = [
        header,
        f"Requester: {requester_name}",
        f"Cohorts: {len(per_cohort)}",
    ]
    if version > 1:
        lines.append(
            "Prior version superseded — review old drafts at LinkedIn Campaign Manager."
        )
    lines.append("")

    for c in per_cohort:
        desc = c.get("cohort_description") or c.get("cohort_id") or "cohort"
        lines.append(f"*Cohort: {desc}*")
        lines.append(f"  • InMail draft: `{c.get('inmail_urn') or '—'}`")
        lines.append(f"  • Static draft: `{c.get('static_urn') or '—'}`")
        inmail_creative = c.get("inmail_creative") or "—"
        static_creative = c.get("static_creative") or "—"
        lines.append(f"  • Creative (InMail): {inmail_creative}")
        lines.append(f"  • Creative (Static): {static_creative}")
        lines.append("")

    lines.append("Review and activate in LinkedIn Campaign Manager:")
    lines.append(LINKEDIN_CAMPAIGN_MANAGER_URL)
    return "\n".join(lines)


def build_escalation_message(
    ramp_id: str,
    project_name: str,
    requester_name: str,
    error_class: str,
    traceback_text: Optional[str],
) -> str:
    """Build the escalation-path Slack body (after SMART_RAMP_FAILURE_THRESHOLD failures).

    Template locked in CONTEXT.md §Slack Notifier. Includes:
      - Error class + first line of traceback
      - Manual recovery command
      - Reset-counter Python snippet
    """
    first_tb_line = "—"
    if traceback_text:
        # First non-empty line that names the actual exception (scan from the bottom)
        for ln in reversed((traceback_text or "").splitlines()):
            if ln.strip():
                first_tb_line = ln.strip()[:300]
                break

    lines = [
        f"*Smart Ramp processing failed {config.SMART_RAMP_FAILURE_THRESHOLD} times: {ramp_id}* — {project_name}",
        f"Requester: {requester_name}",
        "",
        f"Last error: {error_class} — {first_tb_line}",
        "",
        "Manual recovery:",
        "```",
        "cd /Users/pranavpatre/outlier-campaign-agent",
        f"venv/bin/python3 main.py --ramp-id {ramp_id}",
        "```",
        "",
        "Reset retry counter:",
        "```",
        f"venv/bin/python3 -c \"import json; p='data/processed_ramps.json'; "
        f"s=json.load(open(p)); s['ramps']['{ramp_id}']['consecutive_failures']=0; "
        f"s['ramps']['{ramp_id}']['escalation_dm_sent']=False; "
        f"json.dump(s, open(p,'w'), indent=2)\"",
        "```",
    ]
    return "\n".join(lines)


# ─────────────────────────────────────────────────────────────────────────────
# Per-target send helper (RESEARCH §Pattern 6, §Q4)
# ─────────────────────────────────────────────────────────────────────────────


def _send_to_target(client: WebClient, target: tuple[str, str], text: str) -> bool:
    """Send `text` to a single target tuple (kind, id).

    For kind == "user":   two-step conversations_open → chat_postMessage
    For kind == "channel": direct chat_postMessage(channel=id)

    Returns True on success, False on Slack API error (logged but NOT raised —
    per-target isolation per CONTEXT.md / RESEARCH §Pattern 6).
    """
    kind, target_id = target
    try:
        if kind == "user":
            # Two-step: get the IM channel ID, then post (RESEARCH §Q4)
            open_resp = client.conversations_open(users=[target_id])
            channel_id = open_resp["channel"]["id"]
            client.chat_postMessage(channel=channel_id, text=text)
            log.info("Slack DM sent to user %s", target_id)
            return True
        elif kind == "channel":
            client.chat_postMessage(channel=target_id, text=text)
            log.info("Slack channel post sent to %s", target_id)
            return True
        else:
            log.warning("Unknown target kind %r for id %s — skipping", kind, target_id)
            return False
    except SlackApiError as e:
        err_code = "unknown"
        try:
            err_code = e.response.get("error", "unknown")
        except Exception:
            pass
        log.warning(
            "Slack send to %s=%s failed: %s — continuing with other targets",
            kind, target_id, err_code,
        )
        return False
    except Exception as e:
        log.warning(
            "Slack send to %s=%s raised %s: %s — continuing with other targets",
            kind, target_id, type(e).__name__, e,
        )
        return False


def _post_via_webhook(text: str) -> bool:
    """Degraded-mode fallback: post once via SLACK_WEBHOOK_URL.

    Used when ALL bot-token targets fail (e.g., expired token). The webhook
    posts to a single fixed destination (configured when the webhook was
    minted) — typically Pranav's DM. Diego DM and the C0B0NBB986L channel
    are NOT covered by this fallback and remain silent until the bot token
    is refreshed.

    Returns True on success, False on any error.
    """
    webhook_url = getattr(config, "SLACK_WEBHOOK_URL", None)
    if not webhook_url:
        log.error(
            "All Slack targets failed and no SLACK_WEBHOOK_URL configured — "
            "this notification is silently dropped. Refresh SLACK_BOT_TOKEN "
            "or set SLACK_WEBHOOK_URL in .env to recover."
        )
        return False
    try:
        resp = requests.post(webhook_url, json={"text": text}, timeout=10)
        if resp.ok:
            log.warning(
                "Degraded mode: all 3 Slack bot targets failed; fell back to "
                "webhook. Diego DM and channel C0B0NBB986L SKIPPED — refresh "
                "SLACK_BOT_TOKEN to restore full delivery."
            )
            return True
        log.error(
            "Webhook fallback also failed: %s %s — notification silently dropped",
            resp.status_code, resp.text[:200],
        )
        return False
    except Exception as e:
        log.error("Webhook fallback raised %s: %s", type(e).__name__, e)
        return False


def _send_to_all_targets(text: str) -> dict:
    """Send `text` to every target in config.SLACK_RAMP_NOTIFY_TARGETS.

    Returns a dict {target_str: success_bool} so callers (and tests) can verify
    EXACTLY 3 sends were attempted, with per-target outcome. The reserved key
    `webhook_fallback` is populated only when ALL three primary targets failed
    AND `SLACK_WEBHOOK_URL` was attempted.
    """
    if not config.SLACK_BOT_TOKEN:
        log.error(
            "SLACK_BOT_TOKEN not set — attempting webhook fallback for at least Pranav DM."
        )
        outcomes = {f"{kind}:{tid}": False for kind, tid in config.SLACK_RAMP_NOTIFY_TARGETS}
        outcomes["webhook_fallback"] = _post_via_webhook(text)
        return outcomes

    client = WebClient(token=config.SLACK_BOT_TOKEN)
    outcomes: dict[str, bool] = {}
    for target in config.SLACK_RAMP_NOTIFY_TARGETS:
        kind, tid = target
        outcomes[f"{kind}:{tid}"] = _send_to_target(client, target, text)

    # Degraded-mode fallback: if EVERY primary target failed, try the webhook
    # so Pranav at least sees the notification while the bot token is broken.
    if not any(outcomes.values()):
        outcomes["webhook_fallback"] = _post_via_webhook(text)

    return outcomes


# ─────────────────────────────────────────────────────────────────────────────
# Public API — called by scripts/smart_ramp_poller.py
# ─────────────────────────────────────────────────────────────────────────────


def notify_success(ramp_record, result: dict, version: int = 1) -> dict:
    """Send the success Slack message to all 3 targets. Returns per-target outcomes."""
    text = build_success_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=ramp_record.requester_name or "—",
        per_cohort=result.get("per_cohort") or [],
        version=version,
    )
    return _send_to_all_targets(text)


def notify_escalation(
    ramp_record, error_class: str, traceback_text: Optional[str]
) -> dict:
    """Send the escalation Slack message to all 3 targets. Returns per-target outcomes."""
    text = build_escalation_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=ramp_record.requester_name or "—",
        error_class=error_class,
        traceback_text=traceback_text,
    )
    return _send_to_all_targets(text)

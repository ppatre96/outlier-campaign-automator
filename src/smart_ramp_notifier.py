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


def _build_mention_prefix() -> str:
    """Render `<@U1> <@U2> <@U3>` from config.SLACK_CHANNEL_MENTION_IDS.

    Returns an empty string when no IDs are configured (e.g., during tests
    that monkey-patch the list to []). Pranav's DM target is excluded — DMs
    don't need self-pings; only the channel post needs the operator pings.
    Slack auto-resolves the same mentions in DMs too, so the same body
    works across all 3 targets without per-target customization.
    """
    ids = list(getattr(config, "SLACK_CHANNEL_MENTION_IDS", []) or [])
    if not ids:
        return ""
    return " ".join(f"<@{uid}>" for uid in ids)


def build_success_message(
    ramp_id: str,
    project_name: str,
    requester_name: str,
    per_cohort: list[dict],
    version: int = 1,
    extra_platform_campaigns: dict | None = None,
    manual_handoff_urls: dict | None = None,
) -> str:
    """Build the success-path Slack body.

    Vocabulary-clean: uses "draft", "review and activate", "creative".
    Never emits banned tokens.

    `extra_platform_campaigns` is the dict returned by run_launch_for_ramp:
        {"meta": ["120245...", ...], "google": ["customers/.../adGroups/...", ...]}
    Each entry is a top-level Ad Set (Meta) or Ad Group (Google) created
    during the run. Counts are surfaced in the summary; full lists in the
    Triggers sheet → Campaign Registry tab.

    `manual_handoff_urls` is `{"meta": "drive_url", "google": "drive_url"}`
    pointing to the JSON manifest the Meta/Google arm writes to Drive when
    platform-side ad creation fails (graceful degradation). Surfaced in the
    body so Diego (Meta) and Bryan (Google) can pick up the creatives + copy
    and build the campaign manually.
    """
    if version > 1:
        header = f"*Smart Ramp processed (v{version}): {ramp_id}* — {project_name}"
    else:
        header = f"*Smart Ramp processed: {ramp_id}* — {project_name}"

    extra_platform_campaigns = extra_platform_campaigns or {}
    manual_handoff_urls = manual_handoff_urls or {}
    meta_count   = len(extra_platform_campaigns.get("meta") or [])
    google_count = len(extra_platform_campaigns.get("google") or [])

    mention_prefix = _build_mention_prefix()
    lines = []
    if mention_prefix:
        # First line is the operator-ping prefix so the message is impossible
        # to miss in the channel. Each ramp summary pings Diego (Meta) +
        # Bryan (Google) + Tuan (oversight).
        lines.append(mention_prefix)
    lines += [
        header,
        f"Requester: {requester_name}",
        f"Cohorts: {len(per_cohort)}",
    ]
    if meta_count or google_count:
        lines.append(
            f"Multi-channel ad sets: LinkedIn={len(per_cohort)}  Meta={meta_count}  Google={google_count}"
        )
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

    if extra_platform_campaigns:
        lines.append("*Other channels (DRAFT)*")
        for plat, ids in extra_platform_campaigns.items():
            if not ids:
                continue
            label = {"meta": "Meta", "google": "Google Ads"}.get(plat, plat.title())
            lines.append(f"  • {label}: {len(ids)} ad set(s)/group(s) — first: `{ids[0]}`")
        lines.append("")

    # Manual-handoff manifests for arms where platform-side creation failed
    # (e.g., Meta SAC, Google permission denied). PNGs + cohort/copy details
    # are still in Drive — Diego/Bryan can build the campaign by hand from
    # the manifest at the URL below.
    # URL FORMATTING RULE (Pranav 2026-05-13): every URL goes on its OWN line
    # with a blank line before AND after. Inline URLs ("Label: https://...")
    # get merged with the next word in Slack's mobile/text renderer. Indent
    # the URL line with 4 spaces so it visually nests under the bullet.
    handoff_for_render = {p: u for p, u in (manual_handoff_urls or {}).items() if u}
    if handoff_for_render:
        lines.append("*Manual handoff — creatives ready in Drive*")
        for plat, url in handoff_for_render.items():
            label = {"meta": "Meta (Diego)", "google": "Google Ads (Bryan)"}.get(plat, plat.title())
            lines.append(f"  • {label}:")
            lines.append("")
            lines.append(f"    {url}")
            lines.append("")
        lines.append("")

    lines.append("Review and activate in LinkedIn Campaign Manager:")
    lines.append("")
    lines.append(LINKEDIN_CAMPAIGN_MANAGER_URL)
    lines.append("")
    # Console deep-link added 2026-05-22 for consistency with the new-ramp +
    # briefs-ready lifecycle pings — operators land on the same page across
    # the whole flow.
    console_url = _console_ramp_url(ramp_id)
    if console_url:
        lines.append("Console:")
        lines.append("")
        lines.append(console_url)
        lines.append("")
    lines.append("Full per-creative breakdown in the Triggers sheet → Campaign Registry tab.")
    return "\n".join(lines)


def _console_ramp_url(ramp_id: str) -> str:
    """Deep-link to the ramp detail page in outlier-campaign-console.
    Used by every lifecycle notification (new_ramp / briefs_ready / shipped)
    so Diego + Bryan can click straight through from Slack."""
    base = getattr(config, "OUTLIER_CONSOLE_URL", "").rstrip("/")
    return f"{base}/ramps/{ramp_id}" if base else ""


def build_new_ramp_message(
    ramp_id: str,
    project_name: str,
    requester_name: str,
    summary: str = "",
) -> str:
    """Slack body fired when the poller first sees a new Smart Ramp.

    The poller's cron tick auto-kicks prep right after sending this, so the
    message frames it as "prep is running" rather than asking Diego/Bryan to
    click anything yet. The action-required ping comes after prep finishes
    (build_briefs_ready_message). Reviewers can also force-rerun prep via
    the Run prep button if the auto-prep failed.
    """
    mention = _build_mention_prefix()
    url = _console_ramp_url(ramp_id)
    lines = []
    if mention:
        lines.append(mention)
    lines += [
        f"*New Smart Ramp detected: {ramp_id}* — {project_name}",
        f"Requester: {requester_name or '—'}",
    ]
    if summary:
        lines.append(f"Brief: {summary[:200]}")
    lines += [
        "",
        "Prep is running now — Stage A → B → C → ICP enrichment → competitor intel + brief generation (~2-5 min).",
        "You'll get a follow-up ping once briefs are ready for review.",
        "",
        "Track progress in the console:",
        "",
        url,
        "",
    ]
    return "\n".join(lines)


def build_briefs_ready_message(
    ramp_id: str,
    project_name: str,
    requester_name: str,
    briefs_generated: int,
    cohorts_count: int,
    fell_back_to_legacy: bool = False,
) -> str:
    """Slack body fired after prep completes and the decision row is written.

    Two flavors:
      - briefs_generated > 0 → "Briefs ready for review" (awaiting_brief_review)
      - fell_back_to_legacy → "Prep done, no briefs persisted" (awaiting_approval)
        — happens when ICP-fallback couldn't synthesize a cohort. Reviewer
        still picks channels + budget but has no brief gate.
    """
    mention = _build_mention_prefix()
    url = _console_ramp_url(ramp_id)
    lines = []
    if mention:
        lines.append(mention)
    lines += [
        f"*Smart Ramp prep complete: {ramp_id}* — {project_name}",
        f"Requester: {requester_name or '—'}",
    ]
    if fell_back_to_legacy:
        lines += [
            f"Cohorts mined: {cohorts_count}",
            "",
            "No briefs persisted (sparse-mode ICP fallback unavailable). "
            "Status: awaiting_approval — pick channels + budget and click Launch in the console.",
        ]
    else:
        lines += [
            f"Cohorts mined: {cohorts_count}",
            f"Briefs ready for review: {briefs_generated}",
            "",
            "Open the ramp detail page and review each brief. Drop a comment "
            "per row if you want the copy/design writer to redirect, then "
            "click *Confirm briefs* to flip to channels + budget approval.",
        ]
    lines += [
        "",
        "Console:",
        "",
        url,
        "",
    ]
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

    mention_prefix = _build_mention_prefix()
    lines = []
    if mention_prefix:
        lines.append(mention_prefix)
    lines += [
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


def _send_to_target(
    client: WebClient,
    target: tuple[str, str],
    text: str,
    thread_ts: Optional[str] = None,
) -> tuple[bool, Optional[str]]:
    """Send `text` to a single target tuple (kind, id).

    For kind == "user":   two-step conversations_open → chat_postMessage
    For kind == "channel": direct chat_postMessage(channel=id)

    When `thread_ts` is set, posts as a reply in that thread instead of
    top-level. Used by lifecycle pings (briefs_ready, launched) so all
    ramp-scoped Slack chatter lives under one parent message in the
    channel.

    Returns (ok, posted_ts). `posted_ts` is Slack's message timestamp for
    the new post — used by notify_new_ramp to capture the parent ts the
    FIRST time, so subsequent pings can reply in-thread.
    """
    kind, target_id = target
    extra_kw: dict = {"thread_ts": thread_ts} if thread_ts else {}
    try:
        if kind == "user":
            # Two-step: get the IM channel ID, then post (RESEARCH §Q4)
            open_resp = client.conversations_open(users=[target_id])
            channel_id = open_resp["channel"]["id"]
            resp = client.chat_postMessage(channel=channel_id, text=text, **extra_kw)
            log.info("Slack DM sent to user %s%s", target_id, " (threaded)" if thread_ts else "")
            return True, resp.get("ts") if hasattr(resp, "get") else None
        elif kind == "channel":
            resp = client.chat_postMessage(channel=target_id, text=text, **extra_kw)
            log.info("Slack channel post sent to %s%s", target_id, " (threaded)" if thread_ts else "")
            return True, resp.get("ts") if hasattr(resp, "get") else None
        else:
            log.warning("Unknown target kind %r for id %s — skipping", kind, target_id)
            return False, None
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
        return False, None
    except Exception as e:
        log.warning(
            "Slack send to %s=%s raised %s: %s — continuing with other targets",
            kind, target_id, type(e).__name__, e,
        )
        return False, None


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


def _enqueue_via_drive(
    text: str, ramp_id: str = "", thread_ts: Optional[str] = None,
    targets: Optional[list] = None,
) -> bool:
    """Primary delivery path (2026-05-13+): drop the summary into a Drive
    queue folder. A companion RemoteTrigger cron polls the folder every 5
    minutes and posts each file to Slack via the Claude.ai-inherited Slack
    MCP connector — no bot token required, and the same OAuth identity
    Diego/Bryan/Tuan see on Pranav's hourly heartbeat trigger.

    `thread_ts` (added 2026-05-22): when set, the queued JSON includes a
    thread_ts field that the RemoteTrigger reads + passes to chat_postMessage
    so the message replies in-thread instead of top-level. Trigger-side
    support is required for this to actually thread; falls back to top-level
    silently when the trigger ignores the field.

    Returns True if the queue write succeeded.
    """
    try:
        from src.gdrive import enqueue_slack_summary
        target_list = (
            getattr(config, "SLACK_RAMP_NOTIFY_TARGETS", []) if targets is None else targets
        )
        targets_payload = [
            {"kind": kind, "id": tid} for kind, tid in (target_list or [])
        ]
        # gdrive.enqueue_slack_summary takes **extra kwargs through to the JSON
        # payload; older trigger versions ignore unknown fields.
        kwargs: dict = {"ramp_id": ramp_id, "summary_text": text, "targets": targets_payload}
        if thread_ts:
            kwargs["thread_ts"] = thread_ts
        url = enqueue_slack_summary(**kwargs)
        if url:
            log.info(
                "Slack delivery: queued via Drive%s → %s",
                " (threaded)" if thread_ts else "", url,
            )
            return True
        log.warning("Slack delivery: Drive queue write returned empty URL")
        return False
    except Exception as exc:
        log.warning(
            "Slack delivery: Drive queue write failed (%s) — falling back to bot/webhook path",
            exc,
        )
        return False


def _send_to_all_targets(
    text: str, ramp_id: str = "", thread_ts: Optional[str] = None,
    targets: Optional[list] = None,
) -> dict:
    """Send `text` to every target in `targets`.

    `targets` defaults to config.SLACK_RAMP_NOTIFY_TARGETS (Pranav + Diego +
    channel — the team-facing list). Granular observability pings pass
    config.SLACK_VERBOSE_TARGETS (Pranav DM only) so Diego + the channel stay
    at exactly two messages per ramp.

    Delivery order (2026-05-13 — bot-tokenless preferred):
      1. Drive queue → RemoteTrigger cron posts via Claude.ai Slack MCP
         (no bot token; works across rotations; covers Diego/Bryan/Tuan
         mentions in the channel post)
      2. Bot token (slack_sdk.WebClient) — kept as legacy path; opportunistic
      3. Webhook fallback — last-resort, posts to Pranav DM only

    Returns a dict {target_str: success_bool}. Reserved keys:
      - `drive_queue`        — Drive queue write outcome (primary)
      - `webhook_fallback`   — only populated when bot+queue both failed
    """
    target_list = config.SLACK_RAMP_NOTIFY_TARGETS if targets is None else targets
    outcomes: dict = {}
    channel_ts: Optional[str] = None  # captured from the channel post for threading

    # 1) Drive queue — primary path
    outcomes["drive_queue"] = _enqueue_via_drive(
        text, ramp_id=ramp_id, thread_ts=thread_ts, targets=target_list
    )

    # 2) Opportunistic bot-token send (kept for back-compat). When threading
    # is requested, this is the path that actually returns a usable parent_ts.
    if config.SLACK_BOT_TOKEN:
        client = WebClient(token=config.SLACK_BOT_TOKEN)
        for target in target_list:
            kind, tid = target
            ok, posted_ts = _send_to_target(client, target, text, thread_ts=thread_ts)
            outcomes[f"{kind}:{tid}"] = ok
            # Capture the channel post's ts as the thread parent. DMs aren't
            # used for threading (one DM channel per recipient already).
            if ok and kind == "channel" and posted_ts and channel_ts is None:
                channel_ts = posted_ts
    else:
        for kind, tid in target_list:
            outcomes[f"{kind}:{tid}"] = False

    if channel_ts is not None:
        outcomes["channel_ts"] = channel_ts

    # 3) Webhook fallback only if BOTH primary (drive queue) AND all bot
    # targets failed.
    bot_targets_ok = any(
        v for k, v in outcomes.items()
        if k not in ("drive_queue", "channel_ts") and isinstance(v, bool)
    )
    if not outcomes["drive_queue"] and not bot_targets_ok:
        outcomes["webhook_fallback"] = _post_via_webhook(text)

    return outcomes


# ─────────────────────────────────────────────────────────────────────────────
# Public API — called by scripts/smart_ramp_poller.py
# ─────────────────────────────────────────────────────────────────────────────


def notify_success(ramp_record, result: dict, version: int = 1) -> dict:
    """Launch-done summary. Threaded under notify_new_ramp's parent."""
    text = build_success_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=ramp_record.requester_name or "—",
        per_cohort=result.get("per_cohort") or [],
        version=version,
        extra_platform_campaigns=result.get("extra_platform_campaigns") or {},
        manual_handoff_urls=result.get("manual_handoff_urls") or {},
    )
    return _send_to_all_targets(
        text, ramp_id=ramp_record.id, thread_ts=_lookup_thread_ts(ramp_record.id),
    )


def notify_new_ramp(ramp_record) -> dict:
    """Detection ping — Pranav DM ONLY (observability).

    Routed to SLACK_VERBOSE_TARGETS so Diego + the channel are not pinged on
    every detection. The team-facing per-ramp thread starts at
    notify_briefs_ready (prep done), which captures the channel thread parent.
    Posts top-level (no thread_ts) so it lands cleanly in Pranav's DM.
    """
    text = build_new_ramp_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=getattr(ramp_record, "requester_name", "") or "—",
        summary=getattr(ramp_record, "summary", "") or "",
    )
    return _send_to_all_targets(
        text, ramp_id=ramp_record.id, targets=config.SLACK_VERBOSE_TARGETS,
    )


def _lookup_thread_ts(ramp_id: str) -> Optional[str]:
    """Read the per-ramp Slack thread ts from Postgres so lifecycle pings
    after the parent (new_ramp) post can reply in-thread instead of
    top-level. Returns None on miss → caller posts top-level."""
    try:
        from src.ui_decisions import get_slack_thread_ts
        return get_slack_thread_ts(ramp_id)
    except Exception as exc:
        log.debug("get_slack_thread_ts failed: %s", exc)
        return None


def notify_briefs_ready(
    ramp_record,
    *,
    briefs_generated: int,
    cohorts_count: int,
    fell_back_to_legacy: bool = False,
) -> dict:
    """Prep-done ping — TOP-LEVEL parent of the per-ramp team thread.

    This is the FIRST of exactly two team-facing messages per ramp (Diego +
    channel + Pranav). It posts top-level and captures the channel post ts as
    the thread parent so notify_success (campaigns-ready) replies in-thread.
    Best-effort: a Postgres outage just means notify_success posts top-level
    instead of threading."""
    text = build_briefs_ready_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=getattr(ramp_record, "requester_name", "") or "—",
        briefs_generated=briefs_generated,
        cohorts_count=cohorts_count,
        fell_back_to_legacy=fell_back_to_legacy,
    )
    out = _send_to_all_targets(text, ramp_id=ramp_record.id)
    posted_ts = out.get("channel_ts")
    if posted_ts:
        try:
            from src.ui_decisions import set_slack_thread_ts
            set_slack_thread_ts(ramp_record.id, posted_ts)
            log.info("Slack thread parent ts=%s persisted for ramp=%s", posted_ts, ramp_record.id)
        except Exception as exc:
            log.warning("set_slack_thread_ts failed (non-fatal): %s", exc)
    return out


def notify_escalation(
    ramp_record, error_class: str, traceback_text: Optional[str]
) -> dict:
    """Send the escalation Slack message via the Drive queue (primary) + bot/webhook
    fallback. Returns per-target outcomes."""
    text = build_escalation_message(
        ramp_id=ramp_record.id,
        project_name=ramp_record.project_name or "—",
        requester_name=ramp_record.requester_name or "—",
        error_class=error_class,
        traceback_text=traceback_text,
    )
    return _send_to_all_targets(text, ramp_id=ramp_record.id)

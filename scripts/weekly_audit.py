"""
Weekly ads audit entrypoint — wired to .github/workflows/weekly_audit.yml
(cron Monday 17:00 UTC = 09:00 PT).

Runs `src.ads_auditor.run_weekly_audit` over the last `--lookback-days`
(default 21) of `data/campaign_registry.json`, renders the structured
findings into a Slack message, and posts to `SLACK_AUDIT_CHANNEL`
(defaults to the same channel as ramp launch notifications).

Usage:
    doppler run -- python3 scripts/weekly_audit.py
    doppler run -- python3 scripts/weekly_audit.py --lookback-days 7
    doppler run -- python3 scripts/weekly_audit.py --dry-run   # build + log message, don't post
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import sys
from pathlib import Path

# Make src/ importable when run from repo root.
ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import config
from src.ads_auditor import run_weekly_audit

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
log = logging.getLogger("weekly_audit")


# ── Slack message rendering ─────────────────────────────────────────────


_PLATFORM_DISPLAY = {
    "linkedin": ("LinkedIn", "SLACK_DIEGO_USER_ID"),
    "meta":     ("Meta",     "SLACK_DIEGO_USER_ID"),
    "google":   ("Google",   "SLACK_BRYAN_USER_ID"),
}


def _health_emoji(score: int | None) -> str:
    if score is None:
        return "⚪"   # n/a (empty platform)
    if score >= 80:
        return "🟢"
    if score >= 60:
        return "🟡"
    if score >= 40:
        return "🟠"
    return "🔴"


def render_slack_message(audit: dict) -> str:
    """Build the mrkdwn body for the weekly audit Slack post."""
    lookback = audit.get("lookback_days", 21)
    as_of    = audit.get("as_of_utc", "—")
    total    = audit.get("total_audited", 0)
    platforms = audit.get("platforms", {})

    lines: list[str] = []
    lines.append(f"*📊 Outlier Weekly Ads Audit — last {lookback} days*")
    lines.append(f"_As of {as_of} · {total} campaigns audited across {len([p for p,d in platforms.items() if d.get('campaigns_audited', 0) > 0])} platforms_")
    lines.append("")

    for platform_key, finding in platforms.items():
        name, mention_attr = _PLATFORM_DISPLAY.get(
            platform_key, (platform_key.title(), "SLACK_DIEGO_USER_ID")
        )
        mention_id = getattr(config, mention_attr, "")
        mention = f"<@{mention_id}>" if mention_id else ""
        score = finding.get("health_score")
        emoji = _health_emoji(score)
        n_camp = finding.get("campaigns_audited", 0)
        spend = finding.get("total_spend_usd", 0.0)
        imp   = finding.get("total_impressions", 0)

        header_bits = [f"{emoji} *{name}*"]
        if score is not None:
            header_bits.append(f"score {score}/100")
        header_bits.append(f"{n_camp} campaigns")
        if spend:
            header_bits.append(f"${spend:,.0f} spend")
        if imp:
            header_bits.append(f"{imp:,} imp")
        if mention:
            header_bits.append(mention)
        lines.append(" · ".join(header_bits))

        summary = finding.get("executive_summary", "").strip()
        if summary:
            lines.append(f"> {summary}")

        issues = finding.get("top_issues") or []
        if issues:
            lines.append("• *Top issues:*")
            for issue in issues:
                lines.append(f"   – {issue}")

        recs = finding.get("top_recommendations") or []
        if recs:
            lines.append("• *Recommendations:*")
            for rec in recs:
                lines.append(f"   – {rec}")

        lines.append("")

    tuan = getattr(config, "SLACK_TUAN_USER_ID", "")
    if tuan:
        lines.append(f"_cc <@{tuan}> for visibility_")

    return "\n".join(lines).rstrip()


# ── Slack send (delegated to smart_ramp_notifier helpers) ───────────────


def _post_to_slack(text: str, channel_id: str) -> bool:
    """Post `text` to the given Slack channel. Reuses the canonical
    `_send_to_target` from smart_ramp_notifier so we get the same
    bot-token-then-webhook fallback ordering for free.
    """
    try:
        from slack_sdk import WebClient
        from src.smart_ramp_notifier import _send_to_target, _post_via_webhook
    except ImportError as exc:
        log.error("Could not import Slack helpers: %s", exc)
        return False

    token = getattr(config, "SLACK_BOT_TOKEN", "")
    if token:
        client = WebClient(token=token)
        ok, _ts = _send_to_target(client, ("channel", channel_id), text)
        if ok:
            return True
        log.warning("Bot-token post to %s failed — falling back to webhook", channel_id)

    return _post_via_webhook(text)


# ── CLI ─────────────────────────────────────────────────────────────────


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--lookback-days", type=int, default=int(os.getenv("AUDIT_LOOKBACK_DAYS", "21")),
        help="How many days of campaigns to audit (default: 21)",
    )
    parser.add_argument(
        "--registry-path", default="data/campaign_registry.json",
        help="Path to campaign_registry.json (default: data/campaign_registry.json)",
    )
    parser.add_argument(
        "--channel", default=getattr(config, "SLACK_AUDIT_CHANNEL", ""),
        help="Slack channel ID to post to (default: $SLACK_AUDIT_CHANNEL)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Build the audit + message but don't post to Slack",
    )
    parser.add_argument(
        "--json-out", default="",
        help="Optional path to write the raw audit findings as JSON",
    )
    args = parser.parse_args()

    log.info("Running weekly audit: lookback=%dd, registry=%s, channel=%s, dry_run=%s",
             args.lookback_days, args.registry_path, args.channel or "(none)", args.dry_run)

    audit = run_weekly_audit(
        lookback_days=args.lookback_days,
        registry_path=args.registry_path,
    )

    if args.json_out:
        out_path = Path(args.json_out)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(json.dumps(audit, indent=2, default=str))
        log.info("Wrote raw findings to %s", out_path)

    message = render_slack_message(audit)
    log.info("Audit summary message:\n%s", message)

    if args.dry_run:
        log.info("--dry-run set — skipping Slack post")
        return 0

    if not args.channel:
        log.error("No Slack channel configured (SLACK_AUDIT_CHANNEL empty and no --channel arg)")
        return 2

    ok = _post_to_slack(message, args.channel)
    if not ok:
        log.error("All Slack send paths failed — audit message NOT delivered")
        return 1

    log.info("Posted audit summary to %s", args.channel)
    return 0


if __name__ == "__main__":
    sys.exit(main())

"""
Weekly report poster — runs every Monday 9 AM IST (3:30 AM UTC).

Generates InMail + Static weekly reports and posts each to Slack DM.
Run via cron: 30 3 * * 1

Usage:
    PYTHONPATH=. python3 scripts/post_weekly_reports.py
"""
import logging
import sys
from pathlib import Path

# Load .env before importing anything that reads config
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
import config

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def _post_to_slack(text: str) -> None:
    """Post text to Slack.

    Tries Bot Token (chat.postMessage) first; falls back to Incoming Webhook.
    Bot Token requires SLACK_BOT_TOKEN in .env and the bot invited to the
    target DM (user ID stored in config.SLACK_REPORT_USER).
    """
    # Split into <=3000-char chunks (Slack block limit)
    chunks = [text[i : i + 3000] for i in range(0, len(text), 3000)]

    if config.SLACK_BOT_TOKEN:
        from slack_sdk import WebClient
        from slack_sdk.errors import SlackApiError

        client = WebClient(token=config.SLACK_BOT_TOKEN)
        for chunk in chunks:
            try:
                client.chat_postMessage(
                    channel=config.SLACK_REPORT_USER,
                    text=chunk,
                )
            except SlackApiError as exc:
                log.error("Slack Bot Token post failed: %s", exc.response["error"])
                return
        log.info("Posted to Slack via Bot Token (user=%s)", config.SLACK_REPORT_USER)
        return

    webhook_url = config.SLACK_WEBHOOK_URL
    if not webhook_url:
        log.error(
            "Neither SLACK_BOT_TOKEN nor SLACK_WEBHOOK_URL set in .env — cannot post report"
        )
        return
    for chunk in chunks:
        resp = requests.post(webhook_url, json={"text": chunk}, timeout=10)
        if not resp.ok:
            log.error("Webhook post failed: %s %s", resp.status_code, resp.text)
            return
    log.info("Posted to Slack via webhook")


def main() -> None:
    log.info("=== Weekly InMail Report ===")
    try:
        from src.inmail_weekly_report import run_weekly_report as inmail_report
        inmail_text = inmail_report()
        log.info("InMail report generated (%d chars)", len(inmail_text))
        _post_to_slack(inmail_text)
    except Exception as exc:
        log.error("InMail report failed: %s", exc, exc_info=True)

    log.info("=== Weekly Static Report ===")
    try:
        from src.static_weekly_report import run_weekly_report as static_report
        static_text = static_report()
        log.info("Static report generated (%d chars)", len(static_text))
        _post_to_slack(static_text)
    except Exception as exc:
        log.error("Static report failed: %s", exc, exc_info=True)


if __name__ == "__main__":
    main()

"""
Weekly report poster — runs every Monday 9 AM IST (3:30 AM UTC).

Generates InMail + Static weekly reports and posts each to Slack DM.
Run via cron: 30 3 * * 1

Usage:
    PYTHONPATH=. python3 scripts/post_weekly_reports.py
"""
import logging
import sys
from datetime import datetime
from pathlib import Path

# Load .env before importing anything that reads config
from dotenv import load_dotenv
load_dotenv(Path(__file__).resolve().parent.parent / ".env")

import requests
import config
from src.feedback_agent import FeedbackAgent
from src.redash_db import RedashClient

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


def post_weekly_feedback_alert() -> dict:
    """
    Analyze creative and cohort performance; post underperformers to Slack.
    Called as part of weekly reporting cycle (Monday 9 AM IST).

    Returns dict with underperformers, hypotheses, and alert timestamp,
    or empty dict on error.
    """
    try:
        redash_client = RedashClient()
        agent = FeedbackAgent(redash_client)

        # Analyze past 7 days of creative + cohort performance
        underperformers = agent.identify_underperforming_cohorts(days_back=7)
        hypothesis_summary = agent.analyze_creative_performance(days_back=7)

        # Generate Slack alert text
        alert_text = agent.generate_slack_alert(underperformers, hypothesis_summary)

        # Post to Slack
        _post_to_slack(alert_text)

        log.info(
            "Feedback alert posted; underperformers: %d",
            len(underperformers),
        )

        # Return underperformers for potential downstream use (experiment_scientist_agent)
        return {
            "underperformers": underperformers,
            "hypotheses": hypothesis_summary,
            "alert_posted_at": datetime.utcnow().isoformat(),
        }
    except Exception as e:
        log.error("Feedback alert failed: %s", e, exc_info=True)
        return {}


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

    log.info("=== Campaign Monitor Summary ===")
    try:
        from src.sheets import SheetsClient
        from src.campaign_monitor import read_monitor_summary
        sheets = SheetsClient()
        monitor_text = read_monitor_summary(sheets)
        if monitor_text:
            log.info("Monitor summary generated (%d chars)", len(monitor_text))
            _post_to_slack(monitor_text)
        else:
            log.info("Monitor summary: no data (monitor has not run yet or Monitor tab is empty)")
    except Exception as exc:
        log.error("Monitor summary failed: %s", exc, exc_info=True)

    log.info("=== Weekly Feedback Alert ===")
    try:
        feedback_result = post_weekly_feedback_alert()
        if feedback_result:
            log.info(
                "Feedback alert posted; underperformers: %d",
                len(feedback_result.get("underperformers", [])),
            )
    except Exception as exc:
        log.error("Feedback alert failed: %s", exc, exc_info=True)


if __name__ == "__main__":
    main()

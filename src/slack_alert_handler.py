"""
Slack Reaction Event Handler — Feedback Loop Integration

Listens for Slack emoji reactions (👍 pause cohort, 🧪 test new angles)
and routes them to registered callback functions for downstream processing.

Phase 2.5 feedback loop — FEED-08 artifact.

Usage:
    from src.slack_alert_handler import SlackReactionHandler, parse_cohort_from_message
    from slack_sdk import WebClient

    handler = SlackReactionHandler(slack_bot_token="xoxb-...")
    handler.register_reaction_callback('👍', on_pause_cohort)
    handler.register_reaction_callback('🧪', on_test_new_angles)

    # In an event listener:
    event = {...}  # from Slack Events API
    result = handler.handle_reaction_event(event)
"""

import logging
import re
from typing import Any, Callable, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

log = logging.getLogger(__name__)

# Emoji mapping: Slack emoji name → action type
EMOJI_ACTION_MAP = {
    "thumbsup": "PAUSE",            # 👍 = pause cohort
    "lab": "TEST_NEW_ANGLES",       # 🧪 = test new angles
}


class SlackReactionHandler:
    """
    Handler for Slack reaction_added events.

    Parses reaction context (emoji, user, cohort_name), validates,
    and invokes registered callbacks.
    """

    def __init__(self, slack_bot_token: str, callback_registry: dict = None):
        """
        Initialize handler with Slack bot token.

        Args:
            slack_bot_token: Bot user OAuth token (starts with xoxb-)
            callback_registry: Optional dict mapping emoji → callback function
        """
        self.token = slack_bot_token
        self.client = WebClient(token=self.token)
        self.callbacks: dict[str, Callable] = callback_registry or {}

    def register_reaction_callback(self, emoji: str, callback: Callable) -> None:
        """
        Register a callback function for a specific emoji reaction.

        Args:
            emoji: Emoji character (e.g. '👍', '🧪')
            callback: Async or sync function to invoke on reaction
        """
        self.callbacks[emoji] = callback
        log.debug("Registered callback for emoji=%s", emoji)

    def handle_reaction_event(self, event: dict) -> dict:
        """
        Process a Slack reaction_added event.

        Input event schema (from Slack Events API):
        {
            "type": "reaction_added",
            "user": "U...",
            "reaction": "thumbsup",  # Slack emoji name
            "item": {
                "type": "message",
                "channel": "C...",
                "ts": "1234567890.123456"
            }
        }

        Returns:
        {
            "success": bool,
            "action": str (action_type or error reason),
            "cohort": str (cohort_name or empty),
            "message": str (optional context)
        }
        """
        try:
            # Extract event components
            reaction_emoji_name = event.get("reaction", "")  # e.g. "thumbsup"
            user_id = event.get("user", "")
            item = event.get("item", {})
            channel_id = item.get("channel", "")
            message_ts = item.get("ts", "")

            # Map emoji name to action type
            action_type = EMOJI_ACTION_MAP.get(reaction_emoji_name)
            if not action_type:
                log.warning("Unknown emoji reaction: %s", reaction_emoji_name)
                return {
                    "success": False,
                    "action": "unknown_emoji",
                    "cohort": "",
                }

            # Fetch original message from Slack to extract cohort_name
            try:
                response = self.client.conversations_history(
                    channel=channel_id,
                    latest=message_ts,
                    inclusive=True,
                    limit=1,
                )
                messages = response.get("messages", [])
                if not messages:
                    log.warning("Message not found: channel=%s ts=%s", channel_id, message_ts)
                    return {
                        "success": False,
                        "action": "message_not_found",
                        "cohort": "",
                    }

                message_text = messages[0].get("text", "")
            except SlackApiError as e:
                log.error("Slack API error fetching message: %s", e.response.get("error"))
                return {
                    "success": False,
                    "action": "slack_api_error",
                    "cohort": "",
                    "message": str(e),
                }

            # Parse cohort_name from message text
            cohort_name = parse_cohort_from_message(message_text)
            if not cohort_name:
                log.error("Could not parse cohort from message: %s", message_text[:100])
                return {
                    "success": False,
                    "action": "cohort_parse_error",
                    "cohort": "",
                }

            # Validate reaction + cohort
            if not self.validate_reaction(reaction_emoji_name, cohort_name):
                return {
                    "success": False,
                    "action": "validation_failed",
                    "cohort": cohort_name,
                }

            log.info(
                "Reaction %s from user %s on cohort %s",
                reaction_emoji_name,
                user_id,
                cohort_name,
            )

            # Build action context
            action_context = {
                "emoji": reaction_emoji_name,
                "action_type": action_type,
                "cohort_name": cohort_name,
                "user_id": user_id,
                "timestamp": message_ts,
                "channel_id": channel_id,
                "message_text": message_text,
            }

            # Look up and invoke callback if registered
            emoji_char = "👍" if reaction_emoji_name == "thumbsup" else "🧪"
            if emoji_char in self.callbacks:
                callback = self.callbacks[emoji_char]
                log.debug("Triggering callback for action=%s", action_type)
                try:
                    # Callback is typically async; caller responsible for asyncio.run()
                    callback(action_context)
                except Exception as e:
                    log.error("Callback failed: %s", e, exc_info=True)
                    # Don't re-raise; return clean failure
                    return {
                        "success": False,
                        "action": "callback_error",
                        "cohort": cohort_name,
                        "message": str(e),
                    }
            else:
                log.debug("No callback registered for emoji=%s", emoji_char)

            return {
                "success": True,
                "action": action_type,
                "cohort": cohort_name,
            }

        except Exception as e:
            log.error("Unexpected error in handle_reaction_event: %s", e, exc_info=True)
            return {
                "success": False,
                "action": "unexpected_error",
                "cohort": "",
                "message": str(e),
            }

    def validate_reaction(self, emoji_name: str, cohort_name: str) -> bool:
        """
        Validate emoji and cohort_name.

        Args:
            emoji_name: Slack emoji name (e.g. "thumbsup")
            cohort_name: Extracted cohort name

        Returns True if valid, False otherwise (logs warning).
        """
        if emoji_name not in EMOJI_ACTION_MAP:
            log.warning("Invalid emoji: %s (must be 'thumbsup' or 'lab')", emoji_name)
            return False

        if not cohort_name or not isinstance(cohort_name, str):
            log.warning("Invalid cohort_name: %s", cohort_name)
            return False

        return True


def parse_cohort_from_message(text: str) -> Optional[str]:
    """
    Extract cohort name from alert message text.

    Looks for pattern: "Cohort {NAME}" where NAME is alphanumeric + underscore.

    Args:
        text: Message text from Slack alert

    Returns:
        Cohort name string, or None if not found.
    """
    if not text:
        return None

    # Pattern: "Cohort " followed by one or more word chars or underscores
    pattern = r"Cohort\s+([A-Za-z_][A-Za-z0-9_]*)"
    match = re.search(pattern, text)

    if match:
        cohort_name = match.group(1)
        log.debug("Parsed cohort_name=%s from message", cohort_name)
        return cohort_name

    log.warning("No cohort pattern found in message")
    return None

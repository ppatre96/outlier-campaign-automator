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

import asyncio
import logging
import re
from datetime import datetime
from typing import Any, Callable, Optional

from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

log = logging.getLogger(__name__)

# Emoji mapping: Slack emoji name → action type
EMOJI_ACTION_MAP = {
    "thumbsup": "PAUSE",            # 👍 = pause cohort
    "lab": "TEST_NEW_ANGLES",       # 🧪 = test new angles
}


async def on_pause_cohort(action_context: dict) -> dict:
    """
    Handle 👍 reaction: user wants to pause the underperforming cohort.

    action_context: {emoji: 'thumbsup', cohort_name: 'DATA_ANALYST', user_id: 'U...', timestamp: '...'}

    Action:
    1. Log pause decision with timestamp + user
    2. Trigger ReanalysisOrchestrator reanalysis on fresh screening data (exclude paused cohort)
    3. Return {success: true, action: 'PAUSE', cohort: ..., triggered_by: ..., next_step: ...}
    """
    cohort = action_context.get("cohort_name", "")
    user = action_context.get("user_id", "")
    log.info("Pause request for cohort %s from user %s", cohort, user)
    log.info("Reaction thumbsup from %s on %s", user, cohort)

    try:
        from src.reanalysis_loop import ReanalysisOrchestrator
        orchestrator = ReanalysisOrchestrator()
        log.info("Callback invoked: on_pause_cohort with args cohort=%s, reason=user_pause", cohort)
        await orchestrator.trigger_reanalysis(cohort_to_exclude=cohort, reason="user_pause")
        next_step = "reanalysis_queued"
    except Exception as e:
        log.error("Failed to queue reanalysis for pause: %s", str(e))
        return {
            "success": True,
            "action": "PAUSE",
            "cohort": cohort,
            "triggered_by": user,
            "next_step": "reanalysis_queue_failed",
            "warning": "reanalysis_queue_failed",
        }

    return {
        "success": True,
        "action": "PAUSE",
        "cohort": cohort,
        "triggered_by": user,
        "next_step": next_step,
    }


async def on_test_new_angles(action_context: dict) -> dict:
    """
    Handle 🧪 reaction: user wants to test new angles for this cohort.

    action_context: {emoji: 'lab', cohort_name: 'ML_ENGINEER', user_id: 'U...', ...}

    Action:
    1. Log test request with user + cohort
    2. Boost priority of pending experiments for this cohort in backlog
    3. Trigger reanalysis on fresh data to discover angle variations
    4. Return {success: true, action: 'TEST_NEW_ANGLES', cohort: ..., experiments_boosted: N}
    """
    cohort = action_context.get("cohort_name", "")
    user = action_context.get("user_id", "")
    log.info("Test new angles request for cohort %s from user %s", cohort, user)
    log.info("Reaction lab from %s on %s", user, cohort)

    # Boost experiment priority in backlog
    boosted_count = 0
    try:
        from src.memory import ExperimentBacklog
        backlog = ExperimentBacklog()
        for exp in backlog.backlog:
            if exp.get("cohort") == cohort and exp.get("status") == "pending":
                exp["priority_score"] = exp.get("priority_score", 1.0) * 1.5
                boosted_count += 1
        backlog.save()
    except Exception as e:
        log.error("Failed to boost experiment priority: %s", str(e))

    # Trigger reanalysis
    try:
        from src.reanalysis_loop import ReanalysisOrchestrator
        orchestrator = ReanalysisOrchestrator()
        log.info("Callback invoked: on_test_new_angles with args cohort=%s, reason=user_test_request", cohort)
        await orchestrator.trigger_reanalysis(cohort_to_focus=cohort, reason="user_test_request")
    except Exception as e:
        log.error("Failed to queue reanalysis for test new angles: %s", str(e))
        return {
            "success": True,
            "action": "TEST_NEW_ANGLES",
            "cohort": cohort,
            "triggered_by": user,
            "experiments_boosted": boosted_count,
            "warning": "reanalysis_queue_failed",
        }

    return {
        "success": True,
        "action": "TEST_NEW_ANGLES",
        "cohort": cohort,
        "triggered_by": user,
        "experiments_boosted": boosted_count,
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

        Registers default callbacks for 'thumbsup' (pause) and 'lab' (test new angles)
        using the module-level on_pause_cohort and on_test_new_angles functions.

        Args:
            slack_bot_token: Bot user OAuth token (starts with xoxb-)
            callback_registry: Optional dict mapping emoji name → callback function
                               (overrides default callbacks if provided)
        """
        self.token = slack_bot_token
        self.client = WebClient(token=self.token)
        # Default callbacks for known emoji reactions
        default_callbacks: dict[str, Callable] = {
            "thumbsup": on_pause_cohort,
            "lab": on_test_new_angles,
        }
        # Allow caller to override defaults
        if callback_registry:
            default_callbacks.update(callback_registry)
        self.callbacks: dict[str, Callable] = default_callbacks

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

            # Look up and invoke callback if registered (by emoji name: 'thumbsup', 'lab')
            callback = self.callbacks.get(reaction_emoji_name)
            if callback:
                log.debug("Triggering callback for action=%s", action_type)
                try:
                    # Support both sync and async callbacks
                    if asyncio.iscoroutinefunction(callback):
                        result = asyncio.run(callback(action_context))
                    else:
                        result = callback(action_context)
                    return result
                except Exception as e:
                    log.error("Callback invocation failed: %s", str(e), exc_info=True)
                    return {
                        "success": False,
                        "action": "callback_error",
                        "cohort": cohort_name,
                        "message": str(e),
                    }
            else:
                log.debug("No callback registered for emoji=%s", reaction_emoji_name)

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

# Phase 2.5-04: Reaction callbacks for reanalysis
import asyncio
from src.reanalysis_loop import ReanalysisOrchestrator
from src.memory import ExperimentBacklog


async def on_pause_cohort(action_context: dict) -> dict:
    """Handle 👍 reaction: pause underperforming cohort."""
    cohort = action_context['cohort_name']
    user = action_context['user_id']
    log.info("Pause cohort: %s (user=%s)", cohort, user)
    
    orchestrator = ReanalysisOrchestrator()
    new_cohorts = await asyncio.to_thread(
        orchestrator.trigger_reanalysis,
        cohort_to_exclude=cohort,
        reason='user_pause'
    )
    
    return {
        'success': True,
        'action': 'PAUSE',
        'cohort': cohort,
        'triggered_by': user,
        'reanalysis_cohorts': len(new_cohorts)
    }


async def on_test_new_angles(action_context: dict) -> dict:
    """Handle 🧪 reaction: test new angles for cohort."""
    cohort = action_context['cohort_name']
    user = action_context['user_id']
    log.info("Test new angles: %s (user=%s)", cohort, user)
    
    backlog = ExperimentBacklog()
    boosted = 0
    for exp in backlog.backlog:
        if exp['cohort'] == cohort and exp.get('status') == 'pending':
            exp['priority_score'] = exp.get('priority_score', 0) * 1.5
            boosted += 1
    backlog.save()
    
    orchestrator = ReanalysisOrchestrator()
    new_cohorts = await asyncio.to_thread(
        orchestrator.trigger_reanalysis,
        cohort_to_focus=cohort,
        reason='user_test_request'
    )
    
    return {
        'success': True,
        'action': 'TEST_NEW_ANGLES',
        'cohort': cohort,
        'triggered_by': user,
        'experiments_boosted': boosted
    }

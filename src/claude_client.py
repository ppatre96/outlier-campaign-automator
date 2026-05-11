"""
Shared Anthropic SDK client helper.

All Claude copy-gen calls (ICP extraction, copy variants, InMail, rewriter,
campaign feedback) route through here so there's one place to tune caching,
model version, and timeout.

Usage:
    from src.claude_client import call_claude

    text = call_claude(
        messages=[{"role": "user", "content": prompt}],
        max_tokens=2048,
    )

Prompt caching: pass `cache_system=True` (or a literal system string) when the
system context is reused across multiple calls in the same ramp run (e.g., the
copy-gen preamble + vocab rules block). The Anthropic SDK sends a
cache_control=ephemeral breakpoint on the system block; subsequent calls within
the 5-min TTL window return a cache hit and cost ~10x less.
"""
from __future__ import annotations

import logging
import threading

import anthropic

import config

log = logging.getLogger(__name__)

_client: anthropic.Anthropic | None = None
# Double-checked locking around lazy client init — required once Phase 3.2
# parallelizes copy gen across (cohort × geo) and multiple threads call
# call_claude simultaneously. Without this, racing threads can each
# construct their own anthropic.Anthropic (resource leak, not crash). The
# fast-path `if _client is None` outside the lock keeps warm-call overhead
# at zero.
_client_lock = threading.Lock()


def get_client() -> anthropic.Anthropic:
    global _client
    if _client is None:                          # fast path — no lock once warm
        with _client_lock:
            if _client is None:                  # re-check inside the lock
                _client = anthropic.Anthropic(api_key=config.ANTHROPIC_API_KEY)
    return _client


def call_claude(
    messages: list[dict],
    *,
    max_tokens: int = 2048,
    system: str = "",
    cache_system: bool = False,
    model: str = "",
) -> str:
    """
    Call Claude and return the text response.

    Args:
        messages:     List of {"role": "user"|"assistant", "content": "..."} dicts.
        max_tokens:   Maximum tokens in the response.
        system:       Optional system prompt (extracted from messages if needed).
        cache_system: If True and system is non-empty, adds cache_control=ephemeral
                      to the system block. Use when this system prompt is repeated
                      across many calls (vocab rules, copy preamble, etc.).
        model:        Override model (defaults to config.ANTHROPIC_MODEL).
    Returns:
        Response text string.
    Raises:
        anthropic.APIError on API failures (callers should catch).
    """
    client = get_client()
    used_model = model or config.ANTHROPIC_MODEL

    kwargs: dict = {
        "model":      used_model,
        "max_tokens": max_tokens,
        "messages":   messages,
    }

    if system:
        if cache_system:
            kwargs["system"] = [
                {
                    "type": "text",
                    "text": system,
                    "cache_control": {"type": "ephemeral"},
                }
            ]
        else:
            kwargs["system"] = system

    resp = client.messages.create(**kwargs)
    return resp.content[0].text

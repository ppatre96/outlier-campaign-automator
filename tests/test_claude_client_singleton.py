"""Thread-safety of `src/claude_client.get_client()` lazy singleton.

Phase 3.2 parallelizes `build_copy_variants` across (cohort × geo) inside
`_process_static_campaigns`. The first-ever `call_claude` of a process can
land in N threads simultaneously when the executor warms up — without
locking, each thread would construct its own anthropic.Anthropic instance.
This test pins the contract: anthropic.Anthropic.__init__ must run exactly
once regardless of how many threads call get_client() concurrently.
"""
from __future__ import annotations

import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _reset_singleton():
    """Ensure each test starts with a fresh _client = None."""
    from src import claude_client
    claude_client._client = None


def test_get_client_constructs_exactly_once_under_32_threads():
    """32 threads behind a Barrier all call get_client() simultaneously.
    Patch anthropic.Anthropic to a MagicMock so we can count instantiations
    without touching the real network."""
    _reset_singleton()

    barrier = threading.Barrier(32)
    results: list = [None] * 32

    with patch("src.claude_client.anthropic.Anthropic") as anthropic_ctor:
        # Each construction returns a distinct MagicMock instance so we can
        # also assert all callers received the SAME (single) instance.
        anthropic_ctor.side_effect = lambda **kw: MagicMock(name="anthropic_inst")

        from src.claude_client import get_client

        def worker(idx: int):
            barrier.wait()  # release all 32 threads at once
            results[idx] = get_client()

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(32)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

    assert anthropic_ctor.call_count == 1, (
        f"anthropic.Anthropic.__init__ ran {anthropic_ctor.call_count} times; "
        "expected exactly 1 under concurrent get_client calls"
    )
    # All threads must see the SAME instance.
    assert all(r is results[0] for r in results), (
        "concurrent get_client() returned different instances — singleton broken"
    )


def test_get_client_returns_same_instance_on_repeated_calls():
    """Sanity: sequential repeated calls hit the cached singleton path."""
    _reset_singleton()
    with patch("src.claude_client.anthropic.Anthropic") as anthropic_ctor:
        anthropic_ctor.return_value = MagicMock(name="anthropic_inst")
        from src.claude_client import get_client
        a = get_client()
        b = get_client()
        c = get_client()
    assert a is b is c, "repeated get_client() must return the same instance"


def test_warm_path_does_not_acquire_lock():
    """Once the singleton is constructed, subsequent calls must bypass the
    lock entirely (fast path). Patch the lock with a MagicMock and assert
    __enter__ is never called once warm."""
    _reset_singleton()
    from src import claude_client

    with patch("src.claude_client.anthropic.Anthropic") as anthropic_ctor:
        anthropic_ctor.return_value = MagicMock(name="anthropic_inst")
        # Warm up first
        first = claude_client.get_client()

    # Now replace the lock with a MagicMock that records __enter__ calls
    fake_lock = MagicMock()
    fake_lock.__enter__ = MagicMock(return_value=None)
    fake_lock.__exit__ = MagicMock(return_value=False)
    with patch.object(claude_client, "_client_lock", fake_lock):
        for _ in range(50):
            claude_client.get_client()
    assert fake_lock.__enter__.call_count == 0, (
        "warm-path get_client() should NOT acquire the lock; "
        f"got {fake_lock.__enter__.call_count} acquisitions"
    )
    # Cleanup so subsequent tests start fresh
    _reset_singleton()

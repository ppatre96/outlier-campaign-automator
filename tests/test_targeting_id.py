"""Unit tests for src.targeting_id — sequential allocation, idempotency,
Base36 encoding, persistence across processes, and edge cases."""
from __future__ import annotations

import json
import os
import sys
import threading
from pathlib import Path

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import targeting_id as tid


# ── Encoding ────────────────────────────────────────────────────────────


def test_to_base36_basic_values():
    assert tid._to_base36(0) == "0"
    assert tid._to_base36(1) == "1"
    assert tid._to_base36(9) == "9"
    assert tid._to_base36(10) == "A"
    assert tid._to_base36(35) == "Z"
    assert tid._to_base36(36) == "10"
    assert tid._to_base36(1295) == "ZZ"
    assert tid._to_base36(1296) == "100"


def test_format_targeting_id_zero_padded_width():
    assert tid._format_targeting_id(1) == "TAR-0001"
    assert tid._format_targeting_id(9) == "TAR-0009"
    assert tid._format_targeting_id(10) == "TAR-000A"
    assert tid._format_targeting_id(35) == "TAR-000Z"
    assert tid._format_targeting_id(36) == "TAR-0010"


def test_format_targeting_id_expands_beyond_4_chars():
    """At n = 36^4 = 1,679,616, the encoded part is exactly 5 chars: '10000'.
    For n < 36^4 it stays 4 chars wide via zero-padding."""
    assert tid._format_targeting_id(36 ** 4 - 1) == "TAR-ZZZZ"   # last 4-char value
    assert tid._format_targeting_id(36 ** 4)     == "TAR-10000"  # first 5-char value
    assert tid._format_targeting_id(36 ** 4 + 1) == "TAR-10001"


def test_format_targeting_id_rejects_zero_and_negative():
    with pytest.raises(ValueError):
        tid._format_targeting_id(0)
    with pytest.raises(ValueError):
        tid._format_targeting_id(-1)


# ── Allocation ──────────────────────────────────────────────────────────


def test_get_or_assign_sequential_allocation(tmp_path):
    reg = tmp_path / "tar_registry.json"
    a = tid.get_or_assign_targeting_id("skills__python",      "latin_american", registry_path=reg)
    b = tid.get_or_assign_targeting_id("skills__python",      "south_asian",    registry_path=reg)
    c = tid.get_or_assign_targeting_id("skills__java",        "latin_american", registry_path=reg)
    assert a == "TAR-0001"
    assert b == "TAR-0002"
    assert c == "TAR-0003"


def test_get_or_assign_idempotent(tmp_path):
    """Calling twice with the same args returns the same ID, no new allocation."""
    reg = tmp_path / "tar_registry.json"
    a1 = tid.get_or_assign_targeting_id("skills__python", "latin_american", registry_path=reg)
    a2 = tid.get_or_assign_targeting_id("skills__python", "latin_american", registry_path=reg)
    assert a1 == a2 == "TAR-0001"
    # next_id should NOT have advanced past 2 (only one allocation)
    data = json.loads(reg.read_text())
    assert data["next_id"] == 2


def test_get_or_assign_persists_to_disk(tmp_path):
    """Closing + re-reading the registry preserves IDs."""
    reg = tmp_path / "tar_registry.json"
    tid.get_or_assign_targeting_id("a", "x", registry_path=reg)
    tid.get_or_assign_targeting_id("b", "y", registry_path=reg)
    # Now read it back via a different module-internal helper
    data = tid._load(reg)
    assert data["registry"]["a|x"] == "TAR-0001"
    assert data["registry"]["b|y"] == "TAR-0002"
    assert data["next_id"] == 3


def test_get_or_assign_survives_corrupted_registry(tmp_path):
    reg = tmp_path / "tar_registry.json"
    reg.write_text("{not valid json")
    out = tid.get_or_assign_targeting_id("a", "x", registry_path=reg)
    # Corrupted file → reset to a fresh registry, allocate TAR-0001
    assert out == "TAR-0001"


def test_get_or_assign_creates_parent_dir(tmp_path):
    """Registry path with a missing parent dir gets created."""
    reg = tmp_path / "nested" / "deep" / "tar_registry.json"
    out = tid.get_or_assign_targeting_id("a", "x", registry_path=reg)
    assert out == "TAR-0001"
    assert reg.exists()


def test_get_or_assign_strips_whitespace_in_keys(tmp_path):
    reg = tmp_path / "tar_registry.json"
    a1 = tid.get_or_assign_targeting_id("skills__python",   "latin_american", registry_path=reg)
    a2 = tid.get_or_assign_targeting_id("  skills__python ", " latin_american ", registry_path=reg)
    assert a1 == a2, "leading/trailing whitespace must not produce a new ID"


# ── Edge cases ──────────────────────────────────────────────────────────


def test_get_or_assign_rejects_empty_cohort_signature(tmp_path):
    reg = tmp_path / "tar_registry.json"
    with pytest.raises(ValueError):
        tid.get_or_assign_targeting_id("", "latin_american", registry_path=reg)
    with pytest.raises(ValueError):
        tid.get_or_assign_targeting_id("   ", "latin_american", registry_path=reg)


def test_get_or_assign_rejects_empty_geo_cluster(tmp_path):
    reg = tmp_path / "tar_registry.json"
    with pytest.raises(ValueError):
        tid.get_or_assign_targeting_id("skills__python", "", registry_path=reg)
    with pytest.raises(ValueError):
        tid.get_or_assign_targeting_id("skills__python", None, registry_path=reg)


def test_lookup_targeting_id_does_not_mutate(tmp_path):
    reg = tmp_path / "tar_registry.json"
    assert tid.lookup_targeting_id("a", "x", registry_path=reg) is None
    # Registry should still be empty (or not created) — lookup is read-only
    if reg.exists():
        data = json.loads(reg.read_text())
        assert data["registry"] == {}
        assert data["next_id"] == 1


def test_lookup_returns_existing_id(tmp_path):
    reg = tmp_path / "tar_registry.json"
    assigned = tid.get_or_assign_targeting_id("a", "x", registry_path=reg)
    found = tid.lookup_targeting_id("a", "x", registry_path=reg)
    assert found == assigned == "TAR-0001"


# ── Concurrency ─────────────────────────────────────────────────────────


def test_concurrent_allocation_no_duplicate_ids(tmp_path):
    """20 threads each allocating 5 unique tuples — must yield 100 distinct TARs."""
    reg = tmp_path / "tar_registry.json"
    results: list[str] = []
    lock = threading.Lock()

    def worker(thread_idx: int) -> None:
        for cohort_idx in range(5):
            sig = f"sig_{thread_idx}_{cohort_idx}"
            tar = tid.get_or_assign_targeting_id(sig, "geo", registry_path=reg)
            with lock:
                results.append(tar)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(results) == 100
    assert len(set(results)) == 100, "TAR IDs must be unique under concurrent allocation"
    # Final next_id must be 101 (100 allocations + 1 = next would-be ID)
    data = json.loads(reg.read_text())
    assert data["next_id"] == 101


def test_concurrent_same_tuple_returns_same_id(tmp_path):
    """20 threads all asking for the SAME tuple — must all get the same TAR-0001."""
    reg = tmp_path / "tar_registry.json"
    results: list[str] = []
    lock = threading.Lock()

    def worker() -> None:
        tar = tid.get_or_assign_targeting_id("same_sig", "same_geo", registry_path=reg)
        with lock:
            results.append(tar)

    threads = [threading.Thread(target=worker) for _ in range(20)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert len(results) == 20
    assert set(results) == {"TAR-0001"}, "Same tuple must always map to the same ID"
    data = json.loads(reg.read_text())
    assert data["next_id"] == 2

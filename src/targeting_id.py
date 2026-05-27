"""Stable per-cohort targeting IDs for campaign naming.

Each unique (cohort_signature × geo_cluster) tuple gets a stable TAR-XXXX
identifier the first time the pipeline sees it. The mapping persists to
`data/targeting_id_registry.json` so re-runs across days/branches/machines
get the same ID for the same targeting tuple — important for downstream
attribution (the operator can grep a TAR ID across multiple ramps to find
every campaign that used that exact targeting).

ID format: `TAR-####` where `####` is a 4-character Base36 string (0-9, A-Z),
zero-padded for natural sort order:
    TAR-0001, TAR-0002, ..., TAR-0009, TAR-000A, ..., TAR-000Z,
    TAR-0010, ..., TAR-ZZZZ
    → 1,679,615-tuple namespace (36^4)

If we ever exhaust the 4-char space, the format auto-expands to 5 chars
(`TAR-10000`) — the ` | ` delimiter in the campaign name is fixed-width
agnostic, so parsers don't care.

Concurrency: file-level lock via `fcntl.flock` so parallel pipeline workers
(`RAMP_CONCURRENCY > 1` or thread-pooled cohort processing) don't allocate
the same counter twice. POSIX-only; fine since the pipeline runs on Linux
(GitHub Actions) and macOS (local dev).
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import threading
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_DEFAULT_REGISTRY_PATH = Path("data/targeting_id_registry.json")
_KEY_SEP = "|"   # separates cohort_signature from geo_cluster in the registry key
_TAR_PREFIX = "TAR-"
_BASE36_ALPHABET = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ"
_DEFAULT_WIDTH = 4

# In-process thread lock — fcntl handles cross-process locking but is a no-op
# for threads inside the same process. Combine both so multiple workers AND
# multiple processes are safe.
_thread_lock = threading.Lock()


# ── Public API ──────────────────────────────────────────────────────────


def get_or_assign_targeting_id(
    cohort_signature: str,
    geo_cluster: str,
    *,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
) -> str:
    """Return the TAR-#### ID for this (cohort × geo) pair, allocating one if new.

    Idempotent: calling twice with the same arguments returns the same ID.
    Thread-safe and process-safe via fcntl flock on the registry file.

    Args:
        cohort_signature: e.g., "skills__python" or
            "fields_of_study__electronics_engineering + skills__python"
        geo_cluster: e.g., "latin_american" or "south_asian"
        registry_path: where the persistent mapping lives (override for tests)

    Returns:
        A TAR-#### string (e.g., "TAR-0001").

    Raises:
        ValueError: if cohort_signature or geo_cluster is empty.
    """
    if not cohort_signature or not str(cohort_signature).strip():
        raise ValueError("cohort_signature must be a non-empty string")
    if not geo_cluster or not str(geo_cluster).strip():
        raise ValueError("geo_cluster must be a non-empty string")

    key = _make_key(cohort_signature, geo_cluster)
    path = Path(registry_path)

    with _thread_lock, _file_lock(path) as data:
        if key in data["registry"]:
            return data["registry"][key]
        next_id = int(data.get("next_id", 1))
        tar = _format_targeting_id(next_id)
        data["registry"][key] = tar
        data["next_id"] = next_id + 1
        _save(path, data)
        log.info("targeting_id: assigned %s to %r", tar, key)
        return tar


def lookup_targeting_id(
    cohort_signature: str,
    geo_cluster: str,
    *,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
) -> Optional[str]:
    """Read-only lookup — returns None if no ID assigned yet, never mutates."""
    if not cohort_signature or not geo_cluster:
        return None
    data = _load(Path(registry_path))
    return data["registry"].get(_make_key(cohort_signature, geo_cluster))


# ── Internal: encoding ──────────────────────────────────────────────────


def _format_targeting_id(n: int, *, min_width: int = _DEFAULT_WIDTH) -> str:
    """Convert positive integer `n` to `TAR-####` with Base36 zero-padding.

    Widths beyond `min_width` are allowed — when the 4-char space is full
    (n=1679616), output naturally becomes 5 chars wide.
    """
    if n < 1:
        raise ValueError(f"targeting_id ordinals must be >= 1 (got {n})")
    encoded = _to_base36(n).rjust(min_width, "0")
    return f"{_TAR_PREFIX}{encoded}"


def _to_base36(n: int) -> str:
    """Encode a positive int as an uppercase Base36 string (no leading 0s)."""
    if n == 0:
        return "0"
    digits: list[str] = []
    while n > 0:
        n, rem = divmod(n, 36)
        digits.append(_BASE36_ALPHABET[rem])
    return "".join(reversed(digits))


def _make_key(cohort_signature: str, geo_cluster: str) -> str:
    return f"{cohort_signature.strip()}{_KEY_SEP}{geo_cluster.strip()}"


# ── Internal: registry I/O + locking ────────────────────────────────────


@contextlib.contextmanager
def _file_lock(path: Path):
    """Hold an exclusive flock on `path` while yielding its parsed contents.

    On exit (success or exception) the lock is released. Caller is
    responsible for writing the mutated data back via `_save(path, data)`
    BEFORE the context exits — we don't auto-write because the caller
    may decide nothing changed.

    Behaves correctly when the file doesn't exist yet — we create the
    parent dir + an empty registry, then lock against the freshly-made file.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        # Seed an empty registry — fine to do unlocked because if two
        # workers race here, both will write the same empty dict and one
        # gets overwritten under the lock on the next iteration anyway.
        path.write_text(json.dumps({"next_id": 1, "registry": {}}, indent=2))
    fh = open(path, "r+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        try:
            data = json.loads(fh.read() or "{}")
        except json.JSONDecodeError:
            log.warning("targeting_id: registry at %s was corrupted; resetting", path)
            data = {"next_id": 1, "registry": {}}
        # Defensive defaults for partially-written files.
        data.setdefault("next_id", 1)
        data.setdefault("registry", {})
        yield data
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


def _load(path: Path) -> dict:
    if not path.exists():
        return {"next_id": 1, "registry": {}}
    try:
        return json.loads(path.read_text() or "{}")
    except json.JSONDecodeError:
        return {"next_id": 1, "registry": {}}


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

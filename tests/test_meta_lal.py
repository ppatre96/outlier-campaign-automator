"""Unit tests for src.meta_lal — LAL audience creation, caching, registry I/O."""
from __future__ import annotations

import json
import os
import sys
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src import meta_lal


def _client(create_returns: str | Exception = "lal_audience_123") -> MagicMock:
    """Mock MetaClient that intercepts the SDK call inside meta_lal."""
    c = MagicMock()
    c._ensure_init = MagicMock()
    c._ad_account_id = "act_179828021490349"
    c.AGENT_NAME_PREFIX = "agent_"
    return c


# Patch the Meta SDK call seam directly so we don't pull in facebook_business.
@pytest.fixture(autouse=True)
def _stub_sdk_create(monkeypatch):
    calls: list = []
    def fake_create(client, seed, country, ratio):
        calls.append((seed, country, ratio))
        # Include the full seed in the id so different seeds → different stub ids
        # (real seeds ending in same chars like '0257' collide if we slice).
        return f"lal_{seed}_{country}_{int(ratio*100)}"
    monkeypatch.setattr(meta_lal, "_create_lookalike_on_meta", fake_create)
    fake_create.calls = calls   # type: ignore[attr-defined]
    return fake_create


# ── get_or_create_lookalike ─────────────────────────────────────────────


def test_first_call_creates_lal(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    result = meta_lal.get_or_create_lookalike(
        "120211889244260257", "US",
        registry_path=reg, meta_client=_client(),
    )
    assert result.newly_created
    assert result.audience_id == "lal_120211889244260257_US_1"
    assert result.country == "US"
    assert result.ratio == 0.01
    assert len(_stub_sdk_create.calls) == 1


def test_second_call_returns_cached(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    a = meta_lal.get_or_create_lookalike("120211889244260257", "US", registry_path=reg, meta_client=_client())
    b = meta_lal.get_or_create_lookalike("120211889244260257", "US", registry_path=reg, meta_client=_client())
    assert a.audience_id == b.audience_id
    assert b.newly_created is False
    # SDK called exactly ONCE despite two top-level calls.
    assert len(_stub_sdk_create.calls) == 1


def test_different_countries_different_lals(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    us = meta_lal.get_or_create_lookalike("120211889244260257", "US", registry_path=reg, meta_client=_client())
    in_ = meta_lal.get_or_create_lookalike("120211889244260257", "IN", registry_path=reg, meta_client=_client())
    assert us.audience_id != in_.audience_id
    assert len(_stub_sdk_create.calls) == 2


def test_different_seeds_different_lals(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    a = meta_lal.get_or_create_lookalike("120211889244260257", "US", registry_path=reg, meta_client=_client())
    b = meta_lal.get_or_create_lookalike("120211112196180257", "US", registry_path=reg, meta_client=_client())
    assert a.audience_id != b.audience_id


def test_country_case_normalized(tmp_path, _stub_sdk_create):
    """ISO-2 country codes uppercased so 'us' and 'US' map to the same LAL."""
    reg = tmp_path / "registry.json"
    a = meta_lal.get_or_create_lookalike("seed1", "us", registry_path=reg, meta_client=_client())
    b = meta_lal.get_or_create_lookalike("seed1", "US", registry_path=reg, meta_client=_client())
    assert a.audience_id == b.audience_id
    assert len(_stub_sdk_create.calls) == 1


def test_rejects_empty_args(tmp_path):
    reg = tmp_path / "registry.json"
    with pytest.raises(ValueError):
        meta_lal.get_or_create_lookalike("", "US", registry_path=reg, meta_client=_client())
    with pytest.raises(ValueError):
        meta_lal.get_or_create_lookalike("seed", "", registry_path=reg, meta_client=_client())


def test_rejects_ratio_out_of_bounds(tmp_path):
    reg = tmp_path / "registry.json"
    with pytest.raises(ValueError):
        meta_lal.get_or_create_lookalike("seed", "US", ratio=0,    registry_path=reg, meta_client=_client())
    with pytest.raises(ValueError):
        meta_lal.get_or_create_lookalike("seed", "US", ratio=1.5,  registry_path=reg, meta_client=_client())
    with pytest.raises(ValueError):
        meta_lal.get_or_create_lookalike("seed", "US", ratio=-0.1, registry_path=reg, meta_client=_client())


def test_registry_persists_across_runs(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    meta_lal.get_or_create_lookalike("seed1", "US", registry_path=reg, meta_client=_client())
    # Simulate "different process" — load registry fresh, expect cached.
    data = json.loads(reg.read_text())
    assert any("seed1" in k for k in data["registry"].keys())


def test_corrupted_registry_resets(tmp_path, _stub_sdk_create):
    reg = tmp_path / "registry.json"
    reg.write_text("{not valid json")
    r = meta_lal.get_or_create_lookalike("seed1", "US", registry_path=reg, meta_client=_client())
    assert r.newly_created


# ── resolve_lal_audiences_for_targeting ─────────────────────────────────


def test_resolve_lal_audiences_returns_cross_product(tmp_path, _stub_sdk_create, monkeypatch):
    import config
    monkeypatch.setattr(config, "META_LAL_SEED_AUDIENCES", ["seedA", "seedB"])
    reg = tmp_path / "registry.json"
    out = meta_lal.resolve_lal_audiences_for_targeting(
        ["US", "IN"],
        registry_path=reg, meta_client=_client(),
    )
    # 2 seeds × 2 countries = 4 audience ID dicts
    assert len(out) == 4
    assert all("id" in entry for entry in out)
    assert len({entry["id"] for entry in out}) == 4   # all unique


def test_resolve_lal_audiences_empty_seeds_returns_empty(tmp_path, monkeypatch):
    import config
    monkeypatch.setattr(config, "META_LAL_SEED_AUDIENCES", [])
    reg = tmp_path / "registry.json"
    out = meta_lal.resolve_lal_audiences_for_targeting(["US", "IN"], registry_path=reg)
    assert out == []


def test_resolve_lal_audiences_skips_meta_failures(tmp_path, monkeypatch):
    """A failing seed×country combo logs + skips; other combos still resolve."""
    import config
    monkeypatch.setattr(config, "META_LAL_SEED_AUDIENCES", ["seedA", "seedB"])

    fail_for = {"seedA"}
    def selective_create(client, seed, country, ratio):
        if seed in fail_for:
            raise RuntimeError("Meta API rejected seedA")
        return f"lal_{seed}_{country}"
    monkeypatch.setattr(meta_lal, "_create_lookalike_on_meta", selective_create)

    reg = tmp_path / "registry.json"
    out = meta_lal.resolve_lal_audiences_for_targeting(
        ["US", "IN"], registry_path=reg, meta_client=_client(),
    )
    # seedA × {US, IN} both fail; seedB × {US, IN} both succeed → 2 results
    assert len(out) == 2
    assert all(entry["id"].startswith("lal_seedB_") for entry in out)


# ── Concurrency ─────────────────────────────────────────────────────────


def test_concurrent_creation_no_duplicate_lals(tmp_path, _stub_sdk_create):
    """10 threads racing to allocate distinct (seed × country) combos must
    produce 10 distinct LAL IDs."""
    reg = tmp_path / "registry.json"
    results: list[str] = []
    lock = threading.Lock()

    def worker(i: int) -> None:
        r = meta_lal.get_or_create_lookalike(f"seed_{i}", "US", registry_path=reg, meta_client=_client())
        with lock:
            results.append(r.audience_id)

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(10)]
    for t in threads: t.start()
    for t in threads: t.join()
    assert len(results) == 10
    assert len(set(results)) == 10

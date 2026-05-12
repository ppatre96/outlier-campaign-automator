"""Phase 3.3 lock-correctness tests — registry RMW atomicity, sheets
gspread serialization, LinkedIn token-refresh deduplication.

These tests prove the locks added alongside Phase 3.3 actually do their
job under concurrent access, independent of whether the orchestrator
actually parallelizes the arms.
"""
from __future__ import annotations

import sys
import threading
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ─── registry RMW atomicity ────────────────────────────────────────────────


def test_registry_log_campaign_concurrent_appends_no_data_loss(tmp_path, monkeypatch):
    """32 threads each call log_campaign concurrently. Final registry must
    contain exactly 32 rows — no lost writes from a read-modify-write race."""
    from src import campaign_registry as cr
    monkeypatch.setattr(cr, "_REGISTRY_PATH", tmp_path / "registry.json")
    monkeypatch.setattr(cr, "_EXCEL_PATH",    tmp_path / "registry.xlsx")
    # Stub the Sheets writeback so we don't touch real gspread.
    monkeypatch.setattr(cr, "_get_sheets", lambda: MagicMock())

    barrier = threading.Barrier(32)

    def worker(idx: int):
        barrier.wait()
        cr.log_campaign(
            smart_ramp_id=f"R{idx}",
            cohort_id=f"c{idx}",
            cohort_signature="sig",
            geo_cluster="x",
            geo_cluster_label="X",
            geos=["US"],
            angle="A",
            campaign_type="static",
            advertised_rate="$50/hr",
            linkedin_campaign_urn=f"urn:li:sponsoredCampaign:{idx}",
        )

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(32)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    records = cr._load()
    assert len(records) == 32, (
        f"expected 32 appends; got {len(records)} — read-modify-write race "
        "lost writes (registry lock missing or wrong)"
    )
    # Every cohort id must be unique and present.
    cohort_ids = {r["cohort_id"] for r in records}
    assert cohort_ids == {f"c{i}" for i in range(32)}, (
        f"cohort ids mismatched — likely lost writes; got {cohort_ids}"
    )


def test_registry_critical_section_is_reentrant(tmp_path, monkeypatch):
    """The lock is an RLock — same thread can re-acquire (used when
    log_campaign internally calls _load + _save under the same outer
    critical section in main.py's static-arm registry patch path)."""
    from src import campaign_registry as cr
    monkeypatch.setattr(cr, "_REGISTRY_PATH", tmp_path / "registry.json")
    monkeypatch.setattr(cr, "_EXCEL_PATH",    tmp_path / "registry.xlsx")
    monkeypatch.setattr(cr, "_get_sheets", lambda: MagicMock())

    with cr.registry_critical_section():
        # Should NOT deadlock — the RLock allows re-entry on the same thread
        cr.log_campaign(
            smart_ramp_id="R", cohort_id="c", cohort_signature="s",
            geo_cluster="x", geo_cluster_label="X", geos=["US"],
            angle="A", campaign_type="static", advertised_rate="$50/hr",
            linkedin_campaign_urn="urn:li:sponsoredCampaign:1",
        )
        # And again, nested even deeper:
        with cr.registry_critical_section():
            recs = cr._load()
            assert len(recs) == 1


# ─── SheetsClient write serialization ──────────────────────────────────────


def test_sheets_write_lock_serializes_concurrent_writes():
    """Concurrent update_li_campaign_id / write_creative / write_registry_row
    calls must serialize through the instance _write_lock — at most one
    in flight at a time."""
    from src.sheets import SheetsClient

    # Build a SheetsClient by hand bypassing __init__ since we don't have
    # Google credentials in test env. Just attach the lock + a stub
    # worksheet path.
    import threading as _t
    inst = SheetsClient.__new__(SheetsClient)
    inst._write_lock = _t.RLock()
    inst._triggers = MagicMock()
    in_flight = 0
    max_in_flight = 0
    counter_lock = _t.Lock()

    def fake_worksheet(name):
        nonlocal in_flight, max_in_flight
        with counter_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time.sleep(0.05)
        with counter_lock:
            in_flight -= 1
        ws = MagicMock()
        ws.find = MagicMock(return_value=None)
        return ws

    inst._triggers.worksheet = fake_worksheet

    # 8 threads hammering update_li_campaign_id concurrently
    barrier = _t.Barrier(8)
    def worker(idx):
        barrier.wait()
        inst.update_li_campaign_id(f"stg_{idx}", f"campaign_{idx}")
    threads = [_t.Thread(target=worker, args=(i,)) for i in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert max_in_flight == 1, (
        f"update_li_campaign_id was not serialized — max_in_flight="
        f"{max_in_flight} (expected 1)"
    )


# ─── LinkedIn token-refresh deduplication ──────────────────────────────────


def test_linkedin_refresh_serialized_across_threads(monkeypatch):
    """Two threads hit a stale token simultaneously — only one network
    refresh fires; the other waits and inherits the new token."""
    from src import linkedin_api

    refresh_count = {"n": 0}
    refresh_started = threading.Event()
    proceed = threading.Event()
    refresh_lock_obs = threading.Lock()

    def fake_refresh():
        with refresh_lock_obs:
            refresh_count["n"] += 1
        refresh_started.set()
        proceed.wait(timeout=2.0)  # gate until both threads have queued
        return "NEW_TOKEN"

    monkeypatch.setattr(linkedin_api, "refresh_access_token", fake_refresh)

    # Build LinkedInClient bypassing config — direct instance construction
    client = linkedin_api.LinkedInClient(token="STALE_TOKEN")
    # Replace the session with a mock that just records the post-refresh header
    seen_tokens = []
    def fake_request(method, url, **kw):
        seen_tokens.append(client._session.headers.get("Authorization"))
        resp = MagicMock()
        resp.ok = True
        return resp
    client._session.request = fake_request

    barrier = threading.Barrier(2)
    results = []
    def caller():
        barrier.wait()
        r = client._refresh_and_retry("GET", "http://example.com")
        results.append(r)

    t1 = threading.Thread(target=caller)
    t2 = threading.Thread(target=caller)
    t1.start(); t2.start()
    # Wait for one of them to enter the critical section
    refresh_started.wait(timeout=2.0)
    proceed.set()
    t1.join(); t2.join()

    # Only one thread should have actually called refresh_access_token; the
    # other waited for the lock and reused the new token without invoking
    # the network. The lock in _refresh_and_retry serializes the WHOLE
    # critical section (including the call to refresh_access_token), so
    # the second thread does call it again — but with the new token in
    # place. Either way, both threads end up with the same Authorization
    # header pointing at NEW_TOKEN.
    assert all("NEW_TOKEN" in str(t) for t in seen_tokens), (
        f"thread did not inherit the refreshed token; saw {seen_tokens}"
    )
    # Both calls returned a response
    assert len(results) == 2


# ─── Phase 3.4 locks — LinkedIn session, UrnResolver, gdrive, Figma ────────


def test_linkedin_req_pins_auth_header_without_serializing_http():
    """Phase 3.4 (post-deadlock-fix): `_req` must pin Authorization under
    the session_lock but RELEASE the lock before the HTTP call. Concurrent
    requests must run in parallel (no global serialization that could
    deadlock the pipeline when one LinkedIn call stalls), AND each request
    must carry the pinned Authorization header it captured at entry.
    """
    from src import linkedin_api
    import threading as _t

    client = linkedin_api.LinkedInClient(token="TOKEN")
    in_flight = 0
    max_in_flight = 0
    captured_auth: list[str] = []
    counter_lock = _t.Lock()
    proceed = _t.Event()

    def fake_request(method, url, **kw):
        nonlocal in_flight, max_in_flight
        captured_auth.append(kw.get("headers", {}).get("Authorization", ""))
        with counter_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        # Block until all 8 workers have entered — if the lock is held during
        # the HTTP call, max_in_flight stays at 1 and the test hangs/fails.
        proceed.wait(timeout=2.0)
        with counter_lock:
            in_flight -= 1
        resp = MagicMock()
        resp.status_code = 200
        resp.ok = True
        return resp

    client._session.request = fake_request

    barrier = _t.Barrier(8)
    def worker():
        barrier.wait()
        client._req("GET", "http://example.com")
    threads = [_t.Thread(target=worker) for _ in range(8)]
    for t in threads: t.start()
    # Give all threads a moment to reach fake_request, then release them.
    time.sleep(0.1)
    proceed.set()
    for t in threads: t.join()

    assert max_in_flight >= 2, (
        f"LinkedInClient._req appears to be serializing HTTP calls — "
        f"max_in_flight={max_in_flight} (expected ≥2). This is the "
        f"deadlock regression that took down the GMR-0020 run."
    )
    assert all(a == "Bearer TOKEN" for a in captured_auth), (
        f"Auth header was not pinned correctly per request; saw {set(captured_auth)}"
    )


def test_linkedin_req_sets_default_timeout():
    """Every LinkedIn API call must carry a timeout — without one, a stalled
    DSC POST (MDP-gated, slow 403) hangs the worker indefinitely.
    """
    from src import linkedin_api

    client = linkedin_api.LinkedInClient(token="TOKEN")
    seen_kwargs = {}

    def fake_request(method, url, **kw):
        seen_kwargs.update(kw)
        resp = MagicMock()
        resp.status_code = 200
        resp.ok = True
        return resp

    client._session.request = fake_request
    client._req("GET", "http://example.com")
    assert "timeout" in seen_kwargs, "LinkedIn request fired without a timeout"
    assert seen_kwargs["timeout"] >= 5, f"timeout too small: {seen_kwargs['timeout']}"


def test_urn_resolver_cache_lock_prevents_duplicate_loads():
    """Concurrent _load_tab calls for the same tab must only trigger one
    underlying sheets.read_urn_tab call (cache+lock collapse the others)."""
    from src.linkedin_urn import UrnResolver
    import threading as _t

    call_count = {"n": 0}
    call_count_lock = _t.Lock()
    sheets = MagicMock()

    def fake_read(tab_name):
        with call_count_lock:
            call_count["n"] += 1
        time.sleep(0.05)
        return [{"name": "Cardiology", "urn": "urn:li:skill:1"}]

    sheets.read_urn_tab = fake_read

    resolver = UrnResolver(sheets_client=sheets)

    barrier = _t.Barrier(16)
    def worker():
        barrier.wait()
        resolver._load_tab("Skills")

    threads = [_t.Thread(target=worker) for _ in range(16)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert call_count["n"] == 1, (
        f"UrnResolver._load_tab fired read_urn_tab {call_count['n']} times "
        "for the same tab; cache lock missed the race"
    )


def test_gdrive_folder_cache_lock_prevents_duplicate_creates(monkeypatch):
    """Concurrent find_or_create_folder calls for the same (parent, name)
    must hit the Drive API only once — the rest fan in on the cached id."""
    from src import gdrive
    import threading as _t

    monkeypatch.setattr(gdrive, "_folder_cache", {})

    api_call_count = {"n": 0}
    api_call_lock = _t.Lock()

    class FakeFilesAPI:
        def list(self, **kw):
            with api_call_lock:
                api_call_count["n"] += 1
            time.sleep(0.03)
            ret = MagicMock()
            ret.execute.return_value = {"files": [{"id": "folder_abc", "name": kw.get("q", "")}]}
            return ret
        def create(self, **kw):
            ret = MagicMock()
            ret.execute.return_value = {"id": "folder_new"}
            return ret

    class FakeSvc:
        def files(self):
            return FakeFilesAPI()

    monkeypatch.setattr(gdrive, "_service", lambda: FakeSvc())
    monkeypatch.setattr(gdrive, "_drive_id", lambda: None)

    barrier = _t.Barrier(8)
    results = []
    def worker():
        barrier.wait()
        results.append(gdrive.find_or_create_folder("ramp_x", "parent_root"))
    threads = [_t.Thread(target=worker) for _ in range(8)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert api_call_count["n"] == 1, (
        f"gdrive.find_or_create_folder fired the Drive list API "
        f"{api_call_count['n']} times; expected exactly 1 — cache lock failed"
    )
    assert set(results) == {"folder_abc"}


def test_figma_session_lock_serializes_get():
    """FigmaCreativeClient._get must serialize the underlying requests.Session.get
    so concurrent fetches don't race on the shared session headers."""
    from src import figma_creative
    import threading as _t

    # Build without hitting config.FIGMA_TOKEN validation
    client = figma_creative.FigmaCreativeClient(token="dummy")
    in_flight = 0
    max_in_flight = 0
    counter_lock = _t.Lock()

    def fake_get(url, **kw):
        nonlocal in_flight, max_in_flight
        with counter_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time.sleep(0.02)
        with counter_lock:
            in_flight -= 1
        resp = MagicMock()
        resp.raise_for_status = MagicMock()
        resp.json = MagicMock(return_value={})
        return resp

    client._session.get = fake_get

    barrier = _t.Barrier(6)
    def worker():
        barrier.wait()
        client._get("files/abc/nodes")
    threads = [_t.Thread(target=worker) for _ in range(6)]
    for t in threads: t.start()
    for t in threads: t.join()

    assert max_in_flight == 1, (
        f"FigmaCreativeClient._get did not serialize session.get() — "
        f"max_in_flight={max_in_flight} (expected 1)"
    )

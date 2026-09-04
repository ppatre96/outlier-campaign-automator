"""URN lookups come from Postgres, with the Google Sheet as fallback.

GMR-0029's LinkedIn launch (2026-09-04) died in `SheetsClient.__init__` on
`open_by_key(URN_SHEET_ID)` with `APIError: [503]: The service is currently
unavailable.` — before a single campaign was created. Two things had to be
true for that: URN reference data lived in a spreadsheet, and the client opened
that spreadsheet eagerly at construction.

These tests pin both fixes: the resolver prefers the Postgres cache, and
constructing a SheetsClient makes no Sheets calls at all.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from src.linkedin_urn import UrnResolver  # noqa: E402

_DB_ROWS = [{"name": "Graphic Design", "urn": "urn:li:skill:db"}]
_SHEET_ROWS = [{"Skills": "Graphic Design", "urn": "urn:li:skill:sheet"}]


def _sheets_stub():
    s = MagicMock()
    s.read_urn_tab.return_value = _SHEET_ROWS
    return s


def test_postgres_is_preferred_over_the_sheet(monkeypatch):
    monkeypatch.setattr("src.urn_store.read_facet", lambda facet: _DB_ROWS)
    sheets = _sheets_stub()
    entries = UrnResolver(sheets)._load_tab("Skills")
    assert entries == [("graphic design", "urn:li:skill:db")]
    sheets.read_urn_tab.assert_not_called()


def test_empty_cache_falls_back_to_the_sheet(monkeypatch):
    """Pre-backfill state: connected to Postgres, nothing seeded yet."""
    monkeypatch.setattr("src.urn_store.read_facet", lambda facet: [])
    sheets = _sheets_stub()
    entries = UrnResolver(sheets)._load_tab("Skills")
    assert entries == [("graphic design", "urn:li:skill:sheet")]
    sheets.read_urn_tab.assert_called_once_with("Skills")


def test_db_unreachable_falls_back_to_the_sheet(monkeypatch):
    def boom(facet):
        raise RuntimeError("DATABASE_URL is not set")

    monkeypatch.setattr("src.urn_store.read_facet", boom)
    sheets = _sheets_stub()
    assert UrnResolver(sheets)._load_tab("Skills") == [("graphic design", "urn:li:skill:sheet")]


def test_both_sources_down_degrades_to_empty(monkeypatch):
    """A URN tab we cannot read must not raise out of _load_tab — the campaign
    falls back to typeahead / looser targeting instead of the run dying."""
    monkeypatch.setattr("src.urn_store.read_facet",
                        lambda facet: (_ for _ in ()).throw(RuntimeError("db down")))
    sheets = MagicMock()
    sheets.read_urn_tab.side_effect = RuntimeError("APIError: [503]")
    assert UrnResolver(sheets)._load_tab("Skills") == []


def test_constructing_sheets_client_opens_no_spreadsheets(monkeypatch):
    """The 503 that killed GMR-0029 happened at construction, before any work."""
    import src.sheets as sheets_mod

    monkeypatch.setattr(sheets_mod, "_credentials_available", lambda: True)

    gc = MagicMock()
    gc.open_by_key.side_effect = AssertionError(
        "SheetsClient.__init__ must not open spreadsheets"
    )
    fake_gspread = MagicMock()
    fake_gspread.authorize.return_value = gc
    monkeypatch.setitem(sys.modules, "gspread", fake_gspread)
    monkeypatch.setattr(
        "google.oauth2.service_account.Credentials.from_service_account_file",
        lambda *a, **k: MagicMock(),
    )

    client = sheets_mod.SheetsClient()
    gc.open_by_key.assert_not_called()

    # ...and it still opens on first real use.
    gc.open_by_key.side_effect = None
    gc.open_by_key.return_value = "opened"
    assert client._urn_sheet == "opened"
    assert client._urn_sheet == "opened"          # cached, not reopened
    assert gc.open_by_key.call_count == 1

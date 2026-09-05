"""LinkedIn URN cache in Postgres (replaces the URN mapping Google Sheet).

The URN mapping — facet value ("Graphic Design") → LinkedIn URN
("urn:li:skill:1234") — lived in a Google Sheet read through gspread. Two
problems, both hit in production:

  1. `SheetsClient.__init__` opened the sheet eagerly, so a Sheets blip took
     down a whole launch before it touched LinkedIn. On 2026-09-04 GMR-0029's
     LinkedIn run died on `open_by_key(URN_SHEET_ID)` with
     `APIError: [503]: The service is currently unavailable.` — zero campaigns,
     lock released, never retried.
  2. It is read-mostly reference data behind an API with no transactions, no
     uniqueness guarantee and a shared-mutable-document concurrency story that
     needed an RLock in the client.

This module keeps the same `{name, urn}` row shape the resolver already
consumes, so `UrnResolver` is unchanged apart from where it reads from. The
sheet stays as a fallback until the backfill has run everywhere
(`scripts/backfill_urn_cache.py`).

Connection plumbing is reused from `ui_decisions` — same database, same
DATABASE_URL, same psycopg-optional degradation.
"""
from __future__ import annotations

import logging
from typing import Iterable

from src.ui_decisions import UIDecisionsUnavailable, _connect

log = logging.getLogger(__name__)

_SCHEMA_READY = False

_SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS linkedin_urn_cache (
    facet       TEXT        NOT NULL,
    name        TEXT        NOT NULL,
    name_lower  TEXT        NOT NULL,
    urn         TEXT        NOT NULL,
    source      TEXT        NOT NULL DEFAULT 'sheet',
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (facet, name_lower)
)
"""


def _ensure_schema(cur) -> None:
    """Create the table once per process. Idempotent (mirrors ui_decisions)."""
    global _SCHEMA_READY
    if _SCHEMA_READY:
        return
    cur.execute(_SCHEMA_SQL)
    _SCHEMA_READY = True


def read_facet(facet: str) -> list[dict]:
    """Return `[{"name": ..., "urn": ...}, ...]` for one facet tab.

    Raises UIDecisionsUnavailable when Postgres isn't reachable so the caller
    can fall back to the sheet. An empty list means "connected, nothing cached"
    — a real answer, not an error.
    """
    with _connect() as conn, conn.cursor() as cur:
        _ensure_schema(cur)
        cur.execute(
            "SELECT name, urn FROM linkedin_urn_cache WHERE facet = %s ORDER BY name",
            (facet,),
        )
        rows = [{"name": r[0], "urn": r[1]} for r in cur.fetchall()]
        conn.commit()
    return rows


def upsert_facet(facet: str, rows: Iterable[dict], *, source: str = "sheet") -> int:
    """Write `{name, urn}` pairs for one facet. Returns the number written.

    Last write wins per (facet, lowercased name) — the sheet had no uniqueness
    guarantee, so duplicates there collapse here rather than being resolved by
    whichever row gspread happened to return first.
    """
    payload = []
    for r in rows or []:
        name = str((r or {}).get("name") or "").strip()
        urn = str((r or {}).get("urn") or "").strip()
        if not name or not urn:
            continue
        payload.append((facet, name, name.lower(), urn, source))
    if not payload:
        return 0
    with _connect() as conn, conn.cursor() as cur:
        _ensure_schema(cur)
        cur.executemany(
            """
            INSERT INTO linkedin_urn_cache (facet, name, name_lower, urn, source, updated_at)
            VALUES (%s, %s, %s, %s, %s, NOW())
            ON CONFLICT (facet, name_lower)
            DO UPDATE SET name = EXCLUDED.name,
                          urn = EXCLUDED.urn,
                          source = EXCLUDED.source,
                          updated_at = NOW()
            """,
            payload,
        )
        conn.commit()
    return len(payload)


def remember(facet: str, name: str, urn: str) -> None:
    """Persist a single live-typeahead resolution so the next run is a cache hit.

    Best-effort: a URN we just resolved from LinkedIn is worth keeping, but
    failing to store it must never break the campaign that resolved it.
    """
    try:
        upsert_facet(facet, [{"name": name, "urn": urn}], source="typeahead")
    except UIDecisionsUnavailable as exc:
        log.debug("urn_store.remember skipped (%s/%s): %s", facet, name, exc)
    except Exception as exc:                                   # pragma: no cover
        log.debug("urn_store.remember failed (%s/%s): %s", facet, name, exc)


def counts() -> dict[str, int]:
    """`{facet: n}` — used by the backfill script to report what landed."""
    with _connect() as conn, conn.cursor() as cur:
        _ensure_schema(cur)
        cur.execute("SELECT facet, COUNT(*) FROM linkedin_urn_cache GROUP BY facet ORDER BY facet")
        out = {r[0]: int(r[1]) for r in cur.fetchall()}
        conn.commit()
    return out

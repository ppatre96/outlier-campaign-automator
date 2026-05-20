"""scripts/sync_registry_header.py
================================

One-shot: force a header re-sync on the Campaign Registry tab so newly-added
COLUMNS entries (most recently `audience_size`) become visible to consumers
that read by header name (the console's lib/sheets.ts parser).

The header sync runs implicitly on every `write_registry_row()` call —
`_get_or_create_registry_tab()` diffs the live row 1 against
`[c.replace("_", " ").title() for c in COLUMNS]` and rewrites if drift is
detected. This script just instantiates SheetsClient + touches the registry
tab to trigger that same code path WITHOUT writing a registry row.

Idempotent. Re-running after the header already matches is a no-op.

Run:
    DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib \\
    doppler run --project outlier-campaign-agent --config dev -- \\
    venv/bin/python scripts/sync_registry_header.py
"""
from __future__ import annotations

import logging
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from dotenv import load_dotenv  # noqa: E402

load_dotenv(ROOT / ".env")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger("sync_registry_header")


def main() -> int:
    from src.sheets import SheetsClient  # noqa: E402

    sc = SheetsClient()
    log.info("Connecting to Campaign Registry tab — will write header if drift detected …")
    ws = sc._get_or_create_registry_tab()
    log.info("Header sync complete. Sheet: %r tab: %r", ws.spreadsheet.title, ws.title)
    row1 = ws.row_values(1)
    log.info("Header row (%d cols): %s", len(row1), row1)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

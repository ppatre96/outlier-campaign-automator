"""
Google Sheets client — reads Triggers 2 / Config tabs and writes results back.

In the Smart Ramp pipeline, Sheets is OPTIONAL:
  - Config values (LinkedIn token, etc.) all fall back to env vars.
  - Write-back calls (cohort IDs, campaign IDs, creative URNs) are nice-to-have
    audit trail — the Slack notification + processed_ramps.json cover monitoring.

If credentials.json is absent (e.g. GitHub Actions without GOOGLE_CREDENTIALS_JSON
secret), SheetsClient() returns a NullSheetsClient stub that reads from env vars
and silently no-ops all writes. No credentials needed for the Smart Ramp path.
"""
import logging
import os
import random
from datetime import datetime
from pathlib import Path
from typing import Any

import config

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    # drive.file lets the service account upload creative PNGs that we then embed
    # via =IMAGE(...) in the Campaign Registry tab. Scope is limited to files this
    # app creates/opens (not the full drive).
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Pixel size for the Creative cell in the Campaign Registry tab.
# Row height & cell width chosen so the 1200×1200 PNG is legible without
# bloating the sheet. IMAGE() mode 4 takes pixel width × height.
_REGISTRY_IMAGE_PX = 200
_REGISTRY_IMAGE_ROW_HEIGHT = 210
_REGISTRY_IMAGE_COL_WIDTH = 220


class NullSheetsClient:
    """No-op stub used when Google credentials are unavailable.
    Reads config from environment variables; silently ignores all writes.
    """

    def read_config(self) -> dict[str, str]:
        return {
            "LINKEDIN_TOKEN":       os.getenv("LINKEDIN_ACCESS_TOKEN", ""),
            "CLAUDE_API_KEY":       os.getenv("ANTHROPIC_API_KEY", ""),
            "MIDJOURNEY_API_TOKEN": os.getenv("MIDJOURNEY_API_TOKEN", ""),
            "INMAIL_SENDER_URN":    os.getenv("LINKEDIN_INMAIL_SENDER_URN", ""),
            "SCREENING_CONFIG_NAME": "",
        }

    def read_pending_rows(self) -> list[dict]:
        return []

    def read_li_retry_rows(self) -> list[dict]:
        return []

    def write_cohorts(self, *a, **kw) -> None:
        log.debug("NullSheetsClient.write_cohorts — no-op (credentials unavailable)")

    def update_li_campaign_id(self, *a, **kw) -> None:
        log.debug("NullSheetsClient.update_li_campaign_id — no-op")

    def write_creative(self, *a, **kw) -> None:
        log.debug("NullSheetsClient.write_creative — no-op")

    def write_registry_row(self, *a, **kw) -> None:
        log.debug("NullSheetsClient.write_registry_row — no-op")

    def get_urn_sheet(self) -> None:
        return None

    def get_text_layer_map(self, *a, **kw) -> dict:
        return {}


def _credentials_available() -> bool:
    """True when a service-account credentials file exists and is readable."""
    cred_path = Path(config.GOOGLE_CREDENTIALS)
    return cred_path.exists() and cred_path.stat().st_size > 10

# Column indices (0-based) for "Triggers 2"
COL = {
    "date": 0,              # A
    "unique_id": 1,         # B — unique identifier for image naming
    "flow_id": 2,           # C
    "tg_status": 3,         # D
    "master_campaign": 4,   # E
    "location": 5,          # F
    "figma_file": 6,        # G
    "figma_node": 7,        # H
    "stg_id": 8,            # I
    "stg_name": 9,          # J
    "targeting_facet": 10,  # K
    "targeting_criteria": 11,  # L
    "li_status": 12,        # M — LI Campaign Creation Status
    "li_campaign_id": 13,   # N — LI Campaign ID (numeric)
    "error_detail": 14,     # O — Error Detail
    "ad_type": 15,          # P — "INMAIL" or blank (defaults to image ad)
}


def _col_letter(idx: int) -> str:
    """Convert 0-based column index to A1 letter."""
    result = ""
    n = idx + 1
    while n:
        n, rem = divmod(n - 1, 26)
        result = chr(65 + rem) + result
    return result


class SheetsClient:
    def __new__(cls):
        """Return NullSheetsClient when credentials are unavailable."""
        if not _credentials_available():
            log.info("Google credentials not found — using NullSheetsClient (env-var fallback, writes no-op)")
            return NullSheetsClient()
        return super().__new__(cls)

    def __init__(self):
        if isinstance(self, NullSheetsClient):
            return  # already initialised by __new__
        import gspread
        import threading
        from google.oauth2.service_account import Credentials
        creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=SCOPES)
        self._creds = creds
        self._gc = gspread.authorize(creds)
        self._triggers = self._gc.open_by_key(config.TRIGGERS_SHEET_ID)
        self._urn_sheet = self._gc.open_by_key(config.URN_SHEET_ID)
        self._drive_service = None  # lazy-init in _get_drive()
        # Phase 3.3 — gspread is not thread-safe (sequential http client + no
        # internal locking). With the InMail + Static arms now running
        # concurrently, calls to update_li_campaign_id / write_creative /
        # write_registry_row can race against each other on the same Sheet.
        # A coarse instance-level RLock serializes those writes; concurrent
        # READs are still allowed (we don't lock the reader methods).
        self._write_lock = threading.RLock()

    # ── Drive helpers (registry image embedding) ──────────────────────────────

    def _get_drive(self):
        """Lazy-init Google Drive API client (used for creative image uploads)."""
        if self._drive_service is None:
            from googleapiclient.discovery import build
            self._drive_service = build("drive", "v3", credentials=self._creds, cache_discovery=False)
        return self._drive_service

    def _upload_creative_to_drive(self, local_path: str) -> str | None:
        """Upload a PNG to Drive (anyone-with-link viewable) and return the file ID.
        Returns None on any failure — registry write must continue without the image.
        """
        path = Path(local_path)
        if not path.exists():
            log.warning("Registry image upload skipped — file missing: %s", local_path)
            return None
        try:
            from googleapiclient.http import MediaFileUpload
            drive = self._get_drive()
            metadata = {"name": path.name, "mimeType": "image/png"}
            media = MediaFileUpload(str(path), mimetype="image/png", resumable=False)
            created = drive.files().create(
                body=metadata, media_body=media, fields="id"
            ).execute()
            file_id = created["id"]
            # Make readable by anyone with the link so =IMAGE() can fetch it
            drive.permissions().create(
                fileId=file_id,
                body={"role": "reader", "type": "anyone"},
                fields="id",
            ).execute()
            log.info("Registry: uploaded creative to Drive id=%s (%s)", file_id, path.name)
            return file_id
        except Exception as exc:
            log.warning("Registry image upload failed (non-fatal): %s", exc)
            return None

    # ── Config tab ────────────────────────────────────────────────────────────

    def read_config(self) -> dict[str, str]:
        """Read the Config tab: returns {key: value} mapping."""
        # Phase 3.4 — gspread is not thread-safe; serialize reads against writes
        # under the same lock to prevent interleaved HTTP responses.
        with self._write_lock:
            ws = self._triggers.worksheet(config.CONFIG_TAB)
            rows = ws.get_all_values()
        result = {}
        for row in rows[1:]:  # skip header
            if len(row) >= 2 and row[0]:
                result[row[0].strip()] = row[1].strip()
        return result

    # ── Triggers 2 tab ───────────────────────────────────────────────────────

    def read_pending_rows(self) -> list[dict]:
        """Return all rows where column C (TG Creation Status) == 'PENDING'."""
        with self._write_lock:
            ws = self._triggers.worksheet(config.TRIGGERS_TAB)
            all_rows = ws.get_all_values()
        if not all_rows:
            return []

        header = all_rows[0]
        pending = []
        for idx, row in enumerate(all_rows[1:], start=2):  # row 2 is first data row
            # Pad row to expected width (max col index + 1)
            while len(row) <= max(COL.values()):
                row.append("")
            status = row[COL["tg_status"]].strip().upper()
            if status == "PENDING":
                unique_id = row[COL["unique_id"]].strip() or f"ROW_{idx}"
                pending.append({
                    "sheet_row": idx,
                    "unique_id":       unique_id,
                    "date":            row[COL["date"]],
                    "flow_id":         row[COL["flow_id"]].strip(),
                    "tg_status":       row[COL["tg_status"]].strip(),
                    "master_campaign": row[COL["master_campaign"]].strip(),
                    "location":        row[COL["location"]].strip(),
                    "figma_file":      row[COL["figma_file"]].strip(),
                    "figma_node":      row[COL["figma_node"]].strip(),
                    "ad_type":         row[COL["ad_type"]].strip().upper() if len(row) > COL["ad_type"] else "",
                })
        return pending

    def read_li_retry_rows(self) -> list[dict]:
        """
        Return rows where cohorts already exist (tg_status=Completed) but
        LinkedIn campaign creation failed or is still pending (li_status in Failed/Pending).
        These rows already have stg_id, stg_name, targeting_criteria filled in.
        """
        with self._write_lock:
            ws = self._triggers.worksheet(config.TRIGGERS_TAB)
            all_rows = ws.get_all_values()
        if not all_rows:
            return []

        retry = []
        for idx, row in enumerate(all_rows[1:], start=2):
            while len(row) <= max(COL.values()):
                row.append("")
            tg_status = row[COL["tg_status"]].strip().upper()
            li_status = row[COL["li_status"]].strip().upper()
            stg_id    = row[COL["stg_id"]].strip()
            criteria  = row[COL["targeting_criteria"]].strip()
            if tg_status == "COMPLETED" and li_status in ("FAILED", "PENDING") and stg_id and criteria:
                retry.append({
                    "sheet_row":          idx,
                    "date":               row[COL["date"]],
                    "flow_id":            row[COL["flow_id"]].strip(),
                    "tg_status":          row[COL["tg_status"]].strip(),
                    "master_campaign":    row[COL["master_campaign"]].strip(),
                    "location":           row[COL["location"]].strip(),
                    "figma_file":         row[COL["figma_file"]].strip(),
                    "figma_node":         row[COL["figma_node"]].strip(),
                    "stg_id":             stg_id,
                    "stg_name":           row[COL["stg_name"]].strip(),
                    "targeting_facet":    row[COL["targeting_facet"]].strip(),
                    "targeting_criteria": criteria,
                    "ad_type":            row[COL["ad_type"]].strip().upper(),
                })
        return retry

    def write_cohorts(self, input_row: dict, cohorts: list[dict]) -> None:
        """
        Write 1–5 cohort results for a given input row.
        cohort dict keys: stg_id, stg_name, targeting_facet, targeting_criteria_json
        """
        ws = self._triggers.worksheet(config.TRIGGERS_TAB)
        sheet_row = input_row["sheet_row"]

        for i, cohort in enumerate(cohorts):
            values_hk = [
                cohort["stg_id"],
                cohort["stg_name"],
                cohort["targeting_facet"],
                cohort["targeting_criteria_json"],
                "Pending",  # L: LI Campaign Creation Status
            ]
            if i == 0:
                # Update existing row H-L + set C = Completed
                start_col = _col_letter(COL["stg_id"])
                end_col   = _col_letter(COL["li_status"])
                ws.update(f"{start_col}{sheet_row}:{end_col}{sheet_row}", [values_hk])
                ws.update_cell(sheet_row, COL["tg_status"] + 1, "Completed")
                log.info("Updated row %d with cohort %s", sheet_row, cohort["stg_id"])
            else:
                # Insert new row: copy A-G from input row, write H-L
                all_rows = ws.get_all_values()
                base_row = all_rows[sheet_row - 1]  # 0-indexed
                while len(base_row) < 12:
                    base_row.append("")
                new_row = list(base_row[:7]) + values_hk
                new_row[COL["tg_status"]] = "Completed"
                ws.append_row(new_row, value_input_option="RAW")
                log.info("Appended new row for cohort %s", cohort["stg_id"])

    def update_li_campaign_id(self, stg_id: str, campaign_id: str) -> None:
        """Write the LinkedIn campaign status + ID back to the row matching stg_id."""
        with self._write_lock:
            ws = self._triggers.worksheet(config.TRIGGERS_TAB)
            cell = ws.find(stg_id)
            if cell:
                ws.update_cell(cell.row, COL["li_status"] + 1, "Created")
                ws.update_cell(cell.row, COL["li_campaign_id"] + 1, campaign_id)

    # ── Creatives tab ─────────────────────────────────────────────────────────

    def write_creative(self, stg_id: str, creative_name: str, li_creative_id: str) -> None:
        with self._write_lock:
            ws = self._triggers.worksheet(config.CREATIVES_TAB)
            ws.append_row([
                stg_id,
                creative_name,
                li_creative_id,
                datetime.utcnow().isoformat(timespec="seconds") + "Z",
            ], value_input_option="RAW")
            log.info("Wrote creative %s → %s", stg_id, creative_name)

    # ── Campaign Registry tab ─────────────────────────────────────────────────

    def _get_or_create_registry_tab(self):
        """Return the Campaign Registry worksheet (cached), creating + header-syncing as needed."""
        if getattr(self, "_registry_ws_cache", None) is not None:
            return self._registry_ws_cache

        from src.campaign_registry import COLUMNS
        expected = [c.replace("_", " ").title() for c in COLUMNS]
        try:
            ws = self._triggers.worksheet(config.REGISTRY_TAB)
            # Header sync: if columns were added in code but not in the sheet,
            # write the canonical header row. Existing data rows are untouched.
            existing = ws.row_values(1) if ws.row_count else []
            if existing != expected:
                if ws.col_count < len(expected):
                    ws.add_cols(len(expected) - ws.col_count)
                ws.update("A1", [expected])
                log.info(
                    "Registry tab headers synced: %d -> %d cols",
                    len(existing), len(expected),
                )
        except Exception:
            ws = self._triggers.add_worksheet(
                title=config.REGISTRY_TAB, rows=1000, cols=len(COLUMNS)
            )
            ws.update("A1", [expected])
            ws.freeze(rows=1)
            log.info("Created '%s' tab with %d columns", config.REGISTRY_TAB, len(COLUMNS))

        self._registry_ws_cache = ws
        return ws

    def write_registry_row(self, record: dict) -> None:
        """Append one campaign registry row to the Campaign Registry tab.

        If `creative_image_path` is set and the local PNG exists, it is uploaded to
        Drive (anyone-with-link viewable) and the corresponding cell receives an
        `=IMAGE(...)` formula so the creative renders inline in the row.

        Locked under self._write_lock so concurrent calls from the InMail
        and Static arms (Phase 3.3) can't race against gspread's internal
        state.
        """
        from src.campaign_registry import COLUMNS
        with self._write_lock:
            self._write_registry_row_locked(record, COLUMNS)

    def _write_registry_row_locked(self, record: dict, COLUMNS: list) -> None:
        ws = self._get_or_create_registry_tab()

        # Resolve image: upload first so we can put the formula straight in the row.
        image_col_idx = COLUMNS.index("creative_image_path") if "creative_image_path" in COLUMNS else -1
        image_formula: str | None = None
        local_path = record.get("creative_image_path") or ""
        if local_path and image_col_idx >= 0:
            file_id = self._upload_creative_to_drive(local_path)
            if file_id:
                image_url = f"https://drive.google.com/uc?export=view&id={file_id}"
                # Mode 4 = explicit pixel dims (width, height)
                image_formula = (
                    f'=IMAGE("{image_url}", 4, {_REGISTRY_IMAGE_PX}, {_REGISTRY_IMAGE_PX})'
                )

        row = [record.get(col, "") or "" for col in COLUMNS]
        if image_formula and image_col_idx >= 0:
            row[image_col_idx] = image_formula

        ws.append_row(row, value_input_option="USER_ENTERED")

        # If we embedded an image, bump that row's height + ensure the column
        # is wide enough so the image isn't clipped.
        if image_formula and image_col_idx >= 0:
            try:
                new_row_idx = len(ws.col_values(1))   # 1-based — last filled row
                self._resize_registry_image_cell(ws, new_row_idx, image_col_idx)
            except Exception as exc:
                log.warning("Registry row/col resize failed (non-fatal): %s", exc)

        log.info(
            "Registry sheet: appended row campaign=%s angle=%s geo=%s image=%s",
            record.get("linkedin_campaign_urn", ""),
            record.get("angle", ""),
            record.get("geo_cluster_label", ""),
            "yes" if image_formula else "no",
        )

    def _resize_registry_image_cell(self, ws, row_idx_1based: int, col_idx_0based: int) -> None:
        """Bump the row height + creative column width so the embedded image is visible."""
        self._triggers.batch_update({
            "requests": [
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": row_idx_1based - 1,
                            "endIndex": row_idx_1based,
                        },
                        "properties": {"pixelSize": _REGISTRY_IMAGE_ROW_HEIGHT},
                        "fields": "pixelSize",
                    }
                },
                {
                    "updateDimensionProperties": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "COLUMNS",
                            "startIndex": col_idx_0based,
                            "endIndex": col_idx_0based + 1,
                        },
                        "properties": {"pixelSize": _REGISTRY_IMAGE_COL_WIDTH},
                        "fields": "pixelSize",
                    }
                },
            ]
        })

    # ── URN sheets ────────────────────────────────────────────────────────────

    def read_urn_tab(self, tab_name: str) -> list[dict]:
        """Return all rows from a URN mapping tab as list of {name, urn} dicts."""
        # Called lazily from UrnResolver._load_tab; with ramp parallelism this
        # can fire from multiple threads racing on the gspread URN spreadsheet.
        with self._write_lock:
            ws = self._urn_sheet.worksheet(tab_name)
            rows = ws.get_all_records()
        return rows


def make_stg_id() -> str:
    date_str = datetime.utcnow().strftime("%Y%m%d")
    rand     = random.randint(10000, 99999)
    return f"STG-{date_str}-{rand}"

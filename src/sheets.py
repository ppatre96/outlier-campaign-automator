"""
Google Sheets client — reads Triggers 2 / Config tabs and writes results back.
"""
import logging
import random
from datetime import datetime
from typing import Any

import gspread
from google.oauth2.service_account import Credentials

import config

log = logging.getLogger(__name__)

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.readonly",
]

# Column indices (0-based) for "Triggers 2"
COL = {
    "date": 0,              # A
    "flow_id": 1,           # B
    "tg_status": 2,         # C
    "master_campaign": 3,   # D
    "location": 4,          # E
    "figma_file": 5,        # F
    "figma_node": 6,        # G
    "stg_id": 7,            # H
    "stg_name": 8,          # I
    "targeting_facet": 9,   # J
    "targeting_criteria": 10,  # K
    "li_status": 11,        # L — LI Campaign Creation Status
    "li_campaign_id": 12,   # M — LI Campaign ID (numeric)
    "error_detail": 13,     # N — Error Detail
    "ad_type": 14,          # O — "INMAIL" or blank (defaults to image ad)
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
    def __init__(self):
        creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=SCOPES)
        self._gc = gspread.authorize(creds)
        self._triggers = self._gc.open_by_key(config.TRIGGERS_SHEET_ID)
        self._urn_sheet = self._gc.open_by_key(config.URN_SHEET_ID)

    # ── Config tab ────────────────────────────────────────────────────────────

    def read_config(self) -> dict[str, str]:
        """Read the Config tab: returns {key: value} mapping."""
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
                pending.append({
                    "sheet_row": idx,
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
        ws = self._triggers.worksheet(config.TRIGGERS_TAB)
        cell = ws.find(stg_id)
        if cell:
            ws.update_cell(cell.row, COL["li_status"] + 1, "Created")
            ws.update_cell(cell.row, COL["li_campaign_id"] + 1, campaign_id)

    # ── Creatives tab ─────────────────────────────────────────────────────────

    def write_creative(self, stg_id: str, creative_name: str, li_creative_id: str) -> None:
        ws = self._triggers.worksheet(config.CREATIVES_TAB)
        ws.append_row([
            stg_id,
            creative_name,
            li_creative_id,
            datetime.utcnow().isoformat(timespec="seconds") + "Z",
        ], value_input_option="RAW")
        log.info("Wrote creative %s → %s", stg_id, creative_name)

    # ── URN sheets ────────────────────────────────────────────────────────────

    def read_urn_tab(self, tab_name: str) -> list[dict]:
        """Return all rows from a URN mapping tab as list of {name, urn} dicts."""
        ws = self._urn_sheet.worksheet(tab_name)
        rows = ws.get_all_records()
        return rows


def make_stg_id() -> str:
    date_str = datetime.utcnow().strftime("%Y%m%d")
    rand     = random.randint(10000, 99999)
    return f"STG-{date_str}-{rand}"

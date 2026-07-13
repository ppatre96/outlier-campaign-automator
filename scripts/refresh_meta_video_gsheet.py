"""Meta VIDEO-ad metrics → Google Sheet (agency-facing, daily refresh).

Reads the `meta_creative_format_daily` Postgres table (creative_format='video')
for a fixed set of agency-tracked locales and writes a single tidy tab that the
media agency can view. One row per (locale × ramp) with lifetime-to-date
delivery, video-engagement, and derived rates — including completion rate and
average watch time.

Data provenance: `meta_creative_format_daily` is delivery + video-engagement
ONLY, sourced straight from the Meta Marketing API (issues #94/#95 — activations
are NOT format-attributable, so no funnel columns here). By default this script
first refreshes that table from the Meta API (build_meta_creative_format_daily)
so it is self-sufficient and does not depend on the daily_feedback job's timing.

Scheduling: .github/workflows/meta_video_gsheet.yml runs this once per day in
CI (cloud) so the sheet stays fresh regardless of any local session. Run
standalone:

    # first time — create the sheet, share it, print the URL + id:
    doppler run -- python3 scripts/refresh_meta_video_gsheet.py --create \
        --share-email pranav.patre@scale.com --link-viewable

    # subsequent refreshes (sheet id from Doppler META_VIDEO_GSHEET_ID):
    doppler run -- python3 scripts/refresh_meta_video_gsheet.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402

log = logging.getLogger("refresh_meta_video_gsheet")

# Agency-tracked locales, in display order. The `language` column in
# meta_creative_format_daily stores the display language (see
# creative_format_metrics._LANG_TOKENS); "Mexican" maps to Spanish (es-MX).
LOCALES = ["German", "French", "Italian", "Spanish", "Korean", "Bengali", "Thai"]
DISPLAY = {"Spanish": "Mexican (es-MX)"}

HEADERS = [
    "Locale", "Ramp", "Launched", "Last day", "Days live",
    "Impressions", "Video plays", "3-sec views", "ThruPlays",
    "Watched 25%", "Watched 50%", "Watched 75%", "Watched 100%",
    "Avg watch time (s)", "Completion % (100%/plays)",
    "Clicks", "Spend (USD)",
    "CTR %", "Hook rate % (3s/plays)", "ThruPlay rate % (thru/plays)",
    "CPM (USD)", "Cost / 3-sec view", "Cost / ThruPlay",
    "Reactions", "Comments", "Shares", "Saves",
]


def _f(x) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


def fetch_rows() -> tuple[list[list], list[str]]:
    """Return (data_rows, missing_locales). data_rows are display-ready lists
    aligned to HEADERS, one per (locale × ramp) that has video delivery."""
    import psycopg

    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL not set — cannot read meta_creative_format_daily")

    with psycopg.connect(url, connect_timeout=15) as conn, conn.cursor() as cur:
        cur.execute(
            """
            SELECT language, ramp_id,
                   MIN(metric_date), MAX(metric_date), COUNT(DISTINCT metric_date),
                   SUM(impressions), SUM(video_plays), SUM(video_3sec), SUM(video_thruplays),
                   SUM(video_p25), SUM(video_p50), SUM(video_p75), SUM(video_p100),
                   SUM(video_watch_seconds), SUM(clicks), SUM(spend_usd),
                   SUM(reactions), SUM(comments), SUM(shares), SUM(saves)
            FROM meta_creative_format_daily
            WHERE creative_format = 'video' AND language = ANY(%s)
            GROUP BY language, ramp_id
            ORDER BY language, ramp_id
            """,
            (LOCALES,),
        )
        raw = cur.fetchall()

    by_lang: dict[str, list] = {}
    rows: list[list] = []
    totals = {k: 0.0 for k in ("imp", "plays", "v3", "thru", "p25", "p50", "p75",
                               "p100", "ws", "clk", "spend", "rx", "cm", "sh", "sv")}
    for r in raw:
        (lang, ramp, launched, last, days, imp, plays, v3, thru,
         p25, p50, p75, p100, ws, clk, spend, rx, cm, sh, sv) = r
        by_lang[lang] = r
        imp, plays, v3, thru = _f(imp), _f(plays), _f(v3), _f(thru)
        p25, p50, p75, p100 = _f(p25), _f(p50), _f(p75), _f(p100)
        ws, clk, spend = _f(ws), _f(clk), _f(spend)
        rx, cm, sh, sv = _f(rx), _f(cm), _f(sh), _f(sv)
        rows.append([
            DISPLAY.get(lang, lang), ramp, str(launched), str(last), int(days),
            int(imp), int(plays), int(v3), int(thru),
            int(p25), int(p50), int(p75), int(p100),
            round(ws / plays, 1) if plays else 0,
            round(p100 / plays * 100, 1) if plays else 0,
            int(clk), round(spend, 2),
            round(clk / imp * 100, 3) if imp else 0,
            round(v3 / plays * 100, 1) if plays else 0,
            round(thru / plays * 100, 1) if plays else 0,
            round(spend / imp * 1000, 2) if imp else 0,
            round(spend / v3, 4) if v3 else 0,
            round(spend / thru, 4) if thru else 0,
            int(rx), int(cm), int(sh), int(sv),
        ])
        for k, val in zip(("imp", "plays", "v3", "thru", "p25", "p50", "p75", "p100",
                           "ws", "clk", "spend", "rx", "cm", "sh", "sv"),
                          (imp, plays, v3, thru, p25, p50, p75, p100,
                           ws, clk, spend, rx, cm, sh, sv)):
            totals[k] += val

    if rows:
        t = totals
        rows.append([
            "TOTAL", "—", "", "", "",
            int(t["imp"]), int(t["plays"]), int(t["v3"]), int(t["thru"]),
            int(t["p25"]), int(t["p50"]), int(t["p75"]), int(t["p100"]),
            round(t["ws"] / t["plays"], 1) if t["plays"] else 0,
            round(t["p100"] / t["plays"] * 100, 1) if t["plays"] else 0,
            int(t["clk"]), round(t["spend"], 2),
            round(t["clk"] / t["imp"] * 100, 3) if t["imp"] else 0,
            round(t["v3"] / t["plays"] * 100, 1) if t["plays"] else 0,
            round(t["thru"] / t["plays"] * 100, 1) if t["plays"] else 0,
            round(t["spend"] / t["imp"] * 1000, 2) if t["imp"] else 0,
            round(t["spend"] / t["v3"], 4) if t["v3"] else 0,
            round(t["spend"] / t["thru"], 4) if t["thru"] else 0,
            int(t["rx"]), int(t["cm"]), int(t["sh"]), int(t["sv"]),
        ])

    missing = [DISPLAY.get(l, l) for l in LOCALES if l not in by_lang]
    return rows, missing


def _open_or_create(gc, args):
    """Return (spreadsheet, worksheet). Creates + shares when --create."""
    if args.create:
        title = args.title or "Meta Video Ad Metrics — Agency View"
        # Service accounts have no personal Drive quota, so the file MUST be
        # created inside the org Shared Drive (folder_id = Shared Drive id).
        folder_id = args.folder_id or getattr(config, "GDRIVE_DRIVE_ID", "") or None
        sh = gc.create(title, folder_id=folder_id) if folder_id else gc.create(title)
        log.info("Created sheet %s (%s)", title, sh.id)
        print(f"SHEET_ID={sh.id}")
        print(f"SHEET_URL=https://docs.google.com/spreadsheets/d/{sh.id}")
        if args.share_email:
            sh.share(args.share_email, perm_type="user", role="writer", notify=False)
            log.info("Shared writer access with %s", args.share_email)
        if args.link_viewable:
            try:
                sh.share(None, perm_type="anyone", role="reader")
                log.info("Enabled anyone-with-link viewer access")
            except Exception as exc:  # noqa: BLE001 — org policy may forbid public links
                log.warning("Could not enable anyone-with-link (org policy?); "
                            "share manually via Drive: %s", exc)
    else:
        sheet_id = args.sheet_id or os.environ.get("META_VIDEO_GSHEET_ID", "").strip()
        if not sheet_id:
            raise RuntimeError(
                "No sheet id: pass --sheet-id, set META_VIDEO_GSHEET_ID, or use --create")
        sh = gc.open_by_key(sheet_id)
    ws = sh.sheet1
    return sh, ws


def _write(sh, ws, rows: list[list], missing: list[str]) -> None:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    subtitle = f"GMR-0023 · Meta video creatives · refreshed {now}"
    if missing:
        subtitle += f"  |  No Meta video for: {', '.join(missing)} (static only)"

    body = [["Meta Video Ad Metrics — Agency View"], [subtitle], [], HEADERS] + rows
    ws.clear()
    ws.update(body, "A1", value_input_option="RAW")

    n_cols = len(HEADERS)
    header_row = 4                       # 1-based row of HEADERS
    last_row = header_row + len(rows)    # inclusive
    total_row = last_row if rows and rows[-1][0] == "TOTAL" else None

    def a1(r1, c1, r2, c2):
        from gspread.utils import rowcol_to_a1
        return f"{rowcol_to_a1(r1, c1)}:{rowcol_to_a1(r2, c2)}"

    reqs = []
    sid = ws.id
    # title / subtitle
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A2", {"textFormat": {"italic": True, "fontSize": 10,
                                    "foregroundColor": {"red": .4, "green": .4, "blue": .4}}})
    # header band
    ws.format(a1(header_row, 1, header_row, n_cols), {
        "backgroundColor": {"red": .122, "green": .306, "blue": .471},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE",
        "horizontalAlignment": "CENTER",
    })
    if total_row:
        ws.format(a1(total_row, 1, total_row, n_cols), {
            "backgroundColor": {"red": .905, "green": .933, "blue": .968},
            "textFormat": {"bold": True},
        })
    # number formats: integers (counts), USD, percents
    data_top, data_bot = header_row + 1, last_row
    int_cols = [6, 7, 8, 9, 10, 11, 12, 13, 16, 24, 25, 26, 27]
    usd2_cols = [17, 21]
    usd4_cols = [22, 23]
    pct_cols = [15, 18, 19, 20]
    for c in int_cols:
        ws.format(a1(data_top, c, data_bot, c), {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
    for c in usd2_cols:
        ws.format(a1(data_top, c, data_bot, c), {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.00"}})
    for c in usd4_cols:
        ws.format(a1(data_top, c, data_bot, c), {"numberFormat": {"type": "CURRENCY", "pattern": "$#,##0.0000"}})
    for c in pct_cols:
        ws.format(a1(data_top, c, data_bot, c), {"numberFormat": {"type": "NUMBER", "pattern": "0.0\"%\""}})
    ws.format(a1(data_top, 14, data_bot, 14), {"numberFormat": {"type": "NUMBER", "pattern": "0.0"}})

    # freeze header + first two columns, size columns
    sh.batch_update({"requests": [
        {"updateSheetProperties": {
            "properties": {"sheetId": sid, "gridProperties": {"frozenRowCount": header_row, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        {"autoResizeDimensions": {"dimensions": {"sheetId": sid, "dimension": "COLUMNS",
                                                 "startIndex": 0, "endIndex": n_cols}}},
    ]})
    log.info("Wrote %d data rows (+header) to sheet %s", len(rows), sh.id)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Refresh the agency-facing Meta video-metrics Google Sheet.")
    ap.add_argument("--create", action="store_true", help="Create a new spreadsheet and print its id/url.")
    ap.add_argument("--sheet-id", default="", help="Target spreadsheet id (else META_VIDEO_GSHEET_ID env).")
    ap.add_argument("--title", default="", help="Title for the new sheet (with --create).")
    ap.add_argument("--share-email", default="", help="Email to grant writer access (with --create).")
    ap.add_argument("--folder-id", default="", help="Shared Drive / folder id to create the sheet in (with --create). Defaults to config.GDRIVE_DRIVE_ID.")
    ap.add_argument("--link-viewable", action="store_true", help="Anyone-with-link can view (with --create).")
    ap.add_argument("--no-refresh-table", action="store_true",
                    help="Skip rebuilding meta_creative_format_daily from the Meta API first.")
    ap.add_argument("--window", type=int, default=30, help="Meta API look-back window when refreshing the table.")
    args = ap.parse_args()

    if not args.no_refresh_table:
        try:
            from src.creative_format_metrics import build_meta_creative_format_daily
            wrote = build_meta_creative_format_daily(window_days=args.window)
            log.info("Refreshed meta_creative_format_daily: %d rows", wrote)
        except Exception as exc:  # noqa: BLE001 — non-fatal; fall back to existing table
            log.warning("Meta table refresh failed (non-fatal, using existing data): %s", exc)

    rows, missing = fetch_rows()
    if not rows:
        log.warning("No Meta video rows for locales %s — writing empty sheet with note.", LOCALES)

    import gspread
    from google.oauth2.service_account import Credentials

    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=scopes)
    gc = gspread.authorize(creds)

    sh, ws = _open_or_create(gc, args)
    _write(sh, ws, rows, missing)
    print(f"OK: {len(rows)} rows → https://docs.google.com/spreadsheets/d/{sh.id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

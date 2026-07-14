"""Video-ad metrics → Google Sheet (agency-facing, daily refresh).

The media agency produces video creatives that go live across Meta, TikTok,
Reddit and Google/YouTube. This script maintains a shared Google Sheet with
**one tab per ramp id** (e.g. "GMR-0023"), each tab listing every video that is
live for that ramp — one row per (channel × locale) — with lifetime-to-date
delivery, video-engagement, and derived rates (incl. completion rate + avg
watch time). An "Overview" tab indexes the ramps and flags channel coverage.

Scope: tabs are created for any ramp that currently has live video delivery in
the rolling window and are NEVER deleted (history preserved). Ramps that never
ran agency video simply never get a tab — so there is no historical backfill;
new ramps appear automatically as their videos go live.

Channel coverage TODAY:
  - Meta   ✓ video-vs-static delivery + engagement from the Meta Marketing API
           (meta_creative_format_daily). Delivery/engagement only — activations
           are NOT format-attributable (issues #94/#95).
  - YouTube / Reddit  — API is enabled but there is no per-creative video-format
           extractor yet (campaign-level metrics only). Pending build.
  - TikTok — blocked: TIKTOK_API_ENABLED is false (no API creds yet).
Pending channels are listed on the Overview tab so the gap is visible.

Scheduling: .github/workflows/meta_video_gsheet.yml runs this daily in CI so the
sheet stays fresh regardless of any local session. Sheet id lives in Doppler as
META_VIDEO_GSHEET_ID (dev + prd). Standalone:

    # first time — create + share the sheet, print its id/url:
    doppler run -- python3 scripts/refresh_meta_video_gsheet.py --create \
        --share-email pranav.patre@scale.com

    # subsequent refreshes:
    doppler run -- python3 scripts/refresh_meta_video_gsheet.py
"""
from __future__ import annotations

import argparse
import logging
import os
import sys
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402

log = logging.getLogger("refresh_meta_video_gsheet")

# "Mexican" tasking is es-MX; the warehouse stores it as display language Spanish.
DISPLAY_LOCALE = {"Spanish": "Mexican (es-MX)"}

# Overview coverage note. Monitored = extractor live, rows appear once a
# GMR-named video campaign delivers. Pending = not yet wired.
CHANNELS_MONITORED = {
    "Meta": "live — video delivery + engagement from the Meta Marketing API",
    "Reddit": "live — keyed by GMR ramp id; awaiting GMR-named video campaigns",
    "YouTube": "live — keyed by GMR ramp id; awaiting GMR-named video campaigns",
}
CHANNELS_PENDING = {
    "TikTok": "blocked — TIKTOK_API_ENABLED=false (no API creds yet)",
}

HEADERS = [
    "Channel", "Locale",
    # ── who this video is targeting (from the campaign registry) ──
    "ICP / cohort", "Geo", "Pay rate", "Est. audience", "Targeting detail",
    # ── delivery + engagement ──
    "Launched", "Last day", "Days live",
    "Impressions", "Video plays", "3-sec views", "ThruPlays",
    "Watched 25%", "Watched 50%", "Watched 75%", "Watched 100%",
    "Avg watch time (s)", "Completion % (100%/plays)",
    "Clicks", "CTR %", "Hook rate % (3s/plays)", "ThruPlay rate % (thru/plays)",
    "Reactions", "Comments", "Shares", "Saves",
]
HEADER_ROW = 4  # title(1) subtitle(2) blank(3) header(4)


def _ci(name: str) -> int:
    """1-based column index of a header (keeps number-formatting robust to
    column moves)."""
    return HEADERS.index(name) + 1


def _f(x) -> float:
    try:
        return float(x or 0)
    except (TypeError, ValueError):
        return 0.0


# ── targeting / ICP (joined from the campaign registry) ────────────────────────
# Which registry channels back each video channel's targeting.
_REG_CHANNELS = {"Meta": {"meta"}, "YouTube": {"google", "google_search"}, "Reddit": {"reddit"}}
_AUD_FIELD = {"Meta": "meta_audience_size", "YouTube": "google_audience_size", "Reddit": None}


def _clean_icp(v) -> str:
    """cohort_signature is occasionally polluted with the campaign name (some
    Google rows; sometimes slugified/lowercased) — drop anything that looks like
    a campaign name rather than an ICP description."""
    v = (v or "").strip()
    low = v.lower()
    if not v or "gmr" in low or "|" in v or low.startswith("scale-"):
        return ""
    return v


def _load_registry_rows() -> list[dict]:
    """Authoritative campaign rows. Postgres is current in CI; fall back to the
    committed JSON locally / on failure."""
    try:
        from src.ui_decisions import list_all_campaign_data
        rows = list_all_campaign_data()
        if rows:
            return rows
    except Exception as exc:  # noqa: BLE001
        log.warning("targeting: Postgres registry read failed (%s) — using local JSON", exc)
    try:
        from src.campaign_registry import _load
        return _load()
    except Exception:  # noqa: BLE001
        return []


def build_targeting_index() -> dict:
    """Index registry targeting by (ramp, language, channel) plus a channel-
    agnostic (ramp, language, '*') fallback for the cohort-level ICP fields."""
    import collections
    import json as _json
    from src.creative_format_metrics import _lang_of, _ramp_of

    idx: dict = collections.defaultdict(lambda: collections.defaultdict(set))
    for r in _load_registry_rows():
        if not isinstance(r, dict):
            continue
        name = r.get("campaign_name", "")
        ramp, lang = _ramp_of(name), _lang_of(name)
        if not (ramp and lang):
            continue
        ch = str(r.get("channel", "")).lower()
        icp = _clean_icp(r.get("cohort_signature"))
        geo_label = (r.get("geo_cluster_label") or "").strip()
        geos = (r.get("geos") or "").strip()
        geo = f"{geo_label} ({geos})" if (geo_label and geos) else (geo_label or geos)
        rate = (r.get("advertised_rate") or "").strip()
        for key in ((ramp, lang, ch), (ramp, lang, "*")):
            if icp:
                idx[key]["icp"].add(icp)
            if geo:
                idx[key]["geo"].add(geo)
            if rate:
                idx[key]["rate"].add(rate)
        for fld in ("meta_audience_size", "google_audience_size", "audience_size"):
            if r.get(fld):
                idx[(ramp, lang, ch)][fld].add(int(r[fld]))
        kw = r.get("google_keywords")
        if kw:
            try:
                lst = _json.loads(kw) if isinstance(kw, str) else kw
                if isinstance(lst, list):
                    idx[(ramp, lang, ch)]["keywords"].update(str(x) for x in lst[:12])
            except Exception:  # noqa: BLE001
                pass
    return idx


def _targeting_for(idx: dict, ramp: str, lang: str, video_channel: str) -> dict:
    """Resolve targeting for a (ramp × locale × channel) video row: channel-
    specific where available, cohort-level fallback for ICP/geo/rate."""
    icp, geo, rate, aud, kw = set(), set(), set(), set(), set()
    aud_field = _AUD_FIELD.get(video_channel)
    for ch in _REG_CHANNELS.get(video_channel, set()):
        d = idx.get((ramp, lang, ch), {})
        icp |= d.get("icp", set()); geo |= d.get("geo", set()); rate |= d.get("rate", set())
        kw |= d.get("keywords", set())
        if aud_field:
            aud |= d.get(aud_field, set())
    agnostic = idx.get((ramp, lang, "*"), {})
    icp = icp or agnostic.get("icp", set())
    geo = geo or agnostic.get("geo", set())
    rate = rate or agnostic.get("rate", set())

    def j(s):
        return " · ".join(sorted(s))
    return {"icp": j(icp), "geo": j(geo), "rate": j(rate),
            "audience": max(aud) if aud else None,
            "detail": (", ".join(sorted(kw))[:200]) if kw else ""}


def _row_cells(channel, locale, t, launched, last, days, m) -> list:
    """Build a display row aligned to HEADERS from targeting dict `t` and
    raw-metric dict `m`. A None metric renders blank (channel doesn't support
    it); derived rates blank when their inputs are missing — never fabricated."""
    def I(v):
        return "" if v is None else int(v)

    def rate(num, den):
        return round(num / den * 100, 1) if (num is not None and den) else ""

    t = t or {}
    imp, plays, v3, thru, p100, ws, clk = (m.get(k) for k in ("imp", "plays", "v3", "thru", "p100", "ws", "clk"))
    return [
        channel, locale,
        t.get("icp", ""), t.get("geo", ""), t.get("rate", ""),
        "" if t.get("audience") is None else int(t["audience"]), t.get("detail", ""),
        str(launched) if launched else "", str(last) if last else "",
        int(days) if days else "", I(imp), I(plays), I(v3), I(thru),
        I(m.get("p25")), I(m.get("p50")), I(m.get("p75")), I(p100),
        round(ws / plays, 1) if (ws is not None and plays) else "",
        rate(p100, plays),
        I(clk),
        round(clk / imp * 100, 3) if (clk is not None and imp) else "",
        rate(v3, plays),
        rate(thru, plays),
        I(m.get("rx")), I(m.get("cm")), I(m.get("sh")), I(m.get("sv")),
    ]


# ── channel fetchers ──────────────────────────────────────────────────────────
_METRIC_KEYS = ("imp", "plays", "v3", "thru", "p25", "p50", "p75", "p100",
                "ws", "clk", "spend", "rx", "cm", "sh", "sv")


def fetch_meta(conn) -> list[dict]:
    """One entry per (ramp × locale) Meta video. Returns dicts with ramp_id,
    channel, launched, last, days, and a raw-metric dict `m`."""
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ramp_id, language, MIN(metric_date), MAX(metric_date),
                   COUNT(DISTINCT metric_date),
                   SUM(impressions), SUM(video_plays), SUM(video_3sec), SUM(video_thruplays),
                   SUM(video_p25), SUM(video_p50), SUM(video_p75), SUM(video_p100),
                   SUM(video_watch_seconds), SUM(clicks), SUM(spend_usd),
                   SUM(reactions), SUM(comments), SUM(shares), SUM(saves)
            FROM meta_creative_format_daily
            WHERE creative_format = 'video'
            GROUP BY ramp_id, language
            ORDER BY ramp_id, language
            """
        )
        raw = cur.fetchall()
    out = []
    for r in raw:
        (ramp, lang, launched, last, days, imp, plays, v3, thru,
         p25, p50, p75, p100, ws, clk, spend, rx, cm, sh, sv) = r
        vals = (imp, plays, v3, thru, p25, p50, p75, p100, ws, clk, spend, rx, cm, sh, sv)
        m = {k: _f(v) for k, v in zip(_METRIC_KEYS, vals)}
        out.append({"ramp_id": ramp, "channel": "Meta", "lang": lang,
                    "locale": DISPLAY_LOCALE.get(lang, lang),
                    "launched": launched, "last": last, "days": int(days), "m": m})
    return out


def fetch_all_video_rows(conn) -> list[dict]:
    """All monitored channels. Meta comes from its persisted daily table;
    Reddit + YouTube are pulled live (best-effort) and keyed by ramp id — they
    yield rows once GMR-named video campaigns go live. TikTok is still blocked."""
    from src.video_format_metrics import build_reddit_video_rows, build_youtube_video_rows
    rows = fetch_meta(conn)
    rows += build_reddit_video_rows()
    rows += build_youtube_video_rows()
    return rows


# ── sheet writing ───────────────────────────────────────────────────────────
def _ws(sh, title, rows=200, cols=30):
    import gspread
    try:
        return sh.worksheet(title)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=rows, cols=cols)


def _a1(r1, c1, r2, c2):
    from gspread.utils import rowcol_to_a1
    return f"{rowcol_to_a1(r1, c1)}:{rowcol_to_a1(r2, c2)}"


def _write_ramp_tab(sh, ramp_id: str, entries: list[dict], targeting: dict, refreshed: str) -> None:
    """Write one ramp's tab: header + one row per (channel × locale) + TOTAL."""
    ws = _ws(sh, ramp_id)
    entries = sorted(entries, key=lambda e: (e["channel"], e["locale"]))
    data_rows = [_row_cells(
        e["channel"], e["locale"],
        _targeting_for(targeting, e["ramp_id"], e.get("lang", e["locale"]), e["channel"]),
        e["launched"], e["last"], e["days"], e["m"]) for e in entries]
    # TOTAL across every video in the ramp. None = channel didn't supply the
    # metric → summed as absent; stays blank if no channel supplied it.
    def _sum(k):
        vals = [e["m"].get(k) for e in entries if e["m"].get(k) is not None]
        return sum(vals) if vals else None
    tot = {k: _sum(k) for k in _METRIC_KEYS}
    data_rows.append(_row_cells("TOTAL", "", {}, "", "", 0, tot))

    channels = ", ".join(sorted({e["channel"] for e in entries}))
    subtitle = f"{ramp_id} · video creatives · channels: {channels} · refreshed {refreshed}"
    body = [[f"Video Ad Metrics — {ramp_id}"], [subtitle], [], HEADERS] + data_rows

    ws.clear()
    ws.update(body, "A1", value_input_option="RAW")

    n = len(HEADERS)
    last_row = HEADER_ROW + len(data_rows)
    total_row = last_row
    top = HEADER_ROW + 1

    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A2", {"textFormat": {"italic": True, "fontSize": 10,
                                    "foregroundColor": {"red": .4, "green": .4, "blue": .4}}})
    ws.format(_a1(HEADER_ROW, 1, HEADER_ROW, n), {
        "backgroundColor": {"red": .122, "green": .306, "blue": .471},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}},
        "wrapStrategy": "WRAP", "verticalAlignment": "MIDDLE", "horizontalAlignment": "CENTER",
    })
    ws.format(_a1(total_row, 1, total_row, n),
              {"backgroundColor": {"red": .905, "green": .933, "blue": .968},
               "textFormat": {"bold": True}})
    fmt = {
        "#,##0": ["Est. audience", "Impressions", "Video plays", "3-sec views", "ThruPlays",
                 "Watched 25%", "Watched 50%", "Watched 75%", "Watched 100%",
                 "Clicks", "Reactions", "Comments", "Shares", "Saves"],
        '0.0"%"': ["Completion % (100%/plays)", "CTR %", "Hook rate % (3s/plays)",
                   "ThruPlay rate % (thru/plays)"],
        "0.0": ["Avg watch time (s)"],
    }
    for pattern, names in fmt.items():
        for name in names:
            c = _ci(name)
            ws.format(_a1(top, c, last_row, c), {"numberFormat": {"type": "NUMBER", "pattern": pattern}})
    # Wrap the free-text targeting columns so they stay readable.
    for name in ("ICP / cohort", "Geo", "Targeting detail"):
        c = _ci(name)
        ws.format(_a1(top, c, last_row, c), {"wrapStrategy": "WRAP"})

    detail_i = _ci("Targeting detail") - 1  # 0-based for the dimension range
    sh.batch_update({"requests": [
        {"updateSheetProperties": {
            "properties": {"sheetId": ws.id,
                           "gridProperties": {"frozenRowCount": HEADER_ROW, "frozenColumnCount": 2}},
            "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}},
        {"autoResizeDimensions": {"dimensions": {"sheetId": ws.id, "dimension": "COLUMNS",
                                                 "startIndex": 0, "endIndex": n}}},
        # Clamp the free-text columns so auto-resize can't blow the layout wide.
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": _ci("ICP / cohort") - 1, "endIndex": _ci("ICP / cohort")},
            "properties": {"pixelSize": 200}, "fields": "pixelSize"}},
        {"updateDimensionProperties": {
            "range": {"sheetId": ws.id, "dimension": "COLUMNS", "startIndex": detail_i, "endIndex": detail_i + 1},
            "properties": {"pixelSize": 260}, "fields": "pixelSize"}},
    ]})
    log.info("  %s: %d video rows across %s", ramp_id, len(entries), channels)


def _write_overview(sh, by_ramp: dict, refreshed: str) -> None:
    ws = _ws(sh, "Overview")
    ov_headers = ["Ramp", "Tab", "Channels live", "Locales", "Videos (rows)",
                  "Impressions"]
    rows = []
    for ramp in sorted(by_ramp):
        entries = by_ramp[ramp]
        chans = ", ".join(sorted({e["channel"] for e in entries}))
        locs = ", ".join(sorted({e["locale"] for e in entries}))
        imp = sum(e["m"].get("imp") or 0 for e in entries)
        rows.append([ramp, ramp, chans, locs, len(entries), int(imp)])

    coverage = ([["", ""], ["Channel coverage", ""]]
                + [[ch, why] for ch, why in CHANNELS_MONITORED.items()]
                + [[ch, why] for ch, why in CHANNELS_PENDING.items()])
    body = ([["Video Ad Metrics — Overview"],
             [f"One tab per ramp · refreshed {refreshed}"],
             [], ov_headers] + rows + coverage)
    ws.clear()
    ws.update(body, "A1", value_input_option="RAW")
    ws.format("A1", {"textFormat": {"bold": True, "fontSize": 14}})
    ws.format("A2", {"textFormat": {"italic": True, "fontSize": 10,
                                    "foregroundColor": {"red": .4, "green": .4, "blue": .4}}})
    ws.format(_a1(4, 1, 4, len(ov_headers)), {
        "backgroundColor": {"red": .122, "green": .306, "blue": .471},
        "textFormat": {"bold": True, "foregroundColor": {"red": 1, "green": 1, "blue": 1}}})
    pend_row = 4 + len(rows) + 2
    ws.format(f"A{pend_row}", {"textFormat": {"bold": True, "foregroundColor": {"red": .75, "green": 0, "blue": 0}}})
    ws.format(_a1(5, 6, 4 + len(rows), 6), {"numberFormat": {"type": "NUMBER", "pattern": "#,##0"}})
    sh.batch_update({"requests": [
        {"updateSheetProperties": {"properties": {"sheetId": ws.id, "gridProperties": {"frozenRowCount": 4}},
                                    "fields": "gridProperties.frozenRowCount"}},
        {"autoResizeDimensions": {"dimensions": {"sheetId": ws.id, "dimension": "COLUMNS",
                                                 "startIndex": 0, "endIndex": len(ov_headers)}}},
    ]})


def _get_spreadsheet(gc, args):
    if args.create:
        title = args.title or "Video Ad Metrics — Agency View"
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
            except Exception as exc:  # noqa: BLE001 — org policy may forbid public links
                log.warning("Could not enable anyone-with-link (share manually): %s", exc)
        return sh
    sheet_id = args.sheet_id or os.environ.get("META_VIDEO_GSHEET_ID", "").strip()
    if not sheet_id:
        raise RuntimeError("No sheet id: pass --sheet-id, set META_VIDEO_GSHEET_ID, or use --create")
    return gc.open_by_key(sheet_id)


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    ap = argparse.ArgumentParser(description="Refresh the agency-facing video-metrics Google Sheet (one tab per ramp).")
    ap.add_argument("--create", action="store_true", help="Create a new spreadsheet and print its id/url.")
    ap.add_argument("--sheet-id", default="", help="Target spreadsheet id (else META_VIDEO_GSHEET_ID env).")
    ap.add_argument("--title", default="", help="Title for the new sheet (with --create).")
    ap.add_argument("--share-email", default="", help="Email to grant writer access (with --create).")
    ap.add_argument("--folder-id", default="", help="Shared Drive/folder id for --create. Defaults to config.GDRIVE_DRIVE_ID.")
    ap.add_argument("--link-viewable", action="store_true", help="Anyone-with-link viewer (with --create; org policy may block).")
    ap.add_argument("--no-refresh-table", action="store_true", help="Skip rebuilding meta_creative_format_daily from the Meta API first.")
    ap.add_argument("--window", type=int, default=30, help="Meta API look-back window when refreshing the table.")
    args = ap.parse_args()

    if not args.no_refresh_table:
        try:
            from src.creative_format_metrics import build_meta_creative_format_daily
            wrote = build_meta_creative_format_daily(window_days=args.window)
            log.info("Refreshed meta_creative_format_daily: %d rows", wrote)
        except Exception as exc:  # noqa: BLE001 — non-fatal; fall back to existing table
            log.warning("Meta table refresh failed (non-fatal, using existing data): %s", exc)

    import psycopg
    url = os.environ.get("DATABASE_URL", "").strip()
    if not url:
        raise RuntimeError("DATABASE_URL not set — cannot read video metrics")
    with psycopg.connect(url, connect_timeout=15) as conn:
        all_rows = fetch_all_video_rows(conn)

    by_ramp: dict[str, list[dict]] = defaultdict(list)
    for r in all_rows:
        by_ramp[r["ramp_id"]].append(r)

    import gspread
    from google.oauth2.service_account import Credentials
    scopes = ["https://www.googleapis.com/auth/spreadsheets", "https://www.googleapis.com/auth/drive"]
    creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=scopes)
    gc = gspread.authorize(creds)
    sh = _get_spreadsheet(gc, args)

    targeting = build_targeting_index()
    refreshed = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    if not by_ramp:
        log.warning("No live video rows on any channel — writing Overview only.")
    _write_overview(sh, by_ramp, refreshed)
    for ramp in sorted(by_ramp):
        _write_ramp_tab(sh, ramp, by_ramp[ramp], targeting, refreshed)

    # Repurpose the default empty "Sheet1" if it's still hanging around.
    try:
        stray = sh.worksheet("Sheet1")
        if stray.title not in by_ramp and stray.title != "Overview":
            sh.del_worksheet(stray)
    except Exception:  # noqa: BLE001 — no stray sheet, fine
        pass

    print(f"OK: {len(by_ramp)} ramp tab(s) + Overview → https://docs.google.com/spreadsheets/d/{sh.id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

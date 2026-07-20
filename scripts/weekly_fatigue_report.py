"""Weekly creative-fatigue report → #outlier-campaign-atomation-bot.

Scores every launched ramp for creative fatigue (src/fatigue.compute_fatigue —
Meta 7-day frequency + CTR week-over-week), then posts a summary to the campaign
automation Slack bot via the tokenless Drive-queue path. Approval (add fresh
creatives / pause weak ads) happens in the console "Fatigue" tab — the tokenless
Slack path can't render interactive buttons — so each ramp links there.

Scheduled by .github/workflows/weekly_fatigue_report.yml (Monday). Standalone:

    doppler run -- python3 scripts/weekly_fatigue_report.py --dry-run
    doppler run -- python3 scripts/weekly_fatigue_report.py --ramp-id GMR-0023
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402

log = logging.getLogger("weekly_fatigue_report")


def _launched_ramp_ids() -> list[str]:
    from src.ui_decisions import _connect
    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ramp_id FROM ramp_decisions "
            "WHERE status::text IN ('launching','completed') ORDER BY ramp_id"
        )
        return [r[0] for r in cur.fetchall()]


def _fmt_signal(s: dict) -> str:
    parts = []
    if s.get("frequency") is not None:
        parts.append(f"freq {s['frequency']}")
    if s.get("ctr_wow_pct") is not None:
        parts.append(f"CTR {s['ctr_wow_pct']:+.0f}% WoW")
    if s.get("spend_usd") is not None:
        parts.append(f"${s['spend_usd']:,.0f} spent")
    if s.get("weak_ad_count"):
        parts.append(f"{s['weak_ad_count']} weak creative(s)")
    return " · ".join(parts) or "—"


def build_report(ramp_ids: list[str]) -> tuple[str, int]:
    """Return (report_text, total_fatigued). Empty text when nothing is fatiguing."""
    from src.fatigue import compute_fatigue

    sections: list[str] = []
    total = 0
    for ramp_id in ramp_ids:
        try:
            rows = compute_fatigue(ramp_id)  # persists to ramp_fatigue too
        except Exception as exc:
            log.warning("fatigue compute failed for %s: %s", ramp_id, exc)
            continue
        if not rows:
            continue
        reached = [r for r in rows if r["classification"] == "reached"]
        reaching = [r for r in rows if r["classification"] == "reaching"]
        total += len(rows)
        console_link = f"{config.OUTLIER_CONSOLE_URL}/ramps/{ramp_id}"
        lines = [
            f"*{ramp_id}* — {len(reached)} reached, {len(reaching)} reaching fatigue",
        ]
        for r in (reached + reaching)[:6]:
            tag = "🔴" if r["classification"] == "reached" else "🟠"
            label = f"{r['cohort_signature']} · {r['geo_cluster']}".strip(" ·")
            lines.append(f"  {tag} {label}: {_fmt_signal(r['signals'])}")
        lines.append(f"  → Review + approve in the console Fatigue tab: {console_link}")
        sections.append("\n".join(lines))

    if not sections:
        return "", 0

    header = (
        "*Weekly creative-fatigue report*\n"
        "Campaigns showing fatigue (audiences seeing creatives too often, or click "
        "progress declining). Approve a creative refresh, or pause the weak creatives, "
        "in each ramp's Fatigue tab.\n"
        "🔴 = reached fatigue (act now) · 🟠 = reaching fatigue (early warning)\n"
    )
    return header + "\n\n".join(sections), total


def main() -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--ramp-id", default=None, help="Only this ramp (default: all launched).")
    ap.add_argument("--dry-run", action="store_true", help="Print the report; don't post to Slack.")
    args = ap.parse_args()

    if not getattr(config, "FATIGUE_ENABLED", True):
        log.info("FATIGUE_ENABLED off — skipping.")
        return 0

    ramp_ids = [args.ramp_id] if args.ramp_id else _launched_ramp_ids()
    if not ramp_ids:
        log.info("No launched ramps. Exiting.")
        return 0
    log.info("Scoring fatigue for %d ramp(s): %s", len(ramp_ids), ", ".join(ramp_ids))

    text, total = build_report(ramp_ids)
    if not text:
        log.info("No fatiguing campaigns across %d ramp(s) — nothing to report.", len(ramp_ids))
        return 0

    if args.dry_run:
        print("\n===== WEEKLY FATIGUE REPORT (dry-run) =====\n")
        print(text)
        print(f"\n===== {total} fatiguing campaign(s); would post to #outlier-campaign-atomation-bot =====")
        return 0

    from src.smart_ramp_notifier import _send_to_all_targets
    outcomes = _send_to_all_targets(text, ramp_id="fatigue-weekly", targets=config.SLACK_RAMP_NOTIFY_TARGETS)
    log.info("Weekly fatigue report sent (%d fatiguing): %s", total, outcomes)
    return 0


if __name__ == "__main__":
    sys.exit(main())

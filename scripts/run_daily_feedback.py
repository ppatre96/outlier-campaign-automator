"""Phase 6 entrypoint — daily per-campaign recommendation pass.

Iterates every ramp in `ramp_decisions` with `status IN ('launching','completed')`
and calls `FeedbackAgent.recommend_actions(ramp_id)`. Each call walks that
ramp's rows in the local Campaign Registry, classifies them (working /
underperforming / failing / insufficient_data), and upserts the result
into `ramp_recommendations` so the console can render Accept / Reject.

Invoked by .github/workflows/daily_feedback.yml on the daily cron. Safe to
run standalone too:

    doppler run -- python3 scripts/run_daily_feedback.py
    doppler run -- python3 scripts/run_daily_feedback.py --ramp-id GMR-0020
"""
from __future__ import annotations

import argparse
import logging
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import config  # noqa: E402  (needs _PROJECT_ROOT on sys.path first)


log = logging.getLogger("run_daily_feedback")


def _launched_ramp_ids() -> list[str]:
    """Read ramp_ids whose status indicates campaigns are live or were live.

    'launching' covers the window between Approve click and poller's
    completion event. 'completed' covers the steady-state. Other statuses
    (awaiting_approval, prep_running, failed) have no campaigns to score.
    """
    from src.ui_decisions import _connect

    with _connect() as conn, conn.cursor() as cur:
        cur.execute(
            "SELECT ramp_id FROM ramp_decisions "
            "WHERE status::text IN ('launching','completed') "
            "ORDER BY ramp_id"
        )
        return [row[0] for row in cur.fetchall()]


def _all_registry_ramp_ids() -> list[str]:
    """Fallback when Postgres is unavailable — iterate every ramp_id present
    in the local Campaign Registry. Less precise but keeps the workflow
    useful in dev / when DATABASE_URL is unset."""
    from src import campaign_registry

    rows = campaign_registry.get_active_campaigns()
    return sorted({r.get("smart_ramp_id") for r in rows if r.get("smart_ramp_id")})


def main() -> int:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--ramp-id",
        default=None,
        help="If set, only score this ramp; otherwise iterate all launched ramps.",
    )
    args = parser.parse_args()

    from src.feedback_agent import FeedbackAgent
    from src.redash_db import RedashClient
    from src.ui_decisions import UIDecisionsUnavailable

    if args.ramp_id:
        ramp_ids = [args.ramp_id]
    else:
        try:
            ramp_ids = _launched_ramp_ids()
        except UIDecisionsUnavailable as exc:
            log.warning(
                "Postgres unavailable (%s) — falling back to registry-wide iteration",
                exc,
            )
            ramp_ids = _all_registry_ramp_ids()

    if not ramp_ids:
        log.info("No launched ramps to score. Exiting cleanly.")
        return 0

    log.info("Scoring %d ramp(s): %s", len(ramp_ids), ", ".join(ramp_ids))

    # RedashClient init only matters for legacy methods on FeedbackAgent — the
    # recommend_actions path reads from campaign_registry, not Redash. Pass a
    # bare client so init doesn't probe the network in CI without secrets.
    try:
        client = RedashClient()
    except Exception as exc:
        log.warning("RedashClient init failed (%s) — using None placeholder", exc)
        client = None  # type: ignore[assignment]

    agent = FeedbackAgent(redash_client=client)  # type: ignore[arg-type]

    total = 0
    errors = 0
    for ramp_id in ramp_ids:
        try:
            recs = agent.recommend_actions(ramp_id)
            total += len(recs)
            log.info("  %s → %d recommendations", ramp_id, len(recs))
            # Angle double-down: decide which angle wins per cohort, surface
            # scale/refresh/pause into the console "Live performance &
            # recommendations" section + post a Slack change summary. Live
            # execution gated behind ANGLE_AUTO_ACT_ENABLED (default off). Same
            # per-ramp try/except → one ramp's failure never aborts the pass.
            if config.ANGLE_LOOP_ENABLED:
                from src import angle_performance
                verdicts = angle_performance.analyze_angles(ramp_id)
                angle_performance.act_on_verdicts(
                    verdicts, ramp_id=ramp_id, auto_act=config.ANGLE_AUTO_ACT_ENABLED,
                )
        except Exception as exc:
            errors += 1
            log.exception("Failed to score %s: %s", ramp_id, exc)

    log.info(
        "Daily feedback pass complete: %d recommendations written across %d ramps (errors=%d)",
        total, len(ramp_ids), errors,
    )
    # Non-zero exit only if EVERY ramp failed — partial failures are routine
    # (a single ramp's missing data shouldn't fail the workflow).
    return 1 if errors and errors == len(ramp_ids) else 0


if __name__ == "__main__":
    sys.exit(main())

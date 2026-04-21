"""
Campaign lifecycle monitor.
  1. Check LinkedIn learning phase completion per campaign
  2. Query Snowflake for downstream pass rates since campaign launch
  3. Score campaigns — tag KEEP / PAUSE / TEST_NEW
  4. Pause underperformers via LinkedIn API
  5. Discover new ICPs for flows with paused campaigns
  6. Write monitor results back to a sheet tab
"""
import logging
from datetime import datetime, timezone

import requests

import config

log = logging.getLogger(__name__)

# ── Context notes ──────────────────────────────────────────────────────────────
#
# GEO RESTRICTIONS: Campaigns may not run in all geos due to client-imposed
# restrictions, not just our own targeting choices. Low volume or missing geos
# should not be interpreted as a TG signal — always check whether a geo
# restriction is in place before drawing conclusions about TG performance.
#
# TG EARNINGS BASELINE: Lower conversion rates for high-skill TGs (e.g. PhD-
# level STEM, Math specialists) do not automatically indicate weak targeting.
# These professionals typically have higher average earnings and fewer financial
# pain points, making them harder to convert via financial hooks alone. When
# comparing pass rates or CPAs across TGs (e.g. Math vs. Languages), normalise
# for earnings baseline before concluding that a TG is underperforming.
# ──────────────────────────────────────────────────────────────────────────────

# Relative threshold: a campaign is flagged if its pass_rate is below
# (cohort_avg * (1 - UNDERPERFORM_THRESHOLD))
UNDERPERFORM_THRESHOLD = config.CAMPAIGN_UNDERPERFORM_THRESHOLD


# ── LinkedIn learning phase ────────────────────────────────────────────────────

def check_learning_phase(li_client, campaign_ids: list[str]) -> dict[str, bool]:
    """
    Check whether each campaign has exited LinkedIn's learning phase.
    Returns {campaign_id: is_learning_complete}.

    LinkedIn marks learning complete when:
      - servingStatuses no longer contains "LEARNING"
      - OR runningStatus is "ACTIVE" (not "PAUSED_LEARNING")
    """
    if not campaign_ids:
        return {}

    results = {}
    # Batch up to 20 per call
    batch_size = 20
    for i in range(0, len(campaign_ids), batch_size):
        batch = campaign_ids[i : i + batch_size]
        ids_param = ",".join(
            f"urn:li:sponsoredCampaign:{cid}" if not cid.startswith("urn:") else cid
            for cid in batch
        )
        try:
            resp = li_client._session.get(
                li_client._url("adCampaigns"),
                params={
                    "ids": ids_param,
                    "fields": "id,status,runningStatus,servingStatuses",
                },
            )
            li_client._raise_for_status(resp, "checkLearningPhase")
            data = resp.json()
            elements = data.get("results") or data.get("elements") or {}

            for elem in (elements.values() if isinstance(elements, dict) else elements):
                raw_id = str(elem.get("id", "")).rsplit(":", 1)[-1]
                serving = elem.get("servingStatuses", [])
                running = elem.get("runningStatus", "")
                is_learning = "LEARNING" in serving or running in ("PAUSED_LEARNING",)
                results[raw_id] = not is_learning
                log.debug("Campaign %s learning_complete=%s (serving=%s running=%s)",
                          raw_id, results[raw_id], serving, running)

        except Exception as exc:
            log.warning("Failed to check learning phase for batch: %s", exc)
            for cid in batch:
                results[cid] = False   # treat as still in learning if unknown

    return results


# ── Pass rate query ────────────────────────────────────────────────────────────

def get_pass_rates_from_snowflake(
    snowflake,
    flow_ids: list[str],
    since_date: str,
) -> dict[str, float]:
    """
    Query Snowflake for pass rates per flow since campaign launch.
    Returns {flow_id: pass_rate_pct}.
    """
    results = {}
    for flow_id in flow_ids:
        try:
            df = snowflake.fetch_pass_rates_since(flow_id, since_date)
            if df.empty:
                results[flow_id] = 0.0
                continue
            # Sum across all UTM sources for this flow
            total_n      = df["n"].sum()
            total_passes = df["passes"].sum()
            rate         = (total_passes / total_n * 100) if total_n > 0 else 0.0
            results[flow_id] = round(float(rate), 2)
            log.info("Flow %s pass_rate=%.2f%% (%d/%d since %s)",
                     flow_id, rate, total_passes, total_n, since_date)
        except Exception as exc:
            log.warning("Pass rate query failed for flow %s: %s", flow_id, exc)
            results[flow_id] = 0.0
    return results


# ── Scoring ────────────────────────────────────────────────────────────────────

def score_campaigns(
    campaigns: list[dict],
    pass_rates: dict[str, float],
) -> list[dict]:
    """
    Score each campaign against the cohort average.
    campaign dict keys: campaign_id, flow_id, stg_id, stg_name, launch_date
    Returns list of campaigns with added 'pass_rate', 'verdict' (KEEP/PAUSE/TEST_NEW).
    """
    # Compute cohort average (mean of all campaigns that have data)
    rates_with_data = [pass_rates[c["flow_id"]] for c in campaigns if c["flow_id"] in pass_rates and pass_rates[c["flow_id"]] > 0]
    cohort_avg = sum(rates_with_data) / len(rates_with_data) if rates_with_data else 0.0
    threshold  = cohort_avg * (1 - UNDERPERFORM_THRESHOLD)

    log.info("Cohort avg pass_rate=%.2f%% threshold=%.2f%%", cohort_avg, threshold)

    scored = []
    for c in campaigns:
        rate = pass_rates.get(c["flow_id"], 0.0)
        if rate == 0.0:
            verdict = "INSUFFICIENT_DATA"
        elif rate < threshold:
            verdict = "PAUSE"
        elif rate >= cohort_avg * 1.2:
            verdict = "TEST_NEW"   # top performer — also look for new ICPs in same space
        else:
            verdict = "KEEP"

        scored.append({**c, "pass_rate": rate, "cohort_avg": round(cohort_avg, 2), "verdict": verdict})
        log.info("Campaign %s flow=%s rate=%.2f%% verdict=%s",
                 c.get("campaign_id", "?"), c["flow_id"], rate, verdict)

    return scored


# ── Pause ──────────────────────────────────────────────────────────────────────

def pause_campaign(li_client, campaign_id: str) -> None:
    """Pause a LinkedIn campaign by ID."""
    urn = campaign_id if campaign_id.startswith("urn:") else f"urn:li:sponsoredCampaign:{campaign_id}"
    payload = {"patch": {"$set": {"status": "PAUSED"}}}
    resp = li_client._session.post(
        li_client._url(f"adCampaigns/{campaign_id}"),
        json=payload,
    )
    li_client._raise_for_status(resp, f"pauseCampaign:{campaign_id}")
    log.info("Paused campaign %s", urn)


# ── Discover new ICPs ──────────────────────────────────────────────────────────

def discover_new_icps(
    snowflake,
    flow_id: str,
    config_name: str,
    existing_rule_sets: list[list[tuple]],
) -> list:
    """
    Re-run Stages A+B on fresh Snowflake data for a flow and return
    cohorts whose rules don't overlap with any existing active campaign.

    Returns a list of Cohort objects (may be empty).
    """
    from src.features import engineer_features, build_frequency_maps, binary_features
    from src.analysis import stage_a, stage_b

    try:
        df_raw = snowflake.fetch_screenings(flow_id, config_name)
        if df_raw.empty:
            return []

        df     = engineer_features(df_raw)
        freqs  = build_frequency_maps(df, min_freq=5)
        df_bin = binary_features(df, freqs)
        binary_cols = [
            c for c in df_bin.columns
            if c.startswith((
                "skills__", "job_titles_norm__", "fields_of_study__",
                "highest_degree_level__", "accreditations_norm__", "experience_band__",
            ))
        ]

        cohorts_a = stage_a(df_bin, binary_cols)
        cohorts_b = stage_b(df_bin, cohorts_a)

        # Filter out cohorts whose features overlap with existing campaigns
        existing_features = {feat for rules in existing_rule_sets for feat, _ in rules}

        new_cohorts = []
        for c in cohorts_b:
            cohort_features = {feat for feat, _ in c.rules}
            overlap = cohort_features & existing_features
            if len(overlap) / max(len(cohort_features), 1) < 0.5:  # < 50% overlap
                new_cohorts.append(c)

        log.info("Discovered %d new ICPs for flow=%s (excluded %d overlapping)",
                 len(new_cohorts), flow_id, len(cohorts_b) - len(new_cohorts))
        return new_cohorts

    except Exception as exc:
        log.error("discover_new_icps failed for flow=%s: %s", flow_id, exc)
        return []


# ── Sheet helpers ──────────────────────────────────────────────────────────────

def write_monitor_results(sheets, results: list[dict]) -> None:
    """
    Append one row per campaign to the 'Monitor' tab.
    Columns: date, stg_id, campaign_id, flow_id, pass_rate, cohort_avg, verdict
    """
    try:
        ws = sheets._triggers.worksheet("Monitor")
    except Exception:
        # Create the tab if it doesn't exist
        ws = sheets._triggers.add_worksheet(title="Monitor", rows=1000, cols=10)
        ws.append_row(
            ["Date", "STG ID", "Campaign ID", "Flow ID", "Pass Rate %", "Cohort Avg %", "Verdict"],
            value_input_option="RAW",
        )

    date_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    for r in results:
        ws.append_row([
            date_str,
            r.get("stg_id", ""),
            r.get("campaign_id", ""),
            r.get("flow_id", ""),
            r.get("pass_rate", ""),
            r.get("cohort_avg", ""),
            r.get("verdict", ""),
        ], value_input_option="RAW")

    log.info("Wrote %d monitor rows to Monitor tab", len(results))


def read_monitor_summary(sheets, max_rows: int = 20) -> str:
    """
    Read the most recent rows from the 'Monitor' tab and return a
    Slack-ready summary string.

    Returns an empty string if the Monitor tab does not exist or has no data
    rows (i.e. monitor has never been run).

    Args:
        sheets:   SheetsClient instance (already authenticated).
        max_rows: Maximum number of recent rows to include in the summary.
                  Defaults to 20 (covers up to 20 campaigns per monitor run).

    Returns:
        Formatted text block, e.g.:
            Campaign Monitor (2026-04-21):
              KEEP  : skills__diagnosis__healthcare (progress 34.2%, avg 28.1%)
              PAUSE : skills__engineering__electrical (progress 18.5%, avg 28.1%)
            1 campaign flagged for review.
        Or empty string if no data is available.
    """
    try:
        ws = sheets._triggers.worksheet("Monitor")
    except Exception:
        log.info("Monitor tab not found — skipping monitor summary")
        return ""

    rows = ws.get_all_values()
    if len(rows) < 2:
        log.info("Monitor tab has no data rows")
        return ""

    # Header: Date, STG ID, Campaign ID, Flow ID, Pass Rate %, Cohort Avg %, Verdict
    data_rows = rows[1:]  # skip header
    # Take the most recent max_rows
    recent = data_rows[-max_rows:]

    # Group by date — use the date of the last row as the report date
    report_date = recent[-1][0] if recent else "unknown"

    lines = [f"*Campaign Monitor ({report_date}):*"]
    pause_count = 0
    for row in recent:
        if len(row) < 7:
            continue
        _date, stg_id, campaign_id, flow_id, pass_rate, cohort_avg, verdict = row[:7]
        label = stg_id or campaign_id or flow_id or "unknown"
        lines.append(
            f"  {verdict:<10}: {label} "
            f"(progress {pass_rate}%, avg {cohort_avg}%)"
        )
        if verdict.upper() == "PAUSE":
            pause_count += 1

    if pause_count:
        lines.append(f"{pause_count} campaign(s) flagged for review — check Monitor tab.")
    else:
        lines.append("All monitored campaigns within progress thresholds.")

    return "\n".join(lines)


# ── Parse active campaigns from sheet ─────────────────────────────────────────

def read_active_campaigns(sheets) -> list[dict]:
    """
    Read all rows in Triggers 2 where column L starts with "Created:".
    Returns [{stg_id, campaign_id, flow_id, launch_date, stg_name, figma_file, figma_node}, ...]
    """
    from src.sheets import COL
    ws       = sheets._triggers.worksheet("Triggers 2")
    all_rows = ws.get_all_values()

    active = []
    for idx, row in enumerate(all_rows[1:], start=2):
        while len(row) < max(COL.values()) + 1:
            row.append("")
        li_status = row[COL["li_status"]].strip()
        if li_status.startswith("Created:"):
            campaign_id = li_status.split(":", 1)[1].strip()
            active.append({
                "sheet_row":  idx,
                "stg_id":     row[COL["stg_id"]].strip(),
                "stg_name":   row[COL["stg_name"]].strip(),
                "flow_id":    row[COL["flow_id"]].strip(),
                "launch_date": row[COL["date"]].strip(),
                "figma_file": row[COL["figma_file"]].strip(),
                "figma_node": row[COL["figma_node"]].strip(),
                "campaign_id": campaign_id,
            })

    log.info("Found %d active campaigns in sheet", len(active))
    return active

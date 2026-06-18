"""
Angle-performance "double-down" loop (2026-06-18).

Scouts launched campaigns, decides which ANGLE (A/B/C/…) wins per cohort×geo, and
drives three actions:
  1. refresh the winner — generate a fresh creative variant in the SAME winning hook,
  2. scale the winner's budget (+ pause clear losers),
  3. pause + replace losers in the winning direction.

Analysis + the angle recommendations + the Slack change summary are ALWAYS produced
(safe — no live mutation). The live-money / draft EXECUTION runs only when
`config.ANGLE_AUTO_ACT_ENABLED` is true; otherwise the changes are surfaced as
recommendations in the console "Live performance & recommendations" section
(`ramp_recommendations`) and a Slack summary, awaiting human Accept. This honors
the draft-default contract.

Data source is the Campaign Registry (`campaign_registry.get_active_campaigns`) —
the same authoritative, daily-refreshed source `feedback_agent.recommend_actions`
uses — so the angle verdicts line up with the per-campaign recommendations.

Persistence avoids any schema migration: angle intent rides in the existing
`ramp_recommendations.metric_signal` JSONB (`kind`/`winning_angle`/`losing_angle`/
`scale_action`/`target_budget_cents`) the console already reads.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from typing import Any, Optional

import config
from src.feedback_agent import (
    _coerce_float,
    REC_CTR_PCT_FLOOR,
    REC_CPA_CEILING_USD,
    REC_MIN_SPEND_FOR_JUDGEMENT_USD,
)

log = logging.getLogger(__name__)


@dataclass
class AngleStat:
    angle: str
    campaign_urn: str
    platform: str
    impressions: float
    clicks: float
    ctr_pct: float
    spend_usd: float
    cpa_usd: Optional[float]
    qualified: bool          # cleared the volume + spend floor
    row: dict = field(default_factory=dict)  # representative registry row (for copy/budget reuse)


@dataclass
class CohortAngleVerdict:
    ramp_id: str
    cohort_signature: str
    geo_cluster: str
    platform: str
    verdict: str                       # "decided" | "insufficient_data"
    winning_angle: Optional[str]
    losing_angles: list[str]
    angle_stats: list[AngleStat]
    winner: Optional[AngleStat] = None


# ── statistics helpers (no numpy dep — small lists) ─────────────────────────────
def _median(xs: list[float]) -> float:
    s = sorted(xs)
    n = len(s)
    if n == 0:
        return 0.0
    mid = n // 2
    return s[mid] if n % 2 else (s[mid - 1] + s[mid]) / 2.0


def _stdev(xs: list[float]) -> float:
    n = len(xs)
    if n < 2:
        return 0.0
    mean = sum(xs) / n
    var = sum((x - mean) ** 2 for x in xs) / (n - 1)  # sample stdev (ddof=1)
    return math.sqrt(var)


def _agg_angle(angle: str, rows: list[dict]) -> AngleStat:
    """Aggregate one angle's registry rows (usually 1) into an AngleStat."""
    imp = sum(_coerce_float(r.get("impressions")) or 0.0 for r in rows)
    clk = sum(_coerce_float(r.get("clicks")) or 0.0 for r in rows)
    spend = sum(_coerce_float(r.get("spend_usd")) or 0.0 for r in rows)
    apps = sum(_coerce_float(r.get("applications")) or 0.0 for r in rows)
    # CTR: prefer computed clicks/impressions; fall back to the stored ctr_pct.
    if imp > 0:
        ctr = clk / imp * 100.0
    else:
        ctr = _coerce_float(rows[0].get("ctr_pct")) or 0.0
    # CPA: spend / applications when we have conversions; else the stored cpa_usd.
    if apps > 0:
        cpa: Optional[float] = spend / apps
    else:
        cpa = _coerce_float(rows[0].get("cpa_usd"))
    urn = rows[0].get("platform_campaign_id") or rows[0].get("linkedin_campaign_urn") or ""
    platform = rows[0].get("platform") or rows[0].get("channel") or ""
    qualified = imp >= config.ANGLE_MIN_IMPRESSIONS and spend >= REC_MIN_SPEND_FOR_JUDGEMENT_USD
    return AngleStat(
        angle=angle, campaign_urn=urn, platform=platform,
        impressions=imp, clicks=clk, ctr_pct=ctr, spend_usd=spend,
        cpa_usd=cpa, qualified=qualified, row=rows[0],
    )


def analyze_angles(ramp_id: str, *, rows: Optional[list[dict]] = None) -> list[CohortAngleVerdict]:
    """Aggregate launched campaigns by (cohort × geo) and rank the angles within
    each group. Returns one CohortAngleVerdict per group.

    `rows` is injectable for tests; otherwise loaded from the Campaign Registry.
    Thin groups (<2 angles clearing the volume/spend floor) → verdict
    "insufficient_data" so we never declare a winner on noise.
    """
    if rows is None:
        from src import campaign_registry
        rows = campaign_registry.get_active_campaigns(smart_ramp_id=ramp_id)

    # group rows by (cohort_signature, geo_cluster) → {angle: [rows]}
    groups: dict[tuple[str, str], dict[str, list[dict]]] = {}
    for row in rows or []:
        cohort = (row.get("cohort_signature") or row.get("cohort_id") or "").strip()
        geo = (row.get("geo_cluster") or row.get("geo_cluster_label") or "global").strip() or "global"
        angle = (row.get("angle") or "").strip()
        urn = row.get("platform_campaign_id") or row.get("linkedin_campaign_urn") or ""
        # only consider live campaigns with an angle + id
        status = (row.get("status") or "").strip().lower()
        if not (cohort and angle and urn) or status in ("deprecated", "archived"):
            continue
        groups.setdefault((cohort, geo), {}).setdefault(angle, []).append(row)

    verdicts: list[CohortAngleVerdict] = []
    for (cohort, geo), by_angle in groups.items():
        stats = [_agg_angle(a, arows) for a, arows in by_angle.items()]
        platform = next((s.platform for s in stats if s.platform), "")
        qualified = [s for s in stats if s.qualified]

        if len(qualified) < 2:
            verdicts.append(CohortAngleVerdict(
                ramp_id=ramp_id, cohort_signature=cohort, geo_cluster=geo, platform=platform,
                verdict="insufficient_data", winning_angle=None, losing_angles=[],
                angle_stats=stats, winner=None,
            ))
            continue

        winner, losers = _pick_winner_losers(qualified)
        verdict = "decided" if (winner or losers) else "insufficient_data"
        verdicts.append(CohortAngleVerdict(
            ramp_id=ramp_id, cohort_signature=cohort, geo_cluster=geo, platform=platform,
            verdict=verdict,
            winning_angle=winner.angle if winner else None,
            losing_angles=[s.angle for s in losers],
            angle_stats=stats, winner=winner,
        ))
    return verdicts


def _pick_winner_losers(qualified: list[AngleStat]) -> tuple[Optional[AngleStat], list[AngleStat]]:
    """Choose a winner + losers among qualified angles. Primary metric is CPA when
    every qualified angle has one (lower is better), else CTR (higher is better) —
    mirrors feedback_agent._classify_campaign_row precedence."""
    have_cpa = [s for s in qualified if s.cpa_usd is not None]
    ctrs = [s.ctr_pct for s in qualified]
    ctr_med, ctr_sd = _median(ctrs), _stdev(ctrs)

    def ctr_z(s: AngleStat) -> float:
        return (s.ctr_pct - ctr_med) / ctr_sd if ctr_sd > 0 else 0.0

    winner: Optional[AngleStat] = None
    if len(have_cpa) == len(qualified):
        # CPA-primary: lowest CPA, must beat the median by the margin.
        cpa_med = _median([s.cpa_usd for s in qualified])  # type: ignore[arg-type]
        best = min(qualified, key=lambda s: s.cpa_usd)      # type: ignore[arg-type,return-value]
        if cpa_med > 0 and best.cpa_usd is not None and best.cpa_usd <= cpa_med * (1 - config.ANGLE_WINNER_MARGIN_PCT):
            winner = best
    if winner is None:
        # CTR-primary: highest CTR. Require BOTH a winner z-score AND a meaningful
        # relative margin over the median, so we don't crown a winner on a 0.05pp
        # gap just because the group's variance is tiny.
        best = max(qualified, key=lambda s: s.ctr_pct)
        beats_margin = ctr_med > 0 and best.ctr_pct >= ctr_med * (1 + config.ANGLE_WINNER_MARGIN_PCT)
        if ctr_z(best) >= config.ANGLE_WINNER_Z and beats_margin:
            winner = best

    losers: list[AngleStat] = []
    for s in qualified:
        if winner is not None and s is winner:
            continue
        # z-based loser must ALSO be a meaningful relative margin below median
        # (same anti-noise guard as the winner side); the absolute floor / CPA
        # ceiling are decisive on their own.
        margin_below = ctr_med > 0 and s.ctr_pct <= ctr_med * (1 - config.ANGLE_WINNER_MARGIN_PCT)
        is_loser = (
            (ctr_z(s) <= -config.ANGLE_LOSER_Z and margin_below)
            or (s.ctr_pct < REC_CTR_PCT_FLOOR and s.clicks > 0)
            or (s.cpa_usd is not None and s.cpa_usd > REC_CPA_CEILING_USD)
        )
        if is_loser:
            losers.append(s)
    return winner, losers


# ── Plan: turn verdicts into concrete changes ─────────────────────────────────
@dataclass
class PlannedChange:
    kind: str                 # "scale" | "pause" | "refresh"
    cohort_signature: str
    geo_cluster: str
    platform: str
    angle: str                # winner angle for scale/refresh; loser angle for pause
    campaign_urn: str         # winner urn (scale/refresh) or loser urn (pause)
    winning_angle: Optional[str]
    losing_angle: Optional[str]
    target_budget_cents: Optional[int] = None
    metric_signal: dict = field(default_factory=dict)
    rationale: str = ""


def plan_changes(verdicts: list[CohortAngleVerdict]) -> list[PlannedChange]:
    """Translate decided verdicts into concrete changes: scale the winner,
    refresh the winner (fresh same-hook variant), pause each loser."""
    changes: list[PlannedChange] = []
    for v in verdicts:
        if v.verdict != "decided":
            continue
        w = v.winner
        if w is not None:
            sig = {
                "kind": "angle_scale", "winning_angle": w.angle,
                "ctr_pct": round(w.ctr_pct, 3), "cpa_usd": None if w.cpa_usd is None else round(w.cpa_usd, 2),
                "impressions": int(w.impressions), "spend_usd": round(w.spend_usd, 2),
            }
            cur_cents = _coerce_float((w.row or {}).get("daily_budget_cents"))
            target = int(cur_cents * config.ANGLE_SCALE_FACTOR) if cur_cents else None
            if target:
                target = min(target, config.ANGLE_SCALE_MAX_CENTS)
            changes.append(PlannedChange(
                kind="scale", cohort_signature=v.cohort_signature, geo_cluster=v.geo_cluster,
                platform=v.platform, angle=w.angle, campaign_urn=w.campaign_urn,
                winning_angle=w.angle, losing_angle=None, target_budget_cents=target,
                metric_signal={**sig, "scale_action": "scale_up", "target_budget_cents": target},
                rationale=(f"Angle {w.angle} is the winner for this cohort "
                           f"(CTR {w.ctr_pct:.2f}%). Scale its budget and double down."),
            ))
            changes.append(PlannedChange(
                kind="refresh", cohort_signature=v.cohort_signature, geo_cluster=v.geo_cluster,
                platform=v.platform, angle=w.angle, campaign_urn=w.campaign_urn,
                winning_angle=w.angle, losing_angle=None,
                metric_signal={"kind": "angle_refresh", "winning_angle": w.angle},
                rationale=(f"Angle {w.angle} is winning. Prepare a fresh creative variant "
                           f"in the same winning direction."),
            ))
        for loser in v.losing_angles:
            lstat = next((s for s in v.angle_stats if s.angle == loser), None)
            if lstat is None:
                continue
            changes.append(PlannedChange(
                kind="pause", cohort_signature=v.cohort_signature, geo_cluster=v.geo_cluster,
                platform=v.platform, angle=loser, campaign_urn=lstat.campaign_urn,
                winning_angle=v.winning_angle, losing_angle=loser,
                metric_signal={
                    "kind": "angle_pause", "losing_angle": loser, "winning_angle": v.winning_angle,
                    "ctr_pct": round(lstat.ctr_pct, 3),
                    "cpa_usd": None if lstat.cpa_usd is None else round(lstat.cpa_usd, 2),
                    "impressions": int(lstat.impressions), "spend_usd": round(lstat.spend_usd, 2),
                },
                rationale=(f"Angle {loser} is underperforming vs winner "
                           f"{v.winning_angle or '?'} (CTR {lstat.ctr_pct:.2f}%). "
                           f"Pause it and replace in the winning direction."),
            ))
    return changes


def _persist_change(ramp_id: str, c: PlannedChange) -> None:
    """Write/overwrite the angle recommendation for this change into
    ramp_recommendations so it shows in the console 'Live performance &
    recommendations' section. No schema migration — angle intent rides in
    metric_signal; action/classification stay within the existing enums."""
    from src import ui_decisions
    if c.kind == "scale":
        classification, action = "working", "keep"
    elif c.kind == "refresh":
        classification, action = "working", "keep"
    else:  # pause
        classification, action = "underperforming", "replace"
    try:
        ui_decisions.upsert_recommendation(
            ramp_id=ramp_id, campaign_urn=c.campaign_urn,
            cohort_signature=c.cohort_signature, channel=c.platform, angle=c.angle,
            classification=classification, action=action,
            rationale=c.rationale, metric_signal=c.metric_signal,
        )
    except Exception as exc:  # pragma: no cover - defensive (Postgres down)
        log.warning("angle rec upsert failed (%s/%s): %s", ramp_id, c.campaign_urn, exc)


def build_angle_change_message(
    ramp_id: str, changes: list[PlannedChange], *, project_name: str = "", applied: bool = False,
) -> str:
    """Vocabulary-clean plain-text Slack summary of the angle changes — what gets
    paused, what gets started (fresh winning-angle variant), what gets scaled.
    Returns "" when there are no changes (caller skips the post)."""
    if not changes:
        return ""
    verb = "Applied" if applied else "Prepared (pending your approval in the console)"
    header = f"*Angle double-down: {ramp_id}*"
    if project_name:
        header += f" — {project_name}"
    lines = [header, verb, ""]

    pauses = [c for c in changes if c.kind == "pause"]
    starts = [c for c in changes if c.kind == "refresh"]
    scales = [c for c in changes if c.kind == "scale"]

    if starts:
        lines.append("*Started (fresh winning-angle variants):*")
        for c in starts:
            lines.append(f"  • {c.cohort_signature} · {c.geo_cluster} · angle {c.winning_angle} ({c.platform})")
        lines.append("")
    if scales:
        lines.append("*Scaling (winning angle):*")
        for c in scales:
            tgt = f" → ${c.target_budget_cents/100:.0f}/day" if c.target_budget_cents else ""
            lines.append(f"  • {c.cohort_signature} · {c.geo_cluster} · angle {c.winning_angle}{tgt} ({c.platform})")
        lines.append("")
    if pauses:
        lines.append("*Pausing (underperforming angles):*")
        for c in pauses:
            lines.append(f"  • {c.cohort_signature} · {c.geo_cluster} · angle {c.losing_angle} ({c.platform})")
        lines.append("")
    return "\n".join(lines).rstrip() + "\n"


def act_on_verdicts(
    verdicts: list[CohortAngleVerdict],
    *,
    ramp_id: str,
    auto_act: bool = False,
    project_name: str = "",
    post_slack: bool = True,
) -> list[PlannedChange]:
    """Turn verdicts into changes, persist them as console recommendations, post a
    Slack change summary, and (only when `auto_act`) execute the live budget/pause
    + draft-refresh actions via the existing machinery. Returns the planned changes.

    Default (auto_act=False): recommend + notify only — no live mutation.
    """
    changes = plan_changes(verdicts)
    if not changes:
        log.info("act_on_verdicts(%s): no decided angle changes", ramp_id)
        return []

    for c in changes:
        _persist_change(ramp_id, c)

    if auto_act:
        _execute_changes(ramp_id, changes)

    if post_slack:
        text = build_angle_change_message(ramp_id, changes, project_name=project_name, applied=auto_act)
        if text:
            _post_slack(ramp_id, text)

    log.info(
        "act_on_verdicts(%s): %d change(s) — %d start, %d scale, %d pause (auto_act=%s)",
        ramp_id, len(changes),
        sum(1 for c in changes if c.kind == "refresh"),
        sum(1 for c in changes if c.kind == "scale"),
        sum(1 for c in changes if c.kind == "pause"),
        auto_act,
    )
    return changes


def _post_slack(ramp_id: str, text: str) -> None:
    """Post the change summary to the automation-bot channel + Pranav DM via the
    existing tokenless Drive-queue path. Best-effort; never raises."""
    try:
        from src import smart_ramp_notifier as N
        thread_ts = None
        try:
            from src import ui_decisions
            thread_ts = ui_decisions.get_slack_thread_ts(ramp_id)
        except Exception:
            pass
        # targets=None → _send_to_all_targets defaults to SLACK_RAMP_NOTIFY_TARGETS
        # (automation-bot channel + Pranav DM + Diego DM).
        N._send_to_all_targets(text, ramp_id=ramp_id, targets=None, thread_ts=thread_ts)
    except Exception as exc:  # pragma: no cover - defensive
        log.warning("angle Slack summary post failed (%s): %s", ramp_id, exc)


def _execute_changes(ramp_id: str, changes: list[PlannedChange]) -> None:
    """Execute the live actions (gated behind ANGLE_AUTO_ACT_ENABLED). Each action
    reuses existing machinery and is best-effort/per-change isolated so one failure
    never aborts the rest. Drafts (refresh) are always safe; budget/pause are live.

    Meta changes route to _execute_meta_change (the angle is an ad-level split, so
    'pause loser' = pause the losing-angle ads, 'scale winner' = an increase-only
    campaign-budget bump). LinkedIn keeps its existing path. The LinkedIn client is
    built lazily so a Meta-only ramp doesn't abort when LinkedIn auth is missing."""
    from src import campaign_feedback_agent as cfa
    _li_box: list = []

    def _li():
        if not _li_box:
            import os
            from src.linkedin_api import LinkedInClient
            _li_box.append(LinkedInClient(config.LINKEDIN_TOKEN or os.getenv("LINKEDIN_TOKEN", "")))
        return _li_box[0]

    for c in changes:
        try:
            platform = (c.platform or "").lower()
            if platform == "meta":
                _execute_meta_change(c)
            elif c.kind == "scale" and c.target_budget_cents:
                _execute_scale(c, _li())
            elif c.kind == "refresh" and hasattr(cfa, "create_winner_variant_draft"):
                cfa.create_winner_variant_draft(ramp_id, c, _li())
            elif c.kind == "pause" and hasattr(cfa, "pause_and_replace_angle"):
                cfa.pause_and_replace_angle(ramp_id, c, _li())
            else:
                log.info(
                    "act_on_verdicts: %s execution not yet wired (%s) — recommendation surfaced for Accept",
                    c.kind, c.campaign_urn,
                )
        except Exception as exc:  # pragma: no cover - live path, per-change isolation
            log.warning("act_on_verdicts execute %s failed (%s): %s", c.kind, c.campaign_urn, exc)


def _execute_meta_change(c: PlannedChange) -> None:  # pragma: no cover - live path
    """Execute a Meta angle change. On Meta the three angles are ADS inside one
    per-language campaign, so:

      - pause (loser): pause the losing-angle ad(s) within the campaign. Meta then
        reallocates that ad set's delivery to the surviving (winner) ads — this IS
        the per-angle budget rebalance, and it's the safe, high-confidence lever.
      - scale (winner): bump the campaign daily budget, but INCREASE-ONLY and only
        when the campaign actually carries a campaign-level budget (CBO). The
        shared ANGLE_SCALE_MAX_CENTS cap is LinkedIn-calibrated and far below Meta's
        budgets, so a naive set would slash a big spender — we never reduce, and we
        skip no-CBO campaigns (budget lives on the ad set; which ad set is
        ambiguous) rather than guess.
    """
    import config
    if not config.META_ACCESS_TOKEN:
        log.warning("meta angle exec: META_ACCESS_TOKEN unset — skipping %s", c.campaign_urn)
        return
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.campaign import Campaign
    from facebook_business.adobjects.ad import Ad
    FacebookAdsApi.init(
        access_token=config.META_ACCESS_TOKEN,
        api_version=config.META_API_VERSION or "v21.0",
    )
    cid = c.campaign_urn

    if c.kind == "pause":
        from src.live_angle_stats import _angle_of
        ads = Campaign(cid).get_ads(fields=["name", "status"])
        paused = 0
        for ad in ads:
            if _angle_of(ad.get("name")) == c.angle and (ad.get("status") or "").upper() == "ACTIVE":
                Ad(ad["id"]).api_update(params={"status": "PAUSED"})
                paused += 1
        log.info("meta pause: campaign %s angle %s → paused %d ad(s)", cid, c.angle, paused)

    elif c.kind == "scale":
        target = c.target_budget_cents
        if not target:
            log.info("meta scale: campaign %s has no campaign-level budget (no-CBO) — skip (recommend-only)", cid)
            return
        try:
            cur = Campaign(cid).api_get(fields=["daily_budget"]).get("daily_budget")
            cur_cents = int(cur) if cur not in (None, "") else None
        except Exception:
            cur_cents = None
        if cur_cents is None:
            log.info("meta scale: campaign %s budget not at campaign level — skip", cid)
            return
        if target <= cur_cents:
            log.info("meta scale: campaign %s target %d <= current %d — skip (never reduce)", cid, target, cur_cents)
            return
        Campaign(cid).api_update(params={"daily_budget": int(target)})
        log.info("meta scale: campaign %s daily_budget %d → %d cents", cid, cur_cents, target)

    else:
        log.info("meta angle exec: %s not wired for Meta (recommend-only)", c.kind)


def _execute_scale(c: PlannedChange, li_client) -> None:  # pragma: no cover - live path
    """Bump a winning campaign's daily budget (LinkedIn only for v1; Meta/Google
    routed via their clients in update_budget when wired)."""
    if (c.platform or "").lower() == "linkedin" and c.target_budget_cents:
        li_client.update_campaign_budget(c.campaign_urn, daily_budget_cents=c.target_budget_cents)
        log.info("scaled %s → %d cents/day", c.campaign_urn, c.target_budget_cents)

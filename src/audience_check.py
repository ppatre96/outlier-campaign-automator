"""
Platform-agnostic audience-size de-narrowing.

LinkedIn's Stage C already runs this pattern (src/stage_c.py): query the
platform's reach API for a cohort × geo, and if it comes in below
config.AUDIENCE_SIZE_MIN, iteratively drop the lowest-importance targeting
rule and re-query until the floor clears or we run out of drops.

This module extracts that loop so the Meta + Google arms reuse it without
duplicating the loop body. Each platform supplies:

  - get_reach_fn(targeting) → int | None
      Returns the platform-reported audience estimate, or None if the call
      failed (network, auth, unsupported account). None signals "skip the
      gate" — pipeline proceeds without an estimate rather than blocking.

  - drop_rule_fn(targeting) → dict | None
      Returns a relaxed copy of `targeting` with one rule dropped (the
      lowest-importance, per the platform's own ordering). Returns None when
      there's nothing left to drop.

Same 50k floor as LinkedIn — single config knob (`config.AUDIENCE_SIZE_MIN`),
no per-channel fork. When de-narrowing exhausts and still misses the floor,
the caller decides whether to skip the cohort for that channel only or ship
anyway. Caller logs the rejection reason.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

import config

log = logging.getLogger(__name__)

DENARROW_MAX_DROPS = 5  # matches stage_c._DENARROW_MAX_DROPS


# ── Per-platform drop rules ─────────────────────────────────────────────────
# Each `drop_rule_for_*` function takes a targeting dict and returns a relaxed
# copy with one rule removed (the lowest-importance one for that platform),
# or None when nothing remains to drop. Pure functions — mutate nothing.


def drop_rule_for_meta(targeting: dict) -> dict | None:
    """Drop the lowest-importance Meta targeting rule.

    Order (least → most disruptive):
      1. Drop one interest at a time from `flexible_spec[0].interests`
      2. Drop the entire `flexible_spec[0]` block once its interests empty out
      3. Drop the age_min/age_max constraint as the last step

    Geo (`geo_locations`) is intentionally never dropped — Meta requires at
    least one location and we trust the cohort's geo selection as ground truth.
    """
    flex = list(targeting.get("flexible_spec") or [])
    if flex:
        head = dict(flex[0] or {})
        interests = list(head.get("interests") or [])
        if interests:
            interests.pop()
            head["interests"] = interests
            # Empty interests block → remove the entire flex entry
            new_flex = ([head] if interests else []) + flex[1:]
            return {**targeting, "flexible_spec": new_flex}
    # No interests left to drop — try removing age constraint
    if targeting.get("age_min") and targeting.get("age_max"):
        new_t = dict(targeting)
        new_t.pop("age_min", None)
        new_t.pop("age_max", None)
        return new_t
    return None


def drop_rule_for_google(targeting: dict) -> dict | None:
    """Drop the lowest-importance Google targeting rule.

    Order (least → most disruptive):
      1. Drop one audience_segment at a time (Display layering — least-priority
         segments are appended last by GoogleSegmentResolver, so .pop() targets
         the right one)
      2. Drop the LAST geo_target if multiple are set (broaden within a
         multi-country cohort by dropping the smallest market). Single-geo
         cohorts skip this step — broadening 1 country to global is too
         aggressive given Outlier's compliance constraints.

    Does NOT touch keyword_ideas — for Search, the reach signal is
    `keyword_volume_estimate` (sum of monthly searches), and removing a keyword
    REDUCES that sum. Opposite of de-narrowing intent. If keyword volume is
    too low even after dropping segments + geos, the cohort genuinely lacks
    intent signal for Google's auction.

    Does NOT touch `demographics` — GoogleSegmentResolver returns an empty
    dict for that field today; if/when demographic constraints get added the
    drop order should put them between segments and geos.

    Caller (denarrow_for_platform in this module) compares the resulting
    audience count against config.AUDIENCE_SIZE_MIN — same 50k floor as
    LinkedIn / Meta. Note: for Search arm, the metric is keyword_volume_estimate
    (monthly-search units), not user-count units, so the 50k floor reads
    differently — directionally correct (low keyword volume ≈ low Search reach)
    but not a literal user count. Document this limitation when stakeholders
    ask why some Search campaigns get gated below floor.
    """
    segs = list(targeting.get("audience_segments") or [])
    if segs:
        segs.pop()  # last-added = least-priority
        return {**targeting, "audience_segments": segs}
    geos = list(targeting.get("geo_targets") or [])
    if len(geos) > 1:
        geos.pop()  # drop the last geo_target_constant (smallest market)
        return {**targeting, "geo_targets": geos}
    return None


def denarrow_for_platform(
    *,
    platform: str,
    targeting: dict,
    get_reach_fn: Callable[[dict], Optional[int]],
    drop_rule_fn: Callable[[dict], Optional[dict]],
    cohort_label: str = "(unnamed)",
    max_drops: int = DENARROW_MAX_DROPS,
) -> tuple[Optional[int], dict, str]:
    """
    Run the de-narrowing loop for a single platform.

    Returns (final_count, final_targeting, status) where status is:
      - "pass": audience >= floor on first try, no drops needed
      - "denarrowed": audience >= floor after dropping N rules (N>0)
      - "below_floor": audience < floor even after exhausting drops
      - "skipped": reach API returned None (unsupported / failure); caller
                   should ship without gating

    Mutates nothing — `targeting` is treated as immutable. The returned
    `final_targeting` is the dict the platform should actually use to create
    the campaign.
    """
    threshold = config.AUDIENCE_SIZE_MIN

    initial = get_reach_fn(targeting)
    if initial is None:
        log.info(
            "audience_check[%s][%s]: reach API returned None — skipping gate",
            platform, cohort_label,
        )
        return None, targeting, "skipped"

    if initial >= threshold:
        log.info(
            "audience_check[%s][%s]: %d ≥ %d (pass on first try)",
            platform, cohort_label, initial, threshold,
        )
        return initial, targeting, "pass"

    log.info(
        "audience_check[%s][%s]: %d < %d — attempting de-narrowing (max %d drops)",
        platform, cohort_label, initial, threshold, max_drops,
    )

    current = dict(targeting)  # shallow copy — drop_rule_fn returns fresh dicts anyway
    last_count = initial
    for drop_n in range(1, max_drops + 1):
        relaxed = drop_rule_fn(current)
        if relaxed is None:
            log.info(
                "audience_check[%s][%s]: nothing left to drop after %d drops — stopping",
                platform, cohort_label, drop_n - 1,
            )
            break
        try:
            new_count = get_reach_fn(relaxed)
        except Exception as exc:
            log.warning(
                "audience_check[%s][%s]: reach call failed during de-narrow #%d: %s",
                platform, cohort_label, drop_n, exc,
            )
            break
        if new_count is None:
            log.warning(
                "audience_check[%s][%s]: reach returned None during de-narrow #%d — stopping",
                platform, cohort_label, drop_n,
            )
            break
        log.info(
            "audience_check[%s][%s]: de-narrow #%d → %d (was %d, threshold %d)",
            platform, cohort_label, drop_n, new_count, last_count, threshold,
        )
        current = relaxed
        last_count = new_count
        if new_count >= threshold:
            return new_count, current, "denarrowed"

    if last_count < threshold:
        log.info(
            "audience_check[%s][%s]: below floor (%d < %d) after de-narrowing — caller decides",
            platform, cohort_label, last_count, threshold,
        )
        return last_count, current, "below_floor"
    return last_count, current, "denarrowed"

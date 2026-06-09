"""Meta conversion-tracking audit — second deterministic per-ramp check.

The bug this guards (Tuan, GMR-0023): Meta ad sets optimized on a custom
conversion (986478843749388) that was archived in Meta → archived custom
conversions track NOTHING → 14 campaigns logged 0 conversions despite real
traffic. The fix is to optimize on the pixel event directly. This check reads
each Meta ad set's LIVE promoted_object and verifies it is the correct
pixel-event form; when autofix is on it PATCHES any drift back to correct
(`MetaClient.repair_promoted_object`) BEFORE a human un-pauses the draft, so the
campaign tracks from the first impression.

Returns the same shape every per-ramp check returns:
{name, checked, violations[], handled[container ids], detail[]}. Best-effort.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

import config

log = logging.getLogger(__name__)


def _is_correct(po: dict) -> bool:
    """True iff promoted_object optimizes on the configured pixel event."""
    return (
        str(po.get("pixel_id") or "") == str(config.META_PIXEL_ID)
        and (po.get("custom_event_type") or "").upper() == "OTHER"
        and (po.get("custom_event_str") or "") == config.META_CUSTOM_EVENT_STR
    )


def audit_meta_tracking(
    rows: list[dict],
    *,
    autofix: Optional[bool] = None,
    exclude_containers: Optional[set] = None,
    reader: Optional[Callable[[str], dict]] = None,
    fixer: Optional[Callable[[str], bool]] = None,
) -> dict:
    autofix = config.RAMP_AUDIT_AUTOFIX if autofix is None else autofix
    exclude_containers = exclude_containers or set()

    _client = {"c": None}

    def _get_client():
        if _client["c"] is None:
            from src.meta_api import MetaClient
            _client["c"] = MetaClient()
        return _client["c"]

    reader = reader or (lambda aid: _get_client().get_promoted_object(aid))
    fixer = fixer or (lambda aid: _get_client().repair_promoted_object(aid))

    seen: set[str] = set()
    checked = 0
    violations: list[dict] = []
    for row in rows:
        if (row.get("platform") or "").strip().lower() != "meta":
            continue
        adset = row.get("platform_campaign_id") or ""
        if not adset or adset in seen:
            continue
        seen.add(adset)
        try:
            po = reader(adset)
        except Exception as exc:
            log.warning("meta_tracking_audit: could not read promoted_object for %s (%s)", adset, exc)
            continue
        checked += 1
        if not _is_correct(po):
            violations.append({
                "platform":      "meta",
                "container_id":  adset,
                "ramp_id":       row.get("smart_ramp_id") or row.get("ramp_id") or "",
                "campaign_name": row.get("campaign_name") or "",
                "cohort_geo":    row.get("cohort_geo") or "",
                "promoted_object": po,
                "expected":      f"pixel {config.META_PIXEL_ID} event '{config.META_CUSTOM_EVENT_STR}' (custom_event_type=OTHER)",
            })

    if violations:
        log.warning(
            "meta_tracking_audit: %d/%d Meta ad set(s) not optimizing on the pixel event — %s",
            len(violations), checked, [v["container_id"] for v in violations],
        )

    handled: list[str] = []
    detail: list[dict] = []
    if autofix:
        for v in violations:
            if v["container_id"] in exclude_containers:
                continue
            ok = False
            try:
                ok = fixer(v["container_id"])
            except Exception as exc:
                log.error("meta_tracking_audit: repair failed for %s: %s", v["container_id"], str(exc)[:200])
            try:
                from src.ui_decisions import log_event
                log_event(v["ramp_id"] or "", "meta_tracking_repaired",
                          {k: v[k] for k in ("platform", "container_id", "campaign_name", "cohort_geo")}
                          | {"was": v["promoted_object"], "repaired": ok})
            except Exception as exc:
                log.debug("meta_tracking_audit: log_event skipped: %s", exc)
            detail.append({**v, "repaired": ok})
            if ok:
                handled.append(v["container_id"])

    return {
        "name":       "meta_tracking",
        "checked":    checked,
        "violations": violations,
        "handled":    handled,
        "detail":     detail,
    }

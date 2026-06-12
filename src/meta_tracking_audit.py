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
    auto_rebuild: Optional[bool] = None,
    exclude_containers: Optional[set] = None,
    reader: Optional[Callable[[str], dict]] = None,
    fixer: Optional[Callable[[str], bool]] = None,
    rebuilder: Optional[Callable[[str], str]] = None,
    old_pauser: Optional[Callable[[str], bool]] = None,
) -> dict:
    autofix = config.RAMP_AUDIT_AUTOFIX if autofix is None else autofix
    auto_rebuild = config.META_TRACKING_AUTO_REBUILD if auto_rebuild is None else auto_rebuild
    exclude_containers = exclude_containers or set()

    _client = {"c": None}

    def _get_client():
        if _client["c"] is None:
            from src.meta_api import MetaClient
            _client["c"] = MetaClient()
        return _client["c"]

    reader = reader or (lambda aid: _get_client().get_promoted_object(aid))
    fixer = fixer or (lambda aid: _get_client().repair_promoted_object(aid))
    rebuilder = rebuilder or (lambda aid: _get_client().rebuild_adset_with_correct_tracking(aid))
    if old_pauser is None:
        from src.launch_verify import _archive_meta_adset
        old_pauser = _archive_meta_adset

    seen: set[str] = set()
    checked = 0
    violations: list[dict] = []
    for row in rows:
        if (row.get("platform") or "").strip().lower() != "meta":
            continue
        # Skip the parent campaign-group row. Its platform_campaign_id is the
        # Meta CAMPAIGN id, which has no promoted_object — that lives on the ad
        # set. Only the ad-set rows (campaign_type "static"/"inmail") carry the
        # ad_set_id we can read tracking from. Without this guard every parent
        # row reads {} → flagged as a violation + a futile repair attempt on a
        # campaign id (false-positive on every new Meta campaign).
        if (row.get("campaign_type") or "").strip().lower() == "parent":
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
            err = ""
            needs_rebuild = False
            rebuilt_to = ""
            try:
                ok = fixer(v["container_id"])
            except Exception as exc:
                err = str(exc)
                # Meta forbids editing the conversion/pixel on a PUBLISHED ad set
                # (error_subcode 3260011 "Can't Make Edits to Published Ad Set").
                # An in-place patch is impossible — recreate the ad set with
                # correct tracking + pause the old one (what Tuan did manually).
                if "3260011" in err or "published" in err.lower() or "can't edit" in err.lower():
                    needs_rebuild = True
                    log.warning("meta_tracking_audit: %s is PUBLISHED — can't patch", v["container_id"])
                else:
                    log.error("meta_tracking_audit: repair failed for %s: %s", v["container_id"], err[:200])

            # Auto-rebuild path: recreate the published ad set with correct
            # tracking (copy → fix copy → pause old). Gated; failure → needs-human.
            if needs_rebuild and auto_rebuild:
                try:
                    rebuilt_to = rebuilder(v["container_id"])
                    old_pauser(v["container_id"])
                    ok = True
                    needs_rebuild = False
                    log.warning("meta_tracking_audit: rebuilt %s → %s (old paused)",
                                v["container_id"], rebuilt_to)
                except Exception as exc2:
                    err = (err + " | rebuild: " + str(exc2))[:300]
                    log.error("meta_tracking_audit: rebuild failed for %s: %s",
                              v["container_id"], str(exc2)[:200])

            v["repaired"] = ok
            v["needs_rebuild"] = needs_rebuild
            v["rebuilt_to"] = rebuilt_to
            v["fix_error"] = err[:300]
            try:
                from src.ui_decisions import log_event
                event = ("meta_tracking_rebuilt" if rebuilt_to
                         else "meta_tracking_repaired" if ok
                         else "meta_tracking_needs_rebuild")
                log_event(v["ramp_id"] or "", event,
                          {k: v[k] for k in ("platform", "container_id", "campaign_name", "cohort_geo")}
                          | {"was": v["promoted_object"], "repaired": ok,
                             "needs_rebuild": needs_rebuild, "rebuilt_to": rebuilt_to})
            except Exception as exc:
                log.debug("meta_tracking_audit: log_event skipped: %s", exc)
            detail.append(v)
            if ok:
                handled.append(v["container_id"])

    return {
        "name":       "meta_tracking",
        "checked":    checked,
        "violations": violations,
        "handled":    handled,
        "detail":     detail,
    }

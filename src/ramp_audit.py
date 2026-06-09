"""Per-ramp consolidated audit — the last step of every ramp LAUNCH.

Runs in headless CI (the Claude Code `audit-*` subagents can't), as the final
step of `run_launch_for_ramp`. Recursively **check → auto-fix → re-check** the
campaigns just created for this ramp until a fixpoint (no new fixable issue) or
`max_iterations`, then reports residual issues to console + Slack. Deterministic
and safe; never raises into the launch path.

Checks are registered in `_CHECKS` so more (budget / structure / tracking /
compliance) can slot in over time. Each check is responsible for its own
detection + (gated) fix and returns the set of container ids it handled this
pass; `audit_ramp` accumulates those across iterations so a re-check never
re-touches an already-fixed container.

Today's checks:
  - creative_resolution — pauses sub-MIN_CREATIVE_DIMENSION (pixelated) creatives.
"""
from __future__ import annotations

import logging
from typing import Optional

import config

log = logging.getLogger(__name__)


def _registry_rows_for_ramp(ramp_id: str) -> list[dict]:
    from src.campaign_registry import _load
    rid = str(ramp_id or "")
    return [
        r for r in _load()
        if str(r.get("smart_ramp_id") or r.get("ramp_id") or "") == rid
    ]


def _check_creative_resolution(rows, *, autofix, handled) -> dict:
    """Pause sub-minimum (pixelated) creatives. Returns {name, violations,
    handled (container ids fixed this pass), detail}."""
    from src.creative_resolution_audit import audit_creative_resolution
    cr = audit_creative_resolution(rows, autofix=autofix, exclude_containers=handled)
    return {
        "name": "creative_resolution",
        "violations": cr["violations"],
        "handled": [p["container_id"] for p in cr["paused"] if p.get("container_id")],
        "detail": cr["paused"],
    }


def _check_meta_tracking(rows, *, autofix, handled) -> dict:
    """Repair Meta ad sets not optimizing on the pixel event (the archived-
    custom-conversion trap that zeroed GMR-0023 conversions)."""
    from src.meta_tracking_audit import audit_meta_tracking
    mt = audit_meta_tracking(rows, autofix=autofix, exclude_containers=handled)
    return {
        "name": "meta_tracking",
        "violations": mt["violations"],
        "handled": mt["handled"],
        "detail": mt["detail"],
    }


def _check_linkedin_geo_only(rows, *, autofix, handled) -> dict:
    """Pause LinkedIn campaigns targeting geo-only (no narrowing facet) — the
    GMR-0024 ~290M country-wide class a cold-start facet collapse can ship."""
    from src.linkedin_geo_only_audit import audit_linkedin_geo_only
    go = audit_linkedin_geo_only(rows, autofix=autofix, exclude_containers=handled)
    return {
        "name": "linkedin_geo_only",
        "violations": go["violations"],
        "handled": go["handled"],
        "detail": go["detail"],
    }


# Registered deterministic checks. Each: (rows, *, autofix, handled) -> dict
# returning {name, violations[], handled[container ids fixed], detail[]}.
_CHECKS = [_check_creative_resolution, _check_meta_tracking, _check_linkedin_geo_only]


def audit_ramp(
    ramp_id: str,
    *,
    max_iterations: int = 3,
    autofix: Optional[bool] = None,
    notify: bool = True,
) -> dict:
    """Audit (and, when autofix on, fix) the campaigns created for `ramp_id`.

    Loops the registered checks until a pass applies zero new fixes (fixpoint)
    or `max_iterations`. Returns a summary dict; best-effort, never raises.
    """
    if not config.RAMP_AUDIT_ENABLED:
        return {"ramp_id": ramp_id, "skipped": "RAMP_AUDIT_ENABLED=false"}
    autofix = config.RAMP_AUDIT_AUTOFIX if autofix is None else autofix

    handled: set[str] = set()
    fixes: list[dict] = []
    residual: list[dict] = []
    iterations = 0
    try:
        for _ in range(max(1, max_iterations)):
            iterations += 1
            rows = _registry_rows_for_ramp(ramp_id)
            applied_this_pass = 0
            residual = []
            for check in _CHECKS:
                res = check(rows, autofix=autofix, handled=handled)
                for cid in res["handled"]:
                    handled.add(cid)
                applied_this_pass += len(res["handled"])
                fixes.extend(res.get("detail", []))
                # Anything still flagged whose container wasn't (or couldn't be) fixed.
                residual.extend([
                    v for v in res["violations"]
                    if v.get("container_id") and v["container_id"] not in handled
                ])
            if applied_this_pass == 0:
                break
    except Exception as exc:
        log.exception("ramp_audit: audit_ramp(%s) failed (%s)", ramp_id, exc)
        return {"ramp_id": ramp_id, "error": str(exc)[:200], "iterations": iterations}

    summary = {
        "ramp_id":       ramp_id,
        "iterations":    iterations,
        "fixes_applied": fixes,
        "residual":      residual,
        "autofix":       autofix,
        "checks":        [c.__name__ for c in _CHECKS],
    }
    log.info(
        "ramp_audit(%s): %d iteration(s), %d fix(es), %d residual issue(s)",
        ramp_id, iterations, len(fixes), len(residual),
    )
    if notify and (fixes or residual):
        _notify(ramp_id, fixes, residual)
    return summary


def _describe(item: dict) -> str:
    """One-line, check-agnostic description of an audit issue/fix."""
    plat = item.get("platform", "?")
    cid = item.get("container_id", "?")
    if item.get("width") is not None and item.get("height") is not None:
        return f"{plat} {cid} — creative {item['width']}x{item['height']}px (below minimum)"
    if "promoted_object" in item or item.get("expected"):
        return f"{plat} {cid} — wrong conversion tracking (not optimizing on the pixel event)"
    if item.get("geo_only"):
        aud = item.get("audience_size")
        aud_str = f", ~{aud} audience" if aud else ""
        return f"{plat} {cid} — geo-only targeting (no skill/title facet{aud_str})"
    return f"{plat} {cid}"


def _notify(ramp_id: str, fixes: list[dict], residual: list[dict]) -> None:
    lines = [f"🔎 Ramp audit for {ramp_id}:"]
    if fixes:
        lines.append(f"  • auto-fixed {len(fixes)} issue(s):")
        for f in fixes[:20]:
            verb = "repaired tracking on" if f.get("repaired") is not None else "paused"
            lines.append(f"    – {verb} {_describe(f)}")
    if residual:
        lines.append(f"  • ⚠️ {len(residual)} issue(s) NEED REVIEW (not auto-fixed):")
        for r in residual[:20]:
            lines.append(f"    – {_describe(r)}")
    try:
        from src.smart_ramp_notifier import _send_to_all_targets, _lookup_thread_ts
        _send_to_all_targets("\n".join(lines), ramp_id=ramp_id, thread_ts=_lookup_thread_ts(ramp_id))
    except Exception as exc:
        log.warning("ramp_audit: Slack notify failed (non-fatal): %s", exc)

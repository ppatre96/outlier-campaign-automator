"""Creative-resolution audit — catch (and optionally fix) thumbnail-resolution
creatives that reached an ad platform.

Background: GMR-0023 native-language B/C ad variants were uploaded at 64×64 and
rendered pixelated (Tuan, 2026-06-09). The launch-time upload guard
(`image_adapter.assert_min_dimensions`) now blocks that going forward, but the
auditor is the standing safety net for anything already live or that slips
through a path the guard doesn't cover.

Runs on every audit pass. Detection is deterministic — it reads the ACTUAL
pixel dimensions of each recent campaign's creative (Drive URL or local path)
and flags any whose short side is below `config.MIN_CREATIVE_DIMENSION`.

Autofix (gated by `config.AUDIT_AUTOFIX_LOWRES`): pauses the offending container
via the same proven `launch_verify` archivers used by verify-and-heal — keyed by
the registry's `platform_campaign_id` (Meta ad set / Google ad group / LinkedIn
campaign) — and writes a `creative_lowres_paused` audit row the console reads.

NOTE on granularity: for Meta/Google the angle A/B/C ads share one ad set / ad
group, so pausing the container pauses sibling angles too. That's the safe,
proven primitive (reviewer relaunches); per-ad pausing is a future refinement.
"""
from __future__ import annotations

import logging
from io import BytesIO
from pathlib import Path
from typing import Callable, Optional

import config

log = logging.getLogger(__name__)


def _default_dim_reader(path: str) -> Optional[tuple[int, int]]:
    """Return (width, height) for a local path or http(s)/Drive URL; None if
    unreadable. Best-effort — never raises."""
    try:
        from PIL import Image
        if str(path).startswith(("http://", "https://")):
            import requests
            resp = requests.get(path, timeout=30)
            resp.raise_for_status()
            with Image.open(BytesIO(resp.content)) as im:
                return im.size
        p = Path(path)
        if not p.exists():
            return None
        with Image.open(p) as im:
            return im.size
    except Exception as exc:
        log.debug("creative_resolution_audit: could not read dims of %s (%s)", path, exc)
        return None


def _default_pauser(platform: str, container_id: str) -> bool:
    """Pause the offending container via the proven launch_verify archivers."""
    from src import launch_verify
    p = (platform or "").strip().lower()
    if p == "meta":
        return launch_verify._archive_meta_adset(container_id)
    if p in ("google", "google_search"):
        return launch_verify._archive_google_adgroup(
            container_id, channel="search" if p == "google_search" else "display"
        )
    if p == "linkedin":
        return launch_verify._archive_linkedin_campaign(container_id)
    log.warning("creative_resolution_audit: no pauser for platform=%r", platform)
    return False


def audit_creative_resolution(
    rows: list[dict],
    *,
    min_px: Optional[int] = None,
    autofix: Optional[bool] = None,
    dim_reader: Callable[[str], Optional[tuple[int, int]]] = _default_dim_reader,
    pauser: Callable[[str, str], bool] = _default_pauser,
) -> dict:
    """Scan `rows` (campaign-registry rows) for sub-minimum creatives.

    Returns a summary dict: {checked, violations[], paused[], autofix, min_px}.
    Each violation/paused entry carries platform, container_id, creative_id,
    ramp_id, cohort_geo, path, width, height. Best-effort — never raises into
    the audit run.
    """
    min_px = config.MIN_CREATIVE_DIMENSION if min_px is None else min_px
    autofix = config.AUDIT_AUTOFIX_LOWRES if autofix is None else autofix

    checked = 0
    violations: list[dict] = []
    for row in rows:
        path = (row.get("creative_image_path") or "").strip()
        if not path:
            continue
        dims = dim_reader(path)
        if dims is None:
            continue
        checked += 1
        w, h = dims
        if min(w, h) < min_px:
            violations.append({
                "platform":     (row.get("platform") or "").strip().lower(),
                "container_id": row.get("platform_campaign_id") or row.get("linkedin_campaign_urn") or "",
                "creative_id":  row.get("platform_creative_id") or row.get("creative_urn") or "",
                "ramp_id":      row.get("smart_ramp_id") or row.get("ramp_id") or "",
                "cohort_geo":   row.get("cohort_geo") or "",
                "campaign_name": row.get("campaign_name") or "",
                "path":         path,
                "width":        w,
                "height":       h,
            })

    if violations:
        log.warning(
            "creative_resolution_audit: %d/%d creatives below %dpx minimum — %s",
            len(violations), checked, min_px,
            [f'{v["platform"]}:{v["width"]}x{v["height"]}' for v in violations],
        )

    paused: list[dict] = []
    if autofix and violations:
        seen: set[tuple[str, str]] = set()
        for v in violations:
            key = (v["platform"], v["container_id"])
            if not v["container_id"] or key in seen:
                continue
            seen.add(key)
            reason = (
                f"creative {v['width']}x{v['height']}px below {min_px}px minimum "
                f"(pixelated) — paused by auditor"
            )
            ok = False
            try:
                ok = pauser(v["platform"], v["container_id"])
            except Exception as exc:
                log.error("creative_resolution_audit: pause failed %s %s: %s",
                          v["platform"], v["container_id"], str(exc)[:200])
            try:
                from src.ui_decisions import log_event
                log_event(v["ramp_id"] or "", "creative_lowres_paused",
                          {**{k: v[k] for k in ("platform", "container_id", "creative_id",
                                                "cohort_geo", "campaign_name", "width", "height")},
                           "reason": reason, "paused": ok})
            except Exception as exc:
                log.debug("creative_resolution_audit: log_event skipped: %s", exc)
            paused.append({**v, "paused": ok})

    return {
        "checked":    checked,
        "violations": violations,
        "paused":     paused,
        "autofix":    autofix,
        "min_px":     min_px,
    }

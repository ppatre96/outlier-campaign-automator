"""LinkedIn geo-only targeting audit — third deterministic per-ramp check.

Guards the GMR-0024 class: a cold-start cohort whose LLM-coined skill/title
facets don't resolve to LinkedIn URNs ships a campaign targeting profileLocations
ONLY — i.e. the whole country (~290M for the US). `linkedin_targeting_guard`
prevents these at create time; this auditor is the post-hoc net for anything
that slips past (a different create path, a pre-guard campaign, a manual build).

A LinkedIn campaign is "geo-only" when its live include targetingCriteria
carries no NARROWING facet — only location facets (profileLocations / locations
/ ipLocations) and/or interfaceLocales. Generalist-locale campaigns carry a
language SKILL facet, so they're naturally exempt.

When autofix is on (RAMP_AUDIT_AUTOFIX and AUDIT_AUTOFIX_GEO_ONLY), the offending
campaign is archived (paused) via the proven launch_verify archiver before a
human un-pauses the draft, and a `linkedin_geo_only_paused` audit row is written
for the console.

Returns the same shape every per-ramp check returns:
{name, violations[], handled[container ids], detail[]}. Best-effort.
"""
from __future__ import annotations

import logging
from typing import Callable, Optional

import config

log = logging.getLogger(__name__)

# Facets that do NOT narrow to a profession/identity — a campaign whose include
# criteria is built only from these is effectively the whole geo.
_LOCATION_OR_LOCALE = {"profileLocations", "locations", "ipLocations", "interfaceLocales"}


def _short_facet(key: str) -> str:
    """'urn:li:adTargetingFacet:skills' → 'skills'; short keys pass through."""
    return key.rsplit(":", 1)[-1] if key.startswith("urn:li:adTargetingFacet:") else key


def _is_geo_only(targeting: dict) -> bool:
    """True iff the include criteria has no narrowing (non-location, non-locale)
    facet. Returns False on empty/missing targeting — we can't conclude, so we
    don't flag blind."""
    include = ((targeting or {}).get("include") or {}).get("and") or []
    facet_keys: set[str] = set()
    for clause in include:
        for k in ((clause or {}).get("or") or {}).keys():
            facet_keys.add(_short_facet(k))
    if not facet_keys:
        return False
    return not (facet_keys - _LOCATION_OR_LOCALE)


def audit_linkedin_geo_only(
    rows: list[dict],
    *,
    autofix: Optional[bool] = None,
    exclude_containers: Optional[set] = None,
    reader: Optional[Callable[[str], dict]] = None,
    pauser: Optional[Callable[[str], bool]] = None,
) -> dict:
    autofix = config.RAMP_AUDIT_AUTOFIX if autofix is None else autofix
    do_fix = bool(autofix) and config.AUDIT_AUTOFIX_GEO_ONLY
    exclude_containers = exclude_containers or set()

    _client = {"c": None}

    def _get_client():
        if _client["c"] is None:
            import os
            from src.linkedin_api import LinkedInClient
            token = (os.getenv("LINKEDIN_ACCESS_TOKEN") or os.getenv("LINKEDIN_TOKEN")
                     or config.LINKEDIN_TOKEN)
            _client["c"] = LinkedInClient(token)
        return _client["c"]

    reader = reader or (lambda cid: _get_client().get_campaign(cid).get("targetingCriteria") or {})
    if pauser is None:
        from src import launch_verify
        pauser = lambda cid: launch_verify._archive_linkedin_campaign(cid)

    violations: list[dict] = []
    detail: list[dict] = []
    handled: list[str] = []
    seen: set[str] = set()

    for row in rows:
        if str(row.get("platform") or "").lower() != "linkedin":
            continue
        cid = row.get("linkedin_campaign_urn") or row.get("platform_campaign_id") or ""
        if not cid or cid in exclude_containers or cid in seen:
            continue
        if str(row.get("status") or "").lower() in ("paused", "deprecated", "archived"):
            continue
        seen.add(cid)
        try:
            targeting = reader(cid)
        except Exception as exc:
            log.warning("linkedin_geo_only_audit: read failed for %s: %s", cid, exc)
            continue
        if not _is_geo_only(targeting):
            continue

        v = {
            "platform": "linkedin",
            "container_id": cid,
            "ramp_id": row.get("smart_ramp_id") or row.get("ramp_id") or "",
            "cohort_signature": row.get("cohort_signature") or "",
            "audience_size": row.get("audience_size") or "",
            "geo_only": True,
        }
        violations.append(v)

        paused: Optional[bool] = None
        if do_fix:
            try:
                paused = pauser(cid)
            except Exception as exc:
                log.error("linkedin_geo_only_audit: pause failed %s: %s", cid, exc)
                paused = False
            try:
                from src.ui_decisions import log_event
                log_event(v["ramp_id"] or "", "linkedin_geo_only_paused", {
                    "container_id": cid,
                    "cohort_signature": v["cohort_signature"],
                    "audience_size": v["audience_size"],
                    "paused": paused,
                    "reason": "geo-only targeting (no skill/title facet) — would target the whole country",
                })
            except Exception as exc:
                log.warning("linkedin_geo_only_audit: log_event failed (non-fatal): %s", exc)
            if paused:
                handled.append(cid)
        detail.append({**v, "paused": paused})

    return {
        "name": "linkedin_geo_only",
        "violations": violations,
        "handled": handled,
        "detail": detail,
    }

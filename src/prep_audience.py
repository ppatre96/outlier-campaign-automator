"""
Prep-time audience measurement for each cohort × platform.

Runs after Stage C accepts a cohort, before any campaign-create. For each
enabled channel (LinkedIn / Meta / Google), measures the reach API estimate
using the cohort's rules + the ramp's included_geos as targeting input.

Results are persisted to the `cohort_audience` Postgres table so the
console can render per-channel AudienceBadge for every cohort BEFORE
Diego/Bryan click Approve. Without this step, reviewers had to launch and
wait for registry rows to see audience size per channel.

Best-effort: any platform whose client/resolver fails (missing creds,
network, API rejection) gets status='skipped' and audience_size=None.
LinkedIn is always available because Stage C already computed it — we just
re-persist that number here so the console reads it from one place.
"""
from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Optional

import config

log = logging.getLogger(__name__)


@dataclass
class ChannelAudience:
    platform: str
    audience_size: Optional[int]
    status: str               # measured | denarrowed | below_floor | skipped | forecast
    geos_used: list[str]
    rules_dropped: int = 0
    # Resolved targeting facets for this channel, persisted to cohort_targeting
    # so the console can show reviewers what's actually being targeted per
    # channel. Meta/Google hold the resolver's targeting dict; LinkedIn holds
    # the cohort rule features.
    facets: dict = field(default_factory=dict)
    # Google Search keyword forecast (estimated clicks/conversions/cost). Empty
    # for every other channel. status='forecast' when populated; the console
    # renders this instead of an audience-size badge for the Search row.
    forecast: dict = field(default_factory=dict)


def measure_audience_for_cohort(
    cohort,
    *,
    included_geos: list[str],
    enabled_platforms: list[str],
    li_audience_size: Optional[int] = None,
    li_client=None,
    urn_resolver=None,
) -> list[ChannelAudience]:
    """
    Return audience estimates for every enabled platform for one cohort.

    LinkedIn estimate is taken from `li_audience_size` if provided (Stage C
    already computed it). Meta + Google fire fresh estimates against the
    given geos. Any platform that fails is reported with status='skipped'
    so the caller can persist it without losing the row.

    Caller is responsible for persisting via upsert_cohort_audience.
    """
    results: list[ChannelAudience] = []
    enabled = {p.lower() for p in (enabled_platforms or []) if p}
    geos = [g.upper() for g in (included_geos or []) if g]

    # ── LinkedIn ──
    if "linkedin" in enabled or li_audience_size is not None:
        _gen_locale = (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale")
        # Geos to size on: the ramp's included_geos, falling back to the locale
        # region for generalist cohorts the ramp left empty (e.g. ko-KR → KR).
        est_geos = list(geos)
        if not est_geos and _gen_locale:
            from src.locales import region_for_locale
            _r = region_for_locale(_gen_locale)
            if _r:
                est_geos = [_r]

        size = li_audience_size if li_audience_size is not None else getattr(cohort, "audience_size", None)
        # Live LinkedIn reach estimate based on the cohort's facets, so the
        # console shows TRUE reach every run rather than the unset default.
        # Stage C already sizes specialist cohorts (passed as li_audience_size);
        # the generalist locale path skips the beam, so audience_size defaults
        # to 0 and the console showed a misleading "0 (below floor)". When a
        # client + URN resolver are available and there's no Stage C number,
        # query audienceCounts on the geo facet — LinkedIn targets these
        # cohorts by geo (the languages aren't LinkedIn interface locales).
        # Language skill (Diego 2026-06-04): generalist cohorts target the
        # language as a LinkedIn skill within the geo, so the estimate must use
        # skill + geo to match the real campaign targeting (geo-only over-counts).
        _skill_urn = None
        if _gen_locale:
            from src.locales import linkedin_skill_urn
            _skill_urn = linkedin_skill_urn(_gen_locale)
        if li_audience_size is None and li_client is not None and urn_resolver is not None and est_geos:
            try:
                geo_urns: list[str] = []
                for _c in est_geos:
                    _u = urn_resolver.resolve("profileLocations", _c)
                    if _u:
                        geo_urns.append(_u)
                if geo_urns:
                    _facets = {"profileLocations": geo_urns}
                    if _skill_urn:
                        _facets["skills"] = [_skill_urn]
                    live = li_client.get_audience_count(_facets)
                    if live and live > 0:
                        size = live
                        log.info(
                            "LinkedIn live audience: cohort=%s geos=%s skill=%s → %d",
                            getattr(cohort, "name", "?"), est_geos, _skill_urn or "(geo-only)", live,
                        )
            except Exception as exc:
                log.warning(
                    "LinkedIn live geo audience estimate failed for cohort=%s: %s",
                    getattr(cohort, "name", "?"), exc,
                )

        status = "measured" if size is not None else "skipped"
        if size is not None and size < config.AUDIENCE_SIZE_MIN:
            status = "below_floor"
        if _gen_locale:
            # Generalist locale cohort → LinkedIn targets by geo only (v1).
            # Surface human-readable facets instead of the raw synthetic rule.
            from src.locales import get_locale
            _lt = get_locale(_gen_locale)
            li_facets = {
                "locale": _gen_locale,
                "language": (_lt.display_language if _lt else _gen_locale),
                "geos": est_geos,
                "language_skill_urn": _skill_urn,
                "geo_only": _skill_urn is None,
            }
        else:
            li_facets = {"rules": [str(feat) for feat, _val in (getattr(cohort, "rules", None) or [])]}
        results.append(ChannelAudience(
            platform="linkedin", audience_size=size,
            status=status, geos_used=est_geos,
            facets=li_facets,
        ))

    # ── Meta ──
    if "meta" in enabled:
        results.append(_measure_meta(cohort, geos))

    # ── Google ──
    if "google" in enabled:
        results.append(_measure_google(cohort, geos))

    # ── Google Search (keyword forecast) ──
    # Search isn't auto-created (Bryan builds it), but the forecast — estimated
    # clicks + conversions for the cohort's keyword pool — is a useful review
    # signal, and the right "number" for a keyword-targeted channel (audience
    # size never fit Search). Produced whenever Google is in play.
    if "google" in enabled or "google_search" in enabled:
        results.append(_measure_google_search(cohort, geos))

    return results


def _measure_meta(cohort, geos: list[str]) -> ChannelAudience:
    """Best-effort Meta delivery_estimate call. Falls back to status='skipped'
    on any failure so the row still persists for the UI."""
    try:
        from src.meta_api import MetaClient
        from src.meta_targeting import MetaInterestResolver

        client = MetaClient()
        resolver = MetaInterestResolver()
        # Meta SAC=EMPLOYMENT requires non-empty countries. Pre-launch we use
        # whatever the Smart Ramp's included_geos says; if that's empty
        # the resolver raises, which we catch and report as 'skipped'.
        targeting = resolver.resolve_cohort(cohort, geos=geos)
        size = client.get_reach_estimate(targeting)
        status = "measured" if size is not None else "skipped"
        if size is not None and size < config.AUDIENCE_SIZE_MIN:
            status = "below_floor"
        return ChannelAudience(
            platform="meta", audience_size=size,
            status=status, geos_used=geos,
            facets=targeting,
        )
    except Exception as exc:
        log.info(
            "prep_audience[meta]: skipped for cohort=%s — %s: %s",
            getattr(cohort, "name", "?"), type(exc).__name__, exc,
        )
        return ChannelAudience(
            platform="meta", audience_size=None,
            status="skipped", geos_used=geos,
        )


def _measure_google(cohort, geos: list[str]) -> ChannelAudience:
    """Best-effort Google Ads ReachPlan / audience-segment estimate."""
    try:
        from src.google_ads_api import GoogleAdsClient
        from src.google_targeting import GoogleSegmentResolver

        client = GoogleAdsClient()
        resolver = GoogleSegmentResolver()
        targeting = resolver.resolve_cohort(cohort, geos=geos)
        size = client.get_reach_estimate(targeting)
        status = "measured" if size is not None else "skipped"
        if size is not None and size < config.AUDIENCE_SIZE_MIN:
            status = "below_floor"
        return ChannelAudience(
            platform="google", audience_size=size,
            status=status, geos_used=geos,
            facets=targeting,
        )
    except Exception as exc:
        log.info(
            "prep_audience[google]: skipped for cohort=%s — %s: %s",
            getattr(cohort, "name", "?"), type(exc).__name__, exc,
        )
        return ChannelAudience(
            platform="google", audience_size=None,
            status="skipped", geos_used=geos,
        )


def _measure_google_search(cohort, geos: list[str]) -> ChannelAudience:
    """Best-effort Google Search keyword forecast (estimated clicks +
    conversions for the cohort's keyword pool). status='forecast' on success;
    'skipped' on any failure so the row still persists for the UI."""
    # Generalist locale cohorts may have empty included_geos → fall back to the
    # locale region so the forecast has a geo to scope to (mirrors LinkedIn).
    est_geos = list(geos)
    _gen_locale = (getattr(cohort, "facet_strength", None) or {}).get("generalist_locale")
    if not est_geos and _gen_locale:
        from src.locales import region_for_locale
        _r = region_for_locale(_gen_locale)
        if _r:
            est_geos = [_r]
    try:
        from src.google_ads_api import GoogleAdsClient
        from src.google_targeting import GoogleSegmentResolver

        resolver = GoogleSegmentResolver()
        targeting = resolver.resolve_cohort(cohort, geos=est_geos)
        client = GoogleAdsClient(channel="search")
        forecast = client.get_keyword_forecast(targeting)
        if not forecast:
            return ChannelAudience(
                platform="google_search", audience_size=None,
                status="skipped", geos_used=est_geos,
                facets={"keywords": (targeting.get("keyword_ideas") or [])[:30]},
            )
        return ChannelAudience(
            platform="google_search", audience_size=None,
            status="forecast", geos_used=est_geos,
            facets={"keywords": (targeting.get("keyword_ideas") or [])[:30]},
            forecast=forecast,
        )
    except Exception as exc:
        log.info(
            "prep_audience[google_search]: skipped for cohort=%s — %s: %s",
            getattr(cohort, "name", "?"), type(exc).__name__, exc,
        )
        return ChannelAudience(
            platform="google_search", audience_size=None,
            status="skipped", geos_used=est_geos,
        )

"""
Platform-aware metric fetchers for the campaign feedback agent.

The feedback agent's existing LinkedIn metric fetch (in
campaign_feedback_agent.py) has been extended to also iterate non-LinkedIn
registry rows; this module provides the per-platform fetchers it calls.

`fetch_metrics_for_active_extra_platforms()` is the single entry point.
For each active Meta or Google registry row it calls the platform-specific
Insights / reporting API and pushes the result to
`campaign_registry.update_metrics`. Failures are non-fatal.

For v1 the fetchers are conservative — they pull impressions, clicks, and
spend, computing CTR / CPC / CPM downstream in `update_metrics`. Application
counts (the conversion metric) are left at 0 unless conversion tracking is
configured on the platform side; that's deferred to a v2 cleanup with
proper conversion-pixel + Google conversion-action setup.
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import config

log = logging.getLogger(__name__)


def fetch_metrics_for_active_extra_platforms(window_days: int = 7) -> int:
    """Iterate active non-LinkedIn registry rows and pull fresh metrics from
    each platform's reporting API. Returns the number of rows updated."""
    from src.campaign_registry import _load, update_metrics

    records = _load()
    updated = 0
    for rec in records:
        if rec.get("status") != "active":
            continue
        platform = (rec.get("platform") or "linkedin").lower()
        if platform == "linkedin":
            continue   # LinkedIn fetch lives in campaign_feedback_agent.run()

        campaign_id = rec.get("platform_campaign_id") or ""
        if not campaign_id:
            continue
        try:
            if platform == "meta":
                metrics = _fetch_meta_insights(campaign_id, window_days)
            elif platform == "google":
                metrics = _fetch_google_metrics(campaign_id, window_days)
            else:
                log.debug("platform_metrics: unknown platform %r — skipping", platform)
                continue

            if metrics:
                update_metrics(
                    linkedin_campaign_urn=campaign_id,   # arg name is legacy; matches by id
                    impressions=metrics.get("impressions", 0),
                    clicks=metrics.get("clicks", 0),
                    spend_usd=float(metrics.get("spend_usd", 0.0)),
                    applications=metrics.get("applications", 0),
                )
                updated += 1
        except Exception as exc:
            log.warning(
                "platform_metrics[%s]: fetch failed for %s — %s",
                platform, campaign_id, exc,
            )
    log.info("platform_metrics: updated %d rows across non-LinkedIn platforms", updated)
    return updated


# ── Meta Insights ─────────────────────────────────────────────────────────────


def _fetch_meta_insights(campaign_id: str, window_days: int) -> dict[str, Any] | None:
    """Pull aggregate metrics for a Meta Campaign / Ad Set from the Insights
    API. `campaign_id` is the numeric Meta ID (not a URN)."""
    if not config.META_ACCESS_TOKEN:
        return None
    from facebook_business.api import FacebookAdsApi
    from facebook_business.adobjects.adset import AdSet

    FacebookAdsApi.init(
        access_token=config.META_ACCESS_TOKEN,
        api_version=config.META_API_VERSION or "v21.0",
    )
    since = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    ad_set = AdSet(campaign_id)
    insights = ad_set.get_insights(
        params={
            "time_range":      {"since": since, "until": until},
            "fields":          ["impressions", "clicks", "spend"],
            "level":           "adset",
        }
    )
    if not insights:
        return None
    row = insights[0]
    return {
        "impressions": int(row.get("impressions", 0) or 0),
        "clicks":      int(row.get("clicks", 0) or 0),
        "spend_usd":   float(row.get("spend", 0.0) or 0.0),
        "applications": 0,
    }


# ── Google Ads reporting ──────────────────────────────────────────────────────


def _fetch_google_metrics(ad_group_resource: str, window_days: int) -> dict[str, Any] | None:
    """Pull aggregate metrics for a Google Ads Ad Group via search_stream.
    `ad_group_resource` is the resource name (`customers/<cid>/adGroups/<id>`)."""
    if not (config.GOOGLE_ADS_DEVELOPER_TOKEN and config.GOOGLE_ADS_CUSTOMER_ID):
        return None
    creds = {
        "developer_token":  config.GOOGLE_ADS_DEVELOPER_TOKEN,
        "refresh_token":    config.GOOGLE_ADS_REFRESH_TOKEN,
        "client_id":        config.GOOGLE_ADS_CLIENT_ID,
        "client_secret":    config.GOOGLE_ADS_CLIENT_SECRET,
        "use_proto_plus":   True,
    }
    if config.GOOGLE_ADS_LOGIN_CUSTOMER_ID:
        creds["login_customer_id"] = str(config.GOOGLE_ADS_LOGIN_CUSTOMER_ID).replace("-", "")
    from google.ads.googleads.client import GoogleAdsClient as _SDKClient
    sdk = _SDKClient.load_from_dict(creds)
    ga_service = sdk.get_service("GoogleAdsService")
    customer_id = str(config.GOOGLE_ADS_CUSTOMER_ID).replace("-", "")

    # Pull last N days from the metrics view, scoped to this ad group.
    since = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()

    query = f"""
        SELECT
          metrics.impressions,
          metrics.clicks,
          metrics.cost_micros
        FROM ad_group
        WHERE ad_group.resource_name = '{ad_group_resource}'
          AND segments.date BETWEEN '{since}' AND '{until}'
    """
    impressions = 0
    clicks = 0
    cost_micros = 0
    for batch in ga_service.search_stream(customer_id=customer_id, query=query):
        for row in batch.results:
            impressions += int(row.metrics.impressions or 0)
            clicks      += int(row.metrics.clicks or 0)
            cost_micros += int(row.metrics.cost_micros or 0)
    return {
        "impressions": impressions,
        "clicks":      clicks,
        "spend_usd":   cost_micros / 1_000_000.0,
        "applications": 0,
    }

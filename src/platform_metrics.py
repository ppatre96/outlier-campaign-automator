"""
Platform-aware metric fetchers for the campaign feedback agent.

The feedback agent's existing LinkedIn metric fetch (in
campaign_feedback_agent.py) has been extended to also iterate non-LinkedIn
registry rows; this module provides the per-platform fetchers it calls.

`fetch_metrics_for_active_extra_platforms()` is the single entry point.
For each active Meta / Google / Reddit registry row it calls the platform-specific
Insights / reporting API and pushes the result to
`campaign_registry.update_metrics`. Failures are non-fatal.

The fetchers pull impressions, clicks, spend, and conversions (CTR / CPC / CPM
/ CPA are computed downstream in `update_metrics`). Conversions map to
`applications`: Meta sums the worker_skill_all custom action_type
(config.META_CONVERSION_ACTION_TYPE); Google reads metrics.conversions
(GOOGLE_CONVERSION_ACTION_ID 7625599821, worker_skill_all).
"""
from __future__ import annotations

import logging
from datetime import datetime, timedelta, timezone
from typing import Any

import config

log = logging.getLogger(__name__)

# Statuses that mean the campaign is gone — skip fetching metrics for these.
# Everything else (active / ENABLED / PAUSED / LIMITED / empty) still has
# historical metrics worth refreshing. The registry mixes pipeline vocab
# ("active") with platform-native vocab ("ENABLED", "PAUSED", "REMOVED"), so a
# strict `== "active"` check silently skipped live ENABLED rows.
_DEAD_STATUSES = {"removed", "archived", "deleted", "deprecated", "cancelled", "superseded"}


def _is_live(status: str | None) -> bool:
    """True if a campaign row is worth refreshing metrics for."""
    return (status or "").strip().lower() not in _DEAD_STATUSES


def fetch_metrics_for_active_extra_platforms(window_days: int = 7) -> int:
    """Iterate active non-LinkedIn registry rows and pull fresh metrics from
    each platform's reporting API. Returns the number of rows updated."""
    from src.campaign_registry import _load, update_metrics

    records = _load()
    updated = 0
    for rec in records:
        if not _is_live(rec.get("status")):
            continue
        platform = (rec.get("platform") or "linkedin").lower()
        if platform == "linkedin":
            continue   # LinkedIn fetch lives in campaign_feedback_agent.refresh_linkedin_metrics()

        campaign_id = rec.get("platform_campaign_id") or ""
        if not campaign_id:
            continue
        try:
            if platform == "meta":
                metrics = _fetch_meta_insights(campaign_id, window_days)
            elif platform in ("google", "google_search"):
                metrics = _fetch_google_metrics(campaign_id, window_days)
            elif platform == "reddit":
                metrics = _fetch_reddit_metrics(campaign_id, window_days)
            elif platform == "tiktok":
                metrics = _fetch_tiktok_metrics(campaign_id, window_days)
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


def _meta_conversions_from_actions(actions: Any, action_type: str) -> int:
    """Sum the value of the conversion `action_type` from a Meta Insights
    `actions` list. Meta reports worker_skill_all under a custom action_type
    (see config.META_CONVERSION_ACTION_TYPE). Returns 0 when absent or the
    action_type is unset. Pure — unit-tested."""
    if not action_type or not isinstance(actions, (list, tuple)):
        return 0
    total = 0
    for a in actions:
        if isinstance(a, dict) and a.get("action_type") == action_type:
            try:
                total += int(float(a.get("value", 0) or 0))
            except (TypeError, ValueError):
                pass
    return total


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
            "fields":          ["impressions", "clicks", "spend", "actions"],
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
        "applications": _meta_conversions_from_actions(
            row.get("actions"), config.META_CONVERSION_ACTION_TYPE
        ),
    }


# ── Google Ads reporting ──────────────────────────────────────────────────────


def _google_query_for_id(campaign_ref: str, since: str, until: str) -> str | None:
    """Build the GAQL metrics query for a Google Ads id. The registry stores
    three id shapes across Display + Search rows:
      - adGroup resource  `customers/<cid>/adGroups/<id>`  → FROM ad_group
      - campaign resource `customers/<cid>/campaigns/<id>` → FROM campaign
      - bare-numeric campaign id `2390173...`              → FROM campaign (by id)
    Returns None for an unrecognized shape (caller skips). Pure — unit-tested."""
    ref = (campaign_ref or "").strip()
    select = "SELECT metrics.impressions, metrics.clicks, metrics.cost_micros, metrics.conversions"
    date = f"AND segments.date BETWEEN '{since}' AND '{until}'"
    if "/adGroups/" in ref:
        return f"{select} FROM ad_group WHERE ad_group.resource_name = '{ref}' {date}"
    if "/campaigns/" in ref:
        return f"{select} FROM campaign WHERE campaign.resource_name = '{ref}' {date}"
    if ref.isdigit():
        return f"{select} FROM campaign WHERE campaign.id = {ref} {date}"
    return None


def _customer_id_from_ref(campaign_ref: str) -> str | None:
    """Customer id embedded in a `customers/<cid>/...` resource name, else None
    (caller falls back to the configured customer id)."""
    ref = (campaign_ref or "").strip()
    if ref.startswith("customers/"):
        parts = ref.split("/")
        if len(parts) >= 2 and parts[1].isdigit():
            return parts[1]
    return None


def _fetch_google_metrics(campaign_ref: str, window_days: int) -> dict[str, Any] | None:
    """Pull aggregate metrics for a Google Ads campaign or ad group via
    search_stream. `campaign_ref` may be an adGroup resource, a campaign
    resource, or a bare-numeric campaign id (Display vs Search differ)."""
    if not (config.GOOGLE_ADS_DEVELOPER_TOKEN and config.GOOGLE_ADS_CUSTOMER_ID):
        return None

    since = (datetime.now(timezone.utc) - timedelta(days=window_days)).date().isoformat()
    until = datetime.now(timezone.utc).date().isoformat()
    query = _google_query_for_id(campaign_ref, since, until)
    if not query:
        log.debug("platform_metrics[google]: unrecognized id shape %r — skipping", campaign_ref)
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
    customer_id = (
        _customer_id_from_ref(campaign_ref) or str(config.GOOGLE_ADS_CUSTOMER_ID)
    ).replace("-", "")

    impressions = 0
    clicks = 0
    cost_micros = 0
    conversions = 0.0
    for batch in ga_service.search_stream(customer_id=customer_id, query=query):
        for row in batch.results:
            impressions += int(row.metrics.impressions or 0)
            clicks      += int(row.metrics.clicks or 0)
            cost_micros += int(row.metrics.cost_micros or 0)
            conversions += float(row.metrics.conversions or 0)
    return {
        "impressions": impressions,
        "clicks":      clicks,
        "spend_usd":   cost_micros / 1_000_000.0,
        "applications": int(round(conversions)),
    }


# Reddit's reporting API returns every campaign in one call, so fetch once per
# refresh window and serve rows from the cache rather than calling per row.
_reddit_metrics_cache: dict[int, dict] = {}


def _fetch_reddit_metrics(campaign_id: str, window_days: int) -> dict[str, Any] | None:
    """Impressions / clicks / spend for a Reddit campaign from the Ads reporting
    API. sign-ups / activations come from the funnel (funnel_writeback), so
    applications is left at 0 here."""
    if not config.REDDIT_API_ENABLED:
        return None
    if window_days not in _reddit_metrics_cache:
        try:
            from src.reddit_api import RedditClient
            _reddit_metrics_cache[window_days] = RedditClient().fetch_campaign_metrics(window_days)
        except Exception as exc:  # noqa: BLE001 — best-effort, non-fatal
            log.warning("platform_metrics[reddit]: reporting fetch failed — %s", exc)
            _reddit_metrics_cache[window_days] = {}
    m = _reddit_metrics_cache[window_days].get(str(campaign_id))
    if not m:
        return None
    return {
        "impressions":  m["impressions"],
        "clicks":       m["clicks"],
        "spend_usd":    m["spend_usd"],
        "applications": 0,
    }


# ── TikTok reporting ──────────────────────────────────────────────────────────

_tiktok_metrics_cache: dict[int, dict] = {}


def _fetch_tiktok_metrics(campaign_id: str, window_days: int) -> dict[str, Any] | None:
    """Impressions / clicks / spend for a TikTok campaign from the Marketing
    reporting API. sign-ups / activations come from the funnel (funnel_writeback,
    by UTM), so applications is left at 0 here. One report call covers all
    campaigns → cache by window."""
    if not config.TIKTOK_API_ENABLED:
        return None
    if window_days not in _tiktok_metrics_cache:
        try:
            from src.tiktok_api import TikTokClient
            _tiktok_metrics_cache[window_days] = TikTokClient().fetch_campaign_metrics(window_days)
        except Exception as exc:  # noqa: BLE001 — best-effort, non-fatal
            log.warning("platform_metrics[tiktok]: reporting fetch failed — %s", exc)
            _tiktok_metrics_cache[window_days] = {}
    m = _tiktok_metrics_cache[window_days].get(str(campaign_id))
    if not m:
        return None
    return {
        "impressions":  m["impressions"],
        "clicks":       m["clicks"],
        "spend_usd":    m["spend_usd"],
        "applications": 0,
    }

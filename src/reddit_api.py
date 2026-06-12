"""
Reddit Ads API client.

Implements `AdPlatformClient` so the Outlier campaign pipeline can dispatch to
Reddit the same way it does LinkedIn / Meta / Google. Reddit's hierarchy is
Campaign → Ad Group → Ad, mirroring Meta (Campaign → Ad Set → Ad).

TWO-PHASE rollout (see the plan + config.REDDIT_API_ENABLED):

  • v1 — CREATIVE-ONLY (REDDIT_API_ENABLED off, the default). The Reddit Ads
    API is allow-list gated and SEPARATE from the Ads Manager UI; until access
    is granted we do not create campaigns programmatically. The arm
    (`_process_extra_platform_arm`) still runs its Phase 1 — generate the image
    + free-form creatives and write a handoff manifest to Drive — and these
    create methods degrade cleanly: `create_image_ad` returns a
    `local_fallback` result, and `create_campaign_group` / `create_campaign` /
    `upload_image` raise a descriptive RuntimeError so the arm's per-(cohort×geo)
    try/except short-circuits Phase 2 with the creatives already exported.
    (Same path Meta takes when ad creation can't proceed.)

  • Phase 2 — PROGRAMMATIC (REDDIT_API_ENABLED on, once allow-list granted).
    The method bodies below the gate are TODO: the exact Reddit Ads API v3
    request/response shapes (OAuth scopes, endpoints, targeting + conversion
    payload field names, media upload, CTA enum, budget units) MUST be verified
    against the official v3 reference / Postman collection before implementing.
    Per-pod conversion attaches via REDDIT_PIXEL_ID + REDDIT_POD_CONVERSION_EVENTS.

Everything will be created PAUSED (mirrors the LinkedIn DRAFT / Meta PAUSED
default). Names are auto-prefixed with `config.AGENT_NAME_PREFIX` (empty by
default — see config).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    REDDIT_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)

# Reddit Ads API v3 base (verify before Phase 2 use).
REDDIT_ADS_API_BASE = "https://ads-api.reddit.com/api/v3"

_DISABLED_MSG = (
    "REDDIT_API_ENABLED is off — Reddit Ads API access is not yet granted. "
    "Creatives + targeting/conversion manifest were exported to Drive for "
    "manual upload in Reddit Ads Manager."
)


def reddit_pod_conversion_event(pod: Optional[str]) -> str:
    """Map a Smart Ramp pod → its Reddit per-pod worker_skill_grant event name.

    Mirrors `main._linkedin_pod_conversion_id`. Falls back to the WS_all event
    when the pod is missing/unrecognized. Returns "" when nothing is configured
    yet (the Reddit pixel/event names are pending from Tuan — see config).
    """
    events = getattr(config, "REDDIT_POD_CONVERSION_EVENTS", {}) or {}
    key = (pod or "").strip().lower()
    return events.get(key) or events.get("all") or ""


class RedditClient(AdPlatformClient):
    """Reddit Ads API client.

    Construction is cheap (no network/creds needed) so the arm builds even in
    creative-only mode. The create methods self-gate on `config.REDDIT_API_ENABLED`.
    """

    name = "reddit"
    constraints: PlatformConstraints = REDDIT_CONSTRAINTS

    AGENT_NAME_PREFIX = config.AGENT_NAME_PREFIX

    def __init__(self) -> None:
        self._token = config.REDDIT_ACCESS_TOKEN
        self._account_id = config.REDDIT_AD_ACCOUNT_ID
        self._session = None  # lazily created in Phase 2

    # ── helpers ────────────────────────────────────────────────────────────────

    def _prefixed(self, name: str) -> str:
        """Prepend AGENT_NAME_PREFIX (idempotent). No-op when prefix empty."""
        if not self.AGENT_NAME_PREFIX or name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    def _ensure_init(self) -> None:
        """Validate creds for Phase-2 programmatic calls. Raises a clear error
        when the API is enabled but creds/impl aren't ready."""
        if not config.REDDIT_API_ENABLED:
            raise RuntimeError(_DISABLED_MSG)
        missing = [
            k for k, v in {
                "REDDIT_ACCESS_TOKEN": self._token,
                "REDDIT_AD_ACCOUNT_ID": self._account_id,
            }.items() if not v
        ]
        if missing:
            raise RuntimeError(
                f"REDDIT_API_ENABLED is on but {', '.join(missing)} not set — "
                "cannot call the Reddit Ads API."
            )

    # ── lifecycle (AdPlatformClient) ────────────────────────────────────────────

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        self._ensure_init()
        # Phase 2 TODO — verify against Reddit Ads API v3 before implementing:
        #   POST {base}/ad_accounts/{account_id}/campaigns
        #   body: {name, objective: CONVERSIONS, status: PAUSED, budget, ...}
        raise NotImplementedError(
            "Reddit programmatic campaign creation not implemented yet — verify "
            "the Reddit Ads API v3 contract (campaigns endpoint) first."
        )

    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int = 10000,
    ) -> str:
        self._ensure_init()
        # Phase 2 TODO — verify against Reddit Ads API v3:
        #   POST {base}/ad_accounts/{account_id}/ad_groups
        #   body: {campaign_id, targeting:{subreddits, interests, keywords, geo},
        #          conversion: {pixel_id: REDDIT_PIXEL_ID,
        #                       event: reddit_pod_conversion_event(targeting["pod"])},
        #          bid_strategy, daily_budget, status: PAUSED}
        raise NotImplementedError(
            "Reddit programmatic ad-group creation not implemented yet — verify "
            "the Reddit Ads API v3 contract (ad_groups + conversion attach) first."
        )

    def upload_image(self, image_path: str | Path) -> str:
        self._ensure_init()
        # Phase 2 TODO — verify Reddit media-upload endpoint + returned asset id.
        raise NotImplementedError(
            "Reddit programmatic image upload not implemented yet — verify the "
            "Reddit Ads API v3 media-upload contract first."
        )

    def create_image_ad(
        self,
        campaign_id: str,
        image_id: str,
        headline: str,
        description: str,
        primary_text: Optional[str] = None,
        ad_headline: Optional[str] = None,
        intro_text: Optional[str] = None,
        cta: Optional[str] = None,
        destination_url: Optional[str] = None,
    ) -> CreateAdResult:
        # Creative-only mode: signal local_fallback so the arm logs the PNG +
        # manifest path and continues (no exception needed here — this is the
        # expected v1 outcome until the API is enabled).
        if not config.REDDIT_API_ENABLED:
            return CreateAdResult(
                status="local_fallback",
                error_class="RedditApiDisabled",
                error_message=_DISABLED_MSG,
            )
        self._ensure_init()
        # Phase 2 TODO — verify Reddit Ads API v3:
        #   create the promoted-post image ad (title=headline, cta) + the
        #   free-form/native text post, both referencing the ad group.
        raise NotImplementedError(
            "Reddit programmatic ad creation not implemented yet — verify the "
            "Reddit Ads API v3 contract (ads / posts endpoints) first."
        )

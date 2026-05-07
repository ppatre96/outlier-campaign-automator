"""
Meta Ads (Facebook/Instagram) Marketing API client.

Implements `AdPlatformClient` so the Outlier campaign pipeline can dispatch
to Meta the same way it dispatches to LinkedIn. Mirrors the LinkedIn
client's contracts:

  - `create_campaign_group` → creates a Meta Campaign with the right
    objective (OUTCOME_TRAFFIC by default) and special_ad_categories.
  - `create_campaign`       → creates a Meta Ad Set under the campaign with
    targeting + daily budget.
  - `upload_image`          → uploads via AdImage.api_create, returns the
    image_hash.
  - `create_image_ad`       → creates an AdCreative + Ad referencing the
    ad set, with object_story_spec.link_data.

Everything is created PAUSED (mirrors the LinkedIn DRAFT default) so nothing
launches without a human approval step in Ads Manager. Names are
auto-prefixed with `config.AGENT_NAME_PREFIX` ("agent_").

Failures inside `create_image_ad` surface as `CreateAdResult(status=...)`
(local_fallback / error) so the pipeline's per-cohort isolation can save
the PNG locally and continue.
"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any, Optional

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    META_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)


# Meta CTA enum — facebook_business uses these literal strings on AdCreative.
_VALID_META_CTAS = {
    "APPLY_NOW", "LEARN_MORE", "SIGN_UP", "GET_STARTED",
    "DOWNLOAD", "CONTACT_US", "GET_OFFER", "SUBSCRIBE",
}


class MetaClient(AdPlatformClient):
    """Meta Ads API client.

    The pipeline calls `create_campaign_group` once per (cohort × geo) to
    create a Campaign, then `create_campaign` per (cohort × geo × angle) to
    create an Ad Set under it (Meta's hierarchy is Campaign → Ad Set → Ad).

    Construction is lazy — the Facebook SDK is only initialised when the
    first call is made, so importing this module doesn't require credentials
    to be present.
    """

    name = "meta"
    constraints: PlatformConstraints = META_CONSTRAINTS

    AGENT_NAME_PREFIX = config.AGENT_NAME_PREFIX  # "agent_"
    DEFAULT_OBJECTIVE = "OUTCOME_TRAFFIC"

    # Placeholder daily budget. The Meta ABO ad-set creation API requires a
    # daily_budget value, but our pipeline only creates DRAFT/PAUSED entities
    # — the human reviewer sets the real budget in Ads Manager before
    # un-pausing. Use the API-allowed minimum ($1.00 = 100 cents) so no
    # real spend can occur even if a campaign is accidentally activated.
    PLACEHOLDER_DAILY_BUDGET_CENTS = 100

    def __init__(
        self,
        access_token: Optional[str] = None,
        app_id:       Optional[str] = None,
        app_secret:   Optional[str] = None,
        ad_account_id: Optional[str] = None,
        api_version:  Optional[str] = None,
        page_id:      Optional[str] = None,
    ):
        self._access_token = access_token   or config.META_ACCESS_TOKEN
        self._app_id       = app_id         or config.META_APP_ID
        self._app_secret   = app_secret     or config.META_APP_SECRET
        self._ad_account_id = (ad_account_id or config.META_AD_ACCOUNT_ID)
        # Allow callers to pass either "1234" or "act_1234"
        if self._ad_account_id and not self._ad_account_id.startswith("act_"):
            self._ad_account_id = f"act_{self._ad_account_id}"
        self._api_version  = api_version    or config.META_API_VERSION
        self._page_id      = page_id        or config.META_PAGE_ID
        self._initialized  = False

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _ensure_init(self) -> None:
        if self._initialized:
            return
        if not self._access_token:
            raise RuntimeError(
                "MetaClient: META_ACCESS_TOKEN not set — pipeline cannot create Meta campaigns."
            )
        from facebook_business.api import FacebookAdsApi
        # Initialise with the access token only. When app_id+app_secret are
        # also passed, the SDK auto-computes appsecret_proof and Meta's API
        # rejects calls if the token wasn't minted under the same app — set
        # META_USE_APP_SECRET_PROOF=true only if your access token is
        # confirmed to have been minted by the configured app.
        if (
            self._app_id and self._app_secret
            and (os.getenv("META_USE_APP_SECRET_PROOF", "").lower() == "true")
        ):
            FacebookAdsApi.init(
                app_id=self._app_id,
                app_secret=self._app_secret,
                access_token=self._access_token,
                api_version=self._api_version or "v21.0",
            )
        else:
            FacebookAdsApi.init(
                access_token=self._access_token,
                api_version=self._api_version or "v21.0",
            )
        self._initialized = True
        log.info("MetaClient initialised (account=%s, api=%s)", self._ad_account_id, self._api_version)

    def _prefixed(self, name: str) -> str:
        if name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    # ── Campaign (one per cohort×geo) ────────────────────────────────────────

    def create_campaign_group(self, name: str) -> str:
        """Create a Meta Campaign (Meta's top-level entity, equivalent to a
        LinkedIn campaign group). Returns the numeric campaign ID as a string.

        Status is PAUSED. Special ad category honors `config.SPECIAL_AD_CATEGORY`
        (defaults to "EMPLOYMENT" for safety).
        """
        self._ensure_init()
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.campaign import Campaign

        name = self._prefixed(name)
        category = (config.SPECIAL_AD_CATEGORY or "NONE").upper()
        special = [category] if category and category != "NONE" else []

        params = {
            Campaign.Field.name:                    name,
            Campaign.Field.objective:               self.DEFAULT_OBJECTIVE,
            Campaign.Field.status:                  Campaign.Status.paused,
            Campaign.Field.special_ad_categories:   special,
            Campaign.Field.buying_type:             "AUCTION",
        }
        try:
            account = AdAccount(self._ad_account_id)
            campaign = account.create_campaign(params=params)
        except Exception as exc:
            log.error("Meta create_campaign_group failed for %r: %s", name, exc)
            raise
        campaign_id = str(campaign["id"])
        log.info("Meta campaign created %s (name=%s, special=%s)", campaign_id, name, special or "none")
        return campaign_id

    # ── Ad Set (one per cohort×geo×angle) ────────────────────────────────────

    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int | None = None,
    ) -> str:
        """Create a Meta Ad Set (the targeting + budget layer, child of a
        Meta Campaign). `targeting` is the dict produced by `MetaInterestResolver`.

        Budget is a $1/day placeholder by default — the human reviewer sets
        the real number in Ads Manager before un-pausing. Pass an explicit
        `daily_budget_cents` only if you really want the agent to set it.

        Returns the numeric ad set ID as a string.
        """
        self._ensure_init()
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.adset import AdSet

        name = self._prefixed(name)
        budget = daily_budget_cents if daily_budget_cents is not None else self.PLACEHOLDER_DAILY_BUDGET_CENTS
        params = {
            AdSet.Field.name:               name,
            AdSet.Field.campaign_id:        campaign_group_id,
            AdSet.Field.daily_budget:       budget,
            AdSet.Field.billing_event:      AdSet.BillingEvent.impressions,
            AdSet.Field.optimization_goal:  AdSet.OptimizationGoal.link_clicks,
            AdSet.Field.bid_strategy:       AdSet.BidStrategy.lowest_cost_without_cap,
            AdSet.Field.targeting:          targeting,
            AdSet.Field.status:             AdSet.Status.paused,
        }
        try:
            account = AdAccount(self._ad_account_id)
            ad_set = account.create_ad_set(params=params)
        except Exception as exc:
            log.error("Meta create_campaign (ad set) failed for %r: %s", name, exc)
            raise
        ad_set_id = str(ad_set["id"])
        log.info("Meta ad set created %s (name=%s, campaign=%s)", ad_set_id, name, campaign_group_id)
        return ad_set_id

    # ── Image upload ─────────────────────────────────────────────────────────

    def upload_image(self, image_path: str | Path) -> str:
        """Upload a PNG; return the image_hash that AdCreative.link_data references."""
        self._ensure_init()
        from facebook_business.adobjects.adaccount import AdAccount
        from facebook_business.adobjects.adimage import AdImage

        image_path = str(Path(image_path))
        try:
            account = AdAccount(self._ad_account_id)
            image = AdImage(parent_id=self._ad_account_id)
            image[AdImage.Field.filename] = image_path
            image.remote_create()
        except Exception as exc:
            log.error("Meta upload_image failed for %s: %s", image_path, exc)
            raise
        image_hash = image[AdImage.Field.hash]
        log.info("Meta image uploaded %s → hash %s", Path(image_path).name, image_hash)
        return image_hash

    # ── Ad creative + ad ─────────────────────────────────────────────────────

    def create_image_ad(
        self,
        campaign_id: str,
        image_id: str,
        headline: str,
        description: str,
        primary_text: Optional[str] = None,
        ad_headline: Optional[str] = None,   # unused — kept for ABC compat
        intro_text: Optional[str] = None,    # unused — kept for ABC compat
        cta: Optional[str] = None,
        destination_url: Optional[str] = None,
    ) -> CreateAdResult:
        """Create AdCreative + Ad referencing the given ad set + image.

        - `campaign_id` is actually the Meta ad_set_id (we kept the kwarg name
          `campaign_id` for ABC consistency).
        - `image_id` is the image_hash returned from `upload_image`.

        Returns CreateAdResult(status="ok") on success. Falls back to
        status="local_fallback" if the page_id isn't configured (so the
        pipeline can save the PNG locally and continue).
        """
        if not self._page_id:
            return CreateAdResult(
                status="local_fallback",
                error_class="ConfigError",
                error_message=(
                    "META_PAGE_ID not set — Meta image-ad creation requires an "
                    "object_story_spec.page_id. Saving PNG locally instead."
                ),
            )
        self._ensure_init()

        try:
            from facebook_business.adobjects.adaccount import AdAccount
            from facebook_business.adobjects.adcreative import AdCreative
            from facebook_business.adobjects.ad import Ad

            cta_type = (cta or "LEARN_MORE").upper()
            if cta_type not in _VALID_META_CTAS:
                cta_type = "LEARN_MORE"

            link_data: dict[str, Any] = {
                "image_hash": image_id,
                "link":       destination_url or config.LINKEDIN_DESTINATION,
                "name":       headline,
                "description": description,
                "call_to_action": {
                    "type": cta_type,
                    "value": {"link": destination_url or config.LINKEDIN_DESTINATION},
                },
            }
            if primary_text:
                link_data["message"] = primary_text

            object_story_spec = {
                "page_id":   self._page_id,
                "link_data": link_data,
            }
            account = AdAccount(self._ad_account_id)

            creative = account.create_ad_creative(params={
                AdCreative.Field.name:              self._prefixed(f"creative_{campaign_id}"),
                AdCreative.Field.object_story_spec: object_story_spec,
            })
            creative_id = str(creative["id"])

            ad = account.create_ad(params={
                Ad.Field.name:        self._prefixed(f"ad_{campaign_id}"),
                Ad.Field.adset_id:    campaign_id,
                Ad.Field.creative:    {"creative_id": creative_id},
                Ad.Field.status:      Ad.Status.paused,
            })
            ad_id = str(ad["id"])
            log.info("Meta ad created %s (ad set=%s, creative=%s)", ad_id, campaign_id, creative_id)
            return CreateAdResult(creative_id=ad_id, status="ok")
        except Exception as exc:
            msg = str(exc)
            upper = msg.upper()
            status: Any = "error"
            if "PAGE" in upper or "PERMISSION" in upper or "OAUTH" in upper:
                status = "local_fallback"
            log.error("Meta create_image_ad failed: %s", exc)
            return CreateAdResult(
                status=status,
                error_class=type(exc).__name__,
                error_message=msg[:500],
            )

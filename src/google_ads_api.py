"""
Google Ads API client (Display ads — Responsive Display Ads).

Implements `AdPlatformClient` so the Outlier campaign pipeline can target
Google Display alongside LinkedIn and Meta.

Mapping of pipeline concepts to Google Ads entities:
  - `create_campaign_group` → Google Ads Campaign (advertising_channel_type
    DISPLAY, status PAUSED, MANUAL_CPC bidding to avoid conversion-tracking
    pre-reqs).
  - `create_campaign`       → Google Ads Ad Group under that Campaign, with
    audience + geo criteria.
  - `upload_image`          → Asset of type IMAGE.
  - `create_image_ad`       → Responsive Display Ad referencing the ad
    group + image asset, with multiple short headlines + 1 long headline +
    multiple descriptions.

Everything PAUSED. Names auto-prefixed with `config.AGENT_NAME_PREFIX`.

Special ad category: when `config.SPECIAL_AD_CATEGORY == "EMPLOYMENT"` the
campaign is created with `advertising_channel_sub_type` and the
`special_ad_category=EMPLOYMENT` field — this restricts demographic
targeting to comply with Google's Personalized Ads policy.
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    GOOGLE_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)


class GoogleAdsClient(AdPlatformClient):
    """Google Ads Display client. Lazy-init: SDK is only imported on the
    first API call."""

    name = "google"
    constraints: PlatformConstraints = GOOGLE_CONSTRAINTS

    AGENT_NAME_PREFIX = config.AGENT_NAME_PREFIX  # "agent_"

    # Placeholder daily budget + per-click cap. Google Ads requires both for
    # a Display campaign + ad group — but the pipeline only creates
    # PAUSED/DRAFT entities. The reviewer sets real values in the Ads UI
    # before un-pausing. Use $1/day budget + $0.10 cap so no real spend
    # can occur even if accidentally activated.
    PLACEHOLDER_DAILY_BUDGET_MICROS = 1_000_000     # $1/day
    PLACEHOLDER_CPC_BID_MICROS      =   100_000     # $0.10 max-CPC

    def __init__(
        self,
        client_id:           Optional[str] = None,
        client_secret:       Optional[str] = None,
        developer_token:     Optional[str] = None,
        refresh_token:       Optional[str] = None,
        customer_id:         Optional[str] = None,
        login_customer_id:   Optional[str] = None,
    ):
        self._client_id          = client_id          or config.GOOGLE_ADS_CLIENT_ID
        self._client_secret      = client_secret      or config.GOOGLE_ADS_CLIENT_SECRET
        self._developer_token    = developer_token    or config.GOOGLE_ADS_DEVELOPER_TOKEN
        self._refresh_token      = refresh_token      or config.GOOGLE_ADS_REFRESH_TOKEN
        self._customer_id        = customer_id        or config.GOOGLE_ADS_CUSTOMER_ID
        self._login_customer_id  = login_customer_id  or config.GOOGLE_ADS_LOGIN_CUSTOMER_ID
        self._client = None  # lazy

    # ── Lifecycle ────────────────────────────────────────────────────────────

    def _ensure_client(self):
        if self._client is not None:
            return self._client
        creds = {
            "developer_token":    self._developer_token,
            "refresh_token":      self._refresh_token,
            "client_id":          self._client_id,
            "client_secret":      self._client_secret,
            "use_proto_plus":     True,
        }
        if self._login_customer_id:
            creds["login_customer_id"] = str(self._login_customer_id).replace("-", "")
        from google.ads.googleads.client import GoogleAdsClient as _SDKClient
        self._client = _SDKClient.load_from_dict(creds)
        log.info("GoogleAdsClient initialised (customer=%s, login=%s)",
                 self._customer_id, self._login_customer_id or "none")
        return self._client

    def _prefixed(self, name: str) -> str:
        if name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    @property
    def _customer_id_str(self) -> str:
        # Google customer IDs are passed as 10-digit strings (no dashes).
        return str(self._customer_id).replace("-", "")

    # ── Campaign (one per cohort×geo) ────────────────────────────────────────

    def create_campaign_group(self, name: str) -> str:
        """Create a Google Ads Campaign (Display channel, PAUSED, MANUAL_CPC).

        Returns the Google Ads campaign resource name (e.g.
        `customers/1234567890/campaigns/9876543210`).
        """
        client = self._ensure_client()
        name = self._prefixed(name)
        category = (config.SPECIAL_AD_CATEGORY or "NONE").upper()

        # 1. Create a budget — required parent for the campaign. The amount
        # is a $1/day placeholder; pipeline only creates PAUSED campaigns,
        # the reviewer sets real numbers in the Ads UI before un-pausing.
        budget_service = client.get_service("CampaignBudgetService")
        budget_op = client.get_type("CampaignBudgetOperation")
        budget = budget_op.create
        budget.name = self._prefixed(f"budget_{name}")
        budget.amount_micros = self.PLACEHOLDER_DAILY_BUDGET_MICROS
        budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
        budget.explicitly_shared = False
        budget_resp = budget_service.mutate_campaign_budgets(
            customer_id=self._customer_id_str,
            operations=[budget_op],
        )
        budget_resource = budget_resp.results[0].resource_name

        # 2. Create the campaign.
        campaign_service = client.get_service("CampaignService")
        campaign_op = client.get_type("CampaignOperation")
        c = campaign_op.create
        c.name = name
        c.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.DISPLAY
        c.status = client.enums.CampaignStatusEnum.PAUSED
        c.campaign_budget = budget_resource
        c.manual_cpc.enhanced_cpc_enabled = False
        # EU political advertising declaration — required field in Google
        # Ads v21+ (2026-05-08 verified). Outlier's tasking ads are not
        # political; explicitly declare DOES_NOT_CONTAIN.
        try:
            c.contains_eu_political_advertising = (
                client.enums.EuPoliticalAdvertisingStatusEnum
                .DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
            )
        except Exception as exc:  # noqa: BLE001 — defensive on SDK churn
            log.warning("Could not set contains_eu_political_advertising (%s) — "
                        "campaign create will fail if Google requires it.", exc)
        # Special Ad Category was removed from the Campaign proto in
        # google-ads v21+ (2026-05-08 verified — no `special_ad_category` or
        # `SpecialAdCategoryEnum` attribute exists on the SDK any more). For
        # EMPLOYMENT campaigns the reviewer must set the category manually
        # in Google Ads Manager before activating. Keep the call resilient
        # so the rest of the arm proceeds.
        if category in ("EMPLOYMENT", "HOUSING", "CREDIT"):
            sac_enum = getattr(client.enums, "SpecialAdCategoryEnum", None)
            target = sac_enum and getattr(sac_enum, category, None)
            if target is not None and hasattr(c, "special_ad_category"):
                try:
                    c.special_ad_category = target
                except Exception as exc:  # noqa: BLE001 — defensive
                    log.warning(
                        "Google special_ad_category=%s could not be set "
                        "(SDK rejected assignment: %s) — reviewer must set "
                        "it manually in Ads Manager before activating.",
                        category, exc,
                    )
            else:
                log.warning(
                    "Google special_ad_category=%s requested but SDK "
                    "(google-ads >=22) no longer exposes the field. Reviewer "
                    "must set the category manually in Ads Manager before "
                    "activating.",
                    category,
                )

        try:
            resp = campaign_service.mutate_campaigns(
                customer_id=self._customer_id_str,
                operations=[campaign_op],
            )
        except Exception as exc:
            log.error("Google create_campaign_group failed for %r: %s", name, exc)
            raise
        resource = resp.results[0].resource_name
        log.info("Google campaign created %s (name=%s, special=%s)", resource, name, category)
        return resource

    # ── Ad Group (one per cohort×geo×angle) ──────────────────────────────────

    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int | None = None,
    ) -> str:
        """Create a Google Ads Ad Group under the given campaign.

        `targeting` is the dict from `GoogleSegmentResolver`. Audience
        criteria (in-market / affinity segments + demographics) are attached
        to the ad group via separate AdGroupCriterion mutations.

        CPC bid is a placeholder; pipeline only creates PAUSED ad groups.
        """
        client = self._ensure_client()
        name = self._prefixed(name)

        ad_group_service = client.get_service("AdGroupService")
        ad_group_op = client.get_type("AdGroupOperation")
        ag = ad_group_op.create
        ag.name = name
        ag.campaign = campaign_group_id
        ag.status = client.enums.AdGroupStatusEnum.PAUSED
        ag.type_ = client.enums.AdGroupTypeEnum.DISPLAY_STANDARD
        # MANUAL_CPC ad groups need a max-CPC; placeholder $0.10 so no real
        # spend can occur even if the ad group is accidentally activated.
        ag.cpc_bid_micros = self.PLACEHOLDER_CPC_BID_MICROS

        try:
            resp = ad_group_service.mutate_ad_groups(
                customer_id=self._customer_id_str,
                operations=[ad_group_op],
            )
        except Exception as exc:
            log.error("Google create_campaign (ad group) failed for %r: %s", name, exc)
            raise
        ad_group_resource = resp.results[0].resource_name
        log.info("Google ad group created %s (campaign=%s)", ad_group_resource, campaign_group_id)

        # Apply audience criteria (best-effort — failure here doesn't kill the
        # ad group; it just runs with broader targeting).
        self._apply_audience_criteria(ad_group_resource, targeting)
        return ad_group_resource

    def _apply_audience_criteria(self, ad_group_resource: str, targeting: dict[str, Any]) -> None:
        try:
            client = self._ensure_client()
            criterion_service = client.get_service("AdGroupCriterionService")
            ops = []
            for seg_resource in targeting.get("audience_segments", []) or []:
                op = client.get_type("AdGroupCriterionOperation")
                c = op.create
                c.ad_group = ad_group_resource
                c.user_interest.user_interest_category = seg_resource
                ops.append(op)
            if ops:
                criterion_service.mutate_ad_group_criteria(
                    customer_id=self._customer_id_str,
                    operations=ops,
                )
                log.info("Attached %d audience segments to %s", len(ops), ad_group_resource)
        except Exception as exc:
            # Audience-segment criteria are nice-to-have; geo + Display-network
            # defaults still produce a reachable audience without them.
            log.warning("Google audience criteria attach failed (non-fatal): %s", exc)

    # ── Image asset ──────────────────────────────────────────────────────────

    def upload_image(self, image_path: str | Path) -> str:
        client = self._ensure_client()
        path = Path(image_path)
        with open(path, "rb") as fh:
            image_bytes = fh.read()
        asset_service = client.get_service("AssetService")
        op = client.get_type("AssetOperation")
        a = op.create
        a.name = self._prefixed(f"img_{path.stem}")
        a.type_ = client.enums.AssetTypeEnum.IMAGE
        a.image_asset.data = image_bytes
        try:
            resp = asset_service.mutate_assets(
                customer_id=self._customer_id_str,
                operations=[op],
            )
        except Exception as exc:
            log.error("Google upload_image failed for %s: %s", path.name, exc)
            raise
        resource = resp.results[0].resource_name
        log.info("Google image asset uploaded %s → %s", path.name, resource)
        return resource

    # ── Responsive Display Ad ────────────────────────────────────────────────

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
        # Google-specific extras (passed through from copy_adapter):
        headlines: Optional[list[str]] = None,
        long_headline: Optional[str] = None,
        descriptions: Optional[list[str]] = None,
    ) -> CreateAdResult:
        """Create a Responsive Display Ad. `campaign_id` is actually the
        Google ad_group resource name (we kept the kwarg name for ABC
        consistency).

        Required RDA inputs:
          - headlines:        list of 3-5 short headlines (each ≤30 chars)
          - long_headline:    one long headline (≤90 chars)
          - descriptions:     list of 1-5 descriptions (each ≤90 chars)
          - marketing_image:  resource name from `upload_image`
          - business_name:    "Outlier"
          - final_urls:       list of landing pages
        """
        try:
            client = self._ensure_client()

            ad_service = client.get_service("AdGroupAdService")
            op = client.get_type("AdGroupAdOperation")
            aga = op.create
            aga.ad_group = campaign_id
            aga.status = client.enums.AdGroupAdStatusEnum.PAUSED

            ad = aga.ad
            ad.name = self._prefixed(f"rda_{Path(image_id).name}")
            ad.final_urls.append(destination_url or config.LINKEDIN_DESTINATION)

            rda = ad.responsive_display_ad
            rda.business_name = "Outlier"

            for h in (headlines or [headline])[:5]:
                if not h:
                    continue
                ah = client.get_type("AdTextAsset")
                ah.text = h
                rda.headlines.append(ah)

            ah_long = client.get_type("AdTextAsset")
            ah_long.text = long_headline or headline
            rda.long_headline = ah_long

            for d in (descriptions or [description])[:5]:
                if not d:
                    continue
                ad_desc = client.get_type("AdTextAsset")
                ad_desc.text = d
                rda.descriptions.append(ad_desc)

            img_asset = client.get_type("AdImageAsset")
            img_asset.asset = image_id
            rda.marketing_images.append(img_asset)

            try:
                resp = ad_service.mutate_ad_group_ads(
                    customer_id=self._customer_id_str,
                    operations=[op],
                )
            except Exception as exc:
                log.error("Google create_image_ad failed: %s", exc)
                msg = str(exc)
                upper = msg.upper()
                status: Any = "error"
                if "REQUIRED" in upper or "INVALID" in upper or "QUOTA" in upper:
                    status = "local_fallback"
                return CreateAdResult(
                    status=status,
                    error_class=type(exc).__name__,
                    error_message=msg[:500],
                )

            resource = resp.results[0].resource_name
            log.info("Google RDA created %s (ad group=%s)", resource, campaign_id)
            return CreateAdResult(creative_id=resource, status="ok")
        except Exception as exc:
            log.error("Google create_image_ad outer failure: %s", exc)
            return CreateAdResult(
                status="error",
                error_class=type(exc).__name__,
                error_message=str(exc)[:500],
            )

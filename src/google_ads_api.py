"""
Google Ads API client (Display ads — Responsive Display Ads).

Implements `AdPlatformClient` so the Outlier campaign pipeline can target
Google Display alongside LinkedIn and Meta.

Mapping of pipeline concepts to Google Ads entities:
  - `create_campaign_group` → Google Ads Campaign (advertising_channel_type
    DISPLAY, status PAUSED, MAXIMIZE_CONVERSIONS bidding — Diego confirmed
    2026-05-22 that conversion tracking is configured on customer
    8840244968. Override via GOOGLE_BID_STRATEGY=MANUAL_CPC env var.).
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

    # Default daily budget when no console override is supplied. Per Diego
    # (2026-05-22): $100/day for new campaigns. Pipeline still creates
    # PAUSED/DRAFT entities, so this only takes effect once the reviewer
    # un-pauses in the Ads UI. Override per-campaign via decision.budgets
    # in the console UI.
    PLACEHOLDER_DAILY_BUDGET_MICROS = 100_000_000   # $100/day
    PLACEHOLDER_CPC_BID_MICROS      =     100_000   # $0.10 max-CPC (Manual CPC fallback only)

    def __init__(
        self,
        client_id:           Optional[str] = None,
        client_secret:       Optional[str] = None,
        developer_token:     Optional[str] = None,
        refresh_token:       Optional[str] = None,
        customer_id:         Optional[str] = None,
        login_customer_id:   Optional[str] = None,
        channel:             str = "display",
    ):
        self._client_id          = client_id          or config.GOOGLE_ADS_CLIENT_ID
        self._client_secret      = client_secret      or config.GOOGLE_ADS_CLIENT_SECRET
        self._developer_token    = developer_token    or config.GOOGLE_ADS_DEVELOPER_TOKEN
        self._refresh_token      = refresh_token      or config.GOOGLE_ADS_REFRESH_TOKEN
        self._customer_id        = customer_id        or config.GOOGLE_ADS_CUSTOMER_ID
        self._login_customer_id  = login_customer_id  or config.GOOGLE_ADS_LOGIN_CUSTOMER_ID
        # 2026-05-24 — channel toggle. "display" preserves the legacy
        # Responsive Display Ad path (RDA + audience segments). "search"
        # creates Search campaigns with Responsive Search Ads + keyword
        # criteria, mirroring Diego's manual Search campaigns surfaced by
        # the 2026-05-24 live-account audit.
        ch = (channel or "display").strip().lower()
        if ch not in ("display", "search"):
            raise ValueError(f"GoogleAdsClient channel must be 'display' or 'search', got {channel!r}")
        self._channel = ch
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

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        """Create a Google Ads Campaign. Channel is `self._channel` (DISPLAY
        or SEARCH). Status PAUSED, bidding MAXIMIZE_CONVERSIONS by default.

        Returns the Google Ads campaign resource name (e.g.
        `customers/1234567890/campaigns/9876543210`).

        `geos` is accepted for interface parity with the Meta arm (which sets
        special_ad_category_country at this level). Google Ads scopes geo at
        the criterion level, not the campaign level, so it's ignored here.
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
        c.advertising_channel_type = (
            client.enums.AdvertisingChannelTypeEnum.SEARCH
            if self._channel == "search"
            else client.enums.AdvertisingChannelTypeEnum.DISPLAY
        )
        c.status = client.enums.CampaignStatusEnum.PAUSED
        c.campaign_budget = budget_resource
        # Bidding strategy. Default: MAXIMIZE_CONVERSIONS (Diego, 2026-05-22:
        # "Google Ads default bidding should be maximize conversions with no
        # more restrictions"). Requires conversion tracking on the customer
        # account — Diego confirmed it's set up on 8840244968. To roll back
        # to the pre-2026-05-22 placeholder behavior set
        # GOOGLE_BID_STRATEGY=MANUAL_CPC in the env.
        import os as _os
        _bid_strategy = (_os.getenv("GOOGLE_BID_STRATEGY") or "MAXIMIZE_CONVERSIONS").upper()
        if _bid_strategy == "MANUAL_CPC":
            c.manual_cpc.enhanced_cpc_enabled = False
        else:
            # Setting any sub-field of the maximize_conversions oneof
            # activates that strategy. target_cpa_micros=0 = no target
            # CPA, pure conversion maximization.
            c.maximize_conversions.target_cpa_micros = 0
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
        ag.type_ = (
            client.enums.AdGroupTypeEnum.SEARCH_STANDARD
            if self._channel == "search"
            else client.enums.AdGroupTypeEnum.DISPLAY_STANDARD
        )
        # Ad-group max-CPC is only set when the parent campaign uses
        # MANUAL_CPC. MAXIMIZE_CONVERSIONS manages bids automatically and
        # rejects ad groups that pin cpc_bid_micros.
        import os as _os
        if (_os.getenv("GOOGLE_BID_STRATEGY") or "MAXIMIZE_CONVERSIONS").upper() == "MANUAL_CPC":
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

        # Apply targeting criteria (best-effort — failure doesn't kill the
        # ad group; it just runs with broader targeting). Display uses
        # audience segments; Search uses keyword criteria.
        if self._channel == "search":
            self._apply_keyword_criteria(ad_group_resource, targeting)
        else:
            self._apply_audience_criteria(ad_group_resource, targeting)
        return ad_group_resource

    def update_campaign_budget(
        self,
        campaign_id: str,
        daily_budget_cents: int,
    ) -> None:
        """Phase 7 — update a Google Ads Campaign's daily budget.

        Google budgets live on the Campaign (not the Ad Group), and via a
        SHARED CampaignBudget resource that the Campaign references. We:
          1. GET the campaign to resolve its campaign_budget resource_name
          2. PATCH that CampaignBudget's amount_micros via
             CampaignBudgetService.mutate_campaign_budgets

        `campaign_id` accepts either the bare numeric campaign id or the full
        resource name `customers/{cust}/campaigns/{id}` (matches what
        `create_campaign_group` returns).

        Conversion: $1 = 1,000,000 micros = 100 cents.
                    amount_micros = daily_budget_cents * 10_000

        daily_budget_cents=0 is allowed by the API; Google interprets it as
        "campaign cannot spend". The reviewer can then bump it back up via
        the UI to resume delivery (or this method, called from the console).
        """
        if daily_budget_cents < 0:
            raise ValueError(f"daily_budget_cents must be ≥ 0, got {daily_budget_cents}")
        client = self._ensure_client()

        # Resolve the campaign_budget resource_name. Caller may have passed
        # either the full resource name or the bare numeric id.
        if campaign_id.startswith("customers/"):
            campaign_resource = campaign_id
        else:
            campaign_resource = (
                f"customers/{self._customer_id_str}/campaigns/{campaign_id}"
            )

        ga_service = client.get_service("GoogleAdsService")
        query = (
            "SELECT campaign.campaign_budget "
            "FROM campaign "
            f"WHERE campaign.resource_name = '{campaign_resource}' "
            "LIMIT 1"
        )
        try:
            stream = ga_service.search(
                customer_id=self._customer_id_str,
                query=query,
            )
            budget_resource: str | None = None
            for row in stream:
                budget_resource = row.campaign.campaign_budget
                break
        except Exception as exc:
            log.error(
                "Google update_campaign_budget search failed for %s: %s",
                campaign_resource, exc,
            )
            raise

        if not budget_resource:
            raise RuntimeError(
                f"Google campaign {campaign_resource} has no campaign_budget "
                "resource — cannot update budget.",
            )

        # PATCH the CampaignBudget. update_mask must list every field we set.
        budget_service = client.get_service("CampaignBudgetService")
        op = client.get_type("CampaignBudgetOperation")
        b = op.update
        b.resource_name = budget_resource
        b.amount_micros = int(daily_budget_cents) * 10_000

        try:
            from google.api_core import protobuf_helpers
        except Exception:
            protobuf_helpers = None  # type: ignore[assignment]

        if protobuf_helpers is not None:
            op.update_mask.CopyFrom(
                protobuf_helpers.field_mask(None, b._pb)
            )
        else:
            # Fallback for environments without google.api_core helpers.
            op.update_mask.paths.append("amount_micros")

        try:
            resp = budget_service.mutate_campaign_budgets(
                customer_id=self._customer_id_str,
                operations=[op],
            )
        except Exception as exc:
            log.error(
                "Google mutate_campaign_budgets failed for %s: %s",
                budget_resource, exc,
            )
            raise

        updated_resource = resp.results[0].resource_name
        log.info(
            "Google campaign %s budget %s → %d cents/day (%d micros)",
            campaign_resource, updated_resource,
            daily_budget_cents, daily_budget_cents * 10_000,
        )

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

    def _apply_keyword_criteria(self, ad_group_resource: str, targeting: dict[str, Any]) -> None:
        """Attach keyword criteria to a Search ad-group.

        Reads `targeting["keyword_ideas"]` (populated by GoogleSegmentResolver
        via KeywordPlanIdeaService.generate_keyword_ideas). Each keyword is
        attached with PHRASE match by default — captures variations (singular/
        plural, word-order tweaks) without going full-broad. Mirrors Diego's
        manual Search campaigns surfaced by the 2026-05-24 audit which use
        ~30 keywords per ad-group with phrase match.

        Cap at 30 keywords/ad-group. Mass-mutate in one API call.
        """
        try:
            client = self._ensure_client()
            criterion_service = client.get_service("AdGroupCriterionService")
            kws = (targeting or {}).get("keyword_ideas") or []
            if not kws:
                log.info("Search ad-group %s has no keyword_ideas to attach", ad_group_resource)
                return
            ops = []
            for kw in kws[:30]:
                if not (kw or "").strip():
                    continue
                op = client.get_type("AdGroupCriterionOperation")
                c = op.create
                c.ad_group = ad_group_resource
                c.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
                c.keyword.text = str(kw).strip()
                c.keyword.match_type = client.enums.KeywordMatchTypeEnum.PHRASE
                ops.append(op)
            if ops:
                criterion_service.mutate_ad_group_criteria(
                    customer_id=self._customer_id_str,
                    operations=ops,
                )
                log.info("Attached %d keyword criteria to %s", len(ops), ad_group_resource)
        except Exception as exc:
            log.warning("Google keyword criteria attach failed (non-fatal): %s", exc)

    # ── Reach estimation (pre-campaign audience check) ──────────────────────

    def get_reach_estimate(self, targeting: dict[str, Any]) -> Optional[int]:
        """Return a Google-reported audience estimate for the given targeting.

        Google Ads' Reach Planner (ReachPlanService.GenerateReachForecast) is
        the canonical API but requires a prepared CampaignDuration + product
        list and isn't trivially callable mid-pipeline. As a pragmatic
        substitute we estimate via segment sizes pulled from the
        GoogleAdsService — summing `user_list.size_for_display` across the
        targeting's audience_segments. Returns None on any failure to signal
        `audience_check` should skip the gate (rather than blocking creation).

        Caveats:
          - Sums are upper-bound and double-count overlapping segments.
          - Geo filters aren't applied — Google segment sizes are usually
            country-agnostic anyway.
          - Returns None on the typical "Standard Access required" auth
            error so the gate doesn't block live runs while access is pending.
        """
        # 2026-05-25: Search arm reach estimate via keyword_volume_estimate
        # (sum of avg_monthly_searches across cohort's keyword pool). Used
        # when audience_segments is empty — common because Google's
        # user_interest taxonomy lacks most technical/professional segments
        # (java / deep learning / video editor → 0 segments). Not a true
        # audience size, but the closest numeric signal Google provides for
        # keyword-targeted campaigns. Read directly from the targeting dict
        # — no Google Ads client call needed. Display arm continues to use
        # the user_list summation below.
        if self._channel == "search":
            kw_vol = int((targeting or {}).get("keyword_volume_estimate") or 0)
            if kw_vol > 0:
                log.info(
                    "Google Search reach estimate: %d monthly searches "
                    "across cohort keyword pool",
                    kw_vol,
                )
                return kw_vol

        try:
            client = self._ensure_client()
        except Exception as exc:
            log.warning("Google get_reach_estimate: client init failed: %s", exc)
            return None

        segs = (targeting or {}).get("audience_segments") or []
        if not segs:
            return None

        total = 0
        try:
            ga_service = client.get_service("GoogleAdsService")
            # Resource names look like `customers/{cid}/userLists/{id}` or
            # `customers/{cid}/audiences/{id}`. We query UserList for the
            # in-market / affinity / custom segments the pipeline attaches.
            ids = []
            for s in segs:
                if isinstance(s, str) and "/userLists/" in s:
                    ids.append(s.rsplit("/", 1)[-1])
            if not ids:
                return None
            query = (
                "SELECT user_list.size_for_display "
                "FROM user_list "
                f"WHERE user_list.id IN ({','.join(ids)})"
            )
            for row in ga_service.search(
                customer_id=self._customer_id_str, query=query,
            ):
                size = getattr(row.user_list, "size_for_display", 0) or 0
                total += int(size)
            return total or None
        except Exception as exc:
            log.warning("Google get_reach_estimate failed: %s — skipping gate", exc)
            return None

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

    def upload_image_landscape(self, square_image_path: str | Path,
                               width: int = 1200, height: int = 628) -> str:
        """Generate a 1.91:1 landscape variant from a 1:1 source and upload.

        Google RDA requires BOTH `marketing_images` (1.91:1) AND
        `square_marketing_images` (1:1). The Outlier pipeline only produces
        1:1 sources (Gemini's default). To satisfy `marketing_images`
        without compromising the composed text + subject (which a center-crop
        would chop), we **pillarbox**: scale the square to fit the 628px
        height, paint centered on a 1200×628 white canvas. All original
        content is preserved; white side-bars are unobtrusive in Google's
        display placements.

        Returns the uploaded asset resource_name.
        """
        from PIL import Image
        src_path = Path(square_image_path)
        with Image.open(src_path) as src:
            src = src.convert("RGB")
            scale = height / src.height
            scaled_w = max(1, int(src.width * scale))
            scaled = src.resize((scaled_w, height), Image.LANCZOS)
            canvas = Image.new("RGB", (width, height), "white")
            offset_x = max(0, (width - scaled_w) // 2)
            # If the scaled width exceeds canvas width (source was already
            # wider than 1.91:1), crop the scaled image horizontally instead.
            if scaled_w > width:
                left = (scaled_w - width) // 2
                scaled = scaled.crop((left, 0, left + width, height))
                offset_x = 0
            canvas.paste(scaled, (offset_x, 0))
            landscape_path = src_path.with_name(
                f"{src_path.stem}_landscape{width}x{height}.png"
            )
            canvas.save(landscape_path, "PNG", optimize=True)
        return self.upload_image(landscape_path)

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
        # Aspect-ratio support (added 2026-05-18): Google RDA requires BOTH
        # 1.91:1 (marketing_images) AND 1:1 (square_marketing_images). When
        # `local_png_path` is provided, the function auto-generates a 1.91:1
        # pillarboxed variant and uploads both. Without it, only `image_id`
        # is attached and the create call WILL fail on aspect mismatch — kept
        # as a defensive default so callers that hand-roll uploads don't break.
        local_png_path: Optional[str] = None,
    ) -> CreateAdResult:
        """Create a Responsive Display Ad. `campaign_id` is actually the
        Google ad_group resource name (we kept the kwarg name for ABC
        consistency).

        Required RDA inputs:
          - headlines:               3-5 short headlines (each ≤30 chars)
          - long_headline:           one long headline (≤90 chars)
          - descriptions:            1-5 descriptions (each ≤90 chars)
          - marketing_images:        1.91:1, derived from local_png_path
          - square_marketing_images: 1:1, from `image_id` (or local_png_path)
          - business_name:           "Outlier"
          - final_urls:              list of landing pages
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

            # Per Google Ads API spec, `final_urls` stores only the destination
            # URL (no tracking params), and tracking params go in
            # `final_url_suffix`. If we pass a URL with `?utm_...` in
            # final_urls, Google strips the query string during storage and
            # the UTM tracking is lost. Split here so:
            #   final_urls         = ['https://outlier.ai/experts/qfinance']
            #   final_url_suffix   = 'utm_source=Google&utm_medium=paid&...'
            # This matches the canonical Google Ads pattern and keeps both
            # the policy-validated destination AND the marketing team's UTM
            # tracking intact.
            from urllib.parse import urlsplit, urlunsplit
            _final = destination_url or config.LINKEDIN_DESTINATION
            _parts = urlsplit(_final)
            _base = urlunsplit((_parts.scheme, _parts.netloc, _parts.path, "", ""))
            ad.final_urls.append(_base)
            if _parts.query:
                ad.final_url_suffix = _parts.query

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

            # Auto-generate the 1.91:1 landscape variant when given a source
            # PNG; without it we'd fail ASPECT_RATIO_NOT_ALLOWED at create-time.
            landscape_id: Optional[str] = None
            if local_png_path:
                try:
                    landscape_id = self.upload_image_landscape(local_png_path)
                except Exception as exc:
                    log.warning(
                        "Google upload_image_landscape failed for %s — falling "
                        "back to source image_id in both slots (will likely hit "
                        "ASPECT_RATIO_NOT_ALLOWED): %s",
                        local_png_path, exc,
                    )

            # marketing_images: 1.91:1. Use the generated landscape if we
            # have one; otherwise fall back to the source (legacy behaviour
            # — will fail aspect check on Google's side but the rest of the
            # call is preserved for diagnostic clarity).
            landscape_asset = client.get_type("AdImageAsset")
            landscape_asset.asset = landscape_id or image_id
            rda.marketing_images.append(landscape_asset)

            # square_marketing_images: 1:1. Always the source image_id, which
            # the Outlier pipeline produces natively as 1024×1024 / 1480×1480.
            square_asset = client.get_type("AdImageAsset")
            square_asset.asset = image_id
            rda.square_marketing_images.append(square_asset)

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

    # ── Responsive Search Ad ─────────────────────────────────────────────────

    def create_search_ad(
        self,
        ad_group_resource: str,
        *,
        headlines: list[str],
        descriptions: list[str],
        destination_url: Optional[str] = None,
        final_url_path: Optional[str] = None,
    ) -> CreateAdResult:
        """Create a Responsive Search Ad in the given ad-group.

        Search ads are text-only — no image asset needed (unlike RDA).
        Required RSA inputs:
          - headlines:      3-15 short text assets, each ≤30 chars
          - descriptions:   2-4 text assets, each ≤90 chars
          - final_urls:     destination URL (query string moves to suffix)

        Status PAUSED so Diego/Bryan eyeball before un-pausing. Mirrors
        Diego's manual Search campaigns from the 2026-05-24 audit.
        """
        try:
            client = self._ensure_client()
            ad_service = client.get_service("AdGroupAdService")
            op = client.get_type("AdGroupAdOperation")
            aga = op.create
            aga.ad_group = ad_group_resource
            aga.status = client.enums.AdGroupAdStatusEnum.PAUSED

            ad = aga.ad

            # Split destination_url so UTM query string lives in final_url_suffix
            # (Google strips ?utm_* from final_urls — same gotcha as RDA path).
            from urllib.parse import urlsplit, urlunsplit
            _final = destination_url or config.LINKEDIN_DESTINATION
            _parts = urlsplit(_final)
            _base = urlunsplit((_parts.scheme, _parts.netloc, _parts.path, "", ""))
            ad.final_urls.append(_base)
            if _parts.query:
                ad.final_url_suffix = _parts.query

            rsa = ad.responsive_search_ad
            # Headlines — Google needs 3-15. Truncate each to 30 chars.
            valid_headlines = [h.strip() for h in (headlines or []) if h and h.strip()]
            if len(valid_headlines) < 3:
                return CreateAdResult(
                    status="error",
                    error_class="InsufficientHeadlines",
                    error_message=f"RSA needs ≥3 headlines, got {len(valid_headlines)}",
                )
            for h in valid_headlines[:15]:
                ah = client.get_type("AdTextAsset")
                ah.text = h[:30]
                rsa.headlines.append(ah)

            # Descriptions — Google needs 2-4. Truncate each to 90 chars.
            valid_descs = [d.strip() for d in (descriptions or []) if d and d.strip()]
            if len(valid_descs) < 2:
                return CreateAdResult(
                    status="error",
                    error_class="InsufficientDescriptions",
                    error_message=f"RSA needs ≥2 descriptions, got {len(valid_descs)}",
                )
            for d in valid_descs[:4]:
                ad_desc = client.get_type("AdTextAsset")
                ad_desc.text = d[:90]
                rsa.descriptions.append(ad_desc)

            try:
                resp = ad_service.mutate_ad_group_ads(
                    customer_id=self._customer_id_str,
                    operations=[op],
                )
            except Exception as exc:
                msg = str(exc)[:500]
                log.error("Google create_search_ad mutate failed (ag=%s): %s",
                          ad_group_resource, msg)
                return CreateAdResult(
                    status="error",
                    error_class=type(exc).__name__,
                    error_message=msg,
                )

            resource = resp.results[0].resource_name
            log.info("Google RSA created %s (ad group=%s, %d headlines, %d descriptions)",
                     resource, ad_group_resource,
                     len(valid_headlines[:15]), len(valid_descs[:4]))
            return CreateAdResult(creative_id=resource, status="ok")
        except Exception as exc:
            log.error("Google create_search_ad outer failure: %s", exc)
            return CreateAdResult(
                status="error",
                error_class=type(exc).__name__,
                error_message=str(exc)[:500],
            )

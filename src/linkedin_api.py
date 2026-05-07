"""
LinkedIn Marketing API client.
  Stage C  — Audience Counts validation
  Campaign — Create campaign + campaign group
  Creative — Upload image + create adCreative + attach to campaign
  Auth     — Auto-refresh access token on 401 using refresh token
"""
import logging
import mimetypes
import os
import re
from pathlib import Path
from typing import Any, Literal, Optional

import requests

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    LINKEDIN_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)


# Back-compat alias — `ImageAdResult` was the original LinkedIn-only return
# type; `CreateAdResult` is the platform-agnostic version with identical
# semantics. The alias keeps existing call sites and tests working unchanged
# (`ImageAdResult(creative_urn=...)`, `result.creative_urn`).
ImageAdResult = CreateAdResult

_LINKEDIN_TOKEN_URL = "https://www.linkedin.com/oauth/v2/accessToken"
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"


def refresh_access_token(
    refresh_token: str | None = None,
    client_id: str | None = None,
    client_secret: str | None = None,
) -> str:
    """
    Exchange a refresh token for a new LinkedIn access token.
    Writes the new LINKEDIN_ACCESS_TOKEN back to .env automatically.
    Returns the new access token.
    """
    refresh_token  = refresh_token  or config.LINKEDIN_REFRESH_TOKEN
    client_id      = client_id      or config.LINKEDIN_CLIENT_ID
    client_secret  = client_secret  or config.LINKEDIN_CLIENT_SECRET

    if not all([refresh_token, client_id, client_secret]):
        raise RuntimeError(
            "Cannot refresh LinkedIn token — LINKEDIN_REFRESH_TOKEN, "
            "LINKEDIN_CLIENT_ID, and LINKEDIN_CLIENT_SECRET must all be set."
        )

    resp = requests.post(_LINKEDIN_TOKEN_URL, data={
        "grant_type":    "refresh_token",
        "refresh_token": refresh_token,
        "client_id":     client_id,
        "client_secret": client_secret,
    })
    if not resp.ok:
        log.error("Token refresh failed %d: %s", resp.status_code, resp.text[:300])
        resp.raise_for_status()

    data        = resp.json()
    new_token   = data["access_token"]
    new_refresh = data.get("refresh_token", refresh_token)  # LinkedIn may rotate it

    # Persist back to .env so the next process startup picks it up
    _update_env_token(new_token, new_refresh)
    log.info("LinkedIn access token refreshed and written to .env")
    return new_token


def _update_env_token(new_access: str, new_refresh: str) -> None:
    """Overwrite LINKEDIN_ACCESS_TOKEN (and optionally LINKEDIN_REFRESH_TOKEN) in .env."""
    if not _ENV_FILE.exists():
        return
    text = _ENV_FILE.read_text()
    text = re.sub(r"^LINKEDIN_ACCESS_TOKEN=.*$",  f"LINKEDIN_ACCESS_TOKEN={new_access}",  text, flags=re.MULTILINE)
    text = re.sub(r"^LINKEDIN_REFRESH_TOKEN=.*$", f"LINKEDIN_REFRESH_TOKEN={new_refresh}", text, flags=re.MULTILINE)
    _ENV_FILE.write_text(text)


class LinkedInClient(AdPlatformClient):
    """LinkedIn Marketing API client. Implements `AdPlatformClient` so the
    pipeline can dispatch generically across LinkedIn, Meta, and Google. The
    method signatures retain LinkedIn-specific kwarg names (campaign_urn,
    image_urn, cta_button, ...) for backward compatibility with existing
    call sites — Python's ABC contract is satisfied as long as the method
    names match."""

    name = "linkedin"
    constraints: PlatformConstraints = LINKEDIN_CONSTRAINTS

    def __init__(self, token: str):
        self._token = token
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "LinkedIn-Version": config.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        })

    # ── helpers ───────────────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{config.LINKEDIN_API_BASE}/{path.lstrip('/')}"

    def _raise_for_status(self, resp: requests.Response, context: str) -> None:
        if not resp.ok:
            log.error("%s failed %d: %s", context, resp.status_code, resp.text[:500])
            resp.raise_for_status()

    def _refresh_and_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Refresh the access token and retry a failed request once."""
        new_token = refresh_access_token()
        self._token = new_token
        self._session.headers.update({"Authorization": f"Bearer {new_token}"})
        return self._session.request(method, url, **kwargs)

    def _default_headers(self) -> dict:
        """Return a copy of the default request headers for one-off calls that bypass _session."""
        return {
            "Authorization":             f"Bearer {self._token}",
            "LinkedIn-Version":          config.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type":              "application/json",
        }

    def _req(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request; auto-refresh and retry once on 401."""
        resp = self._session.request(method, url, **kwargs)
        if resp.status_code == 401 and config.LINKEDIN_REFRESH_TOKEN and config.LINKEDIN_CLIENT_ID:
            log.warning("LinkedIn 401 — attempting token refresh")
            resp = self._refresh_and_retry(method, url, **kwargs)
        return resp

    # ── Stage C: Audience Counts ───────────────────────────────────────────────

    def get_audience_count(
        self,
        facet_urns: dict[str, list[str]],
        exclude_facet_urns: dict[str, list[str]] | None = None,
    ) -> int:
        """
        Call GET /rest/audienceCounts?q=targetingCriteriaV2 with Rest.li-encoded
        targeting. Returns the total estimated audience size, or 0 on error.

        facet_urns: { "urn:li:adTargetingFacet:skills": ["urn:li:skill:1"], ... }
        exclude_facet_urns: same shape — adds an `exclude` block to the criteria.

        Rules (from LinkedIn docs):
          - q=targetingCriteriaV2  (NOT targetingCriteria)
          - targetingCriteria is Rest.li format (NOT JSON)
          - Do NOT include account param
          - At least ONE include criterion is required (LinkedIn errors otherwise)
          - URN-internal chars (`:`, `(`, `)`) MUST be percent-encoded; structural
            delimiters of the Rest.li expression MUST be raw. We build the final
            URL ourselves so requests doesn't double-encode.
          - Response: elements[0]["total"] (fall back to "active" if absent)
        """
        # Empty-include guard: LinkedIn rejects calls that have no include.
        non_empty = {k: v for k, v in (facet_urns or {}).items() if v}
        if not non_empty:
            log.warning("get_audience_count: empty include — skipping API call (returning 0)")
            return 0

        targeting_str = _build_restli_targeting(non_empty, exclude_facet_urns or {})
        # Build the URL by hand. The targeting string is already in
        # "structural-raw, URN-internal-encoded" form per LinkedIn's spec —
        # passing it through requests params= would double-encode.
        url = f"{self._url('audienceCounts')}?q=targetingCriteriaV2&targetingCriteria={targeting_str}"
        try:
            resp = self._req("GET", url)
            self._raise_for_status(resp, "audienceCounts")
            data = resp.json()
            elements = data.get("elements", [])
            if elements:
                el = elements[0]
                total = int(el.get("total", 0) or el.get("active", 0))
                log.info("Audience count: %d", total)
                return total
        except Exception as exc:
            log.error("Audience count error: %s", exc)
            raise
        return 0

    # ── Campaign group ─────────────────────────────────────────────────────────

    # Prefix applied automatically to every campaign + campaign-group + creative name
    # so resources created by this pipeline are easy to find in Campaign Manager.
    AGENT_NAME_PREFIX = "agent_"

    def _prefixed(self, name: str) -> str:
        """Return name with AGENT_NAME_PREFIX prepended (idempotent)."""
        if name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    def create_campaign_group(self, name: str) -> str:
        """
        Create a sponsored content campaign group.
        Returns the campaign group URN. Name auto-prefixed with "agent_".
        """
        name = self._prefixed(name)
        # Always create as DRAFT — user-configured default so nothing launches
        # without an explicit human approval step in LinkedIn Campaign Manager.
        payload = {
            "account":  f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "name":     name,
            "status":   "DRAFT",
            "runSchedule": {"start": _now_ms()},
        }
        for attempt in range(3):
            try:
                resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaignGroups"), json=payload)
                self._raise_for_status(resp, "createCampaignGroup")
                break
            except Exception as exc:
                if attempt == 2:
                    raise
                log.warning("createCampaignGroup attempt %d failed (%s) — retrying", attempt + 1, exc)
                import time; time.sleep(3)
        group_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaignGroup:{group_id}"
        log.info("Created campaign group %s (name=%s)", urn, name)
        return urn

    def rename_campaign_group(self, group_id_or_urn: str, new_name: str) -> None:
        """
        Rename an existing campaign group via PATCH. Auto-prefixes the new name.
        """
        new_name = self._prefixed(new_name)
        group_id = group_id_or_urn.rsplit(":", 1)[-1]
        payload = {"patch": {"$set": {"name": new_name}}}
        resp = self._req(
            "POST",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaignGroups/{group_id}"),
            json=payload,
            headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
        )
        self._raise_for_status(resp, "renameCampaignGroup")
        log.info("Renamed campaign group %s → %s", group_id, new_name)

    def rename_campaign(self, campaign_id_or_urn: str, new_name: str) -> None:
        """
        Rename an existing campaign via PATCH. Auto-prefixes the new name.
        """
        new_name = self._prefixed(new_name)
        campaign_id = campaign_id_or_urn.rsplit(":", 1)[-1]
        payload = {"patch": {"$set": {"name": new_name}}}
        resp = self._req(
            "POST",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{campaign_id}"),
            json=payload,
            headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
        )
        self._raise_for_status(resp, "renameCampaign")
        log.info("Renamed campaign %s → %s", campaign_id, new_name)

    def get_campaign(self, campaign_urn_or_id: str) -> dict:
        """Fetch full campaign JSON from LinkedIn API (includes targetingCriteria)."""
        campaign_id = str(campaign_urn_or_id).rsplit(":", 1)[-1]
        resp = self._req(
            "GET",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{campaign_id}"),
        )
        self._raise_for_status(resp, "getCampaign")
        return resp.json()

    def clone_campaign(self, source_urn: str, new_name: str) -> str:
        """
        Create a new DRAFT campaign by cloning the targeting criteria from an
        existing campaign. The new campaign is placed in the same campaign group.
        Returns new campaign URN.
        """
        source = self.get_campaign(source_urn)
        targeting    = source.get("targetingCriteria") or {}
        group_urn    = source.get("campaignGroup") or ""
        daily_budget = source.get("dailyBudget") or {"currencyCode": "USD", "amount": "50.00"}
        unit_cost    = source.get("unitCost")    or {"currencyCode": "USD", "amount": "10.00"}
        locale       = source.get("locale")      or {"country": "US", "language": "en"}
        obj_type     = source.get("objectiveType") or "WEBSITE_VISIT"

        name = self._prefixed(new_name)
        payload = {
            "account":                f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaignGroup":          group_urn,
            "name":                   name,
            "type":                   "SPONSORED_UPDATES",
            "costType":               "CPM",
            "dailyBudget":            daily_budget,
            "unitCost":               unit_cost,
            "targetingCriteria":      targeting,
            "status":                 "DRAFT",
            "locale":                 locale,
            "objectiveType":          obj_type,
            "offsiteDeliveryEnabled": False,
            "politicalIntent":        "NOT_POLITICAL",
            "runSchedule":            {"start": _now_ms()},
        }
        resp = self._req(
            "POST",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns"),
            json=payload,
        )
        self._raise_for_status(resp, "cloneCampaign")
        campaign_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        log.info("Cloned campaign %s → %s '%s'", source_urn, urn, name)
        return urn

    # ── Campaign ───────────────────────────────────────────────────────────────

    def create_campaign(
        self,
        name: str,
        campaign_group_urn: str,
        facet_urns: dict[str, list[str]],
        daily_budget_cents: int = 5000,
        exclude_facet_urns: dict[str, list[str]] | None = None,
    ) -> str:
        """
        Create a Sponsored Content campaign with the given targeting.
        Returns the campaign URN. Name auto-prefixed with "agent_".

        `exclude_facet_urns` is an optional `{facet: [urns]}` map of negation
        targeting (recruiters/sales/etc.) — emitted as the `exclude` block of
        targetingCriteria. See `config.DEFAULT_EXCLUDE_FACETS`.
        """
        name = self._prefixed(name)
        targeting = _build_targeting_criteria(facet_urns, exclude_facet_urns)
        payload = {
            "account":       f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaignGroup": campaign_group_urn,
            "name":          name,
            "type":          "SPONSORED_UPDATES",
            "costType":      "CPM",
            "dailyBudget":   {"currencyCode": "USD", "amount": str(daily_budget_cents / 100)},
            "unitCost":      {"currencyCode": "USD", "amount": "10.00"},
            "targetingCriteria": targeting,
            "status":                 "DRAFT",
            "locale":                 {"country": "US", "language": "en"},
            "objectiveType":          "WEBSITE_VISIT",
            "offsiteDeliveryEnabled": False,
            "politicalIntent":        "NOT_POLITICAL",
            "runSchedule":            {"start": _now_ms()},
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns"), json=payload)
        self._raise_for_status(resp, "createCampaign")
        campaign_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        log.info("Created campaign %s '%s'", urn, name)
        return urn

    # ── Image upload ───────────────────────────────────────────────────────────

    def upload_image(self, image_path: str | Path) -> str:
        """
        Upload an image asset to LinkedIn using the Images API (REST).
        Returns the image URN (urn:li:image:...).
        """
        image_path = Path(image_path)

        # Step 1: initialize upload
        # The image owner MUST match the DSC post author (create_image_ad uses
        # LINKEDIN_MEMBER_URN as the author). LinkedIn rejects the post creation
        # with INVALID_CONTENT_OWNERSHIP if the image owner is different from the
        # post author — so we use the member URN here, not the sponsored account.
        init_payload = {
            "initializeUploadRequest": {
                "owner": config.LINKEDIN_MEMBER_URN,
            }
        }
        resp = self._req("POST", self._url("images?action=initializeUpload"), json=init_payload)
        self._raise_for_status(resp, "initializeImageUpload")
        init_data = resp.json()
        upload_url = init_data["value"]["uploadUrl"]
        image_urn  = init_data["value"]["image"]

        # Step 2: PUT binary to upload URL
        mime, _ = mimetypes.guess_type(str(image_path))
        with open(image_path, "rb") as fh:
            put_resp = requests.put(
                upload_url,
                data=fh,
                headers={"Content-Type": mime or "image/png"},
            )
        if not put_resp.ok:
            log.error("Image PUT failed %d: %s", put_resp.status_code, put_resp.text[:300])
            put_resp.raise_for_status()

        log.info("Uploaded image %s → %s", image_path.name, image_urn)
        return image_urn

    # ── InMail Campaign ────────────────────────────────────────────────────────

    def create_inmail_campaign(
        self,
        name: str,
        campaign_group_urn: str,
        facet_urns: dict[str, list[str]],
        daily_budget_cents: int = 5000,
        exclude_facet_urns: dict[str, list[str]] | None = None,
    ) -> str:
        """
        Create a Sponsored InMail (Message Ad) campaign.
        facet_urns keys must be full facet URNs (urn:li:adTargetingFacet:titles, etc.)
        Returns the campaign URN. `exclude_facet_urns` is the negation analog —
        see `create_campaign` and `config.DEFAULT_EXCLUDE_FACETS`.
        """
        name = self._prefixed(name)
        targeting = _build_targeting_criteria(facet_urns, exclude_facet_urns)
        payload = {
            "account":               f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaignGroup":         campaign_group_urn,
            "name":                  name,
            "type":                  "SPONSORED_INMAILS",
            "costType":              "CPM",
            "dailyBudget":           {"currencyCode": "USD", "amount": str(daily_budget_cents / 100)},
            "unitCost":              {"currencyCode": "USD", "amount": "0.40"},
            "targetingCriteria":     targeting,
            "status":                "DRAFT",
            "locale":                {"country": "US", "language": "en"},
            "objectiveType":         "LEAD_GENERATION",
            "offsiteDeliveryEnabled": False,
            "politicalIntent":        "NOT_POLITICAL",
            "creativeSelection":      "ROUND_ROBIN",
            "runSchedule":            {"start": _now_ms()},
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns"), json=payload)
        self._raise_for_status(resp, "createInMailCampaign")
        campaign_id = resp.headers.get("x-restli-id") or resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        log.info("Created InMail campaign %s '%s'", urn, name)
        return urn

    # ── InMail Creative ────────────────────────────────────────────────────────

    def create_inmail_ad(
        self,
        campaign_urn: str,
        sender_urn: str,
        subject: str,
        body: str,
        cta_label: str,
        destination_url: str | None = None,
    ) -> str:
        """
        Create a LinkedIn Message Ad creative and attach it to a campaign.
        Two-step: (1) create inMailContent via REST API, (2) create creative referencing it.
        Uses /rest/inMailContents (no MDP needed) with LinkedIn-Version: 202506 header.
        sender_urn: urn:li:person:... (must be connected to the ad account).
        Returns the sponsoredCreative URN.
        """
        dest = destination_url or config.LINKEDIN_DESTINATION

        # Step 1 — create the InMail content object via REST API (no MDP required)
        content_payload = {
            "account": f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "name": f"inmail_{int(__import__('time').time())}",
            "sender": sender_urn,
            "htmlBody": body[:1000],
            "subject": subject[:60],
            "subContent": {
                "regular": {
                    "callToActionText": cta_label[:20],
                    "callToActionLandingPageUrl": dest,
                }
            }
        }
        content_headers = self._default_headers()
        content_headers["LinkedIn-Version"] = "202506"

        import requests as _req_lib
        resp = _req_lib.post("https://api.linkedin.com/rest/inMailContents", json=content_payload, headers=content_headers)
        self._raise_for_status(resp, "createInMailContent")
        content_id  = resp.headers.get("x-restli-id") or _id_from_location(resp)
        content_urn = f"urn:li:adInMailContent:{content_id}"
        log.info("Created InMail content %s (no MDP required)", content_urn)

        # Step 2 — create the creative referencing the content
        creative_payload = {
            "campaign": campaign_urn,
            "content": {"reference": content_urn},
            "intendedStatus": "DRAFT",
        }
        creative_headers = self._default_headers()
        creative_headers["LinkedIn-Version"] = "202506"
        resp = self._req("POST", f"https://api.linkedin.com/rest/adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives", json=creative_payload, headers=creative_headers)
        self._raise_for_status(resp, "createInMailCreative")
        creative_id = resp.headers.get("x-restli-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCreative:{creative_id}"
        log.info("Created InMail creative %s", urn)
        return urn

    # ── Ad Creative ────────────────────────────────────────────────────────────

    def create_image_ad(
        self,
        campaign_urn: str,
        image_urn: str,
        headline: str,
        description: str,
        destination_url: str | None = None,
        intro_text: str = "",
        ad_headline: str = "",
        ad_description: str = "",
        cta_button: str = "APPLY",
    ) -> ImageAdResult:
        """
        Create a Single Image Ad creative and attach it to a campaign.

        Returns ImageAdResult — NEVER raises for the LINKEDIN_MEMBER_URN /
        DSC-403 cases (those return status="local_fallback" so callers can
        fall back to saving the PNG locally and continue). Other unexpected
        errors return status="error" — caller decides whether to log + skip
        or re-raise.

        Backwards compat: callers that previously bound the result as a
        string MUST migrate to `.creative_urn`.

        See _create_image_ad_impl for the underlying API logic.
        """
        try:
            urn = self._create_image_ad_impl(
                campaign_urn=campaign_urn,
                image_urn=image_urn,
                headline=headline,
                description=description,
                destination_url=destination_url,
                intro_text=intro_text,
                ad_headline=ad_headline,
                ad_description=ad_description,
                cta_button=cta_button,
            )
            return ImageAdResult(creative_urn=urn, status="ok")
        except RuntimeError as exc:
            msg = str(exc)
            if "LINKEDIN_MEMBER_URN" in msg:
                return ImageAdResult(
                    status="local_fallback",
                    error_class="RuntimeError",
                    error_message=msg,
                )
            return ImageAdResult(
                status="error",
                error_class="RuntimeError",
                error_message=msg,
            )
        except Exception as exc:
            msg = str(exc)
            upper = msg.upper()
            if "403" in msg or "FORBIDDEN" in upper:
                return ImageAdResult(
                    status="local_fallback",
                    error_class=type(exc).__name__,
                    error_message=msg,
                )
            return ImageAdResult(
                status="error",
                error_class=type(exc).__name__,
                error_message=msg,
            )

    def _create_image_ad_impl(
        self,
        campaign_urn: str,
        image_urn: str,
        headline: str,
        description: str,
        destination_url: str | None = None,
        intro_text: str = "",
        ad_headline: str = "",
        ad_description: str = "",
        cta_button: str = "APPLY",
    ) -> str:
        """
        Inner raise-based implementation of create_image_ad. Returns the
        adCreative URN on success; raises RuntimeError or HTTPError on
        failure. Public wrapper translates exceptions into ImageAdResult.

        Flow:
        1. Create a Direct Sponsored Content (DSC) post via /rest/posts.
           DSC posts are dark — never shown organically, only as ads.
           Author must be the LinkedIn public profile URN of whoever authorized
           the OAuth token (LINKEDIN_MEMBER_URN in .env, e.g. urn:li:person:AbCdEfGhIj).
        2. Create the creative referencing that post URN.

        Scope requirements: w_member_social (already present).
        """
        dest = destination_url or config.LINKEDIN_DESTINATION

        member_urn = config.LINKEDIN_MEMBER_URN
        if not member_urn:
            raise RuntimeError(
                "LINKEDIN_MEMBER_URN is not set in .env. "
                "Set it to the LinkedIn public profile URN of whoever authorized the OAuth token "
                "(e.g. urn:li:person:AbCdEfGhIj — find it at linkedin.com/in/<id>)."
            )

        # Step 1 — create DSC post via /rest/posts (LinkedIn API 202510).
        # lifecycleState=DRAFT + adContext.dscAdAccount = Direct Sponsored Content.
        # feedDistribution=NONE ensures it never appears organically.
        # Field priority for what shows to the user:
        #   commentary  = intro_text (above the image in feed, ≤140 chars preferred)
        #                 Falls back to `description` if intro_text is empty.
        #   media.title = ad_headline (bold text BELOW image in feed, ≤70 chars)
        #                 Falls back to `headline` if ad_headline is empty.
        # ad_description and cta_button are attached on the creative payload (Step 2).
        commentary = (intro_text or description)[:700]
        media_title = (ad_headline or headline)[:200]
        dsc_payload = {
            "author":        member_urn,
            "commentary":    commentary,
            "visibility":    "PUBLIC",
            "lifecycleState": "DRAFT",
            "adContext": {
                "dscAdAccount": f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            },
            "distribution": {
                "feedDistribution":             "NONE",
                "targetEntities":               [],
                "thirdPartyDistributionChannels": [],
            },
            "content": {
                "media": {
                    "id":    image_urn,
                    "title": media_title,
                }
            },
        }
        dsc_resp = requests.post(
            "https://api.linkedin.com/rest/posts",
            json=dsc_payload,
            headers={
                "Authorization":              f"Bearer {self._token}",
                "LinkedIn-Version":           config.LINKEDIN_VERSION,
                "X-Restli-Protocol-Version":  "2.0.0",
                "Content-Type":               "application/json",
            },
        )
        self._raise_for_status(dsc_resp, "createDscPost")
        post_id  = dsc_resp.headers.get("x-restli-id") or _id_from_location(dsc_resp)
        post_urn = f"urn:li:share:{post_id}"
        log.info("Created DSC post %s", post_urn)

        # Step 2 — create creative referencing the DSC post.
        # The creative holds the CTA button + destination URL + optional description.
        # The DSC post (Step 1) holds commentary + media; the creative overlays the
        # click-through spec on top of that post.
        creative_content = {"reference": post_urn}
        # LinkedIn creative API accepts `callToAction` + `landingPage` + `description`
        # at the creative level (not in the DSC post) for Sponsored Content image ads.
        creative_extras: dict = {}
        if cta_button:
            creative_extras["callToAction"] = {"label": cta_button.upper()}
        if dest:
            creative_extras["landingPage"] = {"url": dest}
        if ad_description:
            creative_extras["description"] = ad_description[:100]

        payload = {
            "campaign":       campaign_urn,
            "intendedStatus": "ACTIVE",
            "content":        creative_content,
        }
        if creative_extras:
            payload["content"]["inlineContent"] = creative_extras
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives"), json=payload)
        self._raise_for_status(resp, "createAdCreative")
        creative_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCreative:{creative_id}"
        log.info("Created adCreative %s", urn)
        return urn


# ── Targeting helpers ──────────────────────────────────────────────────────────

def _build_targeting_criteria(
    facet_urns: dict[str, list[str]],
    exclude_facet_urns: dict[str, list[str]] | None = None,
) -> dict:
    """
    Convert { facetKey: [urn, ...] } to a LinkedIn targetingCriteria object.
    The campaign API (adCampaigns) requires full URN keys
    (e.g. "urn:li:adTargetingFacet:degrees"), NOT short keys ("degrees").
    Account-level defaults inject interfaceLocales as a full URN key;
    mixing short + full URN keys in the same targeting causes a 400 INVALID_VALUE.
    All facets are ANDed together; values within each facet are ORed.

    LinkedIn also requires at least one location facet (profileLocations, locations,
    or ipLocations). If none is present, we add a worldwide fallback.

    Negation facets (`exclude_facet_urns`) are emitted as a peer `exclude` block.
    Per LinkedIn semantics, an audience matches the campaign IFF it satisfies
    `include` AND does not match any value in `exclude` (exclude is OR-of-OR).
    """
    _LOCATION_FACETS = {
        "urn:li:adTargetingFacet:profileLocations",
        "urn:li:adTargetingFacet:locations",
        "urn:li:adTargetingFacet:ipLocations",
        "profileLocations",
        "locations",
        "ipLocations",
    }
    # Worldwide geo URN — confirmed working 2026-04-21
    _WORLDWIDE_URN = "urn:li:geo:90009492"

    include = []
    has_location = False

    for facet, urns in facet_urns.items():
        if not urns:
            continue
        # Normalize to full URN key
        full_key = _FACET_SHORT_TO_URN.get(facet, facet)
        include.append({"or": {full_key: urns}})
        if facet in _LOCATION_FACETS or full_key in _LOCATION_FACETS:
            has_location = True

    # LinkedIn requires a location facet — add worldwide if not present
    if not has_location:
        include.append({
            "or": {
                "urn:li:adTargetingFacet:profileLocations": [_WORLDWIDE_URN],
            }
        })
        log.debug("No location facet in targeting — added worldwide fallback")

    out: dict = {"include": {"and": include}}

    if exclude_facet_urns:
        # LinkedIn exclude.or must be a dict (DataMap) not a list:
        #   {"exclude": {"or": {"urn:li:adTargetingFacet:titles": ["urn:li:title:1"]}}}
        # All excluded facets are merged into one DataMap object (OR semantics across all keys).
        exclude_map: dict[str, list[str]] = {}
        for facet, urns in exclude_facet_urns.items():
            if not urns:
                continue
            full_key = _FACET_SHORT_TO_URN.get(facet, facet)
            exclude_map[full_key] = urns
        if exclude_map:
            out["exclude"] = {"or": exclude_map}

    return out


_FACET_SHORT_TO_URN = {
    "skills":           "urn:li:adTargetingFacet:skills",
    "titles":           "urn:li:adTargetingFacet:titles",
    "fieldsOfStudy":    "urn:li:adTargetingFacet:fieldsOfStudy",
    "degrees":          "urn:li:adTargetingFacet:degrees",
    "profileLocations": "urn:li:adTargetingFacet:profileLocations",
    "locations":        "urn:li:adTargetingFacet:locations",
    "industries":       "urn:li:adTargetingFacet:industries",
    "seniorities":      "urn:li:adTargetingFacet:seniorities",
    "interfaceLocales": "urn:li:adTargetingFacet:interfaceLocales",
    "staffCountRanges": "urn:li:adTargetingFacet:staffCountRanges",
    "yearsOfExperienceRanges": "urn:li:adTargetingFacet:yearsOfExperienceRanges",
    "ageRanges":        "urn:li:adTargetingFacet:ageRanges",
    "genders":          "urn:li:adTargetingFacet:genders",
    "memberBehaviors":  "urn:li:adTargetingFacet:memberBehaviors",
    "groups":           "urn:li:adTargetingFacet:groups",
    "schools":          "urn:li:adTargetingFacet:schools",
    "employers":        "urn:li:adTargetingFacet:employers",
    "followedCompanies":"urn:li:adTargetingFacet:followedCompanies",
}


def _encode_urn(urn: str) -> str:
    """Percent-encode the URN-internal characters that conflict with Rest.li's
    structural delimiters: `:` `(` `)` `,`. Leaves alphanumerics + `-` `_` `.`
    untouched. Result is what LinkedIn's audienceCounts parser expects inside
    the `targetingCriteria` query param.
    """
    return (urn
            .replace("%", "%25")  # encode any literal % first
            .replace(":", "%3A")
            .replace("(", "%28")
            .replace(")", "%29")
            .replace(",", "%2C"))


def _build_restli_targeting(
    include: dict[str, list[str]],
    exclude: dict[str, list[str]] | None = None,
) -> str:
    """Build a Rest.li targeting string for audienceCounts?q=targetingCriteriaV2.

    Accepts either short facet names ("skills") or full URNs. URN values are
    percent-encoded character-by-character per `_encode_urn` — the structural
    parens, colons, and commas of the Rest.li expression are left raw because
    LinkedIn's URL parser uses them as delimiters.

    Output shape (URN-encoded portions shown lower-case for readability):
      include only:
        (include:(and:List((or:(<facet>:List(<urn>,<urn>))))))
      with exclude:
        (include:(and:List(...)),exclude:(or:(<facet>:List(<urn>))))

    Spec source: LinkedIn Marketing API docs + colleague's working call
    pattern (skill `rw_ads`, version 202510, no MDP tier required).
    """
    include_or_blocks = []
    for facet, values in (include or {}).items():
        if not values:
            continue
        full_facet = _encode_urn(_FACET_SHORT_TO_URN.get(facet, facet))
        encoded_vals = ",".join(_encode_urn(v) for v in values)
        include_or_blocks.append(f"(or:({full_facet}:List({encoded_vals})))")
    include_part = f"(and:List({','.join(include_or_blocks)}))"

    exclude_or_blocks = []
    for facet, values in (exclude or {}).items():
        if not values:
            continue
        full_facet = _encode_urn(_FACET_SHORT_TO_URN.get(facet, facet))
        encoded_vals = ",".join(_encode_urn(v) for v in values)
        # No outer parens — exclude items go directly inside `or:(...)`.
        exclude_or_blocks.append(f"{full_facet}:List({encoded_vals})")

    if exclude_or_blocks:
        # Exclude uses `or:(...)` directly — no `and:List` wrapper.
        exclude_part = f",exclude:(or:({','.join(exclude_or_blocks)}))"
        return f"(include:{include_part}{exclude_part})"
    return f"(include:{include_part})"


def _encode_targeting_for_query(targeting: dict) -> str:
    """Legacy helper — kept for campaign-creation path which uses JSON format."""
    import json, urllib.parse
    return urllib.parse.quote(json.dumps(targeting))


# ── Utility ────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _id_from_location(resp: requests.Response) -> str:
    loc = resp.headers.get("Location", "")
    return loc.rstrip("/").rsplit("/", 1)[-1]

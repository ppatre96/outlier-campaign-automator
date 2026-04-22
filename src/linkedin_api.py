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

import requests

import config

log = logging.getLogger(__name__)

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


class LinkedInClient:
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

    def _req(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request; auto-refresh and retry once on 401."""
        resp = self._session.request(method, url, **kwargs)
        if resp.status_code == 401 and config.LINKEDIN_REFRESH_TOKEN and config.LINKEDIN_CLIENT_ID:
            log.warning("LinkedIn 401 — attempting token refresh")
            resp = self._refresh_and_retry(method, url, **kwargs)
        return resp

    # ── Stage C: Audience Counts ───────────────────────────────────────────────

    def get_audience_count(self, facet_urns: dict[str, list[str]]) -> int:
        """
        Call GET /rest/audienceCounts?q=targetingCriteriaV2 with Rest.li-encoded targeting.
        Returns the total estimated audience size, or 0 on error.

        facet_urns: { "urn:li:adTargetingFacet:skills": ["urn:li:skill:1"], ... }

        Rules (from LinkedIn docs):
          - q=targetingCriteriaV2  (NOT targetingCriteria)
          - targetingCriteria is Rest.li format (NOT JSON)
          - Do NOT include account param
          - Response: elements[0]["total"]
        """
        targeting_str = _build_restli_targeting(facet_urns)
        params = {
            "q":                 "targetingCriteriaV2",
            "targetingCriteria": targeting_str,
        }
        try:
            resp = self._req("GET", self._url("audienceCounts"), params=params)
            self._raise_for_status(resp, "audienceCounts")
            data = resp.json()
            elements = data.get("elements", [])
            if elements:
                total = int(elements[0].get("total", 0))
                log.info("Audience count: %d", total)
                return total
        except Exception as exc:
            log.error("Audience count error: %s", exc)
            raise
        return 0

    # ── Campaign group ─────────────────────────────────────────────────────────

    def create_campaign_group(self, name: str) -> str:
        """
        Create a sponsored content campaign group.
        Returns the campaign group URN.
        """
        payload = {
            "account":  f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "name":     name,
            "status":   "ACTIVE",
            "runSchedule": {"start": _now_ms()},
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaignGroups"), json=payload)
        self._raise_for_status(resp, "createCampaignGroup")
        group_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaignGroup:{group_id}"
        log.info("Created campaign group %s", urn)
        return urn

    # ── Campaign ───────────────────────────────────────────────────────────────

    def create_campaign(
        self,
        name: str,
        campaign_group_urn: str,
        facet_urns: dict[str, list[str]],
        daily_budget_cents: int = 5000,
    ) -> str:
        """
        Create a Sponsored Content campaign with the given targeting.
        Returns the campaign URN.
        """
        targeting = _build_targeting_criteria(facet_urns)
        payload = {
            "account":       f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaignGroup": campaign_group_urn,
            "name":          name,
            "type":          "SPONSORED_UPDATES",
            "costType":      "CPM",
            "dailyBudget":   {"currencyCode": "USD", "amount": str(daily_budget_cents / 100)},
            "unitCost":      {"currencyCode": "USD", "amount": "10.00"},
            "targetingCriteria": targeting,
            "status":                 "PAUSED",
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
        # Use sponsoredAccount as owner (covered by rw_ads scope).
        # urn:li:organization requires r_organization_social which we don't have.
        init_payload = {
            "initializeUploadRequest": {
                "owner": f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
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
    ) -> str:
        """
        Create a Sponsored InMail (Message Ad) campaign.
        facet_urns keys must be full facet URNs (urn:li:adTargetingFacet:titles, etc.)
        Returns the campaign URN.
        """
        targeting = _build_targeting_criteria(facet_urns)
        payload = {
            "account":               f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaignGroup":         campaign_group_urn,
            "name":                  name,
            "type":                  "SPONSORED_INMAILS",
            "costType":              "CPM",
            "dailyBudget":           {"currencyCode": "USD", "amount": str(daily_budget_cents / 100)},
            "unitCost":              {"currencyCode": "USD", "amount": "0.40"},
            "targetingCriteria":     targeting,
            "status":                "PAUSED",
            "locale":                {"country": "US", "language": "en"},
            "objectiveType":         "LEAD_GENERATION",
            "offsiteDeliveryEnabled": False,
            "politicalIntent":        "NOT_POLITICAL",
            "creativeSelection":      "ROUND_ROBIN",
            "runSchedule":            {"start": _now_ms()},
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns"), json=payload)
        self._raise_for_status(resp, "createInMailCampaign")
        campaign_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
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
        Two-step: (1) create adInMailContent, (2) create creative referencing it.
        sender_urn: urn:li:person:... (must be connected to the ad account).
        Returns the sponsoredCreative URN.
        """
        dest = destination_url or config.LINKEDIN_DESTINATION

        # Step 1 — create the InMail content object
        # adInMailContents requires an older API version (202502)
        content_payload = {
            "account":   f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "adSenders": [{"sender": sender_urn}],
            "subject":   subject[:60],
            "body":      body[:1000],
            "callToAction": {
                "text":          cta_label[:20],
                "landingPageUrl": dest,
            },
        }
        content_headers = {
            "Authorization": f"Bearer {self._token}",
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        }
        import requests as _req_lib
        resp = _req_lib.post("https://api.linkedin.com/v2/adInMailContents", json=content_payload, headers=content_headers)
        self._raise_for_status(resp, "createInMailContent")
        content_id  = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        content_urn = f"urn:li:adInMailContent:{content_id}"
        log.info("Created InMail content %s", content_urn)

        # Step 2 — create the creative referencing the content
        creative_payload = {
            "campaign": campaign_urn,
            "content":  {"reference": content_urn},
            "status":   "ACTIVE",
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives"), json=creative_payload)
        self._raise_for_status(resp, "createInMailCreative")
        creative_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
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
    ) -> str:
        """
        Create a Single Image Ad creative and attach it to a campaign.
        Returns the adCreative URN.

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
        dsc_payload = {
            "author":        member_urn,
            "commentary":    description[:700],
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
                    "title": headline[:200],
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
        payload = {
            "campaign":       campaign_urn,
            "intendedStatus": "ACTIVE",
            "content": {
                "reference": post_urn,
            },
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives"), json=payload)
        self._raise_for_status(resp, "createAdCreative")
        creative_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCreative:{creative_id}"
        log.info("Created adCreative %s", urn)
        return urn


# ── Targeting helpers ──────────────────────────────────────────────────────────

def _build_targeting_criteria(facet_urns: dict[str, list[str]]) -> dict:
    """
    Convert { facetKey: [urn, ...] } to a LinkedIn targetingCriteria object.
    The campaign API (adCampaigns) requires full URN keys
    (e.g. "urn:li:adTargetingFacet:degrees"), NOT short keys ("degrees").
    Account-level defaults inject interfaceLocales as a full URN key;
    mixing short + full URN keys in the same targeting causes a 400 INVALID_VALUE.
    All facets are ANDed together; values within each facet are ORed.

    LinkedIn also requires at least one location facet (profileLocations, locations,
    or ipLocations). If none is present, we add a worldwide fallback.
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

    return {"include": {"and": include}}


_FACET_SHORT_TO_URN = {
    "skills":           "urn:li:adTargetingFacet:skills",
    "titles":           "urn:li:adTargetingFacet:titles",
    "fieldsOfStudy":    "urn:li:adTargetingFacet:fieldsOfStudy",
    "degrees":          "urn:li:adTargetingFacet:degrees",
    "profileLocations": "urn:li:adTargetingFacet:profileLocations",
    "locations":        "urn:li:adTargetingFacet:locations",
    "industries":       "urn:li:adTargetingFacet:industries",
}


def _build_restli_targeting(facet_urns: dict[str, list[str]]) -> str:
    """
    Build a Rest.li targeting string for audienceCounts?q=targetingCriteriaV2.

    Accepts either short facet names ("skills") or full URNs.
    Values must be raw URN strings — do NOT pre-encode them.
    requests will URL-encode the entire param value exactly once.

    LinkedIn parses the decoded string as Rest.li:
      (include:(and:List((or:(urn:li:adTargetingFacet:skills:List(urn:li:skill:123))))))
    """
    or_blocks = []
    for facet, values in facet_urns.items():
        if not values:
            continue
        full_facet = _FACET_SHORT_TO_URN.get(facet, facet)
        values_str = ",".join(values)  # raw URNs — no encoding
        or_blocks.append(f"(or:({full_facet}:List({values_str})))")
    return f"(include:(and:List({','.join(or_blocks)})))"


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

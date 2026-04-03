"""
LinkedIn Marketing API client.
  Stage C  — Audience Counts validation
  Campaign — Create campaign + campaign group
  Creative — Upload image + create adCreative + attach to campaign
"""
import logging
import mimetypes
import os
from pathlib import Path

import requests

import config

log = logging.getLogger(__name__)


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

    # ── Stage C: Audience Counts ───────────────────────────────────────────────

    def get_audience_count(self, facet_urns: dict[str, list[str]]) -> int:
        """
        Call GET /rest/audienceCounts with a targeting criteria object.
        Returns the total estimated audience size, or 0 on error.

        facet_urns: { "skills": ["urn:li:skill:1"], "titles": [...], ... }
        """
        targeting = _build_targeting_criteria(facet_urns)
        params = {
            "q":             "targetingCriteria",
            "account":       f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "targetingCriteria": _encode_targeting_for_query(targeting),
        }
        try:
            resp = self._session.get(self._url("audienceCounts"), params=params)
            self._raise_for_status(resp, "audienceCounts")
            data = resp.json()
            # Response: { "elements": [{ "targeting": {...}, "count": 123456 }] }
            elements = data.get("elements", [])
            if elements:
                count = int(elements[0].get("count", 0))
                log.info("Audience count: %d", count)
                return count
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
        resp = self._session.post(self._url("adCampaignGroups"), json=payload)
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
            "targeting":     targeting,
            "status":        "PAUSED",
            "locale":        {"country": "US", "language": "en"},
            "objectiveType": "WEBSITE_VISIT",
            "runSchedule":   {"start": _now_ms()},
        }
        resp = self._session.post(self._url("adCampaigns"), json=payload)
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
        init_payload = {
            "initializeUploadRequest": {
                "owner": f"urn:li:organization:{config.LINKEDIN_ORG_ID}",
            }
        }
        resp = self._session.post(
            self._url("images?action=initializeUpload"),
            json=init_payload,
        )
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
        """
        dest = destination_url or config.LINKEDIN_DESTINATION
        payload = {
            "account":  f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            "campaign": campaign_urn,
            "name":     headline[:50],
            "status":   "ACTIVE",
            "type":     "SPONSORED_UPDATE_V2",
            "content": {
                "singleImage": {
                    "landingPageUrl": dest,
                    "title":          headline[:200],
                    "description":    description[:600],
                    "logo":           image_urn,
                }
            },
        }
        resp = self._session.post(self._url("adCreatives"), json=payload)
        self._raise_for_status(resp, "createAdCreative")
        creative_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCreative:{creative_id}"
        log.info("Created adCreative %s", urn)
        return urn


# ── Targeting helpers ──────────────────────────────────────────────────────────

def _build_targeting_criteria(facet_urns: dict[str, list[str]]) -> dict:
    """
    Convert { facet_name: [urn, ...] } to a LinkedIn targeting criteria object.
    All facets are ANDed together (include).
    """
    include = []
    for facet, urns in facet_urns.items():
        if urns:
            include.append({
                "type": facet,
                "values": urns,
            })

    return {
        "include": {
            "and": [
                {
                    "or": {
                        "urn:li:adTargetingFacet:locations": [
                            # Always include a broad location to avoid empty audience
                            # "urn:li:geo:103644278"  # United States — handled by location column
                        ]
                    }
                }
            ] if False else include   # just include the facets
        }
    }


def _encode_targeting_for_query(targeting: dict) -> str:
    """
    Encode targeting dict as a Rest.li JSON-encoded query param string.
    LinkedIn's audienceCounts endpoint expects the value URL-encoded.
    """
    import json, urllib.parse
    return urllib.parse.quote(json.dumps(targeting))


# ── Utility ────────────────────────────────────────────────────────────────────

def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _id_from_location(resp: requests.Response) -> str:
    loc = resp.headers.get("Location", "")
    return loc.rstrip("/").rsplit("/", 1)[-1]

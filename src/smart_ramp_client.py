"""Smart Ramp API client for fetching ramps and their cohort specifications."""

import os
import requests
from typing import Optional
from dataclasses import dataclass


@dataclass
class CohortSpec:
    """Cohort row specification from a Smart Ramp form."""

    id: str
    cohort_description: str
    signup_flow_id: Optional[str]
    selected_lp_url: Optional[str]
    included_geos: list[str]
    matched_locales: Optional[list[str]]
    target_activations: Optional[int]
    job_post_id: Optional[str]
    # Fields below feed the campaign-naming spec at
    # https://genai-smart-ramp-v2.vercel.app/ramps/<id>/campaigns
    # Each is sourced from a different slot in the raw Smart Ramp JSON; all
    # are optional because legacy ramps don't carry the metadata.
    job_post_pod: Optional[str] = None              # "specialist"|"generalist"|"coders"|"languages"
    matched_domain: Optional[str] = None            # e.g. "Finance & Quantitative Analysis"
    job_post_language_code: Optional[str] = None    # e.g. "en-US"
    campaign_state: Optional[dict] = None           # formData.cohorts[].campaign_state — full nested dict


@dataclass
class RampRecord:
    """Complete ramp record from Smart Ramp API."""

    id: str
    project_id: str
    project_name: Optional[str]
    requester_name: str
    summary: str
    submitted_at: str
    updated_at: str
    status: str
    linear_issue_id: Optional[str]
    linear_url: Optional[str]
    cohorts: list[CohortSpec]


class SmartRampClient:
    """HTTP client for Smart Ramp API with Vercel protection bypass."""

    BASE_URL = "https://genai-smart-ramp-v2.vercel.app"
    API_BASE = f"{BASE_URL}/api"

    def __init__(
        self,
        bypass_secret: Optional[str] = None,
        api_key: Optional[str] = None,
    ):
        """
        Initialize Smart Ramp client.

        Args:
            bypass_secret: Vercel Protection Bypass for Automation secret.
                          If not provided, reads from VERCEL_AUTOMATION_BYPASS_SECRET env var.
            api_key: Reserved for future use (HTTP-only for now, no API key needed).
        """
        self.bypass_secret = bypass_secret or os.getenv(
            "VERCEL_AUTOMATION_BYPASS_SECRET"
        )
        if not self.bypass_secret:
            raise ValueError(
                "VERCEL_AUTOMATION_BYPASS_SECRET not set. "
                "Get it from Smart Ramp project → Settings → Deployment Protection → Vercel Authentication."
            )

    def _headers(self) -> dict:
        """Build headers for Smart Ramp API requests."""
        return {
            "x-vercel-protection-bypass": self.bypass_secret,
            "x-requested-with": "genai-internal",
            "Content-Type": "application/json",
        }

    def fetch_ramp(self, ramp_id: str) -> Optional[RampRecord]:
        """
        Fetch a single ramp by ID.

        Args:
            ramp_id: The ramp ID (e.g., "GMR-0016")

        Returns:
            RampRecord if found, None if 404 or error.
        """
        try:
            url = f"{self.API_BASE}/ramps/{ramp_id}"
            resp = requests.get(url, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            return self._parse_ramp(resp.json())
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ramp {ramp_id}: {e}")
            return None

    def fetch_ramp_list(self) -> list[RampRecord]:
        """
        Fetch list of all ramps (paginated if needed).

        Returns:
            List of RampRecord objects (at least summary fields populated).
        """
        try:
            url = f"{self.API_BASE}/ramps"
            resp = requests.get(url, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            data = resp.json()

            # API returns list of ramps directly
            if isinstance(data, list):
                return [self._parse_ramp(r) for r in data]
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ramp list: {e}")
            return []

    def _parse_ramp(self, raw: dict) -> RampRecord:
        """Parse raw API response into RampRecord."""
        form_data = raw.get("formData", {})
        cohorts_raw = form_data.get("cohorts", [])

        cohorts = [self._parse_cohort(c) for c in cohorts_raw]

        return RampRecord(
            id=raw.get("id", ""),
            project_id=form_data.get("project_id", ""),
            project_name=form_data.get("project_name"),
            requester_name=form_data.get("requester_name", ""),
            summary=form_data.get("summary", ""),
            submitted_at=raw.get("submittedAt", ""),
            updated_at=raw.get("updatedAt", ""),
            status=raw.get("status", "draft"),
            linear_issue_id=raw.get("linearIssueId"),
            linear_url=raw.get("linearUrl"),
            cohorts=cohorts,
        )

    def _parse_cohort(self, raw: dict) -> CohortSpec:
        """Parse raw cohort row into CohortSpec."""
        return CohortSpec(
            id=raw.get("id", ""),
            cohort_description=raw.get("cohort_description", ""),
            signup_flow_id=raw.get("signup_flow_id"),
            selected_lp_url=raw.get("selected_lp_url"),
            included_geos=raw.get("included_geos", []),
            matched_locales=raw.get("matched_locales"),
            target_activations=raw.get("target_activations"),
            job_post_id=raw.get("job_post_id"),
            job_post_pod=raw.get("job_post_pod"),
            matched_domain=raw.get("matched_domain"),
            job_post_language_code=raw.get("job_post_language_code"),
            campaign_state=raw.get("campaign_state"),
        )

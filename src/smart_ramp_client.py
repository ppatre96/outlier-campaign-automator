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
    job_post_domain: Optional[str] = None           # Smart Ramp tool's name "domain" segment, e.g. "bn-IN"
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
        api_token: Optional[str] = None,
    ):
        """
        Initialize Smart Ramp client.

        Args:
            bypass_secret: Vercel Protection Bypass for Automation secret.
                          If not provided, reads from VERCEL_AUTOMATION_BYPASS_SECRET env var.
            api_token: Bearer token for Smart Ramp API (required since 2026-05-27 auth rotation).
                       If not provided, reads SMART_RAMP_API_TOKEN, then the legacy
                       TARGETS_API_TOKEN (renamed 2026-06-09; both work during the
                       transition, new name wins).
        """
        self.bypass_secret = bypass_secret or os.getenv(
            "VERCEL_AUTOMATION_BYPASS_SECRET"
        )
        if not self.bypass_secret:
            raise ValueError(
                "VERCEL_AUTOMATION_BYPASS_SECRET not set. "
                "Get it from Smart Ramp project → Settings → Deployment Protection → Vercel Authentication."
            )
        # 2026-06-09: token renamed TARGETS_API_TOKEN → SMART_RAMP_API_TOKEN.
        # Prefer the new name; fall back to the legacy name during the transition.
        self.api_token = (
            api_token
            or os.getenv("SMART_RAMP_API_TOKEN")
            or os.getenv("TARGETS_API_TOKEN")
        )
        if not self.api_token:
            raise ValueError(
                "SMART_RAMP_API_TOKEN not set. "
                "Smart Ramp requires a Bearer token in addition to the Vercel bypass secret. "
                "Get it from Quintin and set in Doppler outlier-campaign-agent/{dev,prd}."
            )

    def _headers(self) -> dict:
        """Build headers for Smart Ramp API requests."""
        return {
            "Authorization": f"Bearer {self.api_token}",
            "x-vercel-protection-bypass": self.bypass_secret,
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
        Fetch list of all ramps via the discovery endpoint /api/ramps/all.

        The summary endpoint returns lightweight ramp objects without formData /
        full cohort details — only id, name, alias, status, startDate, endDate
        and a stub cohort list (pod/domain/locale only, no cohort IDs). Callers
        that need full cohort detail must follow up with fetch_ramp(id).

        Returns:
            List of RampRecord objects with summary-only fields populated.
            cohorts is always [] — drill into individual ramps for cohort data.
        """
        try:
            url = f"{self.API_BASE}/ramps/all"
            resp = requests.get(url, headers=self._headers(), timeout=10)
            resp.raise_for_status()
            data = resp.json()

            if isinstance(data, list):
                return [self._parse_ramp_summary(r) for r in data]
            return []
        except requests.exceptions.RequestException as e:
            print(f"Error fetching ramp list: {e}")
            return []

    def _parse_ramp_summary(self, raw: dict) -> RampRecord:
        """Parse a summary item from /api/ramps/all into a RampRecord.

        The summary shape is much lighter than the single-ramp endpoint:
        no formData, no requester, no Linear linkage. Empty strings stand in
        for fields the consumer can backfill via fetch_ramp(id) if needed.
        """
        return RampRecord(
            id=raw.get("id") or "",
            project_id="",  # not in summary; call fetch_ramp(id) to get it
            project_name=raw.get("name"),
            requester_name="",
            summary=raw.get("alias") or "",
            submitted_at=raw.get("startDate") or "",
            updated_at=raw.get("endDate") or "",
            status=raw.get("status") or "draft",
            linear_issue_id=None,
            linear_url=None,
            cohorts=[],  # always empty — drill into fetch_ramp(id) for cohorts
        )

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
            job_post_domain=raw.get("job_post_domain"),
            job_post_language_code=raw.get("job_post_language_code"),
            campaign_state=raw.get("campaign_state"),
        )

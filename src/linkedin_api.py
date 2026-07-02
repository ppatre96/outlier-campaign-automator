"""
LinkedIn Marketing API client.
  Stage C  — Audience Counts validation
  Campaign — Create campaign + campaign group
  Creative — Upload image + create adCreative + attach to campaign
  Auth     — Auto-refresh access token on 401 using refresh token
"""
import html
import logging
import mimetypes
import os
import re
import time
from pathlib import Path
from typing import Any, Literal, Optional
from urllib.parse import quote, unquote

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
        import threading
        self._token = token
        self._session = requests.Session()
        self._session.headers.update({
            "Authorization": f"Bearer {token}",
            "LinkedIn-Version": config.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type": "application/json",
        })
        # Phase 3.3 — serialize token refresh across threads. Both the
        # InMail and Static arms (now run concurrently) share this
        # LinkedInClient instance; if both hit a 401 simultaneously they
        # would each kick off a refresh_access_token() call, write competing
        # tokens back to .env, and could leave the session header pointing
        # at a stale value. The lock ensures only one thread refreshes; the
        # other waits and inherits the new token.
        self._refresh_lock = threading.Lock()
        # Phase 3.4 — guard reads of `_session.headers["Authorization"]`
        # against concurrent rewrites by `_refresh_and_retry`. Lock is held
        # ONLY long enough to snapshot the header; never during the HTTP
        # call (an earlier version did, and a hanging LinkedIn DSC call
        # deadlocked the whole pipeline in 2026-05-13's GMR-0020 run).
        self._session_lock = threading.Lock()

    # ── helpers ───────────────────────────────────────────────────────────────

    def _url(self, path: str) -> str:
        return f"{config.LINKEDIN_API_BASE}/{path.lstrip('/')}"

    def _raise_for_status(self, resp: requests.Response, context: str) -> None:
        if not resp.ok:
            log.error("%s failed %d: %s", context, resp.status_code, resp.text[:500])
            resp.raise_for_status()

    def _refresh_and_retry(self, method: str, url: str, **kwargs) -> requests.Response:
        """Refresh the access token and retry a failed request once."""
        # Serialize the refresh + session-header update across threads.
        with self._refresh_lock:
            new_token = refresh_access_token()
            self._token = new_token
            self._session.headers.update({"Authorization": f"Bearer {new_token}"})
        # Apply pinned auth to this retry WITHOUT holding the session_lock
        # during the HTTP call — see _req for the deadlock motivation.
        user_headers = kwargs.pop("headers", None) or {}
        merged_headers = {**user_headers, "Authorization": f"Bearer {new_token}"}
        kwargs.setdefault("timeout", 60)
        return self._session.request(method, url, headers=merged_headers, **kwargs)

    def _default_headers(self) -> dict:
        """Return a copy of the default request headers for one-off calls that bypass _session."""
        return {
            "Authorization":             f"Bearer {self._token}",
            "LinkedIn-Version":          config.LINKEDIN_VERSION,
            "X-Restli-Protocol-Version": "2.0.0",
            "Content-Type":              "application/json",
        }

    def _req(self, method: str, url: str, **kwargs) -> requests.Response:
        """Make a request; auto-refresh and retry once on 401.

        Concurrency model: under the session_lock, snapshot the Authorization
        header (a string copy — releases instantly). Then make the HTTP call
        UNLOCKED with that pinned header passed via the per-request `headers=`
        kwarg. This eliminates the race against `_refresh_and_retry` mutating
        `_session.headers["Authorization"]` mid-request, WITHOUT serializing
        the actual network call (an earlier Phase 3.4 implementation held the
        lock during transit and deadlocked the entire LinkedIn arm whenever
        a single request stalled — see GMR-0020 run 2026-05-13 post-mortem).

        A 60-second timeout is set on every request as a hard ceiling; without
        it, a stalled DSC POST (e.g. MDP-gated 403 returning slowly) would
        hang the calling worker indefinitely.
        """
        with self._session_lock:
            pinned_auth = self._session.headers.get("Authorization", f"Bearer {self._token}")
        user_headers = kwargs.pop("headers", None) or {}
        merged_headers = {**user_headers, "Authorization": pinned_auth}
        kwargs.setdefault("timeout", 60)
        resp = self._session.request(method, url, headers=merged_headers, **kwargs)
        if resp.status_code == 401 and config.LINKEDIN_REFRESH_TOKEN and config.LINKEDIN_CLIENT_ID:
            log.warning("LinkedIn 401 — attempting token refresh")
            resp = self._refresh_and_retry(method, url, **kwargs)
        return resp

    # ── Targeting catalog: live typeahead ──────────────────────────────────────

    def typeahead_facet(
        self,
        facet_api_name: str,
        query: str,
        locale: tuple[str, str] = ("en", "US"),
        limit: int = 10,
    ) -> list[dict[str, str]]:
        """Live typeahead against LinkedIn's ad-targeting catalog.

        Returns up to `limit` matches as [{name, urn}, ...] ranked by LinkedIn's
        typeahead score. Used as a fallback by UrnResolver when the cached URN
        sheet doesn't include a newer/less-common facet value (e.g. "UX Engineer"
        — Campaign Manager's UI finds it instantly, but our cached snapshot
        didn't have an exact entry so fuzzy match either misfired or skipped to
        a skill-fallback that overcounted by 100x). Returns [] on error.

        Endpoint: GET /rest/adTargetingEntities?q=typeahead
          - q=typeahead (NOT typeaheadV2; that's the v2 finder name)
          - queryVersion=QUERY_USES_URNS so response has `urn` + `name` fields
          - locale as RAW Rest.li tuple `(language:en,country:US)` — no URL
            encoding (LinkedIn rejects encoded form here, unlike audienceCounts)

        facet_api_name: short name like "titles", "skills" — prepended with
                        `urn:li:adTargetingFacet:` to form the facet URN.
        """
        facet_urn = f"urn:li:adTargetingFacet:{facet_api_name}"
        encoded_facet = facet_urn.replace(":", "%3A")
        encoded_query = requests.utils.quote(query)
        lang, country = locale
        # Locale is a RAW Rest.li tuple; do NOT percent-encode the colons
        # or parens. The `&` and `,` inside the value are fine because there
        # are no nested params (verified against /rest/adTargetingEntities
        # 202506: encoded variants return PARAM_INVALID).
        locale_param = f"(language:{lang},country:{country})"
        url = (
            f"{self._url('adTargetingEntities')}"
            f"?q=typeahead"
            f"&facet={encoded_facet}"
            f"&query={encoded_query}"
            f"&queryVersion=QUERY_USES_URNS"
            f"&locale={locale_param}"
        )
        try:
            resp = self._req("GET", url)
            if not resp.ok:
                log.warning(
                    "typeahead %s='%s' failed %d: %s",
                    facet_api_name, query, resp.status_code, resp.text[:200],
                )
                return []
            elements = resp.json().get("elements", [])
            return [
                {"name": el.get("name", ""), "urn": el.get("urn", "")}
                for el in elements[:limit]
                if el.get("urn")
            ]
        except Exception as exc:
            log.warning("typeahead %s='%s' exception: %s", facet_api_name, query, exc)
            return []

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
    # Read from config (empty by default) so names match the Smart Ramp
    # nomenclature verbatim across all platforms. Was hardcoded "agent_".
    AGENT_NAME_PREFIX = config.AGENT_NAME_PREFIX

    def _prefixed(self, name: str) -> str:
        """Return name with AGENT_NAME_PREFIX prepended (idempotent). No-op when
        the prefix is empty (the default)."""
        if not self.AGENT_NAME_PREFIX or name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        """
        Create a sponsored content campaign group.
        Returns the campaign group URN. Name auto-prefixed with "agent_".

        `geos` is accepted for interface parity with the Meta arm (which needs
        the country list at campaign level for SAC); LinkedIn ignores it.
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

    def get_or_create_staging_group(self, name: str = "agent") -> str:
        """Return the URN of the single shared DRAFT staging campaign group
        named `name` (default "agent"), creating it once if absent.

        Reviewer feedback (GMR-0024, 2026-06-11): the pipeline used to spin up a
        fresh campaign group per ramp/row, cluttering Campaign Manager. Instead,
        every agent-built campaign lands as a DRAFT inside ONE general "agent"
        group; a human then duplicates each into the correct active group. The
        per-campaign Smart Ramp name keeps them identifiable inside the group.
        Idempotent across runs — looks up the existing group by name first.
        """
        target = self._prefixed(name)
        try:
            url = self._url(
                f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaignGroups"
                f"?q=search&search=(name:(values:List({quote(target)})))&count=100"
            )
            resp = self._req("GET", url)
            if resp.ok:
                for el in resp.json().get("elements", []):
                    if str(el.get("name", "")).strip() == target:
                        gid = el.get("id")
                        urn = f"urn:li:sponsoredCampaignGroup:{gid}"
                        log.info("Reusing staging group %s (name=%s)", urn, target)
                        return urn
        except Exception as exc:
            log.warning("staging-group lookup failed (%s) — creating a new one", exc)
        return self.create_campaign_group(name)

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

    def get_creative(self, creative_urn_or_id: str) -> dict:
        """GET a sponsoredCreative. Used to resolve a Message Ad creative's
        backing inMailContents URN (`content.reference`). Uses LinkedIn-Version
        202506 to match the InMail create path."""
        from urllib.parse import quote
        cid = creative_urn_or_id
        if not cid.startswith("urn:li:"):
            cid = f"urn:li:sponsoredCreative:{cid}"
        headers = self._default_headers()
        headers["LinkedIn-Version"] = "202506"
        resp = self._req(
            "GET",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives/{quote(cid, safe='')}"),
            headers=headers,
        )
        self._raise_for_status(resp, "getCreative")
        return resp.json()

    def rename_inmail_content(self, content_urn_or_id: str, new_name: str) -> None:
        """Rename an existing inMailContents object (the InMail ad name shown in
        Campaign Manager) via Rest.li PARTIAL_UPDATE. Unlike rename_campaign this
        does NOT prefix — the caller passes the full pipe-delimited spec name.
        Uses LinkedIn-Version 202506 (same as the InMail create path).

        The resource is keyed by the FULL adInMailContent URN (URL-encoded), not
        the bare numeric id — a numeric key 400s ("Key parameter value invalid")."""
        from urllib.parse import quote
        urn = content_urn_or_id
        if not urn.startswith("urn:li:"):
            urn = f"urn:li:adInMailContent:{urn}"
        headers = self._default_headers()
        headers["LinkedIn-Version"] = "202506"
        headers["X-RestLi-Method"] = "PARTIAL_UPDATE"
        resp = self._req(
            "POST",
            self._url(f"inMailContents/{quote(urn, safe='')}"),
            json={"patch": {"$set": {"name": new_name[:255]}}},
            headers=headers,
        )
        self._raise_for_status(resp, "renameInMailContent")
        log.info("Renamed inMailContent %s → %s", urn, new_name)

    def update_campaign_budget(
        self,
        campaign_id_or_urn: str,
        daily_budget_cents: int,
    ) -> None:
        """Phase 7 — PATCH a sponsored campaign's dailyBudget.

        `daily_budget_cents` is dollars-cents (e.g. 5000 = $50/day). LinkedIn's
        dailyBudget is a Money object `{currencyCode, amount}` with `amount`
        formatted as a decimal string in MAJOR units ($50.00, not 5000).

        A budget of 0 effectively pauses spend without flipping campaign status
        — useful for the Accept-action='pause' flow in the console: setting
        dailyBudget=$0 stops delivery while keeping the campaign in its current
        state so it can be reactivated without losing the URN.

        Caveat: LinkedIn enforces a minimum daily budget (~$10/day) on most
        objectives. Sending 0 may 422 with FIELD_VALUE_NOT_ALLOWED in some
        accounts. The caller should fall through to a real pause (status PATCH)
        when this returns non-2xx.
        """
        if daily_budget_cents < 0:
            raise ValueError(f"daily_budget_cents must be ≥ 0, got {daily_budget_cents}")
        campaign_id = str(campaign_id_or_urn).rsplit(":", 1)[-1]
        # LinkedIn Money.amount is a decimal string in MAJOR currency units.
        amount_str = f"{daily_budget_cents / 100:.2f}"
        payload = {
            "patch": {
                "$set": {
                    "dailyBudget": {"currencyCode": "USD", "amount": amount_str}
                }
            }
        }
        resp = self._req(
            "POST",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{campaign_id}"),
            json=payload,
            headers={"X-RestLi-Method": "PARTIAL_UPDATE", "Content-Type": "application/json"},
        )
        self._raise_for_status(resp, "updateCampaignBudget")
        log.info(
            "LinkedIn campaign %s dailyBudget → $%s/day",
            campaign_id, amount_str,
        )

    def get_campaign(self, campaign_urn_or_id: str) -> dict:
        """Fetch full campaign JSON from LinkedIn API (includes targetingCriteria)."""
        campaign_id = str(campaign_urn_or_id).rsplit(":", 1)[-1]
        resp = self._req(
            "GET",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns/{campaign_id}"),
        )
        self._raise_for_status(resp, "getCampaign")
        return resp.json()

    def attach_conversion_to_campaign(
        self, campaign_urn: str, conversion_id: int | None = None, *, max_attempts: int = 3,
    ) -> bool:
        """Associate a LinkedIn conversion with a sponsored campaign.

        WEBSITE_CONVERSION campaigns require at least one conversion attached
        (otherwise LinkedIn doesn't optimize / report). We attach by appending
        `campaign_urn` to the conversion's `campaigns` list via PATCH on
        `/conversions/{id}` (Rest.li PARTIAL_UPDATE). Idempotent — skips if
        the campaign is already linked.

        Robustness (2026-06-09): the shared conversion's `campaigns` array has
        grown large (600+), and `$set`-ing the whole array makes LinkedIn return
        an intermittent 500 — but the write usually STILL lands. A bare 500 was
        a false-negative (logged "failed" while the campaign was actually
        linked), so a real miss looked identical to a transient one. We now:
          1. retry the PATCH on 5xx / network error (bounded, with backoff),
          2. after any PATCH error, re-GET and VERIFY whether the campaign is
             linked anyway (catches the landed-despite-500 case),
          3. give up immediately on 4xx (won't fix itself),
          4. do a final verify so the return value reflects ground truth.

        Returns True iff the campaign is confirmed linked. Non-fatal (logged).
        Set LINKEDIN_CONVERSION_ID=0 to disable auto-attach globally.
        """
        cid = conversion_id if conversion_id is not None else config.LINKEDIN_CONVERSION_ID
        if not cid:
            log.debug("attach_conversion_to_campaign: LINKEDIN_CONVERSION_ID=0 — skipping")
            return False
        url = f"{config.LINKEDIN_API_BASE}/conversions/{cid}"

        def _is_linked() -> bool | None:
            """True/False if the campaigns list is readable; None on read failure."""
            try:
                r = self._req("GET", url)
                if not r.ok:
                    return None
                return campaign_urn in (r.json().get("campaigns", []) or [])
            except Exception:
                return None

        for attempt in range(1, max(1, max_attempts) + 1):
            # Fetch current campaigns list. Required because PATCH $set replaces
            # the whole array — must include existing entries to preserve them.
            try:
                resp = self._req("GET", url)
                self._raise_for_status(resp, "getConversion")
                current = resp.json().get("campaigns", []) or []
            except Exception as exc:
                log.warning("attach_conversion: getConversion failed (attempt %d/%d): %s",
                            attempt, max_attempts, exc)
                if attempt < max_attempts:
                    time.sleep(min(2 ** (attempt - 1), 5))
                    continue
                return False

            if campaign_urn in current:
                log.info("conversion %d already linked to %s — skipping", cid, campaign_urn)
                return True

            patch_headers = self._default_headers()
            patch_headers["X-RestLi-Method"] = "PARTIAL_UPDATE"
            payload = {"patch": {"$set": {"campaigns": current + [campaign_urn]}}}
            try:
                resp = self._req("POST", url, json=payload, headers=patch_headers)
            except Exception as exc:
                resp = None
                log.warning("attach_conversion: PATCH raised (attempt %d/%d): %s",
                            attempt, max_attempts, exc)

            if resp is not None and resp.ok:
                log.info("attached conversion %d to %s (now %d campaigns linked)",
                         cid, campaign_urn, len(current) + 1)
                return True

            # PATCH failed/raised. The write often lands despite a 500 on the
            # large-array $set — verify before deciding it failed.
            if _is_linked() is True:
                log.info("attach_conversion: %s linked to %d despite PATCH %s — verified",
                         campaign_urn, cid, (resp.status_code if resp is not None else "exception"))
                return True

            status = resp.status_code if resp is not None else None
            if status is not None and 400 <= status < 500:
                log.warning("attach_conversion: PATCH %s (4xx, not retrying): %s",
                            status, (resp.text[:200] if resp is not None else ""))
                return False
            log.warning("attach_conversion: attempt %d/%d failed (status=%s) — retrying",
                        attempt, max_attempts, status)
            if attempt < max_attempts:
                time.sleep(min(2 ** (attempt - 1), 5))

        # Retries exhausted — final ground-truth check.
        if _is_linked() is True:
            return True
        log.error("attach_conversion_to_campaign(%s, %s): FAILED after %d attempts — NOT linked",
                  campaign_urn, cid, max_attempts)
        return False

    def get_account_reference_urn(self) -> str:
        """Return the ad account's `reference` field — the URN of the LinkedIn
        organization that owns the account. Required as the `sender` for
        Sponsored InMail creatives (LinkedIn rejects person URNs with
        SINMAIL_SENDER_NOT_APPROVED). Cached after first call.
        """
        cached = getattr(self, "_account_reference_urn", None)
        if cached:
            return cached
        headers = self._default_headers()
        headers["LinkedIn-Version"] = "202506"
        resp = self._req(
            "GET",
            self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}"),
            headers=headers,
        )
        self._raise_for_status(resp, "getAdAccount")
        ref = resp.json().get("reference") or ""
        if not ref:
            raise RuntimeError(
                "LinkedIn ad account has no `reference` field — InMail sender "
                "URN cannot be auto-derived. Set LINKEDIN_INMAIL_SENDER_URN "
                "manually to the owning organization URN."
            )
        self._account_reference_urn = ref
        log.info("Resolved ad account reference URN: %s", ref)
        return ref

    def clone_campaign(self, source_urn: str, new_name: str) -> str:
        """
        Create a new DRAFT campaign by cloning the targeting criteria from an
        existing campaign. The new campaign is placed in the same campaign group.
        Returns new campaign URN.
        """
        source = self.get_campaign(source_urn)
        targeting    = source.get("targetingCriteria") or {}
        group_urn    = source.get("campaignGroup") or ""
        daily_budget = source.get("dailyBudget") or {"currencyCode": "USD", "amount": "100.00"}
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
        daily_budget_cents: int = 10000,
        exclude_facet_urns: dict[str, list[str]] | None = None,
        campaign_state: dict | None = None,
        conversion_id: int | None = None,
    ) -> str:
        """
        Create a Sponsored Content campaign with the given targeting.
        Returns the campaign URN. Name auto-prefixed with "agent_".

        `conversion_id` overrides the conversion attached for optimization. When
        None, the configured default (`LINKEDIN_CONVERSION_ID`) is attached.
        Callers pass the per-pod WS Grant rule so the campaign optimizes on
        worker_skill_grant (see config.LINKEDIN_POD_CONVERSION_IDS).

        `exclude_facet_urns` is an optional `{facet: [urns]}` map of negation
        targeting (recruiters/sales/etc.) — emitted as the `exclude` block of
        targetingCriteria. See `config.DEFAULT_EXCLUDE_FACETS`.

        `campaign_state` is an optional dict from Smart Ramp's
        `formData.cohorts[].campaign_state`. If `campaign_state.linkedin`
        contains `liAdLanguage` (e.g. "EN") or `mainCountry` (e.g. "GB"), they
        override the default locale of US/en. Channel-manager-saved overrides
        win over pipeline defaults. liTargetingFacet + liAdFormat are NOT
        consumed here yet — they're surfaced in campaign naming via
        campaign_name.py but don't affect actual targeting / format selection.
        TODO: wire liTargetingFacet into the targeting block + route liAdFormat
        to the InMail arm dispatch in main.py.
        """
        name = self._prefixed(name)
        targeting = _build_targeting_criteria(facet_urns, exclude_facet_urns)
        locale = _locale_from_campaign_state(campaign_state)
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
            "locale":                 locale,
            # WEBSITE_CONVERSION matches Outlier's standard for production
            # Sponsored Content (78% of active campaigns; per
            # 2026-05-08 ad-account audit). Attached conversion is
            # auto-linked below via attach_conversion_to_campaign().
            "objectiveType":          "WEBSITE_CONVERSION",
            "offsiteDeliveryEnabled": False,
            "politicalIntent":        "NOT_POLITICAL",
            "runSchedule":            {"start": _now_ms()},
        }
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/adCampaigns"), json=payload)
        self._raise_for_status(resp, "createCampaign")
        campaign_id = resp.headers.get("x-linkedin-id") or _id_from_location(resp)
        urn = f"urn:li:sponsoredCampaign:{campaign_id}"
        log.info("Created campaign %s '%s'", urn, name)
        # Auto-attach the conversion when objective is WEBSITE_CONVERSION.
        # Required for LinkedIn to optimize + report on conversion events.
        # `conversion_id` (per-pod WS Grant) overrides the default; None →
        # LINKEDIN_CONVERSION_ID.
        if payload.get("objectiveType") == "WEBSITE_CONVERSION":
            self.attach_conversion_to_campaign(urn, conversion_id)
        return urn

    # ── Image upload ───────────────────────────────────────────────────────────

    def upload_image(self, image_path: str | Path) -> str:
        """
        Upload an image asset to LinkedIn using the Images API (REST).
        Returns the image URN (urn:li:image:...).
        """
        image_path = Path(image_path)

        # Reject thumbnail-resolution creatives before upload (GMR-0023: 64×64
        # variants rendered pixelated). Raises → static arm's verify-and-heal
        # surfaces the reason instead of shipping a pixelated ad.
        from src.image_adapter import assert_min_dimensions
        assert_min_dimensions(image_path, config.MIN_CREATIVE_DIMENSION, platform="linkedin")

        # Step 1: initialize upload.
        # The image owner MUST match the DSC post author (set in _create_image_ad_inner).
        # 2026-06-03: DSC posts require an ORGANIZATION URN as author (confirmed by
        # LinkedIn support — see _create_image_ad_inner for full root cause). So the
        # image owner is the org URN, not the member URN. The token user must have
        # Sponsored Content Poster (or Super Admin) on this organization to upload.
        if not config.LINKEDIN_ORG_ID:
            raise RuntimeError(
                "LINKEDIN_ORG_ID is not set. DSC image uploads (and the resulting "
                "static-ad posts) require an organization URN as both image owner "
                "and post author — set LINKEDIN_ORG_ID in Doppler to the Outlier "
                "company page numeric ID (e.g. 92583550)."
            )
        org_urn = f"urn:li:organization:{config.LINKEDIN_ORG_ID}"
        init_payload = {
            "initializeUploadRequest": {
                "owner": org_urn,
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

        # Step 3: poll image processing status until AVAILABLE.
        # LinkedIn returns the image URN immediately after PUT but the image
        # takes a few seconds to be indexed before it can be referenced in a
        # /rest/posts payload. Referencing too early returns a generic 404
        # "Could not find entity" on the DSC post call.
        # Statuses: WAITING_UPLOAD → PROCESSING → AVAILABLE (or FAILED).
        # Budget: up to 30s, 1s intervals.
        status_url = self._url(f"images/{image_urn}")
        for attempt in range(30):
            time.sleep(1)
            status_resp = self._req("GET", status_url)
            if not status_resp.ok:
                # Some LinkedIn-Version combos return 404 here briefly while
                # the upload settles; keep polling until our overall budget runs out.
                log.debug("Image status probe attempt %d: %d", attempt, status_resp.status_code)
                continue
            status = (status_resp.json() or {}).get("status")
            if status == "AVAILABLE":
                log.info("Uploaded image %s → %s (ready in ~%ds)", image_path.name, image_urn, attempt + 1)
                return image_urn
            if status == "FAILED":
                raise RuntimeError(f"LinkedIn image processing FAILED for {image_urn}")
            log.debug("Image %s status=%s, polling (attempt %d)", image_urn, status, attempt)
        # Fell through 30s budget — return URN anyway and let the caller's
        # DSC post attempt fail loudly if the image really isn't ready.
        log.warning("Image %s not AVAILABLE after 30s — proceeding anyway", image_urn)
        return image_urn

    # ── InMail Campaign ────────────────────────────────────────────────────────

    def create_inmail_campaign(
        self,
        name: str,
        campaign_group_urn: str,
        facet_urns: dict[str, list[str]],
        daily_budget_cents: int = 10000,
        exclude_facet_urns: dict[str, list[str]] | None = None,
        conversion_id: int | None = None,
    ) -> str:
        """
        Create a Sponsored InMail (Message Ad) campaign.
        facet_urns keys must be full facet URNs (urn:li:adTargetingFacet:titles, etc.)
        Returns the campaign URN. `exclude_facet_urns` is the negation analog —
        see `create_campaign` and `config.DEFAULT_EXCLUDE_FACETS`.

        `conversion_id` overrides the conversion attached for optimization (None →
        LINKEDIN_CONVERSION_ID). Callers pass the per-pod WS Grant rule so the
        campaign optimizes on worker_skill_grant.
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
            # WEBSITE_CONVERSION — Outlier's standard for InMail. 100% of the
            # 1500 ACTIVE InMail campaigns audited 2026-05-08 use this
            # objective. Requires a conversion attached via
            # attach_conversion_to_campaign() — done after create_campaign /
            # create_inmail_campaign returns.
            "objectiveType":         "WEBSITE_CONVERSION",
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
        # WEBSITE_CONVERSION campaigns require a conversion attached for LinkedIn
        # to optimize / report. `conversion_id` (per-pod WS Grant) overrides the
        # default; None → LINKEDIN_CONVERSION_ID ("OCP Complete").
        if payload.get("objectiveType") == "WEBSITE_CONVERSION":
            self.attach_conversion_to_campaign(urn, conversion_id)
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
        ad_name: str | None = None,
    ) -> str:
        """
        Create a LinkedIn Message Ad creative and attach it to a campaign.
        Two-step: (1) create inMailContent via REST API, (2) create creative referencing it.
        Uses /rest/inMailContents (no MDP needed) with LinkedIn-Version: 202506 header.

        sender_urn: must be the URN of the LinkedIn ORGANIZATION that owns the
                    ad account (e.g. `urn:li:organization:92583550`). Person
                    URNs are rejected with SINMAIL_SENDER_NOT_APPROVED. If a
                    person URN or empty string is passed, this method
                    auto-derives the org URN from the ad account's `reference`
                    field via `get_account_reference_urn()`.
        ad_name:    human-readable name for the inMailContents object (what
                    shows as the ad name in Campaign Manager). Callers pass the
                    pipe-delimited campaign-spec name + angle so it's legible to
                    a person/LLM (e.g. "Scale-GMR-0023 | LinkedIn | language |
                    kn-IN | … | Message ads | 06/11/2026 | Angle A"). Falls back
                    to a timestamp token only when not supplied.
        Returns the sponsoredCreative URN.
        """
        dest = destination_url or config.LINKEDIN_DESTINATION

        # Sender resolution. An EMPTY sender always falls back to the org URN
        # that owns the ad account (the always-approved default). A non-empty
        # sender — the org, or an explicitly chosen APPROVED person (a reviewer's
        # pick from the console) — is tried AS-IS: LinkedIn validates approval on
        # the inMailContents POST below, and we fall back to the org URN + retry
        # if it rejects the sender as unapproved. (A person must first be added
        # as an approved sender on the ad account in Campaign Manager.)
        if not sender_urn:
            sender_urn = self.get_account_reference_urn()

        # Step 1 — create the InMail content object via REST API (no MDP required)
        content_payload = {
            "account": f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            # Human-readable name (caller passes the campaign-spec name + angle).
            # Fall back to a timestamp token only when no name is supplied so the
            # object still gets a unique label. LinkedIn caps the field, so trim.
            "name": (ad_name.strip()[:255] if ad_name and ad_name.strip()
                     else f"inmail_{int(__import__('time').time())}"),
            "sender": sender_urn,
            "htmlBody": _inmail_html_body(body),
            "subject": subject[:60],
            "subContent": {
                "regular": {
                    "callToActionText": cta_label[:20],
                    "callToActionLandingPageUrl": dest,
                }
            }
        }
        # Custom footer / Terms & Conditions (reviewer feedback GMR-0024). Set
        # only when configured so the field isn't sent empty. Text lives in
        # config.LINKEDIN_INMAIL_FOOTER (Doppler-overridable).
        footer = (getattr(config, "LINKEDIN_INMAIL_FOOTER", "") or "").strip()
        if footer:
            content_payload["customFooter"] = footer
        content_headers = self._default_headers()
        content_headers["LinkedIn-Version"] = "202506"

        import requests as _req_lib
        resp = _req_lib.post("https://api.linkedin.com/rest/inMailContents", json=content_payload, headers=content_headers)
        # If the chosen sender isn't an approved sender on the account, LinkedIn
        # rejects with SINMAIL_SENDER_NOT_APPROVED. Fall back to the org URN
        # (always approved) and retry once so the InMail still ships — with a
        # loud warning so the reviewer knows their pick didn't stick.
        if resp.status_code >= 400 and "SINMAIL_SENDER_NOT_APPROVED" in (resp.text or ""):
            org_urn = self.get_account_reference_urn()
            if sender_urn != org_urn:
                log.warning(
                    "InMail sender %s is not an approved sender on the ad account "
                    "— falling back to org URN %s. Add it as an approved sender in "
                    "Campaign Manager to use it.",
                    sender_urn, org_urn,
                )
                content_payload["sender"] = org_urn
                sender_urn = org_urn
                resp = _req_lib.post(
                    "https://api.linkedin.com/rest/inMailContents",
                    json=content_payload, headers=content_headers,
                )
        self._raise_for_status(resp, "createInMailContent")
        # x-restli-id sometimes contains a bare numeric id, sometimes a full URN
        # (e.g. "urn:li:adInMailContent:214000216"). Normalize to a full URN.
        raw = resp.headers.get("x-restli-id") or _id_from_location(resp) or ""
        if raw.startswith("urn:li:"):
            content_urn = raw
        else:
            content_urn = f"urn:li:adInMailContent:{raw}"
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
        # Same normalisation as for adInMailContent: x-restli-id may be a bare
        # numeric id or a full URN — return a full URN either way.
        raw = resp.headers.get("x-restli-id") or _id_from_location(resp) or ""
        urn = raw if raw.startswith("urn:li:") else f"urn:li:sponsoredCreative:{raw}"
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
        ad_name: str = "",
    ) -> ImageAdResult:
        """
        Create a Single Image Ad creative and attach it to a campaign.

        Returns ImageAdResult — NEVER raises for the LINKEDIN_ORG_ID /
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
                ad_name=ad_name,
            )
            return ImageAdResult(creative_urn=urn, status="ok")
        except RuntimeError as exc:
            msg = str(exc)
            if "LINKEDIN_ORG_ID" in msg or "LINKEDIN_MEMBER_URN" in msg:
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
        ad_name: str = "",
    ) -> str:
        """
        Inner raise-based implementation of create_image_ad. Returns the
        adCreative URN on success; raises RuntimeError or HTTPError on
        failure. Public wrapper translates exceptions into ImageAdResult.

        Flow:
        1. Create a Direct Sponsored Content (DSC) post via /rest/posts.
           DSC posts are dark — never shown organically, only as ads.
           Author MUST be an organization URN (urn:li:organization:<page_id>) per
           LinkedIn DSC contract, NOT a person URN. The token user needs Sponsored
           Content Poster (or Super Admin) on that organization, AND ADMIN on the
           target ad account. (Confirmed by LinkedIn support 2026-06-03 — see the
           historical-blocker NOTE block below this for the prior misdiagnosis.)
        2. Create the creative referencing that post URN.

        Scope requirements: w_member_social + w_organization_social.
        """
        dest = destination_url or config.LINKEDIN_DESTINATION

        if not config.LINKEDIN_ORG_ID:
            raise RuntimeError(
                "LINKEDIN_ORG_ID is not set. DSC posts must use an organization URN "
                "as author — set LINKEDIN_ORG_ID in Doppler to the Outlier company "
                "page numeric ID (e.g. 92583550)."
            )
        org_urn = f"urn:li:organization:{config.LINKEDIN_ORG_ID}"

        # Step 1 — create DSC post via /rest/posts (LinkedIn API 202510).
        # lifecycleState=PUBLISHED + visibility=PUBLIC + feedDistribution=NONE
        # + adContext.dscAdAccount = a "dark post": published in the system so
        # it can be referenced by ad creatives, but never shown in feed
        # organically. (DRAFT posts can't be referenced from a creative →
        # createAdCreative returns a bare 500.)
        # Field priority for what shows to the user:
        #   commentary       = intro_text (above the image in feed, ≤140 chars preferred)
        #                      Falls back to `description` if intro_text is empty.
        #   article.title    = ad_headline (bold text BELOW image in feed, ≤70 chars)
        #                      Falls back to `headline` if ad_headline is empty.
        #   article.source   = destination_url (where the ad clicks through to)
        #   article.thumbnail= the uploaded image URN
        # Content shape: `article` (NOT `media`). `media` creates an image-only
        # post with no click destination — useless as an ad. `article` is the
        # canonical Sponsored Content single-image-ad shape with a click target.
        # cta_button is configured at campaign-attribute level (LinkedIn supports
        # a limited enum of CTAs that show as the button overlay on the ad).
        commentary  = (intro_text or description)[:700]
        article_title = (ad_headline or headline)[:200]
        dsc_payload = {
            "author":        org_urn,
            "commentary":    commentary,
            "visibility":    "PUBLIC",
            "lifecycleState": "PUBLISHED",
            "adContext": {
                "dscAdAccount": f"urn:li:sponsoredAccount:{config.LINKEDIN_AD_ACCOUNT_ID}",
            },
            "distribution": {
                "feedDistribution":             "NONE",
                "targetEntities":               [],
                "thirdPartyDistributionChannels": [],
            },
            "content": {
                "article": {
                    "source":      dest,
                    "thumbnail":   image_urn,
                    "title":       article_title,
                    **({"description": ad_description[:100]} if ad_description else {}),
                }
            },
        }
        # CTA button overlay — Posts API `contentCallToActionLabel` (e.g. "Apply").
        # The pipeline previously set NO CTA, so single image ads defaulted and the
        # label had to be set by hand per campaign. Validate against LinkedIn's
        # enum; default APPLY (Outlier job-ad standard) on any unknown value.
        _VALID_CTAS = {
            "APPLY", "DOWNLOAD", "VIEW_QUOTE", "LEARN_MORE", "SIGN_UP", "SUBSCRIBE",
            "REGISTER", "JOIN", "ATTEND", "REQUEST_DEMO", "SEE_MORE", "BUY_NOW", "SHOP_NOW",
        }
        _cta = (config.LINKEDIN_CTA_LABEL or "APPLY").strip().upper()
        dsc_payload["contentCallToActionLabel"] = _cta if _cta in _VALID_CTAS else "APPLY"
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
        # HISTORICAL NOTE (2026-05-09 → resolved 2026-06-03):
        # This call used to 403 reliably. We originally diagnosed it as a
        # LinkedIn Marketing Developer Platform (MDP) entitlement gap on the
        # OAuth app — that diagnosis was WRONG. LinkedIn support clarified
        # 2026-06-03 that the real causes were:
        #   (a) author was a person URN (LINKEDIN_MEMBER_URN); DSC posts must
        #       use an organization URN — fixed above by switching to org_urn.
        #   (b) image owner (in upload_image) was also a person URN; ownership
        #       validation failed because owner ≠ author once (a) is fixed.
        #       The owner is now the same org URN.
        #   (c) Out-of-band: the OAuth token user must be ADMIN on the ad
        #       account AND Sponsored Content Poster (or Super Admin) on the
        #       organization. Verify in LinkedIn Campaign Manager UI if 403
        #       returns. (`X-Restli-Id` won't be set on the response.)
        # Duplicate-detection 422 responses on retry still contain phantom URNs
        # (returned for spam-prevention fingerprinting) that 404 on creative
        # attach — they are NOT a viable workaround; let the 422 propagate.
        self._raise_for_status(dsc_resp, "createDscPost")
        # x-restli-id behavior depends on LinkedIn-Version: older versions return
        # just the numeric id; 202510+ returns the full URN. Normalize either shape
        # to a single urn:li:share:<id> form (avoid the double-prefix bug).
        post_id_or_urn = dsc_resp.headers.get("x-restli-id") or _id_from_location(dsc_resp)
        post_urn = (
            post_id_or_urn
            if post_id_or_urn and post_id_or_urn.startswith("urn:li:share:")
            else f"urn:li:share:{post_id_or_urn}"
        )
        log.info("Created DSC post %s", post_urn)

        # Step 2 — create creative referencing the DSC post.
        # In the modern /rest/adAccounts/{id}/creatives schema, `content` is a
        # UNION with exactly ONE variant — either `reference` (point at an
        # existing post) OR `inlineContent` (define the post inline). For DSC
        # the post already exists, so we use `reference` only. Attempting to
        # also set `inlineContent` (legacy attempt to attach CTA/landingPage
        # at the creative level) returns:
        #   /content :: DataMap should have no more than one entry for a union type
        # CTA + landing page + description for Sponsored Content image ads
        # live on the underlying POST (commentary + media.title), not on the
        # creative wrapper. If we need a distinct destination URL different
        # from the post's link, switch the post's content from `media` to
        # `article` (article.source = destination URL).
        # intendedStatus must match (or be lower-state than) the parent campaign's
        # status. Per feedback_linkedin_draft_default.md the pipeline keeps every
        # campaign DRAFT until a human flips it, so the creative starts DRAFT too.
        # Setting ACTIVE while campaign is DRAFT triggers a bare 500 from
        # /rest/adAccounts/{id}/creatives — LinkedIn's typical "wrong state" shape.
        payload = {
            "campaign":       campaign_urn,
            "intendedStatus": "DRAFT",
            "content":        {"reference": post_urn},
        }
        # Name the creative (per-ad name, e.g. "<campaign name> | A") so the A/B/C
        # variant is identifiable in Campaign Manager — the `name` field is a
        # writable creative attribute (same as on campaign create). Omitted when
        # blank to preserve the prior unnamed behavior. Mirrors the Meta arm's
        # ad_name and the InMail name.
        if ad_name and ad_name.strip():
            payload["name"] = ad_name.strip()[:255]
        resp = self._req("POST", self._url(f"adAccounts/{config.LINKEDIN_AD_ACCOUNT_ID}/creatives"), json=payload)
        self._raise_for_status(resp, "createAdCreative")
        # The DSC-static creative endpoint returns x-restli-id (not x-linkedin-id
        # — that's an older header) and the value is the FULL URN, often
        # URL-encoded ("urn%3Ali%3AsponsoredCreative%3A1431560786"). Decode and
        # use as-is; wrapping with a sponsoredCreative prefix produces the
        # double-prefix bug we saw on 2026-06-03.
        raw = (
            resp.headers.get("x-restli-id")
            or resp.headers.get("x-linkedin-id")
            or _id_from_location(resp)
            or ""
        )
        raw = unquote(raw)
        urn = raw if raw.startswith("urn:li:") else f"urn:li:sponsoredCreative:{raw}"
        log.info("Created adCreative %s", urn)
        return urn


# ── InMail body formatting ──────────────────────────────────────────────────────

def _inmail_html_body(text: str) -> str:
    """Render plain-text InMail copy as LinkedIn-style <p> paragraphs.

    LinkedIn's own Message Ad composer wraps each paragraph in <p>…</p>; raw
    `<br><br>` separators render literally in some inboxes (reviewer feedback
    GMR-0024, 2026-06-11). We strip any <br> the copy model emitted, split on
    blank lines, collapse intra-paragraph newlines, and wrap each paragraph.
    Caps total length at LinkedIn's ~1000-char htmlBody limit on a paragraph
    boundary so a tag is never cut mid-render.
    """
    text = re.sub(r"<br\s*/?>", "\n", text or "", flags=re.IGNORECASE)
    paras = [
        re.sub(r"\s*\n\s*", " ", p).strip()
        for p in re.split(r"\n\s*\n", text)
        if p.strip()
    ]
    out: list[str] = []
    total = 0
    for p in paras:
        seg = f"<p>{html.escape(p, quote=False)}</p>"
        if total + len(seg) > 1000:
            break
        out.append(seg)
        total += len(seg)
    return "".join(out)


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
    # Matched-audience and dynamic-audience segments — used by
    # config.DEFAULT_EXCLUDE_URNS_RAW to suppress historical contributor
    # lists and recent-signup audiences. These facets aren't fuzzy-resolved
    # via the URN sheet (no human-readable label space), so they only show
    # up as direct URN injection.
    "audienceMatchingSegments": "urn:li:adTargetingFacet:audienceMatchingSegments",
    "dynamicSegments":  "urn:li:adTargetingFacet:dynamicSegments",
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

def _locale_from_campaign_state(campaign_state: dict | None) -> dict:
    """Build the LinkedIn `locale` payload, honouring channel-manager overrides.

    Reads `campaign_state.linkedin.liAdLanguage` and
    `campaign_state.linkedin.mainCountry`. Each is independent — if only one is
    set the other falls back to the LinkedIn default (US / en). Returns the
    {"country": ..., "language": ...} shape LinkedIn's API expects.

    LinkedIn expects lowercase ISO-639 for language and uppercase ISO-3166 for
    country. We normalize accordingly so the channel manager can save either
    case ("EN" or "en", "gb" or "GB") without breaking the API call.
    """
    state = campaign_state or {}
    li = state.get("linkedin") if isinstance(state, dict) else None
    li = li if isinstance(li, dict) else {}
    language = (li.get("liAdLanguage") or "en").strip().lower() or "en"
    country = (li.get("mainCountry") or "US").strip().upper() or "US"
    return {"country": country, "language": language}


def _now_ms() -> int:
    import time
    return int(time.time() * 1000)


def _id_from_location(resp: requests.Response) -> str:
    loc = resp.headers.get("Location", "")
    return loc.rstrip("/").rsplit("/", 1)[-1]

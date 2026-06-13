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

  • Phase 2 — PROGRAMMATIC (REDDIT_API_ENABLED on). Implemented + live-validated
    against the v3 API 2026-06-13. Hierarchy maps to:
      create_campaign_group → POST /ad_accounts/{acct}/campaigns   (CONVERSIONS)
      create_campaign       → POST /ad_accounts/{acct}/ad_groups   (targeting+budget)
      upload_image          → oauth.reddit.com/api/media/asset.json → i.redd.it URL
      create_image_ad       → POST /profiles/{profile}/posts  +  POST .../ads
    All bodies use a {"data": {...}} envelope; budgets/bids are microcurrency
    ($1 = 1_000_000). The Ads API has NO media endpoint — a post ingests a public
    media_url, so the PNG is uploaded to Reddit's own media (asset.json), which
    requires the OAuth token to carry the `submit` scope (the ads-only scopes are
    not enough). conversion_pixel_id defaults to the account-level pixel; per-pod
    REDDIT_POD_CONVERSION_EVENTS feed attribution but the ad group optimizes for
    REDDIT_OPTIMIZATION_GOAL (SIGN_UP).

Everything will be created PAUSED (mirrors the LinkedIn DRAFT / Meta PAUSED
default). Names are auto-prefixed with `config.AGENT_NAME_PREFIX` (empty by
default — see config).
"""
from __future__ import annotations

import logging
from pathlib import Path
from typing import Any, Optional

import requests

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    REDDIT_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)

# Reddit Ads API v3 base + the OAuth host used only for media upload
# (asset.json — the Ads API has no media-upload endpoint; an ad post ingests a
# public media_url, so we upload the PNG to Reddit's own media to get an
# i.redd.it URL). Verified live 2026-06-13.
REDDIT_ADS_API_BASE = "https://ads-api.reddit.com/api/v3"
REDDIT_OAUTH_BASE   = "https://oauth.reddit.com"

_USER_AGENT = "outlier-campaign-agent/1.0 (by OutlierAI)"

# Reddit promoted-post CTA button enum (from the v3 OpenAPI spec). The copy
# adapter emits human labels; anything outside this set is remapped to Sign Up.
_REDDIT_VALID_CTAS = {
    "Apply Now", "Contact Us", "Download", "Get a Quote", "Install",
    "Learn More", "Order Now", "Play Now", "Shop Now", "Sign Up",
    "View More", "Watch Now", "Book Now", "Subscribe", "Read More",
}

_DISABLED_MSG = (
    "REDDIT_API_ENABLED is off — Reddit Ads API access is not yet granted. "
    "Creatives + targeting/conversion manifest were exported to Drive for "
    "manual upload in Reddit Ads Manager."
)


_REDDIT_TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
_ENV_FILE = Path(__file__).resolve().parent.parent / ".env"

def refresh_reddit_token(which: str = "ads") -> str:
    """Exchange the ADS refresh token for a fresh ads access token. Writes the
    new ACCESS/REFRESH back to .env so the CI post-step persists them to Doppler
    (mirrors the LinkedIn flow). Returns the new access token. (Media uses a
    password-grant mint instead — see RedditClient._get_media_token.)"""
    assert which == "ads", "only the ads token uses refresh_token grant"
    cid     = config.REDDIT_CLIENT_ID
    secret  = config.REDDIT_CLIENT_SECRET
    refresh = config.REDDIT_REFRESH_TOKEN
    if not all([cid, secret, refresh]):
        raise RuntimeError(
            "Cannot refresh Reddit ads token — REDDIT_CLIENT_ID, "
            "REDDIT_CLIENT_SECRET, and REDDIT_REFRESH_TOKEN must all be set."
        )
    resp = requests.post(
        _REDDIT_TOKEN_URL,
        auth=(cid, secret),
        data={"grant_type": "refresh_token", "refresh_token": refresh},
        headers={"User-Agent": _USER_AGENT},
        timeout=30,
    )
    if not resp.ok:
        log.error("Reddit ads token refresh failed %d: %s", resp.status_code, resp.text[:300])
        resp.raise_for_status()
    data = resp.json()
    new_access = data["access_token"]
    new_refresh = data.get("refresh_token", refresh)  # Reddit may rotate it
    _update_env_tokens("REDDIT_ACCESS_TOKEN", new_access, "REDDIT_REFRESH_TOKEN", new_refresh)
    log.info("Reddit ads access token refreshed")
    return new_access


def mint_reddit_media_token() -> str:
    """Mint a short-lived media token via Reddit's password grant (script app).
    The browser authorization_code flow is edge-blocked from CI/server contexts,
    but password grant is not. Returns the access token (~1h, no refresh — re-mint
    on demand). Requires a Reddit "script" app + a dedicated no-2FA account whose
    username == REDDIT_MEDIA_USERNAME and who is a developer of the app."""
    cid    = config.REDDIT_MEDIA_CLIENT_ID
    secret = config.REDDIT_MEDIA_CLIENT_SECRET
    user   = config.REDDIT_MEDIA_USERNAME
    pw     = config.REDDIT_MEDIA_PASSWORD
    if not all([cid, secret, user, pw]):
        raise RuntimeError(
            "Cannot mint Reddit media token — REDDIT_MEDIA_CLIENT_ID, "
            "REDDIT_MEDIA_CLIENT_SECRET, REDDIT_MEDIA_USERNAME, and "
            "REDDIT_MEDIA_PASSWORD must all be set (script app + dedicated account)."
        )
    resp = requests.post(
        _REDDIT_TOKEN_URL,
        auth=(cid, secret),
        data={"grant_type": "password", "username": user, "password": pw},
        headers={"User-Agent": _USER_AGENT},
        timeout=30,
    )
    # Reddit returns 200 with an {"error": ...} body for bad creds / wrong app type.
    data = resp.json() if resp.ok else {}
    if not resp.ok or "access_token" not in data:
        raise RuntimeError(
            f"Reddit media password grant failed → {resp.status_code}: {resp.text[:300]} "
            "(is it a `script` app, with REDDIT_MEDIA_USERNAME a developer of it + no 2FA?)"
        )
    log.info("Reddit media token minted via password grant")
    return data["access_token"]


def _update_env_tokens(access_k: str, access_v: str, refresh_k: str, refresh_v: str) -> None:
    """Overwrite the ACCESS/REFRESH keys in .env (best-effort; no-op without .env).
    Appends the key when absent so a fresh checkout still persists it."""
    import re
    if not _ENV_FILE.exists():
        return
    text = _ENV_FILE.read_text()
    for k, v in ((access_k, access_v), (refresh_k, refresh_v)):
        if re.search(rf"^{k}=", text, flags=re.MULTILINE):
            text = re.sub(rf"^{k}=.*$", f"{k}={v}", text, flags=re.MULTILINE)
        else:
            text += ("" if text.endswith("\n") else "\n") + f"{k}={v}\n"
    _ENV_FILE.write_text(text)


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
        import threading
        self._token = config.REDDIT_ACCESS_TOKEN
        self._account_id = config.REDDIT_AD_ACCOUNT_ID
        self._session = None  # lazily created in Phase 2
        self._media_token: Optional[str] = None  # minted on demand via password grant
        # Serialize token refresh so concurrent (cohort×geo) calls don't each
        # kick off a refresh and write competing tokens back to .env.
        self._refresh_lock = threading.Lock()

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

    # ── HTTP helpers ────────────────────────────────────────────────────────────

    def _http(self):
        """Lazily build a requests.Session carrying the bearer token + UA."""
        if self._session is None:
            s = requests.Session()
            s.headers.update({
                "Authorization": f"Bearer {self._token}",
                "User-Agent": _USER_AGENT,
            })
            self._session = s
        return self._session

    def _api(self, method: str, path: str, payload: dict | None = None, _retried: bool = False) -> dict:
        """Call the Ads API v3 with the `{"data": {...}}` envelope and return the
        unwrapped `data`. Refreshes the ads token + retries once on 401. Raises
        RuntimeError with the response body on other non-2xx."""
        url = f"{REDDIT_ADS_API_BASE}{path}"
        resp = self._http().request(
            method, url,
            json={"data": payload} if payload is not None else None,
            timeout=60,
        )
        if resp.status_code == 401 and not _retried and config.REDDIT_REFRESH_TOKEN:
            log.info("Reddit ads token 401 on %s %s — refreshing + retrying once", method, path)
            self._refresh_ads_token()
            return self._api(method, path, payload, _retried=True)
        if not resp.ok:
            raise RuntimeError(f"Reddit {method} {path} → {resp.status_code}: {resp.text[:400]}")
        body = resp.json() if resp.text else {}
        return body.get("data", body)

    def _refresh_ads_token(self) -> None:
        """Refresh the ads access token + update the session header (thread-safe)."""
        with self._refresh_lock:
            self._token = refresh_reddit_token("ads")
            if self._session is not None:
                self._session.headers.update({"Authorization": f"Bearer {self._token}"})

    def _get_media_token(self, force: bool = False) -> str:
        """Return a cached media token, minting one via password grant on first
        use or when `force` (token expired mid-run). Thread-safe."""
        with self._refresh_lock:
            if force or not self._media_token:
                self._media_token = mint_reddit_media_token()
        return self._media_token

    # ── lifecycle (AdPlatformClient) ────────────────────────────────────────────

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        """Create a Reddit Campaign (the top-level container, like Meta's
        Campaign). Returns the campaign id. PAUSED, CONVERSIONS objective.
        `geos` is unused on Reddit (geo is targeted at the ad-group level)."""
        self._ensure_init()
        data = self._api("POST", f"/ad_accounts/{self._account_id}/campaigns", {
            "name":                 self._prefixed(name),
            "objective":            "CONVERSIONS",
            "configured_status":    "PAUSED",
            "funding_instrument_id": config.REDDIT_FUNDING_INSTRUMENT_ID,
        })
        campaign_id = str(data["id"])
        log.info("Reddit campaign created %s (name=%s)", campaign_id, name)
        return campaign_id

    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int | None = None,
    ) -> str:
        """Create a Reddit Ad Group (targeting + budget, child of a Campaign).
        `targeting` is the RedditSubredditResolver dict
        ({geo_locations, subreddits, interests, keywords, pod}). Returns the
        ad-group id. PAUSED; MAXIMIZE_VOLUME CPC; daily-spend budget."""
        self._ensure_init()
        # Budget is microcurrency ($1 = 1_000_000). cents → micros = ×10_000.
        if daily_budget_cents is not None:
            goal_value = daily_budget_cents * 10_000
        else:
            goal_value = config.REDDIT_DEFAULT_DAILY_USD * 1_000_000

        rt = {
            "geolocations":         [g.upper() for g in (targeting.get("geo_locations") or []) if g],
            "communities":          list(targeting.get("subreddits") or []),
            "interests":            list(targeting.get("interests") or []),
            "excluded_communities": list(config.REDDIT_EXCLUDED_SUBREDDITS),
        }
        if targeting.get("keywords"):
            rt["keywords"] = list(targeting["keywords"])

        # Reddit requires a start_time on the ad group. The entity is PAUSED, so
        # "now" is harmless — delivery only begins once a human un-pauses it.
        from datetime import datetime, timezone
        start_time = datetime.now(timezone.utc).replace(microsecond=0).isoformat()

        data = self._api("POST", f"/ad_accounts/{self._account_id}/ad_groups", {
            "campaign_id":          campaign_group_id,
            "name":                 self._prefixed(name),
            "configured_status":    "PAUSED",
            "bid_strategy":         "MAXIMIZE_VOLUME",
            "bid_type":             "CPC",
            "bid_value":            int(config.REDDIT_DEFAULT_BID_USD * 1_000_000),
            "goal_type":            "DAILY_SPEND",
            "goal_value":           goal_value,
            "optimization_goal":    config.REDDIT_OPTIMIZATION_GOAL,
            "conversion_pixel_id":  config.REDDIT_PIXEL_ID or self._account_id,
            "view_through_conversion_type": "SEVEN_DAY_CLICKS",
            "start_time":           start_time,
            "targeting":            rt,
        })
        ad_group_id = str(data["id"])
        log.info(
            "Reddit ad group created %s (campaign=%s, %d communities, geos=%s, $%d/day)",
            ad_group_id, campaign_group_id, len(rt["communities"]),
            rt["geolocations"] or "—", goal_value // 1_000_000,
        )
        return ad_group_id

    def upload_image(self, image_path: str | Path) -> str:
        """Upload a PNG to Reddit's media (asset.json) and return the public
        i.redd.it URL to use as a post's media_url. The Ads API has no media
        endpoint and the ads token can't post, so the bytes go through a SEPARATE
        Reddit "script" app token (minted on demand via password grant —
        config.REDDIT_MEDIA_*) on oauth.reddit.com; Reddit re-hosts them on i.redd.it."""
        self._ensure_init()
        image_path = str(Path(image_path))
        from src.image_adapter import assert_min_dimensions
        assert_min_dimensions(image_path, config.MIN_CREATIVE_DIMENSION, platform="reddit")

        media_headers = {
            "Authorization": f"Bearer {self._get_media_token()}",
            "User-Agent": _USER_AGENT,
        }
        lease_url = f"{REDDIT_OAUTH_BASE}/api/media/asset.json"
        lease_data = {"filepath": Path(image_path).name, "mimetype": "image/png"}
        # 1. Ask Reddit for a presigned S3 lease (re-mint media token on 401).
        lease = requests.post(lease_url, data=lease_data, headers=media_headers, timeout=60)
        if lease.status_code == 401:
            log.info("Reddit media token 401 on asset.json — re-minting + retrying once")
            media_headers["Authorization"] = f"Bearer {self._get_media_token(force=True)}"
            lease = requests.post(lease_url, data=lease_data, headers=media_headers, timeout=60)
        if not lease.ok:
            raise RuntimeError(
                f"Reddit media lease failed → {lease.status_code}: {lease.text[:300]} "
                "(does the token have the `submit` scope?)"
            )
        args = lease.json()["args"]
        action = "https:" + args["action"] if args["action"].startswith("//") else args["action"]
        fields = {f["name"]: f["value"] for f in args["fields"]}

        # 2. PUT the bytes to S3 (no auth header — presigned form post).
        with open(image_path, "rb") as fh:
            up = requests.post(
                action,
                data=fields,
                files={"file": (Path(image_path).name, fh, "image/png")},
                timeout=120,
            )
        if up.status_code not in (200, 201):
            raise RuntimeError(f"Reddit media S3 upload failed → {up.status_code}: {up.text[:300]}")

        # 3. The public asset URL is the S3 object (action + key). Reddit ingests
        #    it into i.redd.it when the post is created.
        asset_url = f"{action.rstrip('/')}/{fields['key']}"
        log.info("Reddit image uploaded %s → %s", Path(image_path).name, asset_url)
        return asset_url

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
        ad_name: Optional[str] = None,
    ) -> CreateAdResult:
        """Create the promoted image post (headline=title, image, CTA) under the
        business profile, then create the Ad that references it under the ad
        group (`campaign_id` is actually the ad-group id, per the ABC). PAUSED.

        `image_id` is the public media URL returned by `upload_image`. The
        free-form `description`/`intro_text` are ABC-compat (the TEXT-post format
        consumes them); the IMAGE ad uses only headline + image + CTA + link."""
        # Creative-only mode: signal local_fallback so the arm logs the PNG +
        # manifest path and continues (expected v1 outcome until the API is on).
        if not config.REDDIT_API_ENABLED:
            return CreateAdResult(
                status="local_fallback",
                error_class="RedditApiDisabled",
                error_message=_DISABLED_MSG,
            )
        self._ensure_init()
        try:
            cta_label = (cta or "Sign Up").strip().title()
            if cta_label not in _REDDIT_VALID_CTAS:
                cta_label = "Sign Up"
            link = destination_url or config.LINKEDIN_DESTINATION

            # 1. Create the promoted image post under the profile. IMAGE posts
            #    carry no body (Reddit rejects a non-empty body on this type) —
            #    the free-form `description` is only used by the TEXT-post format.
            post = self._api("POST", f"/profiles/{config.REDDIT_PROFILE_ID}/posts", {
                "type":     "IMAGE",
                "headline": headline,
                "content":  [{
                    "media_url":       image_id,
                    "call_to_action":  cta_label,
                    "destination_url": link,
                }],
            })
            post_id = str(post["id"])

            # 2. Create the Ad referencing the post + ad group.
            ad = self._api("POST", f"/ad_accounts/{self._account_id}/ads", {
                "ad_group_id":       campaign_id,
                "name":              self._prefixed(ad_name or f"ad_{campaign_id}"),
                "configured_status": "PAUSED",
                "post_id":           post_id,
                "profile_id":        config.REDDIT_PROFILE_ID,
                "click_url":         link,
            })
            ad_id = str(ad["id"])
            log.info("Reddit ad created %s (ad group=%s, post=%s)", ad_id, campaign_id, post_id)
            return CreateAdResult(creative_id=ad_id, status="ok")
        except Exception as exc:
            log.error("Reddit create_image_ad failed: %s", exc)
            return CreateAdResult(
                status="error",
                error_class=type(exc).__name__,
                error_message=str(exc)[:300],
            )

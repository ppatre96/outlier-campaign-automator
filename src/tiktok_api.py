"""
TikTok Ads (Marketing API v1.3) client.

Implements `AdPlatformClient` so the Outlier campaign pipeline can dispatch to
TikTok the same way it does LinkedIn / Meta / Google / Reddit. TikTok's
hierarchy is Campaign → Ad Group → Ad, mirroring Meta (Campaign → Ad Set → Ad).

TWO-PHASE rollout (see config.TIKTOK_API_ENABLED), mirroring Reddit:

  • v1 — CREATIVE-ONLY (TIKTOK_API_ENABLED off, the default). The arm
    (`_process_extra_platform_arm`) still generates the 9:16 / 1:1 image
    creatives + writes a handoff manifest to Drive; the create methods degrade
    cleanly — `create_image_ad` returns a `local_fallback` result and
    `create_campaign_group` / `create_campaign` / `upload_image` raise a
    descriptive RuntimeError so the arm's per-(cohort×geo) try/except
    short-circuits with the creatives already exported (same path Meta/Reddit
    take when ad creation can't proceed).

  • Phase 2 — PROGRAMMATIC (TIKTOK_API_ENABLED on + creds set). Maps to:
      create_campaign_group → POST /campaign/create/   (objective, budget mode)
      create_campaign       → POST /adgroup/create/    (targeting + budget + pixel)
      upload_image          → POST /file/image/ad/upload/ (UPLOAD_BY_FILE) → image_id
      create_image_ad       → POST /ad/create/         (SINGLE_IMAGE creative)

Auth is a long-lived advertiser-scoped access token (does NOT expire on its
own) sent as the `Access-Token` header. `TIKTOK_API_BASE` is swappable so the
whole create→upload→report loop can be validated against TikTok's sandbox
(sandbox-ads.tiktok.com) before spending real budget.

Everything is created PAUSED (operation_status=DISABLE) — a human activates in
TikTok Ads Manager. Names are auto-prefixed with `config.AGENT_NAME_PREFIX`.
See data/specs/TIKTOK_AD_SPEC.md.
"""
from __future__ import annotations

import hashlib
import logging
from pathlib import Path
from typing import Any, Optional

import requests

import config
from src.ad_platform import (
    AdPlatformClient,
    CreateAdResult,
    TIKTOK_CONSTRAINTS,
    PlatformConstraints,
)

log = logging.getLogger(__name__)

# TikTok "operation_status" — DISABLE = paused/draft (mirrors LinkedIn DRAFT /
# Meta PAUSED / Reddit PAUSED). ENABLE would launch immediately.
_STATUS_PAUSED = "DISABLE"

# TikTok `call_to_action` free-text enum (subset we use). The pipeline emits
# LinkedIn-style CTAs; anything outside this set is remapped to SIGN_UP.
_TIKTOK_VALID_CTAS = {
    "APPLY_NOW", "SIGN_UP", "LEARN_MORE", "DOWNLOAD_NOW",
    "CONTACT_US", "ORDER_NOW", "SHOP_NOW", "SUBSCRIBE", "READ_MORE",
}
_TIKTOK_CTA_REMAP = {
    "GET_STARTED": "SIGN_UP",
    "DOWNLOAD":    "DOWNLOAD_NOW",
    "LEARN MORE":  "LEARN_MORE",
    "SIGN UP":     "SIGN_UP",
    "APPLY NOW":   "APPLY_NOW",
}

_DISABLED_MSG = (
    "TIKTOK_API_ENABLED is off — TikTok Marketing API access is not yet "
    "provisioned. Creatives + a manual-upload handoff were exported to Drive "
    "for upload in TikTok Ads Manager."
)


def _normalize_cta(cta: Optional[str]) -> str:
    """Map a pipeline CTA to a TikTok-accepted call_to_action, defaulting to
    SIGN_UP."""
    if not cta:
        return "SIGN_UP"
    up = cta.strip().upper().replace("-", "_")
    up = _TIKTOK_CTA_REMAP.get(up, up)
    up = _TIKTOK_CTA_REMAP.get(cta.strip(), up)
    return up if up in _TIKTOK_VALID_CTAS else "SIGN_UP"


def exchange_auth_code(
    auth_code: str,
    app_id: Optional[str] = None,
    app_secret: Optional[str] = None,
    api_base: Optional[str] = None,
    api_version: Optional[str] = None,
) -> dict:
    """One-time exchange of an OAuth `auth_code` (from the advertiser
    authorization redirect) for a long-lived access token + the list of
    authorized advertiser ids. Returns the raw `data` dict
    ({access_token, advertiser_ids, scope}). Used by an operator script to
    provision TIKTOK_ACCESS_TOKEN / TIKTOK_ADVERTISER_ID into Doppler."""
    base = (api_base or config.TIKTOK_API_BASE).rstrip("/")
    ver = api_version or config.TIKTOK_API_VERSION
    url = f"{base}/open_api/{ver}/oauth2/access_token/"
    resp = requests.post(
        url,
        json={
            "app_id": app_id or config.TIKTOK_APP_ID,
            "secret": app_secret or config.TIKTOK_APP_SECRET,
            "auth_code": auth_code,
        },
        timeout=60,
    )
    body = resp.json() if resp.text else {}
    if not resp.ok or body.get("code") not in (0, None):
        raise RuntimeError(
            f"TikTok oauth2/access_token → {resp.status_code} "
            f"code={body.get('code')} msg={body.get('message')!r}"
        )
    return body.get("data", {})


class TikTokClient(AdPlatformClient):
    """TikTok Marketing API client.

    The pipeline calls `create_campaign_group` once per (cohort × geo) to
    create a Campaign, then `create_campaign` per (cohort × geo × angle) to
    create an Ad Group under it. Construction is lazy — no network/creds are
    touched until the first Phase-2 call."""

    name = "tiktok"
    constraints: PlatformConstraints = TIKTOK_CONSTRAINTS

    AGENT_NAME_PREFIX = config.AGENT_NAME_PREFIX

    def __init__(self) -> None:
        self._token = config.TIKTOK_ACCESS_TOKEN
        self._advertiser_id = config.TIKTOK_ADVERTISER_ID
        self._base = config.TIKTOK_API_BASE.rstrip("/")
        self._version = config.TIKTOK_API_VERSION
        self._session: Optional[requests.Session] = None

    # ── helpers ──────────────────────────────────────────────────────────────

    def _prefixed(self, name: str) -> str:
        if not self.AGENT_NAME_PREFIX or name.startswith(self.AGENT_NAME_PREFIX):
            return name
        return f"{self.AGENT_NAME_PREFIX}{name}"

    def _ensure_init(self) -> None:
        """Validate creds for Phase-2 programmatic calls."""
        if not config.TIKTOK_API_ENABLED:
            raise RuntimeError(_DISABLED_MSG)
        missing = [
            k for k, v in {
                "TIKTOK_ACCESS_TOKEN":  self._token,
                "TIKTOK_ADVERTISER_ID": self._advertiser_id,
            }.items() if not v
        ]
        if missing:
            raise RuntimeError(
                f"TIKTOK_API_ENABLED is on but {', '.join(missing)} not set — "
                "cannot call the TikTok Marketing API."
            )

    def _http(self) -> requests.Session:
        if self._session is None:
            s = requests.Session()
            s.headers.update({"Access-Token": self._token})
            self._session = s
        return self._session

    def _url(self, path: str) -> str:
        return f"{self._base}/open_api/{self._version}/{path.lstrip('/')}"

    def _api(self, method: str, path: str, *, json_body: dict | None = None,
             params: dict | None = None, files: dict | None = None,
             data: dict | None = None) -> dict:
        """Call the Marketing API and return the unwrapped `data`. TikTok always
        returns HTTP 200 with a `code` field — code 0 = success, anything else
        is an error carrying `message`. Raises RuntimeError on failure."""
        resp = self._http().request(
            method, self._url(path),
            json=json_body, params=params, files=files, data=data,
            timeout=90,
        )
        body = resp.json() if resp.text else {}
        code = body.get("code")
        if not resp.ok or (code not in (0, None)):
            raise RuntimeError(
                f"TikTok {method} {path} → http={resp.status_code} "
                f"code={code} msg={body.get('message')!r} {str(body.get('data'))[:200]}"
            )
        return body.get("data", {}) or {}

    # ── lifecycle (AdPlatformClient) ──────────────────────────────────────────

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        """Create a TikTok Campaign (top-level, like Meta's Campaign). Returns
        the campaign id. PAUSED. Objective from config.TIKTOK_OBJECTIVE. Budget
        lives at the ad-group level (no CBO), so we set BUDGET_MODE_INFINITE at
        the campaign. `geos` is unused here — geo targets at the ad-group level."""
        self._ensure_init()
        data = self._api("POST", "campaign/create/", json_body={
            "advertiser_id":  self._advertiser_id,
            "campaign_name":  self._prefixed(name),
            "objective_type": config.TIKTOK_OBJECTIVE,
            "budget_mode":    "BUDGET_MODE_INFINITE",
            "operation_status": _STATUS_PAUSED,
        })
        campaign_id = str(data.get("campaign_id") or "")
        log.info("TikTok campaign created %s (name=%s, objective=%s)",
                 campaign_id, name, config.TIKTOK_OBJECTIVE)
        return campaign_id

    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int | None = None,
    ) -> str:
        """Create a TikTok Ad Group (targeting + budget, child of a Campaign).
        `targeting` is the TikTokTargetingResolver dict (location_ids, age_groups,
        genders, interest/behavior category ids). Returns the ad-group id. PAUSED;
        CONVERT/OCPM optimized against the pixel; daily budget in USD."""
        self._ensure_init()
        budget_usd = (
            round(daily_budget_cents / 100, 2) if daily_budget_cents
            else float(config.TIKTOK_DEFAULT_DAILY_USD)
        )
        # PAUSED entity, so "now" is harmless — delivery begins only on un-pause.
        from datetime import datetime, timezone
        start = datetime.now(timezone.utc).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")

        body: dict[str, Any] = {
            "advertiser_id":     self._advertiser_id,
            "campaign_id":       campaign_group_id,
            "adgroup_name":      self._prefixed(name),
            "operation_status":  _STATUS_PAUSED,
            "placement_type":    "PLACEMENT_TYPE_NORMAL",
            "placements":        ["PLACEMENT_TIKTOK"],
            "budget_mode":       "BUDGET_MODE_DAY",
            "budget":            budget_usd,
            "schedule_type":     "SCHEDULE_FROM_NOW",
            "schedule_start_time": start,
            "optimization_goal": config.TIKTOK_OPTIMIZATION_GOAL,
            "billing_event":     config.TIKTOK_BILLING_EVENT,
            "bid_type":          "BID_TYPE_NO_BID",   # lowest-cost, no bid cap
            "pacing":            "PACING_MODE_SMOOTH",
        }
        # Conversion optimization needs the pixel + which event to optimize for.
        if config.TIKTOK_PIXEL_ID:
            body["pixel_id"] = config.TIKTOK_PIXEL_ID
        if config.TIKTOK_OPTIMIZATION_GOAL == "CONVERT" and config.TIKTOK_OPTIMIZATION_EVENT:
            body["optimization_event"] = config.TIKTOK_OPTIMIZATION_EVENT
        # Targeting (all optional; TikTok geo is numeric location_ids, NOT ISO).
        if targeting.get("location_ids"):
            body["location_ids"] = [str(x) for x in targeting["location_ids"]]
        if targeting.get("age_groups"):
            body["age_groups"] = list(targeting["age_groups"])
        if targeting.get("genders"):
            body["gender"] = targeting["genders"]
        if targeting.get("interest_category_ids"):
            body["interest_category_ids"] = list(targeting["interest_category_ids"])
        if targeting.get("languages"):
            body["languages"] = list(targeting["languages"])

        data = self._api("POST", "adgroup/create/", json_body=body)
        adgroup_id = str(data.get("adgroup_id") or "")
        log.info(
            "TikTok ad group created %s (campaign=%s, $%.2f/day, opt=%s, geos=%s)",
            adgroup_id, campaign_group_id, budget_usd,
            config.TIKTOK_OPTIMIZATION_GOAL, targeting.get("location_ids") or "—",
        )
        return adgroup_id

    def upload_image(self, image_path: str | Path) -> str:
        """Upload a PNG via UPLOAD_BY_FILE (multipart) and return the TikTok
        image_id. TikTok requires an md5 `image_signature` to match the bytes."""
        self._ensure_init()
        p = Path(image_path)
        raw = p.read_bytes()
        sig = hashlib.md5(raw).hexdigest()
        # multipart form — no JSON envelope; the session's Access-Token header
        # still applies. requests sets the multipart Content-Type.
        data = self._api(
            "POST", "file/image/ad/upload/",
            data={
                "advertiser_id":   self._advertiser_id,
                "upload_type":     "UPLOAD_BY_FILE",
                "image_signature": sig,
                "file_name":       p.name,
            },
            files={"image_file": (p.name, raw, "image/png")},
        )
        image_id = str(data.get("image_id") or "")
        if not image_id:
            raise RuntimeError(f"TikTok upload_image returned no image_id for {p.name}")
        log.info("TikTok image uploaded %s → image_id=%s", p.name, image_id)
        return image_id

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
    ) -> CreateAdResult:
        """Create a TikTok image ad under the given ad group (`campaign_id` is
        an adgroup_id per the cross-platform ABC convention). Ships as
        SINGLE_IMAGE. `description` is the native ad-text caption (≤100 chars).
        Returns CreateAdResult; on any failure returns status=error so the arm's
        per-cohort isolation saves the PNG locally and continues."""
        try:
            self._ensure_init()
        except RuntimeError as exc:
            # API disabled / creds missing → creative-only fallback (arm saves PNG).
            return CreateAdResult(status="local_fallback", error_class="RuntimeError",
                                  error_message=str(exc)[:300])
        try:
            ad_text = (description or ad_headline or headline or "").strip()[:100]
            creative = {
                "ad_name":        self._prefixed(headline or "outlier-tiktok")[:100],
                "ad_format":      config.TIKTOK_IMAGE_AD_FORMAT,   # SINGLE_IMAGE
                "image_ids":      [image_id],
                "ad_text":        ad_text,
                "call_to_action": _normalize_cta(cta),
                "landing_page_url": destination_url or "",
                "display_name":   "Outlier",
            }
            if config.TIKTOK_IDENTITY_ID:
                creative["identity_id"] = config.TIKTOK_IDENTITY_ID
                creative["identity_type"] = "CUSTOMIZED_USER"
            data = self._api("POST", "ad/create/", json_body={
                "advertiser_id": self._advertiser_id,
                "adgroup_id":    campaign_id,
                "creatives":     [creative],
            })
            # TikTok returns data.ad_ids (list) and/or data.creatives[].ad_id.
            ad_id = ""
            if data.get("ad_ids"):
                ad_id = str(data["ad_ids"][0])
            elif data.get("creatives"):
                ad_id = str(data["creatives"][0].get("ad_id") or "")
            if not ad_id:
                raise RuntimeError(f"TikTok ad/create returned no ad id: {str(data)[:200]}")
            log.info("TikTok ad created %s (adgroup=%s)", ad_id, campaign_id)
            return CreateAdResult(creative_id=ad_id, status="ok")
        except Exception as exc:  # noqa: BLE001 — per-cohort isolation in the arm
            log.error("TikTok create_image_ad failed (adgroup=%s): %s", campaign_id, exc)
            return CreateAdResult(status="error", error_class=type(exc).__name__,
                                  error_message=str(exc)[:300])

    # ── budget + rotation ─────────────────────────────────────────────────────

    def update_campaign_budget(self, campaign_id: str, daily_budget_cents: int) -> None:
        """Update an ad group's daily budget (console budget-cell → Phase 7).
        TikTok budget lives on the AD GROUP, so `campaign_id` is an adgroup_id
        (matches the cross-platform ABC convention, like Meta's ad-set)."""
        if daily_budget_cents <= 0:
            raise ValueError("TikTok daily budget must be > 0 cents.")
        self._ensure_init()
        self._api("POST", "adgroup/update/", json_body={
            "advertiser_id": self._advertiser_id,
            "adgroup_id":    campaign_id,
            "budget_mode":   "BUDGET_MODE_DAY",
            "budget":        round(daily_budget_cents / 100, 2),
        })
        log.info("TikTok ad group %s budget → $%.2f/day", campaign_id, daily_budget_cents / 100)

    def pause_ad(self, ad_id: str, status: str = "PAUSED") -> bool:
        """Pause (or resume) a single TikTok ad — the in-place creative-rotation
        primitive. Maps PAUSED→DISABLE / ACTIVE→ENABLE via /ad/status/update/.
        Returns True on success."""
        op = "DISABLE" if str(status).upper() in ("PAUSED", "DISABLE") else "ENABLE"
        try:
            self._ensure_init()
            self._api("POST", "ad/status/update/", json_body={
                "advertiser_id":    self._advertiser_id,
                "ad_ids":           [ad_id],
                "operation_status": op,
            })
            log.info("TikTok ad %s → operation_status=%s", ad_id, op)
            return True
        except Exception as exc:  # noqa: BLE001
            log.warning("TikTok pause_ad %s failed: %s", ad_id, str(exc)[:200])
            return False

    # ── reporting ─────────────────────────────────────────────────────────────

    def _report(self, dimensions: list[str], data_level: str,
                start_date: str, end_date: str) -> list[dict]:
        """Call report/integrated/get and return the raw `list` of
        {dimensions, metrics} rows, paging through all results."""
        import json as _json
        metrics = ["spend", "impressions", "clicks", "ctr", "cpc",
                   "conversion", "cost_per_conversion"]
        out: list[dict] = []
        page = 1
        while True:
            data = self._api("GET", "report/integrated/get/", params={
                "advertiser_id": self._advertiser_id,
                "report_type":   "BASIC",
                "data_level":    data_level,
                "dimensions":    _json.dumps(dimensions),
                "metrics":       _json.dumps(metrics),
                "start_date":    start_date,
                "end_date":      end_date,
                "page":          page,
                "page_size":     1000,
            })
            rows = data.get("list", []) or []
            out.extend(rows)
            info = data.get("page_info", {}) or {}
            if page >= int(info.get("total_page", 1) or 1) or not rows:
                break
            page += 1
        return out

    def fetch_campaign_metrics(self, window_days: int = 7) -> dict[str, dict]:
        """Aggregate impressions / clicks / spend / conversions per campaign for
        the last `window_days`. Returns {campaign_id: {...}}."""
        from datetime import datetime, timedelta, timezone
        self._ensure_init()
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=int(window_days))
        rows = self._report(["campaign_id"], "AUCTION_CAMPAIGN",
                            start.isoformat(), end.isoformat())
        out: dict[str, dict] = {}
        for r in rows:
            dims, mets = r.get("dimensions", {}), r.get("metrics", {})
            cid = str(dims.get("campaign_id") or "")
            if not cid:
                continue
            out[cid] = {
                "impressions": int(float(mets.get("impressions") or 0)),
                "clicks":      int(float(mets.get("clicks") or 0)),
                "spend_usd":   round(float(mets.get("spend") or 0), 2),
                "conversions": int(float(mets.get("conversion") or 0)),
            }
        log.info("TikTok reporting: %d campaign(s) (window=%dd)", len(out), window_days)
        return out

    def fetch_campaign_metrics_daily(self, window_days: int = 90) -> list[dict]:
        """Per-(campaign × day) delivery for the Analytics dashboard DoD charts.
        `stat_time_day` dimension = the Meta `time_increment=1` equivalent.
        Returns [{campaign_id, metric_date 'YYYY-MM-DD', impressions, clicks,
        spend_usd, conversions}]."""
        from datetime import datetime, timedelta, timezone
        self._ensure_init()
        end = datetime.now(timezone.utc).date()
        start = end - timedelta(days=int(window_days))
        rows = self._report(["campaign_id", "stat_time_day"], "AUCTION_CAMPAIGN",
                            start.isoformat(), end.isoformat())
        out: list[dict] = []
        for r in rows:
            dims, mets = r.get("dimensions", {}), r.get("metrics", {})
            cid = str(dims.get("campaign_id") or "")
            day = str(dims.get("stat_time_day") or "")[:10]
            if not cid or not day:
                continue
            out.append({
                "campaign_id": cid,
                "metric_date": day,
                "impressions": int(float(mets.get("impressions") or 0)),
                "clicks":      int(float(mets.get("clicks") or 0)),
                "spend_usd":   round(float(mets.get("spend") or 0), 2),
                "conversions": int(float(mets.get("conversion") or 0)),
            })
        log.info("TikTok daily reporting: %d (campaign×day) rows (window=%dd)", len(out), window_days)
        return out

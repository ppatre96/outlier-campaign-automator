"""
Campaign Registry — tracks every campaign created by the pipeline in an Excel sheet.

Columns written at campaign creation time:
  smart_ramp_id, cohort_id, cohort_signature, geo_cluster, geo_cluster_label,
  geos, angle, campaign_type (inmail/static), advertised_rate,
  linkedin_campaign_urn, creative_urn, headline, subheadline, photo_subject,
  inmail_subject, created_at, status (active/deprecated/paused)

Metric columns updated later by the feedback agent:
  impressions, clicks, cpm_usd, ctr_pct, cpc_usd, spend_usd,
  applications, cpa_usd, last_metrics_at

The registry is written to data/campaign_registry.xlsx and also stored in
data/campaign_registry.json (for machine-readable access without openpyxl).

Pranav rule (2026-05-05): experimentation framework — max 3 cohorts per geo
cluster × 3 angles each. Feedback agent surfaces winners/losers; losing
angles are marked deprecated and replaced with new variants.
"""
from __future__ import annotations

import json
import logging
import threading
from contextlib import contextmanager
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

# Phase 3.3 — registry file I/O lock. Both the Static arm and the InMail
# arm (now run concurrently per Phase 3.3) call log_campaign / _load /
# _save against `data/campaign_registry.json` + `.xlsx`. The read-modify-
# write pattern (load → mutate → save) MUST be atomic across threads;
# without a lock, two arms can each load the same snapshot and overwrite
# each other's rows on save. The lock is re-entrant so a single thread
# can hold it across nested helper calls (e.g. log_campaign internally
# calling _save).
_registry_lock = threading.RLock()


@contextmanager
def registry_critical_section():
    """Acquire the registry lock for an atomic read-modify-write block.

    Use this when the caller needs to load → inspect → mutate → save the
    registry as one atomic operation (e.g. patching a creative URN onto
    a previously-logged row). Single-shot writes via log_campaign /
    update_metrics already acquire the lock internally; this context
    manager is for callers that need to hold the lock across multiple
    helper calls.
    """
    with _registry_lock:
        yield

_sheets_client = None

def _get_sheets():
    global _sheets_client
    if _sheets_client is None:
        try:
            from src.sheets import SheetsClient
            _sheets_client = SheetsClient()
        except Exception as exc:
            log.warning("Could not init SheetsClient for registry: %s", exc)
            from src.sheets import NullSheetsClient
            _sheets_client = NullSheetsClient()
    return _sheets_client

_REGISTRY_PATH = Path("data/campaign_registry.json")
_EXCEL_PATH    = Path("data/campaign_registry.xlsx")

COLUMNS = [
    # ── Identity ──────────────────────────────────────────────────────────────
    "smart_ramp_id",
    "cohort_id",
    "cohort_signature",
    "geo_cluster",
    "geo_cluster_label",
    "geos",
    "angle",                    # A / B / C / F
    "campaign_type",            # "static" | "inmail"
    "advertised_rate",          # e.g. "$50/hr"
    # ── Ad platform identity ──────────────────────────────────────────────────
    "channel",                  # display alias for platform — "LinkedIn" | "Meta" | "Google"
    "platform",                 # internal lower-case key — "linkedin" | "meta" | "google"
    "campaign_name",            # human-readable campaign name (matches platform UI exactly)
    "campaign_link",            # direct deep-link to the campaign in the platform's UI
    "platform_campaign_id",     # platform-native id (URN / numeric / resource name)
    "platform_creative_id",     # platform-native creative or ad id
    # ── LinkedIn URNs (legacy — duplicates platform_* for back-compat) ────────
    "linkedin_campaign_urn",
    "creative_urn",             # static: image creative URN; inmail: message ad URN
    # ── Copy snapshot ─────────────────────────────────────────────────────────
    "headline",
    "subheadline",
    "photo_subject",
    "creative_image_path",      # local path to the rendered PNG (used to embed image in Sheets)
    "inmail_subject",
    "inmail_body_preview",      # first 150 chars
    # ── Lifecycle ─────────────────────────────────────────────────────────────
    "created_at",
    "status",                   # active | paused | deprecated
    "deprecation_reason",
    "gemini_prompt",            # base Gemini image gen prompt (first attempt, no QC suffix)
    # ── Performance metrics (filled by feedback agent) ─────────────────────────
    "impressions",
    "clicks",
    "cpm_usd",
    "ctr_pct",
    "cpc_usd",
    "spend_usd",
    "applications",
    "cpa_usd",
    "last_metrics_at",
]


@dataclass
class CampaignEntry:
    smart_ramp_id:          str = ""
    cohort_id:              str = ""
    cohort_signature:       str = ""
    geo_cluster:            str = ""
    geo_cluster_label:      str = ""
    geos:                   str = ""    # comma-joined ISO codes
    angle:                  str = ""
    campaign_type:          str = ""
    advertised_rate:        str = ""
    channel:                str = "LinkedIn"   # mirrors platform, title-cased for the Sheet view
    platform:               str = "linkedin"
    campaign_name:          str = ""    # human-readable campaign name (matches platform UI exactly)
    campaign_link:          str = ""    # deep-link to the campaign in the platform's UI
    platform_campaign_id:   str = ""
    platform_creative_id:   str = ""
    linkedin_campaign_urn:  str = ""    # legacy alias of platform_campaign_id when platform=linkedin
    creative_urn:           str = ""    # legacy alias of platform_creative_id
    headline:               str = ""
    subheadline:            str = ""
    photo_subject:          str = ""
    creative_image_path:    str = ""
    inmail_subject:         str = ""
    inmail_body_preview:    str = ""
    created_at:             str = ""
    status:                 str = "active"
    deprecation_reason:     str = ""
    gemini_prompt:          str = ""
    # metrics — empty until feedback agent runs
    impressions:            int | None = None
    clicks:                 int | None = None
    cpm_usd:                float | None = None
    ctr_pct:                float | None = None
    cpc_usd:                float | None = None
    spend_usd:              float | None = None
    applications:           int | None = None
    cpa_usd:                float | None = None
    last_metrics_at:        str = ""


# Internal lower-case platform key → user-facing channel label shown in Sheet.
_CHANNEL_LABEL = {"linkedin": "LinkedIn", "meta": "Meta", "google": "Google"}


def _channel_label(platform: str) -> str:
    return _CHANNEL_LABEL.get((platform or "").lower(), (platform or "").title())


def _derive_campaign_link(platform: str, campaign_id: str) -> str:
    """Build the platform's UI deep-link from `platform_campaign_id`.

    LinkedIn: strips the URN prefix (`urn:li:sponsoredCampaign:` /
    `urn:li:sponsoredCampaignGroup:`) to the numeric id; campaign-group ids
    fall through the same campaigns/{id}/details URL — the Campaign Manager
    UI redirects to the correct entity type. Meta strips the `act_` prefix
    from META_AD_ACCOUNT_ID. Google needs no account id in the URL.
    """
    if not campaign_id:
        return ""
    p = (platform or "").lower()
    try:
        import config as _cfg
    except Exception:
        return ""
    if p == "linkedin":
        cid = str(campaign_id).rsplit(":", 1)[-1]
        return (
            f"https://www.linkedin.com/campaignmanager/accounts/"
            f"{_cfg.LINKEDIN_AD_ACCOUNT_ID}/campaigns/{cid}/details"
        )
    if p == "meta":
        meta_no_prefix = (_cfg.META_AD_ACCOUNT_ID or "").replace("act_", "")
        return (
            f"https://business.facebook.com/adsmanager/manage/campaigns"
            f"?act={meta_no_prefix}&selected_campaign_ids={campaign_id}"
        )
    if p == "google":
        return f"https://ads.google.com/aw/campaigns?campaignId={campaign_id}"
    return ""


def _load() -> list[dict]:
    # Lock acquisition is cheap on the RLock fast path; load is read-only
    # but we lock to prevent reading a half-written file mid-save.
    with _registry_lock:
        if _REGISTRY_PATH.exists():
            try:
                return json.loads(_REGISTRY_PATH.read_text())
            except Exception:
                pass
        return []


def _save(records: list[dict]) -> None:
    with _registry_lock:
        _REGISTRY_PATH.parent.mkdir(parents=True, exist_ok=True)
        _REGISTRY_PATH.write_text(json.dumps(records, indent=2, default=str))
        _write_excel(records)


def _write_excel(records: list[dict]) -> None:
    try:
        import openpyxl
        from openpyxl.styles import Alignment, Font, PatternFill
        from openpyxl.utils import get_column_letter
    except ImportError:
        log.debug("openpyxl not installed — skipping Excel write (JSON only)")
        return

    wb = openpyxl.Workbook()
    ws = wb.active
    ws.title = "Campaign Registry"

    # Header row
    header_fill = PatternFill("solid", fgColor="1F3864")
    header_font = Font(color="FFFFFF", bold=True)
    for col_idx, col in enumerate(COLUMNS, 1):
        cell = ws.cell(row=1, column=col_idx, value=col.replace("_", " ").title())
        cell.fill = header_fill
        cell.font = header_font
        cell.alignment = Alignment(horizontal="center", wrap_text=True)

    # Section dividers — colour-code by campaign_type
    static_fill  = PatternFill("solid", fgColor="E8F4FD")
    inmail_fill  = PatternFill("solid", fgColor="FEF9E7")
    depr_fill    = PatternFill("solid", fgColor="FDECEA")

    for row_idx, rec in enumerate(records, 2):
        fill = (depr_fill if rec.get("status") == "deprecated"
                else inmail_fill if rec.get("campaign_type") == "inmail"
                else static_fill)
        for col_idx, col in enumerate(COLUMNS, 1):
            val = rec.get(col, "")
            cell = ws.cell(row=row_idx, column=col_idx, value=val)
            cell.fill = fill
            cell.alignment = Alignment(wrap_text=False)

    # Column widths
    col_widths = {
        "smart_ramp_id": 14, "cohort_signature": 40, "geo_cluster_label": 22,
        "geos": 18, "angle": 7, "campaign_type": 10, "advertised_rate": 12,
        "linkedin_campaign_urn": 35, "creative_urn": 35,
        "headline": 30, "subheadline": 30, "photo_subject": 35,
        "inmail_subject": 35, "inmail_body_preview": 40,
        "created_at": 20, "status": 12,
    }
    for col_idx, col in enumerate(COLUMNS, 1):
        ws.column_dimensions[get_column_letter(col_idx)].width = col_widths.get(col, 14)

    ws.freeze_panes = "A2"
    _EXCEL_PATH.parent.mkdir(parents=True, exist_ok=True)
    wb.save(_EXCEL_PATH)
    log.info("Campaign registry written to %s (%d rows)", _EXCEL_PATH, len(records))


def log_campaign(
    smart_ramp_id: str,
    cohort_id: str,
    cohort_signature: str,
    geo_cluster: str,
    geo_cluster_label: str,
    geos: list[str],
    angle: str,
    campaign_type: str,
    advertised_rate: str,
    linkedin_campaign_urn: str = "",
    creative_urn: str = "",
    headline: str = "",
    subheadline: str = "",
    photo_subject: str = "",
    creative_image_path: str = "",
    inmail_subject: str = "",
    inmail_body: str = "",
    gemini_prompt: str = "",
    *,
    platform: str = "linkedin",
    platform_campaign_id: str = "",
    platform_creative_id: str = "",
    campaign_name: str = "",
) -> None:
    """Append one campaign row to the registry. Safe to call from any platform arm.

    `platform` defaults to "linkedin" so existing callers (which pass
    `linkedin_campaign_urn=` only) keep working. New multi-platform callers
    pass `platform=` plus `platform_campaign_id=` / `platform_creative_id=`.

    The legacy `linkedin_campaign_urn` / `creative_urn` columns are
    populated when platform="linkedin" so the existing readers + Google
    Sheets writers continue to work without further changes.
    """
    # Resolve the platform-native id pair from whichever set of kwargs the
    # caller used. LinkedIn callers may pass either set.
    pcid = platform_campaign_id or linkedin_campaign_urn
    pcrid = platform_creative_id or creative_urn

    entry = CampaignEntry(
        smart_ramp_id=smart_ramp_id,
        cohort_id=cohort_id,
        cohort_signature=cohort_signature,
        geo_cluster=geo_cluster,
        geo_cluster_label=geo_cluster_label,
        geos=", ".join(geos) if geos else "",
        angle=angle,
        campaign_type=campaign_type,
        advertised_rate=advertised_rate,
        channel=_channel_label(platform),
        platform=platform,
        campaign_name=campaign_name,
        campaign_link=_derive_campaign_link(platform, pcid),
        platform_campaign_id=pcid,
        platform_creative_id=pcrid,
        # Legacy aliases — kept populated only for LinkedIn rows so old
        # consumers (Sheets columns, downstream queries) keep working.
        linkedin_campaign_urn=pcid if platform == "linkedin" else "",
        creative_urn=pcrid if platform == "linkedin" else "",
        headline=headline,
        subheadline=subheadline,
        photo_subject=photo_subject,
        creative_image_path=creative_image_path,
        inmail_subject=inmail_subject,
        inmail_body_preview=inmail_body[:150] if inmail_body else "",
        gemini_prompt=gemini_prompt,
        created_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
        status="active",
    )
    # Hold the registry lock across the load-mutate-save window. _load and
    # _save also acquire the (re-entrant) lock internally; this ensures the
    # append is atomic w.r.t. concurrent writers from the other arm.
    with _registry_lock:
        records = _load()
        entry_dict = asdict(entry)
        records.append(entry_dict)
        _save(records)
    log.info(
        "Registry: logged %s/%s campaign %s (ramp=%s cohort=%s angle=%s geo=%s)",
        platform, campaign_type, pcid, smart_ramp_id, cohort_id, angle, geo_cluster_label,
    )
    try:
        _get_sheets().write_registry_row(entry_dict)
    except Exception as exc:
        log.warning("Registry sheet write failed (non-fatal): %s", exc)


def _id_match(rec: dict, campaign_id: str) -> bool:
    """True if the registry record matches `campaign_id` on either the new
    `platform_campaign_id` column or the legacy `linkedin_campaign_urn`."""
    return (
        rec.get("platform_campaign_id") == campaign_id
        or rec.get("linkedin_campaign_urn") == campaign_id
    )


def update_metrics(
    linkedin_campaign_urn: str,
    impressions: int,
    clicks: int,
    spend_usd: float,
    applications: int = 0,
) -> None:
    """Update performance metrics for a campaign. Called by the feedback agent.

    The first kwarg name is preserved for back-compat — it accepts any
    platform's campaign id (LinkedIn URN, Meta numeric, Google resource).
    """
    with _registry_lock:
        records = _load()
        updated = False
        for rec in records:
            if _id_match(rec, linkedin_campaign_urn):
                rec["impressions"] = impressions
                rec["clicks"] = clicks
                rec["spend_usd"] = round(spend_usd, 2)
                rec["cpm_usd"] = round(spend_usd / impressions * 1000, 2) if impressions else None
                rec["ctr_pct"] = round(clicks / impressions * 100, 3) if impressions else None
                rec["cpc_usd"] = round(spend_usd / clicks, 2) if clicks else None
                rec["applications"] = applications
                rec["cpa_usd"] = round(spend_usd / applications, 2) if applications else None
                rec["last_metrics_at"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
                updated = True
                break
        if updated:
            _save(records)
            log.info("Registry: metrics updated for %s", linkedin_campaign_urn)
        else:
            log.warning("Registry: campaign not found for metrics update: %s", linkedin_campaign_urn)


def deprecate_campaign(linkedin_campaign_urn: str, reason: str) -> None:
    """Mark a campaign as deprecated. Accepts any platform's campaign id."""
    with _registry_lock:
        records = _load()
        for rec in records:
            if _id_match(rec, linkedin_campaign_urn):
                rec["status"] = "deprecated"
                rec["deprecation_reason"] = reason
                break
        _save(records)
    log.info("Registry: deprecated %s — %s", linkedin_campaign_urn, reason)


def get_active_campaigns(smart_ramp_id: str | None = None) -> list[dict]:
    """Return all active campaigns, optionally filtered by ramp."""
    records = _load()
    return [
        r for r in records
        if r.get("status") == "active"
        and (smart_ramp_id is None or r.get("smart_ramp_id") == smart_ramp_id)
    ]


def get_entry_by_urn(linkedin_campaign_urn: str) -> dict | None:
    """Return the registry entry for a campaign id, or None.

    Accepts either platform_campaign_id (new) or linkedin_campaign_urn (legacy).
    """
    records = _load()
    return next(
        (r for r in records if _id_match(r, linkedin_campaign_urn)),
        None,
    )


def get_cohort_entries(cohort_id: str, geo_cluster: str) -> list[dict]:
    """Return all registry entries for a cohort+geo combo, sorted by CTR desc."""
    records = _load()
    entries = [
        r for r in records
        if r.get("cohort_id") == cohort_id and r.get("geo_cluster") == geo_cluster
    ]
    return sorted(entries, key=lambda r: r.get("ctr_pct") or 0.0, reverse=True)


def get_registry_summary() -> dict:
    """Quick stats for Slack notifications / logging."""
    records = _load()
    return {
        "total": len(records),
        "active": sum(1 for r in records if r.get("status") == "active"),
        "deprecated": sum(1 for r in records if r.get("status") == "deprecated"),
        "by_ramp": {
            ramp: sum(1 for r in records if r.get("smart_ramp_id") == ramp)
            for ramp in {r.get("smart_ramp_id") for r in records}
        },
    }

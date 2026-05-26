"""
Weekly ads audit — runs every campaign launched in the last N days through
the `claude-ads` sub-skill prompts (vendored at `src/audit_prompts/`) and
returns structured per-platform findings: health score, top issues, top
recommendations, exec summary.

One Claude API call per platform (LinkedIn / Meta / Google), batching all
campaigns from the lookback window into a single request. Cheaper + faster
than per-campaign and lines up with how the audit skill is designed —
the skill thinks at the *account* level, not per-ad-set.

Metrics source is the cached `data/campaign_registry.json` rows
(refreshed daily by smart_ramp_poller). Registry is at most 24h stale and
the audit covers 21d of history, so no fresh API fetch is needed.

Public API:
    run_weekly_audit(lookback_days=21, registry_path=...) -> dict
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import re
from pathlib import Path
from typing import Any, Optional

import config
from src.claude_client import call_claude

log = logging.getLogger(__name__)

# Where the vendored claude-ads sub-skill prompts live. One file per
# platform — refresh from upstream via `src/audit_prompts/_README.md`.
_PROMPT_DIR = Path(__file__).parent / "audit_prompts"
_SUPPORTED_PLATFORMS = ("linkedin", "meta", "google")

# Slim down each registry row to the fields the audit prompt actually
# uses. Sending the full 30-field row wastes tokens and confuses the
# model on fields it doesn't have guidance for (e.g., gemini_prompt).
_REGISTRY_FIELDS = (
    "platform_campaign_id", "campaign_name", "created_at", "status",
    "cohort_signature", "geo_cluster_label", "geos", "angle",
    "campaign_type", "headline", "subheadline",
    "impressions", "clicks", "spend_usd", "ctr_pct", "cpc_usd",
    "applications", "cpa_usd", "last_metrics_at",
    "meta_audience_size", "audience_check_status",
    "qc_verdict", "deprecation_reason",
)


# ── Public ──────────────────────────────────────────────────────────────


def run_weekly_audit(
    lookback_days: int = 21,
    registry_path: str | Path = "data/campaign_registry.json",
    *,
    now_utc: Optional[dt.datetime] = None,
    call_claude_fn = call_claude,  # injectable for tests
) -> dict[str, Any]:
    """Audit every campaign launched in the last `lookback_days` days.

    Returns a dict with one entry per supported platform plus run metadata.
    Empty platforms (no new campaigns) get an empty findings stub so
    downstream rendering can show "no new campaigns this period" instead
    of hiding the platform entirely.
    """
    now_utc = now_utc or dt.datetime.now(dt.timezone.utc)
    rows = _load_registry(registry_path)
    recent = _filter_recent(rows, lookback_days, now_utc=now_utc)
    by_platform = _group_by_platform(recent)

    findings: dict[str, dict[str, Any]] = {}
    for platform in _SUPPORTED_PLATFORMS:
        platform_rows = by_platform.get(platform, [])
        if not platform_rows:
            findings[platform] = _empty_finding(platform)
            log.info("ads_auditor: %s — 0 campaigns in last %dd, skipping Claude call",
                     platform, lookback_days)
            continue
        try:
            finding = _audit_platform(platform, platform_rows, call_claude_fn)
        except Exception as exc:
            log.exception("ads_auditor: %s audit failed (%s) — emitting error stub",
                          platform, exc)
            finding = _error_finding(platform, exc, platform_rows)
        findings[platform] = finding

    return {
        "lookback_days":  lookback_days,
        "as_of_utc":      now_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "registry_path":  str(registry_path),
        "total_audited":  sum(f.get("campaigns_audited", 0) for f in findings.values()),
        "platforms":      findings,
    }


# ── Registry I/O + filtering ────────────────────────────────────────────


def _load_registry(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        log.warning("ads_auditor: registry not found at %s — auditing empty set", p)
        return []
    with p.open() as f:
        data = json.load(f)
    if not isinstance(data, list):
        raise ValueError(f"Registry at {p} is not a list (got {type(data).__name__})")
    return data


def _filter_recent(
    rows: list[dict],
    lookback_days: int,
    *,
    now_utc: dt.datetime,
) -> list[dict]:
    cutoff = now_utc - dt.timedelta(days=lookback_days)
    kept: list[dict] = []
    for row in rows:
        created = _parse_created_at(row.get("created_at"))
        if created is None:
            continue
        if created >= cutoff:
            kept.append(row)
    log.info("ads_auditor: %d/%d registry rows fall within last %dd",
             len(kept), len(rows), lookback_days)
    return kept


def _parse_created_at(raw: Any) -> Optional[dt.datetime]:
    """Parse the `created_at` field which comes in as 'YYYY-MM-DD HH:MM UTC'."""
    if not raw or not isinstance(raw, str):
        return None
    s = raw.strip().replace(" UTC", "")
    # Try a few common shapes — registry rows are mostly "%Y-%m-%d %H:%M".
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%dT%H:%M:%S"):
        try:
            return dt.datetime.strptime(s, fmt).replace(tzinfo=dt.timezone.utc)
        except ValueError:
            continue
    return None


def _group_by_platform(rows: list[dict]) -> dict[str, list[dict]]:
    by_platform: dict[str, list[dict]] = {}
    for row in rows:
        platform = (row.get("platform") or "").strip().lower()
        if platform not in _SUPPORTED_PLATFORMS:
            # Some legacy rows have empty `platform` — infer from
            # presence of platform-specific URN field.
            if row.get("linkedin_campaign_urn"):
                platform = "linkedin"
            else:
                continue
        by_platform.setdefault(platform, []).append(row)
    return by_platform


# ── Per-platform audit ──────────────────────────────────────────────────


def _audit_platform(
    platform: str,
    rows: list[dict],
    call_claude_fn,
) -> dict[str, Any]:
    prompt_text = _load_prompt(platform)
    summarized = [_slim_row(r) for r in rows]
    user_payload = {
        "platform":        platform,
        "campaign_count":  len(summarized),
        "campaigns":       summarized,
    }
    spend_total = sum(_safe_float(r.get("spend_usd")) for r in rows)
    impressions_total = sum(_safe_int(r.get("impressions")) for r in rows)

    system = _build_system_prompt(prompt_text)
    user = (
        "Audit the campaigns below and return ONLY a JSON object matching this exact schema:\n"
        '{ "health_score": <int 0-100>, '
        '"executive_summary": <one-paragraph string, max 3 sentences>, '
        '"top_issues": [<3-5 short strings>], '
        '"top_recommendations": [<3-5 short strings>] }\n\n'
        "Don't include any prose outside the JSON. Don't wrap the JSON in markdown fences.\n\n"
        f"Campaign data:\n```json\n{json.dumps(user_payload, indent=2, default=str)}\n```"
    )

    log.info("ads_auditor: calling Claude for %s (%d campaigns, ~%d total impressions, $%.2f total spend)",
             platform, len(rows), impressions_total, spend_total)

    response_text = call_claude_fn(
        messages=[{"role": "user", "content": user}],
        system=system,
        max_tokens=2048,
        cache_system=False,
    )
    parsed = _parse_claude_response(response_text)

    return {
        "platform":              platform,
        "campaigns_audited":     len(rows),
        "total_spend_usd":       round(spend_total, 2),
        "total_impressions":     impressions_total,
        "health_score":          int(parsed.get("health_score", 0)),
        "executive_summary":     str(parsed.get("executive_summary") or "").strip(),
        "top_issues":            [str(x) for x in (parsed.get("top_issues") or [])][:5],
        "top_recommendations":   [str(x) for x in (parsed.get("top_recommendations") or [])][:5],
    }


def _load_prompt(platform: str) -> str:
    path = _PROMPT_DIR / f"ads-{platform}.md"
    if not path.exists():
        raise FileNotFoundError(
            f"Missing vendored prompt at {path}. "
            f"Refresh via instructions in src/audit_prompts/_README.md."
        )
    return path.read_text()


def _build_system_prompt(prompt_text: str) -> str:
    return (
        "You are an expert paid-ads auditor. Apply the audit framework "
        "below to the campaign data the user provides. Return findings "
        "as structured JSON exactly per the schema specified in the user "
        "message. Do not return any prose outside the JSON. Do not wrap "
        "the JSON in markdown code fences.\n\n"
        "===== AUDIT FRAMEWORK =====\n"
        f"{prompt_text}\n"
        "===== END AUDIT FRAMEWORK ====="
    )


def _slim_row(row: dict) -> dict:
    """Keep only the fields the audit prompt actually uses."""
    return {k: row.get(k) for k in _REGISTRY_FIELDS if row.get(k) not in (None, "")}


def _parse_claude_response(text: str) -> dict:
    """Defensive JSON extraction — Claude occasionally wraps JSON in
    markdown fences despite instructions. Strip fences then parse.
    """
    if not text:
        return {}
    cleaned = text.strip()
    # Strip markdown code fences if present.
    fence = re.match(r"^```(?:json)?\s*(.*?)\s*```$", cleaned, re.DOTALL)
    if fence:
        cleaned = fence.group(1).strip()
    try:
        return json.loads(cleaned)
    except json.JSONDecodeError:
        # Try to extract the first {...} block — model sometimes prepends a sentence.
        m = re.search(r"\{.*\}", cleaned, re.DOTALL)
        if m:
            try:
                return json.loads(m.group(0))
            except json.JSONDecodeError:
                pass
        log.warning("ads_auditor: could not parse Claude response as JSON; first 200 chars: %r", cleaned[:200])
        return {}


# ── Empty / error stubs ─────────────────────────────────────────────────


def _empty_finding(platform: str) -> dict:
    return {
        "platform":              platform,
        "campaigns_audited":     0,
        "total_spend_usd":       0.0,
        "total_impressions":     0,
        "health_score":          None,
        "executive_summary":     "No new campaigns this period.",
        "top_issues":            [],
        "top_recommendations":   [],
    }


def _error_finding(platform: str, exc: Exception, rows: list[dict]) -> dict:
    return {
        "platform":              platform,
        "campaigns_audited":     len(rows),
        "total_spend_usd":       round(sum(_safe_float(r.get("spend_usd")) for r in rows), 2),
        "total_impressions":     sum(_safe_int(r.get("impressions")) for r in rows),
        "health_score":          None,
        "executive_summary":     f"Audit failed: {type(exc).__name__}: {exc}",
        "top_issues":            [f"Audit error: {type(exc).__name__}"],
        "top_recommendations":   ["Re-run the audit manually via `python3 scripts/weekly_audit.py`"],
    }


# ── Number coercion ─────────────────────────────────────────────────────


def _safe_float(v: Any) -> float:
    try:
        return float(v) if v not in (None, "") else 0.0
    except (TypeError, ValueError):
        return 0.0


def _safe_int(v: Any) -> int:
    try:
        return int(v) if v not in (None, "") else 0
    except (TypeError, ValueError):
        return 0

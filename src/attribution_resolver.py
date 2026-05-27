"""Snowflake attribution lookups — pay-rate + activations per signup_flow_id.

Two queries, both authored by outlier-data-analyst 2026-05-24, schema-corrected
2026-05-27 against the actual Snowflake views (see
`~/.claude/agent-memory/outlier-data-analyst/attribution_queries.md` for the
analytical write-up).

## Pay-rate resolver (Q2)

`resolve_pay_rate(signup_flow_id)` → `PayRateResult`:
- Looks up the primary qualification for a signup_flow_id (excluding
  pay-multipliers + assessments, preferring non-language quals, tie-breaking on
  highest T1 rate).
- Returns the T1 USD rate to use as `base_rate_usd` in
  `geo_tiers.group_geos_for_campaigns()`. Soft-fails to None if no signal
  (caller ships rate-free copy — NEVER hardcode a default; wrong rate in
  ads is critical-risk).
- Sets `skip_country_multiplier=True` for language quals (their per-locale
  pricing is already baked into the qual rate).

## Activations resolver (Q1)

`resolve_activations(signup_flow_id)` → `ActivationsResult`:
- Counts distinct USER_IDs with `CESF.ACTIVATED = TRUE` (activations) and
  `CESF.EVER_PASSED_SKILL_SCREENING = TRUE` (skill_passes) anchored on
  `APPLICATION_CONVERSION.SIGNUP_FLOW_ID`.
- Used by the nightly feedback agent to fill `CampaignEntry.activations` and
  `.skill_passes` columns in the campaign registry.

## Caching

Both functions accept an optional `cache` dict — pass the same dict for the
lifetime of a single ramp run to dedupe Redash hits across multiple cohorts
that share a signup_flow_id. Pay-rate resolution must NOT persist cache across
ramps (rate cards can change; per-ramp scope only).
"""
from __future__ import annotations

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

log = logging.getLogger(__name__)

_QUERY_DIR = Path(__file__).parent.parent / "queries"
_PAY_RATE_SQL_PATH    = _QUERY_DIR / "snowflake_pay_rate_resolver.sql"
_ACTIVATIONS_SQL_PATH = _QUERY_DIR / "snowflake_activations_attribution.sql"


# ── Result types ────────────────────────────────────────────────────────


@dataclass(frozen=True)
class PayRateResult:
    """Output of resolve_pay_rate.

    All fields are Optional — when the lookup soft-fails (no JOBPOST_IDS,
    no qualifications, no rate in pay-rate card), every field is None and
    the caller should ship rate-free copy.
    """
    t1_rate_usd:             Optional[float]
    qualification_name:      Optional[str]
    qualification_type:      Optional[str]    # 'language' | 'worker_skill' | etc.
    skip_country_multiplier: bool             # True for language quals
    project_id:              Optional[str]
    qualification_id:        Optional[str]

    @property
    def has_rate(self) -> bool:
        return self.t1_rate_usd is not None


@dataclass(frozen=True)
class ActivationsResult:
    """Output of resolve_activations."""
    signup_flow_id: str
    activations:    int
    skill_passes:   int


# ── Public API ──────────────────────────────────────────────────────────


def resolve_pay_rate(
    signup_flow_id: str,
    *,
    redash_client = None,
    cache: Optional[dict] = None,
) -> PayRateResult:
    """Look up the primary qualification + T1 USD rate for a signup_flow_id.

    Args:
        signup_flow_id: e.g., '5f6a...' — the Smart Ramp / Outlier signup flow ID.
        redash_client:  Optional injected client (for tests). Default: lazy
                        construct `RedashClient` from config.
        cache:          Optional dict — pass the same dict across multiple calls
                        in the SAME ramp run to avoid re-querying. Do NOT persist
                        across ramps; rate cards can change.

    Returns:
        PayRateResult — every field None if no signal found.
    """
    if not signup_flow_id or not str(signup_flow_id).strip():
        log.warning("resolve_pay_rate: empty signup_flow_id — returning soft-fail")
        return _empty_pay_rate_result()

    signup_flow_id = signup_flow_id.strip()
    if cache is not None and signup_flow_id in cache:
        log.debug("resolve_pay_rate: cache hit for %s", signup_flow_id)
        return cache[signup_flow_id]

    try:
        sql = _PAY_RATE_SQL_PATH.read_text().replace("{signup_flow_id}", _esc(signup_flow_id))
        client = redash_client or _default_client()
        df = client._run_query(sql, label=f"pay-rate-{signup_flow_id[:12]}")
    except Exception as exc:
        log.warning(
            "resolve_pay_rate: Redash call failed for %s (%s) — returning soft-fail",
            signup_flow_id, exc,
        )
        result = _empty_pay_rate_result()
        if cache is not None:
            cache[signup_flow_id] = result
        return result

    if df.empty:
        log.info("resolve_pay_rate: no qualifications found for signup_flow_id=%s", signup_flow_id)
        result = _empty_pay_rate_result()
    else:
        # PRIMARY_RANK=1 is the chosen primary qualification per the heuristic
        # ordering in the SQL window function.
        top_row = df.sort_values("PRIMARY_RANK" if "PRIMARY_RANK" in df.columns else "primary_rank").iloc[0]
        t1 = top_row.get("T1_RATE_USD") if "T1_RATE_USD" in top_row else top_row.get("t1_rate_usd")
        qual_type = top_row.get("QUALIFICATION_TYPE") or top_row.get("qualification_type") or ""
        result = PayRateResult(
            t1_rate_usd             = float(t1) if t1 not in (None, "") else None,
            qualification_name      = top_row.get("QUALIFICATION_NAME") or top_row.get("qualification_name"),
            qualification_type      = qual_type,
            skip_country_multiplier = (str(qual_type).strip().lower() == "language"),
            project_id              = top_row.get("PROJECT_ID") or top_row.get("project_id"),
            qualification_id        = top_row.get("QUALIFICATION_ID") or top_row.get("qualification_id"),
        )
        log.info(
            "resolve_pay_rate: signup_flow_id=%s → qual='%s' (%s) T1=$%s skip_mult=%s",
            signup_flow_id, result.qualification_name, result.qualification_type,
            result.t1_rate_usd, result.skip_country_multiplier,
        )

    if cache is not None:
        cache[signup_flow_id] = result
    return result


def resolve_activations(
    signup_flow_id: str,
    *,
    redash_client = None,
    cache: Optional[dict] = None,
) -> ActivationsResult:
    """Count activations + skill_passes attributed to a signup_flow_id.

    Args:
        signup_flow_id: e.g., '5f6a...'.
        redash_client:  Optional injected client (for tests).
        cache:          Optional dict — share across calls in the same nightly
                        sweep to avoid re-querying.

    Returns:
        ActivationsResult — counts are 0 if nothing found OR on query failure
        (logged at WARNING level; never raises).
    """
    if not signup_flow_id or not str(signup_flow_id).strip():
        return ActivationsResult(signup_flow_id="", activations=0, skill_passes=0)

    signup_flow_id = signup_flow_id.strip()
    if cache is not None and signup_flow_id in cache:
        return cache[signup_flow_id]

    try:
        sql = _ACTIVATIONS_SQL_PATH.read_text().replace("{signup_flow_id}", _esc(signup_flow_id))
        client = redash_client or _default_client()
        df = client._run_query(sql, label=f"activations-{signup_flow_id[:12]}")
    except Exception as exc:
        log.warning(
            "resolve_activations: Redash call failed for %s (%s) — returning 0/0",
            signup_flow_id, exc,
        )
        result = ActivationsResult(signup_flow_id=signup_flow_id, activations=0, skill_passes=0)
        if cache is not None:
            cache[signup_flow_id] = result
        return result

    if df.empty:
        log.info("resolve_activations: no funnel data for signup_flow_id=%s", signup_flow_id)
        result = ActivationsResult(signup_flow_id=signup_flow_id, activations=0, skill_passes=0)
    else:
        row = df.iloc[0]
        result = ActivationsResult(
            signup_flow_id = signup_flow_id,
            activations    = int(row.get("ACTIVATIONS") or row.get("activations") or 0),
            skill_passes   = int(row.get("SKILL_PASSES") or row.get("skill_passes") or 0),
        )
        log.info(
            "resolve_activations: signup_flow_id=%s → activations=%d skill_passes=%d",
            signup_flow_id, result.activations, result.skill_passes,
        )

    if cache is not None:
        cache[signup_flow_id] = result
    return result


# ── Internals ───────────────────────────────────────────────────────────


def _empty_pay_rate_result() -> PayRateResult:
    return PayRateResult(
        t1_rate_usd=None, qualification_name=None, qualification_type=None,
        skip_country_multiplier=False, project_id=None, qualification_id=None,
    )


def _default_client():
    from src.redash_db import RedashClient
    return RedashClient()


def _esc(val: str) -> str:
    """SQL-string escape for parameter substitution. Mirrors src/redash_db.py:_esc."""
    return str(val).replace("'", "''")

"""src/console_db.py
====================

Minimal Postgres connector for the console's `qc_rule_overrides` table.

The console (outlier-campaign-console) stores reviewer-flagged skip rules
per ramp in Postgres. The pipeline needs to read those overrides at regen
time so it can suppress matching QC rules during gen. Both repos share the
same `DATABASE_URL` (Vercel Postgres pooled connection) via Doppler.

Read-only by design — only the console writes to this table.
"""
from __future__ import annotations

import logging
import os
from typing import Iterable

log = logging.getLogger(__name__)


def list_qc_rule_overrides(ramp_id: str) -> set[str]:
    """Return the set of rule_name strings the reviewer has skip-flagged for
    this ramp. Empty set when nothing is flagged OR when DATABASE_URL is not
    available (e.g., local dev without Doppler).

    Never raises — connection errors fall back to an empty set so a regen
    attempt with no override DB still runs (the QC will reject the same
    rules that failed last time, but the run completes gracefully).
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        log.warning(
            "DATABASE_URL not set — cannot read qc_rule_overrides for ramp=%s. "
            "Regen will use default QC rules (no overrides applied).",
            ramp_id,
        )
        return set()

    try:
        import psycopg
    except ImportError:
        log.error(
            "psycopg not installed — install psycopg[binary] to read overrides. "
            "Regen will use default QC rules.",
        )
        return set()

    try:
        with psycopg.connect(db_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT rule_name FROM qc_rule_overrides WHERE ramp_id = %s",
                    (ramp_id,),
                )
                names = {row[0] for row in cur.fetchall()}
                log.info("Loaded %d QC override(s) for ramp=%s: %s", len(names), ramp_id, sorted(names))
                return names
    except Exception as exc:
        log.warning(
            "Postgres read failed for qc_rule_overrides (ramp=%s): %s — "
            "falling back to empty override set",
            ramp_id, exc,
        )
        return set()


# Pattern → rule_name. MIRROR of app/ramps/[id]/sections/failure-analysis.tsx
# RULE_PATTERNS. Keep in sync.
import re

_RULE_PATTERNS: list[tuple[re.Pattern[str], str]] = [
    (re.compile(r"headline.*words", re.I), "headline_word_count"),
    (re.compile(r"headline.*chars", re.I), "headline_char_count"),
    (re.compile(r"headline.*wraps?", re.I), "headline_line_wrap"),
    (re.compile(r"subheadline.*words", re.I), "subheadline_word_count"),
    (re.compile(r"subheadline.*chars", re.I), "subheadline_char_count"),
    (re.compile(r"subheadline.*wraps?", re.I), "subheadline_line_wrap"),
    (re.compile(r"brand.?voice|banned", re.I), "brand_voice"),
    (re.compile(r"cta_button", re.I), "cta_enum"),
    (re.compile(r"rendered.text|text.in.photo|words.*image|letters.*image", re.I), "image_text"),
    (re.compile(r"matches_reference|mimic", re.I), "image_mimicry"),
    (re.compile(r"headroom|crop|gap", re.I), "image_headroom"),
    (re.compile(r"contrast|overlap", re.I), "image_contrast"),
    (re.compile(r"subject|authentic", re.I), "image_subject"),
]


def classify_violation(violation: str) -> str:
    """Map a raw violation string to the canonical rule_name used by the
    console's override toggles. Returns "other" when no pattern matches.
    """
    for pat, name in _RULE_PATTERNS:
        if pat.search(violation):
            return name
    return "other"


def list_approved_negative_keywords(ramp_id: str) -> list[str]:
    """Return the negative keywords Bryan APPROVED for this ramp via the console
    (negative_keyword_overrides where approved=true). Merged on top of the
    confident config defaults by the Google Search arm. Never raises — missing
    DB/table returns []."""
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url or not ramp_id:
        return []
    try:
        import psycopg
    except ImportError:
        return []
    try:
        with psycopg.connect(db_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT keyword FROM negative_keyword_overrides "
                    "WHERE ramp_id = %s AND approved = TRUE",
                    (ramp_id,),
                )
                kws = [str(row[0]) for row in cur.fetchall() if row and row[0]]
                if kws:
                    log.info("Loaded %d approved negative keyword(s) for ramp=%s", len(kws), ramp_id)
                return kws
    except Exception as exc:
        log.debug("negative_keyword_overrides read failed (ramp=%s): %s", ramp_id, exc)
        return []


def get_lp_url_override(ramp_id: str, platform: str) -> str:
    """Return the reviewer-set LP URL override for (ramp_id × platform),
    or "" when no override exists. Read by utm_builder.resolve_base_lp_url
    BEFORE the Smart Ramp campaign_state lookup so reviewer intent wins.

    Never raises — connection errors / missing table return "".
    """
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url or not ramp_id or not platform:
        return ""
    try:
        import psycopg
    except ImportError:
        return ""
    try:
        with psycopg.connect(db_url, connect_timeout=10) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT url FROM lp_url_overrides "
                    "WHERE ramp_id = %s AND platform = %s",
                    (ramp_id, platform.lower()),
                )
                row = cur.fetchone()
                if row and row[0]:
                    log.info("Using lp_url override for ramp=%s platform=%s", ramp_id, platform)
                    return str(row[0])
                return ""
    except Exception as exc:
        log.debug("lp_url_overrides read failed (ramp=%s): %s", ramp_id, exc)
        return ""


def filter_violations_by_overrides(
    violations: Iterable[str], skip_rules: set[str]
) -> list[str]:
    """Return a new list of violations with all entries whose classification
    is in `skip_rules` removed. Used to soften the QC verdict during regen
    when reviewers have explicitly flagged a rule to skip.
    """
    if not skip_rules:
        return list(violations)
    out = []
    for v in violations:
        if classify_violation(v) in skip_rules:
            log.debug("Suppressing violation per override: %r", v)
            continue
        out.append(v)
    return out

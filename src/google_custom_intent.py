"""Google Custom Intent Audiences — PII-free targeting analog to Meta LAL.

Bypasses Google's `user_interest` taxonomy (which is consumer-shaped — returns
0 segments for terms like "java" or "deep learning") by registering each
cohort's signal keywords as Custom Intent Audience seeds. Google then builds
the audience of users who have recently searched for / visited URLs related
to those keywords.

## Why this path vs Customer Match upload

Customer Match (Snowflake → SHA256 → OfflineUserDataJobService.upload) is the
canonical PII-based audience workflow but requires raw contributor emails/
phones from Snowflake, which the codebase policy-restricts (see
`src/icp_exemplars.py:33`). Until that policy + data access lands, Custom
Intent gives us a PII-free, keyword-driven targeting layer:

  cohort.rules → keyword pool → CustomAudienceService.create →
    AdGroupCriterion.user_list

## Idempotency

Per-`cohort_signature` state persists in
`data/google_custom_intent_registry.json` so re-runs across days don't spawn
duplicate audiences — Google has soft limits on custom audiences per account
and creates clutter that's hard to clean up.

## Lifecycle

Unlike Customer Match (24-48h matching window), Custom Intent Audiences
populate immediately — Google can match against its search-query history
in real time. Ad-set delivery starts within minutes of audience creation.
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import re
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import config

log = logging.getLogger(__name__)

_DEFAULT_REGISTRY_PATH = Path("data/google_custom_intent_registry.json")
_MAX_KEYWORDS = 200       # Google's per-audience limit on Custom Intent seeds
_MIN_KEYWORD_LEN = 3

_thread_lock = threading.Lock()


# ── Public API ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class CustomIntentResult:
    """Output of get_or_create_custom_intent."""
    audience_resource:  str   # `customers/{cid}/audiences/{id}` resource name
    cohort_signature:   str
    keyword_count:      int
    newly_created:      bool


def get_or_create_custom_intent(
    cohort_signature: str,
    keywords: list[str],
    *,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
    google_client = None,    # injectable for tests
) -> CustomIntentResult:
    """Return the Custom Intent Audience resource name for this cohort.

    Idempotent: same cohort_signature → same audience, even across runs.
    First call creates the audience on Google's side; subsequent calls
    return the cached resource name without hitting the API.

    Args:
        cohort_signature: stable identifier for the cohort (e.g.,
            "skills__python + fields_of_study__cs"). Used as the registry key.
        keywords: free-text keywords / search queries to seed the audience.
            Will be deduped, cleaned, and truncated to top _MAX_KEYWORDS.
        registry_path: persistent mapping override (test injection)
        google_client: injectable GoogleAdsClient (for tests)

    Returns:
        CustomIntentResult with the audience resource name.

    Raises:
        ValueError on empty cohort_signature or no valid keywords.
    """
    if not cohort_signature or not str(cohort_signature).strip():
        raise ValueError("cohort_signature must be non-empty")
    cohort_signature = cohort_signature.strip()

    cleaned = _clean_keywords(keywords)
    if not cleaned:
        raise ValueError(
            f"No valid keywords supplied for cohort '{cohort_signature}' "
            f"(min length {_MIN_KEYWORD_LEN}, alphanumeric-ish)"
        )

    path = Path(registry_path)
    with _thread_lock, _file_lock(path) as data:
        if cohort_signature in data["registry"]:
            cached = data["registry"][cohort_signature]
            log.debug("google_custom_intent: cache hit for %r → %s", cohort_signature, cached["resource"])
            return CustomIntentResult(
                audience_resource=cached["resource"],
                cohort_signature=cohort_signature,
                keyword_count=cached.get("keyword_count", len(cleaned)),
                newly_created=False,
            )

        client = google_client or _default_google_client()
        resource = _create_custom_intent_on_google(client, cohort_signature, cleaned)
        data["registry"][cohort_signature] = {
            "resource":      resource,
            "keyword_count": len(cleaned),
            "keywords_preview": cleaned[:10],   # store a sample for debugging
        }
        _save(path, data)
        log.info(
            "google_custom_intent: created audience %s for cohort '%s' (%d keywords)",
            resource, cohort_signature, len(cleaned),
        )
        return CustomIntentResult(
            audience_resource=resource,
            cohort_signature=cohort_signature,
            keyword_count=len(cleaned),
            newly_created=True,
        )


def resolve_custom_intent_for_cohort(
    cohort_signature: str,
    cohort_rules: list[tuple],
    *,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
    google_client = None,
) -> Optional[str]:
    """High-level wrapper: extract keywords from cohort.rules, ensure the
    audience exists, return the resource name.

    Returns None (instead of raising) if Google API fails — caller falls back
    to keyword/segment-only targeting.
    """
    keywords = _extract_keywords_from_rules(cohort_rules)
    if not keywords:
        log.info(
            "google_custom_intent: cohort '%s' has no usable keywords in rules — skipping audience",
            cohort_signature,
        )
        return None
    try:
        result = get_or_create_custom_intent(
            cohort_signature, keywords,
            registry_path=registry_path, google_client=google_client,
        )
        return result.audience_resource
    except Exception as exc:
        log.warning(
            "google_custom_intent: failed to resolve for cohort '%s' (%s) — falling back",
            cohort_signature, exc,
        )
        return None


# ── Internals ───────────────────────────────────────────────────────────


_KEYWORD_RX = re.compile(r"^[a-z0-9][a-z0-9 \-/]{1,80}$", re.IGNORECASE)


def _clean_keywords(raw: list[str]) -> list[str]:
    """Dedupe, normalize, validate, truncate."""
    out: list[str] = []
    seen: set[str] = set()
    for kw in raw or []:
        if not kw:
            continue
        norm = " ".join(str(kw).split()).strip()
        if len(norm) < _MIN_KEYWORD_LEN:
            continue
        if not _KEYWORD_RX.match(norm):
            continue
        key = norm.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(norm)
        if len(out) >= _MAX_KEYWORDS:
            break
    return out


def _extract_keywords_from_rules(rules: list[tuple]) -> list[str]:
    """Pull human-readable terms out of cohort.rules entries.

    cohort.rules format: [(feature_col, value), ...] where feature_col is
    e.g. "skills__python" or "job_titles_norm__data_scientist". We strip the
    prefix and unsuffix underscores to get the search-query-like phrase.
    """
    keywords: list[str] = []
    for entry in rules or []:
        # Support both (feat, val) tuples and plain feature strings
        if isinstance(entry, (list, tuple)):
            feat = entry[0] if entry else ""
        else:
            feat = entry
        feat = str(feat or "")
        if "__" not in feat:
            keywords.append(feat)
            continue
        tail = feat.split("__", 1)[1]
        # job_titles_norm__data_scientist → "data scientist"
        keywords.append(re.sub(r"[_]+", " ", tail).strip())
    return keywords


def _create_custom_intent_on_google(
    client,
    cohort_signature: str,
    keywords: list[str],
) -> str:
    """Call Google Ads API to create a Custom Audience with intent dimension.

    Returns the resource name (`customers/{cid}/audiences/{id}`).
    Raises on API failure — caller catches.
    """
    # Lazy SDK init — mirrors GoogleAdsClient pattern.
    ga = client._ensure_client()
    # Suffix with a short hash of cohort_signature so name stays unique + readable.
    import hashlib
    sig_hash = hashlib.sha1(cohort_signature.encode()).hexdigest()[:8]
    name = f"agent_intent_{sig_hash}"

    audience_service = ga.get_service("CustomAudienceService")
    op = ga.get_type("CustomAudienceOperation")
    audience = op.create
    audience.name = name
    audience.description = (
        f"Outlier auto-generated Custom Intent audience for cohort "
        f"'{cohort_signature[:80]}'. Seeded from {len(keywords)} keywords."
    )
    audience.type_ = ga.enums.CustomAudienceTypeEnum.SEARCH

    # Each keyword becomes a CustomAudienceMember of dimension=KEYWORD
    for kw in keywords:
        member = audience.members.add()
        member.member_type = ga.enums.CustomAudienceMemberTypeEnum.KEYWORD
        member.keyword = kw

    response = audience_service.mutate_custom_audiences(
        customer_id=client._customer_id_str,
        operations=[op],
    )
    return response.results[0].resource_name


def _default_google_client():
    from src.google_ads_api import GoogleAdsClient
    return GoogleAdsClient()


# ── Registry I/O + locking (mirrors src/targeting_id.py + src/meta_lal.py) ──


@contextlib.contextmanager
def _file_lock(path: Path):
    path.parent.mkdir(parents=True, exist_ok=True)
    if not path.exists():
        path.write_text(json.dumps({"registry": {}}, indent=2))
    fh = open(path, "r+")
    try:
        fcntl.flock(fh.fileno(), fcntl.LOCK_EX)
        fh.seek(0)
        try:
            data = json.loads(fh.read() or "{}")
        except json.JSONDecodeError:
            log.warning("google_custom_intent: registry at %s was corrupted; resetting", path)
            data = {"registry": {}}
        data.setdefault("registry", {})
        yield data
    finally:
        try:
            fcntl.flock(fh.fileno(), fcntl.LOCK_UN)
        finally:
            fh.close()


def _save(path: Path, data: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, sort_keys=True))

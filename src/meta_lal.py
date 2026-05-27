"""Meta Lookalike Audiences — PII-free path.

Creates 1% Lookalike Audiences seeded from existing Custom Audiences (NOT
from a PII upload). The 4 "Actives" audiences already in Outlier's Meta ad
account (Generalists/Coders/Languages/Specialists Actives - 12.12.24) serve
as the seed pool — each gets a 1% LAL built per target country.

## Why this path vs PII-upload Customer Audiences

PII upload (Snowflake → SHA256 → CustomAudience.add_users) is the canonical
Meta LAL workflow but requires raw contributor emails/phones from Snowflake,
which the codebase currently policy-restricts (src/icp_exemplars.py:33). Until
that policy + data access lands, we use the seed-from-existing-audience path:

  ExistingCustomAudience  →  CustomAudience(subtype=LOOKALIKE, lookalike_spec)

Operationally equivalent for prospecting — Meta builds a 1% lookalike pool
of users SIMILAR to active contributors, while the ad set still EXCLUDES the
active contributors themselves (via excluded_custom_audiences). Two disjoint
sets, both pulling from the same seed.

## Idempotency

Per-(seed_audience_id × country × ratio) state persists in
`data/meta_lal_registry.json` so re-runs don't spawn duplicate LAL audiences
— Meta charges per audience created and has a hard limit on parallel LAL
builds per account.

## Lifecycle

LAL audiences take ~24h to populate after creation. Pipeline does NOT block
on the populate — it creates the LAL, registers the ID, and ad-set targeting
references the ID even while it's still pre-populated (Meta's ad delivery
gracefully degrades to broad until the LAL fills, then narrows automatically).
"""
from __future__ import annotations

import contextlib
import fcntl
import json
import logging
import threading
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Optional

import config

log = logging.getLogger(__name__)

_DEFAULT_REGISTRY_PATH = Path("data/meta_lal_registry.json")
_KEY_SEP = "|"
_DEFAULT_RATIO = 0.01    # 1% lookalike

# In-process thread lock — combine with fcntl for cross-process safety.
_thread_lock = threading.Lock()


# ── Public API ──────────────────────────────────────────────────────────


@dataclass(frozen=True)
class LookalikeResult:
    """Output of get_or_create_lookalike."""
    audience_id:   str       # Meta CustomAudience ID for the LAL
    seed_audience_id: str
    country:       str
    ratio:         float
    newly_created: bool      # True if we just minted it; False if cache-hit


def get_or_create_lookalike(
    seed_audience_id: str,
    country: str,
    *,
    ratio: float = _DEFAULT_RATIO,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
    meta_client = None,    # injectable for tests
) -> LookalikeResult:
    """Return the LAL CustomAudience ID for (seed × country × ratio).

    Idempotent — if we've already created this LAL, returns the cached ID.
    Otherwise calls Meta's API to create a new LookalikeAudience and stores
    the mapping.

    Args:
        seed_audience_id: Existing Meta CustomAudience to seed from (e.g.,
            "120211889244260257" Generalists Actives - 12.12.24).
        country: ISO-2 country code (e.g., "US"). Meta builds the LAL within
            that country's user pool.
        ratio: Lookalike ratio in (0, 1]. 0.01 = top 1% (highest fidelity);
            0.10 = top 10% (broader, lower fidelity). Default 0.01.
        registry_path: where the persistent mapping lives.
        meta_client: optional injectable MetaClient (for tests).

    Returns:
        LookalikeResult with the LAL audience ID and whether it was newly created.
    """
    if not seed_audience_id or not str(seed_audience_id).strip():
        raise ValueError("seed_audience_id must be a non-empty string")
    if not country or not str(country).strip():
        raise ValueError("country must be a non-empty ISO-2 code")
    if not (0 < ratio <= 1):
        raise ValueError(f"ratio must be in (0, 1]; got {ratio}")

    key = _make_key(seed_audience_id.strip(), country.strip().upper(), ratio)
    path = Path(registry_path)

    with _thread_lock, _file_lock(path) as data:
        if key in data["registry"]:
            cached_id = data["registry"][key]
            log.debug("meta_lal: cache hit for %r → %s", key, cached_id)
            return LookalikeResult(
                audience_id=cached_id,
                seed_audience_id=seed_audience_id,
                country=country.upper(),
                ratio=ratio,
                newly_created=False,
            )

        # No cached LAL — create one via Meta API.
        client = meta_client or _default_meta_client()
        lal_id = _create_lookalike_on_meta(
            client, seed_audience_id, country.upper(), ratio,
        )
        data["registry"][key] = lal_id
        _save(path, data)
        log.info(
            "meta_lal: created LAL %s from seed=%s country=%s ratio=%.2f",
            lal_id, seed_audience_id, country, ratio,
        )
        return LookalikeResult(
            audience_id=lal_id,
            seed_audience_id=seed_audience_id,
            country=country.upper(),
            ratio=ratio,
            newly_created=True,
        )


def resolve_lal_audiences_for_targeting(
    countries: list[str],
    *,
    seed_audiences: Optional[list[str]] = None,
    ratio: float = _DEFAULT_RATIO,
    registry_path: str | Path = _DEFAULT_REGISTRY_PATH,
    meta_client = None,
) -> list[dict[str, str]]:
    """Build the Meta `targeting.custom_audiences` payload (inclusion).

    For each country in `countries`, ensures a 1% LAL exists for each seed
    audience and returns the list of audience-ID dicts ready to drop into
    `targeting["custom_audiences"]` for ad-set creation.

    Args:
        countries: list of ISO-2 country codes the ad set targets
        seed_audiences: optional override; default = `config.META_LAL_SEED_AUDIENCES`
            (the 4 Actives audiences)
        ratio: default 0.01 (1%)
        registry_path, meta_client: same as get_or_create_lookalike

    Returns:
        [{"id": "<lal_audience_id>"}, ...] — at most len(seeds) × len(countries)
        entries. Empty list if Meta API fails on every attempt (caller falls
        back to broad targeting).
    """
    seed_list = seed_audiences if seed_audiences is not None else list(
        getattr(config, "META_LAL_SEED_AUDIENCES", []) or []
    )
    if not seed_list:
        log.info("meta_lal: no seed audiences configured — returning empty LAL list")
        return []

    out: list[dict[str, str]] = []
    for country in countries:
        if not country:
            continue
        for seed_id in seed_list:
            try:
                result = get_or_create_lookalike(
                    seed_id, country,
                    ratio=ratio,
                    registry_path=registry_path,
                    meta_client=meta_client,
                )
                out.append({"id": result.audience_id})
            except Exception as exc:
                log.warning(
                    "meta_lal: failed to resolve LAL for seed=%s country=%s (%s) — skipping",
                    seed_id, country, exc,
                )
    return out


# ── Internals ───────────────────────────────────────────────────────────


def _create_lookalike_on_meta(
    client,
    seed_audience_id: str,
    country: str,
    ratio: float,
) -> str:
    """Call Meta's API to create a 1% Lookalike CustomAudience. Returns the new ID.

    Raises on any Meta-side failure — caller catches and continues with
    other (seed × country) combinations.
    """
    # Lazy import — facebook_business should only be required when actually
    # calling Meta. Mirrors MetaClient pattern.
    from facebook_business.adobjects.adaccount import AdAccount
    from facebook_business.adobjects.customaudience import CustomAudience

    client._ensure_init()
    account = AdAccount(client._ad_account_id)
    name = f"{client.AGENT_NAME_PREFIX}lal_{seed_audience_id[-8:]}_{country}_{int(ratio*100)}pct"

    # Meta's lookalike_spec wants:
    #  1. A JSON-encoded STRING (NOT a dict — confirmed via 400 #2654 on
    #     2026-05-27 smoke test). The facebook_business SDK warns on dict
    #     input but the API outright rejects unless it's JSON-stringified.
    #  2. Each `origin` entry MUST carry `"type": "custom_audience"` — without
    #     it Meta returns "No custom_audience ID given for lookalike cluster".
    import json as _json
    lookalike_spec = _json.dumps({
        "origin":  [{"id": seed_audience_id, "type": "custom_audience"}],
        "country": country,
        "type":    "similarity",  # alternatives: 'reach' (broader)
        "ratio":   ratio,
    })

    params = {
        CustomAudience.Field.name:           name,
        CustomAudience.Field.subtype:        "LOOKALIKE",
        CustomAudience.Field.lookalike_spec: lookalike_spec,
    }
    audience = account.create_custom_audience(params=params)
    return str(audience["id"])


def _default_meta_client():
    from src.meta_api import MetaClient
    return MetaClient()


def _make_key(seed_id: str, country: str, ratio: float) -> str:
    # Round ratio to 4 decimal places to ensure consistent string keys.
    return f"{seed_id}{_KEY_SEP}{country.upper()}{_KEY_SEP}{ratio:.4f}"


# ── Registry I/O + locking (mirrors src/targeting_id.py pattern) ────────


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
            log.warning("meta_lal: registry at %s was corrupted; resetting", path)
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

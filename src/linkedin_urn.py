"""
LinkedIn URN resolver — fuzzy-matches facet values to LinkedIn URNs
using the mapping Google Sheet (URN_SHEET_ID).

Tab names mirror LinkedIn facet names:
  Skills, Titles, ProfileLocations, Degrees, FieldsOfStudy, Industries
"""
import logging
from functools import lru_cache
from typing import Any

from rapidfuzz import fuzz, process

import config
from src.targeting_resolver import TargetingResolver

log = logging.getLogger(__name__)

# Map from our internal facet key → sheet tab name
FACET_TAB_MAP = {
    "skills":       "Skills",
    "titles":       "Titles",
    "fieldsOfStudy": "FieldsOfStudy",
    "degrees":      "Degrees",
    "profileLocations": "ProfileLocations",
    "industries":   "Industries",
    "accreditations": "Skills",   # treat accreditations as skills
}

# Map from our internal facet key → LinkedIn targeting facet API name
FACET_API_NAME = {
    "skills":           "skills",
    "titles":           "titles",
    "fieldsOfStudy":    "fieldsOfStudy",
    "degrees":          "degrees",
    "profileLocations": "profileLocations",
    "industries":       "industries",
    "accreditations":   "skills",
}


class UrnResolver(TargetingResolver):
    """LinkedIn implementation of `TargetingResolver` — fuzzy-matches cohort
    facet values to LinkedIn URNs and returns a `{facet_api_name: [urn]}`
    dict ready for `LinkedInClient.create_campaign(..., facet_urns=...)`."""

    name = "linkedin"

    def __init__(self, sheets_client):
        self._sheets = sheets_client
        # { tab_name: [(name_lower, urn), ...] }
        self._cache: dict[str, list[tuple[str, str]]] = {}

    def resolve_cohort(
        self,
        cohort: Any,
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, list[str]]:
        """Platform-agnostic facade over `resolve_cohort_rules`.

        Existing call sites use `resolve_cohort_rules(cohort.rules)` directly;
        this method wraps that for the new `_process_static_campaigns(platform, ...)`
        dispatcher in main.py. Geos and exclude_pairs are not yet folded in
        here — the existing pipeline applies geo overrides + excludes via
        separate calls (see `_apply_geo_overrides`, `resolve_facet_pairs`).
        """
        rules = getattr(cohort, "rules", None) or []
        return self.resolve_cohort_rules(rules)

    def _load_tab(self, tab_name: str) -> list[tuple[str, str]]:
        if tab_name in self._cache:
            return self._cache[tab_name]
        try:
            rows = self._sheets.read_urn_tab(tab_name)
            # Each row is a dict; expect keys 'name' and 'urn' (case-insensitive)
            entries = []
            for row in rows:
                urn = str(row.get("urn") or row.get("URN") or "").strip()
                # Name column varies per tab (e.g. 'Skills', 'Job Titles', 'Country').
                # Pick the first column that isn't the URN column or a timestamp.
                _skip = {"urn", "fetched at", "fetched_at"}
                name_key = next(
                    (k for k in row.keys() if k.lower() not in _skip), None
                )
                name = str(row.get(name_key, "")).strip() if name_key else ""
                if name and urn:
                    entries.append((name.lower(), urn))
            self._cache[tab_name] = entries
            log.info("Loaded %d URNs from tab '%s'", len(entries), tab_name)
        except Exception as exc:
            log.warning("Could not load URN tab '%s': %s", tab_name, exc)
            self._cache[tab_name] = []
        return self._cache[tab_name]

    def resolve(self, facet_key: str, value: str) -> str | None:
        """
        Fuzzy-match `value` against the appropriate URN sheet tab.
        Returns the URN string if score >= threshold, else None.
        """
        tab_name = FACET_TAB_MAP.get(facet_key)
        if not tab_name:
            log.debug("No tab mapping for facet '%s'", facet_key)
            return None

        entries = self._load_tab(tab_name)
        if not entries:
            return None

        names = [e[0] for e in entries]
        result = process.extractOne(
            value.lower(),
            names,
            scorer=fuzz.WRatio,
        )
        if result is None:
            return None

        best_name, score, idx = result
        threshold = int(config.URN_FUZZY_MATCH_THRESHOLD * 100)
        if score >= threshold:
            urn = entries[idx][1]
            log.debug("Resolved '%s' → '%s' (score=%d)", value, urn, score)
            return urn

        log.debug("No URN match for '%s' (best='%s', score=%d < %d)",
                  value, best_name, score, threshold)
        return None

    def resolve_cohort_rules(self, rules: list[tuple]) -> dict[str, list[str]]:
        """
        Convert Cohort.rules [(feature_col, value), ...] to LinkedIn facet URN dict.

        Returns: { linkedin_facet_name: [urn, ...], ... }
        Skips features that can't be resolved.
        """
        from src.analysis import _feature_to_facet   # local import to avoid circular

        facet_urns: dict[str, list[str]] = {}
        for feat, val in rules:
            facet_key = _feature_to_facet(feat)
            api_name  = FACET_API_NAME.get(facet_key)
            if not api_name:
                continue

            # Extract the human-readable value from the column name
            # e.g. skills__python → python, job_titles_norm__data_scientist → data scientist
            human_val = _col_to_human(feat)
            if not human_val:
                continue

            urn = self.resolve(facet_key, human_val)
            if urn:
                facet_urns.setdefault(api_name, []).append(urn)
            else:
                log.warning("Could not resolve URN for %s='%s'", facet_key, human_val)

        return facet_urns

    def resolve_facet_pairs(self, pairs: list[tuple[str, str]]) -> dict[str, list[str]]:
        """
        Resolve a list of `(facet_short_name, value_name)` pairs to a
        `{facet_api_name: [urn, ...]}` dict — the same shape `resolve_cohort_rules`
        emits. Used for negation facets defined in `config.DEFAULT_EXCLUDE_FACETS`,
        where we already know the facet directly (no `_feature_to_facet` mapping
        from a binary column needed).

        Pairs whose value can't be fuzzy-matched are dropped with a warning so
        a typo in the exclude list doesn't break campaign creation.
        """
        out: dict[str, list[str]] = {}
        for facet_key, value in pairs:
            api_name = FACET_API_NAME.get(facet_key)
            if not api_name:
                log.warning("resolve_facet_pairs: unknown facet %r — skipping (%r)", facet_key, value)
                continue
            urn = self.resolve(facet_key, value)
            if urn:
                out.setdefault(api_name, []).append(urn)
            else:
                log.warning("resolve_facet_pairs: no URN match for %s='%s'", facet_key, value)
        return out

    def resolve_default_excludes(self) -> dict[str, list[str]]:
        """Resolve `config.DEFAULT_EXCLUDE_FACETS` once. Cached for reuse."""
        if not hasattr(self, "_default_exclude_cache"):
            self._default_exclude_cache = self.resolve_facet_pairs(
                list(getattr(config, "DEFAULT_EXCLUDE_FACETS", []))
            )
            log.info(
                "Resolved %d default exclude facet(s): %s",
                sum(len(v) for v in self._default_exclude_cache.values()),
                {k: len(v) for k, v in self._default_exclude_cache.items()},
            )
        return self._default_exclude_cache


def feature_col_to_exclude_pair(col: str) -> tuple[str, str] | None:
    """
    Convert a binary-feature column (e.g. `skills__Lead_Generation`) to the
    `(facet, value)` pair that `UrnResolver.resolve_facet_pairs` expects. Used
    by the data-driven exclusion pipeline: `stage_a_negative` returns feature
    column names; we need `(facet, value)` pairs for the URN resolver.

    Only returns a pair for facet families where our internal feature space
    maps cleanly to LinkedIn's targeting vocabulary. Skipped:
      - `experience_band`  — LinkedIn has no years-of-experience facet.
      - `degrees`          — our 4-bucket `highest_degree_level__*` reflects
                             resume-parser classification, not LinkedIn's
                             degrees taxonomy. In particular `Other` means
                             "parser didn't recognize the degree", NOT LinkedIn's
                             "Other" degree value — so excluding on it would
                             drop the wrong audience.
      - `accreditations`   — aliased to `skills` internally; safer to let
                             explicit accreditation exclusions come from the
                             role-adjacent lists where we can curate them.
    """
    _SAFE_FACETS_FOR_DATA_DRIVEN_EXCLUDE = {"skills", "titles", "fieldsOfStudy", "industries"}

    from src.analysis import _feature_to_facet  # local import to avoid circular
    facet_key = _feature_to_facet(col)
    api_name  = FACET_API_NAME.get(facet_key)
    if not api_name:
        return None
    if api_name not in _SAFE_FACETS_FOR_DATA_DRIVEN_EXCLUDE:
        log.info(
            "feature_col_to_exclude_pair: skipping %r — facet %r is unsafe for "
            "data-driven exclusions (our feature vocab ≠ LinkedIn's).",
            col, api_name,
        )
        return None
    human_val = _col_to_human(col)
    if not human_val:
        return None
    return (api_name, human_val)


def _col_to_human(col: str) -> str:
    """
    Convert binary column name back to a human-readable label.
    e.g. skills__python → python
         job_titles_norm__data_scientist → data scientist
         highest_degree_level__Bachelors → Bachelors
         experience_band__5-7 → 5-7
    """
    prefixes = [
        "skills__",
        "job_titles_norm__",
        "fields_of_study__",
        "highest_degree_level__",
        "accreditations_norm__",
        "experience_band__",
    ]
    for p in prefixes:
        if col.startswith(p):
            return col[len(p):].replace("_", " ").replace("plus", "+")
    return col

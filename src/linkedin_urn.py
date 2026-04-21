"""
LinkedIn URN resolver — fuzzy-matches facet values to LinkedIn URNs
using the mapping Google Sheet (URN_SHEET_ID).

Tab names mirror LinkedIn facet names:
  Skills, Titles, ProfileLocations, Degrees, FieldsOfStudy, Industries
"""
import logging
from functools import lru_cache

from rapidfuzz import fuzz, process

import config

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


class UrnResolver:
    def __init__(self, sheets_client):
        self._sheets = sheets_client
        # { tab_name: [(name_lower, urn), ...] }
        self._cache: dict[str, list[tuple[str, str]]] = {}

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

"""Guard against a cold-start LinkedIn cohort shipping geo-only.

Cold-start cohorts (n_icp < MIN_POSITIVES_FOR_STATS, including empty-frame ramps
like GMR-0024's BLV / accessibility ramp) are returned straight out of
`_resolve_cold_start_cohort` — they BYPASS Stage C, so Stage C's
`no_urns_resolved` reject never runs for them.

Their skill/title facets are coined by an LLM from the job post, not drawn from
LinkedIn's targeting taxonomy, so they frequently don't resolve to real URNs.
When that happens `UrnResolver.resolve_cohort_rules` returns `{}` and the arm's
`_apply_geo_overrides` then fills in `profileLocations` — shipping a campaign
that targets the WHOLE country (e.g. ~290M for the US). This module detects that
collapse so the LinkedIn arm can route the cohort to a human instead of burning
budget on geo-only.

Channel-scoped by construction: only the LinkedIn arm and LinkedIn audience
measurement call this. Google (keyword intent — invented strings ARE the
keywords) and Meta keep using the cohort's rules unchanged.
"""
from typing import Any

# Rule prefixes that are SUPPOSED to become a non-geo LinkedIn facet. A cohort
# carrying any of these "intends" facet targeting — so if none resolve, that's a
# collapse, not a deliberately-broad cohort.
_TARGETING_PREFIXES = (
    "skills__",
    "job_titles_norm__",
    "fields_of_study__",
    "industries__",
    "highest_degree_level__",
    "accreditations_norm__",
)


def has_facet_targeting_rules(cohort: Any) -> bool:
    """True when the cohort carries at least one rule meant to resolve to a
    non-geo LinkedIn facet."""
    return any(
        str(feat).startswith(_TARGETING_PREFIXES)
        for feat, _ in (getattr(cohort, "rules", None) or [])
    )


def is_generalist_locale(cohort: Any) -> bool:
    """Generalist-locale cohorts (e.g. ko-KR) are geo(+language-skill) targeted
    by design — geo-only is correct for them, not a collapse."""
    return bool((getattr(cohort, "facet_strength", None) or {}).get("generalist_locale"))


def linkedin_targeting_collapsed(cohort: Any, facet_urns: dict) -> bool:
    """True when a cohort meant to be facet-targeted resolved to NO non-geo
    LinkedIn facet — i.e. shipping `facet_urns` now would target geo-only.

    `facet_urns` is the post-`_apply_geo_overrides` dict
    ({api_name: [urn, ...]}); `profileLocations` is ignored because geo is added
    by the override, not by the cohort's own targeting.
    """
    if is_generalist_locale(cohort):
        return False
    if not has_facet_targeting_rules(cohort):
        return False
    non_geo = {
        k: v for k, v in (facet_urns or {}).items()
        if k != "profileLocations" and v
    }
    return not non_geo

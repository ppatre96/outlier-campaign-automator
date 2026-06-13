"""
Reddit Ads `TargetingResolver` — translates platform-neutral cohort signals
into a Reddit targeting payload.

Reddit's primary targeting lever is COMMUNITY (subreddit) targeting, layered
optionally with interests + keywords + geo. There is no URN/interest-ID lookup
to do (unlike LinkedIn/Meta), so this resolver is pure config + cohort-derived
pod — no external API call. That keeps the Reddit arm buildable even before
the allow-list Ads API access is granted (v1 is creative-only).

The pod is derived from the cohort the same way campaign naming does it
(`campaign_name._pod_label`), then mapped to its default subreddit list in
`config.REDDIT_POD_SUBREDDITS` (override per ramp via REDDIT_POD_SUBREDDITS_JSON).

Output shape (consumed by the Reddit handoff manifest in Phase 1 and by
`RedditClient.create_campaign` in Phase 2):

  {
    "geo_locations": ["US", "CA", ...],   # ISO country codes
    "subreddits":    ["cscareerquestions", "programming", ...],
    "interests":     [...],               # optional, from config.REDDIT_INTERESTS
    "keywords":      [...],               # optional, from config.REDDIT_KEYWORDS
    "pod":           "coders",            # derived; drives per-pod conversion event
  }
"""
from __future__ import annotations

import logging
from typing import Any

import config
from src.targeting_resolver import TargetingResolver

log = logging.getLogger(__name__)


class RedditSubredditResolver(TargetingResolver):
    """Translate a cohort into a Reddit community-targeting payload.

    No network call — community/interest/keyword targeting comes from config,
    so the resolver works with or without Reddit Ads API access.
    """

    name = "reddit"

    def resolve_cohort(
        self,
        cohort: Any,
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        # Derive the pod the same way campaign naming does (job_post_pod, with a
        # classify_tg fallback) → one of coders|specialist|languages|generalist.
        try:
            from src.campaign_name import _pod_label
            pod = _pod_label(cohort, None)
        except Exception as exc:  # pragma: no cover - defensive
            log.debug("reddit resolver: pod derivation failed (%s) — defaulting generalist", exc)
            pod = "generalist"

        subreddits = list(
            config.REDDIT_POD_SUBREDDITS.get(pod)
            or config.REDDIT_POD_SUBREDDITS.get("generalist")
            or []
        )
        countries = [g.upper() for g in (geos or []) if g]

        return {
            "geo_locations": countries,
            "subreddits":    subreddits,
            "interests":     list(getattr(config, "REDDIT_INTERESTS", []) or []),
            "keywords":      list(getattr(config, "REDDIT_KEYWORDS", []) or []),
            "pod":           pod,
        }

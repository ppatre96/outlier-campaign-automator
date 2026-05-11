"""
Platform-agnostic targeting resolver interface.

Each ad platform encodes audience targeting differently:
  - LinkedIn: facet URNs (urn:li:skill:..., urn:li:title:...).
  - Meta:     interest IDs + demographic enums.
  - Google:   audience segment IDs + geo_target_constants + keywords.

The pipeline produces platform-neutral cohort signals (skill names, job-title
strings, geos, prestige tier, etc. — see src/analysis.Cohort.rules). Each
platform has its own `TargetingResolver` implementation that translates
those signals into the payload shape its API expects.

`resolve_cohort()` is the only contract the pipeline depends on. The return
value is a dict whose shape is platform-specific — the caller hands it
straight back to the matching `AdPlatformClient.create_campaign(..., targeting=...)`.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING

if TYPE_CHECKING:
    from src.analysis import Cohort  # avoid runtime import cycle


class TargetingResolver(ABC):
    """Translates platform-neutral cohort signals into platform-native targeting."""

    name: str  # "linkedin" | "meta" | "google"

    @abstractmethod
    def resolve_cohort(
        self,
        cohort: "Cohort",
        geos: list[str] | None = None,
        exclude_pairs: list[tuple[str, str]] | None = None,
    ) -> dict[str, Any]:
        """Return a platform-native targeting payload.

        Args:
          cohort: an analysis.Cohort. `cohort.rules` is `[(feature_col, value), ...]`
                  (e.g., `("skills__python", "python")`), platform-neutral.
          geos: optional list of ISO country codes overriding cohort default.
          exclude_pairs: optional list of `(facet_short_name, value)` tuples to
                         exclude (recruiters, sales, etc.).

        Returns:
          A dict the matching `AdPlatformClient.create_campaign()` can consume.
          Shape is platform-specific. Implementations document their schema.
        """

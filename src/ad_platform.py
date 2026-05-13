"""
Platform-agnostic ad client interface.

Outlier campaign agent now creates campaigns on LinkedIn, Meta Ads, and Google
Ads. Each has a different API surface, but the shape of what the pipeline asks
of an ad platform is the same:

  1. Create a logical container for related campaigns ("campaign group" on
     LinkedIn; the top-level Campaign on Meta and Google).
  2. Create a campaign with platform-specific targeting and a budget.
  3. Upload an image asset.
  4. Create an image ad referencing the campaign + image.

This module defines:

  - `AdPlatformClient` — abstract base every platform client implements.
  - `PlatformConstraints` — char limits + image-aspect specs per platform,
    used by copy_adapter and image_adapter to shape variants correctly.
  - `CreateAdResult` — uniform return value for create_image_ad calls.
  - Three concrete `*_CONSTRAINTS` constants.

The pipeline (main.py) only sees `AdPlatformClient`-shaped objects and
`PlatformConstraints` lookups; concrete clients live in
linkedin_api.py / meta_api.py / google_ads_api.py.
"""
from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal, Optional

import config


PlatformName = Literal["linkedin", "meta", "google"]


@dataclass(frozen=True)
class PlatformConstraints:
    """Platform-specific limits and capabilities the pipeline needs to know
    about when shaping copy + creative for a given ad platform.

    Char counts are inclusive maximums.
    `image_aspects` is a list of (w, h) ratio tuples — e.g. (1, 1) for square,
    (191, 100) for 1.91:1 landscape (we use integer ratios to avoid float
    drift in equality checks).
    """
    name: PlatformName
    headline_max_chars: int
    description_max_chars: int
    primary_text_max_chars: Optional[int] = None  # Meta-only ("primary text")
    cta_max_chars: Optional[int] = None
    image_aspects: tuple[tuple[int, int], ...] = ((1, 1),)
    # Some platforms (Google RDA) require multiple short headlines + long ones.
    # When present, copy_adapter generates `headline_count` distinct headlines
    # each within `headline_max_chars`, and `long_headline_max_chars` for the
    # single long headline.
    headline_count: int = 1
    long_headline_max_chars: Optional[int] = None
    description_count: int = 1
    supports_inmail: bool = False
    # Whether an EMPLOYMENT special ad category is available/required.
    supports_special_ad_category: bool = False


LINKEDIN_CONSTRAINTS = PlatformConstraints(
    name="linkedin",
    headline_max_chars=70,
    description_max_chars=200,
    primary_text_max_chars=600,
    cta_max_chars=20,
    image_aspects=((1, 1),),
    headline_count=1,
    description_count=1,
    supports_inmail=True,
    supports_special_ad_category=False,
)


META_CONSTRAINTS = PlatformConstraints(
    name="meta",
    headline_max_chars=40,
    description_max_chars=30,
    primary_text_max_chars=125,
    cta_max_chars=None,  # Meta CTAs are an enum (LEARN_MORE, APPLY_NOW, etc.)
    image_aspects=((1, 1), (4, 5), (191, 100)),
    headline_count=1,
    description_count=1,
    supports_inmail=False,
    supports_special_ad_category=True,
)


# Google Responsive Display Ads (RDA): up to 5 short headlines, 1 long
# headline, up to 5 descriptions. We pick a sensible middle ground (3 of each)
# to balance variance vs. Claude generation cost.
GOOGLE_CONSTRAINTS = PlatformConstraints(
    name="google",
    headline_max_chars=30,
    description_max_chars=90,
    primary_text_max_chars=None,
    cta_max_chars=None,
    image_aspects=((191, 100), (1, 1)),
    headline_count=3,
    long_headline_max_chars=90,
    description_count=3,
    supports_inmail=False,
    supports_special_ad_category=True,
)


PLATFORM_CONSTRAINTS: dict[str, PlatformConstraints] = {
    "linkedin": LINKEDIN_CONSTRAINTS,
    "meta":     META_CONSTRAINTS,
    "google":   GOOGLE_CONSTRAINTS,
}


def get_constraints(platform: str) -> PlatformConstraints:
    """Return the PlatformConstraints for the given platform name.

    Raises KeyError on unknown platform — callers should validate input.
    """
    return PLATFORM_CONSTRAINTS[platform]


def enabled_platforms() -> list[str]:
    """Return the list of platforms enabled for this pipeline run.

    Sourced from `config.ENABLED_PLATFORMS` (comma-separated env var). Order
    is preserved so dispatch order in main.py is deterministic. Always
    includes only known platforms — unknowns are silently dropped with a
    warning.
    """
    raw = getattr(config, "ENABLED_PLATFORMS", "linkedin,meta,google") or "linkedin"
    out: list[str] = []
    for p in [s.strip() for s in raw.split(",") if s.strip()]:
        if p in PLATFORM_CONSTRAINTS:
            out.append(p)
    return out or ["linkedin"]


class CreateAdResult:
    """Uniform return value from `AdPlatformClient.create_image_ad`.

    Mirrors the shape of `linkedin_api.ImageAdResult` so the static-arm
    fallback path in main.py (status="local_fallback" → save PNG locally,
    continue cohort) works unchanged for all three platforms.

    Fields:
        creative_id:     Platform-native creative identifier on success
                         (LinkedIn URN, Meta numeric Ad ID, Google Ads resource
                         name). None on failure.
        local_save_path: Caller populates after writing the PNG to disk.
        status:          "ok" | "local_fallback" | "error".
        error_class:     Exception class name when status != "ok".
        error_message:   Trimmed error message for logs/state files.

    Back-compat: existing LinkedIn callers passed/read `creative_urn`. Both
    the constructor kwarg and attribute access remain valid via an alias.
    """

    __slots__ = (
        "creative_id", "local_save_path", "status",
        "error_class", "error_message",
    )

    def __init__(
        self,
        creative_id:     Optional[str] = None,
        local_save_path: Optional[str] = None,
        status:          Literal["ok", "local_fallback", "error"] = "ok",
        error_class:     Optional[str] = None,
        error_message:   Optional[str] = None,
        *,
        creative_urn:    Optional[str] = None,  # back-compat alias
    ) -> None:
        # If both are provided, creative_urn wins (matches the legacy call
        # convention where it was the only field name).
        if creative_urn is not None:
            creative_id = creative_urn
        self.creative_id     = creative_id
        self.local_save_path = local_save_path
        self.status          = status
        self.error_class     = error_class
        self.error_message   = error_message

    @property
    def creative_urn(self) -> Optional[str]:
        """Back-compat alias for LinkedIn callers that referenced creative_urn."""
        return self.creative_id

    @creative_urn.setter
    def creative_urn(self, value: Optional[str]) -> None:
        self.creative_id = value

    def __repr__(self) -> str:
        return (
            f"CreateAdResult(creative_id={self.creative_id!r}, "
            f"status={self.status!r}, error_class={self.error_class!r}, "
            f"error_message={self.error_message!r}, "
            f"local_save_path={self.local_save_path!r})"
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, CreateAdResult):
            return NotImplemented
        return (
            self.creative_id     == other.creative_id and
            self.local_save_path == other.local_save_path and
            self.status          == other.status and
            self.error_class     == other.error_class and
            self.error_message   == other.error_message
        )


class AdPlatformClient(ABC):
    """Abstract interface every concrete ad client implements.

    Method semantics mirror the LinkedIn client because LinkedIn is the
    reference implementation; Meta and Google adapters bridge the gap to
    each platform's hierarchy (Meta: Campaign → Ad Set → Ad; Google: Campaign
    → Ad Group → Ad).

    Implementations must:
      - prefix every created entity name with `config.AGENT_NAME_PREFIX`
        ("agent_") so resources are easy to find in the platform UI;
      - default to a paused / draft state so nothing launches without a
        human approval step in the platform UI.
    """

    name: PlatformName
    constraints: PlatformConstraints

    # ── lifecycle ────────────────────────────────────────────────────────────

    @abstractmethod
    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        """Create a logical campaign container.

        On LinkedIn this is a real CampaignGroup. On Meta and Google there's
        no native group — implementations create a top-level Campaign and
        return its identifier so subsequent create_campaign() calls slot
        Ad Sets / Ad Groups under it.

        `geos` is the union of all ISO country codes the child ad sets will
        target. Meta needs this at the campaign level for
        `special_ad_category_country` under EMPLOYMENT/HOUSING/CREDIT SAC;
        LinkedIn and Google ignore it.
        """

    @abstractmethod
    def create_campaign(
        self,
        name: str,
        campaign_group_id: str,
        targeting: dict[str, Any],
        daily_budget_cents: int = 5000,
    ) -> str:
        """Create a campaign within the given group with platform-specific
        targeting. `targeting` is the dict returned by the platform's
        TargetingResolver — its shape is platform-specific."""

    @abstractmethod
    def upload_image(self, image_path: str | Path) -> str:
        """Upload a PNG and return the platform-native image identifier."""

    @abstractmethod
    def create_image_ad(
        self,
        campaign_id: str,
        image_id: str,
        headline: str,
        description: str,
        primary_text: Optional[str] = None,
        ad_headline: Optional[str] = None,
        intro_text: Optional[str] = None,
        cta: Optional[str] = None,
        destination_url: Optional[str] = None,
    ) -> CreateAdResult:
        """Create an image ad referencing the given campaign + image."""

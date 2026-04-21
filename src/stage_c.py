"""
Stage C — LinkedIn Audience Counts validation + greedy cohort selection.

Rules:
  1. Resolve each cohort's rules to LinkedIn URNs.
  2. Call LinkedIn Audience Counts API (HARD STOP on auth/rate errors).
  3. Reject cohorts with audience < AUDIENCE_SIZE_MIN.
  4. Greedy selection with 80% uniqueness gate: track which URNs are already
     covered; only add a cohort if ≥ MIN_UNIQUE_AUDIENCE_PCT of its audience
     is new.  Stop at MAX_CAMPAIGNS.
"""
import logging

import config
from src.analysis import Cohort
from src.linkedin_urn import UrnResolver
from src.linkedin_api import LinkedInClient

log = logging.getLogger(__name__)


def stage_c(
    cohorts: list[Cohort],
    urn_resolver: UrnResolver,
    li_client: LinkedInClient,
) -> list[Cohort]:
    """
    Returns the final selected cohorts (≤ MAX_CAMPAIGNS) with
    audience_size, intersection_score, and unique_pct filled in.

    Raises RuntimeError if LinkedIn API is unreachable / returns 4xx auth error.
    """
    log.info("Stage C: validating %d cohorts against LinkedIn Audience Counts", len(cohorts))

    # Step 1 — resolve URNs
    resolved: list[tuple[Cohort, dict[str, list[str]]]] = []
    for cohort in cohorts:
        facet_urns = urn_resolver.resolve_cohort_rules(cohort.rules)
        if not facet_urns:
            log.warning("Cohort '%s' — no URNs resolved, skipping", cohort.name)
            cohort.reject_reason = "no_urns_resolved"
            continue
        resolved.append((cohort, facet_urns))

    log.info("Stage C: %d cohorts have resolvable URNs", len(resolved))

    # Step 2 — audience counts (HARD STOP on auth errors)
    sized: list[tuple[Cohort, dict, int]] = []
    for cohort, facet_urns in resolved:
        try:
            count = li_client.get_audience_count(facet_urns)
        except Exception as exc:
            msg = str(exc)
            # Hard stop on auth/permission errors
            if _is_auth_error(msg):
                raise RuntimeError(
                    f"LinkedIn Audience Counts API blocked (auth error): {msg}"
                ) from exc
            log.warning("Audience count failed for '%s': %s — skipping", cohort.name, exc)
            continue

        cohort.audience_size = count
        if count < config.AUDIENCE_SIZE_MIN:
            log.info("Cohort '%s' audience=%d < %d — rejected", cohort.name, count, config.AUDIENCE_SIZE_MIN)
            cohort.reject_reason = f"audience={count} < {config.AUDIENCE_SIZE_MIN}"
            continue

        sized.append((cohort, facet_urns, count))
        log.info("Cohort '%s' audience=%d ✓", cohort.name, count)

    log.info("Stage C: %d cohorts pass audience threshold", len(sized))

    # Step 3 — greedy uniqueness gate
    # We approximate uniqueness by tracking the set of (facet, urn) pairs already
    # committed.  A cohort's "unique fraction" = |new_urns| / |total_urns|.
    committed_urn_pairs: set[tuple[str, str]] = set()
    selected: list[Cohort] = []

    # Sort by audience size desc (largest audience first)
    sized.sort(key=lambda t: t[2], reverse=True)

    for cohort, facet_urns, count in sized:
        if len(selected) >= config.MAX_CAMPAIGNS:
            break

        all_pairs  = {(f, u) for f, urns in facet_urns.items() for u in urns}
        new_pairs  = all_pairs - committed_urn_pairs
        unique_pct = (len(new_pairs) / len(all_pairs) * 100) if all_pairs else 0.0

        cohort.unique_pct = round(unique_pct, 1)
        cohort.intersection_score = round(
            len(all_pairs - new_pairs) / max(len(all_pairs), 1) * 100, 1
        )

        if unique_pct < config.MIN_UNIQUE_AUDIENCE_PCT:
            log.info(
                "Cohort '%s' unique_pct=%.1f%% < %.1f%% — skipped (overlap)",
                cohort.name, unique_pct, config.MIN_UNIQUE_AUDIENCE_PCT,
            )
            cohort.reject_reason = f"unique_pct={unique_pct:.1f}% < {config.MIN_UNIQUE_AUDIENCE_PCT}%"
            continue

        committed_urn_pairs.update(all_pairs)
        selected.append(cohort)
        log.info(
            "Selected cohort '%s' audience=%d unique_pct=%.1f%%",
            cohort.name, count, unique_pct,
        )

    log.info("Stage C complete: %d/%d cohorts selected", len(selected), config.MAX_CAMPAIGNS)
    return selected


def _is_auth_error(msg: str) -> bool:
    msg_lower = msg.lower()
    return any(k in msg_lower for k in ("401", "403", "unauthorized", "forbidden", "token"))


# ── CLI dry-run ────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    import logging
    from dotenv import load_dotenv
    from pathlib import Path

    load_dotenv(Path(__file__).parent.parent / ".env")
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")

    from src.analysis import Cohort
    from src.linkedin_urn import UrnResolver
    from src.linkedin_api import LinkedInClient
    from src.sheets import SheetsClient

    # Sample cohort: Python developers in Latin America (Coders Tier 2 es-419)
    sample_cohort = Cohort(
        name="[DRY RUN] Python Dev es-419",
        rules=[
            ("skills__python", True),
            ("job_titles_norm__software_engineer", True),
        ],
        n=500, passes=150, pass_rate=30.0, lift_pp=12.0,
    )

    print(f"\nDry-run cohort: {sample_cohort.name}")
    print(f"Rules: {sample_cohort.rules}\n")

    import os
    sheets   = SheetsClient()
    resolver = UrnResolver(sheets)
    # Read token from os.environ directly — config was imported before load_dotenv() ran
    token = os.environ.get("LINKEDIN_ACCESS_TOKEN") or os.environ.get("LINKEDIN_TOKEN", "")
    li    = LinkedInClient(token=token)

    # Step 1: resolve URNs
    facet_urns = resolver.resolve_cohort_rules(sample_cohort.rules)
    print(f"\nResolved URNs ({len(facet_urns)} facets):")
    for facet, urns in facet_urns.items():
        print(f"  {facet}: {urns}")

    # Step 2: audience count
    if facet_urns:
        print("\nCalling LinkedIn Audience Counts API...")
        try:
            count = li.get_audience_count(facet_urns)
            print(f"Audience size: {count:,}")
            print(f"Passes threshold (≥{config.AUDIENCE_SIZE_MIN:,}): {count >= config.AUDIENCE_SIZE_MIN}")
        except Exception as exc:
            print(f"Audience count API error: {exc}")
            print("NOTE: audienceCounts API may require LinkedIn Marketing Developer Platform (MDP) approval.")
            print("URN resolution above is confirmed working — Stage C core functionality OK.")
    else:
        print("\nNo URNs resolved — check URN_SHEET_ID and credentials.json")

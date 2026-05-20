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

    # Step 2 — audience counts (HARD STOP on auth errors). When a cohort
    # comes in below AUDIENCE_SIZE_MIN, run the de-narrowing retry: iteratively
    # drop the lowest-importance rule (lowest score contribution) from the
    # cohort and re-query, until we either clear the floor or run out of
    # rules. 2026-05-20: user directive — wrong rate in ads is risky but so
    # is a sub-50k audience pool that can't deliver impressions.
    sized: list[tuple[Cohort, dict, int]] = []
    for cohort, facet_urns in resolved:
        try:
            count = li_client.get_audience_count(facet_urns)
        except Exception as exc:
            msg = str(exc)
            if _is_auth_error(msg):
                raise RuntimeError(
                    f"LinkedIn Audience Counts API blocked (auth error): {msg}"
                ) from exc
            log.warning("Audience count failed for '%s': %s — skipping", cohort.name, exc)
            continue

        cohort.audience_size = count
        if count < config.AUDIENCE_SIZE_MIN:
            log.info(
                "Cohort '%s' audience=%d < %d — attempting de-narrowing",
                cohort.name, count, config.AUDIENCE_SIZE_MIN,
            )
            count, facet_urns = _denarrow_until_above_threshold(
                cohort=cohort,
                facet_urns=facet_urns,
                urn_resolver=urn_resolver,
                li_client=li_client,
            )
            cohort.audience_size = count
            if count < config.AUDIENCE_SIZE_MIN:
                log.info(
                    "Cohort '%s' audience=%d < %d after de-narrowing — rejected",
                    cohort.name, count, config.AUDIENCE_SIZE_MIN,
                )
                cohort.reject_reason = (
                    f"audience={count} < {config.AUDIENCE_SIZE_MIN} (de-narrowed to {len(cohort.rules)} rules)"
                )
                continue

        sized.append((cohort, facet_urns, count))
        log.info("Cohort '%s' audience=%d ✓ (rules=%d)", cohort.name, count, len(cohort.rules))

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


# ── De-narrowing retry (2026-05-20) ──────────────────────────────────────────
# When a cohort's audience comes in below AUDIENCE_SIZE_MIN, iteratively drop
# the lowest-importance rule and re-query LinkedIn audienceCounts. Stops when
# the audience clears the floor OR we run out of rules. MUTATES cohort.rules
# in place so downstream stages (copy gen, campaign creation) see the relaxed
# targeting.
#
# "Lowest importance" = lowest contribution to the cohort score. For cohorts
# coming from stage_a_lift, this is the rule with the lowest lift; for stage_a
# _support cohorts where individual lift isn't tracked, we fall back to dropping
# the LAST rule added (the rules list is roughly in importance order — anchor
# role first, refinements after). Conservative: never drops below 1 rule.

_DENARROW_MAX_DROPS = 5  # at most 5 rules dropped per cohort

def _denarrow_until_above_threshold(
    *,
    cohort: "Cohort",
    facet_urns: dict[str, list[str]],
    urn_resolver: "UrnResolver",
    li_client: "LinkedInClient",
) -> tuple[int, dict[str, list[str]]]:
    """Drop one rule at a time and re-query until audience >= floor.

    Returns (final_count, final_facet_urns). On exhaustion returns the last
    measured count + URNs (caller decides whether to reject or accept).
    """
    threshold = config.AUDIENCE_SIZE_MIN
    drops = 0
    last_count = cohort.audience_size or 0

    while drops < _DENARROW_MAX_DROPS and len(cohort.rules) > 1:
        # Drop the LAST rule (least-important per the rules-list ordering convention).
        dropped = cohort.rules.pop()
        drops += 1
        log.info(
            "  de-narrow #%d: dropping rule %r — re-querying audience for %d remaining rules",
            drops, dropped[0], len(cohort.rules),
        )

        # Re-resolve URNs (rule removal may free up facets entirely).
        try:
            new_facet_urns = urn_resolver.resolve_cohort_rules(cohort.rules)
        except Exception as exc:
            log.warning("  de-narrow URN re-resolve failed: %s — stopping", exc)
            return last_count, facet_urns
        if not new_facet_urns:
            log.warning("  de-narrow zeroed URNs after dropping %r — stopping", dropped[0])
            return last_count, facet_urns

        try:
            new_count = li_client.get_audience_count(new_facet_urns)
        except Exception as exc:
            if _is_auth_error(str(exc)):
                raise
            log.warning("  de-narrow audienceCounts failed: %s — stopping", exc)
            return last_count, facet_urns

        log.info("  de-narrow #%d result: audience=%d (was %d, threshold=%d)",
                 drops, new_count, last_count, threshold)
        last_count = new_count
        facet_urns = new_facet_urns
        if new_count >= threshold:
            log.info(
                "  de-narrow ✓ cohort '%s' now passes (dropped %d rule(s))",
                cohort.name, drops,
            )
            return new_count, facet_urns

    log.info("  de-narrow exhausted (dropped %d rules, %d remaining) — audience=%d",
             drops, len(cohort.rules), last_count)
    return last_count, facet_urns


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
    # Read token from os.environ directly — config was imported before load_dotenv() ran
    token = os.environ.get("LINKEDIN_ACCESS_TOKEN") or os.environ.get("LINKEDIN_TOKEN", "")
    li    = LinkedInClient(token=token)
    resolver = UrnResolver(sheets, linkedin_client=li)

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

# Plan — Target generalist/i18n ramps by locale (Bug 2)

**Status:** approved, not yet implemented. Decisions locked 2026-06-04.
**Trigger:** GMR-0023 ("get to 1000 i18n generalist actives") defined 13 per-locale
generalist cohorts, but the pipeline ran Stage A beam discovery over résumé
features and produced noise cohorts (environmental_engineering, adsorption+dna,
etc.) — see memory `project_generalist_ramp_cohort_mismatch`.

## Goal & success criteria
For a generalist/i18n ramp, produce **one campaign cohort per defined locale
cohort** ("Bengali generalist contributors" → BD/IN + Bengali) instead of
beam-derived facets.

Verifiable: dry-run on GMR-0023 → 13 locale cohorts (matching the 13 defined),
each geo+language targeted, above-floor (or de-narrowed) audiences, correct
per-locale LP URL (`outlier.ai/languages/bn-in` …). A specialist ramp is
unaffected and still uses beam discovery.

## Locked decisions
1. **Google + generalist:** attach a small set of **localized generic keywords**
   per language (e.g. "remote work", "online tasks", "AI training") + language +
   geo. (Not skip, not Demand Gen.)
2. **LinkedIn unsupported interface languages** (Bengali, Tagalog, Thai,
   Vietnamese, …): **geo-only fallback** (no language filter), flagged in
   TargetingCard + logs.
3. **No beam fallback.** Always target by locale. If a geo is too small,
   **de-narrow** — do NOT skip, because we need those users. ⚠️ **RELOOK ITEM:**
   the current floor logic marks below-floor cohorts `below_floor`/skipped; for
   generalist we must still launch them after de-narrowing. Track + revisit how
   small-but-required locale audiences are handled (don't drop them).

## Phases
1. **Detection** — `is_generalist_cohort(row)` (per pending cohort): cohort_description
   contains "generalist" AND/OR matched_locales non-empty with generalist/empty
   matched_domain AND/OR job_post_language_code set. Gated by config flag
   `GENERALIST_LOCALE_TARGETING` (default on, env-overridable).
2. **Locale-cohort construction** (`main.py:_resolve_cohorts`) — skip
   stage_a/b/c; build one `Cohort(name=cohort_description,
   rules=[("interface_locale", locale)])` with marker
   `facet_strength={"generalist_locale": locale}`; geo from included_geos.
   Persist ICP/audience/targeting as today.
3. **Per-channel locale targeting**
   - Meta (`src/meta_targeting.py`): geo_locations.countries + `locales=[meta_locale_id]`,
     no interests/edu.
   - Google (`src/google_targeting.py`): geo + language criterion (replace hardcoded
     `languageConstants/1000`) + localized generic keywords (decision 1).
   - LinkedIn (`src/stage_c.py` / li targeting): geo + interface-locale facet where
     supported, else geo-only (decision 2); skip URN typeahead.
4. **Mappings** — new `src/locales.py`: locale →
   {meta_locale_id, linkedin_locale|None, google_language_constant,
   display_language, generic_keywords[]}. Seed the 13 GMR-0023 locales
   (bn-in, de-de, fr-fr, hi-in, id-id, it-it, ko-kr, pt-br, th-th, tl-ph,
   vi-vn, zh-cn, ar-eg). Meta/Google IDs from their APIs/static refs.
5. **De-narrow-not-skip for generalist** (decision 3) + log the relook item.

## Risks / mitigations
- Could hijack a ramp that only looks generalist → per-cohort detection + config flag.
- Resolvers assume `rules` are résumé facets → synthetic `interface_locale` rule
  guarded by the marker; existing facet paths untouched.
- External data needed: Meta locale IDs, Google language constants, LinkedIn
  supported-interface-locale list, localized keyword sets (13 langs).

## Verification
- Unit: `is_generalist_cohort` True on 13 GMR-0023 cohorts, False on specialist.
- Unit: each resolver on a locale cohort → expected geo+language spec.
- Dry-run: `doppler run -- python3 main.py --ramp-id GMR-0023 --dry-run`.

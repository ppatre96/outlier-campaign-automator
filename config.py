"""
Central configuration for the Outlier Campaign Agent.
All runtime secrets come from environment variables or the Config tab in Google Sheets.
"""
import os
from datetime import datetime

# ── Google Sheets ─────────────────────────────────────────────────────────────
TRIGGERS_SHEET_ID = "1yM2bA_gbdki-IKSf14ddyshsKYh2FATEiv1CtdfYDQY"
URN_SHEET_ID      = "10S5QhB46l-f_ncR7fEGnkT9E2QIAs07RBuoLnahJhW0"
TRIGGERS_TAB      = "Triggers 2"
CREATIVES_TAB     = "Creatives"
CONFIG_TAB        = "Config"
REGISTRY_TAB      = "Campaign Registry"
GOOGLE_CREDENTIALS = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")

# ── Redash ────────────────────────────────────────────────────────────────────
REDASH_URL             = os.getenv("REDASH_URL", "https://redash.scale.com")
REDASH_API_KEY         = os.getenv("REDASH_API_KEY", "")
REDASH_DATA_SOURCE_ID  = int(os.getenv("REDASH_DATA_SOURCE_ID", "30"))  # 30 = _Snowflake (GenAI Ops)

# ── Snowflake (kept for reference; replaced by Redash in pipeline) ────────────
SNOWFLAKE_ACCOUNT   = os.getenv("SNOWFLAKE_ACCOUNT", "")
SNOWFLAKE_USER      = os.getenv("SNOWFLAKE_USER", "")
SNOWFLAKE_PASSWORD  = os.getenv("SNOWFLAKE_PASSWORD", "")
SNOWFLAKE_WAREHOUSE = os.getenv("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH")
SNOWFLAKE_DATABASE  = os.getenv("SNOWFLAKE_DATABASE", "OUTLIER_PROD")
SNOWFLAKE_SCHEMA    = os.getenv("SNOWFLAKE_SCHEMA", "PUBLIC")
SNOWFLAKE_ROLE      = os.getenv("SNOWFLAKE_ROLE", "")

# Date range for screening data (override with env vars or pass at runtime)
SCREENING_START_DATE = os.getenv("SCREENING_START_DATE", "2024-01-01")
SCREENING_END_DATE   = os.getenv("SCREENING_END_DATE",   datetime.utcnow().date().isoformat())

# ── LinkedIn ──────────────────────────────────────────────────────────────────
LINKEDIN_API_BASE      = "https://api.linkedin.com/rest"
LINKEDIN_VERSION       = "202510"
LINKEDIN_AD_ACCOUNT_ID = os.getenv("LINKEDIN_AD_ACCOUNT_ID", "510956407")
LINKEDIN_ORG_ID           = os.getenv("LINKEDIN_ORG_ID", "")
# NB (2026-05-18): the previous default `app.outlier.ai/en/contributors/projects`
# returns HTTP 404 → Google policy reviews flag every ad with
# `DESTINATION_NOT_WORKING` when this fallback is used. `outlier.ai/` returns
# HTTP 200 and works for every channel (LinkedIn doesn't crawl-validate, Meta
# requires public, Google requires public). LP_URL_BY_DOMAIN should still take
# precedence per cohort — this is only the catch-all when matched_domain isn't
# in the map.
LINKEDIN_DESTINATION      = os.getenv("LINKEDIN_DESTINATION_URL", "https://outlier.ai/")
LINKEDIN_INMAIL_SENDER_URN = os.getenv("LINKEDIN_INMAIL_SENDER_URN", "")
# Public profile URN of the LinkedIn member who authorized the OAuth token.
# Required to create image ad creatives (DSC posts via w_member_social scope).
# Find at: linkedin.com/in/<id> → the <id> portion, e.g. urn:li:person:AbCdEfGhIj
LINKEDIN_MEMBER_URN        = os.getenv("LINKEDIN_MEMBER_URN", "")
# Token — check LINKEDIN_ACCESS_TOKEN first, fall back to LINKEDIN_TOKEN
LINKEDIN_TOKEN         = (
    os.getenv("LINKEDIN_ACCESS_TOKEN") or
    os.getenv("LINKEDIN_TOKEN", "")
)
LINKEDIN_REFRESH_TOKEN = os.getenv("LINKEDIN_REFRESH_TOKEN", "")
# OAuth app credentials — needed to exchange refresh token for a new access token
LINKEDIN_CLIENT_ID     = os.getenv("LINKEDIN_CLIENT_ID", "")
LINKEDIN_CLIENT_SECRET = os.getenv("LINKEDIN_CLIENT_SECRET", "")

# LinkedIn conversion to attach to every WEBSITE_CONVERSION campaign created
# by the pipeline. Default is "Marketing Conversion - OCP Complete" (id 19801700,
# type COMPLETE_SIGNUP, enabled) — Outlier's standard signup conversion.
# To use "Successful Application" instead set LINKEDIN_CONVERSION_ID=19259804.
# Set to "0" to disable auto-attach.
LINKEDIN_CONVERSION_ID = int(os.getenv("LINKEDIN_CONVERSION_ID", "19801700"))

# Landing-page URL per Smart Ramp `matched_domain`. The marketing team
# maintains custom LPs under outlier.ai/experts/<slug>; this map matches the
# cohort's domain to its LP so the destination URL on every ad lands on the
# right page. Smart Ramp's `campaign_state.utm_<channel>.base_url` takes
# precedence when filled in.
#
# Values can be:
#   - A SLUG (preferred) — looked up against the LP_URL_SHEET_ID inventory
#     via `SheetsClient.read_lp_url_map()` to resolve the live Full URL.
#     Marketing edits the URL once in the sheet; pipeline picks it up next
#     run. The slug may include or omit a leading "/" — both work.
#   - A full URL (back-compat) — used as-is, no sheet lookup.
#
# Override via env LP_URL_BY_DOMAIN_JSON (JSON string) to extend without
# code changes — useful for new ramps with custom LPs the team hasn't yet
# captured in Smart Ramp.
import json as _json
try:
    _lp_env = os.getenv("LP_URL_BY_DOMAIN_JSON", "")
    LP_URL_BY_DOMAIN = _json.loads(_lp_env) if _lp_env else {}
except Exception:
    LP_URL_BY_DOMAIN = {}
LP_URL_BY_DOMAIN = {
    # GMR-0020 (provided 2026-05-13 by Pranav) — slugs now, URL is sheet-resolved
    "Finance & Quantitative Analysis": "qfinance",
    "Machine Learning":                 "ml",
    "Computer Science & AI":            "cs",
    # Earlier-session references (kept for back-compat; verify when running these ramps):
    "Clinical Medicine":                "cardiology-ctrl",
    "Law":                              "law-sgp",
    **LP_URL_BY_DOMAIN,  # env override last so it wins
}

# Marketing-team-maintained LP URL inventory. One sheet, multiple tabs by
# vertical (Experts / Coding / Math / Languages / Speech / HLE / ...). Each
# row has Page Name + Slug + Full URL + Status. SheetsClient.read_lp_url_map()
# flattens to {slug: full_url} for every Published row across all tabs.
# Override via env LP_URL_SHEET_ID for staging/test sheets.
LP_URL_SHEET_ID = os.getenv(
    "LP_URL_SHEET_ID",
    "1AmM78xzVDf7UWV9icxODw6vxY12nSNZ6121Gcf0ibx4",
)

# Default exclusion targeting applied to EVERY LinkedIn campaign the pipeline
# creates. Sourced from the canonical "exclusions reference" campaign curated
# by the marketing team (https://www.linkedin.com/campaignmanager/accounts/
# 510956407/campaigns/368397536/details — exclude block as of 2026-05-13).
#
# Layered on top of family / data-driven / cohort-specific excludes (see
# main.py:_process_static_campaigns + _process_inmail_campaigns).
#
# Direct URN injection (no fuzzy resolution): these facets have no
# human-readable label space in the URN sheet, so we hard-code the URNs.
# Maintainers: refresh by re-running scripts/inspect_li_exclusions.py against
# campaign 368397536 when the reference campaign's exclude block changes.
DEFAULT_EXCLUDE_URNS_RAW: dict[str, list[str]] = {
    # Geo: California + NYC Metro (Outlier doesn't recruit from these markets
    # for ad-driven acquisition per the reference campaign's policy).
    "profileLocations": [
        "urn:li:geo:102095887",  # California, United States
        "urn:li:geo:90000070",   # New York City Metropolitan Area
    ],
    # Employer suppression — competitors + Scale itself (no internal recruiting
    # via the public-acquisition channel).
    "employers": [
        "urn:li:organization:10667",     # Meta
        "urn:li:organization:11130470",  # OpenAI
        "urn:li:organization:1441",      # Google
        "urn:li:organization:1594050",   # Google DeepMind
        "urn:li:organization:17998520",  # Scale AI
        "urn:li:organization:74691296",  # absolute labs
        "urn:li:organization:96151950",  # xAI
    ],
    # Historical contributor matched-audience suppression — past actives across
    # Coders / Languages / Generalists / Specialists tiers (2024-10 → 2025-Q1).
    "audienceMatchingSegments": [
        "urn:li:adSegment:24551436",  # FY24 Q1 Exclusion — Customer & Partner
        "urn:li:adSegment:30866616",
        "urn:li:adSegment:30866766",
        "urn:li:adSegment:30867466",
        "urn:li:adSegment:30867496",
        "urn:li:adSegment:30867606",
        "urn:li:adSegment:30867656",
        "urn:li:adSegment:30867776",
        "urn:li:adSegment:30867826",
        "urn:li:adSegment:30867866",
        "urn:li:adSegment:30867976",
        "urn:li:adSegment:30867986",
        "urn:li:adSegment:30868006",
        "urn:li:adSegment:32217856",
        "urn:li:adSegment:32218136",
        "urn:li:adSegment:32218226",
        "urn:li:adSegment:32433386",
        "urn:li:adSegment:32433426",
    ],
    # Recent-signup dynamic suppression (built-in LinkedIn audience).
    "dynamicSegments": [
        "urn:li:adSegment:61522684",  # 120 Days SignUps
    ],
}

# ── Multi-platform expansion (Meta + Google Ads) ──────────────────────────────
# Comma-separated list controlling which ad platforms the pipeline targets per
# Smart Ramp run. Order is preserved (LinkedIn first by default for back-compat).
ENABLED_PLATFORMS    = os.getenv("ENABLED_PLATFORMS", "linkedin,meta,google")

# Meta and Google both gate certain campaigns under a "special ad category" for
# regulated verticals. Outlier tasks are 1099 contractor work which Meta/Google
# may classify as EMPLOYMENT — the safe default is to flag this and absorb the
# targeting restrictions (no narrow age/gender). Set to "NONE" to disable.
SPECIAL_AD_CATEGORY  = os.getenv("SPECIAL_AD_CATEGORY", "EMPLOYMENT")

# Common name prefix applied to every campaign / group / creative the agent
# creates so they're easy to filter in each platform's UI. Mirrors the existing
# LinkedInClient.AGENT_NAME_PREFIX rule.
AGENT_NAME_PREFIX    = os.getenv("AGENT_NAME_PREFIX", "agent_")

# ── outlier-campaign-console approval gate (UI) ────────────────────────────────
# When true, the poller writes each new ramp to Vercel Postgres at
# `awaiting_approval` after prep completes and skips `_launch_ramp` until
# Diego/Bryan click Approve in the console (which flips the row to
# 'approved'/'yolo'). Default false → legacy behavior preserved; enable per
# environment only after the console is deployed + verified. See
# `src/ui_decisions.py` for the Postgres wrapper and
# `scripts/sql/001_ramp_decisions.sql` for the schema.
UI_GATE_ENABLED      = os.getenv("UI_GATE_ENABLED", "false").lower() in ("1", "true", "yes")
# Public URL of the outlier-campaign-console. Used in Slack notifications so
# Diego/Bryan can deep-link to the ramp detail page (Run prep / Review briefs
# / Launch CTAs). Override via OUTLIER_CONSOLE_URL when the domain alias
# changes. Trailing slash is stripped at use time.
OUTLIER_CONSOLE_URL  = os.getenv("OUTLIER_CONSOLE_URL", "https://project-4ec1m.vercel.app").rstrip("/")
# Vercel Postgres connection string. Doppler-injected in dev + prd. Used by
# both the pipeline (src/ui_decisions.py) and any local script that reads
# decision rows. Empty string → UIDecisionsUnavailable on first DB call.
DATABASE_URL         = os.getenv("DATABASE_URL", "")

# ── Meta Ads ──────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN    = os.getenv("META_ACCESS_TOKEN", "")
META_APP_ID          = os.getenv("META_APP_ID", "")
META_APP_SECRET      = os.getenv("META_APP_SECRET", "")
META_AD_ACCOUNT_ID   = os.getenv("META_AD_ACCOUNT_ID", "")  # "act_<numeric>"
META_API_VERSION     = os.getenv("META_API_VERSION", "v21.0")
# Outlier Facebook Page ID (provided 2026-05-26 by Tuan). Image ads need
# object_story_spec.page_id — without it create_image_ad falls back to
# "local_fallback" and the PNG is saved locally for manual upload.
META_PAGE_ID         = os.getenv("META_PAGE_ID", "260786120451494")

# Meta Pixel + Custom Conversion (provided 2026-05-26 by Tuan):
#   Pixel ID:             637714478283926
#   Custom Conversion ID: 986478843749388  (event: worker_skill_all)
#   Conversion window:    7-day click only
# When META_CUSTOM_CONVERSION_ID is set, the Meta arm switches the ad set
# optimization_goal from LINK_CLICKS → OFFSITE_CONVERSIONS, attaches
# promoted_object.custom_conversion_id, and sets attribution_spec to N-day
# click-through. Leave empty to fall back to LINK_CLICKS (back-compat).
META_PIXEL_ID                = os.getenv("META_PIXEL_ID", "637714478283926")
META_CUSTOM_CONVERSION_ID    = os.getenv("META_CUSTOM_CONVERSION_ID", "986478843749388")
META_ATTRIBUTION_WINDOW_DAYS = int(os.getenv("META_ATTRIBUTION_WINDOW_DAYS", "7"))

# Custom audiences to exclude on every prospecting ad set (provided 2026-05-26
# by Tuan — the four active-contributor audiences from Outlier's Meta account).
# Override via META_EXCLUDE_AUDIENCE_IDS_JSON to replace the list.
DEFAULT_META_EXCLUDE_AUDIENCE_IDS: list[str] = [
    "120211889244260257",  # Generalists Actives - 12.12.24 (~208k)
    "120211112196180257",  # Coders Actives - 12.12.24      (~118k)
    "120211112198140257",  # Languages Actives - 12.12.24   (~502k)
    "120211112208060257",  # Specialists Actives - 12.12.24 (~240k)
]
try:
    _meta_excl_env = os.getenv("META_EXCLUDE_AUDIENCE_IDS_JSON", "")
    META_EXCLUDE_AUDIENCE_IDS = (
        _json.loads(_meta_excl_env) if _meta_excl_env else DEFAULT_META_EXCLUDE_AUDIENCE_IDS
    )
except Exception:
    META_EXCLUDE_AUDIENCE_IDS = DEFAULT_META_EXCLUDE_AUDIENCE_IDS

# Frequency cap on every prospecting ad set (provided 2026-05-26 by Tuan):
# 3 impressions / 7 days. Set MAX_FREQ to 0 to disable.
META_FREQUENCY_CAP_IMPRESSIONS   = int(os.getenv("META_FREQUENCY_CAP_IMPRESSIONS", "3"))
META_FREQUENCY_CAP_INTERVAL_DAYS = int(os.getenv("META_FREQUENCY_CAP_INTERVAL_DAYS", "7"))

# Meta LAL feature flags (2026-05-27). Toggles the lookalike-audience layer
# in src/meta_lal.py. When META_USE_LAL is True, src/meta_targeting.py
# resolve_cohort() will auto-attach 1% LAL audiences seeded from the 4
# Actives audiences in META_LAL_SEED_AUDIENCES — one LAL per (seed × country)
# tuple. Default seeds = the same IDs as META_EXCLUDE_AUDIENCE_IDS (so we
# target lookalikes of active contributors while excluding the contributors
# themselves; two disjoint sets, same source pool).
META_USE_LAL              = os.getenv("META_USE_LAL", "false").lower() in ("1", "true", "yes")
META_LAL_RATIO            = float(os.getenv("META_LAL_RATIO", "0.01"))   # 1%
try:
    _meta_lal_seeds_env = os.getenv("META_LAL_SEED_AUDIENCES_JSON", "")
    META_LAL_SEED_AUDIENCES = (
        _json.loads(_meta_lal_seeds_env) if _meta_lal_seeds_env else DEFAULT_META_EXCLUDE_AUDIENCE_IDS
    )
except Exception:
    META_LAL_SEED_AUDIENCES = DEFAULT_META_EXCLUDE_AUDIENCE_IDS

# Google Custom Intent feature flag (2026-05-27). Toggles
# src/google_custom_intent.py — per-cohort keyword-seeded audience layer
# attached as ad-group criterion. PII-free path (no Customer Match upload).
# Pairs with the existing audience_segments + keyword_ideas paths.
GOOGLE_USE_CUSTOM_INTENT  = os.getenv("GOOGLE_USE_CUSTOM_INTENT", "false").lower() in ("1", "true", "yes")

# Dual ad-set strategy (decision 2026-05-26). When LAL Custom Audiences ship
# (feature #5: Snowflake seed → SHA256 → Meta upload → 1% LAL), the Meta arm
# creates TWO ad sets per (cohort × geo cluster × angle):
#   1. LAL primary  — `custom_audiences = [{id: <1% LAL audience>}]`,
#                     ~LAL_BUDGET_SPLIT_PCT of the cohort's daily budget.
#   2. Broad control — no `custom_audiences`, geo + education + exclusions only,
#                      remainder of the budget.
# This sidesteps "LAL plateau" and lets Meta's algo discover converters who
# don't look like our existing pool. Until LAL ships, all ad sets are
# functionally broad (no inclusion audience) — flipping BROAD_CONTROL_ENABLED
# off today is a no-op. Set False after feature #5 ships only if Diego/Tuan
# decide pure-LAL outperforms the control over the 14-day window.
META_BROAD_CONTROL_ENABLED   = os.getenv("META_BROAD_CONTROL_ENABLED", "true").lower() in ("1", "true", "yes")
META_LAL_BUDGET_SPLIT_PCT    = int(os.getenv("META_LAL_BUDGET_SPLIT_PCT", "70"))  # LAL gets 70%, broad gets 30%

# ── Google Ads ────────────────────────────────────────────────────────────────
GOOGLE_ADS_CLIENT_ID         = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
GOOGLE_ADS_CLIENT_SECRET     = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
GOOGLE_ADS_DEVELOPER_TOKEN   = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_CUSTOMER_ID       = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
GOOGLE_ADS_REFRESH_TOKEN     = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")

# Google Ads value-based conversion tracking (Tuan, 2026-05-27).
# Conversion Action ID `7625599821` (`worker_skill_all`) on leaf account
# 8840244968, type UPLOAD_CLICKS / category SIGNUP, default value $74.29.
# Per-tier values ($50/$75/$100 for T1/T2/T3) injected upstream by
# worker_skill_tier_value_inject (ID ifnd_6a161027c1c2ad7aff752e7a) — see
# reference_outlier_value_based_conversions memory file. Setting this attaches
# the action to every new campaign via selective_optimization, constraining
# Google's optimizer to this specific conversion (not all account conversions).
GOOGLE_CONVERSION_ACTION_ID  = os.getenv("GOOGLE_CONVERSION_ACTION_ID", "7625599821")

# Google bid strategy. Options (read at campaign-create time):
#   - MAXIMIZE_CONVERSIONS       — count-based (Diego default 2026-05-22)
#   - MAXIMIZE_CONVERSION_VALUE  — value-based (new 2026-05-27 default after
#                                  Tuan shipped value injection across all
#                                  channels). 7-14 day learning phase expected.
#   - MANUAL_CPC                 — legacy fallback; only for debug
# Override via Doppler `GOOGLE_BID_STRATEGY` env var. Existing live campaigns
# do NOT auto-migrate when the default changes — they retain their original
# strategy until edited via the Ads UI.
GOOGLE_BID_STRATEGY          = os.getenv("GOOGLE_BID_STRATEGY", "MAXIMIZE_CONVERSION_VALUE")

# ── LiteLLM proxy (kept for image-gen fallback via /images/generations) ───────
# Public endpoint (no VPN required). Internal: litellm-proxy.ml-serving-internal.scale.com/v1
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "https://litellm-proxy.ml.scale.com/v1")
LITELLM_API_KEY  = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL    = os.getenv("LITELLM_MODEL", "anthropic/claude-sonnet-4-6")

# ── Direct Anthropic SDK ───────────────────────────────────────────────────────
# Used for all Claude copy-gen calls (ICP extraction, copy variants, InMail, rewriter).
# Falls back to LITELLM_API_KEY so the same key value works for both paths.
ANTHROPIC_API_KEY = os.getenv("ANTHROPIC_API_KEY", os.getenv("LITELLM_API_KEY", ""))
# Model ID in Anthropic format (no "anthropic/" prefix — that's LiteLLM's routing prefix)
ANTHROPIC_MODEL   = os.getenv("ANTHROPIC_MODEL", "claude-sonnet-4-6")

# ── Figma ─────────────────────────────────────────────────────────────────────
FIGMA_TOKEN = os.getenv("FIGMA_TOKEN", "")

# ── Analysis thresholds ───────────────────────────────────────────────────────
MIN_SAMPLE_INTERNAL         = int(os.getenv("MIN_SAMPLE_INTERNAL", 30))
MIN_ABSOLUTE_PASSES         = int(os.getenv("MIN_ABSOLUTE_PASSES", 10))
MIN_LIFT_PP                 = float(os.getenv("MIN_LIFT_PP", 3.0))
MIN_PASS_RATE_FLOOR         = float(os.getenv("MIN_PASS_RATE_FLOOR", 5.0))
COUNTRY_VALIDATION_THRESHOLD = int(os.getenv("COUNTRY_VALIDATION_THRESHOLD", 75))
MAX_INCLUDE_FACETS          = int(os.getenv("MAX_INCLUDE_FACETS", 4))
MAX_CAMPAIGNS               = int(os.getenv("MAX_CAMPAIGNS", 5))
# Experimentation caps — Pranav rule 2026-05-05:
# Max 3 cohorts per geo cluster, each with 3 angle variants (A/B/C) for testing.
# Feedback agent surfaces winners/losers; losers get deprecated and replaced.
MAX_COHORTS_PER_GEO_CLUSTER = int(os.getenv("MAX_COHORTS_PER_GEO_CLUSTER", 3))
ANGLES_PER_COHORT           = int(os.getenv("ANGLES_PER_COHORT", 3))
# Cap on natural geo clusters formed by group_geos_for_campaigns. Without this,
# a ramp with many countries (GMR-0021 had 211 geos → 12 clusters) spawns
# MAX_COHORTS × ANGLES × N_clusters campaigns per channel — 108+ for that ramp's
# Meta arm. Default 3 enforces the experimentation cap (3 cohorts × 3 angles
# × 3 geo clusters = 27 campaigns per channel, matching MAX_COHORTS/ANGLES).
# Was an opt-in env-only flag pre-2026-05-22; surfacing the default here so
# new ramps automatically inherit the cap. Set to 0 to disable the cap.
MAX_GEO_CLUSTERS            = int(os.getenv("MAX_GEO_CLUSTERS", 3))
# Brief-review gate (2026-05-22). After prep writes the cohort_briefs rows
# and flips ramp_status='awaiting_brief_review', the reviewer (Diego/Bryan)
# has this many hours to add comments + confirm. If they don't, the poller's
# sweep auto-confirms (flips to 'awaiting_approval' with an audit event so
# the existing channels+budget gate takes over). Set to 0 to disable the
# auto-confirm (briefs would then block forever until manually confirmed).
BRIEF_REVIEW_AUTO_CONFIRM_HOURS = int(os.getenv("BRIEF_REVIEW_AUTO_CONFIRM_HOURS", 4))
# Phase 3.1 — image-gen concurrency. Each (cohort × geo × angle) Gemini call
# is independent; running them in a thread pool collapses ~27 sequential
# calls (~40 min worst case with QC reroll) to ~10 min at workers=4.
# Set to 1 to fall back to fully sequential behavior.
IMAGE_GEN_CONCURRENCY       = int(os.getenv("IMAGE_GEN_CONCURRENCY", 4))
# Phase 3.2 — copy-gen concurrency. build_copy_variants is one Anthropic
# call per (cohort × geo) ≈ 5 s; pooled across 9 combos this drops ~45 s
# sequential to ~12 s at workers=4. Defaults to IMAGE_GEN_CONCURRENCY so a
# single env flip controls both pools; can be tuned independently if
# Anthropic RPM/TPM limits surface separately from Gemini limits.
COPY_GEN_CONCURRENCY        = int(os.getenv("COPY_GEN_CONCURRENCY", IMAGE_GEN_CONCURRENCY))
# Phase 3.4 — ramp/cohort concurrency. Each iteration of the launch loop runs
# a full Stage 1+2+C+creative-gen+campaign-create pipeline for one cohort.
# Running multiple cohorts in parallel collapses the per-ramp wall-clock from
# the dominant cost (Snowflake fetch + LLM ICP + URN resolution + audience
# counts), each of which is IO-bound. Defaults to 1 (sequential) so a merge
# doesn't silently change behavior; bump in Doppler `prd` once verified.
# Practical cap is ~3 — beyond that LinkedIn/Snowflake rate limits dominate.
RAMP_CONCURRENCY            = int(os.getenv("RAMP_CONCURRENCY", 1))
AUDIENCE_SIZE_MIN           = int(os.getenv("AUDIENCE_SIZE_MIN", 50_000))
MIN_UNIQUE_AUDIENCE_PCT     = float(os.getenv("MIN_UNIQUE_AUDIENCE_PCT", 80.0))
URN_FUZZY_MATCH_THRESHOLD   = float(os.getenv("URN_FUZZY_MATCH_THRESHOLD", 0.85))

# Figma MCP server URL (figma-remote-mcp SSE endpoint)
MCP_FIGMA_URL = os.getenv("MCP_FIGMA_URL", "http://127.0.0.1:3845/sse")

# ── Google Drive ──────────────────────────────────────────────────────────────
# Set GDRIVE_ENABLED=true in .env once the target folder is a Shared Drive
# and the service account has been added as Content Manager.
# Until then creatives are saved locally only.
GDRIVE_ENABLED   = os.getenv("GDRIVE_ENABLED", "true").lower() == "true"
# Shared Drive ID (Google Workspace Team Drive — confirmed plugged in).
# The agent walks/creates this hierarchy under the drive root:
#   <ramp_id>/<channel>/<cohort_geo>/<angle>.png
GDRIVE_DRIVE_ID  = os.getenv("GDRIVE_DRIVE_ID", "0ALHAgK4RPbnfUk9PVA")
# Optional sub-folder root inside the Shared Drive — empty uses the drive root.
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "")

# ── Gemini (image generation) ─────────────────────────────────────────────────
# Routed through LiteLLM proxy via /images/generations endpoint.
# Confirmed working: gemini-2.5-flash-image, imagen-4.0-generate-001,
#   imagen-4.0-fast-generate-001, gemini-3.1-flash-image-preview
GEMINI_IMAGE_MODEL = os.getenv("GEMINI_IMAGE_MODEL", "gemini/gemini-2.5-flash-image")
# Legacy direct-API key — no longer needed, kept for fallback only
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── Midjourney (pending MCP — not yet active) ─────────────────────────────────
MIDJOURNEY_API_TOKEN = os.getenv("MIDJOURNEY_API_TOKEN", "")
MIDJOURNEY_MCP_URL   = os.getenv(
    "MIDJOURNEY_MCP_URL",
    "https://midjourney.mcp.acedata.cloud/mcp",
)

# ── Slack ─────────────────────────────────────────────────────────────────────
SLACK_BOT_TOKEN   = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_WEBHOOK_URL = os.getenv("SLACK_WEBHOOK_URL", "")
SLACK_REPORT_USER = "U095J930UEL"   # pranav.patre@scale.com

# ─────────────────────────────────────────────────────────────────────────────
# Phase 2.6 — Smart Ramp Auto-Trigger (poller + notifier)
# ─────────────────────────────────────────────────────────────────────────────
SMART_RAMP_POLL_INTERVAL_SECONDS = int(os.getenv("SMART_RAMP_POLL_INTERVAL_SECONDS", "900"))
SMART_RAMP_FAILURE_THRESHOLD     = int(os.getenv("SMART_RAMP_FAILURE_THRESHOLD", "5"))
SMART_RAMP_TEST_REQUESTER_PATTERN = os.getenv("SMART_RAMP_TEST_REQUESTER_PATTERN", r"\btest\b")

SLACK_DIEGO_USER_ID         = os.getenv("SLACK_DIEGO_USER_ID", "U08AW9FCP27")
# Tuan Hoang — Head of Growth Marketing (escalation contact / oversight).
SLACK_TUAN_USER_ID          = os.getenv("SLACK_TUAN_USER_ID", "U08PDA0T85U")
# Bryan Ponce — Google Ads operator counterpart (matches Diego on Meta).
# Not found via Slack user search 2026-05-13 — fill in via env when his
# Slack ID is known.
SLACK_BRYAN_USER_ID         = os.getenv("SLACK_BRYAN_USER_ID", "")
SLACK_RAMP_NOTIFY_CHANNEL   = os.getenv("SLACK_RAMP_NOTIFY_CHANNEL", "C0B0NBB986L")
# Weekly ads-audit destination. Defaults to the same channel as the ramp
# launch notifier (`#outlier-campaign-atomation-bot`, C0B0NBB986L); override
# in Doppler if you ever want audit posts in a dedicated channel.
SLACK_AUDIT_CHANNEL         = os.getenv("SLACK_AUDIT_CHANNEL", "C0B0NBB986L")
# Three notification targets per success/escalation message: Pranav DM + Diego DM + shared channel.
# Each tuple is (kind, id); kind ∈ {"user", "channel"}. Notifier iterates this list.
SLACK_RAMP_NOTIFY_TARGETS   = [
    ("user", SLACK_REPORT_USER),
    ("user", SLACK_DIEGO_USER_ID),
    ("channel", SLACK_RAMP_NOTIFY_CHANNEL),
]

# Channel-post mentions: Diego (Meta), Bryan (Google), Tuan (oversight).
# Empty string entries are skipped so a missing Bryan ID doesn't render a
# literal "<@>" placeholder in the message.
SLACK_CHANNEL_MENTION_IDS = [
    uid for uid in [SLACK_DIEGO_USER_ID, SLACK_BRYAN_USER_ID, SLACK_TUAN_USER_ID]
    if uid
]

# Slack reaction handler configuration (Phase 2.5 feedback loop)
SLACK_REACTION_BOT_USER_ID = os.getenv("SLACK_REACTION_BOT_USER_ID", "")  # Bot user ID for reaction events
# NOTE: SLACK_FEEDBACK_CHANNEL_ID is the direct message channel with the feedback alert bot.
# In Slack, DM channels have format "D..." (not "C...").
# Set SLACK_FEEDBACK_CHANNEL_ID="{DM_CHANNEL_ID}" in .env if monitoring DM reactions.
# If posting to a public/private channel instead, use "C..." format.
SLACK_FEEDBACK_CHANNEL_ID = os.getenv("SLACK_FEEDBACK_CHANNEL_ID", "")  # Channel where feedback alerts post

# Feedback thresholds (for feedback_agent.py)
CPA_BASELINE_PERCENTILE = 50  # Median CPA used as baseline for z-score calc
CPA_Z_SCORE_THRESHOLD = 2.0   # Number of std devs above baseline to flag underperformance
CTR_DECLINE_THRESHOLD = 0.10   # 10% week-over-week decline triggers underperformance flag

# FEED-16 (V2): flag a funnel stage as a "drop" when its rate is >= 30% below
# the cohort's stage-median rate. Configurable via .env.
FUNNEL_DROP_ALERT_THRESHOLD = float(os.getenv("FUNNEL_DROP_ALERT_THRESHOLD", "0.30"))

# Reaction handler configuration
REACTION_EMOJI_MAPPING = {
    "thumbsup": "PAUSE",        # 👍 = pause cohort
    "lab": "TEST_NEW_ANGLES",   # 🧪 = test new angles
}

# Campaign lifecycle monitor
CAMPAIGN_UNDERPERFORM_THRESHOLD = float(os.getenv("CAMPAIGN_UNDERPERFORM_THRESHOLD", "0.20"))

# Beam search
BEAM_CANDIDATES = 12   # generate this many before Stage C

# --- Phase 2.5 V2: Sentiment Miner (FEED-17, FEED-18, FEED-19) ---
SENTIMENT_THEME_MIN_EVIDENCE = int(os.getenv("SENTIMENT_THEME_MIN_EVIDENCE", "3"))
SENTIMENT_LOOKBACK_DAYS      = int(os.getenv("SENTIMENT_LOOKBACK_DAYS", "7"))
SENTIMENT_REDDIT_SUBS        = [s.strip() for s in os.getenv(
    "SENTIMENT_REDDIT_SUBS", "Outlier_AI,BeerMoney,WorkOnline"
).split(",") if s.strip()]

# Zendesk Search API (HTTP Basic {email}/token:{api_token}) — empty = skip source
ZENDESK_SUBDOMAIN   = os.getenv("ZENDESK_SUBDOMAIN", "")
ZENDESK_EMAIL       = os.getenv("ZENDESK_EMAIL", "")
ZENDESK_API_TOKEN   = os.getenv("ZENDESK_API_TOKEN", "")

# Intercom Conversations API (Bearer) — empty = skip source
INTERCOM_ACCESS_TOKEN = os.getenv("INTERCOM_ACCESS_TOKEN", "")

# --- Phase 2.5 V2: ICP Drift Monitor (FEED-20, FEED-21) ---
ICP_DRIFT_THRESHOLD = float(os.getenv("ICP_DRIFT_THRESHOLD", "0.15"))
# KL divergence threshold above which auto-reanalysis fires. Default 0.15.

ICP_DRIFT_MIN_ROWS  = int(os.getenv("ICP_DRIFT_MIN_ROWS", "200"))
# Noise floor — skip drift check if this week's Stage 1 output has fewer rows.

ICP_DRIFT_LOOKBACK_WEEKS = int(os.getenv("ICP_DRIFT_LOOKBACK_WEEKS", "4"))
# Trailing-N-week median used as the drift baseline.

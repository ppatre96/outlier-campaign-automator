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
LINKEDIN_DESTINATION      = os.getenv("LINKEDIN_DESTINATION_URL", "https://app.outlier.ai/en/contributors/projects")
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

# ── Meta Ads ──────────────────────────────────────────────────────────────────
META_ACCESS_TOKEN    = os.getenv("META_ACCESS_TOKEN", "")
META_APP_ID          = os.getenv("META_APP_ID", "")
META_APP_SECRET      = os.getenv("META_APP_SECRET", "")
META_AD_ACCOUNT_ID   = os.getenv("META_AD_ACCOUNT_ID", "")  # "act_<numeric>"
META_API_VERSION     = os.getenv("META_API_VERSION", "v21.0")
# Image ads need an object_story_spec.page_id — the Outlier Facebook Page ID.
# Empty string disables Meta image-ad creation (campaigns + ad sets still get
# logged, ad creation falls back to "local_fallback" status).
META_PAGE_ID         = os.getenv("META_PAGE_ID", "")

# ── Google Ads ────────────────────────────────────────────────────────────────
GOOGLE_ADS_CLIENT_ID         = os.getenv("GOOGLE_ADS_CLIENT_ID", "")
GOOGLE_ADS_CLIENT_SECRET     = os.getenv("GOOGLE_ADS_CLIENT_SECRET", "")
GOOGLE_ADS_DEVELOPER_TOKEN   = os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN", "")
GOOGLE_ADS_CUSTOMER_ID       = os.getenv("GOOGLE_ADS_CUSTOMER_ID", "")
GOOGLE_ADS_LOGIN_CUSTOMER_ID = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "")
GOOGLE_ADS_REFRESH_TOKEN     = os.getenv("GOOGLE_ADS_REFRESH_TOKEN", "")

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
AUDIENCE_SIZE_MIN           = int(os.getenv("AUDIENCE_SIZE_MIN", 50_000))
MIN_UNIQUE_AUDIENCE_PCT     = float(os.getenv("MIN_UNIQUE_AUDIENCE_PCT", 80.0))
URN_FUZZY_MATCH_THRESHOLD   = float(os.getenv("URN_FUZZY_MATCH_THRESHOLD", 0.85))

# Figma MCP server URL (figma-remote-mcp SSE endpoint)
MCP_FIGMA_URL = os.getenv("MCP_FIGMA_URL", "http://127.0.0.1:3845/sse")

# ── Google Drive ──────────────────────────────────────────────────────────────
# Set GDRIVE_ENABLED=true in .env once the target folder is a Shared Drive
# and the service account has been added as Content Manager.
# Until then creatives are saved locally only.
GDRIVE_ENABLED   = os.getenv("GDRIVE_ENABLED", "false").lower() == "true"
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "0ALHAgK4RPbnfUk9PVA")

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
SLACK_RAMP_NOTIFY_CHANNEL   = os.getenv("SLACK_RAMP_NOTIFY_CHANNEL", "C0B0NBB986L")
# Three notification targets per success/escalation message: Pranav DM + Diego DM + shared channel.
# Each tuple is (kind, id); kind ∈ {"user", "channel"}. Notifier iterates this list.
SLACK_RAMP_NOTIFY_TARGETS   = [
    ("user", SLACK_REPORT_USER),
    ("user", SLACK_DIEGO_USER_ID),
    ("channel", SLACK_RAMP_NOTIFY_CHANNEL),
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

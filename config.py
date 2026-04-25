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

# ── LiteLLM proxy ─────────────────────────────────────────────────────────────
LITELLM_BASE_URL = os.getenv("LITELLM_BASE_URL", "https://litellm-proxy.ml-serving-internal.scale.com/v1")
LITELLM_API_KEY  = os.getenv("LITELLM_API_KEY", "")
LITELLM_MODEL    = os.getenv("LITELLM_MODEL", "anthropic/claude-sonnet-4-6")

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
GDRIVE_FOLDER_ID = os.getenv("GDRIVE_FOLDER_ID", "1TrpyIOq6hS4eGAc0sYUIJom4MAanbnm4")

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

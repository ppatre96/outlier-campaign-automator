"""
Central configuration for the Outlier Campaign Agent.
All runtime secrets come from environment variables or the Config tab in Google Sheets.
"""
import os

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
SCREENING_END_DATE   = os.getenv("SCREENING_END_DATE",   "2025-12-31")

# ── LinkedIn ──────────────────────────────────────────────────────────────────
LINKEDIN_API_BASE      = "https://api.linkedin.com/rest"
LINKEDIN_VERSION       = "202510"
LINKEDIN_AD_ACCOUNT_ID = os.getenv("LINKEDIN_AD_ACCOUNT_ID", "510956407")
LINKEDIN_ORG_ID        = os.getenv("LINKEDIN_ORG_ID", "")        # for image upload owner
LINKEDIN_DESTINATION   = os.getenv("LINKEDIN_DESTINATION_URL", "https://app.outlier.ai/en/contributors/projects")

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

# ── Gemini (image generation) ─────────────────────────────────────────────────
GEMINI_API_KEY = os.getenv("GEMINI_API_KEY", "")

# ── Midjourney (pending MCP — not yet active) ─────────────────────────────────
MIDJOURNEY_API_TOKEN = os.getenv("MIDJOURNEY_API_TOKEN", "")
MIDJOURNEY_MCP_URL   = os.getenv(
    "MIDJOURNEY_MCP_URL",
    "https://midjourney.mcp.acedata.cloud/mcp",
)

# Campaign lifecycle monitor
CAMPAIGN_UNDERPERFORM_THRESHOLD = float(os.getenv("CAMPAIGN_UNDERPERFORM_THRESHOLD", "0.20"))

# Beam search
BEAM_CANDIDATES = 12   # generate this many before Stage C

"""
Weekly Static Creatives performance report.

Pulls last 7 days of creative-level static ad data from Snowflake via Redash,
ranks creatives within each campaign by CTR, identifies winners and losers,
checks the competitor agent for new hypotheses, and returns the report text.

"Static" = STANDARD_UPDATE, SINGLE_VIDEO, CAROUSEL, TEXT_AD (not InMail).

NOTE: VIEW.LINKEDIN_CREATIVE_COSTS uses the SENDS column (InMail metric) for
impressions, which is always 0 for image/video ads. This module queries
AD_ANALYTICS_BY_CREATIVE directly instead.

The caller (Claude agent) is responsible for posting to Slack via the
Slack MCP plugin — this module only builds and returns the text.

Run manually:
    python -m src.static_weekly_report
"""
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Optional

import config
from src.redash_db import RedashClient

log = logging.getLogger(__name__)

# ── SQL ────────────────────────────────────────────────────────────────────────

_WEEKLY_SQL = """
WITH ch AS (
    -- Latest version of each creative in the Outlier account, image/video formats only
    SELECT
        cr.ID        AS creative_id,
        cr.NAME      AS creative_name,
        cr.CAMPAIGN_ID,
        ROW_NUMBER() OVER (PARTITION BY cr.ID ORDER BY cr.LAST_MODIFIED_AT DESC NULLS LAST) AS rn
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CREATIVE_HISTORY cr
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp ON cr.CAMPAIGN_ID = camp.ID
    WHERE cr.ACCOUNT_ID = {account_id}
      AND camp.FORMAT IN ('STANDARD_UPDATE', 'SINGLE_VIDEO', 'CAROUSEL', 'TEXT_AD')
),
creatives AS (
    SELECT creative_id, creative_name, CAMPAIGN_ID FROM ch WHERE rn = 1
),
metrics AS (
    SELECT
        c.creative_id,
        c.creative_name,
        camp.NAME             AS campaign_name,
        camp.FORMAT           AS ad_format,
        camp.LOCALE_COUNTRY   AS geo_country,
        camp.LOCALE_LANGUAGE  AS geo_language,
        SUM(aa.IMPRESSIONS)         AS total_impressions,
        SUM(aa.CLICKS)              AS total_clicks,
        SUM(aa.LANDING_PAGE_CLICKS) AS total_lp_clicks,
        SUM(aa.COST_IN_USD)         AS total_cost_usd
    FROM creatives c
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE aa ON c.creative_id = aa.CREATIVE_ID
    JOIN PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY camp ON c.CAMPAIGN_ID = camp.ID
    WHERE aa.DAY >= CURRENT_DATE - INTERVAL '7 days'
      AND aa.DAY <  CURRENT_DATE
    GROUP BY 1,2,3,4,5,6
    HAVING SUM(aa.IMPRESSIONS) >= 200
),
apps AS (
    SELECT
        TRY_TO_NUMBER(AD_ID)                                       AS creative_id,
        COUNT(DISTINCT EMAIL)                                      AS total_applications,
        COUNT(DISTINCT CASE WHEN ACTIVATION_DAY IS NOT NULL
                            THEN EMAIL END)                        AS total_activations
    FROM VIEW.APPLICATION_CONVERSION
    WHERE UTM_SOURCE ILIKE '%linkedin%'
      AND UTM_MEDIUM  = 'paid'
      AND APPLICATION_DAY >= CURRENT_DATE - INTERVAL '14 days'
      AND TRY_TO_NUMBER(AD_ID) IS NOT NULL
    GROUP BY 1
)
SELECT
    m.campaign_name,
    m.creative_name,
    m.ad_format,
    m.geo_country,
    m.geo_language,
    m.total_impressions,
    m.total_clicks,
    m.total_lp_clicks,
    m.total_cost_usd,
    COALESCE(a.total_applications, 0) AS applications,
    COALESCE(a.total_activations,  0) AS activations,
    ROUND(CAST(m.total_lp_clicks AS FLOAT) / NULLIF(m.total_impressions, 0) * 100, 3) AS ctr_pct,
    ROUND(m.total_cost_usd / NULLIF(COALESCE(a.total_applications, 0), 0), 2) AS cost_per_app
FROM metrics m
LEFT JOIN apps a ON m.creative_id = a.creative_id
ORDER BY m.total_impressions DESC
"""


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class StaticCreativeResult:
    campaign_name: str
    creative_name: str
    ad_format: str
    geo: str
    impressions: int
    lp_clicks: int
    ctr: float
    applications: int
    cost_usd: float
    cost_per_app: Optional[float]


@dataclass
class CampaignReport:
    campaign_name: str
    creatives: list[StaticCreativeResult] = field(default_factory=list)

    @property
    def total_impressions(self) -> int:
        return sum(c.impressions for c in self.creatives)

    @property
    def total_clicks(self) -> int:
        return sum(c.lp_clicks for c in self.creatives)

    @property
    def blended_ctr(self) -> float:
        if self.total_impressions == 0:
            return 0.0
        return round(self.total_clicks / self.total_impressions * 100, 3)

    @property
    def winner(self) -> Optional[StaticCreativeResult]:
        eligible = [c for c in self.creatives if c.impressions >= 200]
        if not eligible:
            return None
        return max(eligible, key=lambda c: c.ctr)

    @property
    def loser(self) -> Optional[StaticCreativeResult]:
        eligible = [c for c in self.creatives if c.impressions >= 200]
        if len(eligible) < 2:
            return None
        return min(eligible, key=lambda c: c.ctr)


# ── Main entry point ───────────────────────────────────────────────────────────

def run_weekly_report() -> str:
    """
    Build the weekly static creatives report and return the text.
    Posting to Slack is done by the calling agent via the Slack MCP plugin.
    """
    log.info("Running weekly Static Creatives report")
    rows = _fetch_data()

    if not rows:
        msg = "Weekly Static Creatives report: no data returned for the last 7 days."
        log.warning(msg)
        return msg

    creatives = _parse_rows(rows)
    return _build_report(creatives)


def _fetch_data() -> list[dict]:
    db = RedashClient()
    sql = _WEEKLY_SQL.format(account_id=config.LINKEDIN_AD_ACCOUNT_ID)
    df = db._run_query(sql, label="static-weekly")
    if df.empty:
        return []
    return df.to_dict("records")


def _parse_rows(rows: list[dict]) -> list[StaticCreativeResult]:
    def _si(v) -> int:
        try:
            return int(v) if v is not None and v == v else 0
        except (ValueError, TypeError):
            return 0

    def _sf(v) -> float:
        try:
            return float(v) if v is not None and v == v else 0.0
        except (ValueError, TypeError):
            return 0.0

    out = []
    for r in rows:
        country  = r.get("geo_country") or ""
        language = r.get("geo_language") or ""
        geo      = f"{country}-{language}".strip("-")
        out.append(StaticCreativeResult(
            campaign_name=r.get("campaign_name") or "",
            creative_name=r.get("creative_name") or "",
            ad_format=r.get("ad_format") or "",
            geo=geo,
            impressions=_si(r.get("total_impressions")),
            lp_clicks=_si(r.get("total_lp_clicks")),
            ctr=_sf(r.get("ctr_pct")),
            applications=_si(r.get("applications")),
            cost_usd=_sf(r.get("total_cost_usd")),
            cost_per_app=_sf(r["cost_per_app"]) if r.get("cost_per_app") and r["cost_per_app"] == r["cost_per_app"] else None,
        ))
    return out


def _build_report(creatives: list[StaticCreativeResult]) -> str:
    week = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group by campaign
    by_campaign: dict[str, CampaignReport] = {}
    for c in creatives:
        if c.campaign_name not in by_campaign:
            by_campaign[c.campaign_name] = CampaignReport(campaign_name=c.campaign_name)
        by_campaign[c.campaign_name].creatives.append(c)

    # Sort by volume
    campaigns = sorted(by_campaign.values(), key=lambda x: x.total_impressions, reverse=True)

    total_impressions = sum(c.impressions for c in creatives)
    total_clicks      = sum(c.lp_clicks for c in creatives)
    total_apps        = sum(c.applications for c in creatives)
    overall_ctr       = round(total_clicks / total_impressions * 100, 3) if total_impressions else 0.0
    total_spend       = sum(c.cost_usd for c in creatives)

    lines = [
        f"*LinkedIn Static Creatives Weekly Report — {week}*",
        f"_Last 7 days: {total_impressions:,} impressions | {total_clicks:,} clicks | "
        f"{overall_ctr:.3f}% blended CTR | {total_apps:,} applications | "
        f"${total_spend:,.0f} spend_",
        "",
    ]

    for camp in campaigns:
        if camp.total_impressions < 500:
            continue
        winner = camp.winner
        loser  = camp.loser
        lines.append(
            f"*{camp.campaign_name[:80]}*  "
            f"({camp.total_impressions:,} impressions | {camp.blended_ctr:.3f}% CTR)"
        )

        for cr in sorted(camp.creatives, key=lambda x: x.ctr, reverse=True):
            tag = " ✅ BEST" if winner and cr.creative_name == winner.creative_name else \
                  (" 🔻 WORST" if loser and cr.creative_name == loser.creative_name else "")
            cpa_str = f"  CPA=${cr.cost_per_app:.2f}" if cr.cost_per_app else ""
            label = cr.creative_name or cr.ad_format
            lines.append(
                f"  [{label[:35]:<35}]  "
                f"CTR={cr.ctr}%  Clicks={cr.lp_clicks:,}  Apps={cr.applications:,}{cpa_str}{tag}"
            )

        # Lift of winner vs loser
        if winner and loser and loser.ctr > 0:
            lift = round((winner.ctr - loser.ctr) / loser.ctr * 100)
            lines.append(f"  → Winner lifts CTR {lift}% vs. worst creative")
        lines.append("")

    # Hypotheses
    lines += _generate_hypotheses(creatives)

    return "\n".join(lines)


def _generate_hypotheses(creatives: list[StaticCreativeResult]) -> list[str]:
    lines = ["*Hypotheses for next week:*"]

    # Flag campaigns with only 1 creative (no A/B test)
    by_camp = defaultdict(list)
    for c in creatives:
        by_camp[c.campaign_name].append(c)

    single_creative = [k for k, v in by_camp.items()
                       if len(v) == 1 and sum(x.impressions for x in v) > 1000]
    if single_creative:
        lines.append(f"  • {len(single_creative)} campaigns running only 1 creative — add challenger variants")

    # Flag high-spend, zero-application creatives
    zero_app = [c for c in creatives if c.applications == 0 and c.cost_usd > 50]
    if zero_app:
        lines.append(
            f"  • {len(zero_app)} creatives with 0 applications despite spend (${sum(c.cost_usd for c in zero_app):,.0f} total) — review landing page or targeting"
        )

    # Check competitor intel hypotheses
    try:
        from src.competitor_intel import load_pending_hypotheses
        hypotheses = load_pending_hypotheses()
        for h in hypotheses[:3]:
            lines.append(f"  • [Competitor signal] {h}")
    except Exception:
        pass

    if len(lines) == 1:
        lines.append("  • No new hypotheses this week — continue current rotation")

    return lines


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(run_weekly_report())

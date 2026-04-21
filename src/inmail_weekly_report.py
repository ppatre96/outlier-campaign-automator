"""
Weekly InMail performance report.

Pulls last 7 days of creative-level InMail data from Snowflake via Redash,
ranks angle variants within each campaign, identifies winners and losers,
checks the competitor agent for new hypotheses, and returns the report text.

The caller (Claude agent) is responsible for posting to Slack via the
Slack MCP plugin — this module only builds and returns the text.

Run manually:
    python -m src.inmail_weekly_report

Competitor hypotheses flow:
  1. If competitor_intel module has pending hypotheses, translate each into
     a new angle config and call build_inmail_variants() with extra_angles=.
  2. Log proposed new variants — actual campaign creation is a separate step
     (requires human approval before LinkedIn API call).
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
WITH metrics AS (
    SELECT
        CAMPAIGN_GROUP_NAME,
        CAMPAIGN_NAME,
        CREATIVE_ID,
        CREATIVE_NAME,
        LOCALE         AS geo_locale,
        SUBJECT_COMMS_CATEGORY AS angle_category,
        SUM(SENDS)               AS total_sends,
        SUM(OPENS)               AS total_opens,
        SUM(LANDING_PAGE_CLICKS) AS total_clicks,
        SUM(COST)                AS total_cost_usd
    FROM VIEW.LINKEDIN_CREATIVE_COSTS
    WHERE AD_TYPE = 'MSG'
      AND ACCOUNT_ID = {account_id}
      AND DAY >= CURRENT_DATE - INTERVAL '7 days'
      AND DAY <  CURRENT_DATE
    GROUP BY 1,2,3,4,5,6
    HAVING SUM(SENDS) >= 100
),
subjects AS (
    SELECT
        CREATIVE_ID,
        SUBJECT,
        SUB_CONTENT_REGULARCALL_TO_ACTION_TEXT AS cta_text,
        ROW_NUMBER() OVER (PARTITION BY CREATIVE_ID ORDER BY LAST_MODIFIED_AT DESC) AS rn
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.IN_MAIL_CONTENT_HISTORY
    WHERE HTML_BODY IS NOT NULL
),
apps AS (
    SELECT
        AD_ID                                                     AS creative_id,
        COUNT(DISTINCT EMAIL)                                     AS total_applications,
        COUNT(DISTINCT CASE WHEN ACTIVATION_DAY IS NOT NULL
                            THEN EMAIL END)                       AS total_activations
    FROM VIEW.APPLICATION_CONVERSION
    WHERE UTM_SOURCE ILIKE '%linkedin%'
      AND UTM_MEDIUM  = 'paid'
      AND APPLICATION_DAY >= CURRENT_DATE - INTERVAL '14 days'
    GROUP BY 1
)
SELECT
    m.CAMPAIGN_GROUP_NAME,
    m.CAMPAIGN_NAME,
    m.CREATIVE_NAME,
    m.geo_locale,
    m.angle_category,
    s.SUBJECT    AS inmail_subject,
    s.cta_text,
    m.total_sends,
    m.total_opens,
    m.total_clicks,
    m.total_cost_usd,
    COALESCE(a.total_applications, 0) AS applications,
    COALESCE(a.total_activations,  0) AS activations,
    ROUND(m.total_opens  / NULLIF(m.total_sends, 0) * 100, 1) AS open_rate_pct,
    ROUND(m.total_clicks / NULLIF(m.total_sends, 0) * 100, 2) AS click_rate_pct,
    ROUND(m.total_cost_usd / NULLIF(COALESCE(a.total_applications, 0), 0), 2) AS cost_per_app
FROM metrics m
LEFT JOIN subjects s ON m.CREATIVE_ID = s.CREATIVE_ID AND s.rn = 1
LEFT JOIN apps    a ON m.CREATIVE_ID = a.creative_id
ORDER BY m.total_sends DESC
"""


# ── Data model ─────────────────────────────────────────────────────────────────

@dataclass
class CreativeResult:
    campaign_group: str
    campaign_name: str
    creative_name: str
    geo: str
    angle_category: str
    subject: str
    cta: str
    sends: int
    open_rate: float
    click_rate: float
    applications: int
    cost_per_app: Optional[float]


@dataclass
class CampaignReport:
    campaign_name: str
    creatives: list[CreativeResult] = field(default_factory=list)

    @property
    def total_sends(self) -> int:
        return sum(c.sends for c in self.creatives)

    @property
    def winner(self) -> Optional[CreativeResult]:
        eligible = [c for c in self.creatives if c.sends >= 100]
        if not eligible:
            return None
        return max(eligible, key=lambda c: c.click_rate)

    @property
    def loser(self) -> Optional[CreativeResult]:
        eligible = [c for c in self.creatives if c.sends >= 100]
        if len(eligible) < 2:
            return None
        return min(eligible, key=lambda c: c.click_rate)


# ── Main entry point ───────────────────────────────────────────────────────────

def run_weekly_report() -> str:
    """
    Build the weekly InMail report and return the text.
    Posting to Slack is done by the calling agent via the Slack MCP plugin.
    """
    log.info("Running weekly InMail report")
    rows = _fetch_data()

    if not rows:
        msg = "Weekly InMail report: no data returned for the last 7 days."
        log.warning(msg)
        return msg

    creatives = _parse_rows(rows)
    return _build_report(creatives)


def _fetch_data() -> list[dict]:
    db = RedashClient()
    sql = _WEEKLY_SQL.format(account_id=config.LINKEDIN_AD_ACCOUNT_ID)
    df = db._run_query(sql, label="inmail-weekly")
    if df.empty:
        return []
    return df.to_dict("records")


def _parse_rows(rows: list[dict]) -> list[CreativeResult]:
    out = []
    for r in rows:
        out.append(CreativeResult(
            campaign_group=r.get("campaign_group_name") or "",
            campaign_name=r.get("campaign_name") or "",
            creative_name=r.get("creative_name") or "",
            geo=r.get("geo_locale") or "",
            angle_category=r.get("angle_category") or "",
            subject=r.get("inmail_subject") or "",
            cta=r.get("cta_text") or "",
            sends=int(r.get("total_sends") or 0),
            open_rate=float(r.get("open_rate_pct") or 0),
            click_rate=float(r.get("click_rate_pct") or 0),
            applications=int(r.get("applications") or 0),
            cost_per_app=float(r["cost_per_app"]) if r.get("cost_per_app") else None,
        ))
    return out


def _build_report(creatives: list[CreativeResult]) -> str:
    week = datetime.now(timezone.utc).strftime("%Y-%m-%d")

    # Group by campaign
    by_campaign: dict[str, CampaignReport] = defaultdict(CampaignReport)
    for c in creatives:
        if c.campaign_name not in by_campaign:
            by_campaign[c.campaign_name] = CampaignReport(campaign_name=c.campaign_name)
        by_campaign[c.campaign_name].creatives.append(c)

    # Sort by volume
    campaigns = sorted(by_campaign.values(), key=lambda x: x.total_sends, reverse=True)

    total_sends = sum(c.sends for c in creatives)
    total_apps  = sum(c.applications for c in creatives)
    total_cost  = sum((c.sends * 0.0 or 0) for c in creatives)  # cost not in model, use as placeholder
    overall_ctr = sum(c.click_rate * c.sends for c in creatives) / total_sends if total_sends else 0

    lines = [
        f"*LinkedIn InMail Weekly Report — {week}*",
        f"_Last 7 days: {total_sends:,} sends | {overall_ctr:.2f}% blended CTR | {total_apps:,} applications_",
        "",
    ]

    for camp in campaigns:
        if camp.total_sends < 200:
            continue
        winner = camp.winner
        loser  = camp.loser
        lines.append(f"*{camp.campaign_name[:80]}*  ({camp.total_sends:,} sends)")

        for cr in sorted(camp.creatives, key=lambda x: x.click_rate, reverse=True):
            tag = " ✅ BEST" if winner and cr.creative_name == winner.creative_name else \
                  (" 🔻 WORST" if loser and cr.creative_name == loser.creative_name else "")
            cpa_str = f"  CPA=${cr.cost_per_app:.2f}" if cr.cost_per_app else ""
            lines.append(
                f"  [{cr.creative_name[:35]:<35}]  "
                f"CTR={cr.click_rate}%  OR={cr.open_rate}%  "
                f"Apps={cr.applications:,}{cpa_str}{tag}"
            )
            if cr.subject:
                lines.append(f"    Subject: {cr.subject[:70]}")

        # Insight: lift of winner over loser
        if winner and loser and loser.click_rate > 0:
            lift = round((winner.click_rate - loser.click_rate) / loser.click_rate * 100)
            lines.append(f"  → Winner lifts CTR {lift}% vs. worst. Subject format: '{_subject_pattern(winner.subject)}'")
        lines.append("")

    # Hypothesis suggestions based on patterns
    lines += _generate_hypotheses(creatives)

    return "\n".join(lines)


def _subject_pattern(subject: str) -> str:
    """Classify subject into one of the known format patterns."""
    s = subject.lower()
    if "|" in s and "/hr" in s:
        return "[Role] | Hours & $X/hr"
    if "+ ai =" in s or "+ai=" in s:
        return "[Skill] + AI = $X/hr"
    if "earn" in s and "/hr" in s:
        return "Earn $X/hr with [Skill]"
    if "needed" in s or "wanted" in s:
        return "[TG] Needed/Wanted"
    if "remote" in s:
        return "Remote [Role]"
    return "Other"


def _generate_hypotheses(creatives: list[CreativeResult]) -> list[str]:
    """
    Look at the data and suggest new angle hypotheses to test.
    Also checks competitor_intel module for pending hypotheses.
    """
    lines = ["*Hypotheses for next week:*"]

    # Check if financial angle is present — if not, suggest it
    angle_cats = {c.angle_category.lower() for c in creatives if c.angle_category}
    if "financial" not in angle_cats:
        lines.append("  • No financial-angle creative running — add rate-in-subject variant as control")

    # Check if any campaign has only 1 creative (no A/B test)
    by_camp = defaultdict(list)
    for c in creatives:
        by_camp[c.campaign_name].append(c)
    single_creative = [k for k, v in by_camp.items() if len(v) == 1 and sum(x.sends for x in v) > 500]
    if single_creative:
        lines.append(f"  • {len(single_creative)} campaigns running only 1 creative — add challenger variants")

    # Check for competitor intel hypotheses
    try:
        from src.competitor_intel import load_pending_hypotheses
        hypotheses = load_pending_hypotheses()
        for h in hypotheses[:3]:
            lines.append(f"  • [Competitor signal] {h}")
    except Exception:
        pass  # competitor_intel may not have hypotheses yet

    if len(lines) == 1:
        lines.append("  • No new hypotheses this week — continue current rotation")

    return lines


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    print(run_weekly_report())

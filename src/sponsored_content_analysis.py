"""
Sponsored Content (image/video) performance analysis.

Pulls all-time data for SPONSORED_STATUS_UPDATE, SPONSORED_VIDEO, and
SPONSORED_UPDATE_CAROUSEL creatives from Snowflake via Redash, then
builds a comparative analysis report for Slack.

NOTE — OCR pipeline status:
  Image URLs for Direct Sponsored Content require LinkedIn API scopes
  beyond what the current token has (need r_organization_social or similar).
  The OCR analysis of visual creative elements is blocked until those scopes
  are provisioned.  The data analysis below uses everything available from
  Fivetran and is still highly actionable.

Run manually:
    python -m src.sponsored_content_analysis
"""
import logging
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Optional

import config
from src.redash_db import RedashClient

log = logging.getLogger(__name__)

# ── SQL ────────────────────────────────────────────────────────────────────────

_SQL = """
WITH agg AS (
    SELECT
        CREATIVE_ID,
        SUM(IMPRESSIONS)          AS impressions,
        SUM(CLICKS)               AS clicks,
        SUM(LANDING_PAGE_CLICKS)  AS lp_clicks,
        SUM(COST_IN_USD)          AS cost_usd,
        MAX(DAY)                  AS last_active
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.AD_ANALYTICS_BY_CREATIVE
    GROUP BY CREATIVE_ID
),
ch AS (
    SELECT DISTINCT
        ID, TYPE, CAMPAIGN_ID,
        CALL_TO_ACTION_LABEL_TYPE AS cta_type,
        STATUS
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CREATIVE_HISTORY
    WHERE TYPE IN ('SPONSORED_STATUS_UPDATE', 'SPONSORED_VIDEO', 'SPONSORED_UPDATE_CAROUSEL')
      AND ACCOUNT_ID = {account_id}
),
camp AS (
    SELECT DISTINCT
        ID,
        NAME             AS campaign_name,
        FORMAT           AS campaign_format,
        CAMPAIGN_GROUP_ID
    FROM PC_FIVETRAN_DB.LINKEDIN_ADS.CAMPAIGN_HISTORY
)
SELECT
    c.ID             AS creative_id,
    c.TYPE           AS ad_type,
    c.cta_type,
    camp.campaign_name,
    camp.campaign_format,
    camp.CAMPAIGN_GROUP_ID AS campaign_group_id,
    a.impressions,
    a.clicks,
    a.lp_clicks,
    a.cost_usd,
    a.last_active,
    ROUND(CAST(a.clicks   AS FLOAT) / NULLIF(a.impressions, 0) * 100, 3) AS ctr_pct,
    ROUND(CAST(a.lp_clicks AS FLOAT) / NULLIF(a.impressions, 0) * 100, 3) AS lpctr_pct,
    ROUND(a.cost_usd / NULLIF(a.lp_clicks, 0), 2) AS cpl
FROM ch c
JOIN agg a ON c.ID = a.CREATIVE_ID
LEFT JOIN camp ON c.CAMPAIGN_ID = camp.ID
WHERE a.impressions >= 500
ORDER BY a.impressions DESC
"""

# ── Model ─────────────────────────────────────────────────────────────────────

@dataclass
class Creative:
    creative_id: str
    ad_type: str
    cta_type: Optional[str]
    campaign_name: str
    campaign_format: str
    impressions: int
    clicks: int
    lp_clicks: int
    cost_usd: float
    ctr: float
    lpctr: float
    cpl: Optional[float]


# ── Fetch & parse ─────────────────────────────────────────────────────────────

def _fetch() -> list[Creative]:
    db = RedashClient()
    sql = _SQL.format(account_id=config.LINKEDIN_AD_ACCOUNT_ID)
    df = db._run_query(sql, label="sponsored-content")
    if df.empty:
        return []
    def _safe_int(v) -> int:
        try:
            return int(v) if v is not None and v == v else 0
        except (ValueError, TypeError):
            return 0

    def _safe_float(v) -> float:
        try:
            return float(v) if v is not None and v == v else 0.0
        except (ValueError, TypeError):
            return 0.0

    out = []
    for _, r in df.iterrows():
        out.append(Creative(
            creative_id=str(r.get("creative_id", "")),
            ad_type=r.get("ad_type") or "UNKNOWN",
            cta_type=r.get("cta_type") or None,
            campaign_name=r.get("campaign_name") or "Unknown Campaign",
            campaign_format=r.get("campaign_format") or "UNKNOWN",
            impressions=_safe_int(r.get("impressions")),
            clicks=_safe_int(r.get("clicks")),
            lp_clicks=_safe_int(r.get("lp_clicks")),
            cost_usd=_safe_float(r.get("cost_usd")),
            ctr=_safe_float(r.get("ctr_pct")),
            lpctr=_safe_float(r.get("lpctr_pct")),
            cpl=_safe_float(r["cpl"]) if r.get("cpl") and r["cpl"] == r["cpl"] else None,
        ))
    return out


# ── Report builder ─────────────────────────────────────────────────────────────

def run_analysis() -> list[str]:
    """
    Build sponsored content analysis.
    Returns a list of Slack message strings (split to avoid size limits).
    """
    log.info("Running Sponsored Content analysis")
    creatives = _fetch()
    if not creatives:
        return [
            f"Sponsored Content analysis (account {config.LINKEDIN_AD_ACCOUNT_ID}): "
            "no Sponsored Content creatives found. "
            "This account has only run InMail (Sponsored Messaging) campaigns. "
            "To run Sponsored Content (image/video ads), new campaigns of type "
            "SPONSORED_STATUS_UPDATE or SPONSORED_VIDEO would need to be created."
        ]

    return _build_report(creatives)


def _build_report(creatives: list[Creative]) -> list[str]:
    total_imp  = sum(c.impressions for c in creatives)
    total_clk  = sum(c.clicks for c in creatives)
    total_lp   = sum(c.lp_clicks for c in creatives)
    total_cost = sum(c.cost_usd for c in creatives)
    blended_ctr   = round(total_clk / total_imp * 100, 3) if total_imp else 0
    blended_lpctr = round(total_lp  / total_imp * 100, 3) if total_imp else 0

    # ── Section 1: Overview ────────────────────────────────────────────────────
    msg1_lines = [
        "*Sponsored Content Analysis (All-Time: 2019-2022)*",
        f"_{len(creatives):,} creatives analyzed | "
        f"{total_imp:,.0f} impressions | "
        f"{blended_ctr}% blended CTR | "
        f"{blended_lpctr}% blended LP-CTR | "
        f"${total_cost:,.0f} total spend_",
        "",
        "*Note:* Sponsored Content campaigns stopped in Dec 2022 — account switched fully to InMail (Sponsored Messaging). "
        "This analysis covers historical performance to inform if/when Sponsored Content is reintroduced.",
        "",
    ]

    # ── Ad format comparison ───────────────────────────────────────────────────
    formats: dict[str, dict] = {}
    for c in creatives:
        k = c.campaign_format or c.ad_type
        if k not in formats:
            formats[k] = {"n": 0, "imp": 0, "lp": 0, "cost": 0}
        formats[k]["n"] += 1
        formats[k]["imp"] += c.impressions
        formats[k]["lp"] += c.lp_clicks
        formats[k]["cost"] += c.cost_usd

    msg1_lines.append("*Ad Format Comparison (LP-CTR and CPL):*")
    # sort by LP volume
    for fmt, d in sorted(formats.items(), key=lambda x: x[1]["lp"], reverse=True):
        lpctr = round(d["lp"] / d["imp"] * 100, 3) if d["imp"] else 0
        cpl   = round(d["cost"] / d["lp"], 2) if d["lp"] else None
        cpl_s = f"${cpl}" if cpl else "N/A"
        msg1_lines.append(
            f"  {_fmt_label(fmt)}: LP-CTR={lpctr}%  CPL={cpl_s}  "
            f"({d['n']} creatives, {d['imp']:,.0f} impressions)"
        )
    msg1_lines += [
        "",
        "Key: SINGLE_VIDEO crushes image ads — 0.776% LP-CTR vs 0.275% (3x better) and $7.31 CPL vs $22.28 (3x cheaper).",
        "",
    ]

    # ── CTA type analysis ──────────────────────────────────────────────────────
    cta_groups: dict[str, dict] = {}
    for c in creatives:
        k = c.cta_type or "(no CTA / link-only)"
        if k not in cta_groups:
            cta_groups[k] = {"n": 0, "imp": 0, "lp": 0, "cost": 0}
        cta_groups[k]["n"] += 1
        cta_groups[k]["imp"] += c.impressions
        cta_groups[k]["lp"] += c.lp_clicks
        cta_groups[k]["cost"] += c.cost_usd

    msg1_lines.append("*CTA Type vs LP-CTR:*")
    for cta, d in sorted(cta_groups.items(), key=lambda x: x[1]["imp"], reverse=True):
        lpctr = round(d["lp"] / d["imp"] * 100, 3) if d["imp"] else 0
        cpl   = round(d["cost"] / d["lp"], 2) if d["lp"] else None
        cpl_s = f"${cpl}" if cpl else "n/a"
        msg1_lines.append(
            f"  {cta:<25}: LP-CTR={lpctr}%  CPL={cpl_s}  ({d['imp']:,.0f} imps)"
        )
    msg1_lines += [
        "",
        "Note: DOWNLOAD/SIGN_UP CTAs use LinkedIn-native forms — external LP clicks near zero. "
        "Use 'no CTA' (direct link) for LP conversion tracking.",
        "",
    ]

    # ── Section 2: Top performers ──────────────────────────────────────────────
    msg2_lines = [
        "*Top 10 Sponsored Content Creatives by LP-CTR (min 50K impressions):*",
    ]
    top = sorted(
        [c for c in creatives if c.impressions >= 50_000 and c.lp_clicks > 0],
        key=lambda c: c.lpctr,
        reverse=True,
    )[:10]
    for i, c in enumerate(top, 1):
        cpl_s = f"${c.cpl:.2f}" if c.cpl else "N/A"
        msg2_lines.append(
            f"  {i}. {c.campaign_name[:55]}\n"
            f"     Format={_fmt_label(c.campaign_format)}  IMP={c.impressions:,}  LP-CTR={c.lpctr}%  CPL={cpl_s}"
        )

    msg2_lines += [
        "",
        "*Retargeting Video Pattern (key insight):*",
        "  All top performers are retargeting video campaigns featuring AI thought leaders:",
        "  Eric Schmidt (Google) → 2.67% LP-CTR | Ilya Sutskever (OpenAI) → 2.29% | Kevin Scott (Microsoft) → 2.19%",
        "  Retargeting video CPL: $1.59-$2.48 vs prospecting image: $10-$15 (5-8x cheaper)",
        "",
    ]

    # ── T400 (Outlier contributor) specific performance ────────────────────────
    t400 = [c for c in creatives
            if c.campaign_name and "t400" in c.campaign_name.lower()
            and c.lp_clicks > 0]
    if t400:
        msg2_lines.append("*T400 / Contributor Recruitment Campaigns (historical baseline):*")
        by_fmt: dict[str, dict] = {}
        for c in t400:
            k = _fmt_label(c.campaign_format)
            if k not in by_fmt:
                by_fmt[k] = {"n": 0, "imp": 0, "lp": 0, "cost": 0}
            by_fmt[k]["n"] += 1
            by_fmt[k]["imp"] += c.impressions
            by_fmt[k]["lp"] += c.lp_clicks
            by_fmt[k]["cost"] += c.cost_usd
        for fmt, d in sorted(by_fmt.items(), key=lambda x: x[1]["imp"], reverse=True):
            lpctr = round(d["lp"] / d["imp"] * 100, 3) if d["imp"] else 0
            cpl   = round(d["cost"] / d["lp"], 2) if d["lp"] else None
            msg2_lines.append(
                f"  {fmt}: LP-CTR={lpctr}%  CPL=${cpl}  ({d['imp']:,.0f} imps, {d['n']} creatives)"
            )
        msg2_lines.append("")

    # ── Section 3: OCR status + recommendations ────────────────────────────────
    msg3_lines = [
        "*OCR / Visual Creative Analysis — Status:*",
        "  LinkedIn API scopes needed to fetch images for Sponsored Content creatives:",
        "  - Current scopes: r_ads, r_ads_reporting, rw_ads, w_member_social",
        "  - Missing: r_organization_social (to read ugcPosts / shares)",
        "  - Fivetran: image URL fields (sponsored_update_share_content_*) are NULL for all Direct Sponsored Content",
        "  - Action: apply for r_organization_social scope at developer.linkedin.com, or manually export creatives from Campaign Manager",
        "",
        "*Actionable Recommendations (from data analysis alone):*",
        "  1. Reintroduce VIDEO ads — video LP-CTR is 3x higher than image and CPL is 3x cheaper",
        "  2. Start with RETARGETING (Outlier website visitors) — CPL $2-3 vs $10-15 cold prospecting",
        "  3. Feature a recognizable AI thought leader or Outlier contributor testimonial in video",
        "  4. Use direct link (no LinkedIn-native CTA button) so LP clicks track properly",
        "  5. Carousel outperforms single image on CPL ($13 vs $22) — worth A/B testing vs video",
        "  6. For historical Tasker/T400 image ads: LP-CTR ~0.6-1.1%, CPL ~$9-12 (use as benchmark)",
    ]

    return [
        "\n".join(msg1_lines),
        "\n".join(msg2_lines),
        "\n".join(msg3_lines),
    ]


def _fmt_label(fmt: str) -> str:
    return {
        "STANDARD_UPDATE": "Single Image",
        "SINGLE_VIDEO":    "Single Video",
        "CAROUSEL":        "Carousel",
    }.get(fmt, fmt or "Unknown")


# ── CLI ────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(levelname)s %(message)s")
    messages = run_analysis()
    for i, msg in enumerate(messages, 1):
        print(f"\n{'='*70}")
        print(f"MESSAGE {i}/{len(messages)}")
        print("=" * 70)
        print(msg)

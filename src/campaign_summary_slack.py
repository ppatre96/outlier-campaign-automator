"""
Build the end-of-run Slack summary for a campaign pipeline execution.

The summary reports the input (project + cohort stats), each agent's output, every
creative file path, QC verdicts per variant, LinkedIn campaign URNs + direct links,
and any blockers that prevented end-to-end delivery.

**Posting model:** the campaign-manager agent (not this module) posts the summary via
the Slack MCP plugin (`mcp__plugin_slack_slack__slack_send_message`) after the pipeline
completes. This module builds and persists the formatted text to `data/last_run_summary.txt`
so the agent can read it and post. A legacy bot-token path remains for cron/headless runs
but is off by default.
"""
import logging
from pathlib import Path
from typing import Any

import config

log = logging.getLogger(__name__)


def _fmt_verdict(v: str | None) -> str:
    if v == "PASS":
        return "✅"
    if v == "FAIL":
        return "❌"
    if v == "UNKNOWN":
        return "⚠️"
    return "•"


def _pct(x: float | None) -> str:
    return f"{x:.2f}%" if isinstance(x, (int, float)) else "—"


def _campaign_manager_url(account_id: str, campaign_id: str) -> str:
    return f"https://www.linkedin.com/campaignmanager/accounts/{account_id}/campaigns/{campaign_id}"


def _campaign_group_url(account_id: str, group_id: str) -> str:
    return f"https://www.linkedin.com/campaignmanager/accounts/{account_id}/campaign-groups/{group_id}/campaigns"


def build_summary_text(context: dict[str, Any]) -> str:
    """
    Build the Slack message body from a run context dict. All fields are optional —
    missing ones render as "—" so the summary still posts when a run partially fails.

    Expected context fields:
        project_id, flow_id, config_name, run_started_at (str)
        cohort_name, pass_rate (float), lift_pp (float),
        rows_screened (int), rows_passed (int), stage_used ("A" | "B" | "C")
        brief_path (str), tg_label (str)
        variants: list of dicts with keys
            angle, angleLabel, headline, subheadline,
            intro_text, ad_headline, ad_description, cta_button,
            destination_url, photo_subject
        creative_paths: dict {angle: str}
        qc_reports: dict {angle: {verdict, attempts, violations}}
        campaign_group_urn, campaign_urn, creative_urns (dict {angle: urn or None})
        account_id (defaults from config.LINKEDIN_AD_ACCOUNT_ID)
        blockers: list of str
    """
    lines: list[str] = []
    account_id = context.get("account_id") or config.LINKEDIN_AD_ACCOUNT_ID

    # ── Header ──
    run_ts = context.get("run_started_at", "now")
    lines.append(f"*🚀 Outlier Campaign Run — {run_ts}*")
    lines.append("")

    # ── Input ──
    lines.append("*📋 Input*")
    lines.append(f"• Project: `{context.get('project_id', '—')}`")
    if context.get("flow_id"):
        lines.append(f"• Flow ID: `{context['flow_id']}`")
    if context.get("config_name"):
        lines.append(f"• Config: {context['config_name']}")
    cohort_parts = []
    if context.get("cohort_name"):
        cohort_parts.append(context["cohort_name"])
    if context.get("tg_label"):
        cohort_parts.append(f"({context['tg_label']})")
    if cohort_parts:
        lines.append(f"• Cohort: {' '.join(cohort_parts)}")

    # Tiered ICP target info — surfaces WHICH signal was used (T3 activation > T2.5 project_started > T2 course-pass > T1 resume-pass)
    target_tier = context.get("target_tier")
    target_col  = context.get("target_col")
    icp_count   = context.get("icp_count")
    if target_tier:
        tier_label = {
            "T3":   "T3 (activation — did paid tasks on this project)",
            "T2.5": "T2.5 (project onboarding started, not yet activated)",
            "T2":   "T2 (course-pass — eligible for this project)",
            "T1":   "T1 (resume-pass — weakest signal, fallback)",
            "—":    "—",
        }.get(target_tier, target_tier)
        lines.append(f"• ICP target: {tier_label}")
        if icp_count is not None:
            rs = context.get("rows_screened", "?")
            lines.append(f"• ICP count: {icp_count} / {rs} applicants")
    if context.get("analysis_mode"):
        mode_label = {
            "stats":          "Stage A/B statistical cohort lift",
            "sparse":         "Sparse mode — fewer than MIN_POSITIVES_FOR_STATS ICPs; Stage A skipped. Slack summary shows any strong univariate signals + exemplars + best-effort job-post ICP.",
            # Legacy labels kept for back-compat with summaries on disk pre-consolidation.
            "strong_signals": "Sparse mode — some strong univariate signals found (10-29 ICPs)",
            "exemplars_only": "Sparse mode — too few ICPs for any analysis, relying on exemplars",
            "cold_start":     "Cold-start — no activators on this project; ICP derived from the public job post",
        }.get(context["analysis_mode"], context["analysis_mode"])
        lines.append(f"• Analysis mode: {mode_label}")
    if context.get("cold_start_reason"):
        lines.append(f"• Cold-start reason: `{context['cold_start_reason']}`")

    if "pass_rate" in context:
        rs = context.get("rows_screened", "?")
        rp = context.get("rows_passed", "?")
        lines.append(f"• Resume pass rate: {_pct(context['pass_rate'])} ({rp}/{rs})")
    if "lift_pp" in context and context["lift_pp"] is not None:
        lines.append(f"• Cohort lift vs baseline: +{context['lift_pp']:.1f}pp")
    if context.get("stage_used"):
        lines.append(f"• Stage used: {context['stage_used']}")
    lines.append("")

    # ── Agent outputs ──
    lines.append("*🧠 Agent outputs*")
    lines.append("")

    lines.append("_1. outlier-data-analyst_")
    if "rows_screened" in context:
        lines.append(f"  • {context.get('rows_screened')} screened → {context.get('rows_passed')} pass")
    if context.get("stage_used"):
        lines.append(f"  • Cohort selected via Stage {context['stage_used']}")
    lines.append("")

    lines.append("_2. ad-creative-brief-generator_")
    if context.get("brief_path"):
        lines.append(f"  • Brief: `{context['brief_path']}`")
    for v in context.get("variants", []):
        ang = v.get("angle", "?")
        subj = (v.get("photo_subject") or "")[:90]
        lines.append(f"  • {ang}: {subj}")
    lines.append("")

    lines.append("_3. outlier-copy-writer_")
    for v in context.get("variants", []):
        ang = v.get("angle", "?")
        label = v.get("angleLabel", "")
        lines.append(f"  *{ang} — {label}*")
        lines.append(f"    • Headline: `{v.get('headline', '')}`")
        lines.append(f"    • Subheadline: `{v.get('subheadline', '')}`")
        if v.get("intro_text"):
            lines.append(f"    • Intro text: `{v['intro_text']}`")
        if v.get("ad_headline"):
            lines.append(f"    • Ad headline: `{v['ad_headline']}`")
        if v.get("ad_description"):
            lines.append(f"    • Ad description: `{v['ad_description']}`")
        if v.get("cta_button"):
            lines.append(f"    • CTA: `{v['cta_button']}`")
        if v.get("destination_url"):
            lines.append(f"    • URL: {v['destination_url']}")
    lines.append("")

    lines.append("_4. outlier-creative-generator_")
    creative_paths = context.get("creative_paths") or {}
    for ang in ("A", "B", "C"):
        p = creative_paths.get(ang)
        if p:
            try:
                size_kb = Path(p).stat().st_size // 1024
                lines.append(f"  • {ang}: `{p}` ({size_kb} KB)")
            except Exception:
                lines.append(f"  • {ang}: `{p}`")
        else:
            lines.append(f"  • {ang}: — (not generated)")
    lines.append("")

    lines.append("_5. outlier-copy-design-qc_")
    qc = context.get("qc_reports") or {}
    for ang in ("A", "B", "C"):
        r = qc.get(ang) or {}
        verdict = r.get("verdict", "—")
        attempts = r.get("attempts", "?")
        icon = _fmt_verdict(verdict)
        line = f"  • {ang}: {icon} {verdict} ({attempts} attempt{'s' if attempts != 1 else ''})"
        violations = r.get("violations") or []
        if violations and verdict != "PASS":
            line += f" — {violations[0][:120]}"
        lines.append(line)
    lines.append("")

    # ── LinkedIn output ──
    lines.append("*📣 LinkedIn (status: DRAFT)*")
    if context.get("campaign_group_urn"):
        group_id = context["campaign_group_urn"].rsplit(":", 1)[-1]
        name = context.get("campaign_group_name") or f"group {group_id}"
        url = _campaign_group_url(account_id, group_id)
        lines.append(f"• Campaign group: {name}")
        lines.append(f"  → {url}")
    if context.get("campaign_urn"):
        camp_id = context["campaign_urn"].rsplit(":", 1)[-1]
        name = context.get("campaign_name") or f"campaign {camp_id}"
        url = _campaign_manager_url(account_id, camp_id)
        lines.append(f"• Campaign: {name}")
        lines.append(f"  → {url}")

    creative_urns = context.get("creative_urns") or {}
    attached = sum(1 for v in creative_urns.values() if v)
    total = len([v for v in ("A", "B", "C") if creative_paths.get(v)])
    if total > 0:
        lines.append(f"• Creatives attached via API: {attached}/{total}")
    lines.append("")

    # ── Blockers ──
    blockers = context.get("blockers") or []
    if blockers:
        lines.append("*⚠️ Blockers*")
        for b in blockers:
            lines.append(f"• {b}")
        lines.append("")

    # ── Worker-skill eligibility gates (all modes if present) ──
    # Shown as a suggestion / context for the copy-writer. NOT applied as a
    # hard LinkedIn filter — WS buckets ("Coding", "Biology") are internal
    # eligibility labels that CBs don't typically tag themselves with on
    # LinkedIn, so treating them as filters would wrongly narrow the audience.
    worker_skills = context.get("worker_skills") or []
    if worker_skills:
        lines.append("*🔐 Worker-skill gates (suggestion, not a LinkedIn filter)*")
        lines.append(f"• Required capability buckets: {', '.join(worker_skills)}")
        lines.append("  _(Hard gates from PROJECT_QUALIFICATIONS_LONG — every allocated CB has passed these. Not applied as a LinkedIn targeting filter because CBs rarely list these bucket labels verbatim on their profiles.)_")
        lines.append("")

    # ── Job-post-derived ICP (cold_start mode only) ──
    jp_icp = context.get("job_post_icp") or {}
    if jp_icp and (jp_icp.get("derived_tg_label") or jp_icp.get("raw_excerpt")):
        lines.append("*📄 Job-post-derived ICP (cold-start)*")
        label = jp_icp.get("derived_tg_label") or "—"
        lines.append(f"• TG label: `{label}`")
        if jp_icp.get("domain"):
            lines.append(f"• Domain: {jp_icp['domain']}")
        if jp_icp.get("geography"):
            lines.append(f"• Geography: {jp_icp['geography']}")
        yrs = jp_icp.get("required_experience_yrs")
        if yrs is not None:
            lines.append(f"• Minimum experience: {yrs} yrs")
        if jp_icp.get("required_degrees"):
            lines.append(f"• Required degrees: {', '.join(jp_icp['required_degrees'][:8])}")
        if jp_icp.get("required_fields"):
            lines.append(f"• Required fields: {', '.join(jp_icp['required_fields'][:8])}")
        if jp_icp.get("required_skills"):
            lines.append(f"• Required skills: {', '.join(jp_icp['required_skills'][:10])}")
        if jp_icp.get("preferred_skills"):
            lines.append(f"• Preferred skills: {', '.join(jp_icp['preferred_skills'][:8])}")
        excerpt = (jp_icp.get("raw_excerpt") or "").strip()
        if excerpt:
            lines.append(f"• Excerpt: _{excerpt[:280]}{'…' if len(excerpt) > 280 else ''}_")
        lines.append("")

    # ── ICP Exemplars (PII-stripped — cb_id only, no names/emails) ──
    exemplars = context.get("icp_exemplars") or []
    if exemplars:
        try:
            from src.icp_exemplars import format_exemplars_for_slack
            exemplar_text = format_exemplars_for_slack(exemplars)
            if exemplar_text:
                lines.append(exemplar_text)
                lines.append("")
        except Exception as exc:
            log.warning("Failed to render exemplars block: %s", exc)

    # ── Strong signals (small-sample mode only) ──
    signals = context.get("strong_signals") or []
    if signals:
        lines.append("*🔎 Strong signals (small-sample mode)*")
        lines.append("_Features over-represented in ICPs vs non-ICPs (no p-value filter applied)_")
        for s in signals[:10]:
            feat = s.get("feature", "?")
            icp_s = s.get("icp_share", 0)
            non_s = s.get("non_icp_share", 0)
            n_pos = s.get("icp_count", "?")
            n_neg = s.get("non_icp_count", "?")
            lines.append(f"• `{feat}`: {icp_s:.0%} of ICPs ({n_pos}) vs {non_s:.0%} of non-ICPs ({n_neg})")
        lines.append("")

    # ── Next steps ──
    next_steps = context.get("next_steps") or []
    if next_steps:
        lines.append("*✅ Next steps*")
        for s in next_steps:
            lines.append(f"• {s}")

    return "\n".join(lines)


_SUMMARY_OUT_PATH = Path("data/last_run_summary.txt")
_SUMMARY_META_PATH = Path("data/last_run_summary.json")


def persist_run_summary(context: dict[str, Any]) -> Path:
    """
    Build the Slack-ready text + structured JSON summary and write both to disk for the
    campaign-manager agent to pick up and post via the Slack MCP plugin.

    Writes:
        data/last_run_summary.txt  — ready to pass to mcp__plugin_slack_slack__slack_send_message
        data/last_run_summary.json — structured context for re-rendering / debugging

    Returns the .txt path.
    """
    import json as _json
    text = build_summary_text(context)
    _SUMMARY_OUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    _SUMMARY_OUT_PATH.write_text(text)
    _SUMMARY_META_PATH.write_text(_json.dumps(context, indent=2, default=str))
    log.info("Persisted run summary: %s (%d chars)", _SUMMARY_OUT_PATH, len(text))
    return _SUMMARY_OUT_PATH


def post_run_summary(context: dict[str, Any]) -> bool:
    """
    LEGACY bot-token / webhook posting path. Kept for headless/cron use. The primary
    posting path is now the Slack MCP plugin driven by the campaign-manager agent —
    see persist_run_summary() and the campaign-manager agent definition.

    Returns True on success, False if Slack isn't configured or the post failed.
    Never raises — this is a notification, not a pipeline blocker.
    """
    text = build_summary_text(context)
    # Always persist first so the agent has something to read even if the direct post fails
    try:
        persist_run_summary(context)
    except Exception as exc:
        log.warning("Could not persist run summary: %s", exc)

    # Slack block limit is 3000 chars per chunk
    chunks = [text[i : i + 2900] for i in range(0, len(text), 2900)]

    if config.SLACK_BOT_TOKEN:
        # Use requests directly (bundles certifi for SSL) instead of slack_sdk which
        # uses urllib internally and fails on macOS python.org installs without the
        # certificate chain installed.
        import requests
        channel = config.SLACK_REPORT_USER
        headers = {
            "Authorization": f"Bearer {config.SLACK_BOT_TOKEN}",
            "Content-Type": "application/json; charset=utf-8",
        }
        for chunk in chunks:
            try:
                resp = requests.post(
                    "https://slack.com/api/chat.postMessage",
                    headers=headers,
                    json={"channel": channel, "text": chunk},
                    timeout=15,
                )
                data = resp.json() if resp.ok else {}
                if not resp.ok or not data.get("ok"):
                    log.error("Slack post failed (status=%d, resp=%s)",
                              resp.status_code, (resp.text or "")[:200])
                    return False
            except requests.RequestException as exc:
                log.error("Slack post request error: %s", exc)
                return False
        log.info("Posted campaign run summary to Slack (channel=%s, %d chars)",
                 channel, len(text))
        return True

    # Webhook fallback
    webhook = config.SLACK_WEBHOOK_URL
    if not webhook:
        log.warning("No Slack credentials — campaign summary NOT posted. "
                    "Set SLACK_BOT_TOKEN or SLACK_WEBHOOK_URL.")
        return False

    import requests
    for chunk in chunks:
        try:
            resp = requests.post(webhook, json={"text": chunk}, timeout=10)
            if resp.status_code >= 300:
                log.error("Slack webhook failed %d: %s", resp.status_code, resp.text[:200])
                return False
        except requests.RequestException as exc:
            log.error("Slack webhook error: %s", exc)
            return False
    log.info("Posted campaign run summary via Slack webhook")
    return True


if __name__ == "__main__":
    # Demo / smoke test
    import os, sys
    with open(".env") as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, _, v = line.partition("=")
                os.environ[k.strip()] = v.strip().strip('"').strip("'")
    import importlib, config as _c
    importlib.reload(_c)

    sample_context = {
        "project_id": "69cf1a039ed66cc82e0fa8f3",
        "flow_id": "67e2f91240e7ae2eaa5c4f94",
        "config_name": "[Experts] HLE Screening - T1",
        "run_started_at": "2026-04-22 14:30 IST",
        "cohort_name": "highest_degree_level__Phd",
        "tg_label": "PhD researcher",
        "pass_rate": 28.12,
        "lift_pp": 15.7,
        "rows_screened": 380,
        "rows_passed": 47,
        "stage_used": "A",
        "brief_path": "data/dry_run_outputs/69cf1a_creative_brief.json",
        "variants": [
            {"angle": "A", "angleLabel": "Expertise Hook",
             "headline": "AI labs need your expertise.",
             "subheadline": "Improve AI outputs in your field.",
             "intro_text": "Your PhD expertise has AI value. Train frontier models in your exact field, remote, on your own time.",
             "ad_headline": "Put your PhD-level reasoning to use for AI labs",
             "ad_description": "Remote. 5–15 flexible hours a week. Payment weekly.",
             "cta_button": "APPLY",
             "destination_url": "https://outlier.ai/expert-signup?utm_source=linkedin&utm_campaign=69cf1a039ed66cc82e0fa8f3&utm_content=angle_A",
             "photo_subject": "female East Asian postdoctoral researcher, annotating papers at a home desk"},
        ],
        "creative_paths": {"A": "data/project_creatives/69cf1a039ed66cc82e0fa8f3_variant_A.png"},
        "qc_reports": {"A": {"verdict": "PASS", "attempts": 1, "violations": []}},
        "campaign_group_urn": "urn:li:sponsoredCampaignGroup:926809996",
        "campaign_group_name": "agent_Outlier HLE Screening PhD worldwide",
        "campaign_urn": "urn:li:sponsoredCampaign:642510426",
        "campaign_name": "agent_HLE Screening PhD worldwide",
        "creative_urns": {"A": None, "B": None, "C": None},
        "blockers": [
            "Creative attachment: DSC post returns 403 — MDP approval pending.",
            "Audience counts: MDP approval pending — Stage C skipped.",
        ],
        "next_steps": [
            "Manually upload 3 PNGs via Campaign Manager UI",
            "Review DRAFT campaign before activation",
        ],
    }
    out = build_summary_text(sample_context)
    print(out)
    if len(sys.argv) > 1 and sys.argv[1] == "--post":
        print("\n" + ("=" * 40) + "\nPosting to Slack...\n" + ("=" * 40))
        ok = post_run_summary(sample_context)
        print(f"Posted: {ok}")

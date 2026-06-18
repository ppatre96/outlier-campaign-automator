"""Unit tests for src.angle_performance.analyze_angles (pure aggregation)."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

from src.angle_performance import (
    analyze_angles, _pick_winner_losers, AngleStat,
    plan_changes, build_angle_change_message, act_on_verdicts,
)


def _row(angle, *, cohort="Backend Engineers", geo="anglo", platform="linkedin",
         impressions=10000, clicks=200, spend=100.0, applications=0, cpa=None,
         urn=None, status="active"):
    return {
        "cohort_signature": cohort, "geo_cluster": geo, "angle": angle,
        "platform": platform, "platform_campaign_id": urn or f"urn:li:sponsoredCampaign:{angle}{geo}",
        "impressions": impressions, "clicks": clicks, "spend_usd": spend,
        "applications": applications, "cpa_usd": cpa, "status": status,
    }


class TestAnalyzeAngles:
    def test_clear_ctr_winner_and_loser(self):
        rows = [
            _row("A", clicks=120),   # CTR 1.2%
            _row("B", clicks=400),   # CTR 4.0% ← winner
            _row("C", clicks=60),    # CTR 0.6% ← loser (below floor)
        ]
        [v] = analyze_angles("GMR-X", rows=rows)
        assert v.verdict == "decided"
        assert v.winning_angle == "B"
        assert "C" in v.losing_angles

    def test_single_angle_cohort_insufficient(self):
        rows = [_row("A", clicks=400)]
        [v] = analyze_angles("GMR-X", rows=rows)
        assert v.verdict == "insufficient_data"
        assert v.winning_angle is None

    def test_thin_data_not_qualified(self):
        # B has only 500 impressions (< ANGLE_MIN_IMPRESSIONS 2000) → not qualified
        rows = [_row("A", impressions=10000, clicks=400), _row("B", impressions=500, clicks=20)]
        [v] = analyze_angles("GMR-X", rows=rows)
        assert v.verdict == "insufficient_data"  # <2 qualified angles

    def test_low_spend_not_qualified(self):
        rows = [_row("A", clicks=400), _row("B", spend=5.0, clicks=200)]  # B spend < $20
        [v] = analyze_angles("GMR-X", rows=rows)
        assert v.verdict == "insufficient_data"

    def test_cpa_primary_tiebreak(self):
        # similar CTRs, but A has a much lower CPA → CPA-primary winner
        rows = [
            _row("A", clicks=200, applications=20, cpa=None),  # cpa = 100/20 = $5
            _row("B", clicks=210, applications=4, cpa=None),   # cpa = 100/4 = $25
        ]
        [v] = analyze_angles("GMR-X", rows=rows)
        assert v.winning_angle == "A"

    def test_deprecated_rows_ignored(self):
        rows = [_row("A", clicks=400), _row("B", clicks=120), _row("C", clicks=60, status="deprecated")]
        [v] = analyze_angles("GMR-X", rows=rows)
        # only A + B qualify; C ignored
        assert all(s.angle != "C" for s in v.angle_stats)

    def test_groups_split_by_cohort_geo(self):
        rows = [
            _row("A", cohort="Coh1", geo="anglo", clicks=400),
            _row("B", cohort="Coh1", geo="anglo", clicks=100),
            _row("A", cohort="Coh2", geo="latam", clicks=400),
            _row("B", cohort="Coh2", geo="latam", clicks=100),
        ]
        verdicts = analyze_angles("GMR-X", rows=rows)
        assert len(verdicts) == 2
        keys = {(v.cohort_signature, v.geo_cluster) for v in verdicts}
        assert keys == {("Coh1", "anglo"), ("Coh2", "latam")}


class TestPickWinnerLosers:
    def _stat(self, angle, ctr, cpa=None, clicks=200):
        return AngleStat(angle, f"urn:{angle}", "linkedin", 10000, clicks, ctr, 100.0, cpa, True, {})

    def test_no_winner_when_flat(self):
        # three near-identical CTRs → no z-score winner, no losers
        stats = [self._stat("A", 2.0), self._stat("B", 2.05), self._stat("C", 1.95)]
        winner, losers = _pick_winner_losers(stats)
        assert winner is None and losers == []


class TestPlanAndMessage:
    def _decided_verdicts(self):
        rows = [_row("A", clicks=120), _row("B", clicks=400), _row("C", clicks=60)]
        return analyze_angles("GMR-X", rows=rows)

    def test_plan_changes_scale_refresh_pause(self):
        changes = plan_changes(self._decided_verdicts())
        kinds = {c.kind for c in changes}
        assert {"scale", "refresh", "pause"} <= kinds
        scale = next(c for c in changes if c.kind == "scale")
        assert scale.winning_angle == "B"
        pause = next(c for c in changes if c.kind == "pause")
        assert pause.losing_angle == "C" and pause.winning_angle == "B"

    def test_message_sections_and_pending(self):
        changes = plan_changes(self._decided_verdicts())
        msg = build_angle_change_message("GMR-X", changes, project_name="Proj", applied=False)
        assert "Started" in msg and "Pausing" in msg and "Scaling" in msg
        assert "pending your approval" in msg.lower()
        assert "GMR-X" in msg

    def test_message_empty_on_no_changes(self):
        assert build_angle_change_message("GMR-X", []) == ""

    def test_message_applied_wording(self):
        changes = plan_changes(self._decided_verdicts())
        msg = build_angle_change_message("GMR-X", changes, applied=True)
        assert "Applied" in msg


class TestActOnVerdicts:
    def test_recommend_path_persists_and_posts_no_execute(self):
        rows = [_row("A", clicks=120), _row("B", clicks=400), _row("C", clicks=60)]
        verdicts = analyze_angles("GMR-X", rows=rows)
        with patch("src.angle_performance._persist_change") as persist, \
             patch("src.angle_performance._post_slack") as slack, \
             patch("src.angle_performance._execute_changes") as execute:
            changes = act_on_verdicts(verdicts, ramp_id="GMR-X", auto_act=False)
        assert changes
        assert persist.call_count == len(changes)
        slack.assert_called_once()
        execute.assert_not_called()

    def test_auto_act_executes(self):
        rows = [_row("A", clicks=120), _row("B", clicks=400), _row("C", clicks=60)]
        verdicts = analyze_angles("GMR-X", rows=rows)
        with patch("src.angle_performance._persist_change"), \
             patch("src.angle_performance._post_slack"), \
             patch("src.angle_performance._execute_changes") as execute:
            act_on_verdicts(verdicts, ramp_id="GMR-X", auto_act=True)
        execute.assert_called_once()

    def test_no_changes_no_post(self):
        rows = [_row("A", clicks=400)]  # single angle → insufficient_data
        verdicts = analyze_angles("GMR-X", rows=rows)
        with patch("src.angle_performance._persist_change") as persist, \
             patch("src.angle_performance._post_slack") as slack:
            changes = act_on_verdicts(verdicts, ramp_id="GMR-X", auto_act=False)
        assert changes == []
        persist.assert_not_called()
        slack.assert_not_called()

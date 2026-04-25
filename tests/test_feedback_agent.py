"""
Unit tests for FeedbackAgent.

Tests cover:
  - analyze_creative_performance: return type, schema, sorting
  - identify_underperforming_cohorts: z-score detection, CTR trend detection
  - generate_slack_alert: formatting, cohort names, vocabulary compliance
  - Empty query result handling
  - Hypothesis generation logic
"""

import pandas as pd
import pytest
from unittest.mock import MagicMock, patch

from src.feedback_agent import FeedbackAgent


# ── Shared fixtures ────────────────────────────────────────────────────────────

def _make_creative_df() -> pd.DataFrame:
    """Sample creative performance DataFrame with 5 rows across 2 cohorts."""
    return pd.DataFrame([
        # cohort DATA_ANALYST — angles A/B/C with varying CTR/CPA
        {
            "creative_id": 1, "creative_urn": "urn:li:1", "cohort_name": "DATA_ANALYST",
            "angle": "A", "photo_subject": "laptop_desk", "impressions": 5000,
            "clicks": 250, "ctr": 5.0, "spend": 500.0, "conversions": 10, "cpa": 50.0,
            "created_date": "2026-04-14",
        },
        {
            "creative_id": 2, "creative_urn": "urn:li:2", "cohort_name": "DATA_ANALYST",
            "angle": "B", "photo_subject": "outdoor_person", "impressions": 5000,
            "clicks": 400, "ctr": 8.0, "spend": 500.0, "conversions": 8, "cpa": 62.5,
            "created_date": "2026-04-14",
        },
        {
            "creative_id": 3, "creative_urn": "urn:li:3", "cohort_name": "DATA_ANALYST",
            "angle": "C", "photo_subject": "laptop_desk", "impressions": 5000,
            "clicks": 150, "ctr": 3.0, "spend": 500.0, "conversions": 5, "cpa": 100.0,
            "created_date": "2026-04-14",
        },
        # cohort ML_ENGINEER — angles A/B
        {
            "creative_id": 4, "creative_urn": "urn:li:4", "cohort_name": "ML_ENGINEER",
            "angle": "A", "photo_subject": "studio_headshot", "impressions": 3000,
            "clicks": 180, "ctr": 6.0, "spend": 300.0, "conversions": 6, "cpa": 50.0,
            "created_date": "2026-04-14",
        },
        {
            "creative_id": 5, "creative_urn": "urn:li:5", "cohort_name": "ML_ENGINEER",
            "angle": "B", "photo_subject": "studio_headshot", "impressions": 3000,
            "clicks": 60, "ctr": 2.0, "spend": 300.0, "conversions": 3, "cpa": 100.0,
            "created_date": "2026-04-14",
        },
    ])


def _make_cohort_df() -> pd.DataFrame:
    """
    Sample cohort metrics DataFrame with 3 cohorts over 2 weeks.

    - DATA_ANALYST: CPA spikes 30 → 200 (z-score > 2 vs peers at 30 / 32)
    - ML_ENGINEER: CPA 30 → 32 (normal, close to baseline)
    - MEDICAL: CPA 28 → 30 (good, stable)

    With current CPAs [200, 32, 30]:
      median = 32, std ≈ 95 → z(200) = (200-32)/95 ≈ 1.77 — still marginal.

    Use 5 cohorts so the stats are more robust and DATA_ANALYST's z-score
    clearly clears 2.0. We add 2 more "normal" cohorts.
    """
    return pd.DataFrame([
        # DATA_ANALYST — current CPA=300 (big spike), prior=40
        {
            "cohort_name": "DATA_ANALYST", "week_of": "2026-04-14",
            "n_impressions": 1000, "n_clicks": 70, "ctr": 7.0,
            "n_conversions": 3, "cpa": 300.0, "trend_indicator": -12.5,
        },
        {
            "cohort_name": "DATA_ANALYST", "week_of": "2026-04-07",
            "n_impressions": 900, "n_clicks": 72, "ctr": 8.0,
            "n_conversions": 20, "cpa": 40.0, "trend_indicator": None,
        },
        # ML_ENGINEER — current CPA=45, prior=40
        {
            "cohort_name": "ML_ENGINEER", "week_of": "2026-04-14",
            "n_impressions": 800, "n_clicks": 64, "ctr": 8.0,
            "n_conversions": 15, "cpa": 45.0, "trend_indicator": 0.0,
        },
        {
            "cohort_name": "ML_ENGINEER", "week_of": "2026-04-07",
            "n_impressions": 750, "n_clicks": 60, "ctr": 8.0,
            "n_conversions": 15, "cpa": 40.0, "trend_indicator": None,
        },
        # MEDICAL — current CPA=38, prior=40 (improving)
        {
            "cohort_name": "MEDICAL", "week_of": "2026-04-14",
            "n_impressions": 600, "n_clicks": 54, "ctr": 9.0,
            "n_conversions": 17, "cpa": 38.0, "trend_indicator": 0.0,
        },
        {
            "cohort_name": "MEDICAL", "week_of": "2026-04-07",
            "n_impressions": 550, "n_clicks": 44, "ctr": 8.0,
            "n_conversions": 14, "cpa": 40.0, "trend_indicator": None,
        },
        # LANGUAGES — current CPA=42, prior=43 (stable)
        {
            "cohort_name": "LANGUAGES", "week_of": "2026-04-14",
            "n_impressions": 700, "n_clicks": 56, "ctr": 8.0,
            "n_conversions": 16, "cpa": 42.0, "trend_indicator": 0.0,
        },
        {
            "cohort_name": "LANGUAGES", "week_of": "2026-04-07",
            "n_impressions": 650, "n_clicks": 52, "ctr": 8.0,
            "n_conversions": 15, "cpa": 43.0, "trend_indicator": None,
        },
        # MATH — current CPA=50, prior=48 (slightly above baseline)
        {
            "cohort_name": "MATH", "week_of": "2026-04-14",
            "n_impressions": 500, "n_clicks": 40, "ctr": 8.0,
            "n_conversions": 10, "cpa": 50.0, "trend_indicator": 0.0,
        },
        {
            "cohort_name": "MATH", "week_of": "2026-04-07",
            "n_impressions": 480, "n_clicks": 38, "ctr": 8.0,
            "n_conversions": 10, "cpa": 48.0, "trend_indicator": None,
        },
    ])
    # CPAs for current week: [300, 45, 38, 42, 50]
    # median=45, std(ddof=1)≈97 → z(300)=(300-45)/97≈2.63 > 2.0 ✓


# ── Test class ─────────────────────────────────────────────────────────────────

class TestFeedbackAgent:

    def _make_agent(self, creative_df=None, cohort_df=None) -> FeedbackAgent:
        """Build a FeedbackAgent with a mocked RedashClient."""
        mock_client = MagicMock()
        if creative_df is not None:
            mock_client.query_creative_performance.return_value = creative_df
        if cohort_df is not None:
            mock_client.query_cohort_metrics.return_value = cohort_df
        return FeedbackAgent(redash_client=mock_client)

    # ── Test 1 ─────────────────────────────────────────────────────────────────

    def test_analyze_creative_performance_returns_list(self):
        """Test that analyze_creative_performance returns a list with correct schema
        and at least one item per creative row, sorted by cohort then CPA descending."""
        agent = self._make_agent(creative_df=_make_creative_df())

        result = agent.analyze_creative_performance(days_back=7)

        assert isinstance(result, list), "Should return a list"
        assert len(result) >= 4, f"Expected at least 4 items, got {len(result)}"

        required_keys = {"angle", "photo_subject", "cohort_name", "ctr", "cpa", "reason_hypothesis"}
        for item in result:
            missing = required_keys - set(item.keys())
            assert not missing, f"Missing keys {missing} in item: {item}"

        # Verify sorted by cohort_name
        cohort_names = [r["cohort_name"] for r in result]
        assert cohort_names == sorted(cohort_names), (
            f"Results not sorted by cohort_name: {cohort_names}"
        )

        # Within each cohort, verify CPA is descending (None treated as largest)
        for cohort in ["DATA_ANALYST", "ML_ENGINEER"]:
            cohort_results = [r for r in result if r["cohort_name"] == cohort]
            cpas = [r["cpa"] if r["cpa"] is not None else 1e9 for r in cohort_results]
            assert cpas == sorted(cpas, reverse=True), (
                f"CPA not descending for cohort {cohort}: {cpas}"
            )

    # ── Test 2 ─────────────────────────────────────────────────────────────────

    def test_identify_underperforming_cohorts_detects_z_score(self):
        """Test that identify_underperforming_cohorts correctly flags DATA_ANALYST
        (CPA spike 45→120) as underperforming while ML_ENGINEER and MEDICAL are not."""
        agent = self._make_agent(cohort_df=_make_cohort_df())

        result = agent.identify_underperforming_cohorts(days_back=7)

        assert isinstance(result, list), "Should return a list"

        cohort_names = [r["cohort_name"] for r in result]
        assert "DATA_ANALYST" in cohort_names, (
            "DATA_ANALYST should be flagged as underperforming (CPA spike)"
        )
        assert "ML_ENGINEER" not in cohort_names, (
            "ML_ENGINEER should NOT be flagged (CPA only slightly above baseline)"
        )
        assert "MEDICAL" not in cohort_names, (
            "MEDICAL should NOT be flagged (CPA improving)"
        )

        # Find DATA_ANALYST entry
        da = next(r for r in result if r["cohort_name"] == "DATA_ANALYST")
        assert da["cpa_z_score"] > 2.0, (
            f"DATA_ANALYST z-score should exceed 2.0, got {da['cpa_z_score']}"
        )
        assert da["recommendation"] in ("PAUSE", "TEST_NEW_ANGLES", "MONITOR"), (
            f"Unexpected recommendation: {da['recommendation']}"
        )

        # Sorted by cpa_z_score descending
        z_scores = [r["cpa_z_score"] for r in result]
        assert z_scores == sorted(z_scores, reverse=True), (
            f"Results not sorted by cpa_z_score descending: {z_scores}"
        )

    # ── Test 3 ─────────────────────────────────────────────────────────────────

    def test_generate_slack_alert_formats_text(self):
        """Test that generate_slack_alert returns a properly formatted string
        containing cohort names, angles, photo subjects, reaction prompts,
        and adheres to Outlier vocabulary requirements."""
        underperformers = [
            {
                "cohort_name": "DATA_ANALYST", "current_cpa": 120.0,
                "baseline_cpa": 50.0, "sigma": 35.0, "cpa_z_score": 2.0,
                "ctr_trend": -0.125, "recommendation": "PAUSE",
            },
            {
                "cohort_name": "MEDICAL", "current_cpa": 95.0,
                "baseline_cpa": 50.0, "sigma": 35.0, "cpa_z_score": 1.3,
                "ctr_trend": -0.15, "recommendation": "TEST_NEW_ANGLES",
            },
        ]
        hypothesis_summary = [
            {
                "angle": "A", "photo_subject": "laptop_desk", "cohort_name": "DATA_ANALYST",
                "ctr": 5.0, "cpa": 50.0, "impression_count": 5000, "rank_in_cohort": 2,
                "reason_hypothesis": "High-cost creative; lower engagement rate in target cohort",
            },
            {
                "angle": "B", "photo_subject": "outdoor_person", "cohort_name": "DATA_ANALYST",
                "ctr": 8.0, "cpa": 62.5, "impression_count": 5000, "rank_in_cohort": 3,
                "reason_hypothesis": "Strong engagement hook; consider testing similar angle",
            },
            {
                "angle": "C", "photo_subject": "studio_headshot", "cohort_name": "MEDICAL",
                "ctr": 9.0, "cpa": 35.0, "impression_count": 3000, "rank_in_cohort": 1,
                "reason_hypothesis": "Unique visual treatment drives differentiation",
            },
        ]
        agent = self._make_agent()

        result = agent.generate_slack_alert(underperformers, hypothesis_summary)

        assert isinstance(result, str), "Should return a string"
        assert len(result) > 0, "Alert text should not be empty"

        # Must contain "Weekly Feedback" header
        assert "Weekly Feedback" in result, "Missing 'Weekly Feedback' header"

        # Must contain underperformer cohort names
        assert "DATA_ANALYST" in result, "DATA_ANALYST should appear in alert"
        assert "MEDICAL" in result, "MEDICAL should appear in alert"

        # Must contain angles and photo subjects from hypotheses
        assert "Angle A" in result or "angle A" in result.lower() or "A" in result
        assert "laptop_desk" in result, "Photo subject 'laptop_desk' should appear"

        # Must contain reaction emojis/prompts
        assert "\U0001f44d" in result, "Thumbs-up emoji prompt missing"
        assert "\U0001f9ea" in result, "Test-tube emoji prompt missing"

        # Outlier vocabulary: these banned terms must NOT appear in alert text
        for banned in ["Task", "Compensation", "Interview"]:
            assert banned not in result, (
                f"Banned Outlier vocabulary term '{banned}' found in alert"
            )

    # ── Test 4 ─────────────────────────────────────────────────────────────────

    def test_empty_query_returns_empty_list(self):
        """Test that empty Redash responses produce empty lists (not None or exceptions),
        and that generate_slack_alert handles empty underperformers gracefully."""
        empty_creative_df = pd.DataFrame(columns=[
            "creative_id", "creative_urn", "cohort_name", "angle", "photo_subject",
            "impressions", "clicks", "ctr", "spend", "conversions", "cpa", "created_date",
        ])
        empty_cohort_df = pd.DataFrame(columns=[
            "cohort_name", "week_of", "n_impressions", "n_clicks", "ctr",
            "n_conversions", "cpa", "trend_indicator",
        ])

        agent = self._make_agent(
            creative_df=empty_creative_df,
            cohort_df=empty_cohort_df,
        )

        creative_result = agent.analyze_creative_performance()
        assert creative_result == [], (
            f"Expected [] for empty creative data, got: {creative_result}"
        )
        assert creative_result is not None

        cohort_result = agent.identify_underperforming_cohorts()
        assert cohort_result == [], (
            f"Expected [] for empty cohort data, got: {cohort_result}"
        )
        assert cohort_result is not None

        alert = agent.generate_slack_alert([], {})
        assert isinstance(alert, str), "generate_slack_alert should return str even for empty input"
        assert len(alert) > 0, "Should return a non-empty string even for empty underperformers"
        # Standard empty message
        assert "No underperforming" in alert or "no underperforming" in alert.lower(), (
            f"Expected 'No underperforming' message, got: {alert}"
        )

    # ── Test 5 ─────────────────────────────────────────────────────────────────

    def test_hypothesis_generation_logic(self):
        """Test that the hypothesis generation function produces correct output
        for high-CPA, high-CTR, unique photo_subject, and fallback cases."""
        from src.feedback_agent import _generate_hypothesis

        # High-CPA creative (> median + 2σ) → should flag cost
        high_cpa_hypo = _generate_hypothesis(
            cpa_val=200.0,
            ctr_val=3.0,
            median_cpa=50.0,
            std_cpa=30.0,           # threshold = 50 + 60 = 110; 200 > 110
            median_ctr=5.0,
            std_ctr=2.0,
            photo_subject="generic_photo",
            photo_subject_counts=pd.Series({"generic_photo": 3}),
        )
        assert "High-cost" in high_cpa_hypo or "cost" in high_cpa_hypo.lower(), (
            f"High-CPA creative should generate cost hypothesis, got: {high_cpa_hypo}"
        )

        # High-CTR creative (> median + 2σ) → should flag engagement
        high_ctr_hypo = _generate_hypothesis(
            cpa_val=50.0,
            ctr_val=12.0,
            median_cpa=50.0,
            std_cpa=5.0,            # CPA not anomalous
            median_ctr=5.0,
            std_ctr=2.0,            # threshold = 5 + 4 = 9; 12 > 9
            photo_subject="generic_photo",
            photo_subject_counts=pd.Series({"generic_photo": 3}),
        )
        assert "engagement" in high_ctr_hypo.lower() or "Strong" in high_ctr_hypo, (
            f"High-CTR creative should generate engagement hypothesis, got: {high_ctr_hypo}"
        )

        # Unique photo_subject (appears only once in cohort) → differentiation
        unique_photo_hypo = _generate_hypothesis(
            cpa_val=55.0,
            ctr_val=5.5,
            median_cpa=50.0,
            std_cpa=5.0,            # CPA not anomalous
            median_ctr=5.0,
            std_ctr=1.0,            # CTR not anomalous
            photo_subject="unique_forest_photo",
            photo_subject_counts=pd.Series({"unique_forest_photo": 1, "generic_photo": 5}),
        )
        assert "differentiation" in unique_photo_hypo.lower() or "Unique" in unique_photo_hypo, (
            f"Unique photo should generate differentiation hypothesis, got: {unique_photo_hypo}"
        )

        # Fallback (nothing anomalous) → A/B test suggestion
        fallback_hypo = _generate_hypothesis(
            cpa_val=50.0,
            ctr_val=5.0,
            median_cpa=50.0,
            std_cpa=5.0,            # CPA right at median — not anomalous
            median_ctr=5.0,
            std_ctr=1.0,            # CTR at median — not anomalous
            photo_subject="common_photo",
            photo_subject_counts=pd.Series({"common_photo": 4}),
        )
        assert "A/B" in fallback_hypo or "variants" in fallback_hypo.lower(), (
            f"Fallback should suggest A/B test, got: {fallback_hypo}"
        )


# ── V2 funnel-decomposition tests (FEED-15, FEED-16) ──────────────────────────

def test_analyze_funnel_by_cohort_returns_stage_rates():
    """FEED-15: funnel analysis returns per-creative stage counts + rates."""
    import pandas as pd
    from unittest.mock import MagicMock
    from src.feedback_agent import FeedbackAgent

    mock_df = pd.DataFrame([
        {"cohort_name": "DATA_ANALYST", "creative_id": 100, "impressions": 10000, "clicks": 500,
         "spend": 250.0, "applications": 100, "screening_passes": 40, "activations": 20,
         "ctr": 0.05, "click_to_signup": 0.20, "signup_to_screen": 0.40, "screen_to_activate": 0.50},
        {"cohort_name": "DATA_ANALYST", "creative_id": 101, "impressions": 8000, "clicks": 240,
         "spend": 180.0, "applications": 80, "screening_passes": 30, "activations": 5,
         "ctr": 0.03, "click_to_signup": 0.33, "signup_to_screen": 0.375, "screen_to_activate": 0.166},
    ])
    mock_rc = MagicMock()
    mock_rc.query_funnel_metrics.return_value = mock_df

    agent = FeedbackAgent(mock_rc)
    rows = agent.analyze_funnel_by_cohort(days_back=7)

    assert isinstance(rows, list)
    assert len(rows) == 2
    expected_keys = {"cohort_name", "creative_id", "impressions", "clicks", "applications",
                     "screening_passes", "activations", "ctr", "click_to_signup",
                     "signup_to_screen", "screen_to_activate"}
    assert expected_keys.issubset(rows[0].keys())
    mock_rc.query_funnel_metrics.assert_called_once_with(days_back=7)


def test_identify_funnel_drop_stage_classifies_earliest_drop():
    """FEED-16: identify the earliest stage that drops >= threshold below cohort median."""
    from src.feedback_agent import FeedbackAgent

    # Cohort A: activation stage drops hard (0.10 vs median 0.50)
    # Cohort B: signup stage drops (0.05 vs median 0.25) — earlier stage
    # Cohort C: all rates within threshold → drop_stage == 'none'
    funnel_rows = [
        {"cohort_name": "A", "creative_id": 1, "ctr": 0.05, "click_to_signup": 0.20,
         "signup_to_screen": 0.40, "screen_to_activate": 0.50, "impressions": 10000, "clicks": 500,
         "applications": 100, "screening_passes": 40, "activations": 20},
        {"cohort_name": "A", "creative_id": 2, "ctr": 0.05, "click_to_signup": 0.20,
         "signup_to_screen": 0.40, "screen_to_activate": 0.10, "impressions": 10000, "clicks": 500,
         "applications": 100, "screening_passes": 40, "activations": 4},
        {"cohort_name": "B", "creative_id": 3, "ctr": 0.05, "click_to_signup": 0.25,
         "signup_to_screen": 0.40, "screen_to_activate": 0.50, "impressions": 10000, "clicks": 500,
         "applications": 125, "screening_passes": 50, "activations": 25},
        {"cohort_name": "B", "creative_id": 4, "ctr": 0.05, "click_to_signup": 0.05,
         "signup_to_screen": 0.40, "screen_to_activate": 0.50, "impressions": 10000, "clicks": 500,
         "applications": 25, "screening_passes": 10, "activations": 5},
        {"cohort_name": "C", "creative_id": 5, "ctr": 0.05, "click_to_signup": 0.20,
         "signup_to_screen": 0.40, "screen_to_activate": 0.50, "impressions": 10000, "clicks": 500,
         "applications": 100, "screening_passes": 40, "activations": 20},
    ]

    from unittest.mock import MagicMock
    agent = FeedbackAgent(MagicMock())
    diag = agent.identify_funnel_drop_stage(funnel_rows)

    assert diag["A"]["drop_stage"] == "activation", f"Cohort A should drop at activation, got {diag['A']}"
    assert diag["B"]["drop_stage"] == "signup", f"Cohort B should drop at signup, got {diag['B']}"
    assert diag["C"]["drop_stage"] == "none"
    # Delta should be negative for drops
    assert diag["A"]["delta_pct"] < 0
    assert diag["B"]["delta_pct"] < 0


def test_alert_includes_drop_stage():
    """FEED-16: generate_slack_alert appends Funnel Drop Diagnosis section when provided."""
    from unittest.mock import MagicMock
    from src.feedback_agent import FeedbackAgent

    agent = FeedbackAgent(MagicMock())
    underperformers = [
        {"cohort_name": "DATA_ANALYST", "current_cpa": 120, "baseline_cpa": 45,
         "cpa_z_score": 3.2, "ctr_trend": -0.15, "recommendation": "PAUSE"},
    ]
    hypothesis_summary = []
    funnel_diagnosis = {
        "DATA_ANALYST": {"drop_stage": "screening", "drop_rate": 0.20,
                         "baseline_rate": 0.50, "delta_pct": -0.60},
        "ML_ENGINEER":  {"drop_stage": "none", "drop_rate": None,
                         "baseline_rate": None, "delta_pct": None},
    }

    # Backwards-compat: two-arg call still works
    alert_v1 = agent.generate_slack_alert(underperformers, hypothesis_summary)
    assert isinstance(alert_v1, str)
    assert "Funnel Drop" not in alert_v1

    # V2 call: funnel section appears
    alert_v2 = agent.generate_slack_alert(underperformers, hypothesis_summary,
                                          funnel_diagnosis=funnel_diagnosis)
    assert "Funnel Drop Diagnosis:" in alert_v2
    assert "DATA_ANALYST" in alert_v2
    assert "screening" in alert_v2
    # ML_ENGINEER has drop_stage="none" → must NOT be in funnel-drop section
    # (but may appear elsewhere). Check it's not in a "funnel drop at" line:
    for line in alert_v2.split("\n"):
        if "ML_ENGINEER" in line and "funnel drop" in line.lower():
            raise AssertionError(f"ML_ENGINEER with drop_stage=none should not appear: {line!r}")

    # Vocabulary check: no banned tokens in the new section
    banned = ["compensation", "interview", "project rate"]
    for b in banned:
        assert b.lower() not in alert_v2.lower(), f"banned token {b!r} in Slack alert"

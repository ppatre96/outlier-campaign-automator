"""
Tests for replacement angle generation — _next_angle_label, _generate_replacement_copy,
and _launch_replacement_campaign.
"""
import json
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from src.campaign_feedback_agent import (
    _next_angle_label,
    _generate_replacement_copy,
    _launch_replacement_campaign,
    _HOOK_ROTATION,
)


# ── _next_angle_label ──────────────────────────────────────────────────────────

def test_next_angle_first_replacement():
    assert _next_angle_label(["A", "B", "C"]) == "D"


def test_next_angle_skips_used():
    assert _next_angle_label(["A", "B", "C", "D"]) == "E"


def test_next_angle_empty():
    assert _next_angle_label([]) == "D"


def test_next_angle_case_insensitive():
    assert _next_angle_label(["a", "b", "c"]) == "D"


def test_next_angle_gaps_not_filled():
    # If D is missing but E/F exist, still returns D (no gap-fill, just linear scan)
    assert _next_angle_label(["A", "B", "C", "E", "F"]) == "D"


# ── _HOOK_ROTATION ─────────────────────────────────────────────────────────────

def test_hook_rotation_learning_growth():
    assert _HOOK_ROTATION["learning_growth"] == "expert_identity"


def test_hook_rotation_expert_identity():
    assert _HOOK_ROTATION["expert_identity"] == "community_identity"


def test_hook_rotation_unknown():
    assert _HOOK_ROTATION["unknown"] == "expert_identity"


# ── _generate_replacement_copy ─────────────────────────────────────────────────

@patch("src.campaign_feedback_agent.call_claude")
def test_generate_replacement_copy_success(mock_call):
    mock_call.return_value = json.dumps({
        "headline": "Your ML skills matter",
        "subheadline": "Earn $700-1100 weekly on your schedule",
        "photo_subject": "ML engineer at dual-monitor desk",
        "cta": "Apply Now",
    })

    result = _generate_replacement_copy(
        cohort_name="ML Engineers",
        old_headline="Grow your AI knowledge",
        old_subheadline="Learn while you earn",
        problems=["learning_growth hook underperforms for senior TGs"],
        winner_entry={"headline": "You know ML already", "subheadline": "Put it to work", "ctr_pct": 0.42},
        rate="$50/hr",
        geo_label="South Asian",
        old_hook="learning_growth",
    )

    assert result is not None
    assert result["headline"] == "Your ML skills matter"
    assert "cta" in result

    # Prompt should have been called with replacement context
    call_args = mock_call.call_args
    prompt_text = call_args[1]["messages"][0]["content"]
    assert "expert_identity" in prompt_text
    assert "learning_growth" in prompt_text  # the OLD hook is mentioned


@patch("src.campaign_feedback_agent.call_claude")
def test_generate_replacement_copy_malformed_json(mock_call):
    mock_call.return_value = "not json at all"

    result = _generate_replacement_copy(
        cohort_name="Data Scientists",
        old_headline="Learn and grow",
        old_subheadline="Build your skills",
        problems=["hook underperformed"],
        winner_entry=None,
        rate="$45/hr",
        geo_label="Global",
    )

    assert result is None


@patch("src.campaign_feedback_agent.call_claude")
def test_generate_replacement_copy_no_winner(mock_call):
    mock_call.return_value = json.dumps({
        "headline": "Python pros wanted",
        "subheadline": "Earn $700/week remote",
        "photo_subject": "Python developer at desk",
        "cta": "Apply Now",
    })

    result = _generate_replacement_copy(
        cohort_name="Python Engineers",
        old_headline="Old headline",
        old_subheadline="Old subheadline",
        problems=[],
        winner_entry=None,  # no winner yet
        rate="$50/hr",
        geo_label="Anglo",
    )

    assert result is not None
    prompt_text = mock_call.call_args[1]["messages"][0]["content"]
    assert "Not yet established" in prompt_text


# ── _launch_replacement_campaign ───────────────────────────────────────────────

def _make_deprecated_entry(**overrides):
    base = {
        "smart_ramp_id": "ramp_001",
        "cohort_id": "cohort_42",
        "cohort_signature": "Python Engineers | 5+ YOE",
        "geo_cluster": "south_asian",
        "geo_cluster_label": "South Asian",
        "geos": "IN, PK",
        "angle": "B",
        "campaign_type": "static",
        "advertised_rate": "$45/hr",
        "linkedin_campaign_urn": "urn:li:sponsoredCampaign:999",
        "headline": "Grow your AI skills",
        "subheadline": "Work remote on your schedule",
        "photo_subject": "Person at desk",
        "status": "deprecated",
    }
    base.update(overrides)
    return base


def test_launch_skips_inmail():
    entry = _make_deprecated_entry(campaign_type="inmail")
    result = _launch_replacement_campaign(entry, {}, MagicMock())
    assert result is None


@patch("src.campaign_registry.get_cohort_entries")
def test_launch_skips_when_replacement_exists(mock_get):
    mock_get.return_value = [
        {"angle": "A", "status": "active", "ctr_pct": 0.3},
        {"angle": "B", "status": "deprecated", "ctr_pct": 0.1},
        {"angle": "D", "status": "active", "ctr_pct": 0.2},  # replacement exists
    ]
    result = _launch_replacement_campaign(_make_deprecated_entry(), {}, MagicMock())
    assert result is None


@patch("src.campaign_feedback_agent._generate_replacement_copy")
@patch("src.campaign_registry.get_cohort_entries")
def test_launch_aborts_on_empty_copy(mock_get, mock_copy):
    mock_get.return_value = [
        {"angle": "A", "status": "active", "ctr_pct": 0.3},
        {"angle": "B", "status": "deprecated", "ctr_pct": 0.1},
    ]
    mock_copy.return_value = None  # copy gen failed

    result = _launch_replacement_campaign(_make_deprecated_entry(), {}, MagicMock())
    assert result is None


@patch("src.campaign_registry.log_campaign")
@patch("src.gemini_creative.generate_imagen_creative_with_qc")
@patch("src.campaign_feedback_agent._generate_replacement_copy")
@patch("src.campaign_registry.get_cohort_entries")
def test_launch_success(mock_get, mock_copy, mock_imagen, mock_reg_log):
    mock_get.return_value = [
        {"angle": "A", "status": "active", "ctr_pct": 0.35},
        {"angle": "B", "status": "deprecated", "ctr_pct": 0.05},
    ]
    mock_copy.return_value = {
        "headline": "Your ML expertise pays",
        "subheadline": "Earn $700/week remote",
        "photo_subject": "ML engineer at desk",
        "cta": "Apply Now",
    }
    fake_png = Path("/tmp/test_replacement.png")
    fake_png.write_bytes(b"fake-png-data")
    mock_imagen.return_value = (fake_png, {"verdict": "PASS"})

    li_mock = MagicMock()
    li_mock.clone_campaign.return_value = "urn:li:sponsoredCampaign:1234"
    li_mock.upload_image.return_value = "urn:li:image:abc"
    creative_result = MagicMock()
    creative_result.status = "ok"
    creative_result.creative_urn = "urn:li:sponsoredCreative:999"
    li_mock.create_image_ad.return_value = creative_result

    result = _launch_replacement_campaign(
        _make_deprecated_entry(), {"problems": ["CTR too low"]}, li_mock
    )

    assert result == "urn:li:sponsoredCampaign:1234"
    li_mock.clone_campaign.assert_called_once_with(
        "urn:li:sponsoredCampaign:999",
        "Python Engineers | 5+ YOE [South Asian] D",
    )
    li_mock.upload_image.assert_called_once_with(fake_png)
    mock_reg_log.assert_called_once()

    fake_png.unlink(missing_ok=True)


@patch("src.campaign_registry.log_campaign")
@patch("src.gemini_creative.generate_imagen_creative_with_qc")
@patch("src.campaign_feedback_agent._generate_replacement_copy")
@patch("src.campaign_registry.get_cohort_entries")
def test_launch_proceeds_without_image(mock_get, mock_copy, mock_imagen, mock_reg_log):
    """Campaign should still be created even when image gen fails."""
    mock_get.return_value = [{"angle": "C", "status": "deprecated", "ctr_pct": 0.04}]
    mock_copy.return_value = {
        "headline": "Real engineers wanted",
        "subheadline": "Earn on your schedule",
        "photo_subject": "Engineer at workspace",
        "cta": "Apply Now",
    }
    mock_imagen.side_effect = RuntimeError("Gemini quota exceeded")

    li_mock = MagicMock()
    li_mock.clone_campaign.return_value = "urn:li:sponsoredCampaign:5678"

    result = _launch_replacement_campaign(_make_deprecated_entry(), {}, li_mock)

    assert result == "urn:li:sponsoredCampaign:5678"
    li_mock.upload_image.assert_not_called()
    mock_reg_log.assert_called_once()

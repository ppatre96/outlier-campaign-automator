"""Unit tests for src.task_card — grounded task extraction + prompt block."""
import os
import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from unittest.mock import patch

from src.task_card import (
    TaskCard,
    build_task_card,
    task_card_prompt_block,
    scrape_lp_text,
)

# Real-world grounding from GMR-0024 (BLV): Android/TalkBack + screen recording.
BLV_CTX = dict(
    job_post_name="Bring Your Perspective to AI Training — Blind & Low Vision",
    ramp_summary="6-10 legally blind contributors (US-based, Android/TalkBack users) for a BLV AI accessibility evaluation, screen recording.",
    cohort_description="Legally blind US-based Android/TalkBack users for BLV AI accessibility evaluation",
)


class TestBuildTaskCard:
    def test_grounded_extraction(self):
        with patch("src.task_card.call_claude") as m:
            m.return_value = (
                '{"what_you_do": "record your screen while using an AI assistant with TalkBack and flag where it fails",'
                ' "device_or_tool": "Android phone with TalkBack", "output_artifact": "short screen recording",'
                ' "time_per_task": "", "who_its_for": "legally blind / low-vision people"}'
            )
            card = build_task_card(lp_text="...task page...", **BLV_CTX)
        assert card.is_thin is False
        assert "TalkBack" in card.device_or_tool
        assert card.output_artifact == "short screen recording"
        assert card.source == "lp+smart_ramp"
        # The prompt sent to the model must carry the no-invention rule.
        sent = m.call_args.kwargs["messages"][0]["content"]
        assert "Do NOT infer, guess, or invent" in sent
        assert "TalkBack" in sent  # grounding context was passed

    def test_thin_when_no_grounding_skips_llm(self):
        with patch("src.task_card.call_claude") as m:
            card = build_task_card(lp_text="", job_post_name="", ramp_summary="", cohort_description="")
        m.assert_not_called()
        assert card.is_thin is True
        assert card.source == "none"

    def test_thin_when_llm_returns_no_concrete_task(self):
        with patch("src.task_card.call_claude") as m:
            # action but no device/artifact → still thin (not enough to be specific)
            m.return_value = '{"what_you_do": "help with AI", "device_or_tool": "", "output_artifact": "", "time_per_task": "", "who_its_for": "experts"}'
            card = build_task_card(lp_text="some page text here that is long enough", **BLV_CTX)
        assert card.is_thin is True

    def test_llm_failure_stays_general(self):
        with patch("src.task_card.call_claude", side_effect=RuntimeError("boom")):
            card = build_task_card(lp_text="long enough grounding text here", **BLV_CTX)
        assert card.is_thin is True

    def test_scrape_called_when_no_lp_text_passed(self):
        with patch("src.task_card.scrape_lp_text", return_value="") as scrape, \
             patch("src.task_card.call_claude") as m:
            m.return_value = '{"what_you_do":"x","device_or_tool":"y","output_artifact":"z","time_per_task":"","who_its_for":"w"}'
            build_task_card(lp_url="https://outlier.ai/experts/blv", **BLV_CTX)
        scrape.assert_called_once()


class TestPromptBlock:
    def test_none_card_empty(self):
        assert task_card_prompt_block(None) == ""

    def test_thin_block_stays_general_and_forbids_invention(self):
        block = task_card_prompt_block(TaskCard(is_thin=True))
        assert "stay general" in block.lower()
        assert "DO NOT invent" in block

    def test_grounded_block_lists_facts_and_forbids_extras(self):
        card = TaskCard(
            what_you_do="flag where the AI fails for blind users",
            device_or_tool="Android phone with TalkBack",
            output_artifact="short screen recording",
            is_thin=False, source="lp+smart_ramp",
        )
        block = task_card_prompt_block(card)
        assert "Android phone with TalkBack" in block
        assert "short screen recording" in block
        assert "invent nothing" in block.lower() or "Do NOT introduce" in block


class TestScrape:
    def test_non_http_returns_empty(self):
        assert scrape_lp_text("") == ""
        assert scrape_lp_text(None) == ""
        assert scrape_lp_text("not-a-url") == ""

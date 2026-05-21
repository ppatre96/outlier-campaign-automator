"""Unit tests for src.brief_generator — Phase 1 (build_briefs) +
Phase 2 (build_copy_from_brief) of the brief-review gate."""
from __future__ import annotations

import json
import os
import sys
from unittest.mock import patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.brief_generator import build_briefs, build_copy_from_brief


class _FakeCohort:
    """Minimal cohort stand-in. brief_generator only reads .name + .rules."""
    def __init__(self, name="dna_researcher", rules=None):
        self.name = name
        self.rules = rules or [
            ("skills__metagenomics", 1.0),
            ("highest_degree_level__PhD", 1.0),
            ("fields_of_study__Biology", 0.5),
        ]


_BRIEF_JSON_RESPONSE = """```json
{
  "briefs": [
    {
      "angle": "A",
      "angle_hook": "Lead with the niche skill, pivot to AI-review use",
      "headline_direction": "Name metagenomics skill, frame AI value",
      "subheadline_direction": "Earnings + flexibility, no number",
      "photo_direction": "male European DNA researcher, looking up from a printed gel image",
      "tone": "medium creative liberty - clear + credible",
      "proof_points": ["paid hourly", "remote"],
      "language_hint": "en-US",
      "competitor_signal": "",
      "must_include": ["AI training"],
      "must_avoid": ["training", "job"]
    },
    {
      "angle": "B",
      "angle_hook": "Earnings + peer social proof",
      "headline_direction": "Name peer count + earnings cadence",
      "subheadline_direction": "Reinforce credibility",
      "photo_direction": "male European DNA researcher, writing in a paper notebook at a window-lit desk",
      "tone": "medium",
      "proof_points": ["500+ researchers", "$45/hr"],
      "language_hint": "en-US",
      "competitor_signal": "Mercor April 2026 breach",
      "must_include": ["paid"],
      "must_avoid": ["training"]
    },
    {
      "angle": "C",
      "angle_hook": "Lab schedule frame -> freedom",
      "headline_direction": "Name external schedule constraint",
      "subheadline_direction": "Low-friction income claim",
      "photo_direction": "male European DNA researcher, closing a notebook at a sunlit window desk",
      "tone": "medium",
      "proof_points": ["work between projects"],
      "language_hint": "en-US",
      "competitor_signal": "",
      "must_include": [],
      "must_avoid": ["job"]
    }
  ]
}
```"""


_PHASE2_JSON_RESPONSE = """```json
{
  "angle": "A",
  "angleLabel": "Expertise Hook",
  "headline": "AI needs your metagenomics eye",
  "subheadline": "Paid hourly. Remote. Set your hours.",
  "intro_text": "Between sequencing runs? Help AI parse the metagenomics it cant.",
  "ad_headline": "Score AI metagenomics output, get paid hourly",
  "ad_description": "Remote, no commute, paid hourly.",
  "cta_button": "APPLY",
  "photo_subject": "male European DNA researcher, looking up from a printed gel image",
  "rationale": "Anchors on metagenomics niche skill which Outlier owns vs broad-skill competitors.",
  "competitor_signal": "",
  "layerUpdates": {}
}
```"""


class TestBuildBriefs:
    def test_returns_three_normalized_briefs(self):
        with patch("src.brief_generator.call_claude", return_value=_BRIEF_JSON_RESPONSE):
            briefs = build_briefs(_FakeCohort())
        assert len(briefs) == 3
        angles = [b["angle"] for b in briefs]
        assert angles == ["A", "B", "C"]
        for b in briefs:
            # Every canonical key must exist after normalization.
            for k in ("angle", "angle_hook", "headline_direction",
                      "subheadline_direction", "photo_direction", "tone",
                      "proof_points", "language_hint", "competitor_signal",
                      "must_include", "must_avoid"):
                assert k in b
            assert isinstance(b["proof_points"], list)
            assert isinstance(b["must_include"], list)
            assert isinstance(b["must_avoid"], list)

    def test_empty_response_returns_empty_list(self):
        with patch("src.brief_generator.call_claude", return_value="not json"):
            briefs = build_briefs(_FakeCohort())
        assert briefs == []

    def test_uses_pay_rate_block_when_provided(self):
        captured = {}

        def _spy(messages, **kwargs):
            captured["prompt"] = messages[0]["content"]
            return _BRIEF_JSON_RESPONSE

        with patch("src.brief_generator.call_claude", side_effect=_spy):
            build_briefs(_FakeCohort(), hourly_rate="$45/hr")
        assert "$45/hr" in captured["prompt"]
        assert "UNRESOLVED" not in captured["prompt"]

    def test_unresolved_pay_rate_locks_brief_against_dollar_claims(self):
        captured = {}

        def _spy(messages, **kwargs):
            captured["prompt"] = messages[0]["content"]
            return _BRIEF_JSON_RESPONSE

        with patch("src.brief_generator.call_claude", side_effect=_spy):
            build_briefs(_FakeCohort(), hourly_rate="")
        # When no $/hr is set, the prompt must explicitly forbid earnings claims.
        assert "UNRESOLVED" in captured["prompt"]
        assert "MUST NOT propose earnings claims" in captured["prompt"]


class TestBuildCopyFromBrief:
    _BRIEF = {
        "angle": "A",
        "angle_hook": "Lead with metagenomics",
        "headline_direction": "Name niche skill + AI value",
        "subheadline_direction": "earnings cadence + flexibility",
        "photo_direction": "male European DNA researcher, looking up from a printed gel image",
        "tone": "medium",
        "proof_points": ["paid hourly", "remote"],
        "language_hint": "en-US",
        "competitor_signal": "",
        "must_include": [],
        "must_avoid": ["training"],
    }
    _LAYER_MAP = {"layer1": "old headline", "layer2": "old sub"}

    def test_phase2_produces_variant_schema(self):
        with patch("src.brief_generator.call_claude", return_value=_PHASE2_JSON_RESPONSE):
            v = build_copy_from_brief(
                self._BRIEF, layer_map=self._LAYER_MAP, cohort=_FakeCohort(),
                geos=["US"], hourly_rate="$45/hr", reviewer_comment="",
            )
        for k in ("angle", "angleLabel", "headline", "subheadline", "intro_text",
                  "ad_headline", "ad_description", "cta_button", "photo_subject",
                  "rationale", "competitor_signal", "layerUpdates"):
            assert k in v, f"missing field {k}"
        assert v["cta_button"] == "APPLY"
        assert v["angle"] == "A"

    def test_reviewer_comment_appears_in_phase2_prompt(self):
        comment = "Reviewer says: lead with the lab-bench scene, not the headline metric"
        captured_prompts: list[str] = []

        def _spy(messages, **kwargs):
            captured_prompts.append(messages[0]["content"])
            return _PHASE2_JSON_RESPONSE

        with patch("src.brief_generator.call_claude", side_effect=_spy):
            build_copy_from_brief(
                self._BRIEF, layer_map=self._LAYER_MAP, cohort=_FakeCohort(),
                geos=["US"], hourly_rate="$45/hr", reviewer_comment=comment,
            )
        assert any(comment in p for p in captured_prompts), \
            "reviewer comment must appear verbatim in the Phase-2 prompt"
        assert any("HARD CONSTRAINT" in p for p in captured_prompts), \
            "reviewer comment must be framed as a hard constraint"

    def test_empty_reviewer_comment_does_not_add_reviewer_block(self):
        captured: list[str] = []

        def _spy(messages, **kwargs):
            captured.append(messages[0]["content"])
            return _PHASE2_JSON_RESPONSE

        with patch("src.brief_generator.call_claude", side_effect=_spy):
            build_copy_from_brief(
                self._BRIEF, layer_map=self._LAYER_MAP, cohort=_FakeCohort(),
                geos=["US"], hourly_rate="$45/hr", reviewer_comment="",
            )
        # No HARD CONSTRAINT block should be appended when comment is empty.
        assert not any("HARD CONSTRAINT" in p for p in captured)

    def test_unparseable_phase2_response_returns_empty_dict(self):
        with patch("src.brief_generator.call_claude", return_value="garbage"):
            v = build_copy_from_brief(
                self._BRIEF, layer_map=self._LAYER_MAP, cohort=_FakeCohort(),
                geos=["US"], hourly_rate="$45/hr", reviewer_comment="",
            )
        assert v == {}

    def test_competitor_signal_carried_through_from_brief(self):
        brief = dict(self._BRIEF, competitor_signal="Surge Q1 layoff narrative")
        # Phase 2 response omits competitor_signal — code should backfill from brief.
        resp_without_signal = json.dumps({**json.loads(_PHASE2_JSON_RESPONSE.strip("` json\n")), "competitor_signal": ""})
        with patch("src.brief_generator.call_claude", return_value=resp_without_signal):
            v = build_copy_from_brief(
                brief, layer_map=self._LAYER_MAP, cohort=_FakeCohort(),
                geos=["US"], hourly_rate="$45/hr",
            )
        assert v.get("competitor_signal") == "Surge Q1 layoff narrative"

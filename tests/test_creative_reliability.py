"""Tests for the 3 reliability fixes in the creative-generation path.

1. `generate_imagen_creative` raising (e.g. "Gemini returned no image")
   must be caught INSIDE the retry loop in `generate_imagen_creative_with_qc`
   so the next attempt runs instead of the whole call propagating.

2. `qc_creative` must detect malformed Gemini-Vision responses (missing
   check keys) and surface them as `qc_infrastructure: False` instead of
   silently treating all 9 checks as "check failed".

3. `_call_gemini_vision` must use a 120s timeout and retry once on
   ReadTimeout before bubbling up.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
import requests


# ─────────────────────────────────────────────────────────────────────────────
# Fix #1: generate_imagen_creative failures retry inside the loop
# ─────────────────────────────────────────────────────────────────────────────


def test_generate_imagen_creative_runtime_error_triggers_retry(monkeypatch, tmp_path):
    """RuntimeError from generate_imagen_creative (e.g. Gemini returned chat
    text with finishReason=STOP) must NOT propagate out of
    generate_imagen_creative_with_qc. The retry loop should swallow it and
    move on to the next attempt."""
    from src import gemini_creative as gc
    from src import copy_design_qc as qc

    call_count = {"n": 0}

    def fake_gen(**kwargs):
        call_count["n"] += 1
        if call_count["n"] == 1:
            raise RuntimeError(
                "Gemini direct API returned no image in response "
                "(finishReason='STOP', text=['Here is your image:\\n'])"
            )
        png = tmp_path / "creative.png"
        png.write_bytes(b"\x89PNG\r\n\x1a\n")
        return png

    fake_report = MagicMock()
    fake_report.verdict = "PASS"
    fake_report.retry_target = "none"
    fake_report.violations = []
    fake_report.copy_violations = []
    fake_report.feedback_crop_paths = []
    fake_report.retry_instruction = ""
    fake_report.to_dict.return_value = {"verdict": "PASS"}

    monkeypatch.setattr(gc, "generate_imagen_creative", fake_gen)
    # qc_creative is imported locally inside the function from copy_design_qc.
    monkeypatch.setattr(qc, "qc_creative", lambda **kw: fake_report)

    variant = {"angle": "A", "headline": "h", "subheadline": "s", "photo_subject": "x"}
    path, report = gc.generate_imagen_creative_with_qc(
        variant=variant, copy_rewriter=None, max_retries=2,
    )
    assert call_count["n"] == 2
    assert report.get("verdict") == "PASS"


def test_generate_imagen_creative_persistent_failure_returns_best_report(monkeypatch):
    """If every attempt raises, the function must return a best-effort
    report (verdict=FAIL with image_generation: False) rather than letting
    the RuntimeError propagate."""
    from src import gemini_creative as gc
    from src import copy_design_qc as qc

    monkeypatch.setattr(
        gc, "generate_imagen_creative",
        lambda **kw: (_ for _ in ()).throw(RuntimeError("Gemini said hi")),
    )
    monkeypatch.setattr(qc, "qc_creative", lambda **kw: None)  # not reached

    variant = {"angle": "A", "headline": "h", "subheadline": "s", "photo_subject": "x"}
    path, report = gc.generate_imagen_creative_with_qc(
        variant=variant, copy_rewriter=None, max_retries=2,
    )
    assert report["verdict"] == "FAIL"
    assert "image_generation" in (report.get("checks") or {})


# ─────────────────────────────────────────────────────────────────────────────
# Fix #2: malformed QC vision response is detected
# ─────────────────────────────────────────────────────────────────────────────


def _fake_png(tmp_path) -> Path:
    p = tmp_path / "creative.png"
    p.write_bytes(b"\x89PNG\r\n\x1a\n")
    return p


def test_qc_creative_malformed_response_flags_infrastructure(monkeypatch, tmp_path):
    """When Gemini Vision returns a dict that's missing all the expected
    check keys, qc_creative must return verdict=FAIL with
    `qc_infrastructure: False` — NOT the old behavior of pretending every
    check failed."""
    from src import copy_design_qc as qc

    # Empty dict — zero expected keys present.
    monkeypatch.setattr(qc, "_call_gemini_vision", lambda *a, **kw: {})
    report = qc.qc_creative(
        creative_path=_fake_png(tmp_path),
        reference_path=None,
        headline="h", subheadline="s",
    )
    assert report.verdict == "FAIL"
    assert "qc_infrastructure" in report.checks
    assert report.checks["qc_infrastructure"] is False
    assert any("malformed" in v.lower() for v in report.violations)


def test_qc_creative_wrong_shape_response_flags_infrastructure(monkeypatch, tmp_path):
    """Wrong-shape JSON (e.g. wrapper object instead of the check map) also
    triggers the malformed-response path."""
    from src import copy_design_qc as qc

    monkeypatch.setattr(
        qc, "_call_gemini_vision",
        lambda *a, **kw: {"status": "ok", "checks": []},  # wrong shape
    )
    report = qc.qc_creative(
        creative_path=_fake_png(tmp_path),
        reference_path=None,
        headline="h", subheadline="s",
    )
    assert report.verdict == "FAIL"
    assert report.checks.get("qc_infrastructure") is False


def test_qc_creative_well_formed_response_proceeds_normally(monkeypatch, tmp_path):
    """Sanity: a well-formed Gemini-Vision response (all check keys present,
    all `pass=True`) still flows through to PASS — the malformed guard
    doesn't block real responses."""
    from src import copy_design_qc as qc

    well_formed = {
        k: {"pass": True, "detail": "ok"} for k in [
            "rendered_text_in_photo", "duplicate_logo", "text_overlaps_subject",
            "headroom_gap_appropriate", "photo_fills_frame", "logo_correct_shape",
            "subject_looks_ai", "matches_reference_person", "text_zone_contrast",
            "professional_quality",
        ]
    }
    monkeypatch.setattr(qc, "_call_gemini_vision", lambda *a, **kw: well_formed)
    report = qc.qc_creative(
        creative_path=_fake_png(tmp_path),
        reference_path=None,
        headline="h", subheadline="s",
    )
    assert report.verdict == "PASS"


# ─────────────────────────────────────────────────────────────────────────────
# Fix #3: QC vision timeout retry
# ─────────────────────────────────────────────────────────────────────────────


def test_qc_vision_retries_once_on_read_timeout(monkeypatch):
    """ReadTimeout on the first POST must retry once with the same payload.
    Verifies _call_gemini_vision uses timeout=120 and retries on transient."""
    from src import copy_design_qc as qc

    monkeypatch.setattr(qc.config, "GEMINI_API_KEY", "fake-key")
    monkeypatch.setattr(qc, "_image_to_b64", lambda p: "fake_b64")

    calls = []
    fake_ok = MagicMock()
    fake_ok.status_code = 200
    fake_ok.json.return_value = {
        "candidates": [{"content": {"parts": [{"text": "{}"}]}}],
    }

    def fake_post(url, json=None, timeout=None):
        calls.append({"timeout": timeout})
        if len(calls) == 1:
            raise requests.exceptions.ReadTimeout("first call timed out")
        return fake_ok

    with patch("requests.post", side_effect=fake_post):
        result = qc._call_gemini_vision("prompt", "/tmp/fake.png", None)

    assert len(calls) == 2, f"expected 2 calls (1 timeout + 1 retry), got {len(calls)}"
    assert calls[0]["timeout"] == 120, "first call must use 120s timeout (was 60s before fix)"
    assert calls[1]["timeout"] == 120, "retry must also use 120s timeout"
    assert result == {}


def test_qc_vision_second_timeout_propagates(monkeypatch):
    """If BOTH the initial call and the single retry time out, the
    ReadTimeout propagates — caller (qc_creative) treats it as
    qc_infrastructure_failure."""
    from src import copy_design_qc as qc

    monkeypatch.setattr(qc.config, "GEMINI_API_KEY", "fake-key")
    monkeypatch.setattr(qc, "_image_to_b64", lambda p: "fake_b64")

    with patch(
        "requests.post",
        side_effect=requests.exceptions.ReadTimeout("perpetual timeout"),
    ):
        with pytest.raises(requests.exceptions.ReadTimeout):
            qc._call_gemini_vision("prompt", "/tmp/fake.png", None)

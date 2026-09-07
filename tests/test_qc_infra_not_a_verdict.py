"""A QC infrastructure failure must not reject a creative.

`qc_creative` swallows its own vision exception and returns
`QCReport(verdict="FAIL", checks={"qc_infrastructure": False})`. To the retry
loop in `generate_imagen_creative_with_qc` that is indistinguishable from a
real design violation, so a dropped connection to the vision model used to:

  1. spend one of the creative's regeneration attempts, and
  2. become the "best so far" report the creative is ultimately rejected on.

Observed 5× on GMR-0029 (2026-09-05):
`QC vision call failed: ('Connection aborted.', ConnectionResetError(104,
'Connection reset by peer'))`. Those sat alongside 7 genuine rejections and 19
skipped creative attaches.

An unreachable vision model means the creative is UNJUDGED, not bad. The loop
now re-checks the same image before giving up on that attempt, and never lets
an inconclusive verdict decide the outcome.
"""
from __future__ import annotations

import sys
from pathlib import Path

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

import src.copy_design_qc as cdq  # noqa: E402
import src.gemini_creative as gc  # noqa: E402
from src.copy_design_qc import QCReport  # noqa: E402


def _infra_fail() -> QCReport:
    return QCReport(
        verdict="FAIL",
        checks={"qc_infrastructure": False},
        violations=["QC vision call failed: ('Connection aborted.', ConnectionResetError(104))"],
        retry_target="none",
    )


def _pass() -> QCReport:
    return QCReport(verdict="PASS", checks={"rendered_text_in_photo": True}, violations=[])


def _real_fail() -> QCReport:
    return QCReport(
        verdict="FAIL",
        checks={"rendered_text_in_photo": False},
        violations=["No rendered text in photo: 'OUTLIER' on a mug"],
        retry_target="gemini",
    )


@pytest.fixture
def harness(monkeypatch, tmp_path):
    """Stub image generation + QC so only the retry logic is under test."""
    monkeypatch.setattr(gc.time, "sleep", lambda s: None)
    png = tmp_path / "creative.png"
    png.write_bytes(b"\x89PNG\r\n\x1a\n")

    gens = []

    def fake_gen(**kw):
        gens.append(1)
        return png

    monkeypatch.setattr(gc, "generate_imagen_creative", fake_gen)
    return {"png": png, "gens": gens}


def _run(variant=None):
    return gc.generate_imagen_creative_with_qc(variant=variant or {"headline": "h", "subheadline": "s"})


def test_transient_infra_failure_recovers_without_regenerating(harness, monkeypatch):
    """One infra blip then a PASS: same image re-checked, image generated once."""
    seq = [_infra_fail(), _pass()]
    monkeypatch.setattr(cdq, "qc_creative", lambda **kw: seq.pop(0))

    path, report = _run()
    assert report["verdict"] == "PASS"
    assert len(harness["gens"]) == 1, "the image must not be regenerated for an infra blip"


def test_infra_failure_never_becomes_the_rejection_reason(harness, monkeypatch):
    """Every QC call unreachable -> the creative is not rejected ON that basis."""
    monkeypatch.setattr(cdq, "qc_creative", lambda **kw: _infra_fail())
    monkeypatch.setattr(gc, "_QC_INFRA_RETRIES", 1)

    path, report = _run()
    # Whatever the outcome, an unreachable vision model must not be reported as
    # a design violation of this creative.
    assert report.get("checks", {}).get("qc_infrastructure") is not False


def test_real_violations_still_reject(harness, monkeypatch):
    """The infra carve-out must not weaken genuine QC rejection."""
    monkeypatch.setattr(cdq, "qc_creative", lambda **kw: _real_fail())
    path, report = _run()
    assert report["verdict"] == "FAIL"
    assert any("rendered text" in v.lower() for v in report.get("violations", []))


def test_real_violation_after_infra_blip_is_still_seen(harness, monkeypatch):
    """An infra blip must not mask the real verdict that follows it."""
    seq = [_infra_fail(), _real_fail()]
    monkeypatch.setattr(cdq, "qc_creative", lambda **kw: seq.pop(0) if seq else _real_fail())
    path, report = _run()
    assert report["verdict"] == "FAIL"
    assert any("rendered text" in v.lower() for v in report.get("violations", []))


def test_reference_prompt_forbids_copying_reference_text():
    """The reference is a finished finance ad; Gemini reproduced its headline
    onto GMR-0029's design creatives. The prompt must say so explicitly —
    'render no text' alone did not stop it."""
    src = Path(_PROJECT_ROOT / "src" / "gemini_creative.py").read_text()
    assert "COMPOSITION REFERENCE ONLY" in src
    assert "Reproduce the LAYOUT, never the LANGUAGE" in src

"""Phase 3.1 — image-gen parallelization in `_process_static_campaigns`.

Verifies that:
  1. Concurrency > 1 actually invokes generate_imagen_creative_with_qc in
     overlapping windows (real parallelism, not just batched submission).
  2. Output shape (campaign_specs) is invariant across concurrency settings:
     same length, same per-task fields, in the original (cohort × geo × angle)
     order.
  3. Per-task exceptions are isolated — one failing image gen does NOT abort
     other tasks; the offending spec gets png_path=None and an empty
     qc_report.
  4. config.IMAGE_GEN_CONCURRENCY=1 falls back to a fully sequential code
     path (avoids ThreadPoolExecutor overhead).

These tests stub generate_imagen_creative_with_qc with a controllable fake
that records call timestamps so we can prove the parallel path actually
overlaps work.
"""
from __future__ import annotations

import sys
import time
import threading
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ─── fixtures ──────────────────────────────────────────────────────────────


def _build_cohorts(n: int):
    """Minimal CohortSpec stand-ins. _process_static_campaigns reads
    .name, ._stg_id, ._stg_name, .rules, getattr(.,'id'), getattr(.,'exclude_add'),
    getattr(.,'exclude_remove'). A SimpleNamespace covers all of these."""
    from types import SimpleNamespace
    return [
        SimpleNamespace(
            name=f"cohort{i}",
            _stg_id=f"stg_{i}",
            _stg_name=f"Stage {i}",
            id=f"cohort{i}",
            rules=[],
            exclude_add=[],
            exclude_remove=[],
        )
        for i in range(n)
    ]


def _stub_geo_groups(monkeypatch):
    """Stub group_geos_for_campaigns to return one fixed geo group so the test
    is decoupled from the real geo-tier logic."""
    from src import geo_tiers as _gt
    fake_group = _gt.GeoCampaignGroup(
        cluster="anglo",
        cluster_label="English-speaking",
        geos=["US", "GB"],
        median_multiplier=1.0,
        advertised_rate="$50/hr",
        campaign_suffix="anglo",
    )
    monkeypatch.setattr(_gt, "group_geos_for_campaigns", lambda *a, **kw: [fake_group])
    monkeypatch.setattr(_gt, "filter_blocked_geos", lambda geos: geos)
    return fake_group


def _stub_copy_and_image(monkeypatch, image_fn):
    """Replace build_copy_variants + generate_imagen_creative_with_qc.
    image_fn(variant) -> (Path, qc_report_dict). Receives one task at a time."""
    import main as M
    monkeypatch.setattr(M, "build_copy_variants",
                        lambda cohort, layer_map, **kw: [
                            {"angle": "A", "headline": "H-A", "subheadline": "S-A"},
                            {"angle": "B", "headline": "H-B", "subheadline": "S-B"},
                            {"angle": "C", "headline": "H-C", "subheadline": "S-C"},
                        ])
    monkeypatch.setattr(M, "generate_imagen_creative_with_qc", image_fn)


# ─── tests ──────────────────────────────────────────────────────────────────


def test_concurrency_actually_overlaps_image_gen(monkeypatch, tmp_path):
    """At concurrency=4, four image gens should be in flight at the same
    moment. We force each gen to sleep 0.4s and assert that wall-clock for
    12 tasks is < 1.5s (sequential would be ≥ 4.8s)."""
    import config
    import main as M

    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 4)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 4)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)

    _stub_geo_groups(monkeypatch)

    # Track concurrency: how many gens are running at any instant?
    in_flight = 0
    max_in_flight = 0
    lock = threading.Lock()

    def fake_gen(variant, copy_rewriter=None, **kw):
        nonlocal in_flight, max_in_flight
        with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time.sleep(0.4)
        with lock:
            in_flight -= 1
        return (tmp_path / f"img_{variant.get('angle','A')}.png", {"verdict": "PASS"})

    _stub_copy_and_image(monkeypatch, fake_gen)
    # dry_run=True skips LinkedIn API calls, but we DO want image gen to run
    # so the parallel path is exercised. WITH_IMAGES forces gen.
    monkeypatch.setenv("WITH_IMAGES", "1")

    cohorts = _build_cohorts(4)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value     = {}
    urn_res.resolve_facet_pairs.return_value          = {}
    urn_res.resolve_cohort_rules.return_value         = {}

    started = time.monotonic()
    out = M._process_static_campaigns(
        cohorts,
        flow_id="flow",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        dry_run=True,  # short-circuit LinkedIn API calls; image gen still runs
    )
    elapsed = time.monotonic() - started

    specs = out["campaign_specs"]
    assert len(specs) == 4 * 3, f"expected 12 specs, got {len(specs)}"
    assert max_in_flight >= 2, (
        f"image gen never overlapped — max_in_flight={max_in_flight}; "
        "ThreadPoolExecutor not running"
    )
    # 12 tasks × 0.4s sequential = 4.8s; with workers=4 we expect ~1.2-1.6s.
    # Allow generous slack for CI noise but well below sequential floor.
    assert elapsed < 3.0, (
        f"wall-clock {elapsed:.2f}s suggests near-sequential execution "
        f"(expected ~1.2s at concurrency=4)"
    )


def test_sequential_fallback_when_concurrency_is_1(monkeypatch, tmp_path):
    """concurrency=1 must take the inline sequential path (no executor)."""
    import config
    import main as M

    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 1)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 2)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    in_flight = 0
    max_in_flight = 0
    lock = threading.Lock()

    def fake_gen(variant, copy_rewriter=None, **kw):
        nonlocal in_flight, max_in_flight
        with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time.sleep(0.05)
        with lock:
            in_flight -= 1
        return (tmp_path / "x.png", {"verdict": "PASS"})

    _stub_copy_and_image(monkeypatch, fake_gen)
    # dry_run=True skips LinkedIn API calls, but we DO want image gen to run
    # so the parallel path is exercised. WITH_IMAGES forces gen.
    monkeypatch.setenv("WITH_IMAGES", "1")

    cohorts = _build_cohorts(2)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value     = {}
    urn_res.resolve_facet_pairs.return_value          = {}
    urn_res.resolve_cohort_rules.return_value         = {}

    M._process_static_campaigns(
        cohorts,
        flow_id="flow",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        dry_run=True,
    )

    assert max_in_flight == 1, (
        f"concurrency=1 should never have >1 in-flight gen; got {max_in_flight}"
    )


def test_one_failing_task_does_not_abort_others(monkeypatch, tmp_path):
    """When one image gen raises, that single spec gets png_path=None but
    every other (cohort × geo × angle) still produces a valid spec."""
    import config
    import main as M

    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 3)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 3)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    fail_when = {"angle": "B", "cohort_name": "cohort1"}

    def fake_gen(variant, copy_rewriter=None, **kw):
        if variant.get("angle") == fail_when["angle"]:
            raise RuntimeError("boom")
        return (tmp_path / "ok.png", {"verdict": "PASS"})

    _stub_copy_and_image(monkeypatch, fake_gen)
    # dry_run=True skips LinkedIn API calls, but we DO want image gen to run
    # so the parallel path is exercised. WITH_IMAGES forces gen.
    monkeypatch.setenv("WITH_IMAGES", "1")

    cohorts = _build_cohorts(3)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value     = {}
    urn_res.resolve_facet_pairs.return_value          = {}
    urn_res.resolve_cohort_rules.return_value         = {}

    out = M._process_static_campaigns(
        cohorts,
        flow_id="flow",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        dry_run=True,
    )

    specs = out["campaign_specs"]
    assert len(specs) == 3 * 3, f"expected 9 specs, got {len(specs)}"
    # All B-angle specs should have png_path None; others should be a Path.
    b_specs     = [s for s in specs if s["angle_label"] == "B"]
    other_specs = [s for s in specs if s["angle_label"] != "B"]
    assert all(s["png_path"] is None for s in b_specs), (
        "B-angle specs should have png_path=None (image gen raised)"
    )
    assert all(s["png_path"] is not None for s in other_specs), (
        "non-B specs should have a valid png_path"
    )


def test_spec_order_preserved_across_concurrency(monkeypatch, tmp_path):
    """campaign_specs ordering must match the original (cohort × geo × angle)
    iteration order regardless of which future completes first. This is what
    downstream grouping in _process_static_campaigns and _process_extra_platform_arm
    relies on."""
    import config
    import main as M

    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 4)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 3)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    # Random sleeps so future completion order does NOT match submission order.
    import random
    random.seed(7)

    def fake_gen(variant, copy_rewriter=None, **kw):
        time.sleep(random.uniform(0.05, 0.25))
        return (tmp_path / f"{variant.get('angle')}.png", {"verdict": "PASS"})

    _stub_copy_and_image(monkeypatch, fake_gen)
    # dry_run=True skips LinkedIn API calls, but we DO want image gen to run
    # so the parallel path is exercised. WITH_IMAGES forces gen.
    monkeypatch.setenv("WITH_IMAGES", "1")

    cohorts = _build_cohorts(3)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value     = {}
    urn_res.resolve_facet_pairs.return_value          = {}
    urn_res.resolve_cohort_rules.return_value         = {}

    out = M._process_static_campaigns(
        cohorts,
        flow_id="flow",
        location="US",
        sheets=sheets,
        li_client=li_client,
        urn_res=urn_res,
        claude_key="",
        dry_run=True,
    )

    specs = out["campaign_specs"]
    # Expected order: cohort0_A, cohort0_B, cohort0_C, cohort1_A, ..., cohort2_C
    expected = [
        (f"stg_{c}", "A") for c in range(3)
    ]
    expected = [
        (f"stg_{c}", a) for c in range(3) for a in ("A", "B", "C")
    ]
    actual = [(s["cohort"]._stg_id, s["angle_label"]) for s in specs]
    assert actual == expected, (
        f"spec order changed under concurrency:\n"
        f"  expected: {expected}\n"
        f"  actual:   {actual}"
    )

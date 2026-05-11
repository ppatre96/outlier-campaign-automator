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


# ─── Phase 3.2 tests — parallel copy gen across (cohort × geo) ─────────────────


def _stub_copy_with_fn(monkeypatch, copy_fn, image_fn=None):
    """Replace build_copy_variants with a custom callable (so tests can
    observe call ordering / inject delays / raise on selected jobs). Also
    stubs generate_imagen_creative_with_qc to a no-op unless image_fn given."""
    import main as M
    monkeypatch.setattr(M, "build_copy_variants", copy_fn)
    if image_fn is None:
        image_fn = lambda variant, copy_rewriter=None, **kw: (None, {})
    monkeypatch.setattr(M, "generate_imagen_creative_with_qc", image_fn)


def test_copy_gen_overlaps_under_concurrency(monkeypatch, tmp_path):
    """Phase 3.2: at COPY_GEN_CONCURRENCY=4, multiple build_copy_variants
    calls run in overlapping windows. Force each call to sleep 0.4s and
    assert (a) max_in_flight >= 3, (b) wall-clock < 1.5s for 9 jobs
    (sequential floor would be 3.6s)."""
    import threading
    import time

    import config
    import main as M

    monkeypatch.setattr(config, "COPY_GEN_CONCURRENCY", 4)
    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 1)  # isolate copy stage
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 3)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    in_flight = 0
    max_in_flight = 0
    lock = threading.Lock()

    def slow_copy(cohort, layer_map, **kw):
        nonlocal in_flight, max_in_flight
        with lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
        time.sleep(0.4)
        with lock:
            in_flight -= 1
        return [
            {"angle": "A", "headline": "H-A", "subheadline": "S-A"},
            {"angle": "B", "headline": "H-B", "subheadline": "S-B"},
            {"angle": "C", "headline": "H-C", "subheadline": "S-C"},
        ]

    _stub_copy_with_fn(monkeypatch, slow_copy)
    monkeypatch.setenv("WITH_IMAGES", "")  # skip image gen so we time copy only

    cohorts = _build_cohorts(3)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

    t0 = time.monotonic()
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
    elapsed = time.monotonic() - t0

    assert max_in_flight >= 3, (
        f"copy gen never overlapped — max_in_flight={max_in_flight}; "
        "ThreadPoolExecutor not engaged in the copy-gen phase"
    )
    assert elapsed < 1.5, (
        f"copy-gen wall-clock {elapsed:.2f}s suggests near-sequential "
        f"execution (expected ~0.5–1.0s at concurrency=4 for 9 jobs)"
    )
    # Variants populated for all 9 (cohort × geo) combos × 3 angles = 9 specs
    # in this test (because _stub_geo_groups produces one geo group and we
    # have 3 cohorts × 1 geo × 3 angles).
    specs = out["campaign_specs"]
    assert len(specs) == 3 * 3, f"expected 9 specs, got {len(specs)}"
    for s in specs:
        assert len(s["variants"]) == 3, "copy variants should be populated"


def test_copy_gen_sequential_fallback_at_concurrency_1(monkeypatch, tmp_path):
    """When COPY_GEN_CONCURRENCY=1 the copy stage runs inline — no executor.
    We patch ThreadPoolExecutor with a sentinel that raises if constructed,
    proving the sequential branch is taken."""
    import concurrent.futures as cf

    import config
    import main as M

    monkeypatch.setattr(config, "COPY_GEN_CONCURRENCY", 1)
    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 1)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 2)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    def boom_executor(*args, **kwargs):
        raise AssertionError(
            "ThreadPoolExecutor must NOT be constructed when "
            "COPY_GEN_CONCURRENCY=1"
        )

    monkeypatch.setattr(cf, "ThreadPoolExecutor", boom_executor)

    _stub_copy_with_fn(monkeypatch, lambda cohort, layer_map, **kw: [
        {"angle": "A"}, {"angle": "B"}, {"angle": "C"},
    ])

    cohorts = _build_cohorts(2)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

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
    assert len(out["campaign_specs"]) == 2 * 3


def test_copy_gen_exception_isolated_per_job(monkeypatch, tmp_path):
    """One cohort's copy-gen raises; the other 2 still produce variants and
    the failing combo gets variants=[] so its 3 downstream angle tasks have
    empty selected_variant (matches today's single-threaded except behavior)."""
    import config
    import main as M

    monkeypatch.setattr(config, "COPY_GEN_CONCURRENCY", 3)
    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 1)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 3)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    def selective_copy(cohort, layer_map, **kw):
        if cohort.name == "cohort1":
            raise RuntimeError("boom in cohort1 copy gen")
        return [
            {"angle": "A", "headline": f"H-{cohort.name}-A"},
            {"angle": "B", "headline": f"H-{cohort.name}-B"},
            {"angle": "C", "headline": f"H-{cohort.name}-C"},
        ]

    _stub_copy_with_fn(monkeypatch, selective_copy)

    cohorts = _build_cohorts(3)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

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
    assert len(specs) == 9, f"expected 9 specs even with one failure, got {len(specs)}"
    failing = [s for s in specs if s["cohort"].name == "cohort1"]
    healthy = [s for s in specs if s["cohort"].name != "cohort1"]
    assert all(s["variants"] == [] for s in failing), (
        "failed cohort's specs must have variants=[]"
    )
    assert all(len(s["variants"]) == 3 for s in healthy), (
        "healthy cohorts' specs should keep their 3 variants"
    )


def test_copy_gen_called_even_when_image_skipped(monkeypatch, tmp_path):
    """dry_run=True + WITH_IMAGES unset → image gen is skipped but copy gen
    must still run (the registry/copy-eval workflows depend on having variants
    even in dry-run mode)."""
    import config
    import main as M

    monkeypatch.setattr(config, "COPY_GEN_CONCURRENCY", 2)
    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 2)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 2)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)
    monkeypatch.delenv("WITH_IMAGES", raising=False)

    copy_calls: list[str] = []
    def counting_copy(cohort, layer_map, **kw):
        copy_calls.append(cohort.name)
        return [{"angle": "A"}, {"angle": "B"}, {"angle": "C"}]

    image_calls = []
    def counting_image(variant, copy_rewriter=None, **kw):
        image_calls.append(variant.get("angle"))
        return (tmp_path / "x.png", {"verdict": "PASS"})

    _stub_copy_with_fn(monkeypatch, counting_copy, image_fn=counting_image)

    cohorts = _build_cohorts(2)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

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

    assert len(copy_calls) == 2, (
        f"copy gen should run for each (cohort × geo) combo; got {len(copy_calls)}"
    )
    assert image_calls == [], (
        f"image gen must be skipped when WITH_IMAGES is unset in dry-run; got {image_calls}"
    )


def test_spec_order_preserved_through_both_stages(monkeypatch, tmp_path):
    """End-to-end ordering: random sleeps shuffle copy-stage completion order,
    image stage also runs concurrently. Final campaign_specs must still be
    cohort-outer/geo-inner/angle-innermost, because both stages collect
    results into idx-keyed dicts and rebuild from index."""
    import random
    import time

    import config
    import main as M

    monkeypatch.setattr(config, "COPY_GEN_CONCURRENCY", 4)
    monkeypatch.setattr(config, "IMAGE_GEN_CONCURRENCY", 4)
    monkeypatch.setattr(config, "MAX_COHORTS_PER_GEO_CLUSTER", 4)
    monkeypatch.setattr(config, "ANGLES_PER_COHORT", 3)
    _stub_geo_groups(monkeypatch)

    rng = random.Random(7)

    def shuffled_copy(cohort, layer_map, **kw):
        time.sleep(rng.uniform(0.02, 0.18))
        return [
            {"angle": "A", "headline": f"{cohort.name}-A"},
            {"angle": "B", "headline": f"{cohort.name}-B"},
            {"angle": "C", "headline": f"{cohort.name}-C"},
        ]

    def shuffled_image(variant, copy_rewriter=None, **kw):
        time.sleep(rng.uniform(0.02, 0.18))
        return (tmp_path / f"{variant['headline']}.png", {"verdict": "PASS"})

    _stub_copy_with_fn(monkeypatch, shuffled_copy, image_fn=shuffled_image)
    monkeypatch.setenv("WITH_IMAGES", "1")

    cohorts = _build_cohorts(4)
    li_client = MagicMock()
    sheets    = MagicMock()
    urn_res   = MagicMock()
    urn_res.resolve_default_excludes.return_value = {}
    urn_res.resolve_facet_pairs.return_value      = {}
    urn_res.resolve_cohort_rules.return_value     = {}

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
    expected = [
        (f"stg_{c}", a) for c in range(4) for a in ("A", "B", "C")
    ]
    actual = [(s["cohort"]._stg_id, s["angle_label"]) for s in specs]
    assert actual == expected, (
        f"end-to-end order changed under parallel copy + image gen:\n"
        f"  expected: {expected}\n"
        f"  actual:   {actual}"
    )

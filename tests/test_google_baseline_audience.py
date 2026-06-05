"""Google Display baseline audience layer (2026-06-05).

Generalist-locale cohorts produce no skill/title terms, so the Display ad group
used to ship geo-only (empty audience_segments → 'not gated'). The resolver now
seeds a broad EMPLOYMENT-safe professional affinity ("Business Professionals")
when no segment was otherwise resolved. Network calls are mocked.
"""
import os
import sys
from types import SimpleNamespace

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.google_targeting import GoogleSegmentResolver


def _resolver(monkeypatch, *, term_segments=None):
    r = GoogleSegmentResolver.__new__(GoogleSegmentResolver)
    # Bypass __init__ (no creds needed for the logic under test).
    r._cache = {}
    r._cache_loaded = True
    r._client = None
    monkeypatch.setattr(r, "_load_cache", lambda: None)
    monkeypatch.setattr(r, "_save_cache", lambda: None)
    monkeypatch.setattr(r, "_resolve_geos", lambda geos: [f"geoTargetConstants/{g}" for g in (geos or [])])
    monkeypatch.setattr(r, "_resolve_audience", lambda term: list(term_segments or []))
    monkeypatch.setattr(r, "_generate_keyword_ideas", lambda term, **kw: [])
    monkeypatch.setattr(r, "get_keyword_volume_for_term", lambda term: 0)
    # Baseline lookup returns a fake Business Professionals affinity.
    monkeypatch.setattr(r, "_search_user_interest",
                        lambda hints: ["customers/1/userInterests/92913"])
    return r


def test_generalist_cohort_gets_baseline_affinity(monkeypatch):
    r = _resolver(monkeypatch)
    cohort = SimpleNamespace(
        name="Bengali generalist contributors",
        rules=[("interface_locale", "bn-IN")],
        facet_strength={"generalist_locale": "bn-IN"},
    )
    out = r.resolve_cohort(cohort, geos=["IN", "BD"])
    assert out["audience_segments"] == ["customers/1/userInterests/92913"]


def test_cohort_with_real_segments_skips_baseline(monkeypatch):
    # A cohort whose skill term resolves to a segment must NOT get the baseline
    # appended (baseline only fills the empty case).
    r = _resolver(monkeypatch, term_segments=["customers/1/userInterests/55555"])
    cohort = SimpleNamespace(
        name="ML engineers",
        rules=[("skills__machine_learning", 1)],
        facet_strength={},
    )
    out = r.resolve_cohort(cohort, geos=["US"])
    assert out["audience_segments"] == ["customers/1/userInterests/55555"]
    assert "customers/1/userInterests/92913" not in out["audience_segments"]

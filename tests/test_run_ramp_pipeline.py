"""Unit tests for the Phase 2.6 Plan 02 pipeline (SR-03 + SR-04).

Tests verify the new dual-arm Smart Ramp pipeline added to main.py:
  - SR-03: BOTH InMail + Static campaigns produced for every cohort
  - SR-04: LinkedIn create_image_ad 403 -> ImageAdResult(status="local_fallback")
           and PNG saved locally; processing continues
  - Pitfall 1: _resolve_cohorts runs ONCE per row (not 2x — once per mode)
  - Per-cohort isolation: one cohort's failure does NOT abort other cohorts

All tests use monkeypatch + MagicMock — NO real Smart Ramp HTTP, NO real
Snowflake/Redash, NO real LinkedIn calls, NO real Slack. Filesystem
assertions use tmp_path.
"""
from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


def _build_ramp_with_cohorts(n_cohorts: int = 2):
    """Build a fake RampRecord with N cohorts for testing."""
    from src.smart_ramp_client import RampRecord, CohortSpec
    cohorts = [
        CohortSpec(
            id=f"cohort{i}",
            cohort_description=f"Cohort {i}",
            signup_flow_id=f"flow_{i}",
            selected_lp_url=f"https://outlier.ai/c{i}",
            included_geos=["IN"],
            matched_locales=None,
            target_activations=100,
            job_post_id=None,
        )
        for i in range(n_cohorts)
    ]
    return RampRecord(
        id="GMR-TEST",
        project_id="p",
        project_name="Test Project",
        requester_name="Pranav",
        summary="test ramp",
        submitted_at="t",
        updated_at="t",
        status="submitted",
        linear_issue_id=None,
        linear_url=None,
        cohorts=cohorts,
    )


def _patch_runtime_dependencies(monkeypatch, ramp):
    """Stub out all the side-effecting clients run_launch_for_ramp constructs."""
    import main as M

    # Smart Ramp client returns our test ramp.
    fake_client = MagicMock()
    fake_client.fetch_ramp.return_value = ramp
    monkeypatch.setattr(M, "SmartRampClient", lambda *a, **kw: fake_client)

    # Sheets / LinkedIn / Redash / brand-voice — all stubbed.
    fake_sheets = MagicMock()
    fake_sheets.read_config.return_value = {"LINKEDIN_TOKEN": "fake-token"}
    monkeypatch.setattr(M, "SheetsClient", lambda *a, **kw: fake_sheets)

    fake_linkedin = MagicMock()
    monkeypatch.setattr(M, "LinkedInClient", lambda *a, **kw: fake_linkedin)

    fake_urn = MagicMock()
    monkeypatch.setattr(M, "UrnResolver", lambda *a, **kw: fake_urn)

    fake_redash = MagicMock()
    monkeypatch.setattr(M, "RedashClient", lambda *a, **kw: fake_redash)

    fake_validator = MagicMock()
    monkeypatch.setattr(M, "BrandVoiceValidator", lambda *a, **kw: fake_validator)


def _stub_cohort(cohort_id: str):
    """Build a minimal cohort-shaped object with the attrs _resolve_cohorts/
    _process_*_campaigns expect downstream."""
    c = MagicMock()
    c.id = cohort_id
    c.name = f"cohort_name_{cohort_id}"
    c._stg_id = f"stg_{cohort_id}"
    c._stg_name = f"agent_grp_{cohort_id}"
    c.cohort_description = f"description for {cohort_id}"
    c.rules = []
    c.exclude_add = []
    c.exclude_remove = []
    return c


# ─────────────────────────────────────────────────────────────────────────────
# SR-03: BOTH InMail + Static per cohort
# ─────────────────────────────────────────────────────────────────────────────


def test_both_modes_per_cohort(monkeypatch):
    """SR-03: every cohort produces an InMail draft AND a Static draft campaign."""
    import main as M

    ramp = _build_ramp_with_cohorts(n_cohorts=3)
    _patch_runtime_dependencies(monkeypatch, ramp)

    # _resolve_cohorts: each row resolves to its own single cohort with id == row["cohort_id"].
    def mock_resolve(row, **kwargs):
        cohort_id = row["cohort_id"]
        cohort = _stub_cohort(cohort_id)
        return M.ResolvedCohorts(
            selected=[cohort],
            group_name=f"agent_grp_{cohort_id}",
            exclude_pairs=[],
            project_id="p",
            tg_cat="GENERAL",
            ad_type_hint="",
            geo_overrides_applied=False,
            family_exclude_pairs=[],
            data_driven_exclude_pairs=[],
            flow_id=row.get("flow_id", ""),
            location="",
        )
    monkeypatch.setattr(M, "_resolve_cohorts", mock_resolve)

    def fake_inmail(*, selected, **kw):
        return {
            "campaigns": [f"urn:li:cmp:in_{c.id}" for c in selected],
            "campaigns_by_cohort": {c.id: f"urn:li:cmp:in_{c.id}" for c in selected},
            "creative_paths": {c.id: f"urn:li:dsc:in_{c.id}" for c in selected},
            "campaign_groups": ["urn:li:sponsoredCampaignGroup:in_grp"],
        }
    def fake_static(*, selected, **kw):
        return {
            "campaigns": [f"urn:li:cmp:st_{c.id}" for c in selected],
            "campaigns_by_cohort": {c.id: f"urn:li:cmp:st_{c.id}" for c in selected},
            "creative_paths": {c.id: f"urn:li:dsc:st_{c.id}" for c in selected},
            "campaign_groups": ["urn:li:sponsoredCampaignGroup:st_grp"],
        }
    monkeypatch.setattr(M, "_process_inmail_campaigns", fake_inmail)
    monkeypatch.setattr(M, "_process_static_campaigns", fake_static)

    out = M.run_launch_for_ramp("GMR-TEST", modes=("inmail", "static"), dry_run=True)

    assert out["ok"] is True, f"expected ok=True, got {out}"
    assert len(out["inmail_campaigns"]) == 3, (
        f"expected 3 InMail campaigns, got {len(out['inmail_campaigns'])}"
    )
    assert len(out["static_campaigns"]) == 3, (
        f"expected 3 Static campaigns, got {len(out['static_campaigns'])}"
    )
    assert len(out["per_cohort"]) == 3
    for entry in out["per_cohort"]:
        assert entry["inmail_urn"] is not None, f"missing inmail_urn for {entry['cohort_id']}"
        assert entry["static_urn"] is not None, f"missing static_urn for {entry['cohort_id']}"


# Removed test_image_403_falls_back_to_local 2026-05-09 — Drive-only policy
# means PNGs from Outlier designer (Gemini) are written ONLY to Shared Drive
# at <ramp_id>/<channel>/<cohort_geo>/<angle>.png. The _save_creative_locally
# helper that this test exercised has been deleted from main.py. When the
# LinkedIn DSC creative attach fails (e.g. MDP gate), the registry simply
# records the Drive URL that was uploaded earlier in the same loop; no local
# disk write occurs.


# ─────────────────────────────────────────────────────────────────────────────
# SR-04: ImageAdResult sentinel translation (verify create_image_ad wrapper)
# ─────────────────────────────────────────────────────────────────────────────


def test_imagead_sentinel_local_fallback_status():
    """SR-04: create_image_ad returns ImageAdResult(status='local_fallback') for
    403 / LINKEDIN_MEMBER_URN, NOT raise."""
    from src.linkedin_api import ImageAdResult, LinkedInClient

    # Bypass __init__ — we don't need a real session for this unit test.
    client = LinkedInClient.__new__(LinkedInClient)

    # Case: 403 from the inner impl
    def boom_403(**kwargs):
        raise Exception("403 Forbidden — DSC post denied")
    client._create_image_ad_impl = boom_403

    out = client.create_image_ad(
        campaign_urn="c", image_urn="urn:li:img:x",
        headline="h", description="d",
    )
    assert isinstance(out, ImageAdResult)
    assert out.status == "local_fallback"
    assert out.creative_urn is None
    assert "403" in (out.error_message or "")

    # Case: LINKEDIN_MEMBER_URN missing
    def boom_member(**kwargs):
        raise RuntimeError("LINKEDIN_MEMBER_URN is not set in .env. ...")
    client._create_image_ad_impl = boom_member

    out2 = client.create_image_ad(
        campaign_urn="c", image_urn="urn:li:img:x",
        headline="h", description="d",
    )
    assert isinstance(out2, ImageAdResult)
    assert out2.status == "local_fallback"
    assert out2.error_class == "RuntimeError"
    assert "LINKEDIN_MEMBER_URN" in (out2.error_message or "")


# ─────────────────────────────────────────────────────────────────────────────
# SR-04: per-cohort isolation
# ─────────────────────────────────────────────────────────────────────────────


def test_403_one_cohort_does_not_abort_others(monkeypatch):
    """SR-04: one cohort's static-arm exception does NOT abort other cohorts."""
    import main as M

    ramp = _build_ramp_with_cohorts(n_cohorts=3)
    _patch_runtime_dependencies(monkeypatch, ramp)

    def mock_resolve(row, **kw):
        cohort_id = row["cohort_id"]
        cohort = _stub_cohort(cohort_id)
        return M.ResolvedCohorts(
            selected=[cohort],
            group_name=f"agent_grp_{cohort_id}",
            exclude_pairs=[],
            project_id="p",
            tg_cat="GENERAL",
            ad_type_hint="",
            geo_overrides_applied=False,
            family_exclude_pairs=[],
            data_driven_exclude_pairs=[],
            flow_id="",
            location="",
        )
    monkeypatch.setattr(M, "_resolve_cohorts", mock_resolve)

    # InMail succeeds for all
    def fake_inmail(*, selected, **kw):
        return {
            "campaigns": [f"urn:li:cmp:in_{c.id}" for c in selected],
            "campaigns_by_cohort": {c.id: f"urn:li:cmp:in_{c.id}" for c in selected},
            "creative_paths": {c.id: f"urn:li:dsc:in_{c.id}" for c in selected},
            "campaign_groups": ["urn:li:sponsoredCampaignGroup:in_grp"],
        }
    monkeypatch.setattr(M, "_process_inmail_campaigns", fake_inmail)

    # Static raises for cohort1 ONLY; succeeds for cohort0 and cohort2.
    def flaky_static(*, selected, **kw):
        if any(c.id == "cohort1" for c in selected):
            raise RuntimeError("simulated static-arm crash on cohort1")
        return {
            "campaigns": [f"urn:li:cmp:st_{c.id}" for c in selected],
            "campaigns_by_cohort": {c.id: f"urn:li:cmp:st_{c.id}" for c in selected},
            "creative_paths": {c.id: f"urn:li:dsc:st_{c.id}" for c in selected},
            "campaign_groups": ["urn:li:sponsoredCampaignGroup:st_grp"],
        }
    monkeypatch.setattr(M, "_process_static_campaigns", flaky_static)

    out = M.run_launch_for_ramp("GMR-TEST", modes=("inmail", "static"), dry_run=True)

    # Per-cohort isolation: 3 entries total (one row per cohort).
    assert len(out["per_cohort"]) == 3, (
        f"expected 3 per_cohort entries, got {len(out['per_cohort'])}"
    )
    by_cohort = {e["cohort_id"]: e for e in out["per_cohort"]}
    # cohort0 + cohort2 have static URNs; cohort1 does NOT.
    assert by_cohort["cohort0"]["static_urn"] is not None
    assert by_cohort["cohort2"]["static_urn"] is not None
    assert by_cohort["cohort1"]["static_urn"] is None, (
        "cohort1 static should be None (its arm crashed)"
    )
    # All cohorts still got InMail (other arm survives).
    assert by_cohort["cohort0"]["inmail_urn"] is not None
    assert by_cohort["cohort1"]["inmail_urn"] is not None
    assert by_cohort["cohort2"]["inmail_urn"] is not None


# ─────────────────────────────────────────────────────────────────────────────
# Pitfall 1: Stage A/B/C runs ONCE per row, not 2x (one per mode)
# ─────────────────────────────────────────────────────────────────────────────


def test_resolve_cohorts_runs_once_per_ramp(monkeypatch):
    """Pitfall 1: _resolve_cohorts MUST be called exactly once per row,
    regardless of how many modes are dispatched. Calling it twice would double
    ~30s of Snowflake/Stage work per ramp."""
    import main as M

    ramp = _build_ramp_with_cohorts(n_cohorts=2)
    _patch_runtime_dependencies(monkeypatch, ramp)

    resolve_calls = []
    def counting_resolve(row, **kw):
        resolve_calls.append(row.get("cohort_id"))
        cohort = _stub_cohort(row["cohort_id"])
        return M.ResolvedCohorts(
            selected=[cohort],
            group_name="g",
            exclude_pairs=[],
            project_id="p",
            tg_cat="GENERAL",
            ad_type_hint="",
            geo_overrides_applied=False,
            family_exclude_pairs=[],
            data_driven_exclude_pairs=[],
            flow_id="",
            location="",
        )
    monkeypatch.setattr(M, "_resolve_cohorts", counting_resolve)

    monkeypatch.setattr(M, "_process_inmail_campaigns", lambda **kw: {
        "campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": [],
    })
    monkeypatch.setattr(M, "_process_static_campaigns", lambda **kw: {
        "campaigns": [], "campaigns_by_cohort": {}, "creative_paths": {}, "campaign_groups": [],
    })

    M.run_launch_for_ramp("GMR-TEST", modes=("inmail", "static"), dry_run=True)

    # Exactly 2 rows × 1 resolve each = 2 calls (NOT 4 — that would be 2 rows × 2 modes).
    assert len(resolve_calls) == 2, (
        f"expected 2 resolve calls (1 per row), got {len(resolve_calls)}: {resolve_calls}"
    )
    assert resolve_calls == ["cohort0", "cohort1"]

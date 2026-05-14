"""Tests for the multi-platform dispatch helpers in main.py."""
import os
import sys
from unittest.mock import MagicMock, patch
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import main
from src.ad_platform import CreateAdResult, AdPlatformClient, LINKEDIN_CONSTRAINTS
from src.targeting_resolver import TargetingResolver


# ── Test doubles ──────────────────────────────────────────────────────────────


class _FakePlatformClient(AdPlatformClient):
    name = "fake"
    constraints = LINKEDIN_CONSTRAINTS

    def __init__(self):
        self.calls: list[tuple[str, dict]] = []
        self._counter = 0

    def _next_id(self, prefix: str) -> str:
        self._counter += 1
        return f"{prefix}-{self._counter}"

    def create_campaign_group(self, name: str, *, geos: list[str] | None = None) -> str:
        self.calls.append(("create_campaign_group", {"name": name}))
        return self._next_id("group")

    def create_campaign(self, name, campaign_group_id, targeting, daily_budget_cents=5000):
        self.calls.append(("create_campaign", {"name": name, "group": campaign_group_id, "targeting": targeting}))
        return self._next_id("campaign")

    def upload_image(self, image_path):
        self.calls.append(("upload_image", {"path": str(image_path)}))
        return self._next_id("img")

    def create_image_ad(self, campaign_id, image_id, headline, description, **kwargs):
        self.calls.append(("create_image_ad", {
            "campaign": campaign_id, "image": image_id, "headline": headline, **kwargs,
        }))
        return CreateAdResult(creative_id=self._next_id("ad"), status="ok")


class _FakeResolver(TargetingResolver):
    name = "fake"

    def resolve_cohort(self, cohort, geos=None, exclude_pairs=None):
        return {"countries": list(geos or []), "rules": list(getattr(cohort, "rules", []) or [])}


class _FakeCohort:
    def __init__(self, name, stg_id):
        self.name = name
        self._stg_name = name
        self._stg_id = stg_id
        self.rules = [("skills__python", "python")]
        self.id = stg_id


class _FakeGeoGroup:
    def __init__(self, label, geos, suffix):
        self.cluster_label = label
        self.cluster = label.lower() if label != "Global" else "global_mix"
        self.geos = geos
        self.advertised_rate = "$50/hr"
        self.campaign_suffix = suffix


# ── Tests ─────────────────────────────────────────────────────────────────────


@pytest.fixture(autouse=True)
def _no_real_drive_or_anthropic(monkeypatch):
    """Module-wide isolation: _process_extra_platform_arm makes 3 kinds of
    real network calls if not mocked:
      1. upload_text_in_hierarchy (Phase 1 manifest write)
      2. _save_creative_to_drive (per-ad PNG upload)
      3. adapt_copy_for_platform (Claude API for copy rewrite)

    Flipping GDRIVE_ENABLED=False skips both Drive paths via existing guards;
    adapt_copy_for_platform needs a direct stub."""
    import config
    monkeypatch.setattr(config, "GDRIVE_ENABLED", False)
    monkeypatch.setattr("src.gdrive.upload_text_in_hierarchy",
                        lambda **kw: "stub://drive-manifest")
    monkeypatch.setattr("src.copy_adapter.adapt_copy_for_platform",
                        lambda variant, platform, **kw: variant)


@pytest.fixture
def png(tmp_path):
    p = tmp_path / "creative.png"
    # Tiny valid PNG header so .exists() check passes — content unused.
    p.write_bytes(b"\x89PNG\r\n\x1a\n")
    return p


def _make_specs(png_path) -> list[dict]:
    cohort = _FakeCohort(name="cohort_a", stg_id="STG-A")
    geo = _FakeGeoGroup(label="English-speaking", geos=["US", "CA"], suffix="anglo")
    return [
        {
            "cohort":      cohort,
            "geo_group":   geo,
            "group_geos":  geo.geos,
            "angle_idx":   0,
            "angle_label": "A",
            "variants":    [{
                "angle": "A", "headline": "h", "subheadline": "s",
                "photo_subject": "subject", "intro_text": "i",
                "ad_headline": "ah", "ad_description": "ad",
            }],
            "png_path":    png_path,
        },
    ]


def test_extra_platform_arm_creates_full_chain(png):
    """End-to-end: arm calls group → campaign → upload → ad → registry."""
    client   = _FakePlatformClient()
    resolver = _FakeResolver()

    with patch("main._reg_log_inmail", create=True), \
         patch("src.campaign_registry.log_campaign") as mock_log:
        out = main._process_extra_platform_arm(
            platform="meta",
            client=client, resolver=resolver,
            campaign_specs=_make_specs(png),
            flow_id="F1", location="US",
            ramp_id="RAMP", cohort_id_override=None,
            destination_url_override="https://example.com",
        )

    # Group + campaign + upload + ad MUST be called exactly once.
    op_names = [c[0] for c in client.calls]
    assert op_names == ["create_campaign_group", "create_campaign", "upload_image", "create_image_ad"]
    assert out["campaigns"] == ["campaign-2"]
    assert out["campaign_groups"] == ["group-1"]
    # Registry MUST be told this is a Meta row.
    assert mock_log.call_args.kwargs["platform"] == "meta"
    assert mock_log.call_args.kwargs["platform_campaign_id"] == "campaign-2"
    assert mock_log.call_args.kwargs["platform_creative_id"] == "ad-4"


def test_extra_platform_arm_isolates_per_cohort_geo_failures(png):
    """A failed create_campaign for one (cohort × geo) group must NOT abort
    other groups. Under the grouped structure, all 3 angles within a single
    (cohort × geo) share one ad set — so isolation is at the group level."""
    client = _FakePlatformClient()
    orig = client.create_campaign

    def boom(name, *a, **kw):
        if "BOOM" in name:
            raise RuntimeError("simulated platform 500")
        return orig(name, *a, **kw)
    client.create_campaign = boom  # type: ignore[assignment]

    cohort_ok = _FakeCohort(name="cohort_ok",   stg_id="STG-OK")
    cohort_x  = _FakeCohort(name="cohort_BOOM", stg_id="STG-BOOM")
    geo = _FakeGeoGroup(label="English-speaking", geos=["US"], suffix="anglo")
    specs = []
    for cohort in (cohort_ok, cohort_x):
        for i, label in enumerate(("A", "B", "C")):
            specs.append({
                "cohort": cohort, "geo_group": geo, "group_geos": geo.geos,
                "angle_idx": i, "angle_label": label,
                "variants": [{"angle": label, "headline": "h"}],
                "png_path": png,
            })

    with patch("src.campaign_registry.log_campaign"):
        out = main._process_extra_platform_arm(
            platform="meta",
            client=client, resolver=_FakeResolver(),
            campaign_specs=specs,
            flow_id="F1", location="US",
            ramp_id="RAMP", cohort_id_override=None,
            destination_url_override=None,
        )
    # 2 (cohort × geo) groups, 1 fails → exactly 1 ad set created (for cohort_ok).
    assert len(out["campaigns"]) == 1


def test_extra_platform_arm_attaches_3_ads_per_group(png):
    """One (cohort × geo) group with 3 angles must produce 1 ad set + 3 ads."""
    client = _FakePlatformClient()
    cohort = _FakeCohort(name="cohort_a", stg_id="STG-A")
    geo = _FakeGeoGroup(label="English-speaking", geos=["US"], suffix="anglo")
    specs = [
        {"cohort": cohort, "geo_group": geo, "group_geos": geo.geos,
         "angle_idx": i, "angle_label": label,
         "variants": [{"angle": label, "headline": f"h_{label}"}],
         "png_path": png}
        for i, label in enumerate(("A", "B", "C"))
    ]
    with patch("src.campaign_registry.log_campaign") as mock_log:
        out = main._process_extra_platform_arm(
            platform="meta",
            client=client, resolver=_FakeResolver(),
            campaign_specs=specs,
            flow_id="F1", location="US",
            ramp_id="RAMP", cohort_id_override=None,
            destination_url_override=None,
        )
    # 1 group → 1 create_campaign call, 3 ads.
    op_names = [c[0] for c in client.calls]
    assert op_names.count("create_campaign") == 1
    assert op_names.count("upload_image") == 3
    assert op_names.count("create_image_ad") == 3
    # 3 ad-level registry rows (one per angle), all sharing the same
    # platform_campaign_id. The arm ALSO logs one campaign_type="parent"
    # row for the platform-level group container — exclude it from the
    # ad-row count.
    ad_calls = [c for c in mock_log.call_args_list
                if c.kwargs.get("campaign_type") != "parent"]
    assert len(ad_calls) == 3
    pcids = {c.kwargs["platform_campaign_id"] for c in ad_calls}
    assert len(pcids) == 1
    pcrids = {c.kwargs["platform_creative_id"] for c in ad_calls}
    assert len(pcrids) == 3   # creative ids differ per angle


def test_extra_platform_arm_skips_when_no_specs():
    out = main._process_extra_platform_arm(
        platform="meta",
        client=_FakePlatformClient(), resolver=_FakeResolver(),
        campaign_specs=[],
        flow_id="F1", location="US",
        ramp_id="RAMP", cohort_id_override=None,
        destination_url_override=None,
    )
    assert out["campaigns"] == []
    assert out["campaign_groups"] == []


def test_extra_platform_arm_local_fallback_when_no_png(tmp_path):
    """If png_path is missing, registry MUST still log the campaign with
    local_fallback status — pipeline keeps moving."""
    client = _FakePlatformClient()
    cohort = _FakeCohort("c1", "STG-X")
    geo = _FakeGeoGroup("English-speaking", ["US"], "anglo")
    specs = [{
        "cohort": cohort, "geo_group": geo, "group_geos": ["US"],
        "angle_idx": 0, "angle_label": "A",
        "variants": [{"angle": "A", "headline": "h"}],
        "png_path": None,
    }]
    with patch("src.campaign_registry.log_campaign") as mock_log:
        out = main._process_extra_platform_arm(
            platform="meta",
            client=client, resolver=_FakeResolver(),
            campaign_specs=specs,
            flow_id="F1", location="US",
            ramp_id="RAMP", cohort_id_override=None,
            destination_url_override=None,
        )
    # campaign was created; ad was not (no PNG); registry got logged with
    # platform_creative_id empty.
    assert out["campaigns"] == ["campaign-2"]
    assert mock_log.call_args.kwargs["platform_creative_id"] == ""


def test_build_extra_platform_clients_skips_when_creds_missing(monkeypatch):
    import config
    monkeypatch.setattr(config, "META_ACCESS_TOKEN", "")
    monkeypatch.setattr(config, "GOOGLE_ADS_DEVELOPER_TOKEN", "")
    out = main._build_extra_platform_clients(["meta", "google"])
    assert out == {}


def test_build_extra_platform_clients_skips_unknown_platform():
    out = main._build_extra_platform_clients(["snapchat"])
    assert out == {}

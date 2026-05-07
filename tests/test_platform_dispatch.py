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

    def create_campaign_group(self, name: str) -> str:
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


def test_extra_platform_arm_isolates_per_spec_failures(png):
    """A spec that throws inside create_campaign must NOT abort sibling specs."""
    client = _FakePlatformClient()
    # Make the second create_campaign call raise.
    orig = client.create_campaign

    def boom(name, *a, **kw):
        if "B" in name:
            raise RuntimeError("simulated platform 500")
        return orig(name, *a, **kw)
    client.create_campaign = boom  # type: ignore[assignment]

    cohort = _FakeCohort(name="cohort_a", stg_id="STG-A")
    geo = _FakeGeoGroup(label="English-speaking", geos=["US"], suffix="anglo")
    specs = [
        {"cohort": cohort, "geo_group": geo, "group_geos": geo.geos,
         "angle_idx": i, "angle_label": label,
         "variants": [{"angle": label, "headline": "h"}],
         "png_path": png}
        for i, label in enumerate(("A", "B", "C"))
    ]

    with patch("src.campaign_registry.log_campaign"):
        out = main._process_extra_platform_arm(
            platform="meta",
            client=client, resolver=_FakeResolver(),
            campaign_specs=specs,
            flow_id="F1", location="US",
            ramp_id="RAMP", cohort_id_override=None,
            destination_url_override=None,
        )
    # 3 specs, 1 fails → 2 successes.
    assert len(out["campaigns"]) == 2


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

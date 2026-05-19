"""Phase 7 — update_campaign_budget across LinkedIn / Meta / Google clients.

Covers:
- LinkedInClient.update_campaign_budget: builds the right PARTIAL_UPDATE
  payload and money formatting (cents → $X.XX major-unit string).
- MetaClient.update_campaign_budget: refuses 0/negative budgets; routes
  through AdSet.api_update with daily_budget in cents.
- GoogleAdsClient.update_campaign_budget: refuses negative budgets; resolves
  the CampaignBudget resource via Google Ads search and routes through
  CampaignBudgetService.mutate_campaign_budgets with cents * 10_000 micros.

All three clients are exercised with mock platforms — no live HTTP / no
live Google Ads SDK call.
"""
from __future__ import annotations

import sys
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))


# ── LinkedIn ─────────────────────────────────────────────────────────────────

def test_linkedin_update_campaign_budget_builds_correct_patch(monkeypatch):
    from src.linkedin_api import LinkedInClient

    client = LinkedInClient(token="dummy")
    captured: dict = {}

    def fake_req(method, url, **kwargs):
        captured["method"] = method
        captured["url"] = url
        captured["headers"] = kwargs.get("headers")
        captured["json"] = kwargs.get("json")
        return SimpleNamespace(ok=True, status_code=200, text="")

    monkeypatch.setattr(client, "_req", fake_req)
    client.update_campaign_budget("urn:li:sponsoredCampaign:9876543", 5000)

    assert captured["method"] == "POST"
    assert "adCampaigns/9876543" in captured["url"]
    assert captured["headers"]["X-RestLi-Method"] == "PARTIAL_UPDATE"
    # Money is a decimal string in MAJOR units ("$50.00", not 5000).
    money = captured["json"]["patch"]["$set"]["dailyBudget"]
    assert money == {"currencyCode": "USD", "amount": "50.00"}


def test_linkedin_update_campaign_budget_accepts_bare_id(monkeypatch):
    from src.linkedin_api import LinkedInClient

    client = LinkedInClient(token="dummy")
    captured_url: list[str] = []
    monkeypatch.setattr(
        client, "_req",
        lambda method, url, **kw: (captured_url.append(url),
                                   SimpleNamespace(ok=True, status_code=200, text=""))[1],
    )
    client.update_campaign_budget("12345678", 1000)
    assert captured_url[0].endswith("/adCampaigns/12345678")


def test_linkedin_update_campaign_budget_rejects_negative():
    from src.linkedin_api import LinkedInClient
    client = LinkedInClient(token="dummy")
    with pytest.raises(ValueError):
        client.update_campaign_budget("urn:li:sponsoredCampaign:1", -100)


def test_linkedin_update_campaign_budget_allows_zero(monkeypatch):
    """LinkedIn allows 0 budget at the API surface (whether the account
    enforces a minimum is a separate concern — the caller handles that)."""
    from src.linkedin_api import LinkedInClient
    client = LinkedInClient(token="dummy")
    captured: dict = {}
    monkeypatch.setattr(client, "_req",
                        lambda method, url, **kw: (captured.update(kw),
                                                   SimpleNamespace(ok=True, status_code=200, text=""))[1])
    client.update_campaign_budget("urn:li:sponsoredCampaign:1", 0)
    money = captured["json"]["patch"]["$set"]["dailyBudget"]
    assert money == {"currencyCode": "USD", "amount": "0.00"}


# ── Meta ─────────────────────────────────────────────────────────────────────

def test_meta_update_campaign_budget_routes_through_adset(monkeypatch):
    from src import meta_api
    client = meta_api.MetaClient(access_token="dummy", ad_account_id="1234")

    # Stub _ensure_init so we don't hit Facebook SDK init.
    monkeypatch.setattr(client, "_ensure_init", lambda: None)

    # Patch AdSet so .api_update can be observed.
    fake_adset_instance = MagicMock()
    fake_adset_cls = MagicMock(return_value=fake_adset_instance)
    # The class also exposes .Field used in the params dict.
    fake_adset_cls.Field = SimpleNamespace(daily_budget="daily_budget")

    import facebook_business.adobjects.adset as adset_module
    monkeypatch.setattr(adset_module, "AdSet", fake_adset_cls)

    client.update_campaign_budget("987654321", 5000)

    fake_adset_cls.assert_called_with("987654321")
    fake_adset_instance.api_update.assert_called_once_with(
        params={"daily_budget": 5000}
    )


def test_meta_update_campaign_budget_rejects_zero():
    from src import meta_api
    client = meta_api.MetaClient(access_token="dummy", ad_account_id="1234")
    with pytest.raises(ValueError):
        client.update_campaign_budget("987", 0)


def test_meta_update_campaign_budget_rejects_negative():
    from src import meta_api
    client = meta_api.MetaClient(access_token="dummy", ad_account_id="1234")
    with pytest.raises(ValueError):
        client.update_campaign_budget("987", -50)


# ── Google ───────────────────────────────────────────────────────────────────

def _fake_google_client_with_budget_lookup(
    monkeypatch,
    budget_resource: str = "customers/1234567890/campaignBudgets/55",
):
    """Build a mock google.ads.googleads client where:
      - GoogleAdsService.search yields one row whose campaign.campaign_budget
        is `budget_resource`,
      - CampaignBudgetService.mutate_campaign_budgets returns a results list
        with the same resource_name (mimicking a successful update),
      - get_type('CampaignBudgetOperation') returns a fresh op whose .update
        is a SimpleNamespace we can read after the call.

    Also patches google.api_core.protobuf_helpers.field_mask to a no-op
    that returns a stub FieldMask — the real helper deep-copies + Clears
    the input proto, which our SimpleNamespace fake doesn't support.
    """
    # Patch protobuf_helpers.field_mask so it doesn't try to introspect our
    # fake proto. The src code only uses the return value as input to
    # update_mask.CopyFrom, which our fake handles.
    from google.api_core import protobuf_helpers as _ph
    monkeypatch.setattr(_ph, "field_mask",
                        lambda original, modified: SimpleNamespace(paths=["amount_micros"]))

    op_state = SimpleNamespace(
        resource_name="", amount_micros=0,
        _pb=SimpleNamespace(),  # placeholder
    )
    update_mask_paths: list[str] = []
    op_state.update_mask = SimpleNamespace(
        paths=update_mask_paths,
        CopyFrom=lambda other: update_mask_paths.append("amount_micros"),
    )

    operation = SimpleNamespace(update=op_state, update_mask=op_state.update_mask)

    budget_service = MagicMock()
    budget_service.mutate_campaign_budgets.return_value = SimpleNamespace(
        results=[SimpleNamespace(resource_name=budget_resource)]
    )

    ga_service = MagicMock()
    ga_service.search.return_value = iter([
        SimpleNamespace(campaign=SimpleNamespace(campaign_budget=budget_resource)),
    ])

    types_registry = {
        "CampaignBudgetOperation": operation,
    }

    services_registry = {
        "GoogleAdsService": ga_service,
        "CampaignBudgetService": budget_service,
    }

    fake_sdk_client = MagicMock()
    fake_sdk_client.get_service = lambda name: services_registry[name]
    fake_sdk_client.get_type = lambda name: types_registry[name]

    return fake_sdk_client, operation, budget_service, ga_service


def test_google_update_campaign_budget_mutates_correct_budget(monkeypatch):
    from src.google_ads_api import GoogleAdsClient
    client = GoogleAdsClient(
        client_id="x", client_secret="x", developer_token="x",
        refresh_token="x", customer_id="1234567890", login_customer_id=None,
    )

    fake_sdk_client, operation, budget_service, ga_service = (
        _fake_google_client_with_budget_lookup(monkeypatch)
    )
    client._client = fake_sdk_client  # short-circuit _ensure_client

    client.update_campaign_budget("customers/1234567890/campaigns/99", 5000)

    # search() called with a query scoped to that campaign
    args, kwargs = ga_service.search.call_args
    assert kwargs["customer_id"] == "1234567890"
    assert "customers/1234567890/campaigns/99" in kwargs["query"]

    # mutate_campaign_budgets called with our op + 5000 cents → 50_000_000 micros
    args, kwargs = budget_service.mutate_campaign_budgets.call_args
    assert kwargs["customer_id"] == "1234567890"
    assert kwargs["operations"] == [operation]
    assert operation.update.amount_micros == 5000 * 10_000


def test_google_update_campaign_budget_accepts_bare_numeric_id(monkeypatch):
    from src.google_ads_api import GoogleAdsClient
    client = GoogleAdsClient(
        client_id="x", client_secret="x", developer_token="x",
        refresh_token="x", customer_id="1234567890", login_customer_id=None,
    )

    fake_sdk_client, _op, _budget_svc, ga_service = (
        _fake_google_client_with_budget_lookup(monkeypatch)
    )
    client._client = fake_sdk_client

    client.update_campaign_budget("99", 2000)

    _args, kwargs = ga_service.search.call_args
    assert "customers/1234567890/campaigns/99" in kwargs["query"]


def test_google_update_campaign_budget_rejects_negative():
    from src.google_ads_api import GoogleAdsClient
    client = GoogleAdsClient(
        client_id="x", client_secret="x", developer_token="x",
        refresh_token="x", customer_id="1234567890", login_customer_id=None,
    )
    with pytest.raises(ValueError):
        client.update_campaign_budget("99", -1)


def test_google_update_campaign_budget_raises_when_no_budget_link(monkeypatch):
    """If the campaign exists but has no campaign_budget reference (empty
    search results), the method should raise — there's nothing to mutate."""
    from src.google_ads_api import GoogleAdsClient
    client = GoogleAdsClient(
        client_id="x", client_secret="x", developer_token="x",
        refresh_token="x", customer_id="1234567890", login_customer_id=None,
    )

    fake_sdk_client = MagicMock()
    ga_service = MagicMock()
    ga_service.search.return_value = iter([])  # no rows
    fake_sdk_client.get_service = lambda name: ga_service if name == "GoogleAdsService" else MagicMock()
    fake_sdk_client.get_type = lambda name: MagicMock()
    client._client = fake_sdk_client

    with pytest.raises(RuntimeError, match="no campaign_budget"):
        client.update_campaign_budget("99", 5000)

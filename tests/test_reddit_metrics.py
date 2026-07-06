"""Reddit impressions/clicks/spend fetcher (platform_metrics._fetch_reddit_metrics).

sign-ups/activations come from the funnel (funnel_writeback), so this fetcher
reports applications=0 and only fills the platform-side metrics."""

import config
import src.platform_metrics as pm
import src.reddit_api as ra


class _FakeReddit:
    def fetch_campaign_metrics(self, window_days=7):
        return {"111": {"impressions": 5000, "clicks": 40, "spend_usd": 120.5}}


def _patch(monkeypatch, enabled=True):
    monkeypatch.setattr(ra, "RedditClient", _FakeReddit)
    monkeypatch.setattr(config, "REDDIT_API_ENABLED", enabled, raising=False)
    pm._reddit_metrics_cache.clear()


def test_fetch_reddit_metrics_maps_campaign(monkeypatch):
    _patch(monkeypatch)
    assert pm._fetch_reddit_metrics("111", 7) == {
        "impressions": 5000, "clicks": 40, "spend_usd": 120.5, "applications": 0}


def test_fetch_reddit_metrics_unknown_campaign(monkeypatch):
    _patch(monkeypatch)
    assert pm._fetch_reddit_metrics("999", 7) is None


def test_fetch_reddit_metrics_disabled(monkeypatch):
    _patch(monkeypatch, enabled=False)
    assert pm._fetch_reddit_metrics("111", 7) is None


def test_reporting_fetch_cached_once(monkeypatch):
    """One reporting call per window is reused across rows."""
    _patch(monkeypatch)
    calls = {"n": 0}

    class _Counting(_FakeReddit):
        def fetch_campaign_metrics(self, window_days=7):
            calls["n"] += 1
            return super().fetch_campaign_metrics(window_days)

    monkeypatch.setattr(ra, "RedditClient", _Counting)
    pm._reddit_metrics_cache.clear()
    pm._fetch_reddit_metrics("111", 7)
    pm._fetch_reddit_metrics("111", 7)
    assert calls["n"] == 1

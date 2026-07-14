"""Unit tests for Reddit + YouTube video-metric mapping + aggregation."""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.video_format_metrics as vf


def test_reddit_metrics_mapping():
    row = {
        "impressions": 1000, "clicks": 20, "spend": 5_000_000,  # micros → $5
        "video_started": 800, "video_watched_3_seconds": 600,
        "video_watched_25_percent": 500, "video_watched_50_percent": 300,
        "video_watched_75_percent": 200, "video_watched_100_percent": 120,
    }
    m = vf._reddit_metrics(row)
    assert m["imp"] == 1000 and m["clk"] == 20 and m["spend"] == 5.0
    assert m["plays"] == 800          # video_started
    assert m["v3"] == 600             # 3-second views
    assert m["thru"] == 120           # fully viewed == 100%
    assert (m["p25"], m["p50"], m["p75"], m["p100"]) == (500, 300, 200, 120)
    assert m["ws"] is None and m["rx"] is None   # unsupported → blank


def test_youtube_metrics_mapping():
    # quartile rates are proportions of impressions → counts = round(rate*imp)
    m = vf._youtube_metrics(2000, 40, 8_000_000, 0.5, 0.3, 0.2, 0.1)
    assert m["imp"] == 2000 and m["clk"] == 40 and m["spend"] == 8.0
    assert (m["p25"], m["p50"], m["p75"], m["p100"]) == (1000, 600, 400, 200)
    # play-denominated metrics unsupported on YouTube → blank
    assert m["plays"] is None and m["v3"] is None and m["thru"] is None and m["ws"] is None


def test_fold_aggregates_and_dates():
    m1 = vf._reddit_metrics({"impressions": 100, "clicks": 1, "spend": 0,
                             "video_started": 90, "video_watched_3_seconds": 50,
                             "video_watched_25_percent": 40, "video_watched_50_percent": 30,
                             "video_watched_75_percent": 20, "video_watched_100_percent": 10})
    m2 = vf._reddit_metrics({"impressions": 200, "clicks": 3, "spend": 0,
                             "video_started": 150, "video_watched_3_seconds": 80,
                             "video_watched_25_percent": 60, "video_watched_50_percent": 40,
                             "video_watched_75_percent": 25, "video_watched_100_percent": 15})
    entries = [
        {"ramp_id": "GMR-0030", "channel": "Reddit", "locale": "German",
         "metric_date": "2026-07-12", "m": m1},
        {"ramp_id": "GMR-0030", "channel": "Reddit", "locale": "German",
         "metric_date": "2026-07-10", "m": m2},
    ]
    folded = vf._fold(entries)
    assert len(folded) == 1
    row = folded[0]
    assert row["ramp_id"] == "GMR-0030" and row["locale"] == "German"
    assert row["launched"] == "2026-07-10" and row["last"] == "2026-07-12"
    assert row["days"] == 2
    assert row["m"]["imp"] == 300 and row["m"]["plays"] == 240 and row["m"]["p100"] == 25
    assert row["m"]["ws"] is None   # all-None metric stays blank after fold


def test_locale_of_falls_back():
    assert vf._locale_of("Scale-GMR-0030 | Reddit | bn-IN") == "Bengali"
    assert vf._locale_of("Scale-GMR-0030 | Reddit | no-locale-token") == "(unspecified)"


# ── targeting resolver (lives in the refresh script) ──
import importlib.util
_spec = importlib.util.spec_from_file_location(
    "rg", os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                       "scripts", "refresh_meta_video_gsheet.py"))
rg = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(rg)


def test_clean_icp_drops_campaign_names():
    assert rg._clean_icp("Bengali generalist contributors") == "Bengali generalist contributors"
    assert rg._clean_icp("Scale-GMR-0023 | Meta | bn-IN") == ""          # has pipe + gmr
    assert rg._clean_icp("scale-gmr-0023+|+meta+|+bn-in") == ""          # slugified/lowercased
    assert rg._clean_icp("") == ""


def test_targeting_for_channel_and_fallback():
    import collections
    idx = collections.defaultdict(lambda: collections.defaultdict(set))
    # Meta row for (GMR-0023, Bengali)
    idx[("GMR-0023", "Bengali", "meta")]["icp"].add("Bengali generalist contributors")
    idx[("GMR-0023", "Bengali", "meta")]["geo"].add("South Asian (BD, IN)")
    idx[("GMR-0023", "Bengali", "meta")]["rate"].add("$5.50/hr")
    idx[("GMR-0023", "Bengali", "meta")]["meta_audience_size"].add(192250000)
    # channel-agnostic fallback bucket
    idx[("GMR-0023", "Bengali", "*")]["icp"].add("Bengali generalist contributors")
    idx[("GMR-0023", "Bengali", "*")]["geo"].add("South Asian (BD, IN)")
    idx[("GMR-0023", "Bengali", "*")]["rate"].add("$5.50/hr")

    meta = rg._targeting_for(idx, "GMR-0023", "Bengali", "Meta")
    assert meta["icp"] == "Bengali generalist contributors"
    assert meta["audience"] == 192250000 and meta["rate"] == "$5.50/hr"

    # Reddit has no registry rows → ICP/geo/rate fall back to the agnostic bucket,
    # audience stays blank (Reddit has no audience field).
    reddit = rg._targeting_for(idx, "GMR-0023", "Bengali", "Reddit")
    assert reddit["icp"] == "Bengali generalist contributors"
    assert reddit["geo"] == "South Asian (BD, IN)" and reddit["audience"] is None


# ── Meta live-targeting summariser (creative_format_metrics) ──
import src.creative_format_metrics as cf


def test_meta_targeting_broad_adset():
    # Shape observed live for a broad Bengali ad set (geo + age + exclusions + Advantage+).
    data = {"age_min": 18, "age_max": 65, "geo_locations": {"countries": ["IN"]},
            "excluded_custom_audiences": [{"name": "Generalists Actives"}],
            "targeting_automation": {"advantage_audience": 1}}
    s = cf._render_targeting([cf._targeting_components(data)])
    assert "Geo: IN" in s and "Age 18–65" in s and "All genders" in s
    assert "Advantage+ audience ON" in s and "Excludes 1 audience(s)" in s
    assert "Interests" not in s   # none set → not shown


def test_meta_targeting_interests_and_lookalike_fold():
    a = {"age_min": 25, "age_max": 55, "genders": [2],
         "geo_locations": {"countries": ["US", "CA"]},
         "flexible_spec": [{"interests": [{"name": "Artificial intelligence"}, {"name": "Data science"}]}]}
    b = {"age_min": 18, "age_max": 65, "genders": [2],
         "geo_locations": {"countries": ["US"]},
         "custom_audiences": [{"name": "LAL 1% Signups"}]}
    s = cf._render_targeting([cf._targeting_components(a), cf._targeting_components(b)])
    assert s.startswith("2 ad sets")
    assert "Age 18–65" in s          # min-of-mins .. max-of-maxs across ad sets
    assert "Women" in s
    assert "Artificial intelligence, Data science" in s
    assert "Custom/Lookalike: LAL 1% Signups" in s

"""Issue #75 — exact utm_campaign attribution + relaunch supersede.

Root-cause fix for the #74 date-stripping workaround:
  1. log_campaign persists the exact stamped utm_campaign (defaults to
     campaign_name, which equals the stamped value at every ad site).
  2. The funnel/delivery join matches the warehouse's UTM_CAMPAIGN against the
     stored utm_campaign (by="utm") — EXACT, so each launch generation keeps its
     own attribution instead of fuzzily merging across the drifting date token.
  3. Relaunch retains the prior generation (status="superseded"), so its
     historical conversions still attribute to it, and it's excluded from the
     next relaunch's archive list.
"""
import os, sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import src.campaign_registry as reg
import src.ui_decisions as ui


def _stub_registry(monkeypatch, rows):
    saved = {}
    monkeypatch.setattr(reg, "_load", lambda: rows)
    monkeypatch.setattr(reg, "_save", lambda rs: saved.__setitem__("rows", rs))
    monkeypatch.setattr(ui, "upsert_campaign", lambda rec: saved.setdefault("pg", []).append(rec))
    return saved


# ── 1. log_campaign persists utm_campaign ──────────────────────────────────

def test_log_campaign_defaults_utm_to_campaign_name(monkeypatch):
    saved = {}
    monkeypatch.setattr(reg, "_load", lambda: [])
    monkeypatch.setattr(reg, "_save", lambda rs: saved.__setitem__("rows", rs))
    monkeypatch.setattr(reg, "_get_sheets", lambda: type("S", (), {"write_registry_row": lambda self, r: None})())
    monkeypatch.setattr("src.ui_decisions.upsert_campaign", lambda rec: None)
    name = "Scale-GMR-0099 | Meta | id-ID | ALL | 07/07/2026"
    reg.log_campaign(
        smart_ramp_id="GMR-0099", cohort_id="c", cohort_signature="sig",
        geo_cluster="g", geo_cluster_label="G", geos=["ID"], angle="A",
        campaign_type="static", advertised_rate="", platform="meta",
        platform_campaign_id="123", campaign_name=name,
    )
    assert saved["rows"][0]["utm_campaign"] == name          # defaulted from campaign_name


def test_log_campaign_explicit_utm_wins(monkeypatch):
    saved = {}
    monkeypatch.setattr(reg, "_load", lambda: [])
    monkeypatch.setattr(reg, "_save", lambda rs: saved.__setitem__("rows", rs))
    monkeypatch.setattr(reg, "_get_sheets", lambda: type("S", (), {"write_registry_row": lambda self, r: None})())
    monkeypatch.setattr("src.ui_decisions.upsert_campaign", lambda rec: None)
    reg.log_campaign(
        smart_ramp_id="GMR-0099", cohort_id="c", cohort_signature="sig",
        geo_cluster="g", geo_cluster_label="G", geos=["ID"], angle="A",
        campaign_type="static", advertised_rate="", platform="meta",
        platform_campaign_id="123", campaign_name="display name",
        utm_campaign="exact | stamped | value",
    )
    assert saved["rows"][0]["utm_campaign"] == "exact | stamped | value"


# ── 1b. Canonicalization (live drift cases, validated 2026-07-07) ───────────

def test_canonical_utm_fixes_encoding_and_whitespace_drift():
    C = reg._canonical_utm
    canon = "scale-gmr-0011 | reddit | coder | tier 3 coders | en | en | hcc | us | conv | 05/01/2026"
    # Title-case registry name canonicalizes to the lowercased warehouse form
    assert C("Scale-GMR-0011 | Reddit | coder | Tier 3 Coders | en | EN | HCC | US | Conv | 05/01/2026") == canon
    # %7c / %20 / %2f encoded variant
    assert C("scale-gmr-0011%20%7c%20reddit%20%7c%20coder%20%7c%20tier%203%20coders%20%7c%20en%20%7c%20en%20%7c%20hcc%20%7c%20us%20%7c%20conv%20%7c%2005%2f01%2f2026") == canon
    # form-encoded +|+ pipes and + spaces
    assert C("scale-gmr-0011+|+reddit+|+coder+|+tier+3+coders+|+en+|+en+|+hcc+|+us+|+conv+|+05/01/2026") == canon


def test_canonical_utm_strips_trailing_pipe_and_doubled_separators():
    C = reg._canonical_utm
    base = C("scale-gmr-0023 | linkedin | language | kn-in | 06/03/2026")
    assert C("scale-gmr-0023 | linkedin | language | kn-in | 06/03/2026 | ") == base   # trailing pipe (GMR-0023 kn-in)
    assert C("scale-gmr-0023 || linkedin |  language | kn-in | 06/03/2026") == base    # doubled/whitespace


def test_canonical_utm_keeps_date_token_distinct_generations():
    """The whole reason we DON'T reuse _normalize_campaign_name: canonicalization
    must NOT strip the date, so two generations stay distinct keys."""
    C = reg._canonical_utm
    assert C("x | linkedin | en-us | 06/03/2026") != C("x | linkedin | en-us | 06/13/2026")


# ── 2. Exact utm join ───────────────────────────────────────────────────────

def test_by_utm_exact_match_first_only(monkeypatch):
    """by='utm' matches the stored utm_campaign verbatim (lowercased); campaign-
    level, so only the first matching row is written."""
    rows = [{"utm_campaign": "Scale-GMR-0023 | Meta | id-ID | 07/07/2026", "angle": "A"},
            {"utm_campaign": "Scale-GMR-0023 | Meta | id-ID | 07/07/2026", "angle": "B"}]
    saved = _stub_registry(monkeypatch, rows)
    n = reg.update_funnel_metrics(
        "scale-gmr-0023 | meta | id-id | 07/07/2026", by="utm",
        applications=100, skill_passes=40, activations=12)
    assert n == 1
    assert saved["rows"][0]["activations"] == 12 and "activations" not in saved["rows"][1]


def test_by_utm_falls_back_to_campaign_name(monkeypatch):
    """Rows created before the utm_campaign column existed have it empty; the
    matcher falls back to campaign_name (== the stamped value)."""
    rows = [{"campaign_name": "Scale-GMR-0023 | LinkedIn | en-US | 06/03/2026", "utm_campaign": ""}]
    saved = _stub_registry(monkeypatch, rows)
    n = reg.update_funnel_metrics(
        "scale-gmr-0023 | linkedin | en-us | 06/03/2026", by="utm", activations=5)
    assert n == 1 and saved["rows"][0]["activations"] == 5


def test_by_utm_no_cross_generation_merge(monkeypatch):
    """The whole point of #75: two generations of the SAME campaign (different
    date tokens) each keep their own conversions — the old name_norm workaround
    would have merged them; exact utm keeps them separate."""
    gen1 = {"utm_campaign": "Scale-GMR-0023 | LinkedIn | en-US | 06/03/2026",
            "status": "superseded", "angle": "A"}
    gen2 = {"utm_campaign": "Scale-GMR-0023 | LinkedIn | en-US | 06/13/2026",
            "status": "active", "angle": "A"}
    saved = _stub_registry(monkeypatch, [gen1, gen2])
    # genesis (06/03) conversions attribute to the superseded gen1 row only
    reg.update_funnel_metrics(
        "scale-gmr-0023 | linkedin | en-us | 06/03/2026", by="utm", activations=66)
    # relaunch (06/13) conversions attribute to gen2 only
    reg.update_funnel_metrics(
        "scale-gmr-0023 | linkedin | en-us | 06/13/2026", by="utm", activations=9)
    assert gen1["activations"] == 66 and gen2["activations"] == 9


def test_update_metrics_by_utm(monkeypatch):
    rows = [{"utm_campaign": "Scale-GMR-0023 | Reddit | en-US | 07/07/2026", "angle": "A"}]
    saved = _stub_registry(monkeypatch, rows)
    reg.update_metrics(
        "scale-gmr-0023 | reddit | en-us | 07/07/2026",
        impressions=1000, clicks=20, spend_usd=50.0, applications=4, by="utm")
    r = saved["rows"][0]
    assert r["impressions"] == 1000 and r["ctr_pct"] == 2.0 and r["cpa_usd"] == 12.5


# ── 3. Relaunch supersede (retain, don't delete) ────────────────────────────

class _Cur:
    def __init__(self, sink): self._sink = sink; self.rowcount = 1
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def execute(self, sql, params): self._sink["sql"] = sql; self._sink["params"] = list(params)
    def fetchall(self): return []


class _Conn:
    def __init__(self, sink): self._sink = sink
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return _Cur(self._sink)
    def commit(self): self._sink["committed"] = True


def test_supersede_sets_status_not_delete(monkeypatch):
    sink = {}
    monkeypatch.setattr(ui, "_connect", lambda: _Conn(sink))
    n = ui.supersede_campaign_rows("GMR-0023", "linkedin", ["urn:1", "urn:2"], reason="relaunch-replace linkedin")
    assert n == 1
    assert "UPDATE campaigns" in sink["sql"] and "DELETE" not in sink["sql"]
    assert '"superseded"' in sink["params"][0]                 # status jsonb value
    assert sink["params"][-1] == ["urn:1", "urn:2"]            # ids bound last


def test_supersede_empty_ids_noop(monkeypatch):
    sink = {}
    monkeypatch.setattr(ui, "_connect", lambda: _Conn(sink))
    assert ui.supersede_campaign_rows("GMR-0023", "linkedin", []) == 0
    assert "sql" not in sink                                    # never touched the DB


def test_list_campaign_ids_excludes_superseded(monkeypatch):
    """The next relaunch must not re-archive an already-superseded generation."""
    sink = {}
    monkeypatch.setattr(ui, "_connect", lambda: _Conn(sink))
    ui.list_campaign_platform_ids("GMR-0023", "linkedin")
    assert "superseded" in sink["sql"]

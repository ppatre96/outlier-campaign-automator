"""Tests for `src.campaign_registry.reconcile_creative_paths`.

The function backfills empty `creative_image_path` registry entries by
walking Drive at the canonical hierarchy `<ramp>/<platform>/<cohort_geo>/<angle>.png`.

Two paths must work:
  - Exact match: row has `cohort_geo` set (rows written after this PR).
  - Legacy positional match: row has no `cohort_geo`, so we pair candidate
    PNGs to empty rows by `(geo_cluster, angle)` ordered by `created_at`.
"""
from __future__ import annotations

import os
import sys
from unittest.mock import MagicMock

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest

import src.campaign_registry as reg


def _row(**over):
    base = {
        "smart_ramp_id":       "GMR-TEST",
        "cohort_id":           "cohort-x",
        "platform":            "linkedin",
        "geo_cluster":         "anglo",
        "angle":               "A",
        "created_at":          "2026-05-14 10:00 UTC",
        "campaign_type":       "static",
        "cohort_geo":          "",
        "creative_image_path": "",
    }
    base.update(over)
    return base


@pytest.fixture
def fake_drive(monkeypatch):
    """Stub out the Drive client + folder resolution so the test doesn't
    need real credentials. Yields a mutable list of folder/file dicts the
    test can populate to simulate Drive state."""
    drive_state = {
        "folders": {},   # folder_id -> [child dicts]
    }

    fake_svc = MagicMock()

    def files_list_mock(**kw):
        # The query is of the form "'<parent_id>' in parents and trashed = false"
        q = kw.get("q", "")
        parent_id = q.split("'")[1] if q.count("'") >= 2 else ""
        result = MagicMock()
        result.execute.return_value = {
            "files": drive_state["folders"].get(parent_id, []),
        }
        return result

    fake_svc.files.return_value.list = files_list_mock

    monkeypatch.setattr(reg, "_registry_lock", reg._registry_lock)  # keep real lock
    monkeypatch.setattr("src.gdrive._service", lambda: fake_svc)
    monkeypatch.setattr("src.gdrive._drive_id", lambda: None)
    monkeypatch.setattr("src.gdrive._root_parent", lambda: "ROOT")

    # Find-or-create just returns a stable derived id based on the path.
    def fake_find_or_create(name, parent_id, svc=None):
        return f"{parent_id}/{name}"
    monkeypatch.setattr("src.gdrive.find_or_create_folder", fake_find_or_create)

    return drive_state


def test_exact_match_on_cohort_geo(monkeypatch, fake_drive):
    """Row with cohort_geo set → direct lookup at <ramp>/<plat>/<cohort_geo>/<angle>.png."""
    # Drive state: ROOT/GMR-TEST/linkedin/STG-1__anglo/A.png
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld1", "name": "STG-1__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld1"] = [
        {"id": "png1", "name": "A.png", "mimeType": "image/png",
         "webViewLink": "https://drive.example/png1", "createdTime": "2026-05-14T10:00:00Z"},
    ]

    records = [_row(cohort_geo="STG-1__anglo")]
    monkeypatch.setattr(reg, "_load", lambda: records)
    saved = []
    monkeypatch.setattr(reg, "_save", lambda rs: saved.append([dict(r) for r in rs]))

    stats = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert stats == {"patched": 1, "unmatched": 0, "ambiguous_legacy": 0}
    assert records[0]["creative_image_path"] == "https://drive.example/png1"
    assert saved, "save was not called"


def test_legacy_rows_left_alone_by_default(monkeypatch, fake_drive):
    """SAFE DEFAULT: rows without `cohort_geo` are NOT touched. Legacy
    positional fallback only activates when `legacy_positional=True`."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld_a", "name": "STG-a__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld_a"] = [{
        "id": "pA", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pA", "createdTime": "2026-05-14T10:00:00Z",
    }]
    records = [_row(angle="A", created_at="2026-05-14 09:55 UTC")]  # no cohort_geo
    monkeypatch.setattr(reg, "_load", lambda: records)
    saved = []
    monkeypatch.setattr(reg, "_save", lambda rs: saved.append(rs))

    stats = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert stats == {"patched": 0, "unmatched": 0, "ambiguous_legacy": 0}
    assert records[0]["creative_image_path"] == ""
    assert not saved


def test_legacy_positional_match_when_opted_in(monkeypatch, fake_drive):
    """Opt-in legacy mode: pair rows to PNGs by (geo, angle) + time window."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld_a", "name": "STG-a__anglo", "mimeType": "application/vnd.google-apps.folder"},
        {"id": "fld_b", "name": "STG-b__anglo", "mimeType": "application/vnd.google-apps.folder"},
        {"id": "fld_c", "name": "STG-c__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    # Three PNGs within the 60-minute window relative to row created_at.
    fake_drive["folders"]["fld_a"] = [{
        "id": "pA", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pA", "createdTime": "2026-05-14T10:00:00Z",
    }]
    fake_drive["folders"]["fld_b"] = [{
        "id": "pB", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pB", "createdTime": "2026-05-14T10:01:00Z",
    }]
    fake_drive["folders"]["fld_c"] = [{
        "id": "pC", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pC", "createdTime": "2026-05-14T10:02:00Z",
    }]

    records = [
        _row(angle="A", created_at="2026-05-14 09:50 UTC"),
        _row(angle="A", created_at="2026-05-14 09:51 UTC"),
        _row(angle="A", created_at="2026-05-14 09:52 UTC"),
    ]
    monkeypatch.setattr(reg, "_load", lambda: records)
    monkeypatch.setattr(reg, "_save", lambda rs: None)

    stats = reg.reconcile_creative_paths(
        "GMR-TEST", "linkedin", legacy_positional=True,
    )
    assert stats["patched"] == 3
    assert stats["unmatched"] == 0
    assert stats["ambiguous_legacy"] == 3
    assert records[0]["creative_image_path"] == "https://drive.example/pA"
    assert records[1]["creative_image_path"] == "https://drive.example/pB"
    assert records[2]["creative_image_path"] == "https://drive.example/pC"


def test_legacy_window_guard_rejects_far_png(monkeypatch, fake_drive):
    """Opt-in legacy mode: PNG outside the time window is NOT matched."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld_old", "name": "STG-old__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld_old"] = [{
        "id": "pOld", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pOld",
        "createdTime": "2026-05-13T03:00:00Z",  # 31 hours before the row
    }]
    records = [_row(angle="A", created_at="2026-05-14 10:00 UTC")]
    monkeypatch.setattr(reg, "_load", lambda: records)
    monkeypatch.setattr(reg, "_save", lambda rs: None)

    stats = reg.reconcile_creative_paths(
        "GMR-TEST", "linkedin", legacy_positional=True,
        legacy_window_minutes=60,
    )
    assert stats["patched"] == 0
    assert stats["unmatched"] == 1
    assert records[0]["creative_image_path"] == ""


def test_no_match_leaves_row_empty(monkeypatch, fake_drive):
    """If Drive has no PNG matching the row, creative_image_path stays empty
    and the row is counted as unmatched."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = []
    records = [_row(cohort_geo="STG-x__anglo")]
    monkeypatch.setattr(reg, "_load", lambda: records)
    monkeypatch.setattr(reg, "_save", lambda rs: None)

    stats = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert stats == {"patched": 0, "unmatched": 1, "ambiguous_legacy": 0}
    assert records[0]["creative_image_path"] == ""


def test_already_populated_rows_left_alone(monkeypatch, fake_drive):
    """Rows that already have creative_image_path are NOT touched, even if
    Drive has a candidate at the matching coords."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld1", "name": "STG-1__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld1"] = [{
        "id": "png1", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/new",
        "createdTime": "2026-05-14T10:00:00Z",
    }]

    records = [_row(cohort_geo="STG-1__anglo",
                    creative_image_path="https://drive.example/preserved")]
    monkeypatch.setattr(reg, "_load", lambda: records)
    saved = []
    monkeypatch.setattr(reg, "_save", lambda rs: saved.append(rs))

    stats = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert stats == {"patched": 0, "unmatched": 0, "ambiguous_legacy": 0}
    assert records[0]["creative_image_path"] == "https://drive.example/preserved"
    assert not saved, "no patch happened, _save must not be called"


def test_legacy_positional_idempotent_no_url_duplication(monkeypatch, fake_drive):
    """REGRESSION: legacy_positional mode must NOT reassign URLs that are
    already in the registry. Running twice with the same Drive + partially-
    populated registry must not produce duplicate URL ↔ row mappings."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld_a", "name": "STG-a__anglo", "mimeType": "application/vnd.google-apps.folder"},
        {"id": "fld_b", "name": "STG-b__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld_a"] = [{
        "id": "pA", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pA", "createdTime": "2026-05-14T10:00:00Z",
    }]
    fake_drive["folders"]["fld_b"] = [{
        "id": "pB", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/pB", "createdTime": "2026-05-14T10:01:00Z",
    }]
    # Pass 1: 2 empty rows + 2 PNGs. Pass 2 same Drive state, but row 1
    # already has /pA assigned (simulating partial pre-run state).
    records = [
        _row(angle="A", created_at="2026-05-14 09:50 UTC",
             creative_image_path="https://drive.example/pA"),
        _row(angle="A", created_at="2026-05-14 09:51 UTC"),
    ]
    monkeypatch.setattr(reg, "_load", lambda: records)
    monkeypatch.setattr(reg, "_save", lambda rs: None)

    stats = reg.reconcile_creative_paths(
        "GMR-TEST", "linkedin", legacy_positional=True,
    )
    # /pA is already in the registry — must NOT be reassigned. /pB is free.
    assert records[0]["creative_image_path"] == "https://drive.example/pA"
    assert records[1]["creative_image_path"] == "https://drive.example/pB"
    # All URLs are unique across rows.
    urls = [r["creative_image_path"] for r in records]
    assert len(set(urls)) == len(urls), f"duplicate URL assignment: {urls}"


def test_idempotent_second_pass(monkeypatch, fake_drive):
    """Running reconcile twice on the same state patches nothing new on pass 2."""
    fake_drive["folders"]["ROOT/GMR-TEST/linkedin"] = [
        {"id": "fld1", "name": "STG-1__anglo", "mimeType": "application/vnd.google-apps.folder"},
    ]
    fake_drive["folders"]["fld1"] = [{
        "id": "png1", "name": "A.png", "mimeType": "image/png",
        "webViewLink": "https://drive.example/png1",
        "createdTime": "2026-05-14T10:00:00Z",
    }]

    records = [_row(cohort_geo="STG-1__anglo")]
    monkeypatch.setattr(reg, "_load", lambda: records)
    monkeypatch.setattr(reg, "_save", lambda rs: None)

    first = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert first["patched"] == 1

    second = reg.reconcile_creative_paths("GMR-TEST", "linkedin")
    assert second["patched"] == 0

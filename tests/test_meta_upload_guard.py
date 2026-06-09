"""Guard: MetaClient.upload_image refuses thumbnail-resolution creatives.

Tuan flagged (GMR-0023, 2026-06-09) that native-language B/C ad variants were
uploaded at 64x64 and rendered pixelated. Real pipeline creatives are >=1080 on
every side, so a sub-600px image is a thumbnail and must never reach Meta — it
should raise so the arm's verify-and-heal surfaces the reason instead of
shipping a pixelated ad.
"""
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from PIL import Image

import config
from src.meta_api import MetaClient


def _png(path, size):
    Image.new("RGB", size, (128, 128, 128)).save(path)
    return str(path)


def _client(monkeypatch):
    c = MetaClient(access_token="x", ad_account_id="act_1", api_version="v21.0", page_id="p")
    monkeypatch.setattr(c, "_ensure_init", lambda: None)
    return c


def test_upload_rejects_64x64_thumbnail(monkeypatch, tmp_path):
    c = _client(monkeypatch)
    thumb = _png(tmp_path / "ko-KR_B.png", (64, 64))
    with pytest.raises(ValueError, match="below the .* minimum"):
        c.upload_image(thumb)


def test_upload_accepts_full_res(monkeypatch, tmp_path):
    c = _client(monkeypatch)
    full = _png(tmp_path / "ko-KR_A.png", (1080, 1350))

    # Mock the SDK boundary so no network call happens.
    import facebook_business.adobjects.adimage as adimage_mod
    import facebook_business.adobjects.adaccount as adaccount_mod

    class _FakeImage(dict):
        class Field:
            filename = "filename"
            hash = "hash"
        def __init__(self, parent_id=None):
            super().__init__()
        def remote_create(self):
            self["hash"] = "abc123"
    monkeypatch.setattr(adimage_mod, "AdImage", _FakeImage)
    monkeypatch.setattr(adaccount_mod, "AdAccount", lambda _id: object())

    assert c.upload_image(full) == "abc123"

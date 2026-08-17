"""Regression tests for the .env token persistence that CI silently skipped."""
from __future__ import annotations
import sys
from pathlib import Path

_ROOT = Path(__file__).resolve().parent.parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))


def test_creates_env_when_missing(tmp_path, monkeypatch):
    """CI has no .env — the old early-return discarded the refreshed token, so
    both workflows' Doppler persist steps read an absent file."""
    import src.linkedin_api as LI
    env = tmp_path / ".env"
    monkeypatch.setattr(LI, "_ENV_FILE", env)
    assert LI._update_env_token("acc-new", "ref-new") is True
    body = env.read_text()
    assert "LINKEDIN_ACCESS_TOKEN=acc-new" in body
    assert "LINKEDIN_REFRESH_TOKEN=ref-new" in body


def test_replaces_existing_values_and_keeps_other_keys(tmp_path, monkeypatch):
    import src.linkedin_api as LI
    env = tmp_path / ".env"
    env.write_text(
        "OTHER_KEY=keepme\n"
        "LINKEDIN_ACCESS_TOKEN=old-acc\n"
        "LINKEDIN_REFRESH_TOKEN=old-ref\n"
    )
    monkeypatch.setattr(LI, "_ENV_FILE", env)
    assert LI._update_env_token("acc-new", "ref-new") is True
    body = env.read_text()
    assert "OTHER_KEY=keepme" in body
    assert "old-acc" not in body and "old-ref" not in body
    assert body.count("LINKEDIN_ACCESS_TOKEN=") == 1


def test_appends_when_env_exists_without_the_keys(tmp_path, monkeypatch):
    """The regex-only path no-op'd here, dropping the token silently."""
    import src.linkedin_api as LI
    env = tmp_path / ".env"
    env.write_text("OTHER_KEY=keepme\n")
    monkeypatch.setattr(LI, "_ENV_FILE", env)
    assert LI._update_env_token("acc-new", "ref-new") is True
    body = env.read_text()
    assert "OTHER_KEY=keepme" in body
    assert "LINKEDIN_ACCESS_TOKEN=acc-new" in body


def test_unwritable_path_reports_failure_instead_of_claiming_success(tmp_path, monkeypatch):
    import src.linkedin_api as LI
    monkeypatch.setattr(LI, "_ENV_FILE", tmp_path / "no-such-dir" / ".env")
    assert LI._update_env_token("a", "b") is False

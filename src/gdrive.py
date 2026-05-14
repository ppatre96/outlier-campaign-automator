"""Google Drive upload helper for Outlier ad creatives.

Uses the service account already configured in GOOGLE_CREDENTIALS.

IMPORTANT — target must be a Google Workspace Shared Drive:
  Personal Drive folders don't allocate quota to service accounts; uploads
  fail with `storageQuotaExceeded`. Configure via:
    GDRIVE_DRIVE_ID  = the Shared Drive ID (e.g. 0ALHAgK4RPbnfUk9PVA)
    GDRIVE_FOLDER_ID = optional sub-folder root inside that drive
                       (defaults to the Shared Drive root)

Folder hierarchy created automatically per upload:
    <Shared Drive root>/
      └── <ramp_id>/
          └── <channel>/        # linkedin | meta | google
              └── <cohort_geo>/ # one folder per (cohort_id × geo_cluster)
                  └── <angle>.png

Usage:
    from src.gdrive import upload_creative_in_hierarchy
    url = upload_creative_in_hierarchy(
        file_path=Path("/tmp/foo.png"),
        ramp_id="GMR-0011",
        channel="linkedin",
        cohort_geo="STG-001__anglo",
        angle="A",
    )
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials

import config

log = logging.getLogger(__name__)

# Need full drive scope for Shared Drive member access (drive.file is too narrow).
_SCOPES = ["https://www.googleapis.com/auth/drive"]

_FOLDER_MIME = "application/vnd.google-apps.folder"

# Cache: (parent_id, name) → folder_id. Saves the find-or-create lookups
# from re-hitting the API for every creative within the same run.
_folder_cache: dict[tuple[str, str], str] = {}
# Phase 3.4 — guard against ramp-parallel threads both missing the cache
# for the same (parent, name) and both creating a duplicate folder.
_folder_cache_lock = threading.Lock()


def _service():
    creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def _drive_id() -> str | None:
    """Shared Drive id (corpora=drive). None means use 'My Drive' (no Shared Drive)."""
    return getattr(config, "GDRIVE_DRIVE_ID", "") or None


def _root_parent() -> str:
    """Where the agent-created hierarchy roots: GDRIVE_FOLDER_ID if set,
    else the Shared Drive root id, else 'root' (My Drive)."""
    return (
        getattr(config, "GDRIVE_FOLDER_ID", "")
        or _drive_id()
        or "root"
    )


def find_or_create_folder(name: str, parent_id: str, svc=None) -> str:
    """Return the id of the folder named `name` under `parent_id`. Creates
    it if missing. Caches results per process to avoid re-querying."""
    cache_key = (parent_id, name)
    # Fast path: cache hit.
    if cache_key in _folder_cache:
        return _folder_cache[cache_key]

    with _folder_cache_lock:
        # Double-checked: another thread may have created the folder while
        # we were waiting on the lock.
        if cache_key in _folder_cache:
            return _folder_cache[cache_key]

        svc = svc or _service()
        drive_id = _drive_id()

        # Look for an existing folder with this exact name under the parent.
        # Escape single-quotes in the name to keep the Drive query syntax valid.
        safe_name = name.replace("'", r"\'")
        q = (
            f"name = '{safe_name}' "
            f"and mimeType = '{_FOLDER_MIME}' "
            f"and '{parent_id}' in parents "
            f"and trashed = false"
        )
        list_kwargs = {
            "q": q,
            "fields": "files(id,name)",
            "pageSize": 5,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if drive_id:
            list_kwargs["corpora"] = "drive"
            list_kwargs["driveId"] = drive_id
        resp = svc.files().list(**list_kwargs).execute()
        files = resp.get("files", [])
        if files:
            folder_id = files[0]["id"]
        else:
            meta = {"name": name, "mimeType": _FOLDER_MIME, "parents": [parent_id]}
            created = svc.files().create(
                body=meta, fields="id", supportsAllDrives=True,
            ).execute()
            folder_id = created["id"]
            log.info("Drive: created folder '%s' under %s → %s", name, parent_id, folder_id)
        _folder_cache[cache_key] = folder_id
        return folder_id


def _ensure_path(parts: list[str], svc=None) -> str:
    """Walk/create each folder name under root. Returns the deepest folder id."""
    svc = svc or _service()
    parent = _root_parent()
    for p in parts:
        parent = find_or_create_folder(p, parent, svc=svc)
    return parent


def upload_creative_in_hierarchy(
    file_path: Path,
    ramp_id: str,
    channel: str,
    cohort_geo: str,
    angle: str,
) -> str:
    """Upload a PNG into <root>/<ramp_id>/<channel>/<cohort_geo>/<angle>.png.

    Folders are find-or-created on each call (idempotent within a run). The
    file is renamed to `<angle>.png` to keep cohort folders tidy. Returns
    the file's webViewLink.
    """
    if not getattr(config, "GDRIVE_ENABLED", False):
        log.warning("GDRIVE_ENABLED=false — skipping Drive upload for %s", file_path.name)
        return ""

    svc = _service()
    target_folder = _ensure_path([ramp_id, channel, cohort_geo], svc=svc)

    # Filename inside the cohort folder: "A.png", "B.png", "C.png".
    # Sanitize angle in case caller passes "A " or similar.
    filename = f"{(angle or 'creative').strip()}.png"
    metadata = {"name": filename, "parents": [target_folder]}
    media = MediaFileUpload(str(file_path), mimetype="image/png", resumable=False)

    f = svc.files().create(
        body=metadata, media_body=media,
        fields="id,webViewLink", supportsAllDrives=True,
    ).execute()

    # Make publicly readable; ignore Shared Drive sharing-policy refusals.
    try:
        svc.permissions().create(
            fileId=f["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        if "publishOutNotPermitted" not in str(exc):
            log.warning("Drive permission grant failed (non-fatal): %s", exc)

    log.info(
        "Drive: uploaded %s → %s/%s/%s/%s (%s)",
        file_path.name, ramp_id, channel, cohort_geo, filename, f["id"],
    )
    return f.get("webViewLink", "")


# ── Slack notification queue (bot-tokenless delivery via RemoteTrigger) ──────


def _summary_signature(summary_text: str) -> str:
    """Short stable signature for dedup: first 12 hex chars of sha256(text)."""
    import hashlib
    return hashlib.sha256((summary_text or "").encode("utf-8")).hexdigest()[:12]


def _find_in_queue(svc, ramp_id: str, sig: str) -> dict | None:
    """Return {id,name,webViewLink,parent} of any file in pending/ or sent/
    whose filename starts with `<ramp_id>_<sig>_`. Used for (ramp, content)
    idempotency on enqueue — same content => skip a second write."""
    pending_id = _ensure_path(["_slack_queue", "pending"], svc=svc)
    sent_id    = _ensure_path(["_slack_queue", "sent"], svc=svc)
    drive_id = _drive_id()
    safe_prefix = f"{ramp_id or 'unknown'}_{sig}_".replace("'", r"\'")
    q = (
        f"name contains '{safe_prefix}' "
        f"and ('{pending_id}' in parents or '{sent_id}' in parents) "
        f"and trashed = false"
    )
    list_kwargs = {
        "q": q,
        "fields": "files(id,name,webViewLink,parents)",
        "pageSize": 5,
        "supportsAllDrives": True,
        "includeItemsFromAllDrives": True,
    }
    if drive_id:
        list_kwargs["corpora"] = "drive"
        list_kwargs["driveId"] = drive_id
    resp = svc.files().list(**list_kwargs).execute()
    files = resp.get("files", [])
    if not files:
        return None
    f = files[0]
    parents = f.get("parents") or []
    parent_folder = (
        "sent" if sent_id in parents else
        "pending" if pending_id in parents else
        "unknown"
    )
    return {
        "id":          f["id"],
        "name":        f.get("name", ""),
        "webViewLink": f.get("webViewLink", ""),
        "parent":      parent_folder,
    }


def _cleanup_pending_with_sent_twin(svc) -> int:
    """Delete pending/ files older than 1h that have a same-name twin in sent/.

    The Drive MCP exposed to the RemoteTrigger has no move/delete tool, so
    the trigger marks delivery by copying the pending file into sent/. This
    leaves the pending/ original around. We can delete it from the pipeline
    side (service account has full Drive scope, MCP doesn't).

    Returns the number of files deleted. Best-effort — never raises.
    """
    from datetime import datetime as _dt, timedelta as _td, timezone as _tz

    deleted = 0
    try:
        pending_id = _ensure_path(["_slack_queue", "pending"], svc=svc)
        sent_id    = _ensure_path(["_slack_queue", "sent"], svc=svc)
        drive_id   = _drive_id()
        cutoff = (_dt.now(_tz.utc) - _td(hours=1)).strftime("%Y-%m-%dT%H:%M:%SZ")

        list_kwargs = {
            "q": (
                f"'{pending_id}' in parents and trashed = false "
                f"and modifiedTime < '{cutoff}'"
            ),
            "fields": "files(id,name)",
            "pageSize": 100,
            "supportsAllDrives": True,
            "includeItemsFromAllDrives": True,
        }
        if drive_id:
            list_kwargs["corpora"] = "drive"
            list_kwargs["driveId"] = drive_id
        old_pending = svc.files().list(**list_kwargs).execute().get("files", [])

        for f in old_pending:
            name = f.get("name", "")
            if not name:
                continue
            safe_name = name.replace("'", r"\'")
            twin_q = (
                f"name = '{safe_name}' and '{sent_id}' in parents "
                f"and trashed = false"
            )
            twin_kwargs = {
                "q": twin_q,
                "fields": "files(id)",
                "pageSize": 1,
                "supportsAllDrives": True,
                "includeItemsFromAllDrives": True,
            }
            if drive_id:
                twin_kwargs["corpora"] = "drive"
                twin_kwargs["driveId"] = drive_id
            twins = svc.files().list(**twin_kwargs).execute().get("files", [])
            if not twins:
                continue
            try:
                # Service account has canTrash=True but canDelete=False on
                # this Shared Drive — use trash (move-to-trash), which excludes
                # the file from our `trashed = false` queries. Functionally
                # equivalent for the queue's purposes.
                svc.files().update(
                    fileId=f["id"], body={"trashed": True},
                    supportsAllDrives=True, fields="id",
                ).execute()
                deleted += 1
                log.info(
                    "Slack queue: trashed pending file %s (twin in sent/: %s)",
                    f["id"], twins[0]["id"],
                )
            except Exception as exc:
                log.warning(
                    "Slack queue: failed to trash pending file %s: %s",
                    f["id"], exc,
                )
    except Exception as exc:
        log.warning("Slack queue: pending cleanup pass failed: %s", exc)
    return deleted


def enqueue_slack_summary(
    *,
    ramp_id: str,
    summary_text: str,
    targets: list[dict],
) -> str:
    """Drop a Slack-summary payload into Drive at
    <root>/_slack_queue/pending/<ramp_id>_<sig>_<timestamp>.json so a separate
    RemoteTrigger cron can read and post it via the Claude.ai-inherited
    Slack MCP connector (no bot token required).

    Idempotency: if a file with the same `(ramp_id, sig)` already exists in
    pending/ or sent/, this function logs and returns the existing file's
    URL without writing a duplicate. `sig` is the first 12 hex chars of
    sha256(summary_text), so identical bodies dedup automatically.

    Schema of the queued file:
        {
          "ramp_id":      "GMR-0020",
          "queued_at":    ISO 8601 UTC,
          "signature":    "<12 hex chars of sha256(summary_text)>",
          "summary_text": "...full markdown body...",
          "targets":      [
            {"kind": "user",    "id": "U095J930UEL"},
            {"kind": "channel", "id": "C0B0NBB986L"},
            ...
          ]
        }

    Returns the Drive webViewLink of the queued (or pre-existing) file.

    The companion RemoteTrigger reads pending/, posts to Slack, and copies
    each handled file into sent/ as the "delivered" marker (the Drive MCP
    can't move/delete). The pipeline opportunistically deletes pending/
    files older than 1h that have a sent/ twin.
    """
    import json as _json
    from datetime import datetime as _dt, timezone as _tz

    if not getattr(config, "GDRIVE_ENABLED", False):
        log.warning("GDRIVE_ENABLED=false — Slack queue write skipped for %s", ramp_id)
        return ""

    svc = _service()
    sig = _summary_signature(summary_text)
    existing = _find_in_queue(svc, ramp_id, sig)
    if existing:
        log.info(
            "Slack queue: dedup hit — ramp=%s sig=%s already in %s/ (file=%s, name=%s); skipping enqueue",
            ramp_id, sig, existing["parent"], existing["id"], existing["name"],
        )
        _cleanup_pending_with_sent_twin(svc)
        return existing.get("webViewLink") or ""

    now_iso = _dt.now(_tz.utc).strftime("%Y-%m-%dT%H-%M-%SZ")
    filename = f"{ramp_id or 'unknown'}_{sig}_{now_iso}.json"
    body = _json.dumps(
        {
            "ramp_id":      ramp_id or "",
            "queued_at":    now_iso,
            "signature":    sig,
            "summary_text": summary_text or "",
            "targets":      list(targets or []),
        },
        indent=2, ensure_ascii=False,
    )

    import io
    from googleapiclient.http import MediaIoBaseUpload

    target_folder = _ensure_path(["_slack_queue", "pending"], svc=svc)
    metadata = {"name": filename, "parents": [target_folder]}
    media = MediaIoBaseUpload(
        io.BytesIO(body.encode("utf-8")),
        mimetype="application/json", resumable=False,
    )
    f = svc.files().create(
        body=metadata, media_body=media,
        fields="id,webViewLink", supportsAllDrives=True,
    ).execute()

    try:
        svc.permissions().create(
            fileId=f["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        if "publishOutNotPermitted" not in str(exc):
            log.warning("Drive permission grant failed (non-fatal): %s", exc)

    url = f.get("webViewLink", "")
    log.info(
        "Slack queue: enqueued summary for ramp=%s sig=%s → %s/%s (file=%s)",
        ramp_id, sig, "_slack_queue/pending", filename, f["id"],
    )
    _cleanup_pending_with_sent_twin(svc)
    return url


# ── Text/JSON manifest upload ────────────────────────────────────────────────


def upload_text_in_hierarchy(
    text: str,
    ramp_id: str,
    channel: str,
    filename: str,
    mimetype: str = "application/json",
) -> str:
    """Upload a text payload (JSON/CSV manifest, etc.) into
    <root>/<ramp_id>/<channel>/<filename>. Used by the Meta/Google graceful-
    degradation path to leave a manual-handoff manifest for human ops when
    the platform-side ad creation fails.

    Returns the file's webViewLink, empty string on disabled/error.
    """
    if not getattr(config, "GDRIVE_ENABLED", False):
        log.warning("GDRIVE_ENABLED=false — skipping Drive upload for %s", filename)
        return ""

    import io
    from googleapiclient.http import MediaIoBaseUpload

    svc = _service()
    target_folder = _ensure_path([ramp_id, channel], svc=svc)

    metadata = {"name": filename, "parents": [target_folder]}
    media = MediaIoBaseUpload(
        io.BytesIO(text.encode("utf-8")),
        mimetype=mimetype, resumable=False,
    )
    f = svc.files().create(
        body=metadata, media_body=media,
        fields="id,webViewLink", supportsAllDrives=True,
    ).execute()

    try:
        svc.permissions().create(
            fileId=f["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        if "publishOutNotPermitted" not in str(exc):
            log.warning("Drive permission grant failed (non-fatal): %s", exc)

    log.info(
        "Drive: uploaded manifest %s → %s/%s/%s (%s)",
        filename, ramp_id, channel, filename, f["id"],
    )
    return f.get("webViewLink", "")


# ── Back-compat wrapper used by existing callers ─────────────────────────────


def upload_creative(file_path: Path, folder_id: str = "") -> str:
    """Legacy single-folder upload. Kept for the campaign_registry image-embed
    path which doesn't have ramp/channel context. Prefer
    `upload_creative_in_hierarchy()` for pipeline creatives.
    """
    if not getattr(config, "GDRIVE_ENABLED", False):
        log.warning("GDRIVE_ENABLED=false — skipping Drive upload for %s", file_path.name)
        return ""
    folder_id = folder_id or _root_parent()
    svc = _service()
    metadata = {"name": file_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(file_path), mimetype="image/png", resumable=False)
    f = svc.files().create(
        body=metadata, media_body=media,
        fields="id,webViewLink", supportsAllDrives=True,
    ).execute()
    try:
        svc.permissions().create(
            fileId=f["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as exc:
        if "publishOutNotPermitted" not in str(exc):
            log.warning("Drive permission grant failed (non-fatal): %s", exc)
    return f.get("webViewLink", "")

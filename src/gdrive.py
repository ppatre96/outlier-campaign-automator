"""Google Drive upload helper for Outlier ad creatives.

Uses the service account already configured in GOOGLE_CREDENTIALS.

IMPORTANT — folder must be a Google Workspace Shared Drive (Team Drive):
  Personal Google Drive folders do not allocate storage quota to service
  accounts. The target folder must be inside a Shared Drive, with the
  service account added as a member (Content Manager or above).

  How to set up:
  1. In Google Drive → New → Shared Drive → create one (e.g. "Outlier Creatives")
  2. Members → Add → outlier-sheets-agent@outlier-campaign-agent.iam.gserviceaccount.com
     → give "Content Manager" role
  3. Move or create the target folder inside that Shared Drive
  4. Update GDRIVE_FOLDER_ID in .env to the new folder ID

Usage:
    from src.gdrive import upload_creative
    from pathlib import Path
    url = upload_creative(Path("data/experiment_outputs/exp_deepanshu_enUS_angleA.png"))
    print(url)  # https://drive.google.com/file/d/.../view
"""
from pathlib import Path

from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload
from google.oauth2.service_account import Credentials

import config

# drive (not drive.file) needed for Shared Drive member access
_SCOPES = ["https://www.googleapis.com/auth/drive"]


def _service():
    creds = Credentials.from_service_account_file(config.GOOGLE_CREDENTIALS, scopes=_SCOPES)
    return build("drive", "v3", credentials=creds, cache_discovery=False)


def upload_creative(file_path: Path, folder_id: str = "") -> str:
    """Upload a PNG creative to the shared Drive folder.

    Works with both Shared Drives (Team Drives) and personal Drive folders
    that the service account has been explicitly shared with.

    Args:
        file_path: Path to the PNG file.
        folder_id: Override the default folder ID from config.

    Returns:
        Web view URL (https://drive.google.com/file/d/.../view).

    Raises:
        HttpError 403 storageQuotaExceeded: folder is a personal Drive, not a
            Shared Drive. Move it to a Google Workspace Shared Drive and add
            the service account as a member.
    """
    folder_id = folder_id or config.GDRIVE_FOLDER_ID
    svc = _service()

    metadata = {"name": file_path.name, "parents": [folder_id]}
    media = MediaFileUpload(str(file_path), mimetype="image/png", resumable=False)

    f = svc.files().create(
        body=metadata,
        media_body=media,
        fields="id,webViewLink",
        supportsAllDrives=True,   # required for Shared Drives
    ).execute()

    # Make publicly readable (anyone with link can view)
    # Skip if Shared Drive policy restricts public sharing (publishOutNotPermitted)
    try:
        svc.permissions().create(
            fileId=f["id"],
            body={"type": "anyone", "role": "reader"},
            supportsAllDrives=True,
        ).execute()
    except Exception as e:
        if "publishOutNotPermitted" in str(e):
            # Shared Drive restricts public sharing — file is still accessible via link
            pass
        else:
            raise

    return f["webViewLink"]

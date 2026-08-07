"""
Google Drive Storage Adapter - implements StorageAdapter from manager.py.

The rest of the application NEVER imports this directly - it only talks
to StorageManager, which delegates here when STORAGE_PROVIDER=google_drive
is set in the environment. Swapping back to local storage (or forward to
S3/R2/MinIO) requires changing only that one env var, zero code changes.

Authentication: uses a Google Cloud service account JSON key file, path
configured via GOOGLE_DRIVE_CREDENTIALS_JSON env var. The service account
(drive-uploader@funding-arb-storage.iam.gserviceaccount.com) must have
Editor access on the target Drive folder.

The target folder is identified by GOOGLE_DRIVE_FOLDER_ID env var - copy
the long ID from the folder's URL in Google Drive
(https://drive.google.com/drive/folders/<THIS_PART>).

Remote keys (e.g. "delta/futures/BTC/2026/08/01/price.duckdb") are
mirrored as real folder hierarchies in Drive, so the archive is human-
browsable and not just a flat bucket of files. This costs a few extra
API calls per upload (one mkdir per folder level) but makes the archive
usable directly from Drive if needed.

Required packages (not yet in requirements.txt - added to the server
via ADDITIONAL PYTHON PACKAGES in HidenCloud Startup settings):
    google-auth google-auth-httplib2 google-api-python-client

Rate limits: Drive API has a 1000 req/100s quota per user. The daily
archiver only runs once per day per symbol, so even with hundreds of
symbols this stays well within limits. Resumable uploads (used for
files > 5MB) handle network interruptions gracefully.

FIX (2026-08-09): was missing the sys.path.append() line every other
src-importing script in this project has - "from src.storage.manager
import StorageAdapter" failed with ModuleNotFoundError: No module named
'src' the moment this ran anywhere except the exact directory layout
python happened to already have on its path. Added the same fix used
throughout the project.
"""
import os
import io
import hashlib
import json
import sys
import time
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.storage.manager import StorageAdapter

# Lazy imports - only loaded when this adapter is actually used,
# so the rest of the app can import manager.py without needing
# google-auth installed if they're using local storage.
_drive_service = None


def _get_service():
    global _drive_service
    if _drive_service is not None:
        return _drive_service

    try:
        from google.oauth2 import service_account
        from googleapiclient.discovery import build
    except ImportError:
        raise RuntimeError(
            "Google Drive packages not installed. Add to HidenCloud "
            "ADDITIONAL PYTHON PACKAGES: "
            "google-auth google-auth-httplib2 google-api-python-client"
        )

    creds_path = os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON")
    if not creds_path:
        raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS_JSON env var not set")
    if not Path(creds_path).exists():
        raise RuntimeError(f"Credentials file not found: {creds_path}")

    creds = service_account.Credentials.from_service_account_file(
        creds_path,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


class GoogleDriveAdapter(StorageAdapter):
    """
    Implements StorageAdapter using Google Drive as the backend.
    Folder hierarchy in Drive mirrors the remote_key path structure:
      delta/futures/BTC/2026/08/01/price.duckdb
    becomes nested folders in Drive with price.duckdb as the file.
    """

    def __init__(self, root_folder_id: str):
        self.root_folder_id = root_folder_id
        self._folder_cache: dict[str, str] = {}  # path -> Drive folder ID

    def _get_or_create_folder(self, name: str, parent_id: str) -> str:
        """Gets or creates a single folder by name under parent_id.
        Caches results to avoid redundant API calls within a session."""
        cache_key = f"{parent_id}/{name}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        svc = _get_service()
        query = (
            f"name='{name}' and "
            f"'{parent_id}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and "
            f"trashed=false"
        )
        results = svc.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])

        if files:
            folder_id = files[0]["id"]
        else:
            meta = {
                "name": name,
                "mimeType": "application/vnd.google-apps.folder",
                "parents": [parent_id],
            }
            folder = svc.files().create(body=meta, fields="id").execute()
            folder_id = folder["id"]

        self._folder_cache[cache_key] = folder_id
        return folder_id

    def _resolve_path(self, remote_key: str) -> tuple[str, str]:
        """Splits remote_key into (parent_folder_id, filename), creating
        intermediate folders in Drive as needed.
        e.g. 'delta/futures/BTC/2026/08/01/price.duckdb'
          -> (id of .../01/ folder, 'price.duckdb')"""
        parts = remote_key.replace("\\", "/").split("/")
        filename = parts[-1]
        folders = parts[:-1]

        current_parent = self.root_folder_id
        for folder_name in folders:
            current_parent = self._get_or_create_folder(folder_name, current_parent)

        return current_parent, filename

    def _find_file_id(self, parent_id: str, filename: str) -> Optional[str]:
        """Returns the Drive file ID if filename exists in parent_id, else None."""
        svc = _get_service()
        query = (
            f"name='{filename}' and "
            f"'{parent_id}' in parents and "
            f"trashed=false"
        )
        results = svc.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        return files[0]["id"] if files else None

    def upload(self, local_path: str, remote_key: str) -> bool:
        """Uploads local_path to Drive at remote_key. Uses resumable upload
        for robustness - handles network interruptions gracefully."""
        try:
            from googleapiclient.http import MediaFileUpload

            parent_id, filename = self._resolve_path(remote_key)
            file_size = Path(local_path).stat().st_size
            resumable = file_size > 5 * 1024 * 1024  # resumable above 5MB

            media = MediaFileUpload(
                local_path,
                mimetype="application/octet-stream",
                resumable=resumable,
            )

            existing_id = self._find_file_id(parent_id, filename)
            svc = _get_service()

            if existing_id:
                # Update existing file rather than creating a duplicate
                svc.files().update(
                    fileId=existing_id,
                    media_body=media,
                ).execute()
            else:
                meta = {"name": filename, "parents": [parent_id]}
                svc.files().create(
                    body=meta,
                    media_body=media,
                    fields="id",
                ).execute()

            print(f"    [drive] uploaded {remote_key} ({file_size:,} bytes)")
            return True
        except Exception as e:
            print(f"    [drive] upload failed ({remote_key}): {e}")
            return False

    def download(self, remote_key: str, local_path: str) -> bool:
        """Downloads remote_key from Drive to local_path."""
        try:
            from googleapiclient.http import MediaIoBaseDownload

            parent_id, filename = self._resolve_path(remote_key)
            file_id = self._find_file_id(parent_id, filename)
            if not file_id:
                print(f"    [drive] download: not found in Drive: {remote_key}")
                return False

            svc = _get_service()
            request = svc.files().get_media(fileId=file_id)

            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            with open(local_path, "wb") as f:
                downloader = MediaIoBaseDownload(f, request)
                done = False
                while not done:
                    _, done = downloader.next_chunk()

            print(f"    [drive] downloaded {remote_key} -> {local_path}")
            return True
        except Exception as e:
            print(f"    [drive] download failed ({remote_key}): {e}")
            return False

    def exists(self, remote_key: str) -> bool:
        try:
            parent_id, filename = self._resolve_path(remote_key)
            return self._find_file_id(parent_id, filename) is not None
        except Exception as e:
            print(f"    [drive] exists check failed ({remote_key}): {e}")
            return False

    def list_keys(self, prefix: str) -> list:
        """Lists all file keys under prefix. Recursively walks folders."""
        try:
            parts = prefix.strip("/").split("/")
            current_parent = self.root_folder_id
            svc = _get_service()

            for part in parts:
                if not part:
                    continue
                query = (
                    f"name='{part}' and "
                    f"'{current_parent}' in parents and "
                    f"mimeType='application/vnd.google-apps.folder' and "
                    f"trashed=false"
                )
                results = svc.files().list(q=query, fields="files(id)").execute()
                files = results.get("files", [])
                if not files:
                    return []
                current_parent = files[0]["id"]

            return self._list_recursive(current_parent, prefix.rstrip("/"))
        except Exception as e:
            print(f"    [drive] list_keys failed ({prefix}): {e}")
            return []

    def _list_recursive(self, folder_id: str, path_prefix: str) -> list:
        svc = _get_service()
        results = []
        page_token = None
        while True:
            kwargs = {
                "q": f"'{folder_id}' in parents and trashed=false",
                "fields": "nextPageToken, files(id, name, mimeType)",
            }
            if page_token:
                kwargs["pageToken"] = page_token
            response = svc.files().list(**kwargs).execute()
            for f in response.get("files", []):
                full_path = f"{path_prefix}/{f['name']}"
                if f["mimeType"] == "application/vnd.google-apps.folder":
                    results.extend(self._list_recursive(f["id"], full_path))
                else:
                    results.append(full_path)
            page_token = response.get("nextPageToken")
            if not page_token:
                break
        return results

    def delete(self, remote_key: str) -> bool:
        try:
            parent_id, filename = self._resolve_path(remote_key)
            file_id = self._find_file_id(parent_id, filename)
            if not file_id:
                return True  # already gone, that's fine
            _get_service().files().delete(fileId=file_id).execute()
            print(f"    [drive] deleted {remote_key}")
            return True
        except Exception as e:
            print(f"    [drive] delete failed ({remote_key}): {e}")
            return False


if __name__ == "__main__":
    """Standalone test - verifies the full round trip against real Drive.
    Requires GOOGLE_DRIVE_CREDENTIALS_JSON and GOOGLE_DRIVE_FOLDER_ID
    to be set in .env before running."""
    import tempfile
    from datetime import date
    from dotenv import load_dotenv
    load_dotenv("/home/container/.env")

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        print("GOOGLE_DRIVE_FOLDER_ID not set in .env - set it first.")
        exit(1)

    print("=" * 54)
    print("  GOOGLE DRIVE ADAPTER - STANDALONE TEST")
    print("=" * 54)

    adapter = GoogleDriveAdapter(root_folder_id=folder_id)
    test_key = "test/adapter-verify/2026/08/01/hello.txt"

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("Google Drive adapter test - if you see this in Drive, it works.")
        test_file = f.name

    print(f"\n[1] Uploading test file to Drive at key: {test_key}...")
    ok = adapter.upload(test_file, test_key)
    print(f"    {'PASS' if ok else 'FAIL'}")

    print(f"\n[2] Checking it exists in Drive...")
    exists = adapter.exists(test_key)
    print(f"    {'PASS' if exists else 'FAIL'}")

    print(f"\n[3] Downloading it back...")
    fetch_path = "/tmp/drive_test_fetch.txt"
    fetched = adapter.download(test_key, fetch_path)
    if fetched:
        content = open(fetch_path).read()
        original = open(test_file).read()
        print(f"    {'PASS - contents match' if content == original else 'FAIL - contents differ'}")
    else:
        print("    FAIL")

    print(f"\n[4] Listing keys under 'test/'...")
    keys = adapter.list_keys("test")
    print(f"    Found: {keys}")

    print(f"\n[5] Deleting the test file...")
    deleted = adapter.delete(test_key)
    print(f"    {'PASS' if deleted else 'FAIL'}")
    still_exists = adapter.exists(test_key)
    print(f"    Confirmed gone: {'PASS' if not still_exists else 'FAIL'}")

    print("\n" + "=" * 54)
    print("  All PASS = Google Drive adapter working end to end.")
    print("  Next: set STORAGE_PROVIDER=google_drive in .env and")
    print("  the DailyArchiver will automatically use Drive instead")
    print("  of local storage - zero other code changes needed.")
    print("=" * 54)

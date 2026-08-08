"""
Google Drive Storage Adapter - implements StorageAdapter from manager.py.

The rest of the application NEVER imports this directly - it only talks
to StorageManager, which delegates here when STORAGE_PROVIDER=google_drive
is set in the environment.

Authentication: GOOGLE_DRIVE_CREDENTIALS_JSON can be EITHER:
    (a) a real file path to the downloaded service-account .json key
        (recommended - upload the actual file via HidenCloud's File
        Manager, same reliable method used for every other file in
        this project, then point this at its path), OR
    (b) the raw JSON content itself, pasted directly into .env

FIX (2026-08-08): manually retyping/pasting the JSON content into .env
kept breaking - the key's private_key field contains escaped `\\n`
sequences that some apps silently convert into real line breaks when
copy-pasted, corrupting the JSON (this is what caused "Expecting value:
line 1 column 1" - the value wasn't valid JSON at all). Rather than
fight that fragile manual-paste process again, this now supports
reading directly from an uploaded file - upload works reliably every
time (used successfully all night for every other file in the project),
so this sidesteps the fragile-paste problem entirely. Content-based
.env values are still supported too, for consistency with how every
other credential in this project works, if you'd rather use that.

The target folder is identified by GOOGLE_DRIVE_FOLDER_ID env var - copy
the long ID from the folder's URL in Google Drive
(https://drive.google.com/drive/folders/<THIS_PART>).
"""
import os
import json
import sys
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.storage.manager import StorageAdapter

_drive_service = None


def _load_credentials_info() -> dict:
    """Reads GOOGLE_DRIVE_CREDENTIALS_JSON and figures out whether it's
    a file path or raw JSON content, then returns the parsed dict either
    way. Tries file-path first (the recommended, reliable route) since a
    value ending in .json that exists on disk is unambiguous."""
    raw = os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS_JSON env var not set")

    raw = raw.strip()

    # Looks like a file path (not starting with '{') - try reading it.
    if not raw.startswith("{"):
        path = Path(raw)
        if not path.exists():
            raise RuntimeError(
                f"GOOGLE_DRIVE_CREDENTIALS_JSON is set to '{raw}', which "
                f"looks like a file path but doesn't exist on disk. If you "
                f"meant to paste JSON content directly instead, it must "
                f"start with '{{' - check for accidental line breaks."
            )
        try:
            with open(path) as f:
                return json.load(f)
        except json.JSONDecodeError as e:
            raise RuntimeError(f"File at '{raw}' isn't valid JSON: {e}")

    # Starts with '{' - treat as raw JSON content pasted into .env.
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise RuntimeError(
            f"GOOGLE_DRIVE_CREDENTIALS_JSON isn't valid JSON ({e}). If "
            f"pasting the content directly is proving fragile (a common "
            f"issue - the key contains escaped newlines some apps corrupt "
            f"on copy/paste), try uploading the .json file via HidenCloud's "
            f"File Manager instead and point this variable at its file "
            f"path, e.g. /home/container/gdrive-credentials.json"
        )


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

    creds_info = _load_credentials_info()

    required_fields = ["type", "private_key", "client_email"]
    missing = [f for f in required_fields if f not in creds_info]
    if missing:
        raise RuntimeError(
            f"Credentials are missing field(s): {missing}. "
            f"This doesn't look like a complete service account key."
        )

    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    _drive_service = build("drive", "v3", credentials=creds, cache_discovery=False)
    return _drive_service


class GoogleDriveAdapter(StorageAdapter):
    def __init__(self, root_folder_id: str):
        self.root_folder_id = root_folder_id
        self._folder_cache: dict = {}

    def _get_or_create_folder(self, name: str, parent_id: str) -> str:
        cache_key = f"{parent_id}/{name}"
        if cache_key in self._folder_cache:
            return self._folder_cache[cache_key]

        svc = _get_service()
        query = (
            f"name='{name}' and '{parent_id}' in parents and "
            f"mimeType='application/vnd.google-apps.folder' and trashed=false"
        )
        results = svc.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])

        if files:
            folder_id = files[0]["id"]
        else:
            meta = {"name": name, "mimeType": "application/vnd.google-apps.folder", "parents": [parent_id]}
            folder = svc.files().create(body=meta, fields="id").execute()
            folder_id = folder["id"]

        self._folder_cache[cache_key] = folder_id
        return folder_id

    def _resolve_path(self, remote_key: str):
        parts = remote_key.replace("\\", "/").split("/")
        filename = parts[-1]
        folders = parts[:-1]

        current_parent = self.root_folder_id
        for folder_name in folders:
            current_parent = self._get_or_create_folder(folder_name, current_parent)

        return current_parent, filename

    def _find_file_id(self, parent_id: str, filename: str) -> Optional[str]:
        svc = _get_service()
        query = f"name='{filename}' and '{parent_id}' in parents and trashed=false"
        results = svc.files().list(q=query, fields="files(id)").execute()
        files = results.get("files", [])
        return files[0]["id"] if files else None

    def upload(self, local_path: str, remote_key: str) -> bool:
        try:
            from googleapiclient.http import MediaFileUpload

            parent_id, filename = self._resolve_path(remote_key)
            file_size = Path(local_path).stat().st_size
            resumable = file_size > 5 * 1024 * 1024

            media = MediaFileUpload(local_path, mimetype="application/octet-stream", resumable=resumable)
            existing_id = self._find_file_id(parent_id, filename)
            svc = _get_service()

            if existing_id:
                svc.files().update(fileId=existing_id, media_body=media).execute()
            else:
                meta = {"name": filename, "parents": [parent_id]}
                svc.files().create(body=meta, media_body=media, fields="id").execute()

            print(f"    [drive] uploaded {remote_key} ({file_size:,} bytes)")
            return True
        except Exception as e:
            print(f"    [drive] upload failed ({remote_key}): {type(e).__name__}: {e}")
            return False

    def download(self, remote_key: str, local_path: str) -> bool:
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
            print(f"    [drive] download failed ({remote_key}): {type(e).__name__}: {e}")
            return False

    def exists(self, remote_key: str) -> bool:
        try:
            parent_id, filename = self._resolve_path(remote_key)
            return self._find_file_id(parent_id, filename) is not None
        except Exception as e:
            print(f"    [drive] exists check failed ({remote_key}): {type(e).__name__}: {e}")
            return False

    def list_keys(self, prefix: str) -> list:
        try:
            parts = prefix.strip("/").split("/")
            current_parent = self.root_folder_id
            svc = _get_service()

            for part in parts:
                if not part:
                    continue
                query = (
                    f"name='{part}' and '{current_parent}' in parents and "
                    f"mimeType='application/vnd.google-apps.folder' and trashed=false"
                )
                results = svc.files().list(q=query, fields="files(id)").execute()
                files = results.get("files", [])
                if not files:
                    return []
                current_parent = files[0]["id"]

            return self._list_recursive(current_parent, prefix.rstrip("/"))
        except Exception as e:
            print(f"    [drive] list_keys failed ({prefix}): {type(e).__name__}: {e}")
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
                return True
            _get_service().files().delete(fileId=file_id).execute()
            print(f"    [drive] deleted {remote_key}")
            return True
        except Exception as e:
            print(f"    [drive] delete failed ({remote_key}): {type(e).__name__}: {e}")
            return False


if __name__ == "__main__":
    import tempfile
    from dotenv import load_dotenv
    load_dotenv("/home/container/.env")

    folder_id = os.getenv("GOOGLE_DRIVE_FOLDER_ID")
    if not folder_id:
        print("GOOGLE_DRIVE_FOLDER_ID not set in .env - set it first.")
        exit(1)

    print("=" * 54)
    print("  GOOGLE DRIVE ADAPTER - STANDALONE TEST")
    print("=" * 54)

    try:
        _get_service()
        print("\n[0] Credentials loaded and authenticated successfully.")
    except Exception as e:
        print(f"\n[0] FAILED to load credentials: {type(e).__name__}: {e}")
        print("\nStopping here - fix the credentials before continuing.")
        exit(1)

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
    print("=" * 54)

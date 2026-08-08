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

The target folder is identified by GOOGLE_DRIVE_FOLDER_ID env var - copy
the long ID from the folder's URL in Google Drive
(https://drive.google.com/drive/folders/<THIS_PART>).

FIX (2026-08-09): googleapiclient's HttpError has a known quirk - str(e)
can come back completely empty depending on the installed version, even
though the error is real and has useful detail (HTTP status code,
reason, and often a JSON body explaining exactly what's wrong). Every
except block below now specifically checks for HttpError and pulls
status/reason/content directly from its known attributes instead of
relying on str(e), which is what was producing "HttpError: " with
nothing useful after it. Also fixed the standalone test's summary,
which was unconditionally printing "All PASS" regardless of what
actually happened - it now tracks real results and only claims success
if every step actually passed. Added an isolated auth-only check
(Drive `about.get`) as test step [0b], since "credentials load fine but
every real operation fails identically" is the classic symptom of the
Drive API not being ENABLED for the Google Cloud project (a separate
step from creating credentials) - this narrows straight to that.
"""
import os
import json
import sys
from pathlib import Path
from typing import Optional

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.storage.manager import StorageAdapter

_drive_service = None


def _http_error_detail(e) -> str:
    """googleapiclient.errors.HttpError's str() can be empty even when
    there's real, useful detail sitting on the exception's own
    attributes - this pulls it out directly instead of trusting str()."""
    try:
        from googleapiclient.errors import HttpError
    except ImportError:
        return str(e)

    if isinstance(e, HttpError):
        status = getattr(e.resp, "status", "?")
        reason = getattr(e.resp, "reason", "")
        content = ""
        try:
            content = e.content.decode("utf-8") if isinstance(e.content, bytes) else str(e.content)
        except Exception:
            pass
        return f"HTTP {status} {reason} - {content}".strip(" -")
    return f"{type(e).__name__}: {e}"


def _load_credentials_info() -> dict:
    """Reads GOOGLE_DRIVE_CREDENTIALS_JSON and figures out whether it's
    a file path or raw JSON content, then returns the parsed dict either
    way. Tries file-path first (the recommended, reliable route) since a
    value ending in .json that exists on disk is unambiguous."""
    raw = os.getenv("GOOGLE_DRIVE_CREDENTIALS_JSON")
    if not raw:
        raise RuntimeError("GOOGLE_DRIVE_CREDENTIALS_JSON env var not set")

    raw = raw.strip()

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
            print(f"    [drive] upload failed ({remote_key}): {_http_error_detail(e)}")
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
            print(f"    [drive] download failed ({remote_key}): {_http_error_detail(e)}")
            return False

    def exists(self, remote_key: str) -> bool:
        try:
            parent_id, filename = self._resolve_path(remote_key)
            return self._find_file_id(parent_id, filename) is not None
        except Exception as e:
            print(f"    [drive] exists check failed ({remote_key}): {_http_error_detail(e)}")
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
            print(f"    [drive] list_keys failed ({prefix}): {_http_error_detail(e)}")
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
            print(f"    [drive] delete failed ({remote_key}): {_http_error_detail(e)}")
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
        svc = _get_service()
        print("\n[0] Credentials loaded and authenticated successfully.")
    except Exception as e:
        print(f"\n[0] FAILED to load credentials: {_http_error_detail(e)}")
        print("\nStopping here - fix the credentials before continuing.")
        exit(1)

    print("\n[0b] Testing a minimal, credentials-only Drive API call...")
    try:
        about = svc.about().get(fields="user").execute()
        print(f"     PASS - Drive API responded. Authenticated as: "
              f"{about.get('user', {}).get('emailAddress')}")
    except Exception as e:
        print(f"     FAIL: {_http_error_detail(e)}")
        print("\n     If this failed but [0] passed, the most common cause is:")
        print("     the Google Drive API isn't ENABLED for this project yet -")
        print("     having valid credentials is a separate step from turning")
        print("     the API itself on. Fix: console.cloud.google.com ->")
        print("     APIs & Services -> Enable APIs -> search 'Google Drive")
        print("     API' -> Enable. Then re-run this test - no restart needed,")
        print("     nothing on the server changes, only Google's side does.")
        print("\n" + "=" * 54)
        exit(1)

    adapter = GoogleDriveAdapter(root_folder_id=folder_id)
    test_key = "test/adapter-verify/2026/08/01/hello.txt"
    results = {}

    with tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False) as f:
        f.write("Google Drive adapter test - if you see this in Drive, it works.")
        test_file = f.name

    print(f"\n[1] Uploading test file to Drive at key: {test_key}...")
    results["upload"] = adapter.upload(test_file, test_key)
    print(f"    {'PASS' if results['upload'] else 'FAIL'}")

    print(f"\n[2] Checking it exists in Drive...")
    results["exists"] = adapter.exists(test_key)
    print(f"    {'PASS' if results['exists'] else 'FAIL'}")

    print(f"\n[3] Downloading it back...")
    fetch_path = "/tmp/drive_test_fetch.txt"
    fetched = adapter.download(test_key, fetch_path)
    if fetched:
        content = open(fetch_path).read()
        original = open(test_file).read()
        results["download"] = content == original
        print(f"    {'PASS - contents match' if results['download'] else 'FAIL - contents differ'}")
    else:
        results["download"] = False
        print("    FAIL")

    print(f"\n[4] Listing keys under 'test/'...")
    keys = adapter.list_keys("test")
    results["list"] = test_key in keys
    print(f"    Found: {keys}")
    print(f"    {'PASS' if results['list'] else 'FAIL - our test file not in the list'}")

    print(f"\n[5] Deleting the test file...")
    deleted = adapter.delete(test_key)
    still_exists = adapter.exists(test_key)
    results["delete"] = deleted and not still_exists
    print(f"    {'PASS' if results['delete'] else 'FAIL'}")
    print(f"    Confirmed gone: {'PASS' if not still_exists else 'FAIL'}")

    print("\n" + "=" * 54)
    if all(results.values()):
        print("  ALL PASS - Google Drive adapter working end to end.")
        print("  Next: set STORAGE_PROVIDER=google_drive in .env and")
        print("  the DailyArchiver will automatically use Drive instead")
        print("  of local storage - zero other code changes needed.")
    else:
        failed = [k for k, v in results.items() if not v]
        print(f"  NOT all passed - failed step(s): {failed}")
        print("  See the [!] messages above for the real HTTP error detail.")
    print("=" * 54)

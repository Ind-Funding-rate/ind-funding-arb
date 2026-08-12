"""
Google Drive OAuth (user) authentication - the correct approach for a
personal Gmail account.

Discovered necessary 2026-08-08: service-account auth failed with
"Service Accounts do not have storage quota" - a real, documented
Google restriction. Service accounts can only write into Shared Drives
(a paid Google Workspace feature, not available on free Gmail
accounts) or via domain-wide delegation (also Workspace-only).

This instead authenticates AS the actual Google account, via a
one-time browser consent screen you personally approve, and writes
using that account's own free 15GB Drive quota - the standard,
correct way this works for a personal account.

Flow (entirely through the live website - no script to run on your PC):
    1. Create an OAuth 2.0 Client ID (type: Web application) in Google
       Cloud Console, with an authorized redirect URI pointing at
       this server's /admin/gdrive-oauth-callback route.
    2. Upload the downloaded client_secret.json to the server, same
       way as the earlier service-account key file.
    3. Visit /admin/gdrive-oauth-start in your browser once - it
       redirects to Google's real consent screen; click Allow; Google
       redirects back with an authorization code.
    4. That callback exchanges the code for an access token + refresh
       token and saves them. The refresh token doesn't expire under
       normal use - this is a ONE-TIME step. After this, the adapter
       silently refreshes access tokens on its own, no browser needed
       again.
"""
import os
import json
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

SCOPES = ["https://www.googleapis.com/auth/drive"]


def _client_secrets_path() -> str:
    path = os.getenv("GOOGLE_OAUTH_CLIENT_SECRET_JSON")
    if not path:
        raise RuntimeError("GOOGLE_OAUTH_CLIENT_SECRET_JSON env var not set")
    if not Path(path).exists():
        raise RuntimeError(f"OAuth client secret file not found: {path}")
    return path


def _token_path() -> str:
    return os.getenv("GOOGLE_DRIVE_OAUTH_TOKEN_JSON", "/home/container/gdrive_oauth_token.json")


def build_auth_url(redirect_uri: str):
    """Returns (auth_url, flow). Visit auth_url in a browser to start
    the consent process."""
    from google_auth_oauthlib.flow import Flow

    flow = Flow.from_client_secrets_file(
        _client_secrets_path(),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )
    auth_url, state = flow.authorization_url(
        access_type="offline",        # required to receive a refresh_token
        include_granted_scopes="true",
        prompt="consent",             # forces a refresh_token even on repeat auth
    )
    return auth_url, flow


def exchange_code_for_token(redirect_uri: str, authorization_response_url: str):
    """Called from the callback route once Google redirects back with a
    code. Exchanges it for real tokens and saves them to disk."""
    from google_auth_oauthlib.flow import Flow

    flow = Flow.from_client_secrets_file(
        _client_secrets_path(),
        scopes=SCOPES,
        redirect_uri=redirect_uri,
    )
    flow.fetch_token(authorization_response=authorization_response_url)
    creds = flow.credentials

    token_data = {
        "token": creds.token,
        "refresh_token": creds.refresh_token,
        "token_uri": creds.token_uri,
        "client_id": creds.client_id,
        "client_secret": creds.client_secret,
        "scopes": creds.scopes,
    }
    with open(_token_path(), "w") as f:
        json.dump(token_data, f)

    return creds


def get_oauth_credentials():
    """Loads saved credentials, auto-refreshing the access token if
    expired using the stored refresh_token - no browser interaction
    needed for this, ever, unless access is explicitly revoked."""
    from google.oauth2.credentials import Credentials
    from google.auth.transport.requests import Request

    token_path = _token_path()
    if not Path(token_path).exists():
        raise RuntimeError(
            f"No saved OAuth token at {token_path} - visit "
            f"/admin/gdrive-oauth-start in your browser once to authorize."
        )

    with open(token_path) as f:
        data = json.load(f)

    creds = Credentials(
        token=data.get("token"),
        refresh_token=data.get("refresh_token"),
        token_uri=data.get("token_uri"),
        client_id=data.get("client_id"),
        client_secret=data.get("client_secret"),
        scopes=data.get("scopes"),
    )

    if creds.expired and creds.refresh_token:
        creds.refresh(Request())
        data["token"] = creds.token
        with open(token_path, "w") as f:
            json.dump(data, f)

    return creds


def has_valid_token() -> bool:
    return Path(_token_path()).exists()


if __name__ == "__main__":
    print("=" * 54)
    print("  GOOGLE DRIVE OAUTH - STATUS CHECK")
    print("=" * 54)
    from dotenv import load_dotenv
    load_dotenv("/home/container/.env")

    if has_valid_token():
        print("\n  Token file exists. Testing it still works (auto-refreshing")
        print("  if the access token has expired)...")
        try:
            creds = get_oauth_credentials()
            print(f"  PASS - credentials loaded, valid={creds.valid}")
        except Exception as e:
            print(f"  FAIL: {e}")
    else:
        print(f"\n  No token found at {_token_path()}")
        print("  Visit /admin/gdrive-oauth-start in your browser to authorize.")
    print("\n" + "=" * 54)

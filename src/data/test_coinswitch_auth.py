"""
CoinSwitch PRO API Authentication Test
---------------------------------------
Confirms:
1. .env keys are present
2. Server time is reachable (no auth needed)
3. API key + Ed25519 signature are valid (validate/keys endpoint)
4. Futures wallet balance can be read (proves Futures surface works)

This is READ-ONLY. No orders are placed. Safe to run anytime.

Auth scheme (from official docs, api-trading.coinswitch.co/get-started/authentication):
  signed_message = METHOD + path_with_query + epoch_ms
  signature = Ed25519(signed_message, secret_key), hex-encoded
  headers: X-AUTH-APIKEY, X-AUTH-SIGNATURE, X-AUTH-EPOCH, Content-Type
"""

import os
import time
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

load_dotenv("/home/container/.env")

API_KEY    = os.getenv("COINSWITCH_API_KEY")
SECRET_KEY = os.getenv("COINSWITCH_API_SECRET")

BASE_URL = "https://coinswitch.co"


def sign_request(method, path, params=None):
    """
    Builds the signed headers CoinSwitch requires on every authenticated
    request. Returns (headers, path_with_query) - use path_with_query
    when building the final URL so it matches exactly what was signed.
    """
    method = method.upper()

    if params:
        qs = "&".join(f"{k}={v}" for k, v in params.items())
        path_with_query = f"{path}?{qs}"
    else:
        path_with_query = path

    epoch = str(int(time.time() * 1000))
    signed_message = method + path_with_query + epoch

    private_key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(SECRET_KEY))
    signature = private_key.sign(signed_message.encode("utf-8"))
    signature_hex = signature.hex()

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature_hex,
        "X-AUTH-EPOCH": epoch,
    }
    return headers, path_with_query


def main():
    print("=" * 58)
    print("  COINSWITCH API AUTHENTICATION TEST")
    print("=" * 58)

    if not API_KEY or not SECRET_KEY:
        print("  ❌ COINSWITCH_API_KEY or COINSWITCH_API_SECRET missing from .env")
        print("     Add both lines to /home/container/.env and re-run.")
        return

    print(f"  API Key : {API_KEY[:6]}...{API_KEY[-4:]} ({len(API_KEY)} chars)")
    print(f"  Secret  : {'*' * 20} ({len(SECRET_KEY)} chars)")

    # ── Step 1: server time (no auth needed) ──────────────
    print("\n[1] Checking server time (unauthenticated)...")
    try:
        r = requests.get(BASE_URL + "/trade/api/v2/time", timeout=10)
        print(f"    HTTP {r.status_code}: {r.json()}")
    except Exception as e:
        print(f"    ❌ Could not reach CoinSwitch: {e}")
        return

    # ── Step 2: validate API key + signature ──────────────
    print("\n[2] Validating API key + signature...")
    headers, path = sign_request("GET", "/trade/api/v2/validate/keys")
    r = requests.get(BASE_URL + path, headers=headers, timeout=10)
    if r.status_code == 200:
        print(f"    ✅ Success: {r.json()}")
    else:
        print(f"    ❌ Failed (HTTP {r.status_code}): {r.text[:300]}")
        print("\n    Common causes:")
        print("    - Secret key isn't the raw 32-byte Ed25519 seed (64 hex chars)")
        print("    - API key/secret don't belong to the same generated pair")
        print("    - Server clock drift on HidenCloud (unlikely, but possible)")
        return

    # ── Step 3: futures wallet balance ─────────────────────
    print("\n[3] Fetching futures wallet balance...")
    headers, path = sign_request("GET", "/trade/api/v2/futures/wallet_balance")
    r = requests.get(BASE_URL + path, headers=headers, timeout=10)
    if r.status_code == 200:
        print(f"    ✅ Success: {r.json()}")
    else:
        print(f"    ❌ Failed (HTTP {r.status_code}): {r.text[:300]}")

    print("\n" + "=" * 58)
    print("  If all 3 steps show ✅ / HTTP 200, CoinSwitch is fully connected.")
    print("=" * 58)


if __name__ == "__main__":
    main()

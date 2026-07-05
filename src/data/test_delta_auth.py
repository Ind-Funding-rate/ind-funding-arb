"""
Test Delta Exchange API authentication.
Safe read-only call - checks your identity and account balance.
Places NO orders, touches NO funds.
"""
import requests
import hashlib
import hmac
import time
import os
from dotenv import load_dotenv

load_dotenv()

API_KEY    = os.getenv("DELTA_API_KEY")
API_SECRET = os.getenv("DELTA_API_SECRET")
BASE_URL   = "https://api.india.delta.exchange"


def sign_request(method, path, payload=""):
    """Create the HMAC signature Delta requires for authenticated requests."""
    timestamp = str(int(time.time()))
    message   = method + timestamp + path + payload
    signature = hmac.new(
        API_SECRET.encode(), message.encode(), hashlib.sha256
    ).hexdigest()
    return timestamp, signature


def get_wallet_balance():
    """Fetch your Delta wallet balance - read only, completely safe."""
    path   = "/v2/wallet/balances"
    method = "GET"
    timestamp, signature = sign_request(method, path)

    headers = {
        "api-key":   API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
    }

    response = requests.get(BASE_URL + path, headers=headers, timeout=10)
    return response.status_code, response.json()


def get_profile():
    """Fetch your Delta profile - confirms authentication works."""
    path   = "/v2/profile"
    method = "GET"
    timestamp, signature = sign_request(method, path)

    headers = {
        "api-key":   API_KEY,
        "timestamp": timestamp,
        "signature": signature,
        "Content-Type": "application/json",
    }

    response = requests.get(BASE_URL + path, headers=headers, timeout=10)
    return response.status_code, response.json()


if __name__ == "__main__":
    print("=" * 52)
    print("  DELTA API AUTHENTICATION TEST")
    print("=" * 52)

    if not API_KEY or not API_SECRET:
        print("❌ DELTA_API_KEY or DELTA_API_SECRET missing from .env")
        exit(1)

    print(f"\n  API Key : {API_KEY[:6]}...{API_KEY[-4:]} ({len(API_KEY)} chars)")
    print(f"  Secret  : {'*' * 20} ({len(API_SECRET)} chars)")

    # Test 1: Profile
    print("\n[1] Testing profile authentication...")
    status, data = get_profile()
    if status == 200 and data.get("success"):
        result = data.get("result", {})
        print(f"  ✅ Authenticated as: {result.get('email', 'unknown')}")
    else:
        print(f"  ❌ Failed (HTTP {status}): {data.get('error', data)}")
        print("\n  Possible reasons:")
        print("  - API key doesn't have 'Read' permission enabled on Delta")
        print("  - API key has IP whitelist that doesn't include your current IP")
        print("  - Wrong API key/secret in .env")
        exit(1)

    # Test 2: Wallet balance
    print("\n[2] Fetching wallet balance...")
    status, data = get_wallet_balance()
    if status == 200 and data.get("success"):
        balances = data.get("result", [])
        if balances:
            print("  ✅ Wallet accessible. Balances:")
            for b in balances:
                available = float(b.get("available_balance", 0))
                if available > 0:
                    print(f"     {b.get('asset_symbol', '?')}: {available:.4f}")
        else:
            print("  ✅ Wallet accessible (empty / no balances found)")
    else:
        print(f"  ❌ Failed (HTTP {status}): {data}")

    print("\n" + "=" * 52)
    print("  If both show ✅ we are ready for Step 7.")
    print("=" * 52)

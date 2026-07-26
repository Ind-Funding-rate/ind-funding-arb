"""
CoinSwitch PRO - Fetch Live BTC Funding Rate
----------------------------------------------
Read-only. Pulls the current funding rate, mark price, and next funding
timestamp for BTCUSDT perpetual futures on CoinSwitch (EXCHANGE_2).

Endpoint: GET /trade/api/v2/futures/ticker
Docs: api-trading.coinswitch.co/futures/reference/get-ticker
"""

import os
import time
import requests
from dotenv import load_dotenv
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey
from datetime import datetime

load_dotenv("/home/container/.env")

API_KEY    = os.getenv("COINSWITCH_API_KEY")
SECRET_KEY = os.getenv("COINSWITCH_API_SECRET")
BASE_URL   = "https://coinswitch.co"


def sign_request(method, path, params=None):
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

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": API_KEY,
        "X-AUTH-SIGNATURE": signature.hex(),
        "X-AUTH-EPOCH": epoch,
    }
    return headers, path_with_query


def get_coinswitch_btc_rate():
    """
    Returns dict with funding_rate (as a FRACTION, e.g. 0.0003968 = 0.03968%,
    matching Pi42's convention already used elsewhere in this project),
    mark_price (in USDT), and next_funding_timestamp (ms epoch).
    Returns None on failure.
    """
    headers, path = sign_request(
        "GET", "/trade/api/v2/futures/ticker",
        params={"symbol": "BTCUSDT", "exchange": "EXCHANGE_2"},
    )
    r = requests.get(BASE_URL + path, headers=headers, timeout=10)
    if r.status_code != 200:
        print(f"  [!] CoinSwitch ticker fetch failed: HTTP {r.status_code}: {r.text[:200]}")
        return None

    ticker = r.json().get("data", {}).get("EXCHANGE_2", {})
    if not ticker:
        print("  [!] CoinSwitch ticker response missing EXCHANGE_2 data")
        return None

    return {
        "coinswitch_funding": float(ticker.get("funding_rate", 0)),
        "coinswitch_price": float(ticker.get("mark_price", 0)),
        "coinswitch_next_funding_ts": ticker.get("next_funding_timestamp"),
    }


def main():
    print("=" * 54)
    print("  COINSWITCH - LIVE BTC FUNDING RATE (BTCUSDT)")
    print("=" * 54)

    result = get_coinswitch_btc_rate()
    if not result:
        print("\n  Failed to fetch. See error above.")
        return

    rate = result["coinswitch_funding"]
    price = result["coinswitch_price"]
    next_ts = result["coinswitch_next_funding_ts"]
    next_dt = datetime.fromtimestamp(next_ts / 1000).strftime("%Y-%m-%d %H:%M:%S") if next_ts else "unknown"

    print(f"\n  Mark price      : ${price:,.2f}")
    print(f"  Funding rate    : {rate * 100:.6f}%  (per funding interval)")
    print(f"  Next funding at : {next_dt}")
    print("\n  Note: verify this % against the CoinSwitch PRO app's futures")
    print("  screen for BTCUSDT to confirm the unit assumption is correct")
    print("  before this feeds into the live opportunity scanner.")


if __name__ == "__main__":
    main()

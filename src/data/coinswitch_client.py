"""
CoinSwitch PRO API client - futures/funding rate data.

Confirmed directly from CoinSwitch's own official docs
(api-trading.coinswitch.co), not guessed:
    - Base URL: https://coinswitch.co
    - Auth: Ed25519 signatures (NOT HMAC like every other exchange in
      this project) - api_key IS the Ed25519 public key (hex), and you
      sign with your Ed25519 private/secret key (also hex).
    - Signed message = METHOD + path_with_query (URL-decoded) + epoch_ms
    - Headers: X-AUTH-APIKEY, X-AUTH-SIGNATURE, X-AUTH-EPOCH
    - GET /trade/api/v2/futures/all-pairs/ticker?exchange=EXCHANGE_2
      returns ALL futures symbols' tickers (incl. funding_rate,
      next_funding_timestamp, mark_price) in ONE call - no need to
      iterate coin by coin.
    - funding_rate is a plain decimal fraction (e.g. 0.00039681 =
      0.039681%) - same convention as Pi42, no unit conversion needed
      (unlike Delta, which needed a /100 fix).

CoinSwitch has NOT been confirmed to offer historical funding data - only
this live snapshot. So (like Pi42 and Delta) it belongs in the live
scanner, not the historical backtest/ingestion pipeline.
"""
import time
import requests
import os
from urllib.parse import urlencode
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

BASE_URL = "https://coinswitch.co"


def _sign_request(method: str, path: str, params: dict = None):
    """Builds the exact signed_message CoinSwitch expects and returns
    (headers, full_path_with_query) ready to use in a requests call."""
    api_key = os.getenv("COINSWITCH_API_KEY")
    secret_key_hex = os.getenv("COINSWITCH_SECRET_KEY")

    query_string = urlencode(params) if params else ""
    path_with_query = f"{path}?{query_string}" if query_string else path

    epoch = str(int(time.time() * 1000))
    signed_message = method.upper() + path_with_query + epoch

    private_key = Ed25519PrivateKey.from_private_bytes(bytes.fromhex(secret_key_hex))
    signature = private_key.sign(signed_message.encode("utf-8")).hex()

    headers = {
        "Content-Type": "application/json",
        "X-AUTH-APIKEY": api_key,
        "X-AUTH-SIGNATURE": signature,
        "X-AUTH-EPOCH": epoch,
    }
    return headers, path_with_query


def get_all_funding_rates():
    """Returns {symbol: {"funding_rate": float, "mark_price": float,
    "next_funding_ts": int}} for every futures pair on CoinSwitch, in one
    API call. Symbol format matches their convention, e.g. "BTCUSDT"."""
    path = "/trade/api/v2/futures/all-pairs/ticker"
    headers, path_with_query = _sign_request("GET", path, {"exchange": "EXCHANGE_2"})

    r = requests.get(BASE_URL + path_with_query, headers=headers, timeout=15)
    r.raise_for_status()
    body = r.json()

    out = {}
    for symbol, t in body.get("data", {}).items():
        out[symbol] = {
            "funding_rate": float(t.get("funding_rate", 0)),
            "mark_price": float(t.get("mark_price", 0)),
            "next_funding_ts": t.get("next_funding_timestamp"),
        }
    return out


if __name__ == "__main__":
    # Quick manual test - confirms real credentials + real data, not a mock
    print("Testing CoinSwitch futures ticker fetch...")
    rates = get_all_funding_rates()
    print(f"Got {len(rates)} symbols")
    if "BTCUSDT" in rates:
        print("BTCUSDT:", rates["BTCUSDT"])

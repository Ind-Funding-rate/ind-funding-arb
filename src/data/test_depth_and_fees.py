"""
Standalone, read-only test script - confirms real order-book depth and
fee data BEFORE any slippage/fee calculation gets built into the Spread
Scanner. Does not touch spread_scanner.py or any production code.

What this checks:
1. Delta's L2 order book endpoint for BTCUSD (public, no auth) - trying
   the documented-by-convention /v2/l2orderbook/{symbol} path.
2. CoinSwitch futures depth - path is NOT confirmed anywhere in official
   docs reviewed so far, only their SPOT public depth path is. Tries
   several plausible futures-namespace variants and reports which ones
   actually respond, reusing the same Ed25519 signing helper that
   coinswitch_client.py already has proven working.
3. CoinSwitch trading fee endpoint - same situation, tries a few
   plausible paths so we get a REAL fee number instead of guessing one.

Run this, read the output, and only then do we build the real feature -
same pattern that already caught the Pi42 field-name bug and the
spread-scanner signature mismatch earlier in this project.
"""
import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import requests
from src.data.coinswitch_client import _sign_request, BASE_URL as CS_BASE_URL


def test_delta_orderbook():
    print("=" * 60)
    print("  DELTA ORDER BOOK")
    print("=" * 60)
    url = "https://api.india.delta.exchange/v2/l2orderbook/BTCUSD"
    try:
        r = requests.get(url, timeout=15)
        print(f"URL: {url}")
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json()
            result = data.get("result", data)
            buy = result.get("buy", result.get("bids", "NOT FOUND - check raw below"))
            sell = result.get("sell", result.get("asks", "NOT FOUND - check raw below"))
            print(f"Top of book - buy side sample: {buy[:3] if isinstance(buy, list) else buy}")
            print(f"Top of book - sell side sample: {sell[:3] if isinstance(sell, list) else sell}")
            print("\nFull raw response (first 1500 chars):")
            print(json.dumps(data, indent=2)[:1500])
        else:
            print(f"Body: {r.text[:500]}")
    except Exception as e:
        print(f"FAILED: {e}")


def test_coinswitch_futures_depth():
    print("\n" + "=" * 60)
    print("  COINSWITCH FUTURES DEPTH - trying candidate paths")
    print("=" * 60)
    candidates = [
        ("/trade/api/v2/futures/depth", {"symbol": "BTCUSDT", "exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/futures/orderbook", {"symbol": "BTCUSDT", "exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/futures/market/depth", {"symbol": "BTCUSDT", "exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/public/depth", {"instrument": "BTC/USDT"}),  # spot-style, long shot for futures
    ]
    for path, params in candidates:
        try:
            headers, path_with_query = _sign_request("GET", path, params)
            r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
            print(f"\nPath: {path}")
            print(f"Status: {r.status_code}")
            if r.status_code == 200:
                print("SUCCESS - raw response (first 800 chars):")
                print(json.dumps(r.json(), indent=2)[:800])
            else:
                print(f"Body: {r.text[:300]}")
        except Exception as e:
            print(f"Path: {path} -> FAILED: {e}")


def test_coinswitch_fee():
    print("\n" + "=" * 60)
    print("  COINSWITCH TRADING FEE - trying candidate paths")
    print("=" * 60)
    candidates = [
        ("/trade/api/v2/user/trading-fee", {"exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/futures/trading-fee", {"exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/futures/fee", {"symbol": "BTCUSDT", "exchange": "EXCHANGE_2"}),
        ("/trade/api/v2/user/fee", {}),
    ]
    for path, params in candidates:
        try:
            headers, path_with_query = _sign_request("GET", path, params)
            r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
            print(f"\nPath: {path}")
            print(f"Status: {r.status_code}")
            if r.status_code == 200:
                print("SUCCESS - raw response:")
                print(json.dumps(r.json(), indent=2)[:800])
            else:
                print(f"Body: {r.text[:300]}")
        except Exception as e:
            print(f"Path: {path} -> FAILED: {e}")


if __name__ == "__main__":
    test_delta_orderbook()
    test_coinswitch_futures_depth()
    test_coinswitch_fee()
    print("\n" + "=" * 60)
    print("  DONE - check which paths returned Status: 200 above")
    print("=" * 60)

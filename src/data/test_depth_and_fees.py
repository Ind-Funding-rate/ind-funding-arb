"""
Standalone, read-only test script - confirms real order-book depth and
fee data BEFORE any slippage/fee calculation gets built into the Spread
Scanner. Does not touch spread_scanner.py or any production code.

2026-08-01 update: CoinSwitch paths below are now taken directly from
their official reference docs (api-trading.coinswitch.co/futures/reference/)
instead of guessed - the earlier guessed paths all returned 404:
    - Order book: GET /trade/api/v2/futures/order_book
      params: symbol, exchange=EXCHANGE_2, l2Orderbook=true for depth
    - Fees: GET /trade/api/v2/futures/instrument_info
      params: exchange=EXCHANGE_2 -> returns taker_fee_rate/maker_fee_rate
      PER SYMBOL, so we get CoinSwitch's real fee instead of guessing one.

What this checks:
1. Delta's L2 order book endpoint for BTCUSD (public, no auth).
2. CoinSwitch futures order book (documented path, both top-of-book and
   L2 mode) for BTCUSDT.
3. CoinSwitch futures instrument info (documented path) - confirms real
   taker/maker fee for BTCUSDT.

Run this, read the output, and only then do we build the real feature.
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


def test_coinswitch_orderbook():
    print("\n" + "=" * 60)
    print("  COINSWITCH FUTURES ORDER BOOK (documented endpoint)")
    print("=" * 60)
    path = "/trade/api/v2/futures/order_book"

    for label, params in [
        ("top-of-book", {"symbol": "btcusdt", "exchange": "EXCHANGE_2"}),
        ("L2 (deep)", {"symbol": "btcusdt", "exchange": "EXCHANGE_2", "l2Orderbook": "true"}),
    ]:
        try:
            headers, path_with_query = _sign_request("GET", path, params)
            r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
            print(f"\nMode: {label}")
            print(f"Status: {r.status_code}")
            if r.status_code == 200:
                data = r.json().get("data", {})
                bids = data.get("bids", [])
                asks = data.get("asks", [])
                print(f"Bid levels returned: {len(bids)} (sample: {bids[:3]})")
                print(f"Ask levels returned: {len(asks)} (sample: {asks[:3]})")
            else:
                print(f"Body: {r.text[:400]}")
        except Exception as e:
            print(f"Mode: {label} -> FAILED: {e}")


def test_coinswitch_instrument_info():
    print("\n" + "=" * 60)
    print("  COINSWITCH INSTRUMENT INFO / REAL FEES (documented endpoint)")
    print("=" * 60)
    path = "/trade/api/v2/futures/instrument_info"
    try:
        headers, path_with_query = _sign_request("GET", path, {"exchange": "EXCHANGE_2"})
        r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json().get("data", {})
            print(f"Symbols returned: {len(data)}")
            if "BTCUSDT" in data:
                btc = data["BTCUSDT"]
                print(f"BTCUSDT taker_fee_rate: {btc.get('taker_fee_rate')}")
                print(f"BTCUSDT maker_fee_rate: {btc.get('maker_fee_rate')}")
                print("Full BTCUSDT entry:", json.dumps(btc, indent=2))
            else:
                print("BTCUSDT not found. Sample keys:", list(data.keys())[:10])
        else:
            print(f"Body: {r.text[:400]}")
    except Exception as e:
        print(f"FAILED: {e}")


if __name__ == "__main__":
    test_delta_orderbook()
    test_coinswitch_orderbook()
    test_coinswitch_instrument_info()
    print("\n" + "=" * 60)
    print("  DONE")
    print("=" * 60)

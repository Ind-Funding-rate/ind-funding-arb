"""
Standalone, read-only test script - confirms real order-book depth and
fee data BEFORE any slippage/fee calculation gets built into the Spread
Scanner. Does not touch spread_scanner.py or any production code.

CoinSwitch paths are taken directly from their official reference docs
(api-trading.coinswitch.co/futures/reference/):
    - Order book: GET /trade/api/v2/futures/order_book
      params: symbol, exchange=EXCHANGE_2, l2Orderbook=true for depth
    - Fees: GET /trade/api/v2/futures/instrument_info
      params: exchange=EXCHANGE_2 -> returns taker_fee_rate/maker_fee_rate
      PER SYMBOL - confirmed working, real fee, not a guess.

Pi42 depth: tries the REST endpoint on their USDT market (matching what
spread_scanner.py already uses for price, native USDT not INR+conversion)
- same URL shape as the INR depth endpoint we used successfully early in
this project (/v1/market/depth/<SYMBOL>), just swapping the symbol
suffix. Confirming the "asks" field name specifically, since we only
ever glimpsed the "bids" ("b") side in a truncated network-tab capture
months ago and never actually verified the asks ("a") side or the field
names on a full REST response.

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
            buy = result.get("buy", result.get("bids", "NOT FOUND"))
            sell = result.get("sell", result.get("asks", "NOT FOUND"))
            print(f"Buy side sample: {buy[:3] if isinstance(buy, list) else buy}")
            print(f"Sell side sample: {sell[:3] if isinstance(sell, list) else sell}")
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
                print(f"Bid levels: {len(bids)} (sample: {bids[:3]})")
                print(f"Ask levels: {len(asks)} (sample: {asks[:3]})")
            else:
                print(f"Body: {r.text[:400]}")
        except Exception as e:
            print(f"Mode: {label} -> FAILED: {e}")


def test_coinswitch_instrument_info():
    print("\n" + "=" * 60)
    print("  COINSWITCH INSTRUMENT INFO / REAL FEES")
    print("=" * 60)
    path = "/trade/api/v2/futures/instrument_info"
    try:
        headers, path_with_query = _sign_request("GET", path, {"exchange": "EXCHANGE_2"})
        r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
        print(f"Status: {r.status_code}")
        if r.status_code == 200:
            data = r.json().get("data", {})
            btc = data.get("BTCUSDT", {})
            print(f"BTCUSDT taker_fee_rate: {btc.get('taker_fee_rate')}")
            print(f"BTCUSDT maker_fee_rate: {btc.get('maker_fee_rate')}")
        else:
            print(f"Body: {r.text[:400]}")
    except Exception as e:
        print(f"FAILED: {e}")


def test_pi42_orderbook():
    print("\n" + "=" * 60)
    print("  PI42 ORDER BOOK (USDT market, REST)")
    print("=" * 60)
    for symbol in ["BTCUSDT"]:
        url = f"https://api.pi42.com/v1/market/depth/{symbol}"
        try:
            r = requests.get(url, timeout=15)
            print(f"URL: {url}")
            print(f"Status: {r.status_code}")
            if r.status_code == 200:
                raw = r.json()
                data = raw.get("data", raw)
                print("Top-level keys in response:", list(data.keys()))
                bids = data.get("b", data.get("bids", "NOT FOUND"))
                asks = data.get("a", data.get("asks", "NOT FOUND"))
                print(f"Bid sample: {bids[:3] if isinstance(bids, list) else bids}")
                print(f"Ask sample: {asks[:3] if isinstance(asks, list) else asks}")
                print("\nFull raw response (first 1000 chars):")
                print(json.dumps(raw, indent=2)[:1000])
            else:
                print(f"Body: {r.text[:400]}")
        except Exception as e:
            print(f"FAILED: {e}")


if __name__ == "__main__":
    test_delta_orderbook()
    test_coinswitch_orderbook()
    test_coinswitch_instrument_info()
    test_pi42_orderbook()
    print("\n" + "=" * 60)
    print("  DONE")
    print("=" * 60)

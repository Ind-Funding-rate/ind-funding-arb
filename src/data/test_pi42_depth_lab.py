"""
Standalone diagnostic - Pi42 order book showing exactly 0.0000% slippage
on BOTH legs for LAB in the Real Cost calculator is suspicious (real
books almost never give exactly zero). This prints the RAW depth
response for a thin coin to see if the book genuinely only has 1-2
levels (real, not a bug) or if something is being parsed wrong.
"""
import sys
import json
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

import requests
from src.data.orderbook_depth import get_pi42_orderbook, walk_book


def inspect(coin):
    print("=" * 60)
    print(f"  PI42 RAW DEPTH FOR {coin}USDT")
    print("=" * 60)
    url = f"https://api.pi42.com/v1/market/depth/{coin}USDT"
    r = requests.get(url, timeout=15)
    print(f"URL: {url}")
    print(f"Status: {r.status_code}")
    if r.status_code != 200:
        print(f"Body: {r.text[:400]}")
        return
    raw = r.json()
    data = raw.get("data", raw)
    b = data.get("b", [])
    a = data.get("a", [])
    print(f"Number of bid levels: {len(b)}")
    print(f"Number of ask levels: {len(a)}")
    print(f"Bids (all): {b}")
    print(f"Asks (all): {a}")

    print("\n  --- via get_pi42_orderbook() + walk_book() ---")
    bids, asks = get_pi42_orderbook(coin)
    print(f"Parsed bid levels: {len(bids)}")
    print(f"Parsed ask levels: {len(asks)}")
    if bids and asks:
        # Simulate a $1000 position like the real feature does
        qty = 1000 / asks[0][0]
        avg_buy, filled, full = walk_book(asks, qty)
        print(f"\nSimulated BUY of qty={qty:.4f} (${1000} position):")
        print(f"  avg_price={avg_buy}, filled={filled}, fully_filled={full}")
        avg_sell, filled2, full2 = walk_book(bids, qty)
        print(f"Simulated SELL of qty={qty:.4f}:")
        print(f"  avg_price={avg_sell}, filled={filled2}, fully_filled={full2}")


if __name__ == "__main__":
    inspect("LAB")
    print()
    inspect("BTC")

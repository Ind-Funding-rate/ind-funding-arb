"""
Coin discovery for the Indian exchanges (Delta, Pi42, CoinSwitch).

Standalone module - fetches each exchange's FULL live symbol list (not
the fixed 133-coin list used elsewhere) and computes which coins are
tradeable on at least 2 of the 3, so the Indian scanner/opportunities
pages can cover everything actually available instead of a fixed subset.

Each fetcher reuses an already-confirmed endpoint from earlier work in
this project - nothing here is guessed:
    - Delta:      /v2/tickers (same endpoint full_market_scanner.py uses)
    - Pi42:       /v1/exchange/exchangeInfo?market=INR (confirmed
                  earlier when comparing Delta vs Pi42 coin overlap)
    - CoinSwitch: the all-pairs ticker call already used by
                  three_way_scanner.py (651 symbols in one call)

Run this file directly to see counts and the actual union list before
wiring it into anything live - same "verify standalone first" approach
used throughout this project.
"""
import re
import requests
from src.data.coinswitch_client import get_all_funding_rates

_MULTIPLIER_PREFIX = re.compile(r"^(1000|1M)")


def strip_multiplier_prefix(coin: str) -> str:
    """'1000BONK' -> 'BONK', '1MBABYDOGE' -> 'BABYDOGE'. Delta/Pi42 use
    multiplier prefixes in their symbol names; CoinSwitch does not."""
    return _MULTIPLIER_PREFIX.sub("", coin)


def get_delta_symbols() -> set:
    """Every base coin Delta lists as a USD perpetual right now."""
    symbols = set()
    try:
        r = requests.get("https://api.india.delta.exchange/v2/tickers", timeout=20).json()
        for t in r.get("result", []):
            symbol = t.get("symbol", "")
            if symbol.endswith("USD") and not symbol.endswith("USDT"):
                symbols.add(strip_multiplier_prefix(symbol[:-3]))
    except Exception as e:
        print(f"  [!] Delta symbol fetch failed: {e}")
    return symbols


def get_pi42_symbols() -> set:
    """Every base coin Pi42 lists as an INR perpetual right now."""
    symbols = set()
    try:
        r = requests.get(
            "https://api.pi42.com/v1/exchange/exchangeInfo", params={"market": "INR"}, timeout=20
        ).json()
        for c in r.get("contracts", []):
            name = c.get("name", "")
            if name.endswith("INR"):
                symbols.add(strip_multiplier_prefix(name[:-3]))
    except Exception as e:
        print(f"  [!] Pi42 symbol fetch failed: {e}")
    return symbols


def get_coinswitch_symbols() -> set:
    """Every base coin CoinSwitch lists as a USDT-margined futures pair
    right now - reuses the same all-pairs call the live 3-way scanner
    already makes, so this doesn't add extra API load."""
    symbols = set()
    try:
        raw = get_all_funding_rates()
        for symbol in raw.keys():
            if symbol.endswith("USDT"):
                symbols.add(strip_multiplier_prefix(symbol[:-4]))
    except Exception as e:
        print(f"  [!] CoinSwitch symbol fetch failed: {e}")
    return symbols


def get_all_exchange_symbols() -> dict:
    """Returns {"Delta": set(...), "Pi42": set(...), "CoinSwitch": set(...)}"""
    return {
        "Delta": get_delta_symbols(),
        "Pi42": get_pi42_symbols(),
        "CoinSwitch": get_coinswitch_symbols(),
    }


def get_coins_on_at_least_two(exchange_symbols: dict = None) -> list:
    """Returns every coin that appears on AT LEAST 2 of the 3 exchanges -
    the actual scan universe, replacing the fixed 133-coin list."""
    exchange_symbols = exchange_symbols or get_all_exchange_symbols()
    counts = {}
    for coins in exchange_symbols.values():
        for coin in coins:
            counts[coin] = counts.get(coin, 0) + 1
    return sorted(coin for coin, count in counts.items() if count >= 2)


if __name__ == "__main__":
    print("=" * 60)
    print("  INDIAN EXCHANGE COIN DISCOVERY")
    print("=" * 60)

    exchange_symbols = get_all_exchange_symbols()
    for name, coins in exchange_symbols.items():
        print(f"  {name:<12}: {len(coins)} coins")

    union_2plus = get_coins_on_at_least_two(exchange_symbols)
    print(f"\n  On 2+ exchanges: {len(union_2plus)} coins")
    print(f"  {union_2plus}")

    # Show the overlap breakdown per pair, useful sanity check
    d, p, c = exchange_symbols["Delta"], exchange_symbols["Pi42"], exchange_symbols["CoinSwitch"]
    print(f"\n  Delta \u2229 Pi42       : {len(d & p)}")
    print(f"  Delta \u2229 CoinSwitch  : {len(d & c)}")
    print(f"  Pi42 \u2229 CoinSwitch   : {len(p & c)}")
    print(f"  All three         : {len(d & p & c)}")

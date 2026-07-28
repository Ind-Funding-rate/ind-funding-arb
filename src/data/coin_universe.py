"""
Coin Universe Discovery
------------------------
Auto-discovers every coin that has perpetual futures on AT LEAST 2 of the
3 connected exchanges (Delta, Pi42, CoinSwitch) - replacing the old fixed
133-coin list, which was really just "whatever Delta and Pi42 happened to
both have" and never accounted for CoinSwitch's 651 symbols at all.

Naming problem this solves: Delta and Pi42 both use a "1000X" or "1MX"
prefix for low-priced meme coins (e.g. "1000BONK" - one contract = 1000
BONK). CoinSwitch does not use this prefix. To correctly detect "is BONK
on 2+ exchanges", every raw symbol is reduced to a CANONICAL name first
(multiplier prefix stripped), and coins are matched by that canonical
name - while still remembering each exchange's own RAW symbol, since
that's what's actually needed to call each exchange's API.

Field name note: two different sessions disagreed on whether Pi42's
exchangeInfo contract objects use "contractPair" or "name" for the
symbol (e.g. "BTCINR"). Rather than trust either claim, get_pi42_all_symbols()
below checks both and prints which one it actually found on the live
API, the first time it runs each process - so this is settled by real
data, not by either AI's memory of a previous session.
"""
import re
import sys
import requests
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.execution.full_market_scanner import get_delta_funding_all
from src.data.coinswitch_client import get_all_funding_rates

_MULTIPLIER_PREFIX = re.compile(r"^(1000|1M)")
_pi42_field_logged = False


def canonical(raw_coin):
    """'1000BONK' -> 'BONK', '1MBABYDOGE' -> 'BABYDOGE', 'BTC' -> 'BTC'."""
    return _MULTIPLIER_PREFIX.sub("", raw_coin)


def get_pi42_all_symbols():
    """Full list of Pi42's own INR-margined perpetual contracts, straight
    from their exchangeInfo endpoint - not the websocket (which needs you
    to already know what to subscribe to)."""
    global _pi42_field_logged

    r = requests.get(
        "https://api.pi42.com/v1/exchange/exchangeInfo", params={"market": "INR"}, timeout=15
    )
    r.raise_for_status()
    contracts = r.json().get("contracts", [])

    if contracts and not _pi42_field_logged:
        sample = contracts[0]
        print(f"  [debug] Pi42 contract object keys: {list(sample.keys())}")
        _pi42_field_logged = True

    out = []
    for c in contracts:
        # Try both field names seen across different sessions - use
        # whichever is actually present rather than assuming one.
        pair = c.get("name") or c.get("contractPair") or ""
        if pair.endswith("INR"):
            out.append(pair[:-3])  # 'BTCINR' -> 'BTC'
    return out


def build_coin_universe(delta_raw=None, pi42_raw=None, cs_symbols=None):
    """
    Returns a dict: {canonical_coin: {"delta": raw_symbol_or_None,
    "pi42": raw_symbol_or_None, "coinswitch": raw_symbol_or_None,
    "exchange_count": int}} - filtered to only coins present on 2 or
    more exchanges.

    Callers that already fetched Delta/CoinSwitch data this cycle (for
    funding rates) should pass delta_raw=list(delta_data.keys()) and
    cs_symbols=list(cs_data.keys()) to avoid fetching the same data
    twice. pi42_raw (list of Pi42's own raw base coin names) is always
    fetched fresh here since nothing else needs Pi42's exchangeInfo.
    """
    if delta_raw is None:
        print("  Fetching Delta's full symbol list...")
        delta_raw = list(get_delta_funding_all().keys())

    if pi42_raw is None:
        print("  Fetching Pi42's full symbol list...")
        pi42_raw = get_pi42_all_symbols()

    if cs_symbols is None:
        print("  Fetching CoinSwitch's full symbol list...")
        cs_symbols = list(get_all_funding_rates().keys())
    cs_raw = [sym[:-4] for sym in cs_symbols if sym.endswith("USDT")]  # 'BTCUSDT' -> 'BTC'

    print(f"  Delta: {len(delta_raw)} symbols | Pi42: {len(pi42_raw)} symbols | "
          f"CoinSwitch: {len(cs_raw)} symbols")

    merged = {}
    for raw in delta_raw:
        c = canonical(raw)
        merged.setdefault(c, {"delta": None, "pi42": None, "coinswitch": None})
        merged[c]["delta"] = raw
    for raw in pi42_raw:
        c = canonical(raw)
        merged.setdefault(c, {"delta": None, "pi42": None, "coinswitch": None})
        merged[c]["pi42"] = raw
    for raw in cs_raw:
        c = canonical(raw)
        merged.setdefault(c, {"delta": None, "pi42": None, "coinswitch": None})
        merged[c]["coinswitch"] = raw

    universe = {}
    for coin, sources in merged.items():
        count = sum(1 for v in sources.values() if v is not None)
        if count >= 2:
            sources["exchange_count"] = count
            universe[coin] = sources

    return universe


if __name__ == "__main__":
    print("=" * 58)
    print("  COIN UNIVERSE DISCOVERY (coins on 2+ of 3 exchanges)")
    print("=" * 58)

    universe = build_coin_universe()

    all_3 = [c for c, v in universe.items() if v["exchange_count"] == 3]
    exactly_2 = [c for c, v in universe.items() if v["exchange_count"] == 2]

    print(f"\n  Total coins on 2+ exchanges: {len(universe)}")
    print(f"    On all 3 exchanges : {len(all_3)}")
    print(f"    On exactly 2       : {len(exactly_2)}")

    dp = sum(1 for c in exactly_2 if universe[c]["delta"] and universe[c]["pi42"])
    dc = sum(1 for c in exactly_2 if universe[c]["delta"] and universe[c]["coinswitch"])
    pc = sum(1 for c in exactly_2 if universe[c]["pi42"] and universe[c]["coinswitch"])
    print(f"      Delta+Pi42 only       : {dp}")
    print(f"      Delta+CoinSwitch only : {dc}")
    print(f"      Pi42+CoinSwitch only  : {pc}")

    print("\n  Sample of 15 coins found:")
    for coin in list(universe.keys())[:15]:
        v = universe[coin]
        flags = "".join([
            "D" if v["delta"] else "-",
            "P" if v["pi42"] else "-",
            "C" if v["coinswitch"] else "-",
        ])
        print(f"    {coin:12s} [{flags}]  delta={v['delta']}  pi42={v['pi42']}  cs={v['coinswitch']}")

    print(f"\n  For comparison, the OLD hardcoded list had 133 coins.")
    print(f"  New auto-discovered universe has {len(universe)} coins.")

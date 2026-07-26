"""
Indian exchanges funding-rate scanner - Pi42, Delta India, and CoinSwitch
PRO. Separate from the global (Bybit/OKX/Binance) scanner and
opportunities pages, as requested.

Reuses the already-confirmed fetch logic from full_market_scanner.py
(Pi42 WebSocket, Delta REST) rather than duplicating it, and adds
CoinSwitch via the confirmed Ed25519-signed client.

For each coin common to all three, computes the pairwise gap for every
combination (Pi42-Delta, Pi42-CoinSwitch, Delta-CoinSwitch) using each
pair's own real, confirmed fee structure - not a single blanket
assumption, since the three exchanges have different fee schedules.

CoinDCX and Mudrex are NOT included yet - their exact live funding-rate
endpoints haven't been independently verified the same way Pi42, Delta,
and CoinSwitch have. Giottus is excluded entirely - confirmed directly
from its own official API docs to be spot-only, no futures/funding
concept applies to it at all.
"""
import time
from src.execution.full_market_scanner import get_delta_funding_all, get_pi42_funding_all, COINS as PI42_DELTA_COINS
from src.data.coinswitch_client import get_all_funding_rates as get_coinswitch_all

# Fee constants - each confirmed from the respective exchange's own
# official fee pages/docs, not assumed to be the same across exchanges.
PI42_FEE       = 0.080 * 1.18 / 100   # 0.0944% (taker + 18% GST, confirmed)
DELTA_FEE      = 0.050 * 1.18 / 100   # 0.0590% (taker + 18% GST, confirmed)
COINSWITCH_FEE = 0.050 / 100          # 0.0500% (taker, confirmed across 3+ sources; no GST confirmed for CoinSwitch specifically)

PAIR_FEES = {
    ("Pi42", "Delta"):      2 * (PI42_FEE + DELTA_FEE),
    ("Pi42", "CoinSwitch"): 2 * (PI42_FEE + COINSWITCH_FEE),
    ("Delta", "CoinSwitch"):2 * (DELTA_FEE + COINSWITCH_FEE),
}


def run_indian_scan_cycle():
    """Fetches live funding rates from Pi42, Delta, and CoinSwitch, and
    returns a ranked list of pairwise gap results for every coin common
    to at least two of the three exchanges."""
    delta_data = get_delta_funding_all()
    pi42_data = get_pi42_funding_all(PI42_DELTA_COINS)

    try:
        coinswitch_raw = get_coinswitch_all()
    except Exception as e:
        print(f"  [!] CoinSwitch fetch failed: {e}")
        coinswitch_raw = {}

    coinswitch_data = {}
    for symbol, t in coinswitch_raw.items():
        if symbol.endswith("USDT"):
            base = symbol[:-4]
            coinswitch_data[base] = {"funding": t["funding_rate"], "price": t["mark_price"]}

    sources = {
        "Pi42": {c: {"funding": v["funding"], "price": v.get("price", 0)} for c, v in pi42_data.items()},
        "Delta": {c: {"funding": v["funding"], "price": v.get("price", 0)} for c, v in delta_data.items()},
        "CoinSwitch": coinswitch_data,
    }

    all_coins = set()
    for src in sources.values():
        all_coins.update(src.keys())

    results = []
    exchange_names = list(sources.keys())

    for coin in sorted(all_coins):
        available = [name for name in exchange_names if coin in sources[name]]
        if len(available) < 2:
            continue

        for i in range(len(available)):
            for j in range(i + 1, len(available)):
                ex_a, ex_b = available[i], available[j]
                rate_a = sources[ex_a][coin]["funding"]
                rate_b = sources[ex_b][coin]["funding"]

                fee_key = (ex_a, ex_b) if (ex_a, ex_b) in PAIR_FEES else (ex_b, ex_a)
                round_trip_pct = PAIR_FEES.get(fee_key, 0) * 100

                gap_pct = abs(rate_a - rate_b) * 100
                net_pct = gap_pct - round_trip_pct

                results.append({
                    "coin": coin,
                    "exchange_a": ex_a,
                    "exchange_b": ex_b,
                    "rate_a_pct": rate_a * 100,
                    "rate_b_pct": rate_b * 100,
                    "gap_pct": gap_pct,
                    "round_trip_pct": round_trip_pct,
                    "net_pct": net_pct,
                    "profitable": net_pct > 0,
                })

    results.sort(key=lambda r: r["net_pct"], reverse=True)
    return results


if __name__ == "__main__":
    print("=" * 70)
    print("  INDIAN EXCHANGES SCANNER - Pi42 / Delta / CoinSwitch")
    print("=" * 70)
    results = run_indian_scan_cycle()
    profitable = [r for r in results if r["profitable"]]
    print(f"  {len(results)} pairwise comparisons \u00b7 {len(profitable)} profitable\n")

    print(f"{'COIN':<8} {'PAIR':<20} {'GAP pp':>9} {'FEE %':>8} {'NET %':>9}  STATUS")
    print("-" * 70)
    for r in results[:40]:
        status = "PROFITABLE" if r["profitable"] else ""
        pair = f"{r['exchange_a']}/{r['exchange_b']}"
        print(f"{r['coin']:<8} {pair:<20} {r['gap_pct']:>9.5f} {r['round_trip_pct']:>8.4f} {r['net_pct']:>+9.5f}  {status}")

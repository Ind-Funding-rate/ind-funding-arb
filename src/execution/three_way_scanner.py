"""
3-Way Funding Gap Scanner (Delta + Pi42 + CoinSwitch)
-------------------------------------------------------
Standalone. Does NOT touch full_market_scanner.py, its CSV logs, its
Telegram alerts, or the website - all of that keeps working exactly as
before, untouched, while this is tested independently.

For every coin, fetches funding rates from all 3 exchanges (where
available) and checks all 3 possible pairs:
    Delta <-> Pi42
    Delta <-> CoinSwitch
    Pi42  <-> CoinSwitch
...then reports whichever pair has the best fee-adjusted net% for that
coin. Detection/logging only - places NO orders, sends NO Telegram
alerts yet (easy to add once this is verified against real numbers).

Reuses the existing, already-proven Delta and Pi42 fetchers from
full_market_scanner.py rather than duplicating that logic, and the
existing CoinSwitch bulk fetcher confirmed working on 2026-07-26
(651 symbols in one call).

FEE ASSUMPTION TO VERIFY: CoinSwitch fee below assumes the same 18% GST
treatment as Delta/Pi42 (both Indian exchanges). This has NOT been
independently confirmed for CoinSwitch specifically - flagged here so
it isn't forgotten before this feeds anything with real money.
"""
import re
import sys
import csv
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.execution.full_market_scanner import (
    COINS, get_delta_funding_all, get_pi42_funding_all,
)
from src.data.coinswitch_client import get_all_funding_rates

PI42_FEE       = 0.080 * 1.18 / 100
DELTA_FEE      = 0.050 * 1.18 / 100
COINSWITCH_FEE = 0.050 * 1.18 / 100  # taker fee, GST assumption flagged above

ROUND_TRIP = {
    "Delta-Pi42":       2 * (DELTA_FEE + PI42_FEE),
    "Delta-CoinSwitch": 2 * (DELTA_FEE + COINSWITCH_FEE),
    "Pi42-CoinSwitch":  2 * (PI42_FEE + COINSWITCH_FEE),
}

LOG_DIR = Path("/home/container/logs")
LOG_DIR.mkdir(exist_ok=True)

_MULTIPLIER_PREFIX = re.compile(r"^(1000|1M)")


def strip_multiplier_prefix(coin):
    """'1000BONK' -> 'BONK', '1MBABYDOGE' -> 'BABYDOGE', 'BTC' -> 'BTC'.
    Delta/Pi42 use the multiplier prefix in their symbol names; CoinSwitch
    does not."""
    return _MULTIPLIER_PREFIX.sub("", coin)


def log_scan_to_csv(rows):
    log_file = LOG_DIR / f"three_way_scan_{datetime.now().strftime('%Y-%m-%d')}.csv"
    file_exists = log_file.exists()
    with open(log_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "coin", "best_pair",
            "delta_funding_pct", "pi42_funding_pct", "coinswitch_funding_pct",
            "gap_pct", "net_pct", "profitable",
        ])
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def best_pair_for_coin(delta_rate, pi42_rate, cs_rate):
    """Given whichever rates are available (any can be None), returns the
    best (pair_name, gap_pct, net_pct) among valid pairs, or None if fewer
    than 2 exchanges have data for this coin."""
    candidates = []
    pairs = [
        ("Delta-Pi42", delta_rate, pi42_rate),
        ("Delta-CoinSwitch", delta_rate, cs_rate),
        ("Pi42-CoinSwitch", pi42_rate, cs_rate),
    ]
    for name, a, b in pairs:
        if a is None or b is None:
            continue
        gap_pct = abs(a - b) * 100
        net_pct = gap_pct - (ROUND_TRIP[name] * 100)
        candidates.append((name, gap_pct, net_pct))

    if not candidates:
        return None
    candidates.sort(key=lambda c: c[2], reverse=True)
    return candidates[0]


def run_scan_cycle():
    print("  Fetching Delta (bulk)...")
    delta_data = get_delta_funding_all()

    print("  Fetching Pi42 (websocket batch, ~25s)...")
    pi42_data = get_pi42_funding_all(COINS)

    print("  Fetching CoinSwitch (bulk, 1 call)...")
    try:
        cs_raw = get_all_funding_rates()
    except Exception as e:
        print(f"  [!] CoinSwitch fetch failed: {e}")
        cs_raw = {}

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []

    for coin in COINS:
        d = delta_data.get(coin)
        p = pi42_data.get(coin)
        cs_symbol = strip_multiplier_prefix(coin) + "USDT"
        c = cs_raw.get(cs_symbol)

        delta_rate = d["funding"] if d else None
        pi42_rate = p["funding"] if p else None
        cs_rate = c["funding_rate"] if c else None

        best = best_pair_for_coin(delta_rate, pi42_rate, cs_rate)
        if best is None:
            continue  # fewer than 2 exchanges have this coin - skip

        pair_name, gap_pct, net_pct = best
        row = {
            "timestamp": now_str,
            "coin": coin,
            "best_pair": pair_name,
            "delta_funding_pct": delta_rate * 100 if delta_rate is not None else "",
            "pi42_funding_pct": pi42_rate * 100 if pi42_rate is not None else "",
            "coinswitch_funding_pct": cs_rate * 100 if cs_rate is not None else "",
            "gap_pct": gap_pct,
            "net_pct": net_pct,
            "profitable": net_pct > 0,
        }
        rows.append(row)

    log_scan_to_csv(rows)

    profitable_rows = [r for r in rows if r["profitable"]]
    rows.sort(key=lambda r: r["net_pct"], reverse=True)

    print(f"\n  Scanned {len(rows)} coins (had data for 2+ exchanges)")
    print(f"  Profitable: {len(profitable_rows)}")
    print(f"  CoinSwitch symbols available this cycle: {len(cs_raw)}")

    print("\n  Top 10 by net%:")
    for r in rows[:10]:
        flag = "✅" if r["profitable"] else "  "
        print(f"  {flag} {r['coin']:12s} best={r['best_pair']:18s} "
              f"gap={r['gap_pct']:+.4f}%  net={r['net_pct']:+.4f}%")

    return rows


if __name__ == "__main__":
    print("=" * 60)
    print("  3-WAY FUNDING GAP SCANNER (Delta + Pi42 + CoinSwitch)")
    print(f"  Scanning {len(COINS)} coins - single test cycle")
    print("  Detection only. No orders placed. No Telegram alerts yet.")
    print("=" * 60)
    run_scan_cycle()
    print("\n  Done. Check the printed table above and the CSV log at:")
    print("  /home/container/logs/three_way_scan_<date>.csv")

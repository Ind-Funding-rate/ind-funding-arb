"""
3-Way Funding Gap Scanner (Delta + Pi42 + CoinSwitch)
-------------------------------------------------------
Runs continuously, same pattern as full_market_scanner.py. Does NOT
touch that file, its CSV logs, its Telegram alerts, or the website -
all of that keeps working exactly as before, completely separate from
this.

For every coin, fetches funding rates from all 3 exchanges (where
available) and checks all 3 possible pairs:
    Delta <-> Pi42
    Delta <-> CoinSwitch
    Pi42  <-> CoinSwitch
...then alerts on whichever pair has the best fee-adjusted net% for
that coin, with a per-coin cooldown so the same opportunity doesn't
spam Telegram every cycle.

Detection/logging/alerting only - places NO orders.

FEE CORRECTION (2026-07-26): the previous version assumed CoinSwitch
charges the same 18% GST as Delta/Pi42. Checked 5+ independent CoinSwitch
fee-review sources directly - none mention GST for CoinSwitch specifically
(unlike Pi42, where GST is explicitly and repeatedly documented on their
own fee page). Corrected to a flat 0.05% taker fee, no GST assumption,
since that's what the evidence actually supports. If CoinSwitch does add
GST later, this constant is the one place to update.
"""
import re
import sys
import csv
import time
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.execution.full_market_scanner import (
    COINS, get_delta_funding_all, get_pi42_funding_all,
)
from src.data.coinswitch_client import get_all_funding_rates
from src.alerts.telegram import send_three_way_opportunity_alert, send_system_alert

PI42_FEE       = 0.080 * 1.18 / 100   # 0.0944% - taker + 18% GST, confirmed on Pi42's own fee page
DELTA_FEE      = 0.050 * 1.18 / 100   # 0.0590% - taker + 18% GST, confirmed on Delta's own fee page
COINSWITCH_FEE = 0.050 / 100          # 0.0500% - taker, no GST (checked 5+ independent sources, none mention GST for CoinSwitch)

ROUND_TRIP = {
    "Delta-Pi42":       2 * (DELTA_FEE + PI42_FEE),
    "Delta-CoinSwitch": 2 * (DELTA_FEE + COINSWITCH_FEE),
    "Pi42-CoinSwitch":  2 * (PI42_FEE + COINSWITCH_FEE),
}

CYCLE_SECONDS = 90
PER_COIN_COOLDOWN_SECONDS = 30 * 60

LOG_DIR = Path("/home/container/logs")
LOG_DIR.mkdir(exist_ok=True)

_MULTIPLIER_PREFIX = re.compile(r"^(1000|1M)")
_last_alert_time = {}


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
    """Runs one full 3-way scan across all COINS. Returns the list of
    result rows (also logs them to CSV and sends any due Telegram alerts
    as a side effect)."""
    delta_data = get_delta_funding_all()
    pi42_data = get_pi42_funding_all(COINS)
    try:
        cs_raw = get_all_funding_rates()
    except Exception as e:
        print(f"  [!] CoinSwitch fetch failed: {e}")
        cs_raw = {}

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    profitable_rows = []

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
        profitable = net_pct > 0
        row = {
            "timestamp": now_str,
            "coin": coin,
            "best_pair": pair_name,
            "delta_funding_pct": delta_rate * 100 if delta_rate is not None else "",
            "pi42_funding_pct": pi42_rate * 100 if pi42_rate is not None else "",
            "coinswitch_funding_pct": cs_rate * 100 if cs_rate is not None else "",
            "gap_pct": gap_pct,
            "net_pct": net_pct,
            "profitable": profitable,
        }
        rows.append(row)
        if profitable:
            profitable_rows.append((row, delta_rate, pi42_rate, cs_rate))

    log_scan_to_csv(rows)

    print(f"  Scanned {len(rows)} coins (had data for 2+ exchanges)")
    print(f"  Profitable: {len(profitable_rows)}  |  CoinSwitch symbols this cycle: {len(cs_raw)}")

    for row, delta_rate, pi42_rate, cs_rate in profitable_rows:
        coin = row["coin"]
        now = time.time()
        last = _last_alert_time.get(coin, 0)
        if now - last > PER_COIN_COOLDOWN_SECONDS:
            print(f"    -> ALERT: {coin}  best={row['best_pair']}  net={row['net_pct']:+.4f}%")
            send_three_way_opportunity_alert(
                coin=coin,
                pair_name=row["best_pair"],
                gap_pct=row["gap_pct"],
                net_pct=row["net_pct"],
                delta_rate=delta_rate,
                pi42_rate=pi42_rate,
                coinswitch_rate=cs_rate,
            )
            _last_alert_time[coin] = now
        else:
            print(f"    -> {coin} profitable but in cooldown "
                  f"({int((PER_COIN_COOLDOWN_SECONDS-(now-last))/60)}m left)")

    rows.sort(key=lambda r: r["net_pct"], reverse=True)
    return rows


if __name__ == "__main__":
    print("=" * 60)
    print("  3-WAY FUNDING GAP MONITOR (Delta + Pi42 + CoinSwitch)")
    print(f"  Scanning {len(COINS)} coins every {CYCLE_SECONDS}s")
    print("  Detection + alerting only. No orders placed.")
    print("=" * 60)

    send_system_alert(f"3-way scanner started - watching {len(COINS)} coins across Delta/Pi42/CoinSwitch")

    cycle = 0
    while True:
        cycle += 1
        print(f"\n-- Scan cycle {cycle} - {time.strftime('%Y-%m-%d %H:%M:%S')} --")
        try:
            run_scan_cycle()
        except Exception as e:
            print(f"  [!] Scan cycle failed: {e}")
        time.sleep(CYCLE_SECONDS)

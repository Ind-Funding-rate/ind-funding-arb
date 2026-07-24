"""
Backtest engine - simulates historical returns for a funding-rate arb
position using OUR OWN logged scan data (logs/scan_YYYY-MM-DD.csv, written
continuously by full_market_scanner.py).

IMPORTANT HONESTY NOTE:
Unlike commercial tools (e.g. loris.tools) which have months of historical
funding data across many exchanges, we do NOT have historical funding data
for Pi42 - it has no historical funding API, only live WebSocket data. So
this engine can only backtest over the period our own scanner has actually
been running and logging. On day one, that's ~0 days of data and results
will be empty. The longer full_market_scanner.py runs, the more useful
this becomes.

Usage:
    python src/execution/backtest_engine.py --coin BTC --days 7 --position 1000
    python src/execution/backtest_engine.py --coin ETH --days 30 --position 5000
"""
import csv
import sys
import argparse
from pathlib import Path
from datetime import datetime, timedelta

LOG_DIR = Path("/home/container/logs")

PI42_FEE   = 0.080 * 1.18 / 100
DELTA_FEE  = 0.050 * 1.18 / 100
ROUND_TRIP_PCT = 2 * (PI42_FEE + DELTA_FEE) * 100  # as a percentage


def load_rows(coin: str, days: int):
    cutoff = datetime.now() - timedelta(days=days)
    rows = []
    for log_file in sorted(LOG_DIR.glob("scan_*.csv")):
        try:
            file_date = datetime.strptime(log_file.stem.replace("scan_", ""), "%Y-%m-%d")
        except ValueError:
            continue
        if file_date < cutoff - timedelta(days=1):
            continue
        with open(log_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                if row.get("coin") != coin:
                    continue
                try:
                    ts = datetime.strptime(row["timestamp"], "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    continue
                if ts < cutoff:
                    continue
                rows.append((ts, row))
    rows.sort(key=lambda x: x[0])
    return rows


def backtest(coin: str, days: int, position_usd: float):
    rows = load_rows(coin, days)

    print("=" * 60)
    print(f"  BACKTEST: {coin}  |  last {days} day(s)  |  ${position_usd:,.0f} position")
    print("=" * 60)

    if len(rows) < 2:
        print(f"  Not enough logged data yet for {coin} over this period.")
        print(f"  Found {len(rows)} data point(s).")
        print()
        print("  This is expected if full_market_scanner.py has only been")
        print("  running for a short time. Historical data builds up the")
        print("  longer it runs - come back after a few days for a real")
        print("  backtest. We don't have (and can't get) funding history")
        print("  from before the scanner started, since Pi42 has no")
        print("  historical funding-rate API.")
        return

    # Simulate: accrue the funding differential between consecutive
    # readings, weighted by actual elapsed time (since our polling
    # interval isn't exactly aligned to funding payout times, this is
    # a time-weighted approximation of continuous accrual, not literal
    # payout-by-payout accounting).
    total_return_pct = 0.0
    entered = False
    entry_time = None
    time_in_position_hours = 0.0

    for i in range(1, len(rows)):
        prev_ts, prev_row = rows[i - 1]
        ts, row = rows[i]
        elapsed_hours = (ts - prev_ts).total_seconds() / 3600

        net_pct = float(prev_row["net_pct"])
        profitable = prev_row["profitable"] == "True"

        if profitable:
            if not entered:
                entered = True
                entry_time = prev_ts
            # Funding accrues continuously; net_pct is per-8h-cycle edge,
            # so scale it down to this actual elapsed slice.
            per_hour_pct = net_pct / 8
            total_return_pct += per_hour_pct * elapsed_hours
            time_in_position_hours += elapsed_hours
        else:
            entered = False

    position_pnl_usd = position_usd * (total_return_pct / 100)
    days_covered = (rows[-1][0] - rows[0][0]).total_seconds() / 86400
    apy_pct = (total_return_pct / days_covered * 365) if days_covered > 0 else 0

    print(f"  Data points               : {len(rows)}")
    print(f"  Period covered            : {days_covered:.2f} days")
    print(f"  Time in profitable state  : {time_in_position_hours:.1f} hours "
          f"({time_in_position_hours / (days_covered*24) * 100 if days_covered else 0:.1f}% of period)")
    print(f"  Simulated total return    : {total_return_pct:+.4f}%")
    print(f"  Simulated P&L on ${position_usd:,.0f}  : ${position_pnl_usd:+,.2f}")
    print(f"  Annualized (APY)          : {apy_pct:+.2f}%")
    print()
    print("  NOTE: this simulates HOLDING the position only while the")
    print("  logged data showed a profitable net edge, and assumes fees")
    print("  were paid once (entry) not repeatedly - real trading would")
    print("  need to re-enter after each flip out of profitability, which")
    print("  costs fees each time. Treat this as an optimistic upper bound,")
    print("  not a realistic guarantee.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--coin", default="BTC")
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--position", type=float, default=1000)
    args = parser.parse_args()

    backtest(args.coin, args.days, args.position)

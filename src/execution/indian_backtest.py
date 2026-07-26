"""
Indian 3-way backtest - simulates historical returns using OUR OWN logged
data from three_way_scanner.py (logs/three_way_scan_YYYY-MM-DD.csv).

Same honesty limitation as backtest_engine.py: none of Delta, Pi42, or
CoinSwitch publish historical funding-rate data anywhere - only live
snapshots. So this can only cover the period since the 3-way scanner has
actually been running and logging, not further back.

net_pct in the logged rows is already fee-adjusted per the SPECIFIC pair
that won that cycle (Delta-Pi42, Delta-CoinSwitch, or Pi42-CoinSwitch each
have their own real fee total) - this reads that value directly rather
than recomputing fees, so it always matches whatever the live scanner
actually used.
"""
import csv
from pathlib import Path
from datetime import datetime, timedelta

LOG_DIR = Path("/home/container/logs")


def load_rows(coin: str, days: int):
    cutoff = datetime.now() - timedelta(days=days)
    rows = []
    for log_file in sorted(LOG_DIR.glob("three_way_scan_*.csv")):
        try:
            file_date = datetime.strptime(
                log_file.stem.replace("three_way_scan_", ""), "%Y-%m-%d"
            )
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


def compute_indian_backtest(coin: str, days: int, position_usd: float) -> dict:
    rows = load_rows(coin, days)

    if len(rows) < 2:
        return {
            "enough_data": False,
            "data_points": len(rows),
            "coin": coin,
            "days": days,
            "position_usd": position_usd,
        }

    total_return_pct = 0.0
    time_in_position_hours = 0.0
    pair_time_hours = {}  # tracks which pair was used while profitable

    for i in range(1, len(rows)):
        prev_ts, prev_row = rows[i - 1]
        ts, row = rows[i]
        elapsed_hours = (ts - prev_ts).total_seconds() / 3600

        net_pct = float(prev_row["net_pct"])
        profitable = prev_row["profitable"] == "True"
        pair = prev_row.get("best_pair", "?")

        if profitable:
            # Assume 8h funding interval for the accrual rate, same
            # convention as backtest_engine.py, for consistency across
            # both backtest pages.
            per_hour_pct = net_pct / 8
            total_return_pct += per_hour_pct * elapsed_hours
            time_in_position_hours += elapsed_hours
            pair_time_hours[pair] = pair_time_hours.get(pair, 0) + elapsed_hours

    position_pnl_usd = position_usd * (total_return_pct / 100)
    days_covered = (rows[-1][0] - rows[0][0]).total_seconds() / 86400
    apy_pct = (total_return_pct / days_covered * 365) if days_covered > 0 else 0
    pct_time_profitable = (
        time_in_position_hours / (days_covered * 24) * 100 if days_covered else 0
    )

    dominant_pair = max(pair_time_hours, key=pair_time_hours.get) if pair_time_hours else "N/A"

    return {
        "enough_data": True,
        "data_points": len(rows),
        "coin": coin,
        "days": days,
        "position_usd": position_usd,
        "days_covered": round(days_covered, 2),
        "time_in_position_hours": round(time_in_position_hours, 1),
        "pct_time_profitable": round(pct_time_profitable, 1),
        "total_return_pct": round(total_return_pct, 4),
        "position_pnl_usd": round(position_pnl_usd, 2),
        "apy_pct": round(apy_pct, 2),
        "dominant_pair": dominant_pair,
    }

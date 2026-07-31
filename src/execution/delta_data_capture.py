"""
Delta market data capture loop - runs continuously, writing price, order
book, and option chain snapshots to the local Parquet store on the
agreed cadence:
    price + order book : every 5 minutes
    option chain        : every 5 minutes (same tick, kept in sync)
    (funding rate already has its own separate, slower 30-90s scanners
    elsewhere in this project - not duplicated here)

Checks disk usage every cycle and prints a clear warning if it crosses
the threshold set in market_data_store.py - this is meant to be watched,
not to silently fill the disk the way earlier crashes did tonight.

Coins captured: BTC and ETH only for now - these are Delta's main option
markets, and keeping the coin list small at this early stage keeps
storage growth predictable while we confirm the whole pipeline works
end to end before expanding to more coins.
"""
import time
from datetime import datetime
from src.data.delta_market_data import (
    get_price_snapshot, get_orderbook_snapshot, get_option_chain_snapshot,
)
from src.data.market_data_store import write_snapshot, check_disk_usage

CYCLE_SECONDS = 5 * 60  # 5 minutes, as agreed

PRICE_SYMBOLS = ["BTCUSD", "ETHUSD"]
OPTION_UNDERLYINGS = ["BTC", "ETH"]
ORDERBOOK_DEPTH = 10


def run_capture_cycle():
    now_str = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"\n-- Delta capture cycle - {now_str} UTC --")

    # Price
    price_rows = get_price_snapshot(PRICE_SYMBOLS)
    write_snapshot("price", "Delta", price_rows)
    print(f"  Price      : {len(price_rows)} symbols captured")

    # Order book (one snapshot per symbol)
    ob_count = 0
    for symbol in PRICE_SYMBOLS:
        rows = get_orderbook_snapshot(symbol, depth=ORDERBOOK_DEPTH)
        write_snapshot("order_book", "Delta", rows)
        ob_count += len(rows)
    print(f"  Order book : {ob_count} price levels captured across {len(PRICE_SYMBOLS)} symbols")

    # Option chain (one snapshot per underlying)
    opt_count = 0
    for underlying in OPTION_UNDERLYINGS:
        rows = get_option_chain_snapshot(underlying, max_expiries=2)
        write_snapshot("option_chain", "Delta", rows)
        opt_count += len(rows)
    print(f"  Options    : {opt_count} contracts captured across {len(OPTION_UNDERLYINGS)} underlyings")

    usage = check_disk_usage()
    status = "\u26a0\ufe0f WARNING - approaching limit" if usage["warning"] else "OK"
    print(f"  Disk usage : {usage['total_gb']} GB / {usage['warning_threshold_gb']} GB warning threshold ({status})")


if __name__ == "__main__":
    print("=" * 60)
    print("  DELTA MARKET DATA CAPTURE")
    print(f"  Price + order book + options every {CYCLE_SECONDS // 60} min")
    print(f"  Coins: {PRICE_SYMBOLS} (price/book), {OPTION_UNDERLYINGS} (options)")
    print("=" * 60)

    cycle = 0
    while True:
        cycle += 1
        try:
            run_capture_cycle()
        except Exception as e:
            print(f"  [!] Capture cycle failed: {e}")
        time.sleep(CYCLE_SECONDS)

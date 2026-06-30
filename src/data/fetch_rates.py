import asyncio
import csv
import sys
import logging
from pathlib import Path
from datetime import datetime

# Windows terminals sometimes default to a codepage that can't print
# special characters like ₹, 🚀, ⚠ - force UTF-8 so this never crashes.
if sys.stdout.encoding.lower() != "utf-8":
    sys.stdout.reconfigure(encoding="utf-8")

# Make "src/" importable so we can reach the websocket/ and database/ packages
PROJECT_ROOT = Path(__file__).resolve().parents[2]   # src/data -> src -> project root
sys.path.append(str(PROJECT_ROOT / "src"))

from database import init_db, save_rate, get_rate_count
from feeds import pi42_client, delta_client
from detection import compare, opportunity
from alerts.telegram import send_opportunity_alert

# ── LOGGING ──────────────────────────────────────────────────
# Operational events (connect/disconnect/errors) are timestamped and saved
# to logs/app.log, so you have a record even if you weren't watching the
# screen. The live price table below uses plain print() - that's a
# dashboard, not something that needs a permanent record.
LOG_DIR = Path("logs")
LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] [%(name)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_DIR / "app.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger("fetch_rates")

# ── CONFIG: which pairs to track ──────────────────────────────
# To track another pair later, just add another entry here.
PAIRS = [
    {
        "name": "BTC",
        "pi42_symbol": "BTCUSDT",
        "pi42_channel": "btcusdt@markPrice",
        "delta_symbol": "BTCUSD",
        "delta_mark_symbol": "MARK:BTCUSD",
    },
]

# ── SHARED STATE ───────────────────────────────────────────────
# Both listeners WRITE into this dict via update_latest(). The reporter
# loop only ever READS from it. Safe without locks since everything runs
# in one asyncio event loop (cooperative, not real threads).
latest = {}
last_alert_time = None   # Track when we last sent a Telegram alert
ALERT_COOLDOWN_MINUTES = 30  # Don't spam — max 1 alert per 30 minutes


def update_latest(exchange, symbol, mark_price=None, funding_rate=None):
    key = (exchange, symbol)
    entry = latest.get(key, {})
    if mark_price is not None:
        entry["mark_price"] = mark_price
    if funding_rate is not None:
        entry["funding_rate"] = funding_rate
    entry["updated_at"] = datetime.now()
    latest[key] = entry


# ── DATA LOG FILE (CSV history, separate from operational logging) ─────
def write_log(data):
    log_file = LOG_DIR / f"rates_{datetime.now().strftime('%Y-%m-%d')}.csv"
    file_exists = log_file.exists()
    with open(log_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "exchange", "symbol", "mark_price", "funding_rate"
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerow(data)


# ── DISPLAY ──────────────────────────────────────────────────
def display():
    print("\n" + "=" * 52)
    print(f"  LIVE RATES  —  {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 52)
    for pair in PAIRS:
        d = latest.get(("Delta", pair["delta_symbol"]))
        p = latest.get(("Pi42", pair["pi42_symbol"]))

        if d:
            print(f"  DELTA  | Mark Price   : ${d.get('mark_price', 0):>12,.2f}")
            fr = d.get("funding_rate")
            print(f"         | Funding Rate : {fr*100:>10.6f}%" if fr is not None else "         | Funding Rate : pending...")
        else:
            print("  DELTA  | ⚠ No data yet")

        print("-" * 52)

        if p:
            print(f"  PI42   | BTC Price    : ${p.get('mark_price', 0):>12,.2f}")
            fr = p.get("funding_rate")
            print(f"         | Funding Rate : {fr*100:>10.6f}%" if fr is not None else "         | Funding Rate : pending...")
        else:
            print("  PI42   | ⚠ No data yet")

        gap_info = compare.compute_gap(p, d)
        if gap_info:
            opp = opportunity.assess(gap_info)
            print("-" * 52)
            print(f"  GAP    | {gap_info['higher_exchange']} is higher by {abs(gap_info['gap_pct']):.6f} pp")
            if opp:
                net = opp['gap_pct'] - opp['round_trip_fee_pct']
                profitable = net > 0
                print(f"  FEES   | {opp['round_trip_fee_pct']:.4f}% round trip (taker + GST)")
                if profitable:
                    print(f"  NET    | +{net:.4f}% ✅ PROFITABLE")
                    print(f"  BRKEVN | {opp['breakeven_payouts']:.1f} payouts (~{opp['breakeven_hours']:.1f} hrs)")
                    # ── TELEGRAM ALERT (max once per 30 minutes) ──
                    global last_alert_time
                    now = datetime.now()
                    if (last_alert_time is None or
                            (now - last_alert_time).total_seconds() > ALERT_COOLDOWN_MINUTES * 60):
                        send_opportunity_alert(
                            gap_pct=gap_info['gap_pct'],
                            net_pct=net,
                            breakeven_hrs=opp['breakeven_hours'],
                            delta_rate=d.get('funding_rate', 0),
                            pi42_rate=p.get('funding_rate', 0) if p else 0,
                        )
                        last_alert_time = now
                        print(f"  📱 Alert sent to Telegram")
                else:
                    print(f"  NET    | {net:.4f}% ❌ NOT profitable after fees")
    print("=" * 52)


# ── REPORTER LOOP ──────────────────────────────────────────────
async def reporter_loop():
    """Every 5 seconds, saves whatever is freshest in `latest` to the CSV
    log and database, then prints it. Makes NO network requests itself -
    pi42_client and delta_client keep `latest` fresh in the background."""
    while True:
        for pair in PAIRS:
            for exchange, symbol in [("Delta", pair["delta_symbol"]), ("Pi42", pair["pi42_symbol"])]:
                entry = latest.get((exchange, symbol))
                if entry is None:
                    continue
                row = {
                    "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "exchange": exchange,
                    "symbol": symbol,
                    "mark_price": entry.get("mark_price", 0),
                    "funding_rate": entry.get("funding_rate"),
                }
                try:
                    write_log(row)
                    save_rate(row)
                except Exception as e:
                    logger.error(f"Failed to save a {exchange} reading: {e}")

        display()
        print(f"  DB records: {get_rate_count()}")
        await asyncio.sleep(5)


# ── MAIN ────────────────────────────────────────────────────────
async def main():
    logger.info("Starting funding rate streamer (real-time WebSocket mode)")
    print("\n🚀 Starting funding rate streamer (real-time WebSocket mode)...")
    print("   Press Ctrl+C to stop\n")
    init_db()
    await asyncio.gather(
        pi42_client.listen(PAIRS, update_latest),
        delta_client.listen(PAIRS, update_latest),
        reporter_loop(),
    )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\n🛑 Stopped by user. Goodbye!")

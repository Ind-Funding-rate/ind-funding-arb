"""
Full-market funding gap monitor.

Runs continuously. Every cycle: fetches funding rates for every coin
available on BOTH Delta and Pi42 (133 pairs, confirmed live from both
exchanges' own APIs), computes the fee-adjusted gap for each, logs
everything to CSV, and sends a Telegram alert (per-coin cooldown) the
moment any coin shows a genuinely profitable gap after fees.

This does NOT place any orders - detection and alerting only.
"""
import requests
import time
import asyncio
import json
import csv
import sys
from pathlib import Path
from datetime import datetime
import websockets
from dotenv import load_dotenv
import os

load_dotenv("/home/container/.env")

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.alerts.telegram import send_multi_opportunity_alert, send_system_alert

# Fee constants (confirmed from official fee pages, taker + 18% GST)
PI42_FEE   = 0.080 * 1.18 / 100
DELTA_FEE  = 0.050 * 1.18 / 100
ROUND_TRIP = 2 * (PI42_FEE + DELTA_FEE)  # as a fraction

PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"

CYCLE_SECONDS = 90            # full scan takes ~20-25s, so this gives breathing room
PER_COIN_COOLDOWN_SECONDS = 30 * 60   # 30 min before re-alerting the same coin

# Full overlap list - every coin confirmed live on BOTH Delta (xUSD) and
# Pi42 (xINR), captured directly from both exchanges' own APIs.
COINS = [
    "1000BONK", "1000FLOKI", "1000PEPE", "1000SATS", "1000SHIB", "1MBABYDOGE",
    "AAVE", "ACT", "ADA", "AIXBT", "AKE", "ALLO", "API3", "APT", "AR", "ARB",
    "ARC", "ASTER", "AVAX", "AXS", "BB", "BCH", "BEAT", "BERA", "BIO", "BLESS",
    "BNB", "BTC", "CHIP", "COAI", "COOKIE", "DASH", "DOGE", "DOGS", "DOT",
    "EIGEN", "ENA", "ENJ", "ESPORTS", "ETC", "ETH", "ETHFI", "EVAA",
    "FARTCOIN", "FIL", "GALA", "GIGGLE", "GOAT", "GRIFFAIN", "H", "HANA",
    "HBAR", "HYPE", "INJ", "IO", "JASMY", "JTO", "JUP", "KAITO", "KITE",
    "LAB", "LDO", "LIGHT", "LINK", "LISTA", "LIT", "LTC", "M", "MASK",
    "MELANIA", "MEME", "MOODENG", "MOVE", "MUBARAK", "NEAR", "NEIRO", "NOT",
    "ONDO", "OP", "ORDI", "PARTI", "PAXG", "PENDLE", "PENGU", "PEOPLE",
    "PIEVERSE", "PIPPIN", "PNUT", "POL", "POPCAT", "PUMP", "RARE", "RAVE",
    "RED", "RIVER", "RUNE", "S", "SAGA", "SAHARA", "SEI", "SIREN", "SKL",
    "SKYAI", "SOL", "SOLV", "SPX", "STBL", "STRK", "SUI", "SWARMS", "TAO",
    "TIA", "TNSR", "TRB", "TRUMP", "TRX", "TST", "UNI", "VIRTUAL", "VVV",
    "WIF", "WLD", "WLFI", "XAI", "XAUT", "XLM", "XMR", "XPL", "XRP", "ZEC",
    "ZEN", "ZORA", "ZRO",
]

LOG_DIR = Path("/home/container/logs")
LOG_DIR.mkdir(exist_ok=True)

_last_alert_time = {}  # coin -> timestamp of last alert


def log_scan_to_csv(rows):
    log_file = LOG_DIR / f"scan_{datetime.now().strftime('%Y-%m-%d')}.csv"
    file_exists = log_file.exists()
    with open(log_file, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "timestamp", "coin", "delta_funding_pct", "pi42_funding_pct",
            "gap_pct", "net_pct", "delta_volume_usd", "profitable"
        ])
        if not file_exists:
            writer.writeheader()
        for row in rows:
            writer.writerow(row)


def get_delta_funding_all():
    r = requests.get("https://api.india.delta.exchange/v2/tickers", timeout=20).json()
    out = {}
    for t in r.get("result", []):
        symbol = t.get("symbol", "")
        if symbol.endswith("USD") and not symbol.endswith("USDT"):
            base = symbol[:-3]
            raw = t.get("funding_rate")
            vol = t.get("turnover_usd") or t.get("volume")
            if raw is not None:
                # Same unit fix as the main executor: Delta's funding_rate
                # is already a percentage, not a fraction - divide by 100.
                out[base] = {
                    "funding": float(raw) / 100,
                    "volume_usd": float(vol) if vol else 0,
                }
    return out


async def _pi42_ws_batch(symbols):
    channels = [f"{s.lower()}inr@markPrice" for s in symbols]
    results = {}
    async with websockets.connect(PI42_WS_URL) as ws:
        await ws.recv()
        await ws.send("40")
        await ws.recv()
        sub_msg = f'42["subscribe", {{"params": {json.dumps(channels)}}}]'
        await ws.send(sub_msg)

        end_time = asyncio.get_event_loop().time() + 25
        while asyncio.get_event_loop().time() < end_time and len(results) < len(symbols):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=25)
            except asyncio.TimeoutError:
                break
            if msg == "2":
                await ws.send("3")
                continue
            if not msg.startswith("42["):
                continue
            payload = json.loads(msg[2:])
            event_name = payload[0]
            data = payload[1] if len(payload) > 1 else {}
            if event_name == "markPriceUpdate":
                sym = data.get("s", "")
                if sym.endswith("INR"):
                    base = sym[:-3]
                    if base not in results:
                        results[base] = {"funding": float(data.get("r", 0))}
    return results


def get_pi42_funding_all(symbols):
    try:
        return asyncio.run(_pi42_ws_batch(symbols))
    except Exception as e:
        print(f"  [!] Pi42 batch fetch failed: {e}")
        return {}


def run_scan_cycle():
    delta_data = get_delta_funding_all()
    pi42_data = get_pi42_funding_all(COINS)

    rows = []
    profitable_rows = []
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    for coin in COINS:
        d = delta_data.get(coin)
        p = pi42_data.get(coin)
        if not d or not p:
            continue
        gap_pct = abs(d["funding"] - p["funding"]) * 100
        net_pct = gap_pct - (ROUND_TRIP * 100)
        profitable = net_pct > 0

        row = {
            "timestamp": now_str,
            "coin": coin,
            "delta_funding_pct": d["funding"] * 100,
            "pi42_funding_pct": p["funding"] * 100,
            "gap_pct": gap_pct,
            "net_pct": net_pct,
            "delta_volume_usd": d["volume_usd"],
            "profitable": profitable,
        }
        rows.append(row)
        if profitable:
            profitable_rows.append((row, d, p))

    log_scan_to_csv(rows)

    print(f"  Scanned {len(rows)} coins (had data for both exchanges)")
    print(f"  Profitable: {len(profitable_rows)}")

    for row, d, p in profitable_rows:
        coin = row["coin"]
        now = time.time()
        last = _last_alert_time.get(coin, 0)
        if now - last > PER_COIN_COOLDOWN_SECONDS:
            print(f"    -> ALERT: {coin}  net={row['net_pct']:+.4f}%  "
                  f"vol=${row['delta_volume_usd']:,.0f}")
            send_multi_opportunity_alert(
                coin=coin,
                gap_pct=row["gap_pct"],
                net_pct=row["net_pct"],
                delta_rate=d["funding"],
                pi42_rate=p["funding"],
                delta_volume_usd=row["delta_volume_usd"],
            )
            _last_alert_time[coin] = now
        else:
            print(f"    -> {coin} profitable but in cooldown ({int((PER_COIN_COOLDOWN_SECONDS-(now-last))/60)}m left)")


if __name__ == "__main__":
    print("=" * 60)
    print("  FULL-MARKET FUNDING GAP MONITOR")
    print(f"  Scanning {len(COINS)} coins every {CYCLE_SECONDS}s")
    print(f"  Round-trip fee: {ROUND_TRIP*100:.4f}%")
    print("=" * 60)

    send_system_alert(f"Full-market scanner started - watching {len(COINS)} coins")

    cycle = 0
    while True:
        cycle += 1
        print(f"\n-- Scan cycle {cycle} - {time.strftime('%Y-%m-%d %H:%M:%S')} --")
        try:
            run_scan_cycle()
        except Exception as e:
            print(f"  [!] Scan cycle failed: {e}")
        time.sleep(CYCLE_SECONDS)

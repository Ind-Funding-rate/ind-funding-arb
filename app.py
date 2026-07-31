import requests
import time
import hmac
import hashlib
import json
import threading
import sys
import os
from pathlib import Path
from dotenv import load_dotenv

sys.path.append(str(Path(__file__).resolve().parent))

load_dotenv("/home/container/bot/.env")

DELTA_KEY    = os.getenv("DELTA_API_KEY")
DELTA_SECRET = os.getenv("DELTA_API_SECRET")
PI42_KEY     = os.getenv("PI42_API_KEY")
PI42_SECRET  = os.getenv("PI42_API_SECRET")

# ══════════════════════════════════════════════════════
#  MASTER SWITCH
#  PAPER_MODE = True  → logs decisions only, zero real orders
#  PAPER_MODE = False → places real orders with real money
#  DO NOT change to False until paper mode is confirmed correct
# ══════════════════════════════════════════════════════
PAPER_MODE = True

# Minimum trade size (confirmed from both exchange docs)
BTC_QTY = 0.001  # 0.001 BTC per side — smallest allowed on both exchanges

# Fee constants (confirmed from official fee pages, taker + 18% GST)
PI42_FEE    = 0.080 * 1.18 / 100   # 0.09440%
DELTA_FEE   = 0.050 * 1.18 / 100   # 0.05900%
ROUND_TRIP  = 2 * (PI42_FEE + DELTA_FEE)  # 4 trades: open+close on both exchanges


# ── Order placement helpers (Delta/Pi42 signed REST calls) ──────

def pi42_post(path, params):
    ts = str(int(time.time() * 1000))
    params["timestamp"] = ts
    body = json.dumps(params, separators=(",", ":"))
    sig  = hmac.new(PI42_SECRET.encode(), body.encode(), hashlib.sha256).hexdigest()
    r    = requests.post(
        f"https://fapi.pi42.com{path}",
        json=params,
        headers={"api-key": PI42_KEY, "signature": sig, "Content-Type": "application/json"},
        timeout=10
    )
    return r.status_code, r.json()

def delta_post(path, params):
    body = json.dumps(params)
    ts   = str(int(time.time()))
    sig  = hmac.new(DELTA_SECRET.encode(),
                    ("POST" + ts + path + body).encode(), hashlib.sha256).hexdigest()
    r    = requests.post(
        f"https://api.india.delta.exchange{path}",
        json=params,
        headers={"api-key": DELTA_KEY, "timestamp": ts, "signature": sig,
                 "Content-Type": "application/json"},
        timeout=10
    )
    return r.status_code, r.json()


# ── Fetch live rates ─────────────────────────────────────────
# 2026-07-27: this used to have its own separate Delta/Pi42 REST calls
# here, with two confirmed bugs - Delta's funding_rate was displayed
# 100x too high (missing the /100 conversion that the proven scanner
# code applies), and Pi42's REST endpoint returned entirely empty data
# (0.00 for both price and funding). Now reuses get_delta_funding_all()
# and get_pi42_funding_all() from full_market_scanner.py, the same
# functions already proven correct and running live in the scanner and
# website for months - just pulls out the "BTC" entry from each.

from src.execution.full_market_scanner import get_delta_funding_all, get_pi42_funding_all


def get_rates():
    rates = {}
    try:
        delta_data = get_delta_funding_all()
        btc = delta_data.get("BTC")
        if btc:
            rates["delta_price"]   = btc["price"]
            rates["delta_funding"] = btc["funding"]
    except Exception as e:
        print(f"  [!] Delta rate fetch failed: {e}")
    try:
        pi42_data = get_pi42_funding_all(["BTC"])
        btc = pi42_data.get("BTC")
        if btc:
            rates["pi42_price"]   = btc["price"]
            rates["pi42_funding"] = btc["funding"]
    except Exception as e:
        print(f"  [!] Pi42 rate fetch failed: {e}")
    return rates


# ── Paper order logger ──────────────────────────────────────────

def log_paper_order(exchange, side, symbol, qty, price, reason):
    print(f"  [PAPER] WOULD PLACE → {exchange} | {side} {qty} {symbol} "
          f"@ ~{price:.2f} | {reason}")


# ── Real order placement ─────────────────────────────────────────

def place_pi42_order(side, qty):
    """Place a market order on Pi42. side = 'BUY' or 'SELL'."""
    params = {
        "placeType":   "ORDER_FORM",
        "quantity":    qty,
        "side":        side,
        "symbol":      "BTCINR",
        "type":        "MARKET",
        "reduceOnly":  False,
        "marginAsset": "INR",
        "deviceType":  "WEB",
        "userCategory": "EXTERNAL",
    }
    return pi42_post("/v1/order/place-order", params)

def place_delta_order(side, qty):
    """Place a market order on Delta. side = 'buy' or 'sell'."""
    params = {
        "product_id": 27,          # BTCUSD perpetual on Delta India (confirmed)
        "order_type": "market_order",
        "side":       side,
        "size":       qty,
    }
    return delta_post("/v2/orders", params)


# ══════════════════════════════════════════════════════
#  WEBSITE (background) - src/web/app.py
#
#  2026-07-31: TEMPORARY compatibility fix. src/web/app.py was
#  independently rewritten (by Codex) since this was last touched here -
#  it no longer has background_three_way_scanner (the CoinSwitch 3-way
#  integration was removed from that file), and it gained a new
#  "Spread Scanner" page with its own start_spread_background_loop().
#  This import list now matches whatever src/web/app.py ACTUALLY
#  exports today, so the site boots instead of crashing - this is not
#  a decision about which version is "right", just getting the site
#  running again so it can be evaluated. See conversation with Nikunj
#  before changing src/web/app.py further - there's an unresolved
#  question about whether to restore the 3-way/CoinSwitch integration
#  and visual redesign that were both dropped in that rewrite.
# ══════════════════════════════════════════════════════

def run_website_background():
    try:
        from src.web.app import (
            app as flask_app,
            background_scanner,
            ingestion_background,
            ranking_background,
            start_spread_background_loop,
        )
    except Exception as e:
        print(f"  [WEB] Failed to load website module: {e}")
        return

    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=ingestion_background, daemon=True).start()
    threading.Thread(target=ranking_background, daemon=True).start()
    start_spread_background_loop()

    port = int(os.getenv("SERVER_PORT", 8080))
    print(f"  [WEB] Starting dashboard on 0.0.0.0:{port}")
    flask_app.run(host="0.0.0.0", port=port, threaded=True, use_reloader=False)


threading.Thread(target=run_website_background, daemon=True).start()


# ── Main loop (BTC-only executor) ────────────────────────────────

print("=" * 54)
print(f"  FUNDING ARB EXECUTOR — {'PAPER MODE 📝' if PAPER_MODE else '🔴 LIVE MODE — REAL MONEY'}")
print("=" * 54)
print()

cycle = 0
while True:
    cycle += 1
    print(f"\n── Cycle {cycle} — {time.strftime('%Y-%m-%d %H:%M:%S')} ──")

    rates = get_rates()

    if "delta_funding" not in rates:
        print("  Waiting for Delta data...")
        time.sleep(30)
        continue
    if "pi42_funding" not in rates:
        print("  Waiting for Pi42 data...")
        time.sleep(30)
        continue

    delta_rate  = rates["delta_funding"]
    pi42_rate   = rates["pi42_funding"]
    delta_price = rates.get("delta_price", 0)
    pi42_price  = rates.get("pi42_price", 0)

    gap        = abs(delta_rate - pi42_rate)
    gap_pct    = gap * 100
    net_pct    = gap_pct - (ROUND_TRIP * 100)
    profitable = net_pct > 0

    # Direction: go SHORT on the higher-rate exchange (receive funding)
    #            go LONG  on the lower-rate exchange  (pay less funding)
    if delta_rate > pi42_rate:
        long_exchange  = "Pi42"
        short_exchange = "Delta"
        long_price     = pi42_price
        short_price    = delta_price
    else:
        long_exchange  = "Delta"
        short_exchange = "Pi42"
        long_price     = delta_price
        short_price    = pi42_price

    print(f"  Delta funding : {delta_rate*100:.6f}%  (price: ${delta_price:,.2f})")
    print(f"  Pi42  funding : {pi42_rate*100:.6f}%  (price: ₹{pi42_price:,.2f})")
    print(f"  Gap           : {gap_pct:.6f} pp")
    print(f"  Round-trip fee: {ROUND_TRIP*100:.4f}%")
    print(f"  Net edge      : {net_pct:+.6f}%  {'[OK] PROFITABLE' if profitable else '[--] not profitable'}")

    if profitable:
        print(f"  Direction     : LONG {long_exchange} / SHORT {short_exchange}")
        print(f"  Qty per side  : {BTC_QTY} BTC")

        if PAPER_MODE:
            log_paper_order(long_exchange,  "BUY",  "BTC", BTC_QTY, long_price,  "funding arb - long leg")
            log_paper_order(short_exchange, "SELL", "BTC", BTC_QTY, short_price, "funding arb - short leg")
            print("  [PAPER] No real orders sent.")
        else:
            print("  Placing REAL orders...")
            pi42_side  = "BUY"  if long_exchange  == "Pi42"  else "SELL"
            delta_side = "buy"  if long_exchange  == "Delta" else "sell"

            s1, r1 = place_pi42_order(pi42_side, BTC_QTY)
            s2, r2 = place_delta_order(delta_side, BTC_QTY)

            print(f"  Pi42  HTTP {s1}: {json.dumps(r1)[:200]}")
            print(f"  Delta HTTP {s2}: {json.dumps(r2)[:200]}")
    else:
        print("  No trade — gap too small after fees.")

    time.sleep(30)

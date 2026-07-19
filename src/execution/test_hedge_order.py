import requests
import time
import hmac
import hashlib
import json
from dotenv import load_dotenv
import os

load_dotenv("/home/container/bot/.env")

DELTA_KEY    = os.getenv("DELTA_API_KEY")
DELTA_SECRET = os.getenv("DELTA_API_SECRET")
PI42_KEY     = os.getenv("PI42_API_KEY")
PI42_SECRET  = os.getenv("PI42_API_SECRET")

# ======================================================
#  MASTER SWITCH
#  PAPER_MODE = True  -> logs decisions only, zero real orders
#  PAPER_MODE = False -> places real orders with real money
#  DO NOT change to False until paper mode is confirmed correct
# ======================================================
PAPER_MODE = True

# Minimum trade size (confirmed from both exchange docs)
BTC_QTY = 0.001  # 0.001 BTC per side - smallest allowed on both exchanges

# Fee constants (confirmed from official fee pages, taker + 18% GST)
PI42_FEE   = 0.080 * 1.18 / 100   # 0.09440%
DELTA_FEE  = 0.050 * 1.18 / 100   # 0.05900%
ROUND_TRIP = 2 * (PI42_FEE + DELTA_FEE)  # 4 trades: open+close on both exchanges


# -- Helpers --

def delta_get(path):
    ts  = str(int(time.time()))
    sig = hmac.new(DELTA_SECRET.encode(), ("GET" + ts + path).encode(), hashlib.sha256).hexdigest()
    r   = requests.get(
        f"https://api.india.delta.exchange{path}",
        headers={"api-key": DELTA_KEY, "timestamp": ts, "signature": sig,
                 "Content-Type": "application/json"},
        timeout=10
    )
    return r.json()

def pi42_get(path, extra_qs=""):
    ts  = str(int(time.time() * 1000))
    qs  = f"timestamp={ts}{extra_qs}"
    sig = hmac.new(PI42_SECRET.encode(), qs.encode(), hashlib.sha256).hexdigest()
    r   = requests.get(
        f"https://fapi.pi42.com{path}?{qs}",
        headers={"api-key": PI42_KEY, "signature": sig},
        timeout=10
    )
    return r.json()

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


# -- Fetch live rates --

def get_rates():
    rates = {}
    try:
        r = requests.get(
            "https://api.india.delta.exchange/v2/tickers/BTCUSD", timeout=10
        ).json()
        t = r.get("result", {})
        rates["delta_price"]   = float(t.get("mark_price", 0))
        rates["delta_funding"] = float(t.get("funding_rate", 0))
    except Exception as e:
        print(f"  [!] Delta rate fetch failed: {e}")
    try:
        r = requests.get(
            "https://api.pi42.com/v1/market/ticker24Hr/BTCINR", timeout=10
        ).json()
        rates["pi42_price"]   = float(r.get("lastPrice", 0))
        rates["pi42_funding"] = float(r.get("fundingRate", 0))
    except Exception as e:
        print(f"  [!] Pi42 rate fetch failed: {e}")
    return rates


# -- Paper order logger --

def log_paper_order(exchange, side, symbol, qty, price, reason):
    print(f"  [PAPER] WOULD PLACE -> {exchange} | {side} {qty} {symbol} "
          f"@ ~{price:.2f} | {reason}")


# -- Real order placement --

def place_pi42_order(side, qty):
    params = {
        "placeType":    "ORDER_FORM",
        "quantity":     qty,
        "side":         side,
        "symbol":       "BTCINR",
        "type":         "MARKET",
        "reduceOnly":   False,
        "marginAsset":  "INR",
        "deviceType":   "WEB",
        "userCategory": "EXTERNAL",
    }
    return pi42_post("/v1/order/place-order", params)

def place_delta_order(side, qty):
    params = {
        "product_id": 27,
        "order_type": "market_order",
        "side":       side,
        "size":       qty,
    }
    return delta_post("/v2/orders", params)


# -- Main loop --

print("=" * 54)
print(f"  FUNDING ARB EXECUTOR - {'PAPER MODE' if PAPER_MODE else 'LIVE MODE - REAL MONEY'}")
print("=" * 54)
print()

cycle = 0
while True:
    cycle += 1
    print(f"\n-- Cycle {cycle} - {time.strftime('%Y-%m-%d %H:%M:%S')} --")

    rates = get_rates()

    delta_rate  = rates.get("delta_funding")
    pi42_rate   = rates.get("pi42_funding")
    delta_price = rates.get("delta_price", 0)
    pi42_price  = rates.get("pi42_price", 0)

    if delta_rate is None or pi42_rate is None:
        print("  Waiting for rate data...")
        time.sleep(30)
        continue

    gap        = abs(delta_rate - pi42_rate)
    gap_pct    = gap * 100
    net_pct    = gap_pct - (ROUND_TRIP * 100)
    profitable = net_pct > 0

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
    print(f"  Pi42  funding : {pi42_rate*100:.6f}%  (price: Rs{pi42_price:,.2f})")
    print(f"  Gap           : {gap_pct:.6f} pp")
    print(f"  Round-trip fee: {ROUND_TRIP*100:.4f}%")
    print(f"  Net edge      : {net_pct:+.6f}%  {'[PROFITABLE]' if profitable else '[not profitable]'}")

    if profitable:
        print(f"  Direction     : LONG {long_exchange} / SHORT {short_exchange}")
        print(f"  Qty per side  : {BTC_QTY} BTC")

        if PAPER_MODE:
            log_paper_order(long_exchange,  "BUY",  "BTC", BTC_QTY, long_price,  "funding arb - long leg")
            log_paper_order(short_exchange, "SELL", "BTC", BTC_QTY, short_price, "funding arb - short leg")
            print("  [PAPER] No real orders sent.")
        else:
            print("  Placing REAL orders...")
            pi42_side  = "BUY"  if long_exchange == "Pi42"  else "SELL"
            delta_side = "buy"  if long_exchange == "Delta" else "sell"
            s1, r1 = place_pi42_order(pi42_side, BTC_QTY)
            s2, r2 = place_delta_order(delta_side, BTC_QTY)
            print(f"  Pi42  HTTP {s1}: {json.dumps(r1)[:200]}")
            print(f"  Delta HTTP {s2}: {json.dumps(r2)[:200]}")
    else:
        print("  No trade - gap too small after fees.")

    time.sleep(30)

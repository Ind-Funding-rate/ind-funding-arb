"""
Test Hedge Order
----------------
Places TWO limit orders far from market price to verify execution
works on both exchanges WITHOUT risking real money.

Delta : SELL limit at 20% ABOVE market  (won't fill - price must rise 20%)
Pi42  : BUY  limit at 20% BELOW market  (won't fill - price must fall 20%)

After running, verify both orders appear in each exchange dashboard.
Then run cancel_hedge_order.py to remove them.
"""

import requests
import hmac
import hashlib
import json
import time
import os
from dotenv import load_dotenv

load_dotenv("/home/container/bot/.env")  # HidenCloud server path

DELTA_KEY    = os.getenv("DELTA_API_KEY")
DELTA_SECRET = os.getenv("DELTA_API_SECRET")
PI42_KEY     = os.getenv("PI42_API_KEY")
PI42_SECRET  = os.getenv("PI42_API_SECRET")

DELTA_BASE       = "https://api.india.delta.exchange"
PI42_BASE        = "https://fapi.pi42.com"
DELTA_PRODUCT_ID = 27       # BTCUSD perpetual on Delta India
DELTA_MIN_SIZE   = 1        # 1 contract = 0.001 BTC
PI42_SYMBOL      = "BTCINR"
PI42_MIN_QTY     = "0.001"  # minimum BTC quantity on Pi42

# ─────────────────────────────────────────────────────
# DELTA HELPERS
# ─────────────────────────────────────────────────────

def delta_headers(method, path, body_str=""):
    ts  = str(int(time.time()))
    msg = method + ts + path + body_str
    sig = hmac.new(DELTA_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {
        "api-key":      DELTA_KEY,
        "timestamp":    ts,
        "signature":    sig,
        "Content-Type": "application/json"
    }

def get_delta_price():
    """Get current BTC mark price from Delta via REST."""
    r      = requests.get(f"{DELTA_BASE}/v2/tickers/BTCUSD", timeout=10)
    result = r.json().get("result", {})
    return float(result.get("mark_price", 0))

def place_delta_order(price):
    """Place a SELL limit order on Delta at given price."""
    path     = "/v2/orders"
    # Round price to nearest 0.5 — Delta tick size for BTCUSD
    rounded  = round(price * 2) / 2
    body     = {
        "product_id": DELTA_PRODUCT_ID,
        "side":       "sell",
        "order_type": "limit_order",
        "size":       DELTA_MIN_SIZE,
        "limit_price": str(rounded)
    }
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.post(
        DELTA_BASE + path,
        headers=delta_headers("POST", path, body_str),
        data=body_str,
        timeout=10
    )
    return r.json()

def cancel_delta_order(order_id):
    """Cancel a Delta order by ID."""
    path     = "/v2/orders"
    body     = {"id": order_id, "product_id": DELTA_PRODUCT_ID}
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.delete(
        DELTA_BASE + path,
        headers=delta_headers("DELETE", path, body_str),
        data=body_str,
        timeout=10
    )
    return r.json()

# ─────────────────────────────────────────────────────
# PI42 HELPERS
# ─────────────────────────────────────────────────────

def pi42_sign(body_str):
    """Sign Pi42 POST request body with HMAC-SHA256."""
    return hmac.new(
        PI42_SECRET.encode(),
        body_str.encode(),
        hashlib.sha256
    ).hexdigest()

def get_pi42_price():
    """Get current BTC mark price from Pi42 via REST."""
    r    = requests.get("https://api.pi42.com/v1/market/ticker24Hr/BTCINR", timeout=10)
    data = r.json()
    return float(data.get("lastPrice", 0))

def place_pi42_order(price):
    """Place a BUY limit order on Pi42 at given price."""
    ts   = str(int(time.time() * 1000))
    body = {
        "symbol":    PI42_SYMBOL,
        "side":      "BUY",
        "type":      "LIMIT",
        "quantity":  PI42_MIN_QTY,
        "price":     str(round(price)),
        "timestamp": ts
    }
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.post(
        f"{PI42_BASE}/v1/order",
        headers={
            "api-key":      PI42_KEY,
            "signature":    pi42_sign(body_str),
            "Content-Type": "application/json"
        },
        data=body_str,
        timeout=10
    )
    return r.json()

def cancel_pi42_order(order_id):
    """Cancel a Pi42 order by ID."""
    ts       = str(int(time.time() * 1000))
    body     = {"orderId": order_id, "timestamp": ts}
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.delete(
        f"{PI42_BASE}/v1/order",
        headers={
            "api-key":      PI42_KEY,
            "signature":    pi42_sign(body_str),
            "Content-Type": "application/json"
        },
        data=body_str,
        timeout=10
    )
    return r.json()

# ─────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────

def main():
    print("=" * 58)
    print("  TEST HEDGE ORDER")
    print("  Orders placed FAR from market price — will NOT fill")
    print("=" * 58)

    # Step 1: Get current prices from both exchanges
    print("\n[1] Fetching current prices...")
    delta_price = get_delta_price()
    pi42_price  = get_pi42_price()

    if delta_price == 0 or pi42_price == 0:
        print("❌ Could not fetch prices. Check internet connection.")
        return

    # Step 2: Calculate safe test prices (far from market)
    delta_order_price = delta_price * 1.20   # 20% ABOVE — sell limit won't fill
    pi42_order_price  = pi42_price  * 0.80   # 20% BELOW — buy  limit won't fill

    print(f"   Delta current price : ${delta_price:,.2f}")
    print(f"   Delta order price   : ${delta_order_price:,.2f}  (20% above — SELL limit)")
    print(f"   Pi42  current price : ₹{pi42_price:,.2f}")
    print(f"   Pi42  order price   : ₹{pi42_order_price:,.2f}  (20% below — BUY  limit)")

    # Step 3: Place Delta SELL limit order
    print("\n[2] Placing SELL limit order on Delta...")
    delta_result = place_delta_order(delta_order_price)
    if delta_result.get("success"):
        delta_order_id = delta_result["result"]["id"]
        print(f"   ✅ Delta order placed! Order ID: {delta_order_id}")
    else:
        print(f"   ❌ Delta order failed: {delta_result}")
        return

    # Step 4: Place Pi42 BUY limit order
    print("\n[3] Placing BUY limit order on Pi42...")
    pi42_result = place_pi42_order(pi42_order_price)
    pi42_order_id = pi42_result.get("orderId") or pi42_result.get("id")
    if pi42_order_id:
        print(f"   ✅ Pi42  order placed! Order ID: {pi42_order_id}")
    else:
        print(f"   ❌ Pi42 order failed: {pi42_result}")
        print(f"   Cancelling Delta order {delta_order_id} to keep things clean...")
        cancel_delta_order(delta_order_id)
        print(f"   ✅ Delta order cancelled.")
        return

    # Step 5: Summary
    print("\n" + "=" * 58)
    print("  ✅ BOTH TEST ORDERS PLACED SUCCESSFULLY")
    print("=" * 58)
    print(f"\n  Delta order ID : {delta_order_id}")
    print(f"  Pi42  order ID : {pi42_order_id}")
    print("\n  What to do next:")
    print("  1. Open delta.exchange  → Futures → Open Orders → verify")
    print("  2. Open pi42.com        → Orders  → Open Orders → verify")
    print("  3. Come back here once verified")
    print("\n  To cancel both orders run:")
    print(f"  python src/execution/cancel_hedge_order.py {delta_order_id} {pi42_order_id}")


if __name__ == "__main__":
    main()

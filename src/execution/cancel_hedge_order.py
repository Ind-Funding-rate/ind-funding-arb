"""
Cancel Hedge Order
------------------
Cancels the test orders placed by test_hedge_order.py

Usage:
  python src/execution/cancel_hedge_order.py <delta_order_id> <pi42_order_id>

Example:
  python src/execution/cancel_hedge_order.py 1390619817 abc123
"""

import sys
import requests
import hmac
import hashlib
import json
import time
import os
from dotenv import load_dotenv

load_dotenv()

DELTA_KEY        = os.getenv("DELTA_API_KEY")
DELTA_SECRET     = os.getenv("DELTA_API_SECRET")
PI42_KEY         = os.getenv("PI42_API_KEY")
PI42_SECRET      = os.getenv("PI42_API_SECRET")
DELTA_BASE       = "https://api.india.delta.exchange"
PI42_BASE        = "https://fapi.pi42.com"
DELTA_PRODUCT_ID = 27

def delta_headers(method, path, body_str=""):
    ts  = str(int(time.time()))
    msg = method + ts + path + body_str
    sig = hmac.new(DELTA_SECRET.encode(), msg.encode(), hashlib.sha256).hexdigest()
    return {"api-key": DELTA_KEY, "timestamp": ts, "signature": sig, "Content-Type": "application/json"}

def pi42_sign(body_str):
    return hmac.new(PI42_SECRET.encode(), body_str.encode(), hashlib.sha256).hexdigest()

def cancel_delta_order(order_id):
    path     = "/v2/orders"
    body     = {"id": int(order_id), "product_id": DELTA_PRODUCT_ID}
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.delete(DELTA_BASE + path, headers=delta_headers("DELETE", path, body_str), data=body_str, timeout=10)
    return r.json()

def cancel_pi42_order(order_id):
    ts       = str(int(time.time() * 1000))
    body     = {"orderId": order_id, "timestamp": ts}
    body_str = json.dumps(body, separators=(",", ":"))
    r = requests.delete(
        f"{PI42_BASE}/v1/order",
        headers={"api-key": PI42_KEY, "signature": pi42_sign(body_str), "Content-Type": "application/json"},
        data=body_str, timeout=10
    )
    return r.json()

def main():
    if len(sys.argv) != 3:
        print("Usage: python src/execution/cancel_hedge_order.py <delta_order_id> <pi42_order_id>")
        sys.exit(1)

    delta_order_id = sys.argv[1]
    pi42_order_id  = sys.argv[2]

    print("=" * 48)
    print("  CANCELLING TEST HEDGE ORDERS")
    print("=" * 48)

    print(f"\n[1] Cancelling Delta order {delta_order_id}...")
    result = cancel_delta_order(delta_order_id)
    if result.get("success"):
        print("   ✅ Delta order cancelled successfully")
    else:
        print(f"   ❌ Delta cancel failed: {result}")

    print(f"\n[2] Cancelling Pi42 order {pi42_order_id}...")
    result = cancel_pi42_order(pi42_order_id)
    if result.get("success") or result.get("orderId"):
        print("   ✅ Pi42 order cancelled successfully")
    else:
        print(f"   ❌ Pi42 cancel failed: {result}")

    print("\n  ✅ Done. Both positions cleared.")

if __name__ == "__main__":
    main()

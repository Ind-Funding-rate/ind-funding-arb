import requests
import time
import os
from dotenv import load_dotenv
from datetime import datetime

load_dotenv()

# ── DELTA EXCHANGE ──────────────────────────────────────────
def fetch_delta_funding_rate():
    url = "https://api.india.delta.exchange/v2/tickers/BTCUSD"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        result = data.get("result", {})
        return {
            "exchange":     "Delta",
            "symbol":       "BTCUSD",
            "mark_price":   float(result.get("mark_price", 0)),
            "funding_rate": float(result.get("funding_rate", 0)),
            "timestamp":    datetime.now().strftime("%H:%M:%S")
        }
    except Exception as e:
        print(f"Delta error: {e}")
        return None

# ── PI42 ─────────────────────────────────────────────────────
def fetch_pi42_price():
    url = "https://api.pi42.com/v1/market/ticker24Hr/BTCINR"
    try:
        response = requests.get(url, timeout=5)
        data = response.json()
        ticker = data.get("data", {})
        return {
            "exchange":     "Pi42",
            "symbol":       "BTCINR",
            "mark_price":   float(ticker.get("c", 0)),
            "funding_rate": None,
            "timestamp":    datetime.now().strftime("%H:%M:%S")
        }
    except Exception as e:
        print(f"Pi42 error: {e}")
        return None

# ── DISPLAY ──────────────────────────────────────────────────
def display_rates(delta, pi42):
    print("\n" + "="*50)
    print(f"  LIVE RATES  —  {datetime.now().strftime('%H:%M:%S')}")
    print("="*50)
    
    if delta:
        print(f"  DELTA  | BTC Mark Price : ₹{delta['mark_price']:,.2f}")
        print(f"         | Funding Rate   : {delta['funding_rate']*100:.6f}%")
    
    if pi42:
        print(f"  PI42   | BTC Price      : ₹{pi42['mark_price']:,.2f}")
        print(f"         | Funding Rate   : Not available yet")
    
    print("="*50)

# ── MAIN LOOP ────────────────────────────────────────────────
def main():
    print("Starting rate fetcher...")
    print("Press Ctrl+C to stop\n")
    
    while True:
        delta = fetch_delta_funding_rate()
        pi42  = fetch_pi42_price()
        display_rates(delta, pi42)
        time.sleep(5)

if __name__ == "__main__":
    main()
"""
Multi-pair funding gap scanner.

Single responsibility: for every coin available on BOTH Delta and Pi42,
fetch funding rate + 24h volume, compute the fee-adjusted gap, and rank
results so real opportunities are easy to spot vs thin/illiquid noise.

This does NOT place any orders and does NOT run continuously - it's a
one-shot scan, meant to be re-run periodically to see which pairs (if any)
show a persistent, liquid, real gap worth investigating further.
"""
import requests
import time
import asyncio
import json
import websockets

# Fee constants (confirmed from official fee pages, taker + 18% GST)
PI42_FEE   = 0.080 * 1.18 / 100
DELTA_FEE  = 0.050 * 1.18 / 100
ROUND_TRIP = 2 * (PI42_FEE + DELTA_FEE)  # as a fraction, e.g. 0.003068

PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"

# Coins to scan - the real overlap list confirmed live from both exchanges.
# Trimmed to reasonably liquid, recognizable names rather than the full 133
# (many of which are thin/obscure). Edit this list to scan others.
COINS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "UNI", "SUI", "TRX", "NEAR", "OP", "INJ", "RUNE",
    "SEI", "ARB", "APT", "TIA", "JUP", "WIF", "PEPE", "BNB",
]


def get_delta_funding_all():
    """One call gets ALL Delta tickers at once - much faster than one
    request per coin."""
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
                    "price": float(t.get("mark_price", 0)),
                    "volume_usd": float(vol) if vol else 0,
                }
    return out


async def _pi42_ws_batch(symbols):
    """Connect once, subscribe to all requested Pi42 channels, and collect
    whatever markPriceUpdate events arrive within the time window."""
    channels = [f"{s.lower()}inr@markPrice" for s in symbols]
    results = {}
    async with websockets.connect(PI42_WS_URL) as ws:
        await ws.recv()
        await ws.send("40")
        await ws.recv()
        sub_msg = f'42["subscribe", {{"params": {json.dumps(channels)}}}]'
        await ws.send(sub_msg)

        end_time = asyncio.get_event_loop().time() + 15
        while asyncio.get_event_loop().time() < end_time and len(results) < len(symbols):
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=15)
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
                        results[base] = {
                            "funding": float(data.get("r", 0)),
                            "price": float(data.get("p", 0)),
                        }
    return results


def get_pi42_funding_all(symbols):
    try:
        return asyncio.run(_pi42_ws_batch(symbols))
    except Exception as e:
        print(f"  [!] Pi42 batch fetch failed: {e}")
        return {}


def scan():
    print("=" * 70)
    print("  MULTI-PAIR FUNDING GAP SCANNER")
    print("=" * 70)
    print(f"  Round-trip fee assumption: {ROUND_TRIP*100:.4f}%  (taker + GST, both legs)")
    print(f"  Scanning {len(COINS)} coins...")
    print()

    delta_data = get_delta_funding_all()
    print(f"  Delta: got data for {len(delta_data)} coins total")

    pi42_data = get_pi42_funding_all(COINS)
    print(f"  Pi42:  got data for {len(pi42_data)} of {len(COINS)} requested")
    print()

    rows = []
    for coin in COINS:
        d = delta_data.get(coin)
        p = pi42_data.get(coin)
        if not d or not p:
            continue
        gap_pct = abs(d["funding"] - p["funding"]) * 100
        net_pct = gap_pct - (ROUND_TRIP * 100)
        rows.append({
            "coin": coin,
            "delta_funding_pct": d["funding"] * 100,
            "pi42_funding_pct": p["funding"] * 100,
            "gap_pct": gap_pct,
            "net_pct": net_pct,
            "delta_volume_usd": d["volume_usd"],
        })

    rows.sort(key=lambda r: r["net_pct"], reverse=True)

    print(f"{'COIN':<8} {'DELTA%':>10} {'PI42%':>10} {'GAP pp':>9} {'NET%':>9} {'DELTA VOL($)':>15}  STATUS")
    print("-" * 70)
    for r in rows:
        status = "PROFITABLE" if r["net_pct"] > 0 else ""
        print(f"{r['coin']:<8} {r['delta_funding_pct']:>10.5f} {r['pi42_funding_pct']:>10.5f} "
              f"{r['gap_pct']:>9.5f} {r['net_pct']:>+9.5f} {r['delta_volume_usd']:>15,.0f}  {status}")

    print()
    profitable = [r for r in rows if r["net_pct"] > 0]
    print(f"  {len(profitable)} of {len(rows)} scanned coins show a profitable gap after fees.")
    print("  NOTE: a profitable gap on a low-volume coin may not be reliably")
    print("  tradeable - check the volume column before trusting a result.")


if __name__ == "__main__":
    scan()

"""
Delta Exchange market data fetchers - price, order book, option chain.

Every endpoint here is confirmed directly from Delta's own official docs
(docs.delta.exchange), not guessed:
    - Price:        GET /v2/tickers                       (already used
                     elsewhere in this project for funding rate too)
    - Order book:   GET /v2/l2orderbook/{symbol}           (public, no auth)
    - Option chain: GET /v2/tickers?contract_types=call_options,put_options
                     &underlying_asset_symbols={coin}&expiry_date=DD-MM-YYYY
                     (public, no auth)

Pi42 and CoinSwitch order-book/option-chain support is NOT yet built -
Pi42's order book endpoint exists but its exact response format hasn't
been verified live yet, and CoinSwitch's options API requires separately
requesting access from their team. Both will be added once confirmed,
same standard as everything else in this project.
"""
import requests
from datetime import datetime, timedelta

BASE_URL = "https://api.india.delta.exchange"


def get_price_snapshot(symbols: list) -> list:
    """Returns [{"symbol", "mark_price", "spot_price"}] for the given
    Delta symbols (e.g. ["BTCUSD", "ETHUSD"])."""
    try:
        r = requests.get(f"{BASE_URL}/v2/tickers", timeout=20).json()
    except Exception as e:
        print(f"  [!] Delta price fetch failed: {e}")
        return []

    wanted = set(symbols)
    rows = []
    for t in r.get("result", []):
        if t.get("symbol") in wanted:
            rows.append({
                "symbol": t["symbol"],
                "mark_price": float(t.get("mark_price", 0) or 0),
                "spot_price": float(t.get("spot_price", 0) or 0),
                "captured_at": datetime.utcnow().isoformat(),
            })
    return rows


def get_orderbook_snapshot(symbol: str, depth: int = 10) -> list:
    """Returns a flat list of order-book rows (one row per price level,
    both sides) for one symbol - easy to store as a Parquet snapshot.
    Limited to `depth` levels per side, NOT the full book - this is the
    deliberate size/storage tradeoff agreed on earlier."""
    try:
        r = requests.get(f"{BASE_URL}/v2/l2orderbook/{symbol}", timeout=15).json()
    except Exception as e:
        print(f"  [!] Delta orderbook fetch failed for {symbol}: {e}")
        return []

    result = r.get("result", {})
    captured_at = datetime.utcnow().isoformat()
    rows = []
    for side, levels in [("buy", result.get("buy", [])), ("sell", result.get("sell", []))]:
        for level in levels[:depth]:
            rows.append({
                "symbol": symbol,
                "side": side,
                "price": float(level.get("price", 0)),
                "size": float(level.get("size", 0)),
                "depth_rank": int(level.get("depth", 0)),
                "captured_at": captured_at,
            })
    return rows


def get_available_option_expiries(underlying: str) -> list:
    """Returns the list of expiry dates (as DD-MM-YYYY strings) Delta
    currently has options listed for on this underlying coin."""
    try:
        r = requests.get(
            f"{BASE_URL}/v2/products",
            params={"contract_types": "call_options,put_options",
                    "underlying_asset_symbols": underlying},
            timeout=20,
        ).json()
    except Exception as e:
        print(f"  [!] Delta option product list fetch failed: {e}")
        return []

    expiries = set()
    for p in r.get("result", []):
        settlement_time = p.get("settlement_time")
        if settlement_time:
            try:
                dt = datetime.fromisoformat(settlement_time.replace("Z", "+00:00"))
                expiries.add(dt.strftime("%d-%m-%Y"))
            except ValueError:
                continue
    return sorted(expiries)


def get_option_chain_snapshot(underlying: str = "BTC", max_expiries: int = 2) -> list:
    """Returns a flat list of option rows for the NEAREST `max_expiries`
    expiries on this underlying - limiting expiries (not just strikes) is
    another deliberate storage control, since option chains can otherwise
    be very large (many strikes x many expiries x 2 contract types)."""
    expiries = get_available_option_expiries(underlying)[:max_expiries]
    if not expiries:
        return []

    captured_at = datetime.utcnow().isoformat()
    rows = []
    for expiry in expiries:
        try:
            r = requests.get(
                f"{BASE_URL}/v2/tickers",
                params={
                    "contract_types": "call_options,put_options",
                    "underlying_asset_symbols": underlying,
                    "expiry_date": expiry,
                },
                timeout=20,
            ).json()
        except Exception as e:
            print(f"  [!] Delta option chain fetch failed for {underlying} {expiry}: {e}")
            continue

        for t in r.get("result", []):
            quotes = t.get("quotes", {}) or {}
            greeks = t.get("greeks", {}) or {}
            rows.append({
                "underlying": underlying,
                "expiry": expiry,
                "symbol": t.get("symbol", ""),
                "contract_type": t.get("contract_type", ""),
                "strike_price": float(t.get("strike_price", 0) or 0),
                "mark_price": float(t.get("mark_price", 0) or 0),
                "best_bid": float(quotes.get("best_bid", 0) or 0),
                "best_ask": float(quotes.get("best_ask", 0) or 0),
                "open_interest": float(t.get("oi", 0) or 0),
                "delta": float(greeks.get("delta", 0) or 0),
                "gamma": float(greeks.get("gamma", 0) or 0),
                "theta": float(greeks.get("theta", 0) or 0),
                "vega": float(greeks.get("vega", 0) or 0),
                "iv": float(greeks.get("iv", 0) or 0),
                "captured_at": captured_at,
            })
    return rows


if __name__ == "__main__":
    print("Testing Delta market data fetchers (BTCUSD)...\n")

    print("-- Price --")
    prices = get_price_snapshot(["BTCUSD"])
    print(prices)

    print("\n-- Order book (top 5) --")
    book = get_orderbook_snapshot("BTCUSD", depth=5)
    for row in book:
        print(row)

    print("\n-- Option chain (nearest expiry) --")
    chain = get_option_chain_snapshot("BTC", max_expiries=1)
    print(f"{len(chain)} option contracts found")
    if chain:
        print("Example row:", chain[0])

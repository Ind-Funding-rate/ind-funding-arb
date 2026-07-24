"""
Historical funding-rate fetchers for major exchanges that actually publish
real historical data via free, public, no-API-key REST endpoints.

Confirmed available (checked against each exchange's own docs):
    - Binance   GET /fapi/v1/fundingRate
    - Bybit     GET /v5/market/funding/history
    - OKX       GET /api/v5/public/funding-rate-history

NOT available anywhere:
    - Pi42  - no historical funding endpoint exists at all (only live
      WebSocket data). This is why the Delta-vs-Pi42 backtest
      (backtest_engine.py) can only use our own logged data going
      forward, not real history.
    - Delta Exchange India - no confirmed public historical funding
      endpoint either; not included here for the same reason.

All fetchers return a list of dicts: {"time": datetime, "rate": float}
sorted oldest-first, rate as a fraction (e.g. 0.0001 = 0.01%).
"""
import requests
from datetime import datetime, timedelta

# Each per-call limit comfortably covers a 30-60 day range at 3
# fundings/day (8h interval), so a single request suffices for the
# ranges this tool is meant to be used for.

def get_binance_history(coin: str, days: int):
    symbol = f"{coin}USDT"
    start_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    r = requests.get(
        "https://fapi.binance.com/fapi/v1/fundingRate",
        params={"symbol": symbol, "startTime": start_ms, "limit": 1000},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json()
    return [
        {"time": datetime.fromtimestamp(d["fundingTime"] / 1000), "rate": float(d["fundingRate"])}
        for d in data
    ]


def get_bybit_history(coin: str, days: int):
    symbol = f"{coin}USDT"
    start_ms = int((datetime.now() - timedelta(days=days)).timestamp() * 1000)
    r = requests.get(
        "https://api.bybit.com/v5/market/funding/history",
        params={"category": "linear", "symbol": symbol, "startTime": start_ms, "limit": 200},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("result", {}).get("list", [])
    return [
        {"time": datetime.fromtimestamp(int(d["fundingRateTimestamp"]) / 1000), "rate": float(d["fundingRate"])}
        for d in data
    ]


def get_okx_history(coin: str, days: int):
    inst_id = f"{coin}-USDT-SWAP"
    r = requests.get(
        "https://www.okx.com/api/v5/public/funding-rate-history",
        params={"instId": inst_id, "limit": 100},
        timeout=15,
    )
    r.raise_for_status()
    data = r.json().get("data", [])
    cutoff = datetime.now() - timedelta(days=days)
    rows = [
        {"time": datetime.fromtimestamp(int(d["fundingTime"]) / 1000), "rate": float(d["fundingRate"])}
        for d in data
    ]
    return [row for row in rows if row["time"] >= cutoff]


FETCHERS = {
    "binance": get_binance_history,
    "bybit": get_bybit_history,
    "okx": get_okx_history,
}


def get_history(exchange: str, coin: str, days: int):
    """exchange: one of 'binance', 'bybit', 'okx'. Returns [] on any error
    (bad symbol, network issue, etc.) rather than raising, so callers can
    handle 'no data' uniformly."""
    fetcher = FETCHERS.get(exchange.lower())
    if not fetcher:
        return []
    try:
        rows = fetcher(coin.upper(), days)
        rows.sort(key=lambda r: r["time"])
        return rows
    except Exception as e:
        print(f"  [!] {exchange} history fetch failed for {coin}: {e}")
        return []

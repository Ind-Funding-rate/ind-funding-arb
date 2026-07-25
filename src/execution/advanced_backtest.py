"""
Advanced multi-exchange historical backtest with real quant risk metrics,
plus an automated best-pair finder and a full opportunity matrix scanner
across ALL available coins and exchange combinations.

SPEED ARCHITECTURE (fixed - see src/data/ingest_funding.py and
src/data/funding_history_store.py): this now reads historical data from
our OWN local store instead of calling exchange APIs live during a scan.
A separate background ingestion pipeline keeps that store up to date on
its own schedule, in parallel. This is the same approach real multi-
exchange dashboards (loris.tools, CoinGlass, etc.) use - their site never
waits on live exchange calls per page load either. It's also what makes
scaling to many more exchanges later realistic: the scan/ranking code
below doesn't change at all when a new exchange is added, it just reads
more rows from the same local table.

Honesty note: still the same real, free data underneath (Bybit/OKX
public APIs) - the "advanced" part is the analysis (equity curve,
drawdown, Sharpe-like ratio, cross-pair/cross-coin scanning) and now the
ingestion/read split, not a secret data source.

KNOWN ISSUES FIXED PREVIOUSLY:
1. Tokenized-stock perpetuals (IBM, MRVL, AMAT, etc.) filtered out via
   CoinGecko's real crypto symbol list.
2. Equity curve/max-drawdown fixed to spread fees per-event instead of
   once up front, so the curve can genuinely fluctuate.
"""
import statistics
import requests
from itertools import combinations
from src.data.funding_history_store import get_cached_history

GENERIC_ROUND_TRIP_PCT = 4 * 0.05  # 0.20% - see note in multi_exchange_backtest.py
ALL_EXCHANGES = ["bybit", "okx", "binance"]  # binance often times out from this server

_real_crypto_symbols_cache = None


def get_real_crypto_symbols():
    """Fetches CoinGecko's free public list of actual cryptocurrencies
    (no API key needed) and returns their ticker symbols in uppercase, so
    we can filter out tokenized-stock perpetuals that share the same
    naming convention on some exchanges. Cached after first call."""
    global _real_crypto_symbols_cache
    if _real_crypto_symbols_cache is not None:
        return _real_crypto_symbols_cache
    try:
        r = requests.get("https://api.coingecko.com/api/v3/coins/list", timeout=30)
        r.raise_for_status()
        data = r.json()
        _real_crypto_symbols_cache = {c["symbol"].upper() for c in data}
    except Exception as e:
        print(f"  [!] Failed to fetch CoinGecko coin list: {e}")
        _real_crypto_symbols_cache = set()
    return _real_crypto_symbols_cache


def get_full_common_coin_universe():
    """Dynamically fetches EVERY perpetual symbol actually listed on both
    Bybit and OKX right now (public, no API key), intersects them, and
    filters out anything that isn't a real cryptocurrency according to
    CoinGecko's public list."""
    bybit_coins = set()
    try:
        r = requests.get(
            "https://api.bybit.com/v5/market/instruments-info",
            params={"category": "linear"}, timeout=20,
        ).json()
        for item in r.get("result", {}).get("list", []):
            symbol = item.get("symbol", "")
            if symbol.endswith("USDT"):
                bybit_coins.add(symbol[:-4])
    except Exception as e:
        print(f"  [!] Failed to fetch Bybit instrument list: {e}")

    okx_coins = set()
    try:
        r = requests.get(
            "https://www.okx.com/api/v5/public/instruments",
            params={"instType": "SWAP"}, timeout=20,
        ).json()
        for item in r.get("data", []):
            inst_id = item.get("instId", "")
            if inst_id.endswith("-USDT-SWAP"):
                okx_coins.add(inst_id.replace("-USDT-SWAP", ""))
    except Exception as e:
        print(f"  [!] Failed to fetch OKX instrument list: {e}")

    common = bybit_coins & okx_coins
    real_crypto = get_real_crypto_symbols()
    if real_crypto:
        filtered = sorted(c for c in common if c in real_crypto)
        removed = sorted(common - set(filtered))
        if removed:
            print(f"  [info] Filtered out {len(removed)} non-crypto symbols "
                  f"(likely tokenized stocks/ETFs): {removed[:20]}"
                  f"{'...' if len(removed) > 20 else ''}")
        return filtered
    print("  [warn] Could not verify against CoinGecko - coin list is NOT filtered for non-crypto symbols")
    return sorted(common)


def compute_advanced_backtest(exchange_a: str, exchange_b: str, coin: str,
                                days: int, position_usd: float,
                                fee_override_pct: float = None) -> dict:
    """Reads from the LOCAL store (get_cached_history) rather than
    calling exchanges live - this is what makes scanning hundreds of
    coins fast. If the store hasn't been populated yet for a coin, this
    will just show no data rather than falling back to a live call
    (keeps ranking runs fast and predictable; ingestion runs separately)."""
    round_trip_pct = fee_override_pct if fee_override_pct is not None else GENERIC_ROUND_TRIP_PCT

    hist_a = get_cached_history(exchange_a, coin, days)
    hist_b = get_cached_history(exchange_b, coin, days)

    base_result = {
        "exchange_a": exchange_a, "exchange_b": exchange_b, "coin": coin,
        "days": days, "position_usd": position_usd,
    }

    if not hist_a or not hist_b:
        return {**base_result, "enough_data": False,
                "points_a": len(hist_a), "points_b": len(hist_b)}

    TOLERANCE_MINUTES = 30
    matched = []
    for point_a in hist_a:
        best, best_diff = None, None
        for point_b in hist_b:
            diff = abs((point_a["time"] - point_b["time"]).total_seconds())
            if best_diff is None or diff < best_diff:
                best_diff, best = diff, point_b
        if best is not None and best_diff <= TOLERANCE_MINUTES * 60:
            matched.append((point_a["time"], point_a["rate"], best["rate"]))

    if len(matched) < 2:
        return {**base_result, "enough_data": False,
                "points_a": len(hist_a), "points_b": len(hist_b),
                "matched_points": len(matched)}

    event_gaps_pct = [abs(ra - rb) * 100 for _, ra, rb in matched]

    per_event_fee_pct = round_trip_pct / len(event_gaps_pct)
    net_events_pct = [g - per_event_fee_pct for g in event_gaps_pct]

    equity_curve = []
    running_total = 0.0
    for net in net_events_pct:
        running_total += net
        equity_curve.append(running_total)

    total_return_pct = equity_curve[-1]
    profitable_events = sum(1 for n in net_events_pct if n > 0)

    peak = equity_curve[0]
    max_drawdown = 0.0
    for v in equity_curve:
        peak = max(peak, v)
        max_drawdown = min(max_drawdown, v - peak)

    if len(net_events_pct) > 1 and statistics.pstdev(net_events_pct) > 0:
        sharpe_like = statistics.mean(net_events_pct) / statistics.pstdev(net_events_pct)
    else:
        sharpe_like = 0.0

    period_days = (matched[-1][0] - matched[0][0]).total_seconds() / 86400 or days
    apy_pct = (total_return_pct / period_days * 365) if period_days > 0 else 0
    position_pnl_usd = position_usd * (total_return_pct / 100)

    sorted_gaps = sorted(event_gaps_pct)
    n = len(sorted_gaps)

    return {
        **base_result,
        "enough_data": True,
        "matched_points": len(matched),
        "period_days": round(period_days, 2),
        "round_trip_pct": round(round_trip_pct, 4),
        "total_return_pct": round(total_return_pct, 4),
        "position_pnl_usd": round(position_pnl_usd, 2),
        "apy_pct": round(apy_pct, 2),
        "profitable_events": profitable_events,
        "pct_events_profitable": round(profitable_events / len(matched) * 100, 1),
        "max_drawdown_pct": round(max_drawdown, 4),
        "sharpe_like": round(sharpe_like, 3),
        "gap_min_pct": round(sorted_gaps[0], 5),
        "gap_median_pct": round(sorted_gaps[n // 2], 5),
        "gap_max_pct": round(sorted_gaps[-1], 5),
        "equity_curve": [round(v, 4) for v in equity_curve],
    }


def find_best_pair(coin: str, days: int, position_usd: float,
                    exchanges=None) -> list:
    exchanges = exchanges or ALL_EXCHANGES
    results = []
    for a, b in combinations(exchanges, 2):
        r = compute_advanced_backtest(a, b, coin, days, position_usd)
        if r.get("enough_data"):
            results.append(r)
    results.sort(key=lambda r: r["apy_pct"], reverse=True)
    return results


def scan_opportunity_matrix(coins: list, days: int, position_usd: float,
                             exchanges=None, progress_callback=None) -> list:
    """Now reads entirely from the local store (fast, no network) - this
    is the ranking/analysis step, fully decoupled from ingestion. Scanning
    a few hundred coins now takes seconds instead of minutes, and adding
    more exchanges later doesn't slow this down."""
    exchanges = exchanges or ["bybit", "okx"]
    results = []
    total = len(coins)
    for i, coin in enumerate(coins):
        for a, b in combinations(exchanges, 2):
            r = compute_advanced_backtest(a, b, coin, days, position_usd)
            if r.get("enough_data") and r["matched_points"] >= 3:
                results.append(r)
        if progress_callback:
            progress_callback(i + 1, total)
    results.sort(key=lambda r: r["apy_pct"], reverse=True)
    return results

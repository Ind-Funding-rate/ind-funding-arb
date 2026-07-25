"""
Advanced multi-exchange historical backtest with real quant risk metrics,
plus an automated best-pair finder and a full opportunity matrix scanner
across many coins and exchange combinations at once.

Honesty note: this is built on the same real, free historical data as
multi_exchange_backtest.py (Binance/Bybit/OKX public APIs) - it doesn't
have access to any data source that isn't otherwise free and public. The
"advanced" part is the analysis (equity curve, drawdown, Sharpe-like
ratio, cross-pair/cross-coin scanning), not a secret data source.
"""
import statistics
from itertools import combinations
from src.data.historical_funding import get_history

GENERIC_ROUND_TRIP_PCT = 4 * 0.05  # 0.20% - see note in multi_exchange_backtest.py
ALL_EXCHANGES = ["bybit", "okx", "binance"]  # binance often times out from this server


def compute_advanced_backtest(exchange_a: str, exchange_b: str, coin: str,
                                days: int, position_usd: float,
                                fee_override_pct: float = None) -> dict:
    """Same core simulation as compute_multi_backtest(), extended with:
    - an equity curve (cumulative return over time, event by event)
    - max drawdown of that curve (worst peak-to-trough dip)
    - a Sharpe-like ratio: mean per-event return / std dev of per-event
      return, as a rough measure of how consistent vs choppy the edge was
    - gap distribution stats (min/median/max), so a single lucky spike
      doesn't get mistaken for a reliable edge
    """
    round_trip_pct = fee_override_pct if fee_override_pct is not None else GENERIC_ROUND_TRIP_PCT

    hist_a = get_history(exchange_a, coin, days)
    hist_b = get_history(exchange_b, coin, days)

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

    # Per-event gap (before the one-time entry fee) and running equity curve
    event_gaps_pct = [abs(ra - rb) * 100 for _, ra, rb in matched]
    equity_curve = []
    running_total = -round_trip_pct  # pay the round-trip fee once, up front
    for gap in event_gaps_pct:
        running_total += gap
        equity_curve.append(running_total)

    total_return_pct = equity_curve[-1]
    profitable_events = sum(1 for g in event_gaps_pct if g > round_trip_pct / len(event_gaps_pct))

    # Max drawdown of the equity curve: worst drop from any prior peak
    peak = equity_curve[0]
    max_drawdown = 0.0
    for v in equity_curve:
        peak = max(peak, v)
        max_drawdown = min(max_drawdown, v - peak)

    # Sharpe-like ratio on the per-event gap series (not annualized -
    # a simple consistency measure: bigger is steadier, near-zero or
    # negative means the edge is noisy/unreliable even if the average
    # looks fine).
    if len(event_gaps_pct) > 1 and statistics.pstdev(event_gaps_pct) > 0:
        sharpe_like = statistics.mean(event_gaps_pct) / statistics.pstdev(event_gaps_pct)
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
        "max_drawdown_pct": round(max_drawdown, 4),
        "sharpe_like": round(sharpe_like, 3),
        "gap_min_pct": round(sorted_gaps[0], 5),
        "gap_median_pct": round(sorted_gaps[n // 2], 5),
        "gap_max_pct": round(sorted_gaps[-1], 5),
        "equity_curve": [round(v, 4) for v in equity_curve],
    }


def find_best_pair(coin: str, days: int, position_usd: float,
                    exchanges=None) -> list:
    """Tests every exchange combination for one coin and returns results
    sorted by APY (best first). Skips combinations with too little data
    rather than erroring."""
    exchanges = exchanges or ALL_EXCHANGES
    results = []
    for a, b in combinations(exchanges, 2):
        r = compute_advanced_backtest(a, b, coin, days, position_usd)
        if r.get("enough_data"):
            results.append(r)
    results.sort(key=lambda r: r["apy_pct"], reverse=True)
    return results


def scan_opportunity_matrix(coins: list, days: int, position_usd: float,
                             exchanges=None) -> list:
    """The flagship feature: scans every coin across every exchange pair
    combination using real historical data, and returns everything sorted
    by APY. This is a real, if slow, operation - each (coin, pair) needs
    2 API calls, so len(coins) * combinations * 2 requests total. Meant
    to be triggered explicitly (a button), not run on every page load.
    """
    exchanges = exchanges or ["bybit", "okx"]  # binance excluded by default (unreliable)
    results = []
    for coin in coins:
        for a, b in combinations(exchanges, 2):
            r = compute_advanced_backtest(a, b, coin, days, position_usd)
            if r.get("enough_data") and r["matched_points"] >= 3:
                results.append(r)
    results.sort(key=lambda r: r["apy_pct"], reverse=True)
    return results

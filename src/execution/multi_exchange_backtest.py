"""
Multi-exchange historical funding backtest - unlike backtest_engine.py
(which is limited to our own logged Delta-vs-Pi42 data going forward),
this uses REAL historical data from exchanges that actually publish it:
Binance, Bybit, OKX. Covers weeks/months of real history, not just what
we've logged ourselves.

FEE ASSUMPTION: exchange fee structures vary. This uses a generic
round-trip assumption (0.05% taker per leg x 4 legs = 0.20%) rather than
the Pi42/Delta-specific numbers used elsewhere. Treat this as a rough
default, not exact - real fees depend on your actual account tier on
each exchange.
"""
from datetime import datetime
from src.data.historical_funding import get_history

GENERIC_ROUND_TRIP_PCT = 4 * 0.05  # 0.20% - see note above
FUNDING_INTERVAL_HOURS = 8


def compute_multi_backtest(exchange_a: str, exchange_b: str, coin: str,
                            days: int, position_usd: float,
                            fee_override_pct: float = None) -> dict:
    round_trip_pct = fee_override_pct if fee_override_pct is not None else GENERIC_ROUND_TRIP_PCT

    hist_a = get_history(exchange_a, coin, days)
    hist_b = get_history(exchange_b, coin, days)

    if not hist_a or not hist_b:
        return {
            "enough_data": False,
            "exchange_a": exchange_a,
            "exchange_b": exchange_b,
            "coin": coin,
            "days": days,
            "position_usd": position_usd,
            "points_a": len(hist_a),
            "points_b": len(hist_b),
        }

    # Match funding events between exchanges by nearest timestamp within
    # a small tolerance window (funding schedules are usually aligned to
    # the same UTC hours across major exchanges, but not always exact).
    TOLERANCE_MINUTES = 30
    b_by_time = hist_b

    matched = []
    for point_a in hist_a:
        best = None
        best_diff = None
        for point_b in b_by_time:
            diff = abs((point_a["time"] - point_b["time"]).total_seconds())
            if best_diff is None or diff < best_diff:
                best_diff = diff
                best = point_b
        if best is not None and best_diff <= TOLERANCE_MINUTES * 60:
            matched.append((point_a["time"], point_a["rate"], best["rate"]))

    if len(matched) < 2:
        return {
            "enough_data": False,
            "exchange_a": exchange_a,
            "exchange_b": exchange_b,
            "coin": coin,
            "days": days,
            "position_usd": position_usd,
            "points_a": len(hist_a),
            "points_b": len(hist_b),
            "matched_points": len(matched),
        }

    total_gap_pct = 0.0
    profitable_events = 0
    for _, rate_a, rate_b in matched:
        gap_pct = abs(rate_a - rate_b) * 100
        total_gap_pct += gap_pct
        if gap_pct > round_trip_pct:
            profitable_events += 1

    # Simple assumption: pay the round-trip fee once at the start, hold
    # for the whole period, collect every matched funding differential.
    total_return_pct = total_gap_pct - round_trip_pct
    period_days = (matched[-1][0] - matched[0][0]).total_seconds() / 86400 or (days)
    apy_pct = (total_return_pct / period_days * 365) if period_days > 0 else 0
    position_pnl_usd = position_usd * (total_return_pct / 100)

    return {
        "enough_data": True,
        "exchange_a": exchange_a,
        "exchange_b": exchange_b,
        "coin": coin,
        "days": days,
        "position_usd": position_usd,
        "matched_points": len(matched),
        "period_days": round(period_days, 2),
        "profitable_events": profitable_events,
        "pct_events_profitable": round(profitable_events / len(matched) * 100, 1),
        "total_gap_pct": round(total_gap_pct, 4),
        "round_trip_pct": round(round_trip_pct, 4),
        "total_return_pct": round(total_return_pct, 4),
        "position_pnl_usd": round(position_pnl_usd, 2),
        "apy_pct": round(apy_pct, 2),
    }

"""
Delta Exchange - LIVE (sub-second) price + funding rate store.

Runs delta_client.listen() forever in its own background thread with its
own asyncio event loop, completely separate from the existing 90-second
full_market_scanner.py / three_way_scanner.py REST-based scanners - those
keep running exactly as before, untouched, as a safety net. If this feed
ever fails, nothing else in the app is affected.

This module's only job: keep an in-memory dict of the LATEST price and
funding rate Delta has pushed for each coin, updated the instant a new
value arrives (typically well under 1 second for mark_price - funding
rate itself only updates whenever Delta recalculates it, which is a
periodic exchange-side event on every exchange, not something any code
can speed up).

Read the latest values with get_delta_live_data(). Start the background
feed once, at app startup, with start_delta_live_feed().
"""
import threading
import asyncio
import time

from src.feeds.delta_client import listen as delta_listen
from src.execution.full_market_scanner import COINS

_lock = threading.Lock()
_live_data = {}   # { "BTC": {"price": 67321.0, "funding": 0.0001, ...} }
_started = False


def _build_pairs():
    """Delta convention (confirmed from Delta's own API docs): the
    funding_rate channel uses the plain symbol (e.g. "BTCUSD"), the
    mark_price channel uses "MARK:" + that same symbol
    (e.g. "MARK:BTCUSD")."""
    pairs = []
    for coin in COINS:
        symbol = f"{coin}USD"
        pairs.append({
            "delta_symbol": symbol,
            "delta_mark_symbol": f"MARK:{symbol}",
        })
    return pairs


def _on_update(exchange, symbol, mark_price=None, funding_rate=None):
    # For funding_rate updates, Delta echoes back the plain "BTCUSD"
    # symbol - strip the "USD" suffix to get the plain coin name used
    # everywhere else in this project.
    coin = symbol[:-3] if symbol.endswith("USD") else symbol
    with _lock:
        entry = _live_data.setdefault(coin, {})
        if mark_price is not None:
            entry["price"] = mark_price
            entry["price_updated_at"] = time.time()
        if funding_rate is not None:
            entry["funding"] = funding_rate
            entry["funding_updated_at"] = time.time()


def _run_forever():
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    pairs = _build_pairs()
    try:
        loop.run_until_complete(delta_listen(pairs, _on_update))
    finally:
        loop.close()


def start_delta_live_feed():
    """Call once, at app startup, in a daemon thread. Safe to call more
    than once - only actually starts the feed the first time."""
    global _started
    if _started:
        return
    _started = True
    t = threading.Thread(target=_run_forever, daemon=True, name="delta-live-feed")
    t.start()


def get_delta_live_data():
    """Returns a snapshot dict: { "BTC": {"price":.., "funding":..,
    "price_age_seconds":.., "funding_age_seconds":..}, ... }. Safe to
    call from Flask request handlers - only holds the lock briefly."""
    with _lock:
        now = time.time()
        return {
            coin: {
                "price": v.get("price"),
                "funding": v.get("funding"),
                "price_age_seconds": round(now - v["price_updated_at"], 1) if "price_updated_at" in v else None,
                "funding_age_seconds": round(now - v["funding_updated_at"], 1) if "funding_updated_at" in v else None,
            }
            for coin, v in _live_data.items()
        }

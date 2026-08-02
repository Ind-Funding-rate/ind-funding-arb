"""
Real order-book depth walker and cost calculator.

Computes the ACTUAL average fill price and slippage % for a given trade
size, using live order book depth from Delta, Pi42, and CoinSwitch -
endpoints confirmed working via src/data/test_depth_and_fees.py on
2026-08-01 (Delta and Pi42 REST depth, CoinSwitch's documented
/trade/api/v2/futures/order_book with l2Orderbook=true).

This is intentionally NOT part of the fast 300-coin price scan in
spread_scanner.py - fetching full depth is much heavier than fetching
last price, so this is built to be called on-demand for ONE coin + ONE
exchange pair at a time (when a user clicks "Real Cost" on a specific
row), not run in a background loop across the whole coin universe.

Confirmed real taker fee rates (before GST):
- Delta:      0.05%   (GST applies: +18% -> 0.0590% effective per fill)
- Pi42:       0.08%   (GST applies: +18% -> 0.0944% effective per fill)
- CoinSwitch: fetched LIVE per-symbol via instrument_info (0.065% for
              BTCUSDT when tested 2026-08-01) - GST treatment on
              CoinSwitch's fee is NOT confirmed, so it is reported
              separately rather than silently assumed and added.

A full spread-arbitrage round trip is 4 fills, not 2:
  1. BUY on the cheap exchange       (open long)
  2. SELL on the expensive exchange  (open short)
  3. SELL on the cheap exchange      (close long)
  4. BUY on the expensive exchange   (close short)
Each fill gets its own live-depth slippage calculation and its own fee.
"""
import requests

from src.data.coinswitch_client import _sign_request, BASE_URL as CS_BASE_URL

GST_MULTIPLIER = 1.18

DELTA_TAKER_FEE = 0.0005   # 0.05%, confirmed earlier in this project
PI42_TAKER_FEE = 0.0008    # 0.08%, confirmed earlier in this project


def get_delta_orderbook(coin):
    symbol = f"{coin}USD"
    url = f"https://api.india.delta.exchange/v2/l2orderbook/{symbol}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    result = r.json()["result"]
    bids = [(float(l["price"]), float(l["size"])) for l in result.get("buy", [])]
    asks = [(float(l["price"]), float(l["size"])) for l in result.get("sell", [])]
    return bids, asks


def get_pi42_orderbook(coin):
    symbol = f"{coin}USDT"
    url = f"https://api.pi42.com/v1/market/depth/{symbol}"
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    data = r.json()["data"]
    bids = [(float(p), float(q)) for p, q in data.get("b", [])]
    asks = [(float(p), float(q)) for p, q in data.get("a", [])]
    return bids, asks


def get_coinswitch_orderbook(coin):
    symbol = f"{coin}USDT"
    path = "/trade/api/v2/futures/order_book"
    params = {"symbol": symbol.lower(), "exchange": "EXCHANGE_2", "l2Orderbook": "true"}
    headers, path_with_query = _sign_request("GET", path, params)
    r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
    r.raise_for_status()
    data = r.json()["data"]
    bids = [(float(p), float(q)) for p, q in data.get("bids", [])]
    asks = [(float(p), float(q)) for p, q in data.get("asks", [])]
    return bids, asks


def get_coinswitch_fee(coin):
    symbol = f"{coin}USDT"
    path = "/trade/api/v2/futures/instrument_info"
    headers, path_with_query = _sign_request("GET", path, {"exchange": "EXCHANGE_2"})
    r = requests.get(CS_BASE_URL + path_with_query, headers=headers, timeout=15)
    r.raise_for_status()
    info = r.json()["data"].get(symbol, {})
    return float(info.get("taker_fee_rate", 0))


ORDERBOOK_FETCHERS = {
    "delta": get_delta_orderbook,
    "pi42": get_pi42_orderbook,
    "coinswitch": get_coinswitch_orderbook,
}


def walk_book(levels, target_quantity):
    """
    levels: list of (price, size), best-price-first (ascending for asks,
    descending for bids).
    Returns (avg_fill_price, quantity_filled, fully_filled: bool).
    fully_filled=False means the visible book didn't have enough depth to
    fill the whole target size - a real, useful warning on its own, not
    just folded into a slippage number.
    """
    remaining = target_quantity
    total_cost = 0.0
    filled = 0.0
    for price, size in levels:
        take = min(remaining, size)
        total_cost += take * price
        filled += take
        remaining -= take
        if remaining <= 1e-12:
            break
    if filled == 0:
        return 0.0, 0.0, False
    return total_cost / filled, filled, remaining <= 1e-12


def compute_real_cost(coin, exchange_cheap, exchange_expensive, position_usd):
    """
    Full round-trip economics for a spread-arbitrage trade using LIVE
    order book depth: buy cheap / sell expensive to open, then sell
    cheap / buy expensive to close - 4 fills, each individually walked
    through the real book for its own slippage.

    Returns a result dict, or {"error": "..."} if depth couldn't be
    fetched or a book was empty.
    """
    try:
        cheap_bids, cheap_asks = ORDERBOOK_FETCHERS[exchange_cheap](coin)
        exp_bids, exp_asks = ORDERBOOK_FETCHERS[exchange_expensive](coin)
    except Exception as e:
        return {"error": f"Could not fetch live order book: {e}"}

    if not cheap_asks or not exp_bids or not cheap_bids or not exp_asks:
        return {"error": "One of the exchanges has no order book depth for this coin right now"}

    best_ask_cheap = cheap_asks[0][0]
    best_bid_cheap = cheap_bids[0][0]
    best_bid_exp = exp_bids[0][0]
    best_ask_exp = exp_asks[0][0]

    quantity = position_usd / best_ask_cheap

    open_buy_price, open_buy_qty, open_buy_full = walk_book(cheap_asks, quantity)
    open_buy_slip = (open_buy_price - best_ask_cheap) / best_ask_cheap * 100

    open_sell_price, open_sell_qty, open_sell_full = walk_book(exp_bids, quantity)
    open_sell_slip = (best_bid_exp - open_sell_price) / best_bid_exp * 100

    close_sell_price, close_sell_qty, close_sell_full = walk_book(cheap_bids, quantity)
    close_sell_slip = (best_bid_cheap - close_sell_price) / best_bid_cheap * 100

    close_buy_price, close_buy_qty, close_buy_full = walk_book(exp_asks, quantity)
    close_buy_slip = (close_buy_price - best_ask_exp) / best_ask_exp * 100

    def fee_pct(exchange):
        if exchange == "coinswitch":
            try:
                rate = get_coinswitch_fee(coin)
            except Exception:
                rate = 0
            return rate * 100, None
        if exchange == "delta":
            return DELTA_TAKER_FEE * GST_MULTIPLIER * 100, True
        if exchange == "pi42":
            return PI42_TAKER_FEE * GST_MULTIPLIER * 100, True
        return 0, None

    cheap_fee_pct, cheap_gst = fee_pct(exchange_cheap)
    exp_fee_pct, exp_gst = fee_pct(exchange_expensive)

    total_fees_pct = (cheap_fee_pct * 2) + (exp_fee_pct * 2)
    total_slippage_pct = open_buy_slip + open_sell_slip + close_sell_slip + close_buy_slip
    raw_spread_pct = (best_bid_exp - best_ask_cheap) / best_ask_cheap * 100
    net_pct = raw_spread_pct - total_fees_pct - total_slippage_pct
    fully_fillable = open_buy_full and open_sell_full and close_sell_full and close_buy_full

    return {
        "coin": coin,
        "exchange_cheap": exchange_cheap,
        "exchange_expensive": exchange_expensive,
        "position_usd": position_usd,
        "quantity": quantity,
        "raw_spread_pct": raw_spread_pct,
        "legs": [
            {"label": "Open: Buy", "exchange": exchange_cheap, "avg_price": open_buy_price, "slippage_pct": open_buy_slip, "fully_filled": open_buy_full},
            {"label": "Open: Sell", "exchange": exchange_expensive, "avg_price": open_sell_price, "slippage_pct": open_sell_slip, "fully_filled": open_sell_full},
            {"label": "Close: Sell", "exchange": exchange_cheap, "avg_price": close_sell_price, "slippage_pct": close_sell_slip, "fully_filled": close_sell_full},
            {"label": "Close: Buy", "exchange": exchange_expensive, "avg_price": close_buy_price, "slippage_pct": close_buy_slip, "fully_filled": close_buy_full},
        ],
        "fee_pct_cheap": cheap_fee_pct,
        "fee_pct_expensive": exp_fee_pct,
        "cheap_gst_included": cheap_gst,
        "expensive_gst_included": exp_gst,
        "total_fees_pct": total_fees_pct,
        "total_slippage_pct": total_slippage_pct,
        "net_pct": net_pct,
        "fully_fillable_at_size": fully_fillable,
    }

"""
Spread Arbitrage Scanner - a fully independent module.

Shows the live PRICE difference for the same coin across exchanges,
expressed as a spread %. This does NOT touch the funding-rate scanner's
detection/alerting/backtesting logic or the SQLite database - it is a
standalone screener, by design.

Delta and CoinSwitch price data is reused from the exchange-calling
utility functions that full_market_scanner.py and coinswitch_client.py
already have (get_delta_funding_all, get_all_funding_rates) - these are
pure "ask exchange X for its current numbers" functions, not strategy
logic, so reusing them avoids re-solving already-fixed bugs. This module
runs its own SEPARATE background refresh loop and keeps its own cache,
so it never reads the funding scanner's _latest_rows or depends on its
timing.

Unit note: Delta uses multiplier-prefixed symbols for some coins (e.g.
"1000BONK" = price of 1000 BONK, "1MBABYDOGE" = price of 1,000,000
BABYDOGE) while CoinSwitch and Pi42's USDT market do not. Delta prices
are normalized to a true per-unit basis before computing spreads, or
coins on the mismatched side would show a fake ~1000x/1,000,000x
"spread" that's really just a unit difference, not a real price gap.

Page requests never call an exchange directly - they only ever read the
in-memory cache built by the background loop, so response time doesn't
depend on exchange latency.

2026-07-31 fix #1: SPREAD_SCANNER_JS is now a raw string. It contains
JavaScript's own \\uXXXX escape for an emoji (a UTF-16 surrogate pair,
valid JS) - without the raw prefix, Python was interpreting that escape
itself, producing invalid characters that crashed every page load.

2026-07-31 fix #2/#3 (superseded): tried converting Pi42's INR prices to
USD via a live forex rate. Wrong endpoint caused a silent fallback to a
stale hardcoded rate, producing a uniform ~+8% "spread" on every coin.

2026-07-31 fix #4: switched Pi42 to its own native USDT-margined market
instead of INR+conversion - verified live via a standalone test. Delta,
Pi42, and CoinSwitch are now all queried in native USD/USDT terms, no
currency conversion, no external FX dependency.

2026-07-31 feature (Nikunj's request): was previously a fixed A-vs-B
dropdown pair. Now supports selecting ANY 2 or all 3 exchanges via
checkboxes - for each coin, every pairwise combination among the
SELECTED exchanges is computed and whichever pair has the largest
spread for that coin is shown (same "best pair wins" pattern used in
the 3-way funding-rate scanner). Also added a "show top N" selector
(10/20/50) so the page shows only the biggest opportunities at a
glance instead of every coin with any data.
"""
import asyncio
import itertools
import json
import threading
import time
from datetime import datetime

import websockets

from src.execution.full_market_scanner import get_delta_funding_all, COINS as _FUNDING_COINS
from src.data.coinswitch_client import get_all_funding_rates

EXCHANGES = ["delta", "pi42", "coinswitch"]
EXCHANGE_LABELS = {"delta": "Delta Exchange", "pi42": "Pi42", "coinswitch": "CoinSwitch"}

REFRESH_INTERVAL_SECONDS = 90
PI42_WS_URL = "wss://fawss.pi42.com/socket.io/?EIO=4&transport=websocket"
PI42_WS_WINDOW_SECONDS = 45  # matches the funding scanner's own window

_price_cache = {}   # {coin: {"delta": price_or_missing, "pi42": ..., "coinswitch": ...}}
_cache_lock = threading.Lock()
_cache_updated_at = None
_cache_error = None


def _multiplier_strip(raw_symbol):
    """'1000BONK' -> ('BONK', 1000), '1MBABYDOGE' -> ('BABYDOGE', 1000000),
    'BTC' -> ('BTC', 1)."""
    if raw_symbol.startswith("1000"):
        return raw_symbol[4:], 1000
    if raw_symbol.startswith("1M"):
        return raw_symbol[2:], 1_000_000
    return raw_symbol, 1


async def _pi42_usdt_ws_batch(coins):
    """Pi42's native USDT-margined market - confirmed working via a
    standalone test (src/data/test_pi42_usdt_channel.py) on 2026-07-31."""
    channels = [f"{_multiplier_strip(c)[0].lower()}usdt@markPrice" for c in coins]
    results = {}
    async with websockets.connect(PI42_WS_URL) as ws:
        await ws.recv()
        await ws.send("40")
        await ws.recv()
        sub_msg = f'42["subscribe", {{"params": {json.dumps(channels)}}}]'
        await ws.send(sub_msg)

        end_time = asyncio.get_event_loop().time() + PI42_WS_WINDOW_SECONDS
        while len(results) < len(channels):
            remaining = end_time - asyncio.get_event_loop().time()
            if remaining <= 0:
                break
            try:
                msg = await asyncio.wait_for(ws.recv(), timeout=remaining)
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
                if sym.endswith("USDT"):
                    base = sym[:-4]
                    if base not in results:
                        price = data.get("p", 0)
                        results[base] = float(price) if price else 0
    return results


def get_pi42_usdt_all(coins):
    try:
        return asyncio.run(_pi42_usdt_ws_batch(coins))
    except Exception as e:
        print(f"[spread-scanner] Pi42 USDT batch fetch failed: {e}")
        return {}


def refresh_price_cache():
    global _price_cache, _cache_updated_at, _cache_error
    try:
        merged = {}

        delta_data = get_delta_funding_all()
        for raw_symbol, info in delta_data.items():
            coin, mult = _multiplier_strip(raw_symbol)
            price = info.get("price", 0)
            if price:
                merged.setdefault(coin, {})["delta"] = price / mult

        pi42_usdt_data = get_pi42_usdt_all(_FUNDING_COINS)
        for coin, price in pi42_usdt_data.items():
            if price:
                merged.setdefault(coin, {})["pi42"] = price

        cs_data = get_all_funding_rates()
        for raw_symbol, info in cs_data.items():
            if not raw_symbol.endswith("USDT"):
                continue
            coin = raw_symbol[:-4]
            price = info.get("mark_price", 0)
            if price:
                merged.setdefault(coin, {})["coinswitch"] = float(price)

        with _cache_lock:
            _price_cache = merged
            _cache_updated_at = datetime.now()
            _cache_error = None

        print(f"[spread-scanner] cache refreshed: {len(merged)} coins have at least one "
              f"price (all native USD/USDT)")
    except Exception as e:
        with _cache_lock:
            _cache_error = str(e)
        print(f"[spread-scanner] price refresh failed: {e}")


def spread_background_loop():
    while True:
        refresh_price_cache()
        time.sleep(REFRESH_INTERVAL_SECONDS)


def start_spread_background_loop():
    threading.Thread(target=spread_background_loop, daemon=True).start()


def get_cache_status():
    with _cache_lock:
        if _cache_updated_at is None:
            if _cache_error:
                return "error", _cache_error, None
            return "warming_up", None, None
        age = (datetime.now() - _cache_updated_at).total_seconds()
        return "live", _cache_error, age


def get_spread_rows(selected_exchanges, search="", min_spread=0.0, limit=10):
    """
    selected_exchanges: list of 2 or 3 from EXCHANGES (e.g. ["delta","pi42"]
    or ["delta","pi42","coinswitch"]).

    For each coin, checks every pairwise combination among the SELECTED
    exchanges, keeps whichever pair has the largest |spread| for that
    coin (mirrors the "best pair wins" approach in the 3-way funding
    scanner), then returns the top `limit` coins by |spread| across the
    whole list.

    Returns list of dicts:
    {coin, exchange_a, exchange_b, price_a, price_b, diff, spread_pct,
     last_updated, status}
    """
    with _cache_lock:
        snapshot = dict(_price_cache)
        updated_at = _cache_updated_at

    now = updated_at.strftime("%H:%M:%S") if updated_at else "--:--:--"
    search = search.strip().upper()
    pairs = list(itertools.combinations(selected_exchanges, 2))

    rows = []
    for coin, prices in snapshot.items():
        if search and search not in coin:
            continue

        best = None
        for ex_a, ex_b in pairs:
            price_a = prices.get(ex_a)
            price_b = prices.get(ex_b)
            if not price_a or not price_b:
                continue
            diff = price_b - price_a
            spread_pct = (diff / price_a) * 100
            if best is None or abs(spread_pct) > abs(best["spread_pct"]):
                best = {
                    "exchange_a": ex_a, "exchange_b": ex_b,
                    "price_a": price_a, "price_b": price_b,
                    "diff": diff, "spread_pct": spread_pct,
                }

        if best is None or abs(best["spread_pct"]) < min_spread:
            continue

        if best["spread_pct"] > 0.05:
            status = "positive"
        elif best["spread_pct"] < -0.05:
            status = "negative"
        else:
            status = "neutral"

        rows.append({
            "coin": coin,
            "exchange_a": best["exchange_a"],
            "exchange_b": best["exchange_b"],
            "price_a": best["price_a"],
            "price_b": best["price_b"],
            "diff": best["diff"],
            "spread_pct": best["spread_pct"],
            "last_updated": now,
            "status": status,
        })

    rows.sort(key=lambda r: abs(r["spread_pct"]), reverse=True)
    return rows[:limit]


SPREAD_SCANNER_CSS = """
  .spread-controls { display:flex; gap:16px; flex-wrap:wrap; align-items:end; margin-bottom:16px; }
  .spread-controls label { display:block; font-size:12px; color:#8b8fa3; margin-bottom:4px; }
  .spread-controls input[type=text], .spread-controls input[type=number], .spread-controls select {
    background:#1a1d27; border:1px solid #2a2d38; color:#e6e6e6;
    padding:8px 10px; border-radius:6px; font-size:14px;
  }
  .exchange-checks { display:flex; gap:12px; align-items:center; background:#1a1d27;
    border:1px solid #2a2d38; border-radius:6px; padding:9px 14px; }
  .exchange-checks label { display:flex; align-items:center; gap:6px; margin:0;
    color:#e6e6e6; font-size:13px; cursor:pointer; }
  .exchange-checks input { width:auto; cursor:pointer; }
  .toggle-wrap { display:flex; align-items:center; gap:8px; padding-bottom:9px; }
  .toggle-wrap input { width:auto; }
  .mock-banner { background:#422006; border:1px solid #92400e; color:#facc15;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px; }
  .live-banner { background:#14532d; border:1px solid #166534; color:#4ade80;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px; }
  .error-banner { background:#2d1515; border:1px solid #7f1d1d; color:#f87171;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px; }
  .fx-note { color:#8b8fa3; font-size:11px; margin:-10px 0 16px; }
  #spread-tbl thead th { position:sticky; top:0; }
  .status-positive { color:#4ade80; font-weight:700; }
  .status-negative { color:#f87171; font-weight:700; }
  .status-neutral { color:#8b8fa3; }
  .pair-tag { font-size:11px; color:#8b8fa3; }
"""

SPREAD_SCANNER_JS = r"""
<script>
let autoRefreshTimer = null;

function buildQuery() {
  const checks = document.querySelectorAll('.exchange-checks input:checked');
  const exchanges = Array.from(checks).map(c => c.value);
  const search = document.getElementById('spread-search').value;
  const minSpread = document.getElementById('min-spread').value || 0;
  const limit = document.getElementById('spread-limit').value || 10;
  const exchangeParams = exchanges.map(e => 'exchanges=' + e).join('&');
  return exchangeParams + '&search=' + encodeURIComponent(search) + '&min_spread=' + minSpread + '&limit=' + limit;
}

function renderRows(rows) {
  const tbody = document.querySelector('#spread-tbl tbody');
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No data yet - either still warming up, fewer than 2 exchanges selected, or nothing matches the filter.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(function(r) {
    const sign = r.diff >= 0 ? '+' : '';
    const pctSign = r.spread_pct >= 0 ? '+' : '';
    return '<tr>' +
      '<td><b>' + r.coin + '</b></td>' +
      '<td class="pair-tag">' + r.exchange_a_label + ' vs ' + r.exchange_b_label + '</td>' +
      '<td>$' + r.price_a.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>$' + r.price_b.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>' + sign + '$' + r.diff.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td class="status-' + r.status + '">' + pctSign + r.spread_pct.toFixed(3) + '%</td>' +
      '<td>' + r.last_updated + '</td>' +
    '</tr>';
  }).join('');
}

function updateBanner(status, error, ageSeconds) {
  const banner = document.getElementById('status-banner');
  if (status === 'live') {
    banner.className = 'live-banner';
    banner.innerHTML = '\ud83d\udfe2 Live data \u00b7 updated ' + Math.round(ageSeconds) + 's ago';
  } else if (status === 'error') {
    banner.className = 'error-banner';
    banner.innerHTML = '\u26a0\ufe0f Price fetch failed: ' + error;
  } else {
    banner.className = 'mock-banner';
    banner.innerHTML = '\u23f3 Fetching real prices for the first time - this can take up to a minute...';
  }
}

function refreshData() {
  const checks = document.querySelectorAll('.exchange-checks input:checked');
  if (checks.length < 2) {
    document.querySelector('#spread-tbl tbody').innerHTML =
      '<tr><td colspan="7" style="text-align:center;color:#facc15">Select at least 2 exchanges to compare.</td></tr>';
    return;
  }
  fetch('/spread-scanner/data?' + buildQuery())
    .then(function(res) { return res.json(); })
    .then(function(data) {
      renderRows(data.rows);
      updateBanner(data.cache_status, data.cache_error, data.cache_age_seconds);
    });
}

function scheduleAutoRefresh() {
  if (autoRefreshTimer) clearInterval(autoRefreshTimer);
  if (document.getElementById('auto-refresh').checked) {
    autoRefreshTimer = setInterval(refreshData, 4000);
  }
}

document.querySelectorAll('.exchange-checks input').forEach(function(cb) {
  cb.addEventListener('change', refreshData);
});
document.getElementById('spread-search').addEventListener('input', refreshData);
document.getElementById('min-spread').addEventListener('input', refreshData);
document.getElementById('spread-limit').addEventListener('change', refreshData);
document.getElementById('refresh-btn').addEventListener('click', refreshData);
document.getElementById('auto-refresh').addEventListener('change', scheduleAutoRefresh);

scheduleAutoRefresh();
</script>
"""

SPREAD_SCANNER_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Spread Arbitrage Scanner</title>
<style>{css}{spread_css}</style></head><body>
{nav}

<h2 style="font-size:18px;margin:0 0 6px;">Spread Arbitrage Scanner</h2>
<p class="meta">Live price spread between exchanges for the same coin. Independent of the funding-rate scanner \u2014 no signals, no backtesting, no trade execution.</p>
<div id="status-banner" class="{banner_class}">{banner_text}</div>
<p class="fx-note">All prices are each exchange's own native USD/USDT market \u2014 Delta USD, Pi42's USDT market (not INR), CoinSwitch USDT. No currency conversion involved.</p>

<div class="spread-controls">
  <div><label>Exchanges to compare (pick 2 or all 3)</label>
    <div class="exchange-checks">{exchange_checkboxes}</div>
  </div>
  <div><label>Search coin</label>
    <input id="spread-search" type="text" placeholder="e.g. BTC" autocomplete="off"></div>
  <div><label>Min spread %</label>
    <input id="min-spread" type="number" step="0.01" value="0" style="width:90px"></div>
  <div><label>Show top</label>
    <select id="spread-limit">{limit_options}</select></div>
  <div><button id="refresh-btn" type="button">Refresh</button></div>
  <div class="toggle-wrap">
    <input id="auto-refresh" type="checkbox" checked>
    <label style="margin:0">Auto refresh (4s)</label>
  </div>
</div>

<table id="spread-tbl">
<thead><tr>
  <th>Coin</th><th>Best Pair</th><th>Price A (USD)</th><th>Price B (USD)</th>
  <th>Price Diff</th><th>Spread %</th><th>Last Updated</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>

{js}
</body></html>
"""


def _render_rows_html(rows):
    if not rows:
        return '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No data yet - either still warming up, fewer than 2 exchanges selected, or nothing matches the filter.</td></tr>'
    html = ""
    for r in rows:
        pair_label = f"{EXCHANGE_LABELS[r['exchange_a']]} vs {EXCHANGE_LABELS[r['exchange_b']]}"
        html += (
            f"<tr>"
            f"<td><b>{r['coin']}</b></td>"
            f"<td class='pair-tag'>{pair_label}</td>"
            f"<td>${r['price_a']:,.8f}</td>"
            f"<td>${r['price_b']:,.8f}</td>"
            f"<td>{r['diff']:+,.8f}</td>"
            f"<td class='status-{r['status']}'>{r['spread_pct']:+.3f}%</td>"
            f"<td>{r['last_updated']}</td>"
            f"</tr>"
        )
    return html


def render_spread_scanner_page(base_css, nav_html, selected_exchanges=None,
                                 search="", min_spread=0.0, limit=10):
    if not selected_exchanges:
        selected_exchanges = ["delta", "pi42", "coinswitch"]

    rows = get_spread_rows(selected_exchanges, search, min_spread, limit) \
        if len(selected_exchanges) >= 2 else []
    status, error, age = get_cache_status()

    if status == "live":
        banner_class = "live-banner"
        banner_text = f"\U0001f7e2 Live data \u00b7 updated {int(age)}s ago"
    elif status == "error":
        banner_class = "error-banner"
        banner_text = f"\u26a0\ufe0f Price fetch failed: {error}"
    else:
        banner_class = "mock-banner"
        banner_text = "\u23f3 Fetching real prices for the first time - this can take up to a minute..."

    checkboxes_html = "".join(
        f'<label><input type="checkbox" value="{e}" '
        f'{"checked" if e in selected_exchanges else ""}>{EXCHANGE_LABELS[e]}</label>'
        for e in EXCHANGES
    )
    limit_options_html = "".join(
        f'<option value="{n}" {"selected" if n == limit else ""}>{n}</option>'
        for n in [10, 20, 50]
    )

    return SPREAD_SCANNER_PAGE.format(
        css=base_css, spread_css=SPREAD_SCANNER_CSS, nav=nav_html,
        banner_class=banner_class, banner_text=banner_text,
        exchange_checkboxes=checkboxes_html,
        limit_options=limit_options_html,
        rows=_render_rows_html(rows),
        js=SPREAD_SCANNER_JS,
    )

"""
Spread Arbitrage Scanner - a fully independent module.

Shows the live PRICE difference for the same coin across two exchanges,
expressed as a spread %. This does NOT touch the funding-rate scanner's
detection/alerting/backtesting logic or the SQLite database - it is a
standalone screener, by design.

Price data IS reused from the exchange-calling utility functions that
full_market_scanner.py and coinswitch_client.py already have
(get_delta_funding_all, get_pi42_funding_all, get_all_funding_rates) -
these are pure "ask exchange X for its current numbers" functions, not
strategy logic, so reusing them avoids re-solving already-fixed bugs
(e.g. the Pi42 field-name issue found earlier). This module runs its
own SEPARATE background refresh loop and keeps its own cache, so it
never reads the funding scanner's _latest_rows or depends on its timing.

Tradeoff worth knowing: Pi42 price data comes via a websocket batch
subscription (same mechanism the funding scanner uses), and running a
second, independent one here roughly doubles Pi42 connection load
compared to before this page existed. Refresh interval is set to match
the funding scanner's own cadence (90s) rather than anything faster, to
avoid pushing that further.

Unit note: Delta/Pi42 use multiplier-prefixed symbols for some coins
(e.g. "1000BONK" = price of 1000 BONK, "1MBABYDOGE" = price of
1,000,000 BABYDOGE) while CoinSwitch does not. Prices are normalized to
a true per-unit basis before computing spreads, or coins on the
mismatched side would show a fake ~1000x/1,000,000x "spread" that's
really just a unit difference, not a real price gap.

Page requests never call an exchange directly - they only ever read the
in-memory cache built by the background loop, so response time doesn't
depend on exchange latency.

2026-07-31 fix #1: SPREAD_SCANNER_JS is now a raw string (r\"\"\"...\"\"\").
It contains JavaScript's own \\uXXXX escape for an emoji
(\\ud83d\\udfe2 - a UTF-16 surrogate pair, valid JS). Without the raw
prefix, PYTHON was interpreting that escape itself before the page ever
reached the browser - producing two lone surrogate codepoints, which
crashed every request to /spread-scanner with
UnicodeEncodeError: 'utf-8' codec can't encode ... surrogates not
allowed the moment Flask tried to encode the response. Raw string stops
Python touching JS's own escape sequences.

2026-07-31 fix #2 (bigger, found by Nikunj testing the live page):
CURRENCY MISMATCH. Delta quotes prices in USD, CoinSwitch in USDT
(~USD), but Pi42 quotes in INR - and prices were being compared
directly with no conversion, producing nonsense spreads like "+9400%"
for every single coin (that's just roughly the USD/INR exchange rate,
not a real arbitrage signal). Pi42 prices are now converted to USD
using a live USD/INR rate from api.frankfurter.dev (free, no API key,
no quota - see their docs) before being placed in the shared price
cache, so every price in _price_cache is now genuinely in the same
currency (USD) and spreads are real. The FX rate itself is fetched on
its own slower cadence (hourly) with a hardcoded fallback, since it
changes far slower than crypto prices and repeatedly hitting a free
public API every 90s would be inconsiderate for no benefit.
"""
import threading
import time
import requests
from datetime import datetime

from src.execution.full_market_scanner import (
    get_delta_funding_all, get_pi42_funding_all, COINS as _FUNDING_COINS,
)
from src.data.coinswitch_client import get_all_funding_rates

EXCHANGES = ["delta", "pi42", "coinswitch"]
EXCHANGE_LABELS = {"delta": "Delta Exchange", "pi42": "Pi42", "coinswitch": "CoinSwitch"}

REFRESH_INTERVAL_SECONDS = 90
FX_REFRESH_INTERVAL_SECONDS = 60 * 60  # USD/INR moves slowly - no need to hit this every cycle
FX_FALLBACK_USD_INR = 88.0  # used only if the FX API is unreachable on first run

_price_cache = {}   # {coin: {"delta": price_or_missing, "pi42": ..., "coinswitch": ...}}
_cache_lock = threading.Lock()
_cache_updated_at = None
_cache_error = None

_fx_lock = threading.Lock()
_usd_inr_rate = FX_FALLBACK_USD_INR
_fx_updated_at = None


def _multiplier_strip(raw_symbol):
    """'1000BONK' -> ('BONK', 1000), '1MBABYDOGE' -> ('BABYDOGE', 1000000),
    'BTC' -> ('BTC', 1). Needed to convert a quoted contract price back to
    a true per-unit price before comparing across an exchange that
    doesn't use the same multiplier-prefix convention."""
    if raw_symbol.startswith("1000"):
        return raw_symbol[4:], 1000
    if raw_symbol.startswith("1M"):
        return raw_symbol[2:], 1_000_000
    return raw_symbol, 1


def _refresh_usd_inr_rate():
    """Updates the cached USD/INR rate if it's stale (>1 hour old) or has
    never been fetched. Free, no API key: api.frankfurter.dev."""
    global _usd_inr_rate, _fx_updated_at
    with _fx_lock:
        if _fx_updated_at is not None:
            age = (datetime.now() - _fx_updated_at).total_seconds()
            if age < FX_REFRESH_INTERVAL_SECONDS:
                return _usd_inr_rate

    try:
        r = requests.get(
            "https://api.frankfurter.dev/v2/latest",
            params={"base": "USD", "symbols": "INR"},
            timeout=10,
        )
        r.raise_for_status()
        rate = float(r.json()["rates"]["INR"])
        with _fx_lock:
            _usd_inr_rate = rate
            _fx_updated_at = datetime.now()
        print(f"[spread-scanner] USD/INR rate refreshed: {rate}")
    except Exception as e:
        print(f"[spread-scanner] USD/INR fetch failed, using last known rate "
              f"({_usd_inr_rate}): {e}")
    return _usd_inr_rate


def refresh_price_cache():
    """Fetches current prices from all three exchanges and rebuilds the
    cache. Only ever called from the background loop below - never from
    a web request, so a slow Pi42 fetch never makes a page load slow.
    Pi42 prices are converted from INR to USD here so every exchange in
    the cache is denominated in the same currency."""
    global _price_cache, _cache_updated_at, _cache_error
    try:
        usd_inr = _refresh_usd_inr_rate()
        merged = {}

        delta_data = get_delta_funding_all()
        for raw_symbol, info in delta_data.items():
            coin, mult = _multiplier_strip(raw_symbol)
            price = info.get("price", 0)
            if price:
                merged.setdefault(coin, {})["delta"] = price / mult

        pi42_data = get_pi42_funding_all(_FUNDING_COINS)
        for raw_symbol, info in pi42_data.items():
            coin, mult = _multiplier_strip(raw_symbol)
            price_inr = info.get("price", 0)
            if price_inr:
                price_usd = price_inr / usd_inr  # INR -> USD conversion
                merged.setdefault(coin, {})["pi42"] = price_usd / mult

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
              f"price (all in USD, Pi42 converted @ {usd_inr:.3f} INR/USD)")
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
    """Returns (status, error_or_None, age_seconds_or_None).
    status is one of: 'warming_up', 'live', 'error'."""
    with _cache_lock:
        if _cache_updated_at is None:
            if _cache_error:
                return "error", _cache_error, None
            return "warming_up", None, None
        age = (datetime.now() - _cache_updated_at).total_seconds()
        return "live", _cache_error, age


def get_spread_rows(exchange_a, exchange_b, search="", min_spread=0.0):
    """
    Returns a list of dicts, one per coin (all prices already in USD):
    {coin, price_a, price_b, diff, spread_pct, last_updated, status}
    Reads from the in-memory price cache only.
    """
    with _cache_lock:
        snapshot = dict(_price_cache)
        updated_at = _cache_updated_at

    rows = []
    now = updated_at.strftime("%H:%M:%S") if updated_at else "--:--:--"
    search = search.strip().upper()

    for coin, prices in snapshot.items():
        if search and search not in coin:
            continue

        price_a = prices.get(exchange_a)
        price_b = prices.get(exchange_b)
        if not price_a or not price_b:
            continue

        diff = price_b - price_a
        spread_pct = (diff / price_a) * 100

        if abs(spread_pct) < min_spread:
            continue

        if spread_pct > 0.05:
            status = "positive"
        elif spread_pct < -0.05:
            status = "negative"
        else:
            status = "neutral"

        rows.append({
            "coin": coin,
            "price_a": price_a,
            "price_b": price_b,
            "diff": diff,
            "spread_pct": spread_pct,
            "last_updated": now,
            "status": status,
        })

    rows.sort(key=lambda r: abs(r["spread_pct"]), reverse=True)
    return rows


SPREAD_SCANNER_CSS = """
  .spread-controls { display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin-bottom:16px; }
  .spread-controls label { display:block; font-size:12px; color:#8b8fa3; margin-bottom:4px; }
  .spread-controls select, .spread-controls input {
    background:#1a1d27; border:1px solid #2a2d38; color:#e6e6e6;
    padding:8px 10px; border-radius:6px; font-size:14px;
  }
  .toggle-wrap { display:flex; align-items:center; gap:8px; padding-bottom:9px; }
  .toggle-wrap input { width:auto; }
  .mock-banner {
    background:#422006; border:1px solid #92400e; color:#facc15;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px;
  }
  .live-banner {
    background:#14532d; border:1px solid #166534; color:#4ade80;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px;
  }
  .error-banner {
    background:#2d1515; border:1px solid #7f1d1d; color:#f87171;
    font-size:12px; padding:8px 14px; border-radius:6px; margin-bottom:16px;
  }
  .fx-note { color:#8b8fa3; font-size:11px; margin:-10px 0 16px; }
  #spread-tbl thead th { position:sticky; top:0; }
  .status-positive { color:#4ade80; font-weight:700; }
  .status-negative { color:#f87171; font-weight:700; }
  .status-neutral { color:#8b8fa3; }
"""

SPREAD_SCANNER_JS = r"""
<script>
let autoRefreshTimer = null;

function buildQuery() {
  const a = document.getElementById('exchange-a').value;
  const b = document.getElementById('exchange-b').value;
  const search = document.getElementById('spread-search').value;
  const minSpread = document.getElementById('min-spread').value || 0;
  return 'exchange_a=' + a + '&exchange_b=' + b + '&search=' + encodeURIComponent(search) + '&min_spread=' + minSpread;
}

function renderRows(rows) {
  const tbody = document.querySelector('#spread-tbl tbody');
  if (rows.length === 0) {
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No data yet for this pair/filter - either still warming up, or no coins match.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(function(r) {
    const sign = r.diff >= 0 ? '+' : '';
    const pctSign = r.spread_pct >= 0 ? '+' : '';
    return '<tr>' +
      '<td><b>' + r.coin + '</b></td>' +
      '<td>$' + r.price_a.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>$' + r.price_b.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>' + sign + '$' + r.diff.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td class="status-' + r.status + '">' + pctSign + r.spread_pct.toFixed(3) + '%</td>' +
      '<td>' + r.last_updated + '</td>' +
      '<td class="status-' + r.status + '">' + r.status.toUpperCase() + '</td>' +
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

document.getElementById('exchange-a').addEventListener('change', refreshData);
document.getElementById('exchange-b').addEventListener('change', refreshData);
document.getElementById('spread-search').addEventListener('input', refreshData);
document.getElementById('min-spread').addEventListener('input', refreshData);
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
<p class="meta">Live price spread between two exchanges for the same coin. Independent of the funding-rate scanner \u2014 no signals, no backtesting, no trade execution.</p>
<div id="status-banner" class="{banner_class}">{banner_text}</div>
<p class="fx-note">All prices shown in USD. Pi42 quotes in INR and is converted using a live USD/INR rate (\u2248 {usd_inr_rate:.2f}), refreshed hourly.</p>

<div class="spread-controls">
  <div><label>Exchange A</label>
    <select id="exchange-a">{exchange_a_options}</select></div>
  <div><label>Exchange B</label>
    <select id="exchange-b">{exchange_b_options}</select></div>
  <div><label>Search coin</label>
    <input id="spread-search" placeholder="e.g. BTC" autocomplete="off"></div>
  <div><label>Min spread %</label>
    <input id="min-spread" type="number" step="0.01" value="0" style="width:90px"></div>
  <div><button id="refresh-btn" type="button">Refresh</button></div>
  <div class="toggle-wrap">
    <input id="auto-refresh" type="checkbox" checked>
    <label style="margin:0">Auto refresh (4s)</label>
  </div>
</div>

<table id="spread-tbl">
<thead><tr>
  <th>Coin</th><th>Exchange A Price (USD)</th><th>Exchange B Price (USD)</th>
  <th>Price Diff</th><th>Spread %</th><th>Last Updated</th><th>Status</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>

{js}
</body></html>
"""


def _render_rows_html(rows):
    if not rows:
        return '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No data yet for this pair/filter - either still warming up, or no coins match.</td></tr>'
    html = ""
    for r in rows:
        html += (
            f"<tr>"
            f"<td><b>{r['coin']}</b></td>"
            f"<td>${r['price_a']:,.8f}</td>"
            f"<td>${r['price_b']:,.8f}</td>"
            f"<td>{r['diff']:+,.8f}</td>"
            f"<td class='status-{r['status']}'>{r['spread_pct']:+.3f}%</td>"
            f"<td>{r['last_updated']}</td>"
            f"<td class='status-{r['status']}'>{r['status'].upper()}</td>"
            f"</tr>"
        )
    return html


def render_spread_scanner_page(base_css, nav_html, exchange_a="delta", exchange_b="pi42",
                                 search="", min_spread=0.0):
    rows = get_spread_rows(exchange_a, exchange_b, search, min_spread)
    status, error, age = get_cache_status()

    with _fx_lock:
        current_rate = _usd_inr_rate

    if status == "live":
        banner_class = "live-banner"
        banner_text = f"\U0001f7e2 Live data \u00b7 updated {int(age)}s ago"
    elif status == "error":
        banner_class = "error-banner"
        banner_text = f"\u26a0\ufe0f Price fetch failed: {error}"
    else:
        banner_class = "mock-banner"
        banner_text = "\u23f3 Fetching real prices for the first time - this can take up to a minute..."

    def options(selected):
        return "".join(
            f'<option value="{e}" {"selected" if e == selected else ""}>{EXCHANGE_LABELS[e]}</option>'
            for e in EXCHANGES
        )

    return SPREAD_SCANNER_PAGE.format(
        css=base_css, spread_css=SPREAD_SCANNER_CSS, nav=nav_html,
        banner_class=banner_class, banner_text=banner_text,
        usd_inr_rate=current_rate,
        exchange_a_options=options(exchange_a),
        exchange_b_options=options(exchange_b),
        rows=_render_rows_html(rows),
        js=SPREAD_SCANNER_JS,
    )

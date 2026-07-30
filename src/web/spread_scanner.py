"""
Spread Arbitrage Scanner - a fully independent module.

Shows the live PRICE difference for the same coin across two exchanges,
expressed as a spread %. This does NOT touch the funding-rate scanner,
the backtest engine, the SQLite database, or any existing strategy logic
- it is a standalone screener, by design.

Currently ships with placeholder/mock data (clearly labeled as such on
the page). get_spread_rows() is the one function that needs to change to
plug in real prices later - the render function, the JSON endpoint, and
all of the front-end JS already work against its current return shape
and won't need to change when real data replaces the mock data.
"""
import random
from datetime import datetime

EXCHANGES = ["delta", "pi42", "coinswitch"]
EXCHANGE_LABELS = {"delta": "Delta Exchange", "pi42": "Pi42", "coinswitch": "CoinSwitch"}

# Base mock prices (USD-equivalent) - stable across refreshes, small jitter
# added per call so "auto refresh" visibly does something even in mock mode.
_MOCK_BASE_PRICES = {
    "BTC": 60000, "ETH": 3300, "SOL": 145, "XRP": 0.62, "DOGE": 0.14,
    "ADA": 0.45, "LINK": 14.5, "AVAX": 28, "DOT": 6.8, "LTC": 82,
    "BCH": 420, "UNI": 8.2, "SUI": 3.6, "TRX": 0.16, "NEAR": 5.4,
    "OP": 1.9, "INJ": 22, "SEI": 0.42, "ARB": 0.78, "APT": 8.9,
    "TIA": 6.1, "JUP": 0.85, "WIF": 2.1, "PEPE": 0.0000091, "BNB": 590,
    "ETC": 26, "FIL": 5.2, "HBAR": 0.075, "ICP": 9.8, "AAVE": 165,
    "MKR": 1650, "LDO": 1.6, "GALA": 0.024, "SAND": 0.34, "SHIB": 0.000015,
}

# Per-coin, per-exchange-pair base spread (%) - deterministic seed so the
# table has a realistic-looking mix of positive/negative/near-zero spreads.
_seed_rng = random.Random(42)
_MOCK_PAIR_SPREADS = {}
for _coin in _MOCK_BASE_PRICES:
    for _a in EXCHANGES:
        for _b in EXCHANGES:
            if _a != _b:
                _MOCK_PAIR_SPREADS[(_coin, _a, _b)] = _seed_rng.uniform(-1.2, 1.2)


def get_spread_rows(exchange_a, exchange_b, search="", min_spread=0.0):
    """
    Returns a list of dicts, one per coin:
    {coin, price_a, price_b, diff, spread_pct, last_updated, status}

    Currently mock data. Replace the body of this function with real
    exchange API calls when live prices are wired in - every caller
    (page render + JSON endpoint) depends only on this return shape,
    not on how the numbers were produced.
    """
    rows = []
    now = datetime.now().strftime("%H:%M:%S")
    search = search.strip().upper()

    for coin, base_price in _MOCK_BASE_PRICES.items():
        if search and search not in coin:
            continue

        base_spread_pct = _MOCK_PAIR_SPREADS.get((coin, exchange_a, exchange_b), 0.0)
        jitter = random.uniform(-0.08, 0.08)
        spread_pct = base_spread_pct + jitter

        price_a = base_price
        price_b = base_price * (1 + spread_pct / 100)
        diff = price_b - price_a

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
  #spread-tbl thead th { position:sticky; top:0; }
  .status-positive { color:#4ade80; font-weight:700; }
  .status-negative { color:#f87171; font-weight:700; }
  .status-neutral { color:#8b8fa3; }
"""

SPREAD_SCANNER_JS = """
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
    tbody.innerHTML = '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No coins match the current filters.</td></tr>';
    return;
  }
  tbody.innerHTML = rows.map(function(r) {
    const sign = r.diff >= 0 ? '+' : '';
    const pctSign = r.spread_pct >= 0 ? '+' : '';
    return '<tr>' +
      '<td><b>' + r.coin + '</b></td>' +
      '<td>' + r.price_a.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>' + r.price_b.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td>' + sign + r.diff.toLocaleString(undefined, {maximumFractionDigits: 8}) + '</td>' +
      '<td class="status-' + r.status + '">' + pctSign + r.spread_pct.toFixed(3) + '%</td>' +
      '<td>' + r.last_updated + '</td>' +
      '<td class="status-' + r.status + '">' + r.status.toUpperCase() + '</td>' +
    '</tr>';
  }).join('');
}

function refreshData() {
  fetch('/spread-scanner/data?' + buildQuery())
    .then(function(res) { return res.json(); })
    .then(function(data) { renderRows(data.rows); });
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
<div class="mock-banner">\u26a0\ufe0f Showing placeholder/mock data. Live exchange prices will be connected in a later step.</div>

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
  <th>Coin</th><th>Exchange A Price</th><th>Exchange B Price</th>
  <th>Price Diff</th><th>Spread %</th><th>Last Updated</th><th>Status</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>

{js}
</body></html>
"""


def _render_rows_html(rows):
    if not rows:
        return '<tr><td colspan="7" style="text-align:center;color:#8b8fa3">No coins match the current filters.</td></tr>'
    html = ""
    for r in rows:
        html += (
            f"<tr>"
            f"<td><b>{r['coin']}</b></td>"
            f"<td>{r['price_a']:,.8f}</td>"
            f"<td>{r['price_b']:,.8f}</td>"
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

    def options(selected):
        return "".join(
            f'<option value="{e}" {"selected" if e == selected else ""}>{EXCHANGE_LABELS[e]}</option>'
            for e in EXCHANGES
        )

    return SPREAD_SCANNER_PAGE.format(
        css=base_css, spread_css=SPREAD_SCANNER_CSS, nav=nav_html,
        exchange_a_options=options(exchange_a),
        exchange_b_options=options(exchange_b),
        rows=_render_rows_html(rows),
        js=SPREAD_SCANNER_JS,
    )

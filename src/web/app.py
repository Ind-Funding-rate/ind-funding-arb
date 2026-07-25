"""
Minimal web dashboard - 4 pages: live scanner, Delta/Pi42 backtest (our
own logged data), multi-exchange historical backtest, and an automated
opportunity matrix scanner across ALL available coins x exchange pairs.

Runs two background threads continuously:
1. The live full-market scanner (Delta vs Pi42, 133 coins, every ~90s)
2. The historical opportunity matrix scanner (ALL coins common to Bybit
   and OKX - dynamically fetched, can be a few hundred - every 3 hours,
   since it makes many real API calls and funding data only updates
   every 8h anyway)

Both keep their latest results in memory and are served as plain
server-rendered HTML - no JS framework, no build step, loads fast on a
free-tier server.

Binds to whatever port HidenCloud/Pterodactyl assigns via the SERVER_PORT
env var (falls back to 8080 if not set, for local testing).
"""
import sys
import threading
import time
import json as pyjson
from pathlib import Path
from datetime import datetime
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

from flask import Flask, request, redirect
from dotenv import load_dotenv

load_dotenv("/home/container/.env")

from src.execution.full_market_scanner import run_scan_cycle, CYCLE_SECONDS, ROUND_TRIP
from src.execution.backtest_engine import compute_backtest
from src.execution.multi_exchange_backtest import compute_multi_backtest, GENERIC_ROUND_TRIP_PCT
from src.execution.advanced_backtest import (
    compute_advanced_backtest, find_best_pair, scan_opportunity_matrix,
    get_full_common_coin_universe,
)

app = Flask(__name__)

LOW_LIQUIDITY_USD = 50_000

MULTI_BACKTEST_COINS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "UNI", "SUI", "TRX", "NEAR", "OP", "INJ", "RUNE",
    "SEI", "ARB", "APT", "TIA", "JUP", "WIF", "PEPE", "BNB", "ETC",
    "FIL", "HBAR", "ICP", "AAVE", "MKR", "LDO", "GALA", "SAND",
]

OPPORTUNITY_RESCAN_SECONDS = 3 * 60 * 60  # 3 hours between full scans

# ── SHARED STATE: live scanner (Delta vs Pi42) ─────────────────
_latest_rows = []
_last_scan_time = None
_scan_error = None


def background_scanner():
    global _latest_rows, _last_scan_time, _scan_error
    while True:
        try:
            _latest_rows = run_scan_cycle()
            _last_scan_time = datetime.now()
            _scan_error = None
        except Exception as e:
            _scan_error = str(e)
            print(f"[web] scan cycle failed: {e}")
        time.sleep(CYCLE_SECONDS)


# ── SHARED STATE: opportunity matrix (ALL coins, Bybit/OKX historical) ─
_opp_results = []
_opp_last_run = None
_opp_running = False
_opp_progress = (0, 0)
_opp_coin_universe = []
_opp_error = None


def run_full_opportunity_scan(days=14, position=1000):
    global _opp_results, _opp_last_run, _opp_running, _opp_progress, _opp_coin_universe, _opp_error
    if _opp_running:
        return
    _opp_running = True
    _opp_error = None
    try:
        if not _opp_coin_universe:
            print("[web] fetching full Bybit/OKX coin universe...")
            _opp_coin_universe = get_full_common_coin_universe()
            print(f"[web] found {len(_opp_coin_universe)} coins common to both exchanges")

        def progress_cb(done, total):
            global _opp_progress
            _opp_progress = (done, total)

        results = scan_opportunity_matrix(
            _opp_coin_universe, days, position, progress_callback=progress_cb
        )
        _opp_results = results
        _opp_last_run = datetime.now()
    except Exception as e:
        _opp_error = str(e)
        print(f"[web] opportunity scan failed: {e}")
    finally:
        _opp_running = False
        _opp_progress = (0, 0)


def opportunity_background():
    while True:
        run_full_opportunity_scan()
        time.sleep(OPPORTUNITY_RESCAN_SECONDS)


# ── SHARED PAGE STYLE ────────────────────────────────────────
BASE_CSS = """
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, system-ui, sans-serif; background:#0f1117;
         color:#e6e6e6; margin:0; padding:0 16px 40px; }
  header { display:flex; gap:20px; align-items:center; padding:16px 0;
           border-bottom:1px solid #2a2d38; margin-bottom:20px; flex-wrap:wrap; }
  header a { color:#e6e6e6; text-decoration:none; font-weight:600; opacity:.7; }
  header a.active { opacity:1; border-bottom:2px solid #4ade80; padding-bottom:14px; }
  h1 { font-size:20px; margin:0 20px 0 0; }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th, td { text-align:right; padding:8px 10px; border-bottom:1px solid #1f222c; }
  th { color:#8b8fa3; font-weight:600; position:sticky; top:0; background:#0f1117;
       cursor:pointer; user-select:none; }
  th:hover { color:#e6e6e6; }
  td:first-child, th:first-child { text-align:left; }
  .profit { color:#4ade80; font-weight:600; }
  .loss { color:#8b8fa3; }
  .near { color:#facc15; font-weight:600; }
  .thin { display:inline-block; background:#3f2d0e; color:#facc15; font-size:10px;
          padding:1px 5px; border-radius:4px; margin-left:6px; vertical-align:middle; }
  .meta { color:#8b8fa3; font-size:13px; margin-bottom:14px; }
  #search { width:100%; max-width:280px; margin-bottom:14px; }
  form { display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin-bottom:24px; }
  label { display:block; font-size:12px; color:#8b8fa3; margin-bottom:4px; }
  input, select { background:#1a1d27; border:1px solid #2a2d38; color:#e6e6e6;
                  padding:8px 10px; border-radius:6px; font-size:14px; }
  button { background:#4ade80; color:#0f1117; border:none; padding:9px 18px;
           border-radius:6px; font-weight:700; cursor:pointer; }
  button.secondary { background:#2a2d38; color:#e6e6e6; }
  .card { background:#1a1d27; border:1px solid #2a2d38; border-radius:10px;
          padding:18px; max-width:520px; }
  .card p { display:flex; justify-content:space-between; margin:8px 0;
            font-size:14px; border-bottom:1px dashed #2a2d38; padding-bottom:8px; }
  .card p span:first-child { color:#8b8fa3; }
  .note { color:#8b8fa3; font-size:12px; margin-top:16px; line-height:1.5; }
  a.coin-link { color:#e6e6e6; text-decoration:underline dotted; }
  .autocomplete-wrap { position:relative; }
  .autocomplete-list { position:absolute; top:100%; left:0; right:0; z-index:20;
                        background:#1a1d27; border:1px solid #2a2d38; border-top:none;
                        border-radius:0 0 6px 6px; max-height:220px; overflow-y:auto;
                        display:none; }
  .autocomplete-list div { padding:8px 10px; cursor:pointer; text-align:left; font-size:14px; }
  .autocomplete-list div:hover, .autocomplete-list div.active-item { background:#2a2d38; }
"""

NAV = """
<header>
  <h1>Funding Arb</h1>
  <a href="/" class="{scanner_active}">Scanner</a>
  <a href="/backtest" class="{backtest_active}">Backtest (ours)</a>
  <a href="/multi-backtest" class="{multi_active}">Backtest (multi-exchange)</a>
  <a href="/opportunities" class="{opp_active}">Opportunities</a>
</header>
"""

SCANNER_JS = """
<script>
function filterTable() {
  const q = document.getElementById('search').value.toLowerCase();
  const rows = document.querySelectorAll('#tbl tbody tr');
  rows.forEach(r => {
    const coin = r.dataset.coin || '';
    r.style.display = coin.includes(q) ? '' : 'none';
  });
}
function sortTable(colIndex, numeric) {
  const tbody = document.querySelector('#tbl tbody');
  const rows = Array.from(tbody.querySelectorAll('tr'));
  const asc = tbody.dataset.sortCol == colIndex && tbody.dataset.sortDir != 'asc';
  rows.sort((a, b) => {
    let av = a.children[colIndex].innerText.trim();
    let bv = b.children[colIndex].innerText.trim();
    if (numeric) { av = parseFloat(av.replace(/,/g,'')) || 0; bv = parseFloat(bv.replace(/,/g,'')) || 0; }
    if (av < bv) return asc ? -1 : 1;
    if (av > bv) return asc ? 1 : -1;
    return 0;
  });
  rows.forEach(r => tbody.appendChild(r));
  tbody.dataset.sortCol = colIndex;
  tbody.dataset.sortDir = asc ? 'asc' : 'desc';
}
</script>
"""

SCANNER_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Funding Arb Scanner</title>
<style>{css}</style></head><body>
{nav}
<div class="meta">{status_line}</div>
<input id="search" placeholder="Filter by coin..." onkeyup="filterTable()">
<table id="tbl">
<thead><tr>
  <th onclick="sortTable(0,false)">Coin</th>
  <th onclick="sortTable(1,true)">Delta %</th>
  <th onclick="sortTable(2,true)">Pi42 %</th>
  <th onclick="sortTable(3,true)">Gap pp</th>
  <th onclick="sortTable(4,true)">Net %</th>
  <th onclick="sortTable(5,true)">Delta Vol ($)</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
{js}
</body></html>
"""

BACKTEST_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Funding Arb Backtest</title>
<style>{css}</style></head><body>
{nav}
<form method="get" action="/backtest">
  <div><label>Coin</label><input name="coin" value="{coin}" style="width:90px"></div>
  <div><label>Days</label><input name="days" type="number" value="{days}" style="width:70px"></div>
  <div><label>Position ($)</label><input name="position" type="number" value="{position}" style="width:110px"></div>
  <button type="submit">Run backtest</button>
</form>
{result_html}
</body></html>
"""

MULTI_BACKTEST_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Multi-Exchange Backtest</title>
<style>{css}</style></head><body>
{nav}
<form method="get" action="/multi-backtest">
  <div><label>Long exchange</label>
    <select name="exchange_a">{exchange_a_options}</select></div>
  <div><label>Short exchange</label>
    <select name="exchange_b">{exchange_b_options}</select></div>
  <div class="autocomplete-wrap">
    <label>Coin (type to search)</label>
    <input id="coin-input" name="coin" value="{coin}" style="width:110px"
           placeholder="e.g. BTC" autocomplete="off"
           oninput="showCoinSuggestions()" onfocus="showCoinSuggestions()">
    <div id="coin-suggestions" class="autocomplete-list"></div>
  </div>
  <div><label>Days</label><input name="days" type="number" value="{days}" style="width:70px"></div>
  <div><label>Position ($)</label><input name="position" type="number" value="{position}" style="width:110px"></div>
  <button type="submit">Run backtest</button>
</form>
{result_html}
<div class="note">
  Uses real historical funding rate data from Binance, Bybit, and OKX -
  their own free, public APIs. Pi42 and Delta India are not included
  here since neither publishes historical funding data. Want to scan
  everything at once instead of one coin at a time? See the
  <a class="coin-link" href="/opportunities">Opportunities</a> page.
</div>
<script>
const ALL_COINS = {coins_json};
function showCoinSuggestions() {{
  const input = document.getElementById('coin-input');
  const box = document.getElementById('coin-suggestions');
  const q = input.value.toUpperCase();
  const matches = ALL_COINS.filter(c => c.startsWith(q)).slice(0, 8);
  if (matches.length === 0) {{ box.style.display = 'none'; return; }}
  box.innerHTML = matches.map(c => `<div onclick="selectCoin('${{c}}')">${{c}}</div>`).join('');
  box.style.display = 'block';
}}
function selectCoin(c) {{
  document.getElementById('coin-input').value = c;
  document.getElementById('coin-suggestions').style.display = 'none';
}}
document.addEventListener('click', function(e) {{
  if (!e.target.closest('.autocomplete-wrap')) {{
    document.getElementById('coin-suggestions').style.display = 'none';
  }}
}});
</script>
</body></html>
"""

OPPORTUNITIES_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Opportunity Matrix</title>
<style>{css}</style></head><body>
{nav}
<div class="note" style="margin-bottom:16px;">
  Continuously scans <b>every coin listed on both Bybit and OKX</b>
  ({universe_size} coins found) using real historical funding data,
  ranked by risk-adjusted return - not just a single APY number.
  <b>Max drawdown</b> is the worst peak-to-trough dip the spread ever
  had; <b>Sharpe-like</b> is mean gap / gap volatility (higher = steadier
  edge, near-zero = noisy/unreliable even if the average looks fine).
  Runs automatically every 3 hours in the background - this page just
  shows the latest cached results, so it loads instantly.
</div>
<div class="meta">{status_line}</div>
<form method="get" action="/opportunities">
  <div><label>Show top</label>
    <select name="top">{top_options}</select>
  </div>
  <button type="submit">Apply</button>
  <a href="/opportunities/rescan"><button type="button" class="secondary"
     onclick="this.form" style="margin-left:4px;">Rescan now</button></a>
</form>
{result_html}
</body></html>
"""


def render_scanner_page():
    if _scan_error:
        status = f'<span style="color:#f87171">Last scan failed: {_scan_error}</span>'
    elif _last_scan_time is None:
        status = "First scan starting up... refresh in a few seconds."
    else:
        age = (datetime.now() - _last_scan_time).total_seconds()
        profitable_count = sum(1 for r in _latest_rows if r["profitable"])
        status = (
            f"Last scan: {_last_scan_time.strftime('%H:%M:%S')} "
            f"({int(age)}s ago) \u00b7 {len(_latest_rows)} coins checked \u00b7 "
            f"{profitable_count} profitable \u00b7 fee floor {ROUND_TRIP*100:.4f}% \u00b7 "
            f"type to filter, click headers to sort"
        )

    rows_html = ""
    for r in _latest_rows:
        if r["profitable"]:
            cls = "profit"
        elif r["net_pct"] > -0.05:
            cls = "near"
        else:
            cls = "loss"
        thin_badge = '<span class="thin">thin</span>' if r["delta_volume_usd"] < LOW_LIQUIDITY_USD else ""
        rows_html += (
            f"<tr data-coin='{r['coin'].lower()}'>"
            f"<td><a class='coin-link' href='/backtest?coin={r['coin']}'>{r['coin']}</a>{thin_badge}</td>"
            f"<td>{r['delta_funding_pct']:.5f}</td>"
            f"<td>{r['pi42_funding_pct']:.5f}</td>"
            f"<td>{r['gap_pct']:.5f}</td>"
            f"<td class='{cls}'>{r['net_pct']:+.5f}</td>"
            f"<td>{r['delta_volume_usd']:,.0f}</td></tr>"
        )
    if not rows_html:
        rows_html = "<tr><td colspan='6'>No data yet.</td></tr>"

    nav = NAV.format(scanner_active="active", backtest_active="", multi_active="", opp_active="")
    return SCANNER_PAGE.format(css=BASE_CSS, nav=nav, status_line=status, rows=rows_html, js=SCANNER_JS)


def render_backtest_page():
    coin = request.args.get("coin", "BTC").upper()
    days = int(request.args.get("days", 7))
    position = float(request.args.get("position", 1000))
    result = compute_backtest(coin, days, position)

    if not result["enough_data"]:
        result_html = (
            f'<div class="card"><p><span>Status</span>'
            f'<span>Not enough data yet ({result["data_points"]} point(s))</span></p>'
            f'<div class="note">Backtests only cover time since the scanner started '
            f'logging - there is no historical funding data available before that '
            f'for Pi42. Come back after this has been running a few days, or try '
            f'<a class="coin-link" href="/opportunities">Opportunities</a> for real '
            f'historical data on Binance/Bybit/OKX instead.</div></div>'
        )
    else:
        result_html = f"""
        <div class="card">
          <p><span>Data points</span><span>{result['data_points']}</span></p>
          <p><span>Period covered</span><span>{result['days_covered']} days</span></p>
          <p><span>Time profitable</span><span>{result['time_in_position_hours']}h ({result['pct_time_profitable']}%)</span></p>
          <p><span>Simulated return</span><span>{result['total_return_pct']:+.4f}%</span></p>
          <p><span>P&amp;L on ${result['position_usd']:,.0f}</span><span>${result['position_pnl_usd']:+,.2f}</span></p>
          <p><span>Annualized (APY)</span><span>{result['apy_pct']:+.2f}%</span></p>
          <div class="note">Assumes fees paid once at entry, not on every re-entry after
          a flip out of profitability. Treat as an optimistic upper bound, not a
          guarantee.</div>
        </div>
        """

    nav = NAV.format(scanner_active="", backtest_active="active", multi_active="", opp_active="")
    return BACKTEST_PAGE.format(css=BASE_CSS, nav=nav, coin=coin, days=days,
                                 position=int(position), result_html=result_html)


def render_multi_backtest_page():
    exchanges = ["binance", "bybit", "okx"]
    exchange_a = request.args.get("exchange_a", "bybit")
    exchange_b = request.args.get("exchange_b", "okx")
    coin = request.args.get("coin", "BTC").upper()
    days = int(request.args.get("days", 30))
    position = float(request.args.get("position", 1000))
    result = compute_multi_backtest(exchange_a, exchange_b, coin, days, position)

    if not result["enough_data"]:
        result_html = (
            f'<div class="card"><p><span>Status</span>'
            f'<span>No matched data found</span></p>'
            f'<div class="note">Got {result.get("points_a", 0)} points from '
            f'{exchange_a} and {result.get("points_b", 0)} from {exchange_b}. '
            f'This coin may not be listed as a perpetual on one of these exchanges. '
            f'Note: Binance has been unreliable from this server (connection '
            f'timeouts) - try Bybit vs OKX instead.</div></div>'
        )
    else:
        result_html = f"""
        <div class="card">
          <p><span>Matched funding events</span><span>{result['matched_points']}</span></p>
          <p><span>Period covered</span><span>{result['period_days']} days</span></p>
          <p><span>Events profitable</span><span>{result['profitable_events']} ({result['pct_events_profitable']}%)</span></p>
          <p><span>Total gap collected</span><span>{result['total_gap_pct']:+.4f}%</span></p>
          <p><span>Round-trip fee used</span><span>{result['round_trip_pct']:.4f}%</span></p>
          <p><span>Simulated return</span><span>{result['total_return_pct']:+.4f}%</span></p>
          <p><span>P&amp;L on ${result['position_usd']:,.0f}</span><span>${result['position_pnl_usd']:+,.2f}</span></p>
          <p><span>Annualized (APY)</span><span>{result['apy_pct']:+.2f}%</span></p>
          <div class="note">Fee assumption is generic ({GENERIC_ROUND_TRIP_PCT:.2f}% round trip) -
          real fees depend on your account tier on each exchange.</div>
        </div>
        """

    def exchange_options(selected):
        return "".join(
            f'<option value="{e}" {"selected" if e == selected else ""}>{e.capitalize()}</option>'
            for e in exchanges
        )

    nav = NAV.format(scanner_active="", backtest_active="", multi_active="active", opp_active="")
    return MULTI_BACKTEST_PAGE.format(
        css=BASE_CSS, nav=nav,
        exchange_a_options=exchange_options(exchange_a),
        exchange_b_options=exchange_options(exchange_b),
        coin=coin, coins_json=pyjson.dumps(MULTI_BACKTEST_COINS),
        days=days, position=int(position), result_html=result_html,
    )


def render_opportunities_page():
    top_n = int(request.args.get("top", 10))

    if _opp_running:
        done, total = _opp_progress
        status = f'<span style="color:#facc15">Scan in progress: {done}/{total} coins checked...</span>'
    elif _opp_error:
        status = f'<span style="color:#f87171">Last scan failed: {_opp_error}</span>'
    elif _opp_last_run is None:
        status = "First scan starting up in the background - this can take a few minutes for the full coin universe. Refresh shortly."
    else:
        age_min = (datetime.now() - _opp_last_run).total_seconds() / 60
        status = (
            f"Last full scan: {_opp_last_run.strftime('%H:%M:%S')} "
            f"({age_min:.0f} min ago) \u00b7 {len(_opp_results)} results with enough data \u00b7 "
            f"rescans automatically every {OPPORTUNITY_RESCAN_SECONDS // 3600}h"
        )

    if _opp_results:
        rows_html = ""
        for r in _opp_results[:top_n]:
            apy_cls = "profit" if r["apy_pct"] > 0 else "loss"
            rows_html += (
                f"<tr>"
                f"<td>{r['coin']}</td>"
                f"<td>{r['exchange_a']}/{r['exchange_b']}</td>"
                f"<td>{r['matched_points']}</td>"
                f"<td class='{apy_cls}'>{r['apy_pct']:+.2f}%</td>"
                f"<td>{r['max_drawdown_pct']:.3f}%</td>"
                f"<td>{r['sharpe_like']:.2f}</td>"
                f"<td>{r['total_return_pct']:+.4f}%</td>"
                f"</tr>"
            )
        result_html = f"""
        <table>
        <thead><tr>
          <th>Coin</th><th>Pair</th><th>Events</th><th>APY</th>
          <th>Max DD</th><th>Sharpe-like</th><th>Return</th>
        </tr></thead>
        <tbody>{rows_html}</tbody>
        </table>
        <div class="note">Showing top {min(top_n, len(_opp_results))} of {len(_opp_results)} results,
        ranked by APY (highest opportunity first). A high APY with a deep max
        drawdown or low Sharpe-like value means the edge was choppy/inconsistent
        even if the headline number looks good - prefer steady results over lucky
        spikes.</div>
        """
    else:
        result_html = '<div class="note">No results yet - the background scan is still running or hasn\'t started.</div>'

    def top_options():
        return "".join(
            f'<option value="{n}" {"selected" if n == top_n else ""}>{n}</option>'
            for n in [10, 25, 50]
        )

    nav = NAV.format(scanner_active="", backtest_active="", multi_active="", opp_active="active")
    return OPPORTUNITIES_PAGE.format(
        css=BASE_CSS, nav=nav, universe_size=len(_opp_coin_universe),
        status_line=status, top_options=top_options(), result_html=result_html,
    )


@app.route("/")
def scanner_route():
    return render_scanner_page()


@app.route("/backtest")
def backtest_route():
    return render_backtest_page()


@app.route("/multi-backtest")
def multi_backtest_route():
    return render_multi_backtest_page()


@app.route("/opportunities")
def opportunities_route():
    return render_opportunities_page()


@app.route("/opportunities/rescan")
def opportunities_rescan_route():
    if not _opp_running:
        threading.Thread(target=run_full_opportunity_scan, daemon=True).start()
    return redirect("/opportunities")


if __name__ == "__main__":
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=opportunity_background, daemon=True).start()

    port = int(os.getenv("SERVER_PORT", 8080))
    print(f"Starting web dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)

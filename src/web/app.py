"""
Minimal web dashboard - 8 pages: live scanner, Indian exchanges overview,
Indian opportunities (Delta+Pi42+CoinSwitch), Indian backtest, 3-way
scanner (+ CoinSwitch), Delta/Pi42 backtest (our own logged data),
multi-exchange historical backtest, and an automated opportunity matrix
scanner across ALL available coins x exchange pairs.

Runs FOUR background loops, decoupled from each other:
1. Live full-market scanner (Delta vs Pi42, 133 coins, every ~90s)
2. Live 3-way scanner (Delta + Pi42 + CoinSwitch, every ~90s)
3. INGESTION: pulls historical data from Bybit/OKX into our own local
   database, in parallel (ThreadPoolExecutor), every 30 min.
4. RANKING: reads from that local database (fast, no network) and
   recomputes the ranked opportunity list every 5 min.

2026-07-26 update: "Indian Opportunities" now reads from the 3-way
scanner's data (Delta+Pi42+CoinSwitch) instead of the old Delta/Pi42-only
feed, and a new "Indian Backtest" page reads from the 3-way scanner's own
CSV logs - both were pointed at the wrong/incomplete data source before.

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
from src.execution.three_way_scanner import (
    run_scan_cycle as run_three_way_scan_cycle,
    CYCLE_SECONDS as THREE_WAY_CYCLE_SECONDS,
    ROUND_TRIP as THREE_WAY_ROUND_TRIP,
)
from src.execution.backtest_engine import compute_backtest
from src.execution.indian_backtest import compute_indian_backtest
from src.execution.multi_exchange_backtest import compute_multi_backtest, GENERIC_ROUND_TRIP_PCT
from src.execution.advanced_backtest import (
    compute_advanced_backtest, find_best_pair, scan_opportunity_matrix,
    get_full_common_coin_universe,
)
from src.data.ingest_funding import ingest_all
from src.data.funding_history_store import init_store, get_store_stats

app = Flask(__name__)

LOW_LIQUIDITY_USD = 50_000

MULTI_BACKTEST_COINS = [
    "BTC", "ETH", "SOL", "XRP", "DOGE", "ADA", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "UNI", "SUI", "TRX", "NEAR", "OP", "INJ", "RUNE",
    "SEI", "ARB", "APT", "TIA", "JUP", "WIF", "PEPE", "BNB", "ETC",
    "FIL", "HBAR", "ICP", "AAVE", "MKR", "LDO", "GALA", "SAND",
]

INGEST_INTERVAL_SECONDS = 30 * 60
RANK_INTERVAL_SECONDS   = 5 * 60
BACKTEST_DAYS_DEFAULT   = 14
POSITION_DEFAULT        = 1000

INDIAN_EXCHANGE_REGISTRY = [
    {
        "name": "Delta Exchange India", "type": "Derivatives",
        "inr_deposit": "UPI \u00b7 IMPS \u00b7 NEFT", "futures": True, "pairs": "50+",
        "funding_interval": "8 hrs", "api_status": "full", "integration": "live",
        "api_docs": "https://docs.delta.exchange", "fiu_registered": True,
        "notes": "FIU-registered. INR-settled USD pairs.",
    },
    {
        "name": "Pi42", "type": "Derivatives",
        "inr_deposit": "UPI", "futures": True, "pairs": "700+",
        "funding_interval": "4\u20138 hrs", "api_status": "full", "integration": "live",
        "api_docs": "https://docs.pi42.com", "fiu_registered": True,
        "notes": "FIU-registered. INR-native margin. Claims no TDS/VDA tax.",
    },
    {
        "name": "CoinSwitch PRO", "type": "Derivatives",
        "inr_deposit": "UPI \u00b7 IMPS \u00b7 NEFT", "futures": True, "pairs": "650+",
        "funding_interval": "8 hrs", "api_status": "full", "integration": "live",
        "api_docs": "https://api-trading.coinswitch.co", "fiu_registered": True,
        "notes": "FIU-registered. Ed25519-signed API, confirmed working 2026-07-26.",
    },
]

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


# ── SHARED STATE: 3-way scanner (Delta + Pi42 + CoinSwitch) ─────
_three_way_rows = []
_three_way_last_scan_time = None
_three_way_error = None


def background_three_way_scanner():
    global _three_way_rows, _three_way_last_scan_time, _three_way_error
    while True:
        try:
            _three_way_rows = run_three_way_scan_cycle()
            _three_way_last_scan_time = datetime.now()
            _three_way_error = None
        except Exception as e:
            _three_way_error = str(e)
            print(f"[web] 3-way scan cycle failed: {e}")
        time.sleep(THREE_WAY_CYCLE_SECONDS)


# ── SHARED STATE: opportunity matrix (ingestion + ranking, decoupled) ──
_opp_coin_universe = []
_ingest_running = False
_ingest_progress = (0, 0)
_ingest_last_run = None
_ingest_error = None
_opp_results = []
_opp_last_run = None
_opp_running = False
_opp_error = None


def ingestion_background():
    global _opp_coin_universe, _ingest_running, _ingest_progress, _ingest_last_run, _ingest_error
    init_store()
    while True:
        _ingest_running = True
        _ingest_error = None
        try:
            if not _opp_coin_universe:
                print("[web] fetching full Bybit/OKX coin universe...")
                _opp_coin_universe = get_full_common_coin_universe()
                print(f"[web] found {len(_opp_coin_universe)} coins common to both exchanges")

            def progress_cb(done, total):
                global _ingest_progress
                _ingest_progress = (done, total)

            ingest_all(_opp_coin_universe, days=30, progress_callback=progress_cb)
            _ingest_last_run = datetime.now()
        except Exception as e:
            _ingest_error = str(e)
            print(f"[web] ingestion failed: {e}")
        finally:
            _ingest_running = False
            _ingest_progress = (0, 0)
        time.sleep(INGEST_INTERVAL_SECONDS)


def run_ranking_pass():
    global _opp_results, _opp_last_run, _opp_running, _opp_error
    if _opp_running or not _opp_coin_universe:
        return
    _opp_running = True
    _opp_error = None
    try:
        results = scan_opportunity_matrix(
            _opp_coin_universe, BACKTEST_DAYS_DEFAULT, POSITION_DEFAULT
        )
        _opp_results = results
        _opp_last_run = datetime.now()
    except Exception as e:
        _opp_error = str(e)
        print(f"[web] ranking pass failed: {e}")
    finally:
        _opp_running = False


def ranking_background():
    while True:
        run_ranking_pass()
        time.sleep(RANK_INTERVAL_SECONDS)


# ── SHARED PAGE STYLE ────────────────────────────────────────
BASE_CSS = """
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, system-ui, sans-serif; background:#0f1117;
         color:#e6e6e6; margin:0; padding:0 16px 40px; }
  header { display:flex; gap:18px; align-items:center; padding:16px 0;
           border-bottom:1px solid #2a2d38; margin-bottom:20px; flex-wrap:wrap; }
  header a { color:#e6e6e6; text-decoration:none; font-weight:600; opacity:.7; font-size:14px; }
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
  .stat-cards { display:flex; gap:14px; flex-wrap:wrap; margin-bottom:24px; }
  .stat-card { background:#1a1d27; border:1px solid #2a2d38; border-radius:10px;
               padding:16px 20px; min-width:180px; }
  .stat-label { font-size:11px; color:#8b8fa3; font-weight:600; letter-spacing:.06em; margin-bottom:6px; }
  .stat-value { font-size:26px; font-weight:700; }
  .stat-sub { font-size:11px; color:#8b8fa3; margin-top:4px; }
  .badge-live { display:inline-block; background:#14532d; color:#4ade80;
                font-size:11px; font-weight:700; padding:2px 8px; border-radius:4px; }
  .badge-soon { display:inline-block; background:#422006; color:#facc15;
                font-size:11px; font-weight:700; padding:2px 8px; border-radius:4px; }
  .badge-no { display:inline-block; background:#2d1515; color:#f87171;
              font-size:11px; padding:2px 8px; border-radius:4px; }
  .badge-na { display:inline-block; background:#1f222c; color:#8b8fa3;
              font-size:11px; padding:2px 8px; border-radius:4px; }
  .section-title { font-size:13px; color:#8b8fa3; font-weight:600;
                   letter-spacing:.08em; margin:28px 0 12px; }
"""

NAV = """
<header>
  <h1>Funding Arb</h1>
  <a href="/" class="{scanner_active}">Scanner</a>
  <a href="/indian-exchanges" class="{indian_active}">\U0001f1ee\U0001f1f3 Exchanges</a>
  <a href="/indian-opportunities" class="{indiaopp_active}">\U0001f1ee\U0001f1f3 Opportunities</a>
  <a href="/indian-backtest" class="{indiabt_active}">\U0001f1ee\U0001f1f3 Backtest</a>
  <a href="/backtest" class="{backtest_active}">Backtest (2-way)</a>
  <a href="/multi-backtest" class="{multi_active}">Backtest (global)</a>
  <a href="/opportunities" class="{opp_active}">Opportunities (global)</a>
</header>
"""

def nav_html(active):
    keys = ["scanner", "indian", "indiaopp", "indiabt", "backtest", "multi", "opp"]
    return NAV.format(**{f"{k}_active": ("active" if k == active else "") for k in keys})

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
  their own free, public APIs.
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
  Scans every real cryptocurrency listed on both Bybit and OKX
  ({universe_size} coins found) using real historical funding data.
</div>
<div class="meta">{ingest_status}</div>
<div class="meta">{rank_status}</div>
<form method="get" action="/opportunities">
  <div><label>Show top</label>
    <select name="top">{top_options}</select>
  </div>
  <button type="submit">Apply</button>
  <a href="/opportunities/rescan"><button type="button" class="secondary"
     style="margin-left:4px;">Re-rank now</button></a>
</form>
{result_html}
</body></html>
"""

INDIAN_EXCHANGES_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Indian Exchanges \u2014 Funding Arb</title>
<style>{css}</style></head><body>
{nav}
<h2 style="font-size:18px;margin:0 0 6px;">\U0001f1ee\U0001f1f3 Indian Crypto Exchanges</h2>
<p class="meta">Connected exchanges only. Live rates updated: {scan_time}</p>
<div class="section-title">LIVE BTC FUNDING RATES</div>
<div class="stat-cards">
  <div class="stat-card">
    <div class="stat-label">DELTA EXCHANGE INDIA \u00b7 BTCUSD</div>
    <div class="stat-value" style="color:#4ade80">{delta_btc_rate}</div>
    <div class="stat-sub">per 8-hour funding period</div>
    <div style="margin-top:10px"><span class="badge-live">\U0001f7e2 LIVE</span></div>
  </div>
  <div class="stat-card">
    <div class="stat-label">PI42 \u00b7 BTCUSDT</div>
    <div class="stat-value" style="color:#4ade80">{pi42_btc_rate}</div>
    <div class="stat-sub">per funding period (4\u20138 hrs)</div>
    <div style="margin-top:10px"><span class="badge-live">\U0001f7e2 LIVE</span></div>
  </div>
  <div class="stat-card">
    <div class="stat-label">COINSWITCH PRO \u00b7 BTCUSDT</div>
    <div class="stat-value" style="color:#4ade80">{coinswitch_btc_rate}</div>
    <div class="stat-sub">per 8-hour funding period</div>
    <div style="margin-top:10px"><span class="badge-live">\U0001f7e2 LIVE</span></div>
  </div>
</div>
<div class="section-title">CONNECTED EXCHANGES</div>
<div style="overflow-x:auto">
<table>
<thead><tr>
  <th>Exchange</th><th>Type</th><th>INR Deposit</th>
  <th style="text-align:center">Perp Futures</th><th>Pairs</th>
  <th>Funding Interval</th><th>API</th>
  <th style="text-align:center">FIU Reg.</th><th>Status</th><th>Notes</th>
</tr></thead>
<tbody>{exchange_rows}</tbody>
</table>
</div>
<div class="note" style="margin-top:24px;max-width:640px">
  Funding rate arbitrage needs perpetual futures, a public funding-rate API, and reliable
  data access. Delta Exchange India, Pi42, and CoinSwitch PRO meet all three today.
  CoinDCX and Mudrex both have real futures products but their exact live funding-rate
  endpoints haven't been independently verified yet - they'll be added once confirmed,
  same standard applied to every exchange here.
</div>
</body></html>
"""

INDIAN_OPPORTUNITIES_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Indian Opportunities \u2014 Funding Arb</title>
<style>{css}</style></head><body>
{nav}
<h2 style="font-size:18px;margin:0 0 6px;">\U0001f1ee\U0001f1f3 Indian Exchange Opportunities</h2>
<p class="meta">
  Best fee-adjusted pair across Delta, Pi42, and CoinSwitch for every coin.
  Last scan: {scan_time} \u00b7 {total_coins} coins monitored \u00b7 each pair uses its own real fee total
  (Delta-Pi42, Delta-CoinSwitch, Pi42-CoinSwitch all differ slightly)
</p>
<div class="stat-cards">
  <div class="stat-card" style="border-color:{profitable_border}">
    <div class="stat-label">PROFITABLE NOW</div>
    <div class="stat-value" style="color:{profitable_color}">{profitable_count}</div>
    <div class="stat-sub">coins above fee threshold</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">BEST COIN</div>
    <div class="stat-value" style="color:#4ade80">{best_coin}</div>
    <div class="stat-sub">net {best_net}% via {best_pair}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">NEAR MISS</div>
    <div class="stat-value" style="color:#facc15">{near_count}</div>
    <div class="stat-sub">coins within 0.05% of profitability</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">TOTAL SCANNED</div>
    <div class="stat-value">{total_coins}</div>
    <div class="stat-sub">coins on 2+ exchanges</div>
  </div>
</div>
{profitable_section}
{near_section}
<div class="note" style="margin-top:24px;max-width:680px">
  <b>How this works:</b> for each coin, all 3 possible pairs (Delta-Pi42, Delta-CoinSwitch,
  Pi42-CoinSwitch) are checked and whichever has the best net% after ITS OWN real fees wins.
  Click a coin to see its historical backtest. Page auto-refreshes every 90 seconds.
</div>
<script>setTimeout(()=>location.reload(), 92000);</script>
</body></html>
"""

INDIAN_BACKTEST_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Indian Backtest \u2014 Funding Arb</title>
<style>{css}</style></head><body>
{nav}
<h2 style="font-size:18px;margin:0 0 6px;">\U0001f1ee\U0001f1f3 Indian Exchange Backtest</h2>
<p class="note" style="margin-bottom:20px;max-width:640px">
  Simulates historical return using our own logged data from the 3-way scanner
  (Delta + Pi42 + CoinSwitch). None of these three exchanges publish historical
  funding data anywhere - this can only cover time since the scanner has actually
  been running, same honest limitation as the global backtest page.
</p>
<form method="get" action="/indian-backtest">
  <div><label>Coin</label><input name="coin" value="{coin}" style="width:90px"></div>
  <div><label>Days</label><input name="days" type="number" value="{days}" style="width:70px"></div>
  <div><label>Position ($)</label><input name="position" type="number" value="{position}" style="width:110px"></div>
  <button type="submit">Run backtest</button>
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
            f"{profitable_count} profitable \u00b7 fee floor {ROUND_TRIP*100:.4f}%"
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

    return SCANNER_PAGE.format(css=BASE_CSS, nav=nav_html("scanner"), status_line=status, rows=rows_html, js=SCANNER_JS)


def render_backtest_page():
    coin = request.args.get("coin", "BTC").upper()
    days = int(request.args.get("days", 7))
    position = float(request.args.get("position", 1000))
    result = compute_backtest(coin, days, position)

    if not result["enough_data"]:
        result_html = (
            f'<div class="card"><p><span>Status</span>'
            f'<span>Not enough data yet ({result["data_points"]} point(s))</span></p>'
            f'<div class="note">Come back after this has run a few days, or try '
            f'<a class="coin-link" href="/opportunities">Opportunities (global)</a> for real '
            f'historical data instead.</div></div>'
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
          <div class="note">Assumes fees paid once at entry, not on every re-entry.</div>
        </div>
        """

    return BACKTEST_PAGE.format(css=BASE_CSS, nav=nav_html("backtest"), coin=coin, days=days,
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
            f'Note: Binance has been unreliable from this server - try Bybit vs OKX instead.</div></div>'
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
          <div class="note">Fee assumption is generic ({GENERIC_ROUND_TRIP_PCT:.2f}% round trip).</div>
        </div>
        """

    def exchange_options(selected):
        return "".join(
            f'<option value="{e}" {"selected" if e == selected else ""}>{e.capitalize()}</option>'
            for e in exchanges
        )

    return MULTI_BACKTEST_PAGE.format(
        css=BASE_CSS, nav=nav_html("multi"),
        exchange_a_options=exchange_options(exchange_a),
        exchange_b_options=exchange_options(exchange_b),
        coin=coin, coins_json=pyjson.dumps(MULTI_BACKTEST_COINS),
        days=days, position=int(position), result_html=result_html,
    )


def render_opportunities_page():
    top_n = int(request.args.get("top", 10))

    if _ingest_running:
        done, total = _ingest_progress
        ingest_status = f'<span style="color:#facc15">Ingesting: {done}/{total} pairs...</span>'
    elif _ingest_error:
        ingest_status = f'<span style="color:#f87171">Last ingestion failed: {_ingest_error}</span>'
    elif _ingest_last_run is None:
        ingest_status = "Ingestion starting up..."
    else:
        age_min = (datetime.now() - _ingest_last_run).total_seconds() / 60
        stats = get_store_stats()
        ingest_status = (
            f"Data ingestion: last run {age_min:.0f} min ago \u00b7 "
            f"{stats['total_rows']} records stored"
        )

    if _opp_running:
        rank_status = '<span style="color:#facc15">Ranking...</span>'
    elif _opp_error:
        rank_status = f'<span style="color:#f87171">Last ranking pass failed: {_opp_error}</span>'
    elif _opp_last_run is None:
        rank_status = "Waiting for first ingestion pass..."
    else:
        age_min = (datetime.now() - _opp_last_run).total_seconds() / 60
        rank_status = f"Ranking: last run {age_min:.0f} min ago \u00b7 {len(_opp_results)} results"

    if _opp_results:
        rows_html = ""
        for r in _opp_results[:top_n]:
            apy_cls = "profit" if r["apy_pct"] > 0 else "loss"
            rows_html += (
                f"<tr><td>{r['coin']}</td><td>{r['exchange_a']}/{r['exchange_b']}</td>"
                f"<td>{r['matched_points']}</td><td class='{apy_cls}'>{r['apy_pct']:+.2f}%</td>"
                f"<td>{r['max_drawdown_pct']:.3f}%</td><td>{r['sharpe_like']:.2f}</td>"
                f"<td>{r['total_return_pct']:+.4f}%</td></tr>"
            )
        result_html = f"""
        <table><thead><tr>
          <th>Coin</th><th>Pair</th><th>Events</th><th>APY</th>
          <th>Max DD</th><th>Sharpe-like</th><th>Return</th>
        </tr></thead><tbody>{rows_html}</tbody></table>
        """
    else:
        result_html = '<div class="note">No results yet.</div>'

    def top_options():
        return "".join(f'<option value="{n}" {"selected" if n == top_n else ""}>{n}</option>' for n in [10, 25, 50])

    return OPPORTUNITIES_PAGE.format(
        css=BASE_CSS, nav=nav_html("opp"), universe_size=len(_opp_coin_universe),
        ingest_status=ingest_status, rank_status=rank_status,
        top_options=top_options(), result_html=result_html,
    )


def render_indian_exchanges_page():
    btc_row = next((r for r in _three_way_rows if r["coin"] == "BTC"), None)
    if btc_row:
        delta_btc_rate = f"{btc_row['delta_funding_pct']}%" if btc_row['delta_funding_pct'] != "" else "N/A"
        pi42_btc_rate = f"{btc_row['pi42_funding_pct']}%" if btc_row['pi42_funding_pct'] != "" else "N/A"
        coinswitch_btc_rate = f"{btc_row['coinswitch_funding_pct']}%" if btc_row['coinswitch_funding_pct'] != "" else "N/A"
    else:
        delta_btc_rate = pi42_btc_rate = coinswitch_btc_rate = "Loading..."

    scan_time = _three_way_last_scan_time.strftime("%H:%M:%S") if _three_way_last_scan_time else "Starting..."

    api_map = {"full": '<span class="badge-live">\u2705 Full</span>'}
    int_map = {"live": '<span class="badge-live">\U0001f7e2 Live</span>'}

    rows_html = ""
    for exc in INDIAN_EXCHANGE_REGISTRY:
        docs = f'<a href="{exc["api_docs"]}" target="_blank" style="color:#8b8fa3;font-size:11px">{exc["name"]} docs \u2197</a>'
        futures_str = "\u2705" if exc["futures"] else "\u274c"
        fiu_str = "\u2705" if exc["fiu_registered"] else "\u274c"
        rows_html += (
            f"<tr><td>{docs}</td><td>{exc['type']}</td><td>{exc['inr_deposit']}</td>"
            f"<td style='text-align:center'>{futures_str}</td><td>{exc.get('pairs','\u2014')}</td>"
            f"<td>{exc.get('funding_interval','N/A')}</td><td>{api_map.get(exc['api_status'],'')}</td>"
            f"<td style='text-align:center'>{fiu_str}</td><td>{int_map.get(exc['integration'],'')}</td>"
            f"<td style='font-size:12px;color:#8b8fa3'>{exc['notes']}</td></tr>"
        )

    return INDIAN_EXCHANGES_PAGE.format(
        css=BASE_CSS, nav=nav_html("indian"),
        delta_btc_rate=delta_btc_rate, pi42_btc_rate=pi42_btc_rate,
        coinswitch_btc_rate=coinswitch_btc_rate,
        scan_time=scan_time, exchange_rows=rows_html,
    )


def render_indian_opportunities_page():
    """FIXED 2026-07-26: now reads from _three_way_rows (Delta+Pi42+
    CoinSwitch) instead of _latest_rows (Delta+Pi42 only) - this page was
    pointed at the wrong data source before, so CoinSwitch never actually
    showed up here despite the 3-way scanner already running."""
    scan_time = _three_way_last_scan_time.strftime("%H:%M:%S") if _three_way_last_scan_time else "Starting..."
    total_coins = len(_three_way_rows)

    profitable = [r for r in _three_way_rows if r["profitable"]]
    near = [r for r in _three_way_rows if not r["profitable"] and r["net_pct"] > -0.05]

    profitable_count = len(profitable)
    near_count = len(near)
    profitable_color = "#4ade80" if profitable_count > 0 else "#8b8fa3"
    profitable_border = "#166534" if profitable_count > 0 else "#2a2d38"

    if profitable:
        best = max(profitable, key=lambda r: r["net_pct"])
        best_coin = best["coin"]
        best_net = f"{best['net_pct']:+.5f}"
        best_pair = best["best_pair"]
    else:
        best_coin, best_net, best_pair = "None", "0.00000", "\u2014"

    def rate_or_na(v):
        return f"{v}%" if v != "" else "N/A"

    if profitable:
        p_rows = ""
        for r in sorted(profitable, key=lambda r: r["net_pct"], reverse=True):
            apy_est = r["net_pct"] * 3 * 365  # rough - assumes 8h funding interval
            p_rows += (
                f"<tr><td><a class='coin-link' href='/indian-backtest?coin={r['coin']}'>{r['coin']}</a></td>"
                f"<td style='text-align:left'>{r['best_pair']}</td>"
                f"<td>{rate_or_na(r['delta_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['pi42_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['coinswitch_funding_pct'])}</td>"
                f"<td>{r['gap_pct']:.5f}%</td>"
                f"<td class='profit'>{r['net_pct']:+.5f}%</td>"
                f"<td class='profit'>{apy_est:+.1f}%</td></tr>"
            )
        profitable_section = f"""
        <div class="section-title">\U0001f7e2 PROFITABLE OPPORTUNITIES ({profitable_count})</div>
        <table><thead><tr>
          <th>Coin</th><th>Best Pair</th><th>Delta %</th><th>Pi42 %</th><th>CoinSwitch %</th>
          <th>Gap %</th><th>Net %</th><th>Est. APY</th>
        </tr></thead><tbody>{p_rows}</tbody></table>
        """
    else:
        profitable_section = (
            '<div class="section-title">\U0001f7e2 PROFITABLE OPPORTUNITIES</div>'
            '<div class="note" style="background:#1a1d27;border:1px solid #2a2d38;'
            'border-radius:8px;padding:16px;margin-bottom:20px;">'
            'No profitable opportunities right now across any of the 3 pairs. '
            'Near misses shown below.</div>'
        )

    if near:
        n_rows = ""
        for r in sorted(near, key=lambda r: r["net_pct"], reverse=True)[:15]:
            n_rows += (
                f"<tr><td><a class='coin-link' href='/indian-backtest?coin={r['coin']}'>{r['coin']}</a></td>"
                f"<td style='text-align:left'>{r['best_pair']}</td>"
                f"<td>{rate_or_na(r['delta_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['pi42_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['coinswitch_funding_pct'])}</td>"
                f"<td>{r['gap_pct']:.5f}%</td>"
                f"<td class='near'>{r['net_pct']:+.5f}%</td></tr>"
            )
        near_section = f"""
        <div class="section-title" style="margin-top:28px">\U0001f7e1 NEAR MISSES ({near_count})</div>
        <table><thead><tr>
          <th>Coin</th><th>Best Pair</th><th>Delta %</th><th>Pi42 %</th><th>CoinSwitch %</th>
          <th>Gap %</th><th>Net %</th>
        </tr></thead><tbody>{n_rows}</tbody></table>
        """
    else:
        near_section = ""

    return INDIAN_OPPORTUNITIES_PAGE.format(
        css=BASE_CSS, nav=nav_html("indiaopp"), scan_time=scan_time, total_coins=total_coins,
        profitable_count=profitable_count, profitable_color=profitable_color,
        profitable_border=profitable_border, best_coin=best_coin, best_net=best_net,
        best_pair=best_pair, near_count=near_count,
        profitable_section=profitable_section, near_section=near_section,
    )


def render_indian_backtest_page():
    coin = request.args.get("coin", "BTC").upper()
    days = int(request.args.get("days", 7))
    position = float(request.args.get("position", 1000))
    result = compute_indian_backtest(coin, days, position)

    if not result["enough_data"]:
        result_html = (
            f'<div class="card"><p><span>Status</span>'
            f'<span>Not enough data yet ({result["data_points"]} point(s))</span></p>'
            f'<div class="note">The 3-way scanner needs to run longer to build up '
            f'history for this coin. Check the <a class="coin-link" '
            f'href="/indian-opportunities">Indian Opportunities</a> page for what\u2019s '
            f'happening live right now instead.</div></div>'
        )
    else:
        result_html = f"""
        <div class="card">
          <p><span>Data points</span><span>{result['data_points']}</span></p>
          <p><span>Period covered</span><span>{result['days_covered']} days</span></p>
          <p><span>Time profitable</span><span>{result['time_in_position_hours']}h ({result['pct_time_profitable']}%)</span></p>
          <p><span>Dominant pair</span><span>{result['dominant_pair']}</span></p>
          <p><span>Simulated return</span><span>{result['total_return_pct']:+.4f}%</span></p>
          <p><span>P&amp;L on ${result['position_usd']:,.0f}</span><span>${result['position_pnl_usd']:+,.2f}</span></p>
          <p><span>Annualized (APY)</span><span>{result['apy_pct']:+.2f}%</span></p>
          <div class="note">Uses each cycle's actual fee-adjusted net% as logged by the
          live 3-way scanner (each pair has its own real fee total). Assumes fees paid
          once at entry, not on every re-entry after a flip - treat as an optimistic
          upper bound, not a guarantee.</div>
        </div>
        """

    return INDIAN_BACKTEST_PAGE.format(
        css=BASE_CSS, nav=nav_html("indiabt"), coin=coin, days=days,
        position=int(position), result_html=result_html,
    )


# ── ROUTES ───────────────────────────────────────────────────
@app.route("/")
def scanner_route():
    return render_scanner_page()


@app.route("/indian-exchanges")
def indian_exchanges_route():
    return render_indian_exchanges_page()


@app.route("/indian-opportunities")
def indian_opportunities_route():
    return render_indian_opportunities_page()


@app.route("/indian-backtest")
def indian_backtest_route():
    return render_indian_backtest_page()


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
        threading.Thread(target=run_ranking_pass, daemon=True).start()
    return redirect("/opportunities")


if __name__ == "__main__":
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_three_way_scanner, daemon=True).start()
    threading.Thread(target=ingestion_background, daemon=True).start()
    threading.Thread(target=ranking_background, daemon=True).start()

    port = int(os.getenv("SERVER_PORT", 8080))
    print(f"Starting web dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)

"""
Minimal web dashboard - 10 pages: live scanner, Indian exchanges overview,
Indian opportunities (Delta+Pi42+CoinSwitch), Indian backtest, spread
arbitrage scanner (independent module), Delta/Pi42 backtest (our own
logged data), multi-exchange historical backtest, an automated
opportunity matrix scanner across ALL available coins x exchange pairs,
and a Test Runner (see below).

Runs FIVE background loops, decoupled from each other:
1. Live full-market scanner (Delta vs Pi42, 133 coins, every ~90s)
2. Live 3-way scanner (Delta + Pi42 + CoinSwitch, auto-discovered coin
   universe, every ~90s) - feeds Indian Opportunities + Indian Backtest
3. INGESTION: pulls historical data from Bybit/OKX into our own local
   database, in parallel (ThreadPoolExecutor), every 30 min.
4. RANKING: reads from that local database (fast, no network) and
   recomputes the ranked opportunity list every 5 min.
5. Spread scanner price cache: independent Delta/Pi42(USDT market)/
   CoinSwitch price refresh, every 90s.

TEST RUNNER (/admin/test): runs a chosen script as a SEPARATE subprocess
from inside this always-running app.py, so the live site never stops
just to test something new. Restricted to files inside this project
only (basic path-traversal guard) - reasonable for a private
single-user tool, not hardened for public multi-user exposure.

REAL COST (Spread Scanner): the fast price-only scan only ever shows a
RAW spread. compute_real_cost() (in src/data/orderbook_depth.py)
fetches LIVE order book depth on demand for one coin/pair and walks it
for actual fill price + slippage on all 4 legs of a real round trip,
plus confirmed real fees.

2026-08-01 addition - REAL COST on funding-rate pages (Scanner, Indian
Opportunities): the SAME compute_real_cost() function, reused here for
a different purpose. For a price-spread trade, "cheap"/"expensive" is
about which side has the lower/higher PRICE. For a funding-rate hedge,
it means something different: "cheap" = the exchange with the LOWER
funding rate (go long/buy there), "expensive" = the exchange with the
HIGHER funding rate (go short/sell there) - that direction is what
actually captures the funding differential.

2026-08-02 addition - inline "Net (real)" column on Spread Scanner: a
new /spread-scanner/real-cost-batch endpoint (POST) computes real cost
for MULTIPLE coins concurrently via compute_real_cost_batch() (thread
pool in orderbook_depth.py), used by that page's JS to populate every
visible row's real net% automatically on the same 4s cadence as the
price scan - an explicit, heavier-load tradeoff requested directly,
see spread_scanner.py's module docstring for the full reasoning. The
single-coin /spread-scanner/real-cost endpoint (used by the "Real Cost"
button's detail panel on all 3 pages) is unchanged.

2026-08-19 addition - Delta LIVE feed (Step 1 of real-time rebuild):
src/feeds/delta_live_store.py runs Delta's already-existing (but
previously unused) delta_client.listen() in its own background thread,
giving sub-second price/funding updates completely separate from the
90s full_market_scanner/three_way_scanner loops above, which are NOT
being touched or replaced by this - both keep running in parallel.
For now this only powers a debug JSON endpoint (/admin/delta-live) to
confirm the live feed works correctly before anything on the actual
site starts using it.

============================================================
COORDINATION NOTE (2026-07-31) - both Claude and Codex edit this file
independently in the same repo, and it's collided repeatedly. Before
editing this file, check with Nikunj whether the other AI is also
mid-change here. The current, agreed state as of this edit:
  - CoinSwitch IS integrated (3-way scanner + Indian Opportunities/
    Backtest pages) - do not remove without asking first.
  - Visual theme is the Loris Tools-inspired dark terminal redesign
    (BASE_CSS below) - do not silently revert to a plainer theme.
  - Spread Scanner (multi-exchange checkbox version, now with inline
    Net (real) column) is meant to stay.
  - Test Runner (/admin/test) is meant to stay - it's the fix for the
    "testing stops the live site" problem, don't remove.
  - Real Cost calculator - present on Spread Scanner, Scanner, AND
    Indian Opportunities - meant to stay on all three.
============================================================

Binds to whatever port HidenCloud/Pterodactyl assigns via the SERVER_PORT
env var (falls back to 8080 if not set, for local testing).
"""
import sys
import subprocess
import threading
import time
import json as pyjson
from pathlib import Path
from datetime import datetime
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))
PROJECT_ROOT = Path(__file__).resolve().parents[2]

from flask import Flask, request, redirect, jsonify
from dotenv import load_dotenv

load_dotenv("/home/container/.env")

from src.execution.full_market_scanner import run_scan_cycle, CYCLE_SECONDS, ROUND_TRIP
from src.execution.three_way_scanner import (
    run_scan_cycle as run_three_way_scan_cycle,
    CYCLE_SECONDS as THREE_WAY_CYCLE_SECONDS,
)
from src.execution.backtest_engine import compute_backtest
from src.execution.indian_backtest import compute_indian_backtest
from src.execution.multi_exchange_backtest import compute_multi_backtest, GENERIC_ROUND_TRIP_PCT
from src.execution.advanced_backtest import (
    scan_opportunity_matrix, get_full_common_coin_universe,
)
from src.data.ingest_funding import ingest_all
from src.data.funding_history_store import init_store, get_store_stats
from src.web.spread_scanner import (
    render_spread_scanner_page, get_spread_rows, get_cache_status,
    start_spread_background_loop, EXCHANGES as SPREAD_EXCHANGES,
)
from src.data.orderbook_depth import compute_real_cost, compute_real_cost_batch
from src.feeds.delta_live_store import start_delta_live_feed, get_delta_live_data

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

# Maps three_way_scanner's "best_pair" string to the two raw exchange
# keys orderbook_depth.py expects.
PAIR_TO_EXCHANGES = {
    "Delta-Pi42": ("delta", "pi42"),
    "Delta-CoinSwitch": ("delta", "coinswitch"),
    "Pi42-CoinSwitch": ("pi42", "coinswitch"),
}
RATE_FIELD = {
    "delta": "delta_funding_pct",
    "pi42": "pi42_funding_pct",
    "coinswitch": "coinswitch_funding_pct",
}


def funding_trade_direction(exchange_a, rate_a, exchange_b, rate_b):
    """For a funding-rate hedge: go LONG (buy) on whichever exchange has
    the LOWER rate, SHORT (sell) on whichever has the HIGHER rate - that
    direction is what captures the funding differential. Returns
    (long_exchange, short_exchange)."""
    if rate_a <= rate_b:
        return exchange_a, exchange_b
    return exchange_b, exchange_a


KNOWN_TEST_SCRIPTS = [
    "src/storage/manager.py",
    "src/storage/metadata_store.py",
    "src/data/duckdb_writer.py",
    "src/data/duckdb_reader.py",
    "src/data/delta_market_data.py",
    "src/data/coin_discovery.py",
    "src/data/market_data_store.py",
    "src/data/coinswitch_client.py",
]

INDIAN_EXCHANGE_REGISTRY = [
    {
        "name": "Delta Exchange India", "type": "Derivatives",
        "inr_deposit": "UPI · IMPS · NEFT", "futures": True, "pairs": "50+",
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
        "inr_deposit": "UPI · IMPS · NEFT", "futures": True, "pairs": "650+",
        "funding_interval": "8 hrs", "api_status": "full", "integration": "live",
        "api_docs": "https://api-trading.coinswitch.co", "fiu_registered": True,
        "notes": "FIU-registered. Ed25519-signed API, confirmed working 2026-07-26.",
    },
]

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


_three_way_rows = []
_three_way_last_scan_time = None
_three_way_error = None

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


BASE_CSS = """
  @import url('https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;600;700&family=Inter:wght@400;500;600;700;800&display=swap');
  :root {
    color-scheme: dark;
    --bg: #0B0E11;
    --panel: #12161C;
    --row: #151920;
    --border: #1E2530;
    --border-soft: #191E27;
    --text: #F4F6FA;
    --muted: #6B7385;
    --muted-2: #8B92A8;
    --profit: #00D084;
    --profit-dim: #0A2E22;
    --loss: #FF4757;
    --loss-dim: #2E1418;
    --near: #FFB020;
    --near-dim: #2E2410;
    --accent: #3B82F6;
  }
  * { box-sizing: border-box; }
  body { font-family: 'Inter', -apple-system, system-ui, sans-serif; background:var(--bg);
         color:var(--text); margin:0; padding:0 18px 48px; font-size:14px; }
  header { display:flex; gap:22px; align-items:center; padding:18px 0;
           border-bottom:1px solid var(--border); margin-bottom:22px; flex-wrap:wrap; }
  header a { color:var(--muted-2); text-decoration:none; font-weight:600; font-size:13px;
             letter-spacing:.01em; padding-bottom:16px; border-bottom:2px solid transparent;
             transition:color .15s; }
  header a:hover { color:var(--text); }
  header a.active { color:var(--text); border-bottom-color:var(--profit); }
  h1 { font-size:17px; margin:0 24px 0 0; font-weight:800; letter-spacing:-.01em; }
  h2 { font-weight:800; letter-spacing:-.01em; }
  table { width:100%; border-collapse:collapse; font-size:13px; }
  th, td { text-align:right; padding:10px 12px; border-bottom:1px solid var(--border-soft); }
  th { color:var(--muted); font-weight:700; font-size:10.5px; letter-spacing:.08em;
       text-transform:uppercase; position:sticky; top:0; background:var(--bg);
       cursor:pointer; user-select:none; }
  th:hover { color:var(--text); }
  td:first-child, th:first-child { text-align:left; }
  tbody tr:hover { background:var(--row); }
  td { font-family:'JetBrains Mono', monospace; font-variant-numeric:tabular-nums; }
  td:first-child { font-family:'Inter', sans-serif; font-weight:600; }
  .profit { color:var(--profit); font-weight:600; }
  .loss { color:var(--muted); }
  .near { color:var(--near); font-weight:600; }
  .thin { display:inline-block; background:var(--near-dim); color:var(--near);
          font-family:'Inter',sans-serif; font-size:9.5px; font-weight:700;
          padding:2px 6px; border-radius:4px; margin-left:6px; vertical-align:middle;
          letter-spacing:.03em; text-transform:uppercase; }
  .meta { color:var(--muted-2); font-size:12.5px; margin-bottom:16px;
          font-family:'JetBrains Mono', monospace; }
  #search { width:100%; max-width:280px; margin-bottom:14px; }
  form { display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin-bottom:24px; }
  label { display:block; font-size:10.5px; color:var(--muted); margin-bottom:5px;
          font-weight:700; letter-spacing:.06em; text-transform:uppercase; }
  input, select { background:var(--panel); border:1px solid var(--border); color:var(--text);
                  padding:9px 11px; border-radius:6px; font-size:13px;
                  font-family:'JetBrains Mono', monospace; }
  input:focus, select:focus { outline:none; border-color:var(--accent); }
  button { background:var(--profit); color:#04120C; border:none; padding:10px 20px;
           border-radius:6px; font-weight:700; cursor:pointer; font-size:13px;
           font-family:'Inter',sans-serif; }
  button:hover { filter:brightness(1.1); }
  button.secondary { background:var(--panel); color:var(--text); border:1px solid var(--border); }
  .card { background:var(--panel); border:1px solid var(--border); border-radius:10px;
          padding:20px; max-width:520px; }
  .card p { display:flex; justify-content:space-between; margin:9px 0;
            font-size:13.5px; border-bottom:1px dashed var(--border-soft); padding-bottom:9px; }
  .card p span:first-child { color:var(--muted-2); font-weight:600; }
  .card p span:last-child { font-family:'JetBrains Mono', monospace; font-variant-numeric:tabular-nums; }
  .note { color:var(--muted-2); font-size:12px; margin-top:16px; line-height:1.6; }
  a.coin-link { color:var(--text); text-decoration:none; border-bottom:1px dotted var(--muted); }
  a.coin-link:hover { border-bottom-color:var(--accent); color:var(--accent); }
  .autocomplete-wrap { position:relative; }
  .autocomplete-list { position:absolute; top:100%; left:0; right:0; z-index:20;
                        background:var(--panel); border:1px solid var(--border); border-top:none;
                        border-radius:0 0 6px 6px; max-height:220px; overflow-y:auto;
                        display:none; }
  .autocomplete-list div { padding:8px 11px; cursor:pointer; text-align:left; font-size:13px;
                            font-family:'JetBrains Mono', monospace; }
  .autocomplete-list div:hover, .autocomplete-list div.active-item { background:var(--row); }
  .stat-cards { display:flex; gap:12px; flex-wrap:wrap; margin-bottom:26px; }
  .stat-card { background:var(--panel); border:1px solid var(--border); border-radius:10px;
               padding:18px 20px; min-width:170px; flex:1; }
  .stat-label { font-size:10px; color:var(--muted); font-weight:700; letter-spacing:.08em;
                margin-bottom:8px; text-transform:uppercase; }
  .stat-value { font-size:28px; font-weight:700; font-family:'JetBrains Mono', monospace; }
  .stat-sub { font-size:11px; color:var(--muted); margin-top:5px; }
  .badge-live { display:inline-block; background:var(--profit-dim); color:var(--profit);
                font-size:10.5px; font-weight:700; padding:3px 9px; border-radius:4px;
                letter-spacing:.03em; }
  .section-title { font-size:11px; color:var(--muted); font-weight:700;
                    letter-spacing:.1em; margin:30px 0 14px; text-transform:uppercase; }
  .spread-wrap { display:inline-flex; align-items:center; gap:8px; justify-content:flex-end;
                 width:100%; }
  .spread-track { width:52px; height:5px; background:var(--border-soft); border-radius:3px;
                   overflow:hidden; flex-shrink:0; }
  .spread-fill { display:block; height:100%; border-radius:3px; }
  .test-btn { display:inline-block; background:var(--panel); border:1px solid var(--border);
              color:var(--text); padding:8px 14px; border-radius:6px; font-size:12.5px;
              font-family:'JetBrains Mono', monospace; text-decoration:none; margin:0 8px 8px 0; }
  .test-btn:hover { border-color:var(--accent); color:var(--accent); }
  pre.test-output { background:#000; color:#B8E6C9; border:1px solid var(--border);
                     border-radius:8px; padding:16px; font-family:'JetBrains Mono', monospace;
                     font-size:12.5px; line-height:1.6; overflow-x:auto; white-space:pre-wrap;
                     word-break:break-word; max-height:600px; overflow-y:auto; }
  pre.test-output .stderr { color:#FF8A8A; }
  .detail-btn { background:var(--panel); color:var(--text); border:1px solid var(--border);
                padding:6px 13px; border-radius:5px; font-size:11.5px; cursor:pointer;
                font-family:'Inter',sans-serif; font-weight:600; }
  .detail-btn:hover { border-color:var(--accent); color:var(--accent); }
  .rc-panel { display:none; margin-top:24px; background:var(--panel); border:1px solid var(--border);
              border-radius:10px; padding:20px; }
  .rc-row { display:flex; justify-content:space-between; padding:6px 0; font-size:13px; }
  .rc-row span:first-child { color:var(--muted-2); }
  .rc-leg { display:flex; justify-content:space-between; padding:4px 0;
            border-bottom:1px dashed var(--border-soft); font-size:13px; }
  .rc-leg span:first-child { color:var(--muted-2); }
"""

NAV = """
<header>
  <h1>Funding Arb</h1>
  <a href="/" class="{scanner_active}">Scanner</a>
  <a href="/indian-exchanges" class="{indian_active}">\U0001f1ee\U0001f1f3 Exchanges</a>
  <a href="/indian-opportunities" class="{indiaopp_active}">\U0001f1ee\U0001f1f3 Opportunities</a>
  <a href="/indian-backtest" class="{indiabt_active}">\U0001f1ee\U0001f1f3 Backtest</a>
  <a href="/spread-scanner" class="{spread_active}">Spread Scanner</a>
  <a href="/backtest" class="{backtest_active}">Backtest (2-way)</a>
  <a href="/multi-backtest" class="{multi_active}">Backtest (global)</a>
  <a href="/opportunities" class="{opp_active}">Opportunities (global)</a>
  <a href="/admin/test" class="{testrunner_active}">\U0001f9ea Test Runner</a>
</header>
"""

def nav_html(active):
    keys = ["scanner", "indian", "indiaopp", "indiabt", "spread", "backtest", "multi", "opp", "testrunner"]
    return NAV.format(**{f"{k}_active": ("active" if k == active else "") for k in keys})


def spread_bar(net_pct, css_class, max_scale=0.3):
    color = {"profit": "var(--profit)", "loss": "var(--muted)", "near": "var(--near)"}.get(css_class, "var(--muted)")
    pct_width = min(abs(net_pct) / max_scale, 1.0) * 100
    return (
        f'<span class="spread-wrap">'
        f'<span class="{css_class}">{net_pct:+.5f}%</span>'
        f'<span class="spread-track"><span class="spread-fill" '
        f'style="width:{pct_width:.0f}%;background:{color};"></span></span>'
        f'</span>'
    )


REAL_COST_PANEL_HTML = """
<div id="real-cost-panel" class="rc-panel">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:14px; flex-wrap:wrap; gap:10px;">
    <h3 id="rc-title" style="font-size:15px; margin:0;"></h3>
    <div>
      <label style="display:inline;font-size:12px;color:var(--muted-2);">Position size ($)</label>
      <input id="rc-position" type="number" value="1000" style="width:100px; margin-left:6px;">
      <button id="rc-recalc" type="button" style="margin-left:6px;">Recalculate</button>
    </div>
  </div>
  <p class="note" style="margin:0 0 12px;">Execution cost only (fees + real order-book slippage to open AND close). Compare this against the funding you'd expect to collect over your holding period - it is not merged into one number here since funding accrues over time and this is a one-time entry/exit cost.</p>
  <div id="rc-body"></div>
</div>
"""

REAL_COST_JS = r"""
<script>
const RC_LABELS = {"delta": "Delta Exchange", "pi42": "Pi42", "coinswitch": "CoinSwitch"};
let rcLastCoin = null, rcLastLong = null, rcLastShort = null;

function calculateRealCost(coin, longEx, shortEx) {
  rcLastCoin = coin; rcLastLong = longEx; rcLastShort = shortEx;
  const panel = document.getElementById('real-cost-panel');
  const body = document.getElementById('rc-body');
  const position = document.getElementById('rc-position').value || 1000;
  panel.style.display = 'block';
  document.getElementById('rc-title').innerText = coin + ': Long ' + RC_LABELS[longEx] + ' / Short ' + RC_LABELS[shortEx] + ' \u2014 execution cost';
  body.innerHTML = '<div style="color:var(--muted-2)">Fetching live order book depth for both exchanges...</div>';
  panel.scrollIntoView({behavior: 'smooth', block: 'nearest'});

  fetch('/spread-scanner/real-cost?coin=' + coin + '&exchange_cheap=' + longEx + '&exchange_expensive=' + shortEx + '&position=' + position)
    .then(function(res) { return res.json(); })
    .then(renderRealCost);
}

function renderRealCost(data) {
  const body = document.getElementById('rc-body');
  if (data.error) {
    body.innerHTML = '<div style="color:#FF4757">' + data.error + '</div>';
    return;
  }
  const legsHtml = data.legs.map(function(leg) {
    const warn = leg.fully_filled ? '' : ' <span style="color:var(--near)">(not enough visible depth!)</span>';
    return '<div class="rc-leg">' +
      '<span>' + leg.label + ' on ' + RC_LABELS[leg.exchange] + '</span>' +
      '<span>avg $' + leg.avg_price.toFixed(6) + ' \u00b7 slippage ' + leg.slippage_pct.toFixed(4) + '%' + warn + '</span>' +
    '</div>';
  }).join('');

  const netColor = data.net_pct > 0 ? 'var(--profit)' : 'var(--loss)';
  const csNote = (data.cheap_gst_included === null || data.expensive_gst_included === null)
    ? '<div style="color:var(--muted-2);font-size:11px;margin-top:6px">CoinSwitch fee shown as-is \u2014 GST treatment on their fee is not confirmed, so it is not added on top.</div>'
    : '';
  const fillWarning = data.fully_fillable_at_size ? '' :
    '<div style="color:var(--near);font-size:12px;margin-top:8px">\u26a0\ufe0f Order book does not have enough visible depth to fully fill this position size on all legs \u2014 the real result would be worse than shown.</div>';

  body.innerHTML =
    '<div style="margin-bottom:10px;">' + legsHtml + '</div>' +
    '<div class="rc-row"><span>Entry price gap (long vs short, at best price)</span><span>' + data.raw_spread_pct.toFixed(4) + '%</span></div>' +
    '<div class="rc-row"><span>Total fees (4 fills)</span><span>-' + data.total_fees_pct.toFixed(4) + '%</span></div>' +
    '<div class="rc-row"><span>Total slippage (4 fills, real order book)</span><span>-' + data.total_slippage_pct.toFixed(4) + '%</span></div>' +
    '<div class="rc-row" style="padding-top:10px;font-size:16px;font-weight:700;border-top:1px solid var(--border);margin-top:6px;"><span>Net execution cost</span><span style="color:' + netColor + '">' + data.net_pct.toFixed(4) + '%</span></div>' +
    csNote + fillWarning;
}

document.getElementById('rc-recalc').addEventListener('click', function() {
  if (rcLastCoin) calculateRealCost(rcLastCoin, rcLastLong, rcLastShort);
});
</script>
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
  <th>Real Cost</th>
</tr></thead>
<tbody>{rows}</tbody>
</table>
{real_cost_panel}
{js}
{real_cost_js}
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
    <div class="stat-value" style="color:#00D084">{delta_btc_rate}</div>
    <div class="stat-sub">per 8-hour funding period</div>
    <div style="margin-top:10px"><span class="badge-live">\U0001f7e2 LIVE</span></div>
  </div>
  <div class="stat-card">
    <div class="stat-label">PI42 \u00b7 BTCUSDT</div>
    <div class="stat-value" style="color:#00D084">{pi42_btc_rate}</div>
    <div class="stat-sub">per funding period (4\u20138 hrs)</div>
    <div style="margin-top:10px"><span class="badge-live">\U0001f7e2 LIVE</span></div>
  </div>
  <div class="stat-card">
    <div class="stat-label">COINSWITCH PRO \u00b7 BTCUSDT</div>
    <div class="stat-value" style="color:#00D084">{coinswitch_btc_rate}</div>
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
</p>
<div class="stat-cards">
  <div class="stat-card" style="border-color:{profitable_border}">
    <div class="stat-label">PROFITABLE NOW</div>
    <div class="stat-value" style="color:{profitable_color}">{profitable_count}</div>
    <div class="stat-sub">coins above fee threshold</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">BEST COIN</div>
    <div class="stat-value" style="color:#00D084">{best_coin}</div>
    <div class="stat-sub">net {best_net}% via {best_pair}</div>
  </div>
  <div class="stat-card">
    <div class="stat-label">NEAR MISS</div>
    <div class="stat-value" style="color:#FFB020">{near_count}</div>
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
  <b>How this works:</b> for each coin, all 3 possible pairs are checked and whichever has
  the best net% after ITS OWN real fees wins. Page auto-refreshes every 90 seconds.
  Click \u201cReal Cost\u201d on any row to check actual execution cost using live order book
  depth before relying on the raw net% above.
</div>
{real_cost_panel}
<script>setTimeout(()=>location.reload(), 92000);</script>
{real_cost_js}
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
  (Delta + Pi42 + CoinSwitch).
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

TEST_RUNNER_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Test Runner \u2014 Funding Arb</title>
<style>{css}</style></head><body>
{nav}
<h2 style="font-size:18px;margin:0 0 6px;">\U0001f9ea Test Runner</h2>
<p class="note" style="margin-bottom:20px;max-width:680px">
  Runs any script in this project as a <b>separate process</b>, without stopping the
  live website. The scanners, alerts, and every other page above keep running the
  whole time - this is what fixes the old problem where testing something new meant
  the entire site going offline until you switched the settings back.
</p>
<div class="section-title">QUICK PICKS (scripts with their own self-tests)</div>
<div style="margin-bottom:24px">{quick_pick_buttons}</div>
<form method="get" action="/admin/run-test">
  <div style="flex:1;min-width:280px">
    <label>Script path (inside this project)</label>
    <input name="module" value="{module}" style="width:100%" placeholder="e.g. src/data/duckdb_writer.py">
  </div>
  <button type="submit">Run</button>
</form>
{output_html}
</body></html>
"""


def render_scanner_page():
    if _scan_error:
        status = f'<span style="color:#FF4757">Last scan failed: {_scan_error}</span>'
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
        long_ex, short_ex = funding_trade_direction("delta", r["delta_funding_pct"], "pi42", r["pi42_funding_pct"])
        rows_html += (
            f"<tr data-coin='{r['coin'].lower()}'>"
            f"<td><a class='coin-link' href='/backtest?coin={r['coin']}'>{r['coin']}</a>{thin_badge}</td>"
            f"<td>{r['delta_funding_pct']:.5f}</td>"
            f"<td>{r['pi42_funding_pct']:.5f}</td>"
            f"<td>{r['gap_pct']:.5f}</td>"
            f"<td>{spread_bar(r['net_pct'], cls)}</td>"
            f"<td>{r['delta_volume_usd']:,.0f}</td>"
            f"<td><button class='detail-btn' onclick=\"calculateRealCost('{r['coin']}','{long_ex}','{short_ex}')\">Real Cost</button></td>"
            f"</tr>"
        )
    if not rows_html:
        rows_html = "<tr><td colspan='7'>No data yet.</td></tr>"

    return SCANNER_PAGE.format(
        css=BASE_CSS, nav=nav_html("scanner"), status_line=status, rows=rows_html,
        js=SCANNER_JS, real_cost_panel=REAL_COST_PANEL_HTML, real_cost_js=REAL_COST_JS,
    )


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
        ingest_status = f'<span style="color:#FFB020">Ingesting: {done}/{total} pairs...</span>'
    elif _ingest_error:
        ingest_status = f'<span style="color:#FF4757">Last ingestion failed: {_ingest_error}</span>'
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
        rank_status = '<span style="color:#FFB020">Ranking...</span>'
    elif _opp_error:
        rank_status = f'<span style="color:#FF4757">Last ranking pass failed: {_opp_error}</span>'
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
        docs = f'<a href="{exc["api_docs"]}" target="_blank" style="color:var(--muted-2);font-size:11px">{exc["name"]} docs \u2197</a>'
        futures_str = "\u2705" if exc["futures"] else "\u274c"
        fiu_str = "\u2705" if exc["fiu_registered"] else "\u274c"
        rows_html += (
            f"<tr><td>{docs}</td><td>{exc['type']}</td><td>{exc['inr_deposit']}</td>"
            f"<td style='text-align:center'>{futures_str}</td><td>{exc.get('pairs','\u2014')}</td>"
            f"<td>{exc.get('funding_interval','N/A')}</td><td>{api_map.get(exc['api_status'],'')}</td>"
            f"<td style='text-align:center'>{fiu_str}</td><td>{int_map.get(exc['integration'],'')}</td>"
            f"<td style='font-size:12px;color:var(--muted-2)'>{exc['notes']}</td></tr>"
        )

    return INDIAN_EXCHANGES_PAGE.format(
        css=BASE_CSS, nav=nav_html("indian"),
        delta_btc_rate=delta_btc_rate, pi42_btc_rate=pi42_btc_rate,
        coinswitch_btc_rate=coinswitch_btc_rate,
        scan_time=scan_time, exchange_rows=rows_html,
    )


def render_indian_opportunities_page():
    scan_time = _three_way_last_scan_time.strftime("%H:%M:%S") if _three_way_last_scan_time else "Starting..."
    total_coins = len(_three_way_rows)

    profitable = [r for r in _three_way_rows if r["profitable"]]
    near = [r for r in _three_way_rows if not r["profitable"] and r["net_pct"] > -0.05]

    profitable_count = len(profitable)
    near_count = len(near)
    profitable_color = "#00D084" if profitable_count > 0 else "var(--muted-2)"
    profitable_border = "#0A2E22" if profitable_count > 0 else "var(--border)"

    if profitable:
        best = max(profitable, key=lambda r: r["net_pct"])
        best_coin = best["coin"]
        best_net = f"{best['net_pct']:+.5f}"
        best_pair = best["best_pair"]
    else:
        best_coin, best_net, best_pair = "None", "0.00000", "\u2014"

    def rate_or_na(v):
        return f"{v}%" if v != "" else "N/A"

    def real_cost_button(r):
        pair = PAIR_TO_EXCHANGES.get(r["best_pair"])
        if not pair:
            return ""
        ex1, ex2 = pair
        rate1 = r[RATE_FIELD[ex1]]
        rate2 = r[RATE_FIELD[ex2]]
        if rate1 == "" or rate2 == "":
            return ""
        long_ex, short_ex = funding_trade_direction(ex1, rate1, ex2, rate2)
        return f"<td><button class='detail-btn' onclick=\"calculateRealCost('{r['coin']}','{long_ex}','{short_ex}')\">Real Cost</button></td>"

    if profitable:
        p_rows = ""
        for r in sorted(profitable, key=lambda r: r["net_pct"], reverse=True):
            apy_est = r["net_pct"] * 3 * 365
            p_rows += (
                f"<tr><td><a class='coin-link' href='/indian-backtest?coin={r['coin']}'>{r['coin']}</a></td>"
                f"<td style='text-align:left'>{r['best_pair']}</td>"
                f"<td>{rate_or_na(r['delta_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['pi42_funding_pct'])}</td>"
                f"<td>{rate_or_na(r['coinswitch_funding_pct'])}</td>"
                f"<td>{r['gap_pct']:.5f}%</td>"
                f"<td>{spread_bar(r['net_pct'], 'profit')}</td>"
                f"<td class='profit'>{apy_est:+.1f}%</td>"
                f"{real_cost_button(r)}</tr>"
            )
        profitable_section = f"""
        <div class="section-title">\U0001f7e2 PROFITABLE OPPORTUNITIES ({profitable_count})</div>
        <table><thead><tr>
          <th>Coin</th><th>Best Pair</th><th>Delta %</th><th>Pi42 %</th><th>CoinSwitch %</th>
          <th>Gap %</th><th>Net %</th><th>Est. APY</th><th>Real Cost</th>
        </tr></thead><tbody>{p_rows}</tbody></table>
        """
    else:
        profitable_section = (
            '<div class="section-title">\U0001f7e2 PROFITABLE OPPORTUNITIES</div>'
            '<div class="note" style="background:var(--panel);border:1px solid var(--border);'
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
                f"<td>{spread_bar(r['net_pct'], 'near')}</td>"
                f"{real_cost_button(r)}</tr>"
            )
        near_section = f"""
        <div class="section-title" style="margin-top:28px">\U0001f7e1 NEAR MISSES ({near_count})</div>
        <table><thead><tr>
          <th>Coin</th><th>Best Pair</th><th>Delta %</th><th>Pi42 %</th><th>CoinSwitch %</th>
          <th>Gap %</th><th>Net %</th><th>Real Cost</th>
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
        real_cost_panel=REAL_COST_PANEL_HTML, real_cost_js=REAL_COST_JS,
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
          live 3-way scanner. Assumes fees paid once at entry - treat as an optimistic
          upper bound, not a guarantee.</div>
        </div>
        """

    return INDIAN_BACKTEST_PAGE.format(
        css=BASE_CSS, nav=nav_html("indiabt"), coin=coin, days=days,
        position=int(position), result_html=result_html,
    )


def render_spread_scanner_route():
    selected = request.args.getlist("exchanges") or ["delta", "pi42", "coinswitch"]
    selected = [e for e in selected if e in SPREAD_EXCHANGES]
    search = request.args.get("search", "")
    min_spread = float(request.args.get("min_spread", 0) or 0)
    limit = int(float(request.args.get("limit", 10) or 10))

    return render_spread_scanner_page(
        BASE_CSS, nav_html("spread"), selected, search, min_spread, limit
    )


def _validate_test_script_path(module: str):
    if not module or not module.endswith(".py"):
        return None
    candidate = (PROJECT_ROOT / module).resolve()
    try:
        candidate.relative_to(PROJECT_ROOT)
    except ValueError:
        return None
    if not candidate.exists():
        return None
    return candidate


def render_test_runner_page():
    module = request.args.get("module", "")
    quick_picks = "".join(
        f'<a class="test-btn" href="/admin/run-test?module={s}">{s.split("/")[-1]}</a>'
        for s in KNOWN_TEST_SCRIPTS
    )
    return TEST_RUNNER_PAGE.format(
        css=BASE_CSS, nav=nav_html("testrunner"),
        quick_pick_buttons=quick_picks, module=module, output_html="",
    )


def render_run_test_route():
    module = request.args.get("module", "")
    quick_picks = "".join(
        f'<a class="test-btn" href="/admin/run-test?module={s}">{s.split("/")[-1]}</a>'
        for s in KNOWN_TEST_SCRIPTS
    )

    path = _validate_test_script_path(module)
    if path is None:
        output_html = (
            '<div class="note" style="color:#FF4757">'
            'Invalid path - must be a .py file inside this project.</div>'
        )
    else:
        try:
            result = subprocess.run(
                [sys.executable, str(path)],
                capture_output=True, text=True, timeout=90, cwd=str(PROJECT_ROOT),
            )
            output = result.stdout
            if result.stderr:
                output += f"\n<span class='stderr'>{result.stderr}</span>"
            exit_note = (
                f"\n\n--- exit code: {result.returncode} "
                f"({'OK' if result.returncode == 0 else 'FAILED'}) ---"
            )
            output_html = f'<pre class="test-output">{output}{exit_note}</pre>'
        except subprocess.TimeoutExpired:
            output_html = (
                '<div class="note" style="color:#FF4757">'
                'Timed out after 90 seconds - this script may run continuously '
                '(a loop) rather than being a one-shot test.</div>'
            )
        except Exception as e:
            output_html = f'<div class="note" style="color:#FF4757">Failed to run: {e}</div>'

    return TEST_RUNNER_PAGE.format(
        css=BASE_CSS, nav=nav_html("testrunner"),
        quick_pick_buttons=quick_picks, module=module, output_html=output_html,
    )


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


@app.route("/spread-scanner")
def spread_scanner_route():
    return render_spread_scanner_route()


@app.route("/spread-scanner/data")
def spread_scanner_data_route():
    selected = request.args.getlist("exchanges") or ["delta", "pi42", "coinswitch"]
    selected = [e for e in selected if e in SPREAD_EXCHANGES]
    search = request.args.get("search", "")
    min_spread = float(request.args.get("min_spread", 0) or 0)
    limit = int(float(request.args.get("limit", 10) or 10))

    rows = get_spread_rows(selected, search, min_spread, limit) if len(selected) >= 2 else []
    status, error, age = get_cache_status()
    return jsonify({
        "rows": rows,
        "cache_status": status,
        "cache_error": error,
        "cache_age_seconds": age,
    })


@app.route("/spread-scanner/real-cost")
def spread_scanner_real_cost_route():
    coin = request.args.get("coin", "").upper()
    exchange_cheap = request.args.get("exchange_cheap", "")
    exchange_expensive = request.args.get("exchange_expensive", "")
    position_usd = float(request.args.get("position", 1000) or 1000)

    if not coin or exchange_cheap not in SPREAD_EXCHANGES or exchange_expensive not in SPREAD_EXCHANGES:
        return jsonify({"error": "Missing or invalid coin/exchange parameters"})

    result = compute_real_cost(coin, exchange_cheap, exchange_expensive, position_usd)
    return jsonify(result)


@app.route("/spread-scanner/real-cost-batch", methods=["POST"])
def spread_scanner_real_cost_batch_route():
    payload = request.get_json(force=True, silent=True) or {}
    raw_items = payload.get("items", [])
    position_usd = float(payload.get("position", 1000) or 1000)

    clean_items = []
    for it in raw_items:
        coin = str(it.get("coin", "")).upper()
        ec = it.get("exchange_cheap", "")
        ee = it.get("exchange_expensive", "")
        if coin and ec in SPREAD_EXCHANGES and ee in SPREAD_EXCHANGES:
            clean_items.append({"coin": coin, "exchange_cheap": ec, "exchange_expensive": ee})

    if not clean_items:
        return jsonify({"results": {}})

    results = compute_real_cost_batch(clean_items, position_usd)
    return jsonify({"results": results})


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


@app.route("/admin/test")
def test_runner_route():
    return render_test_runner_page()


@app.route("/admin/run-test")
def run_test_route():
    return render_run_test_route()


@app.route("/admin/delta-live")
def delta_live_debug_route():
    """Step 1 test endpoint (2026-08-19) - confirms the new sub-second
    Delta live feed is actually receiving data. Not used by any page
    yet. Refresh this a few times and confirm price_age_seconds /
    funding_age_seconds stay low (well under the old 90s cadence) -
    that's the proof the live feed is genuinely working."""
    data = get_delta_live_data()
    sample = dict(list(data.items())[:10])
    return jsonify({
        "coin_count": len(data),
        "sample": sample,
    })


if __name__ == "__main__":
    threading.Thread(target=background_scanner, daemon=True).start()
    threading.Thread(target=background_three_way_scanner, daemon=True).start()
    threading.Thread(target=ingestion_background, daemon=True).start()
    threading.Thread(target=ranking_background, daemon=True).start()
    start_spread_background_loop()
    start_delta_live_feed()

    port = int(os.getenv("SERVER_PORT", 8080))
    print(f"Starting web dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)

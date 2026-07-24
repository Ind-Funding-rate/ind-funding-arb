"""
Minimal web dashboard - 2 pages: live scanner + backtest.

Runs the full-market scanner continuously in a background thread (so we
never stop monitoring), keeps the latest results in memory, and serves
them as plain server-rendered HTML - no JS framework, no build step,
loads fast on a free-tier server.

Binds to whatever port HidenCloud/Pterodactyl assigns via the SERVER_PORT
env var (falls back to 8080 if not set, for local testing).
"""
import sys
import threading
import time
from pathlib import Path
from datetime import datetime
import os

sys.path.append(str(Path(__file__).resolve().parents[2]))

from flask import Flask, request, render_template_string
from dotenv import load_dotenv

load_dotenv("/home/container/.env")

from src.execution.full_market_scanner import run_scan_cycle, CYCLE_SECONDS, ROUND_TRIP
from src.execution.backtest_engine import compute_backtest

app = Flask(__name__)

# ── SHARED STATE (updated by the background scan thread) ──────
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


# ── SHARED PAGE STYLE (kept tiny on purpose - no framework) ────
BASE_CSS = """
  :root { color-scheme: dark; }
  * { box-sizing: border-box; }
  body { font-family: -apple-system, system-ui, sans-serif; background:#0f1117;
         color:#e6e6e6; margin:0; padding:0 16px 40px; }
  header { display:flex; gap:20px; align-items:center; padding:16px 0;
           border-bottom:1px solid #2a2d38; margin-bottom:20px; }
  header a { color:#e6e6e6; text-decoration:none; font-weight:600; opacity:.7; }
  header a.active { opacity:1; border-bottom:2px solid #4ade80; padding-bottom:14px; }
  h1 { font-size:20px; margin:0 20px 0 0; }
  table { width:100%; border-collapse:collapse; font-size:14px; }
  th, td { text-align:right; padding:8px 10px; border-bottom:1px solid #1f222c; }
  th { color:#8b8fa3; font-weight:600; position:sticky; top:0; background:#0f1117; }
  td:first-child, th:first-child { text-align:left; }
  .profit { color:#4ade80; font-weight:600; }
  .loss { color:#8b8fa3; }
  .meta { color:#8b8fa3; font-size:13px; margin-bottom:14px; }
  form { display:flex; gap:10px; flex-wrap:wrap; align-items:end; margin-bottom:24px; }
  label { display:block; font-size:12px; color:#8b8fa3; margin-bottom:4px; }
  input, select { background:#1a1d27; border:1px solid #2a2d38; color:#e6e6e6;
                  padding:8px 10px; border-radius:6px; font-size:14px; }
  button { background:#4ade80; color:#0f1117; border:none; padding:9px 18px;
           border-radius:6px; font-weight:700; cursor:pointer; }
  .card { background:#1a1d27; border:1px solid #2a2d38; border-radius:10px;
          padding:18px; max-width:520px; }
  .card p { display:flex; justify-content:space-between; margin:8px 0;
            font-size:14px; border-bottom:1px dashed #2a2d38; padding-bottom:8px; }
  .card p span:first-child { color:#8b8fa3; }
  .note { color:#8b8fa3; font-size:12px; margin-top:16px; line-height:1.5; }
"""

NAV = """
<header>
  <h1>Funding Arb</h1>
  <a href="/" class="{scanner_active}">Scanner</a>
  <a href="/backtest" class="{backtest_active}">Backtest</a>
</header>
"""

SCANNER_PAGE = """
<!doctype html><html><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<meta http-equiv="refresh" content="30">
<title>Funding Arb Scanner</title>
<style>{css}</style></head><body>
{nav}
<div class="meta">
  {status_line}
</div>
<table>
<tr><th>Coin</th><th>Delta %</th><th>Pi42 %</th><th>Gap pp</th><th>Net %</th><th>Delta Vol ($)</th></tr>
{rows}
</table>
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
            f"({int(age)}s ago) · {len(_latest_rows)} coins checked · "
            f"{profitable_count} profitable · fee floor {ROUND_TRIP*100:.4f}% · "
            f"auto-refreshes every 30s"
        )

    rows_html = ""
    for r in _latest_rows:
        cls = "profit" if r["profitable"] else "loss"
        rows_html += (
            f"<tr><td>{r['coin']}</td>"
            f"<td>{r['delta_funding_pct']:.5f}</td>"
            f"<td>{r['pi42_funding_pct']:.5f}</td>"
            f"<td>{r['gap_pct']:.5f}</td>"
            f"<td class='{cls}'>{r['net_pct']:+.5f}</td>"
            f"<td>{r['delta_volume_usd']:,.0f}</td></tr>"
        )
    if not rows_html:
        rows_html = "<tr><td colspan='6'>No data yet.</td></tr>"

    nav = NAV.format(scanner_active="active", backtest_active="")
    return SCANNER_PAGE.format(css=BASE_CSS, nav=nav, status_line=status, rows=rows_html)


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
            f'for Pi42. Come back after this has been running a few days.</div></div>'
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

    nav = NAV.format(scanner_active="", backtest_active="active")
    return BACKTEST_PAGE.format(
        css=BASE_CSS, nav=nav, coin=coin, days=days, position=int(position),
        result_html=result_html,
    )


@app.route("/")
def scanner_route():
    return render_scanner_page()


@app.route("/backtest")
def backtest_route():
    return render_backtest_page()


if __name__ == "__main__":
    thread = threading.Thread(target=background_scanner, daemon=True)
    thread.start()

    port = int(os.getenv("SERVER_PORT", 8080))
    print(f"Starting web dashboard on 0.0.0.0:{port}")
    app.run(host="0.0.0.0", port=port)

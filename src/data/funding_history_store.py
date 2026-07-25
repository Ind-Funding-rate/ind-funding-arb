"""
Local funding-rate data store - this is the architectural fix for
scan speed. Sites like loris.tools/CoinGlass don't hit every exchange
live on every page load; they run their own continuous ingestion
pipeline into their own database, and serve the dashboard from that
local store instead. This does the same thing on a small scale:

    ingest_funding.py  -> pulls from exchanges, writes here (slow, runs
                          in the background on its own schedule)
    this module        -> local SQLite store, reads are near-instant
    advanced_backtest  -> reads FROM this store, not from exchanges
                          directly, when computing the opportunity matrix

This is also what makes scaling to many more exchanges later realistic:
each new exchange just needs its own ingestion function writing into the
same shared table - the read/ranking side doesn't change at all.
"""
import sqlite3
from pathlib import Path
from datetime import datetime, timedelta

DB_PATH = Path("/home/container/data/funding_history.db")
DB_PATH.parent.mkdir(exist_ok=True)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_store():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_history (
            exchange  TEXT NOT NULL,
            coin      TEXT NOT NULL,
            ts        TEXT NOT NULL,
            rate      REAL NOT NULL,
            PRIMARY KEY (exchange, coin, ts)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_exchange_coin
        ON funding_history (exchange, coin)
    """)
    conn.commit()
    conn.close()


def store_history(exchange: str, coin: str, rows: list):
    """rows: list of {"time": datetime, "rate": float}. Uses INSERT OR
    IGNORE so re-ingesting the same period is a safe no-op for rows
    already stored - only genuinely new funding events get added."""
    if not rows:
        return
    conn = get_connection()
    conn.executemany(
        "INSERT OR IGNORE INTO funding_history (exchange, coin, ts, rate) VALUES (?, ?, ?, ?)",
        [(exchange, coin, r["time"].strftime("%Y-%m-%d %H:%M:%S"), r["rate"]) for r in rows],
    )
    conn.commit()
    conn.close()


def get_cached_history(exchange: str, coin: str, days: int):
    """Reads from the LOCAL store - no network call, near-instant. This
    is what makes the opportunity matrix scan fast: ranking hundreds of
    coins is just SQLite reads, not hundreds of live API requests."""
    cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d %H:%M:%S")
    conn = get_connection()
    rows = conn.execute(
        "SELECT ts, rate FROM funding_history WHERE exchange = ? AND coin = ? AND ts >= ? ORDER BY ts",
        (exchange, coin, cutoff),
    ).fetchall()
    conn.close()
    return [{"time": datetime.strptime(r["ts"], "%Y-%m-%d %H:%M:%S"), "rate": r["rate"]} for r in rows]


def get_store_stats():
    conn = get_connection()
    total = conn.execute("SELECT COUNT(*) FROM funding_history").fetchone()[0]
    per_exchange = conn.execute(
        "SELECT exchange, COUNT(DISTINCT coin) as coins, COUNT(*) as rows FROM funding_history GROUP BY exchange"
    ).fetchall()
    latest = conn.execute("SELECT MAX(ts) FROM funding_history").fetchone()[0]
    conn.close()
    return {
        "total_rows": total,
        "latest_ts": latest,
        "per_exchange": [dict(r) for r in per_exchange],
    }

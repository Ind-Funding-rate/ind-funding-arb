import sqlite3
from pathlib import Path
from datetime import datetime

# ── DATABASE SETUP ────────────────────────────────────────────
DB_PATH = Path("data/rates.db")
DB_PATH.parent.mkdir(exist_ok=True)

def get_connection():
    """Get a database connection."""
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row  # Allows column access by name
    return conn

def init_db():
    """Create tables if they don't exist yet."""
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS funding_rates (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp    TEXT    NOT NULL,
            exchange     TEXT    NOT NULL,
            symbol       TEXT    NOT NULL,
            mark_price   REAL    NOT NULL,
            funding_rate REAL
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_timestamp
        ON funding_rates (timestamp)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_exchange
        ON funding_rates (exchange)
    """)
    conn.commit()
    conn.close()
    print(f"  Database ready: {DB_PATH}")

def save_rate(data: dict):
    """Save one rate reading to the database."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO funding_rates
            (timestamp, exchange, symbol, mark_price, funding_rate)
        VALUES
            (:timestamp, :exchange, :symbol, :mark_price, :funding_rate)
    """, data)
    conn.commit()
    conn.close()

def get_latest_rates():
    """Get the most recent reading from each exchange."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM funding_rates
        WHERE id IN (
            SELECT MAX(id) FROM funding_rates
            GROUP BY exchange
        )
    """).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_recent_rates(exchange: str, limit: int = 100):
    """Get last N readings for a specific exchange."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT * FROM funding_rates
        WHERE exchange = ?
        ORDER BY id DESC
        LIMIT ?
    """, (exchange, limit)).fetchall()
    conn.close()
    return [dict(row) for row in rows]

def get_rate_count():
    """Get total number of readings stored."""
    conn = get_connection()
    count = conn.execute(
        "SELECT COUNT(*) FROM funding_rates"
    ).fetchone()[0]
    conn.close()
    return count

# ── TEST ──────────────────────────────────────────────────────
if __name__ == "__main__":
    print("Initializing database...")
    init_db()
    print(f"  Total records: {get_rate_count()}")
    print("\n  Latest rates:")
    for row in get_latest_rates():
        print(f"    {row['exchange']} | {row['symbol']} | "
              f"Price: {row['mark_price']} | "
              f"Rate: {row['funding_rate']}")
    print("\n  Database test passed ✅")

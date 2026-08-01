"""
DuckDBWriter - Step 2 of the approved storage architecture.

Writes rows into TODAY's local .duckdb file for a given
exchange/market_type/symbol, at the path the approved design specifies:
    data_cache/{exchange}/{market_type}/{symbol}/{YYYY}/{MM}/{DD}.duckdb

This is the file DailyArchiver (Step 3, next) will later finalize,
verify, and hand off to StorageManager.archive_file(). Nothing here
touches archiving - this only ever writes to today's still-open file.

Schema is NOT fixed - each data source (funding rate, price, order book,
option chain) has different columns, so the table is created dynamically
from whatever dict is first passed in, matching the approved design's
"schema per data source, not forced into one universal shape."
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import date as date_type, datetime, timezone

CACHE_DIR = Path("/home/container/data_cache")


def _file_path(exchange: str, market_type: str, symbol: str, date: date_type) -> Path:
    folder = CACHE_DIR / exchange / market_type / symbol / f"{date.year:04d}" / f"{date.month:02d}"
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{date.day:02d}.duckdb"


def append_row(exchange: str, market_type: str, symbol: str, date: date_type, row: dict):
    """Appends one row to today's local file, creating the file and
    table on first write. Safe to call repeatedly throughout the day -
    each call opens, writes, and closes (this data arrives at most every
    few minutes, so connection-pooling overhead isn't a real concern
    here - simplicity over premature optimization)."""
    row = dict(row)
    row.setdefault("_written_at", datetime.now(timezone.utc).isoformat())

    path = _file_path(exchange, market_type, symbol, date)
    df = pd.DataFrame([row])

    conn = duckdb.connect(str(path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots AS
            SELECT * FROM df LIMIT 0
        """)
        conn.execute("INSERT INTO snapshots SELECT * FROM df")
    finally:
        conn.close()


def append_rows(exchange: str, market_type: str, symbol: str, date: date_type, rows: list):
    """Same as append_row but for a batch (e.g. all price levels from
    one order-book snapshot) - one file open/close instead of many."""
    if not rows:
        return
    rows = [dict(r) for r in rows]
    written_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r.setdefault("_written_at", written_at)

    path = _file_path(exchange, market_type, symbol, date)
    df = pd.DataFrame(rows)

    conn = duckdb.connect(str(path))
    try:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS snapshots AS
            SELECT * FROM df LIMIT 0
        """)
        conn.execute("INSERT INTO snapshots SELECT * FROM df")
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 54)
    print("  DUCKDB WRITER - STANDALONE TEST")
    print("=" * 54)

    today = date_type(2026, 8, 1)

    print(f"\n[1] Writing 3 fake price rows for Delta/futures/BTC/{today}...")
    for i in range(3):
        append_row(
            exchange="delta", market_type="futures", symbol="BTC", date=today,
            row={"symbol": "BTCUSD", "mark_price": 63000 + i, "spot_price": 63010 + i},
        )
    print("    Done.")

    print(f"\n[2] Writing a batch of 5 fake order-book rows...")
    fake_book = [
        {"symbol": "BTCUSD", "side": "buy", "price": 63000 - j, "size": 100 * j}
        for j in range(5)
    ]
    append_rows("delta", "futures", "BTC", today, fake_book)
    print("    Done.")

    path = _file_path("delta", "futures", "BTC", today)
    print(f"\n[3] File location: {path}")
    print(f"    Exists: {path.exists()}  |  Size: {path.stat().st_size if path.exists() else 0} bytes")

    conn = duckdb.connect(str(path))
    count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    print(f"\n[4] Row count in file: {count} (expect 8 first run, more if re-run)")
    conn.close()

    print("\n" + "=" * 54)
    print("  If the file exists with 8+ rows, DuckDBWriter works.")
    print("  Next: DuckDBReader (separate file) to read this back.")
    print("=" * 54)

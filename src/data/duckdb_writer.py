"""
DuckDBWriter - Step 2 of the approved storage architecture.

FIX (2026-08-01): the first version put all data types (price, order
book, option chain, ...) into one shared table per symbol/day. Testing
caught this immediately - price rows and order-book rows have
completely different columns, so the second write type crashed with a
schema mismatch. Fixed by giving each data_type its OWN file, using the
`filename` parameter StorageManager.build_key() already supports (e.g.
"price.duckdb", "order_book.duckdb", "option_chain.duckdb") - this
needed zero changes to the already-tested StorageManager, since it was
designed to take a free-form filename from the start.

Writes rows into TODAY's local .duckdb file for a given
exchange/market_type/symbol/data_type, at:
    data_cache/{exchange}/{market_type}/{symbol}/{YYYY}/{MM}/{DD}/{data_type}.duckdb

This is what DailyArchiver (Step 3, next) will later finalize, verify,
and hand off to StorageManager.archive_file(exchange, market_type,
symbol, date, filename=f"{data_type}.duckdb").
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import date as date_type, datetime, timezone

CACHE_DIR = Path("/home/container/data_cache")


def _file_path(exchange: str, market_type: str, symbol: str, date: date_type, data_type: str) -> Path:
    folder = (
        CACHE_DIR / exchange / market_type / symbol
        / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}"
    )
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{data_type}.duckdb"


def append_row(exchange: str, market_type: str, symbol: str, date: date_type,
                data_type: str, row: dict):
    """Appends one row. data_type (e.g. "price", "order_book",
    "option_chain", "funding_rate") determines which file it goes into -
    each file only ever holds ONE kind of row, so its schema stays
    consistent for the whole day."""
    row = dict(row)
    row.setdefault("_written_at", datetime.now(timezone.utc).isoformat())

    path = _file_path(exchange, market_type, symbol, date, data_type)
    df = pd.DataFrame([row])

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS snapshots AS SELECT * FROM df LIMIT 0")
        conn.execute("INSERT INTO snapshots SELECT * FROM df")
    finally:
        conn.close()


def append_rows(exchange: str, market_type: str, symbol: str, date: date_type,
                 data_type: str, rows: list):
    """Same as append_row but for a batch (e.g. all price levels from
    one order-book snapshot) - one file open/close instead of many.
    All rows in one call MUST share the same columns (they're the same
    data_type, captured at the same moment) - that's the actual fix
    here, not just batching."""
    if not rows:
        return
    rows = [dict(r) for r in rows]
    written_at = datetime.now(timezone.utc).isoformat()
    for r in rows:
        r.setdefault("_written_at", written_at)

    path = _file_path(exchange, market_type, symbol, date, data_type)
    df = pd.DataFrame(rows)

    conn = duckdb.connect(str(path))
    try:
        conn.execute("CREATE TABLE IF NOT EXISTS snapshots AS SELECT * FROM df LIMIT 0")
        conn.execute("INSERT INTO snapshots SELECT * FROM df")
    finally:
        conn.close()


if __name__ == "__main__":
    print("=" * 54)
    print("  DUCKDB WRITER - STANDALONE TEST (fixed: separate files per data_type)")
    print("=" * 54)

    today = date_type(2026, 8, 1)

    print(f"\n[1] Writing 3 fake price rows for Delta/futures/BTC/{today}...")
    for i in range(3):
        append_row(
            exchange="delta", market_type="futures", symbol="BTC", date=today,
            data_type="price",
            row={"symbol": "BTCUSD", "mark_price": 63000 + i, "spot_price": 63010 + i},
        )
    print("    Done.")

    print(f"\n[2] Writing a batch of 5 fake order-book rows (separate file this time)...")
    fake_book = [
        {"symbol": "BTCUSD", "side": "buy", "price": 63000 - j, "size": 100 * j}
        for j in range(5)
    ]
    append_rows("delta", "futures", "BTC", today, "order_book", fake_book)
    print("    Done.")

    price_path = _file_path("delta", "futures", "BTC", today, "price")
    book_path = _file_path("delta", "futures", "BTC", today, "order_book")
    print(f"\n[3] Price file:      {price_path}  (exists: {price_path.exists()})")
    print(f"    Order book file: {book_path}  (exists: {book_path.exists()})")

    conn = duckdb.connect(str(price_path), read_only=True)
    price_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    conn.close()
    conn = duckdb.connect(str(book_path), read_only=True)
    book_count = conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()[0]
    conn.close()
    print(f"\n[4] Price rows: {price_count} (expect 3+)  |  Order book rows: {book_count} (expect 5+)")

    print("\n" + "=" * 54)
    print("  If both files exist with the right row counts, the fix worked.")
    print("  Next: re-run duckdb_reader.py's test (also needs a small update).")
    print("=" * 54)

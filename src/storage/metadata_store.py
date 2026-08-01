"""
MetadataStore - Step 1 of the approved storage architecture (see
ARCHITECTURE.md / the design doc this was approved from).

SQLite, deliberately - this is small, frequently-read/written metadata
(which files exist, where, and their status), not historical market data
itself. Historical data lives in DuckDB files per the approved design;
this table just tracks THEM.

Every module that archives or fetches data should consult this FIRST to
know what exists and where, before touching a StorageAdapter directly -
this is what lets the rest of the app stay ignorant of physical storage
locations, matching the design doc's core goal.

Standalone and testable on its own - doesn't depend on DuckDB or Google
Drive existing yet, just tracks whatever gets recorded.
"""
import sqlite3
import hashlib
from pathlib import Path
from datetime import date as date_type, datetime, timezone

DB_PATH = Path("/home/container/data/storage_metadata.db")
DB_PATH.parent.mkdir(parents=True, exist_ok=True)


def get_connection():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_metadata_store():
    conn = get_connection()
    conn.execute("""
        CREATE TABLE IF NOT EXISTS storage_metadata (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            exchange        TEXT NOT NULL,
            market_type     TEXT NOT NULL,
            symbol          TEXT NOT NULL,
            date            TEXT NOT NULL,
            storage_key     TEXT NOT NULL,
            provider        TEXT NOT NULL,
            status          TEXT NOT NULL,
            checksum        TEXT,
            size_bytes      INTEGER,
            compressed      INTEGER DEFAULT 0,
            download_count  INTEGER DEFAULT 0,
            last_access     TEXT,
            created_at      TEXT NOT NULL,
            UNIQUE(exchange, market_type, symbol, date)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_lookup
        ON storage_metadata (exchange, market_type, symbol, date)
    """)
    conn.commit()
    conn.close()


def compute_checksum(local_path: str) -> str:
    """SHA-256 of a file's contents - used to verify an upload round-
    tripped correctly (upload, download it back, compare checksums)."""
    sha256 = hashlib.sha256()
    with open(local_path, "rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha256.update(chunk)
    return sha256.hexdigest()


def record_upload(exchange: str, market_type: str, symbol: str, date: date_type,
                   storage_key: str, provider: str, checksum: str,
                   size_bytes: int, compressed: bool = False,
                   status: str = "pending"):
    """Records (or updates, if this exchange/symbol/date combo was
    already recorded before - e.g. a re-archive) one file's metadata."""
    conn = get_connection()
    conn.execute("""
        INSERT INTO storage_metadata
            (exchange, market_type, symbol, date, storage_key, provider,
             status, checksum, size_bytes, compressed, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(exchange, market_type, symbol, date) DO UPDATE SET
            storage_key = excluded.storage_key,
            provider = excluded.provider,
            status = excluded.status,
            checksum = excluded.checksum,
            size_bytes = excluded.size_bytes,
            compressed = excluded.compressed
    """, (
        exchange, market_type, symbol, date.isoformat(), storage_key, provider,
        status, checksum, size_bytes, int(compressed),
        datetime.now(timezone.utc).isoformat(),
    ))
    conn.commit()
    conn.close()


def mark_verified(exchange: str, market_type: str, symbol: str, date: date_type):
    conn = get_connection()
    conn.execute("""
        UPDATE storage_metadata SET status = 'verified'
        WHERE exchange = ? AND market_type = ? AND symbol = ? AND date = ?
    """, (exchange, market_type, symbol, date.isoformat()))
    conn.commit()
    conn.close()


def mark_failed(exchange: str, market_type: str, symbol: str, date: date_type):
    conn = get_connection()
    conn.execute("""
        UPDATE storage_metadata SET status = 'failed'
        WHERE exchange = ? AND market_type = ? AND symbol = ? AND date = ?
    """, (exchange, market_type, symbol, date.isoformat()))
    conn.commit()
    conn.close()


def get_location(exchange: str, market_type: str, symbol: str, date: date_type):
    """Returns a dict with storage_key/provider/status/checksum, or None
    if this data was never recorded as archived."""
    conn = get_connection()
    row = conn.execute("""
        SELECT * FROM storage_metadata
        WHERE exchange = ? AND market_type = ? AND symbol = ? AND date = ?
    """, (exchange, market_type, symbol, date.isoformat())).fetchone()
    conn.close()
    return dict(row) if row else None


def mark_accessed(exchange: str, market_type: str, symbol: str, date: date_type):
    """Called whenever a backtest actually reads a file - feeds the
    retention policy (files nobody's read in a while are safer to evict
    from the local cache first, per the approved design's Section 10)."""
    conn = get_connection()
    conn.execute("""
        UPDATE storage_metadata
        SET download_count = download_count + 1, last_access = ?
        WHERE exchange = ? AND market_type = ? AND symbol = ? AND date = ?
    """, (datetime.now(timezone.utc).isoformat(), exchange, market_type, symbol, date.isoformat()))
    conn.commit()
    conn.close()


def list_available_dates(exchange: str, market_type: str, symbol: str) -> list:
    """All dates we have (verified) archived data for a given
    exchange/market/symbol - lets a backtest know its real coverage
    before trying to fetch anything."""
    conn = get_connection()
    rows = conn.execute("""
        SELECT date FROM storage_metadata
        WHERE exchange = ? AND market_type = ? AND symbol = ? AND status = 'verified'
        ORDER BY date
    """, (exchange, market_type, symbol)).fetchall()
    conn.close()
    return [r["date"] for r in rows]


if __name__ == "__main__":
    print("=" * 54)
    print("  METADATA STORE - STANDALONE TEST")
    print("=" * 54)

    init_metadata_store()
    print("\n[1] Store initialized.")

    today = date_type(2026, 8, 1)
    print(f"\n[2] Recording a fake upload for Delta/futures/BTC/{today}...")
    record_upload(
        exchange="delta", market_type="futures", symbol="BTC", date=today,
        storage_key="delta/futures/BTC/2026/08/01/data.duckdb",
        provider="local", checksum="fake_checksum_abc123",
        size_bytes=4096, compressed=True, status="pending",
    )
    print("    Recorded.")

    print("\n[3] Marking it verified (simulating a successful round-trip check)...")
    mark_verified("delta", "futures", "BTC", today)
    loc = get_location("delta", "futures", "BTC", today)
    print(f"    Status now: {loc['status']}  |  key: {loc['storage_key']}")

    print("\n[4] Simulating a backtest reading this file 3 times...")
    for _ in range(3):
        mark_accessed("delta", "futures", "BTC", today)
    loc = get_location("delta", "futures", "BTC", today)
    print(f"    download_count = {loc['download_count']}, last_access = {loc['last_access']}")

    print("\n[5] Listing all verified dates for Delta/futures/BTC...")
    dates = list_available_dates("delta", "futures", "BTC")
    print(f"    {dates}")

    print("\n" + "=" * 54)
    print("  If all steps ran without errors, MetadataStore works.")
    print("  Next step (separate): DuckDBWriter / DuckDBReader.")
    print("=" * 54)

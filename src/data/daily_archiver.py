"""
DailyArchiver - Step 3 of the approved storage architecture (see
ARCHITECTURE.md, Section 6). Wires together MetadataStore (Step 1) and
duckdb_writer/duckdb_reader (Step 2) with the already-tested
StorageManager (Step 1 also) into the actual 7-step archive process.

Runs the full pipeline for one (exchange, market_type, symbol,
data_type, date):
  1. Confirm the local .duckdb file exists
  2. Verify integrity (open read-only, run a trivial query)
  3. Compute checksum
  4. Upload via StorageManager (currently LocalStorageAdapter - Google
     Drive is a future step; this code doesn't change when that's added)
  5. Re-download and re-checksum to verify the upload round-tripped
     correctly (catches silent corruption)
  6. Record everything in MetadataStore
  7. Retention: local copy is KEPT, not deleted

Retention decision explained: this project runs on one server with a
free 15GB disk that's nowhere near full. Auto-deleting local copies is
a much worse failure mode (permanent, silent data loss if step 4-5 had
any undetected issue) than temporarily using more disk than strictly
needed. Automatic deletion is a sensible future addition once real
usage/disk-pressure patterns are actually observed - not before.

Not yet scheduled to run automatically at UTC midnight - that's the
last remaining piece, added only after this is confirmed working via
the standalone test below (same incremental-build discipline as every
other module in this project).
"""
import sys
from pathlib import Path
from datetime import date as date_type

sys.path.append(str(Path(__file__).resolve().parents[2]))
import duckdb

from src.data.duckdb_reader import _file_path as duckdb_file_path, list_data_types_available
from src.storage.manager import storage_manager, StorageManager
from src.storage.metadata_store import (
    init_metadata_store, record_upload, mark_verified,
    compute_checksum, get_location,
)


def verify_duckdb_integrity(path) -> bool:
    """Opens read-only and runs a trivial query - if it succeeds, the
    file isn't corrupted. Cheap, real integrity check for DuckDB files."""
    try:
        conn = duckdb.connect(str(path), read_only=True)
        try:
            conn.execute("SELECT COUNT(*) FROM snapshots").fetchone()
        finally:
            conn.close()
        return True
    except Exception as e:
        print(f"    [!] Integrity check failed for {path}: {e}")
        return False


def archive_one(exchange: str, market_type: str, symbol: str, data_type: str,
                 date: date_type) -> bool:
    """Runs the full 7-step archive process for ONE file. Returns True
    only if every step succeeded and the round-trip checksum matched -
    never deletes the local file regardless of outcome."""
    local_path = duckdb_file_path(exchange, market_type, symbol, date, data_type)
    filename = f"{data_type}.duckdb"

    if not local_path.exists():
        print(f"  [skip] No local file for {exchange}/{market_type}/{symbol}/{data_type}/{date}")
        return False

    print(f"  Archiving {exchange}/{market_type}/{symbol}/{data_type}/{date}...")

    if not verify_duckdb_integrity(local_path):
        return False
    print("    [2/7] Integrity OK")

    checksum = compute_checksum(str(local_path))
    size_bytes = local_path.stat().st_size
    print(f"    [3/7] Checksum computed ({size_bytes} bytes)")

    storage_key = StorageManager.build_key(exchange, market_type, symbol, date, filename)
    uploaded = storage_manager.archive_file(
        str(local_path), exchange, market_type, symbol, date, filename=filename,
    )
    if not uploaded:
        print("    [4/7] FAILED to upload")
        record_upload(exchange, market_type, symbol, date, storage_key,
                       provider="local", checksum=checksum, size_bytes=size_bytes,
                       status="failed")
        return False
    print("    [4/7] Uploaded")

    tmp_verify_path = f"/tmp/verify_{exchange}_{symbol}_{data_type}_{date}.duckdb"
    fetched = storage_manager.adapter.download(storage_key, tmp_verify_path)
    if not fetched:
        print("    [5/7] FAILED to re-download for verification")
        record_upload(exchange, market_type, symbol, date, storage_key,
                       provider="local", checksum=checksum, size_bytes=size_bytes,
                       status="failed")
        return False
    reverify_checksum = compute_checksum(tmp_verify_path)
    if reverify_checksum != checksum:
        print(f"    [5/7] FAILED - checksum mismatch after round-trip")
        record_upload(exchange, market_type, symbol, date, storage_key,
                       provider="local", checksum=checksum, size_bytes=size_bytes,
                       status="failed")
        return False
    print("    [5/7] Round-trip verified - checksums match")

    record_upload(exchange, market_type, symbol, date, storage_key,
                   provider="local", checksum=checksum, size_bytes=size_bytes,
                   status="pending")
    mark_verified(exchange, market_type, symbol, date)
    print("    [6/7] Metadata recorded, status=verified")

    print("    [7/7] Local copy retained (no auto-delete yet, by design)")
    return True


def run_daily_archive_for_date(exchange: str, market_type: str, symbol: str,
                                 date: date_type) -> dict:
    """Archives EVERY data_type that has a local file for this date.
    Returns {data_type: True/False}."""
    data_types = list_data_types_available(exchange, market_type, symbol, date)
    if not data_types:
        print(f"  No local data types found for {exchange}/{market_type}/{symbol}/{date}")
        return {}
    return {dt: archive_one(exchange, market_type, symbol, dt, date) for dt in data_types}


if __name__ == "__main__":
    print("=" * 58)
    print("  DAILY ARCHIVER - STANDALONE TEST")
    print("  (run duckdb_writer.py's test first if you haven't, so")
    print("   there's real local data to actually archive)")
    print("=" * 58)

    init_metadata_store()
    today = date_type(2026, 8, 1)

    print(f"\nArchiving all data types for Delta/futures/BTC on {today}...\n")
    results = run_daily_archive_for_date("delta", "futures", "BTC", today)

    print("\n" + "=" * 58)
    print("  RESULTS")
    print("=" * 58)
    for data_type, success in results.items():
        print(f"  {data_type:15s} {'PASS - verified' if success else 'FAIL'}")

    if results:
        loc = get_location("delta", "futures", "BTC", today)
        print(f"\n  MetadataStore now shows: {loc}")

    print("\n" + "=" * 58)
    print("  If all show PASS, the full pipeline works end to end:")
    print("  write -> verify -> checksum -> upload -> re-verify ->")
    print("  record metadata. Scheduling this for real UTC-midnight")
    print("  runs is the next step, once this is confirmed.")
    print("=" * 58)

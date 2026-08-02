"""
DuckDBReader - Step 2 of the approved storage architecture (paired with
duckdb_writer.py).

At THIS stage, read_range() only looks at files already present locally
(today's live file, plus anything not yet cleaned up from previous
days). It does NOT yet reach out to Google Drive for older archived
data - that's what CacheManager (Step 5) adds on top of this. Built this
way deliberately, matching the approved build order: each piece is
useful and testable on its own before the next one wires in on top.

FIX (2026-08-01): this reader originally expected a single file per day
at ".../YYYY/MM/DD.duckdb" - but duckdb_writer.py was fixed (earlier the
same day) to write one file PER DATA TYPE inside a per-day FOLDER
instead (".../YYYY/MM/DD/{data_type}.duckdb", e.g. "price.duckdb",
"order_book.duckdb") after testing caught a schema-mismatch bug from
mixing data types in one table. The reader was never updated to match,
so as written it silently found nothing - path.exists() correctly
returned False for a path the writer never created, and read_range()
would report every date as "missing" rather than erroring, which would
have been easy to miss. Caught by re-reading both files side by side
rather than testing this one in isolation. Now takes a data_type
parameter and matches the writer's actual (tested, working) layout.
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import date as date_type, timedelta

CACHE_DIR = Path("/home/container/data_cache")


def _file_path(exchange: str, market_type: str, symbol: str, date: date_type,
                data_type: str) -> Path:
    """Matches duckdb_writer.py's actual layout exactly:
    CACHE_DIR/exchange/market_type/symbol/YYYY/MM/DD/{data_type}.duckdb"""
    return (
        CACHE_DIR / exchange / market_type / symbol
        / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}"
        / f"{data_type}.duckdb"
    )


def read_range(exchange: str, market_type: str, symbol: str, data_type: str,
               start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """Reads every locally-available day's file for ONE data_type in
    [start_date, end_date] and returns them combined as one DataFrame.
    Missing days (not written yet, or archived remotely and not yet
    fetched - see module docstring) are silently skipped, not errors -
    but ARE reported via the [info] line below, unlike before this fix,
    where "silently skipped" also meant "no visibility that anything
    was skipped at all"."""
    frames = []
    current = start_date
    missing_dates = []

    while current <= end_date:
        path = _file_path(exchange, market_type, symbol, current, data_type)
        if path.exists():
            conn = duckdb.connect(str(path), read_only=True)
            try:
                frames.append(conn.execute("SELECT * FROM snapshots").fetchdf())
            except Exception as e:
                print(f"  [!] Failed to read {path}: {e}")
            finally:
                conn.close()
        else:
            missing_dates.append(current)
        current += timedelta(days=1)

    if missing_dates:
        if len(missing_dates) > 1:
            print(f"  [info] {len(missing_dates)} date(s) not available locally yet "
                  f"for data_type='{data_type}' (not yet written, or archived "
                  f"remotely and not fetched - remote fetch comes with "
                  f"CacheManager, a later step): {missing_dates[0]}..{missing_dates[-1]}")
        else:
            print(f"  [info] {missing_dates[0]} not available locally yet "
                  f"for data_type='{data_type}'")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def list_local_dates(exchange: str, market_type: str, symbol: str, data_type: str) -> list:
    """Every date we have a local file for THIS data_type, regardless of
    whether it's been archived yet - useful for a quick 'what do I
    actually have' check without querying MetadataStore."""
    base = CACHE_DIR / exchange / market_type / symbol
    if not base.exists():
        return []
    dates = []
    for year_dir in sorted(base.iterdir()):
        if not year_dir.is_dir():
            continue
        for month_dir in sorted(year_dir.iterdir()):
            if not month_dir.is_dir():
                continue
            for day_dir in sorted(month_dir.iterdir()):
                if not day_dir.is_dir():
                    continue
                if (day_dir / f"{data_type}.duckdb").exists():
                    try:
                        d = date_type(int(year_dir.name), int(month_dir.name), int(day_dir.name))
                        dates.append(d)
                    except ValueError:
                        continue
    return dates


def list_data_types_available(exchange: str, market_type: str, symbol: str,
                                date: date_type) -> list:
    """What data types (price, order_book, funding_rate, ...) actually
    have a file for this specific day - useful before calling
    read_range() with a data_type that might not exist for that date."""
    day_dir = (
        CACHE_DIR / exchange / market_type / symbol
        / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}"
    )
    if not day_dir.exists():
        return []
    return [p.stem for p in day_dir.glob("*.duckdb")]


if __name__ == "__main__":
    print("=" * 54)
    print("  DUCKDB READER - STANDALONE TEST (fixed: matches writer's layout)")
    print("=" * 54)
    print("  (run duckdb_writer.py's test first if you haven't, so")
    print("   there's real data here to read back)")

    today = date_type(2026, 8, 1)

    print(f"\n[1] Reading Delta/futures/BTC 'price' data for {today}..{today}...")
    df = read_range("delta", "futures", "BTC", "price", today, today)
    print(f"    Got {len(df)} rows")
    if not df.empty:
        print(f"    Columns: {list(df.columns)}")
        print(f"    First row:\n{df.iloc[0].to_dict()}")
    else:
        print("    Got 0 rows - if you already ran duckdb_writer.py's test, "
              "this itself would indicate a bug (that's exactly the class "
              "of bug this fix addresses).")

    print(f"\n[2] Reading 'order_book' data for the same day...")
    df2 = read_range("delta", "futures", "BTC", "order_book", today, today)
    print(f"    Got {len(df2)} rows")

    print(f"\n[3] Listing which data types exist at all for {today}...")
    types = list_data_types_available("delta", "futures", "BTC", today)
    print(f"    {types}")

    print(f"\n[4] Listing all local dates available for 'price' data...")
    dates = list_local_dates("delta", "futures", "BTC", "price")
    print(f"    {dates}")

    print(f"\n[5] Reading a range that includes a missing date (yesterday)...")
    yesterday = today - timedelta(days=1)
    df3 = read_range("delta", "futures", "BTC", "price", yesterday, today)
    print(f"    Got {len(df3)} rows total (should show an [info] message above "
          f"about the missing date, and still return today's data)")

    print("\n" + "=" * 54)
    print("  Steps [1] and [2] should show 3+ and 5+ rows (matching")
    print("  duckdb_writer's test data). If so, reader/writer are now")
    print("  actually compatible. Next: DailyArchiver (Step 3).")
    print("=" * 54)

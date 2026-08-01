"""
DuckDBReader - Step 2 of the approved storage architecture (paired with
duckdb_writer.py).

At THIS stage, read_range() only looks at files already present locally
(today's live file, plus anything not yet cleaned up from previous
days). It does NOT yet reach out to Google Drive for older archived
data - that's what CacheManager (Step 5) adds on top of this. Built this
way deliberately, matching the approved build order: each piece is
useful and testable on its own before the next one wires in on top.
"""
import duckdb
import pandas as pd
from pathlib import Path
from datetime import date as date_type, timedelta

CACHE_DIR = Path("/home/container/data_cache")


def _file_path(exchange: str, market_type: str, symbol: str, date: date_type) -> Path:
    return (
        CACHE_DIR / exchange / market_type / symbol
        / f"{date.year:04d}" / f"{date.month:02d}" / f"{date.day:02d}.duckdb"
    )


def read_range(exchange: str, market_type: str, symbol: str,
               start_date: date_type, end_date: date_type) -> pd.DataFrame:
    """Reads every locally-available day's file in [start_date, end_date]
    and returns them combined as one DataFrame. Missing days (not
    written yet, or not yet fetched from remote archive - see module
    docstring) are silently skipped, not errors."""
    frames = []
    current = start_date
    missing_dates = []

    while current <= end_date:
        path = _file_path(exchange, market_type, symbol, current)
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
        print(f"  [info] {len(missing_dates)} date(s) not available locally yet "
              f"(not yet written, or archived remotely and not fetched - "
              f"remote fetch comes with CacheManager, a later step): "
              f"{missing_dates[0]}..{missing_dates[-1]}" if len(missing_dates) > 1
              else f"  [info] {missing_dates[0]} not available locally yet")

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def list_local_dates(exchange: str, market_type: str, symbol: str) -> list:
    """Every date we have a local file for, regardless of whether it's
    been archived yet - useful for a quick 'what do I actually have'
    check without querying MetadataStore."""
    base = CACHE_DIR / exchange / market_type / symbol
    if not base.exists():
        return []
    dates = []
    for year_dir in sorted(base.iterdir()):
        for month_dir in sorted(year_dir.iterdir()) if year_dir.is_dir() else []:
            for day_file in sorted(month_dir.glob("*.duckdb")) if month_dir.is_dir() else []:
                try:
                    d = date_type(int(year_dir.name), int(month_dir.name),
                                   int(day_file.stem))
                    dates.append(d)
                except ValueError:
                    continue
    return dates


if __name__ == "__main__":
    print("=" * 54)
    print("  DUCKDB READER - STANDALONE TEST")
    print("=" * 54)
    print("  (run duckdb_writer.py's test first if you haven't, so")
    print("   there's real data here to read back)")

    today = date_type(2026, 8, 1)

    print(f"\n[1] Reading Delta/futures/BTC for {today}..{today}...")
    df = read_range("delta", "futures", "BTC", today, today)
    print(f"    Got {len(df)} rows")
    if not df.empty:
        print(f"    Columns: {list(df.columns)}")
        print(f"    First row:\n{df.iloc[0].to_dict()}")

    print(f"\n[2] Listing all local dates available for Delta/futures/BTC...")
    dates = list_local_dates("delta", "futures", "BTC")
    print(f"    {dates}")

    print(f"\n[3] Reading a range that includes a missing date (yesterday)...")
    yesterday = today - timedelta(days=1)
    df2 = read_range("delta", "futures", "BTC", yesterday, today)
    print(f"    Got {len(df2)} rows total (should show an [info] message above "
          f"about the missing date, and still return today's data)")

    print("\n" + "=" * 54)
    print("  If step [1] shows real rows matching duckdb_writer's test")
    print("  data, DuckDBReader works. Next: DailyArchiver (Step 3).")
    print("=" * 54)

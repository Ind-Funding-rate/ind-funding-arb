"""
Market data store - Parquet-based, for backtest speed AND low storage.

WHY PARQUET (not SQLite/CSV): Parquet is a columnar format - a backtest
reading "just the bid/ask columns for BTC over 3 days" only reads those
columns, not whole rows, and typically compresses numeric time-series
data 5-10x smaller than CSV. This is the standard format real market-data
systems use for exactly this reason.

STORAGE SAFETY: this is designed to avoid the disk-exhaustion crashes
this project has already hit more than once tonight. Two safeguards:
  1. Snapshots, not tick data - each data type is captured periodically
     (see CADENCE below), not on every single change. A live order book
     changes many times per second; storing every change would be huge
     and isn't needed for backtesting.
  2. check_disk_usage() actively monitors total size on disk and returns
     a warning before things get out of hand - callers should check this
     before writing.

CADENCE (as agreed):
    funding_rate : every 30-60 min (funding itself only updates every
                   4-8h, so frequent sampling adds no real information)
    price         : every 5 min
    order_book    : every 5 min, top 5-10 depth levels only (not full book)
    option_chain  : every 5 min, major coins only (BTC/ETH) - most
                   exchanges don't offer options on every coin anyway

All four are written as separate Parquet datasets, partitioned by date,
so old data is easy to find/prune and queries can skip whole files that
are outside the requested date range instead of scanning everything.
"""
import shutil
from pathlib import Path
from datetime import datetime, timezone
import pandas as pd

DATA_ROOT = Path("/home/container/data/market_data")
DATA_ROOT.mkdir(parents=True, exist_ok=True)

# Warn once total usage crosses this - well under the 15 GiB disk limit,
# leaving headroom for everything else (SQLite stores, logs, code, etc.)
DISK_WARNING_GB = 8.0


def _partition_path(data_type: str, exchange: str, date: datetime = None) -> Path:
    date = date or datetime.now(timezone.utc)
    folder = DATA_ROOT / data_type / exchange / date.strftime("%Y-%m-%d")
    folder.mkdir(parents=True, exist_ok=True)
    return folder / f"{date.strftime('%H%M%S')}.parquet"


def write_snapshot(data_type: str, exchange: str, rows: list):
    """rows: list of dicts, all with the same keys. Writes one small
    Parquet file per snapshot (one per exchange per data_type per
    capture time) - simple, append-only, no read-modify-write needed."""
    if not rows:
        return
    df = pd.DataFrame(rows)
    path = _partition_path(data_type, exchange)
    df.to_parquet(path, engine="pyarrow", compression="snappy", index=False)


def read_range(data_type: str, exchange: str, start: datetime, end: datetime) -> pd.DataFrame:
    """Reads every snapshot file for one exchange/data_type within a date
    range and concatenates them - this is what a backtest calls. Only
    opens files for dates actually in range, not the whole dataset."""
    frames = []
    current = start
    base = DATA_ROOT / data_type / exchange
    if not base.exists():
        return pd.DataFrame()

    while current.date() <= end.date():
        day_folder = base / current.strftime("%Y-%m-%d")
        if day_folder.exists():
            for f in sorted(day_folder.glob("*.parquet")):
                try:
                    frames.append(pd.read_parquet(f, engine="pyarrow"))
                except Exception as e:
                    print(f"  [!] Failed to read {f}: {e}")
        current = current.replace(hour=0, minute=0, second=0) + pd.Timedelta(days=1)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True)


def check_disk_usage() -> dict:
    """Returns current usage and whether it's crossed the warning
    threshold - call this periodically (e.g. once per ingestion cycle)
    rather than finding out the disk is full after the fact."""
    total_bytes = sum(f.stat().st_size for f in DATA_ROOT.rglob("*.parquet"))
    total_gb = total_bytes / (1024 ** 3)
    return {
        "total_gb": round(total_gb, 3),
        "warning": total_gb > DISK_WARNING_GB,
        "warning_threshold_gb": DISK_WARNING_GB,
    }


def prune_older_than(data_type: str, days: int):
    """Deletes snapshot folders older than N days for one data type -
    a manual/scheduled safety valve if storage does get tight. Not run
    automatically; call this explicitly when needed."""
    cutoff = datetime.now(timezone.utc) - pd.Timedelta(days=days)
    base = DATA_ROOT / data_type
    if not base.exists():
        return 0
    removed = 0
    for exchange_dir in base.iterdir():
        if not exchange_dir.is_dir():
            continue
        for day_dir in exchange_dir.iterdir():
            try:
                day_date = datetime.strptime(day_dir.name, "%Y-%m-%d").replace(tzinfo=timezone.utc)
            except ValueError:
                continue
            if day_date < cutoff:
                shutil.rmtree(day_dir)
                removed += 1
    return removed


if __name__ == "__main__":
    usage = check_disk_usage()
    print(f"Market data on disk: {usage['total_gb']} GB "
          f"({'WARNING - approaching limit' if usage['warning'] else 'OK'})")

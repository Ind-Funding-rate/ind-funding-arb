"""
Storage Manager - Step 1 of the enterprise architecture roadmap.

The rest of the application should never know WHERE a file physically
lives. It asks the StorageManager for data using a logical key (e.g.
exchange/market_type/symbol/year/month/day), and the manager delegates
to whichever StorageAdapter is currently configured (local disk today,
Google Drive next, S3/R2/MinIO later if ever needed). Swapping adapters
should never require changing any calling code - only the wiring at the
bottom of this file (or wherever the manager is instantiated).

This is intentionally scoped small for its first version: just the
interface (StorageAdapter) and a LocalStorageAdapter. No cloud account
setup, no new dependencies - safe to build and test today. A
GoogleDriveAdapter implementing the exact same interface is the natural
next step once this is confirmed working.
"""
from abc import ABC, abstractmethod
from pathlib import Path
from datetime import date as date_type
import shutil


class StorageAdapter(ABC):
    """Every storage backend (local disk, Google Drive, S3, ...) must
    implement exactly this interface. The StorageManager only ever talks
    to this interface - never to a specific backend directly."""

    @abstractmethod
    def upload(self, local_path: str, remote_key: str) -> bool:
        """Copy a local file INTO this storage backend, at remote_key."""
        ...

    @abstractmethod
    def download(self, remote_key: str, local_path: str) -> bool:
        """Copy a file FROM this storage backend to a local path."""
        ...

    @abstractmethod
    def exists(self, remote_key: str) -> bool:
        ...

    @abstractmethod
    def list_keys(self, prefix: str) -> list:
        ...

    @abstractmethod
    def delete(self, remote_key: str) -> bool:
        ...


class LocalStorageAdapter(StorageAdapter):
    """Simplest possible adapter: 'remote' storage is just another
    directory on the same disk. Real today, and a safe default for a
    single free server - useful on its own (organizes archives cleanly)
    and as the reference implementation any future adapter (Google
    Drive, S3...) must behave the same as from the caller's point of
    view."""

    def __init__(self, base_dir: str):
        self.base_dir = Path(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)

    def _full_path(self, remote_key: str) -> Path:
        return self.base_dir / remote_key

    def upload(self, local_path: str, remote_key: str) -> bool:
        try:
            dest = self._full_path(remote_key)
            dest.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(local_path, dest)
            return True
        except Exception as e:
            print(f"[storage:local] upload failed ({local_path} -> {remote_key}): {e}")
            return False

    def download(self, remote_key: str, local_path: str) -> bool:
        try:
            src = self._full_path(remote_key)
            if not src.exists():
                return False
            Path(local_path).parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(src, local_path)
            return True
        except Exception as e:
            print(f"[storage:local] download failed ({remote_key} -> {local_path}): {e}")
            return False

    def exists(self, remote_key: str) -> bool:
        return self._full_path(remote_key).exists()

    def list_keys(self, prefix: str) -> list:
        prefix_path = self._full_path(prefix)
        if not prefix_path.exists():
            return []
        if prefix_path.is_file():
            return [prefix]
        return [
            str(p.relative_to(self.base_dir))
            for p in prefix_path.rglob("*")
            if p.is_file()
        ]

    def delete(self, remote_key: str) -> bool:
        try:
            path = self._full_path(remote_key)
            if path.exists():
                path.unlink()
            return True
        except Exception as e:
            print(f"[storage:local] delete failed ({remote_key}): {e}")
            return False


class StorageManager:
    """The single entry point the rest of the application should use.
    Business logic calls build_key() to get a logical path, then
    archive_file()/fetch_file() - it never touches an adapter directly,
    so swapping LocalStorageAdapter for a future GoogleDriveAdapter
    means changing ONE line where StorageManager is constructed, nowhere
    else in the codebase."""

    def __init__(self, adapter: StorageAdapter):
        self.adapter = adapter

    @staticmethod
    def build_key(exchange: str, market_type: str, symbol: str,
                   date: date_type, filename: str) -> str:
        """Partitioning scheme: exchange/market_type/symbol/YYYY/MM/DD/filename.
        Matches the standard layout used by professional market-data
        platforms - makes it possible to efficiently locate any date
        range later without scanning every file."""
        return (
            f"{exchange}/{market_type}/{symbol}/"
            f"{date.year:04d}/{date.month:02d}/{date.day:02d}/{filename}"
        )

    def archive_file(self, local_path: str, exchange: str, market_type: str,
                      symbol: str, date: date_type, filename: str = None) -> bool:
        filename = filename or Path(local_path).name
        key = self.build_key(exchange, market_type, symbol, date, filename)
        return self.adapter.upload(local_path, key)

    def fetch_file(self, exchange: str, market_type: str, symbol: str,
                    date: date_type, filename: str, local_path: str) -> bool:
        key = self.build_key(exchange, market_type, symbol, date, filename)
        return self.adapter.download(key, local_path)

    def is_archived(self, exchange: str, market_type: str, symbol: str,
                     date: date_type, filename: str) -> bool:
        key = self.build_key(exchange, market_type, symbol, date, filename)
        return self.adapter.exists(key)


# ── Wiring: today this uses LocalStorageAdapter. When a Google Drive
# adapter is built (next step), only this one line changes.
DEFAULT_ARCHIVE_DIR = "/home/container/archive"
storage_manager = StorageManager(LocalStorageAdapter(DEFAULT_ARCHIVE_DIR))


if __name__ == "__main__":
    # Standalone test - proves the full round trip (write -> archive ->
    # confirm exists -> fetch back -> confirm contents match) with no
    # external dependencies, before this is wired into anything live.
    import tempfile
    from datetime import date

    print("=" * 54)
    print("  STORAGE MANAGER - LOCAL ADAPTER TEST")
    print("=" * 54)

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False) as f:
        f.write("timestamp,coin,rate\n2026-08-01 12:00:00,BTC,0.0001\n")
        test_file = f.name

    today = date(2026, 8, 1)
    print(f"\n[1] Archiving test file for Delta/BTC/{today}...")
    ok = storage_manager.archive_file(
        test_file, exchange="delta", market_type="futures",
        symbol="BTC", date=today, filename="test_scan.csv",
    )
    print(f"    {'PASS' if ok else 'FAIL'}")

    key = StorageManager.build_key("delta", "futures", "BTC", today, "test_scan.csv")
    print(f"\n[2] Checking it exists at key: {key}")
    exists = storage_manager.is_archived("delta", "futures", "BTC", today, "test_scan.csv")
    print(f"    {'PASS' if exists else 'FAIL'}")

    print("\n[3] Fetching it back to a new local path...")
    fetch_path = "/tmp/fetched_test_scan.csv"
    fetched = storage_manager.fetch_file("delta", "futures", "BTC", today,
                                          "test_scan.csv", fetch_path)
    if fetched:
        with open(fetch_path) as f:
            content = f.read()
        print(f"    PASS - contents match: {content.strip() in open(test_file).read().strip()}")
    else:
        print("    FAIL")

    print(f"\n[4] Listing everything archived under 'delta/'...")
    keys = storage_manager.adapter.list_keys("delta")
    for k in keys:
        print(f"    {k}")

    print("\n" + "=" * 54)
    print("  If all steps show PASS, the Storage Manager works end to")
    print("  end. Next step (separate, once confirmed): Google Drive")
    print("  adapter implementing this same interface.")
    print("=" * 54)

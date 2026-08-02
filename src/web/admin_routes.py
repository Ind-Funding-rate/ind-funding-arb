"""
Admin test runner - lets standalone test/data scripts be triggered
WITHOUT stopping the live app.py process (website, scanners, executor
all keep running). Solves a real operational problem: testing anything
new used to require switching HidenCloud's APP PY FILE and restarting,
which took the whole live site down for the duration.

How it works: runs a script's own `if __name__ == "__main__":` block in
a background thread inside the SAME running process, via runpy. Output
(print statements) shows up in the same HidenCloud console feed as the
live app's own logs - interleaved, not separated, which is a real minor
downside worth knowing, but the live site never goes down to see it.

SECURITY NOTE: only scripts in TEST_SCRIPTS below can be run - this is
a whitelist, not "run any file path the request sends". This is a
single-user, unauthenticated internal tool on a private server with no
other users, so this is a proportionate safeguard (prevents this
becoming an arbitrary-code-execution endpoint if the URL is ever
stumbled on), not full auth - that would be over-engineering for the
current single-user reality, matching the same judgment applied
elsewhere in this project.
"""
import runpy
import threading
from datetime import datetime

TEST_SCRIPTS = {
    "storage_manager": "src/storage/manager.py",
    "metadata_store": "src/storage/metadata_store.py",
    "duckdb_writer": "src/data/duckdb_writer.py",
    "duckdb_reader": "src/data/duckdb_reader.py",
    "daily_archiver": "src/data/daily_archiver.py",
    "coin_universe": "src/data/coin_universe.py",
    "coinswitch_auth": "src/data/test_coinswitch_auth.py",
    "pi42_usdt_channel": "src/data/test_pi42_usdt_channel.py",
}

_test_status = {}


def _run_script(name, path):
    _test_status[name] = {
        "running": True, "started_at": datetime.now().isoformat(),
        "finished_at": None, "error": None,
    }
    print(f"\n[admin-test] ===== Running: {name} ({path}) =====")
    try:
        runpy.run_path(path, run_name="__main__")
        print(f"[admin-test] ===== Finished OK: {name} =====\n")
    except Exception as e:
        print(f"[admin-test] ===== FAILED: {name}: {e} =====\n")
        _test_status[name]["error"] = str(e)
    finally:
        _test_status[name]["running"] = False
        _test_status[name]["finished_at"] = datetime.now().isoformat()


def start_test(name: str):
    if name not in TEST_SCRIPTS:
        return False, f"Unknown script '{name}'. Available: {list(TEST_SCRIPTS.keys())}"
    if _test_status.get(name, {}).get("running"):
        return False, f"'{name}' is already running"
    threading.Thread(target=_run_script, args=(name, TEST_SCRIPTS[name]), daemon=True).start()
    return True, f"Started '{name}' - check the HidenCloud console for output"


def get_status(name: str = None):
    if name:
        return _test_status.get(name, {"running": False, "started_at": None,
                                        "finished_at": None, "error": None})
    return _test_status

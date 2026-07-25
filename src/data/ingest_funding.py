"""
Ingestion pipeline - the part that actually talks to exchanges. Runs on
its own schedule in the background, writes into funding_history_store.py.
Designed so adding more exchanges later (the plan to scale toward
30-40, like loris.tools) is just adding one more entry to EXCHANGES and
one fetch function - nothing else needs to change.

Uses a thread pool to fetch many (exchange, coin) pairs CONCURRENTLY
instead of one at a time sequentially - this is the main speed fix.
Fetching 200 coins x 2 exchanges sequentially at ~1.5s each takes ~10
minutes; with 15 concurrent workers it takes closer to 40 seconds.
"""
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.data.historical_funding import get_history
from src.data.funding_history_store import store_history, init_store

# Exchanges currently supported for ingestion. Adding a new one later:
# add its name here and make sure historical_funding.get_history() has a
# fetcher for it - the ingestion/storage/scan logic below doesn't change.
EXCHANGES = ["bybit", "okx"]  # binance excluded - unreliable from this server

MAX_WORKERS = 15  # concurrent fetches - keep moderate to be a good API citizen


def ingest_one(exchange: str, coin: str, days: int):
    """Fetches one (exchange, coin) history and stores it. Returns True
    on success (even if zero rows - that's a valid "not listed" result),
    False only on an actual fetch error."""
    try:
        rows = get_history(exchange, coin, days)
        store_history(exchange, coin, rows)
        return True
    except Exception as e:
        print(f"  [!] Ingest failed for {exchange}/{coin}: {e}")
        return False


def ingest_all(coins: list, days: int = 30, progress_callback=None):
    """Ingests every (exchange, coin) combination concurrently. This is
    the slow part (real network calls) but it's now parallelized and
    fully decoupled from the scan/ranking step, which just reads the
    local store afterward - so page loads stay fast regardless of how
    long ingestion takes or how many exchanges are added later."""
    init_store()
    tasks = [(exchange, coin) for exchange in EXCHANGES for coin in coins]
    total = len(tasks)
    done = 0

    start = time.time()
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as pool:
        futures = {pool.submit(ingest_one, ex, coin, days): (ex, coin) for ex, coin in tasks}
        for future in as_completed(futures):
            done += 1
            if progress_callback:
                progress_callback(done, total)

    elapsed = time.time() - start
    print(f"  Ingestion complete: {total} (exchange, coin) pairs in {elapsed:.1f}s "
          f"({MAX_WORKERS} concurrent workers)")

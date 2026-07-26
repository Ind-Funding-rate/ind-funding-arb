"""
3-Way Funding Gap Scanner (Delta + Pi42 + CoinSwitch)
-------------------------------------------------------
Standalone. Does NOT touch full_market_scanner.py, its CSV logs, its
Telegram alerts, or the website - all of that keeps working exactly as
before, untouched, while this is tested independently.

For every coin, fetches funding rates from all 3 exchanges (where
available) and checks all 3 possible pairs:
    Delta <-> Pi42
    Delta <-> CoinSwitch
    Pi42  <-> CoinSwitch
...then reports whichever pair has the best fee-adjusted net% for that
coin. Detection/logging only - places NO orders.

Reuses the existing, already-proven Delta and Pi42 fetchers from
full_market_scanner.py rather than duplicating that logic.
"""
import sys
import csv
import time
from pathlib import Path
from datetime import datetime

sys.path.append(str(Path(__file__).resolve().parents[2]))
from src.execution.full_market_scanner import (
    COINS, get_delta_funding_all, get_pi42_funding_all,
)
from src.data.coinswitch_client import get_all_funding_rates, strip_multiplier_prefix \
    if False else None  # placeholder, real import below

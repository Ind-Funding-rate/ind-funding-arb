"""
Standalone diagnostic - checks whether Delta, Pi42, or CoinSwitch list
any US-stock perpetuals (TSLA, AAPL, NVDA, etc.) before building the
"Cross-Exchange US Stock Funding Arbitrage" strategy on top of them.

We already know Bybit/OKX (the GLOBAL exchanges) list tokenized stock
perpetuals - full_market_scanner's coin universe discovery explicitly
filters ~26 of them out (IBM, TSLA, ADBE, etc.) as "likely tokenized
stocks/ETFs, not crypto majors". That filtering happened for Bybit/OKX
specifically - it says nothing about whether Delta, Pi42, or CoinSwitch
(the three exchanges this new strategy is meant for) list this asset
class at all. This script checks the actual symbol lists directly
rather than assuming.
"""
import sys
import re
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.execution.full_market_scanner import get_delta_funding_all
from src.data.coin_universe import get_pi42_all_symbols
from src.data.coinswitch_client import get_all_funding_rates

# A sample of well-known US stock/ETF tickers to check for, plus a
# generic pattern match for common tokenized-stock naming conventions.
KNOWN_US_STOCKS = {
    "TSLA", "AAPL", "NVDA", "META", "GOOGL", "GOOG", "MSFT", "AMZN",
    "IBM", "ADBE", "CSCO", "INTC", "AMD", "NFLX", "DIS", "BA", "JPM",
    "V", "MA", "WMT", "KO", "PEP", "XOM", "CVX", "PFE", "JNJ",
    "HYUNDAI", "DKNG", "BX", "BZ", "CIEN", "CRDO", "GEV", "HPE",
}


def strip_prefix(sym):
    return re.sub(r"^(1000|1M)", "", sym)


def check_exchange(name, symbols):
    print(f"\n{'=' * 50}")
    print(f"  {name}: {len(symbols)} total symbols")
    print("=" * 50)
    matches = [s for s in symbols if strip_prefix(s).upper() in KNOWN_US_STOCKS]
    if matches:
        print(f"  FOUND {len(matches)} likely US-stock perpetuals:")
        print(f"  {matches}")
    else:
        print("  No matches against known US stock tickers.")
    print(f"  Full symbol list (first 40): {sorted(symbols)[:40]}")


if __name__ == "__main__":
    print("Fetching Delta's full symbol list...")
    delta_symbols = list(get_delta_funding_all().keys())
    check_exchange("DELTA", delta_symbols)

    print("\nFetching Pi42's full symbol list...")
    pi42_symbols = get_pi42_all_symbols()
    check_exchange("PI42", pi42_symbols)

    print("\nFetching CoinSwitch's full symbol list...")
    cs_data = get_all_funding_rates()
    cs_symbols = [sym[:-4] for sym in cs_data.keys() if sym.endswith("USDT")]
    check_exchange("COINSWITCH", cs_symbols)

    print("\n" + "=" * 50)
    print("  CONCLUSION")
    print("=" * 50)
    print("  If all three show 0 matches, the 'US Stock Funding Arb'")
    print("  strategy as described is not applicable to these exchanges -")
    print("  they may simply not offer tokenized US stock perpetuals.")

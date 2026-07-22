"""
Analyze logged funding-gap cycles to answer the real question:
does this gap consistently hold up after fees, or was a given reading
a fluke? Run this after letting the executor run for a day or more.

Usage (on the HidenCloud console, or locally):
    python src/execution/analyze_cycles.py
    python src/execution/analyze_cycles.py 2026-07-22   (a specific day)
"""
import csv
import sys
from pathlib import Path
from collections import defaultdict

LOG_DIR = Path("/home/container/logs")


def load_rows(date_filter=None):
    rows = []
    pattern = f"cycles_{date_filter}.csv" if date_filter else "cycles_*.csv"
    for log_file in sorted(LOG_DIR.glob(pattern)):
        with open(log_file, newline="") as f:
            reader = csv.DictReader(f)
            for row in reader:
                rows.append(row)
    return rows


def analyze(rows):
    if not rows:
        print("No data found yet. Let the executor run longer, or check the logs/ folder.")
        return

    total = len(rows)
    profitable_rows = [r for r in rows if r["profitable"] == "True"]
    profitable_count = len(profitable_rows)

    gaps = [float(r["gap_pct"]) for r in rows]
    net_edges = [float(r["net_edge_pct"]) for r in rows]

    avg_gap = sum(gaps) / total
    avg_net = sum(net_edges) / total
    min_net = min(net_edges)
    max_net = max(net_edges)

    # How often did the direction flip? Frequent flipping = less stable
    # arbitrage, since a real hedge held across a flip loses the benefit.
    directions = [r["direction"] for r in rows if r["direction"]]
    direction_changes = sum(
        1 for i in range(1, len(directions)) if directions[i] != directions[i - 1]
    )

    print("=" * 54)
    print("  FUNDING GAP ANALYSIS")
    print("=" * 54)
    print(f"  Total cycles logged     : {total}")
    print(f"  Profitable cycles       : {profitable_count} "
          f"({profitable_count/total*100:.1f}%)")
    print(f"  Average gap             : {avg_gap:.4f} pp")
    print(f"  Average net edge        : {avg_net:+.4f}%")
    print(f"  Best net edge seen      : {max_net:+.4f}%")
    print(f"  Worst net edge seen     : {min_net:+.4f}%")
    print(f"  Direction changes       : {direction_changes} "
          f"(across {len(directions)} profitable readings)")
    print("=" * 54)

    if profitable_count / total < 0.5:
        print("  NOTE: Profitable less than half the time - the gap may not")
        print("  be reliable enough to trade on consistently.")
    if direction_changes > total * 0.1:
        print("  NOTE: Direction flipped often - a held position could end up")
        print("  on the wrong side of the gap partway through.")


if __name__ == "__main__":
    date_filter = sys.argv[1] if len(sys.argv) > 1 else None
    rows = load_rows(date_filter)
    analyze(rows)

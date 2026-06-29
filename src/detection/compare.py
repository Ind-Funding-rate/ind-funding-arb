"""
Funding-rate gap comparison.

Single responsibility: given the latest known reading for Pi42 and Delta,
work out how far apart their funding rates are right now. Doesn't know
about WebSockets, databases, or display - just the math. No thresholds
or "is this an opportunity" judgment yet - that comes later.
"""


def compute_gap(pi42_entry, delta_entry):
    """
    pi42_entry / delta_entry: dicts like {"funding_rate": ..., "mark_price": ...},
    or None if no data has arrived yet for that exchange.

    Returns a dict describing the gap, or None if either side doesn't have
    a funding_rate to compare yet.
    """
    if not pi42_entry or not delta_entry:
        return None

    pi42_rate = pi42_entry.get("funding_rate")
    delta_rate = delta_entry.get("funding_rate")

    if pi42_rate is None or delta_rate is None:
        return None

    gap = delta_rate - pi42_rate  # positive => Delta's rate is the higher one

    if gap > 0:
        higher_exchange = "Delta"
    elif gap < 0:
        higher_exchange = "Pi42"
    else:
        higher_exchange = "Equal"

    return {
        "pi42_rate": pi42_rate,
        "delta_rate": delta_rate,
        "gap": gap,
        "gap_pct": gap * 100,  # as a percentage point difference
        "higher_exchange": higher_exchange,
    }

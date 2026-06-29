"""
Fee-adjusted opportunity assessment.

Single responsibility: given a raw rate gap (from detection.compare),
work out whether it's actually worth anything once real trading fees are
subtracted. Doesn't place orders and doesn't decide what counts as "good
enough" - just shows the honest breakeven math so you can judge for
yourself.

FEES - confirmed from each exchange's own official fee pages, lowest
(VIP 0 / tier 0) rates, TAKER orders (the conservative assumption, since
fast arbitrage entries are usually market orders, not resting limit
orders):
    Pi42:  0.080% taker + 18% GST  -> 0.0944% per trade
    Delta: 0.050% taker + 18% GST  -> 0.0590% per trade

A full round trip = open AND close, on BOTH exchanges = 4 trades total.

FUNDING INTERVAL ASSUMPTION:
    Delta confirms 8-hour funding for BTCUSD. Pi42's own materials say
    funding happens "every 4 or 8 hours depending on the contract" without
    giving a contract-by-contract table, but reference the same standard
    5:30am/1:30pm/9:30pm IST schedule Delta uses. We're assuming 8 hours
    for Pi42's BTCINR too - this hasn't been independently confirmed and
    is worth double-checking by watching a real funding reset happen.
    If it's actually 4 hours, "breakeven_payouts" below is still correct;
    only "breakeven_hours" would need correcting (halved).
"""

PI42_TAKER_FEE_PCT = 0.080
DELTA_TAKER_FEE_PCT = 0.050
GST_MULTIPLIER = 1.18

PI42_TAKER_WITH_GST_PCT = PI42_TAKER_FEE_PCT * GST_MULTIPLIER
DELTA_TAKER_WITH_GST_PCT = DELTA_TAKER_FEE_PCT * GST_MULTIPLIER

# Open + close, on both exchanges = 4 trades total for one round trip hedge
ROUND_TRIP_FEE_PCT = 2 * (PI42_TAKER_WITH_GST_PCT + DELTA_TAKER_WITH_GST_PCT)

ASSUMED_FUNDING_INTERVAL_HOURS = 8  # see assumption note above


def assess(gap_info):
    """
    gap_info: the dict returned by detection.compare.compute_gap(), or None.

    Returns a dict with the fee-adjusted picture, or None if there's
    nothing to compare yet.
    """
    if not gap_info:
        return None

    gap_pct = abs(gap_info["gap_pct"])

    if gap_pct == 0:
        breakeven_payouts = float("inf")
    else:
        breakeven_payouts = ROUND_TRIP_FEE_PCT / gap_pct

    return {
        "round_trip_fee_pct": ROUND_TRIP_FEE_PCT,
        "gap_pct": gap_pct,
        "breakeven_payouts": breakeven_payouts,
        "breakeven_hours": breakeven_payouts * ASSUMED_FUNDING_INTERVAL_HOURS,
    }

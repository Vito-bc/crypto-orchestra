"""
Frozen constants shared by every STF-CLOSE-55-20 component.

Why this module exists: the power study modelled a 5%-of-capital position while
the cost probe measured the live system's 2% default. Cost, drawdown and the
future shadow journal were therefore describing three different mechanisms, and
the discrepancy was invisible because each component owned its own number.

Everything a trial's arithmetic depends on lives here, once.

SIZING — set by risk decision, not by what makes the strategy fit
-----------------------------------------------------------------
    sleeve   20% of capital is allocated to STF
    position 2% of capital per asset  =  10% of the sleeve
    maximum  4 concurrent positions   =  40% of the sleeve deployed

The sleeve fraction and the drawdown limit are NOT to be relaxed so that a
strategy passes: no edge has been demonstrated, so the correct response to "the
limit rejects it" is a smaller position, which is what this is.

These are deliberately independent of `pipeline.sizing`. They happen to agree
with the live default today; a future change to the live default must not
silently redefine a registered trial.
"""

from __future__ import annotations

# ── Sizing ───────────────────────────────────────────────────────────────────
SLEEVE_FRACTION_OF_CAPITAL = 0.20
POSITION_FRACTION_OF_CAPITAL = 0.02
POSITION_FRACTION_OF_SLEEVE = POSITION_FRACTION_OF_CAPITAL / SLEEVE_FRACTION_OF_CAPITAL
MAX_CONCURRENT_POSITIONS = 4

# ── Rule ─────────────────────────────────────────────────────────────────────
ENTRY_LOOKBACK = 55
EXIT_LOOKBACK = 20
UNIVERSE = ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]

# ── Windows ──────────────────────────────────────────────────────────────────
# Both ends match the main research scope, so the same hydrated inputs serve
# every artifact and CI needs no extra data.
AUDIT_START = "2020-01-01T00:00:00+00:00"
AUDIT_END = "2026-07-12T00:00:00+00:00"

# A portfolio is "flat" when no asset holds a position. Two spans of exposure
# separated by at least this many flat days are counted as separate clusters.
# NOTE the term: cluster, not "independent episode". Sixty flat days makes two
# crypto trend episodes non-contiguous, not statistically independent.
CLUSTER_GAP_DAYS = 60


def position_notional(capital_usd: float) -> float:
    """Notional a single STF position would take."""
    return capital_usd * POSITION_FRACTION_OF_CAPITAL

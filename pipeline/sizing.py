"""
Single source for the paper/live sizing baseline.

`LIVE_BALANCE_USD` is the capital the bot sizes against: order notional is
`LIVE_BALANCE_USD x position_size_pct`, and the drawdown circuit breakers in
`runner.py` measure against it too.

Why this module exists: the default used to be spelled `"10000"` at three call
sites and hardcoded as `10_000.0` at three more (dashboard, daily summary,
Streamlit app), while `.env.example` and the test bootstrap both said 100. A
developer with no `.env` therefore got sizing and circuit-breaker thresholds
computed from a balance 100x larger than the documented cap. The value now has
one definition.

NOT to be confused with `START_BALANCE` in `backtesting/` (backtest.py,
monte_carlo.py, multi_period_backtest.py). That is a simulation convention — a
round starting equity that turns per-trade percentages into readable dollars in
a historical replay. It is not a cap, it is not real money, and it has no reason
to match this value.

Callers that snapshot at import time (`position_tracker.PAPER_BALANCE`) keep
doing so: the root conftest pins the environment before the first project
import, and that ordering is what makes the suite hermetic.
"""

from __future__ import annotations

import os

# Matches `.env.example` and the value the root conftest pins for tests.
DEFAULT_LIVE_BALANCE_USD = 100.0


def live_balance_usd() -> float:
    """Sizing baseline in USD, read from the environment at call time."""
    raw = os.getenv("LIVE_BALANCE_USD")
    if raw is None or not raw.strip():
        return DEFAULT_LIVE_BALANCE_USD
    return float(raw)

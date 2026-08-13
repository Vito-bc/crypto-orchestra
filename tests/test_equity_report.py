"""Unit tests for backtesting/equity_report.py (pure functions, no I/O)."""

import math

import pytest

from backtesting.equity_report import (
    drawdown_episodes,
    equity_curve,
    summary,
)
from backtesting.oos_replay import (
    block_bootstrap_p_pf_gt_1,
    episode_concentration,
    profit_factor,
)
import numpy as np


def _trades(pnls, start="2025-01-01"):
    import pandas as pd
    ts = pd.Timestamp(start, tz="UTC")
    out = []
    for p in pnls:
        out.append({"ts": str(ts), "pnl_pct": p, "hold_h": 12})
        ts += pd.Timedelta(days=7)
    return out


def test_equity_curve_compounds():
    df = equity_curve(_trades([10.0, -10.0]))
    # Row 0 is the pre-trade baseline (1.0) so a losing first trade is visible
    # as a drawdown from initial capital; trades start at row 1.
    assert df["eq"].iloc[0] == pytest.approx(1.00)
    assert df["eq"].iloc[1] == pytest.approx(1.10)
    assert df["eq"].iloc[2] == pytest.approx(0.99)


def test_max_dd_and_underwater():
    m = summary(_trades([10.0, -10.0, -10.0, 30.0]))
    # peak 1.10 -> trough 1.10*0.9*0.9 = 0.891 -> dd = -19%
    assert m["max_dd"] == pytest.approx(-0.19, abs=1e-9)
    assert m["trades"] == 4
    assert 0.0 < m["time_underwater"] <= 1.0


def test_dd_episodes_duration():
    df = equity_curve(_trades([5.0, -3.0, -2.0, 8.0, -1.0]))
    eps = drawdown_episodes(df)
    # two underwater stretches: trades 2-4 (recovers at 4th) and trailing 5th
    assert len(eps) == 2
    assert eps[0]["n_trades"] >= 2
    assert eps[-1]["end"] == df["ts"].iloc[-1]


def test_pf_and_expectancy():
    m = summary(_trades([2.0, 2.0, -1.0]))
    assert m["pf"] == pytest.approx(4.0)
    assert m["expectancy"] == pytest.approx(0.01, abs=1e-9)
    assert m["win_rate"] == pytest.approx(2 / 3)


def test_all_wins_pf_inf():
    m = summary(_trades([1.0, 2.0]))
    assert math.isinf(m["pf"])
    assert m["max_dd"] == 0.0
    assert m["n_dd_episodes"] == 0


def test_profit_factor_edge_cases():
    assert profit_factor(np.array([1.0, -0.5])) == pytest.approx(2.0)
    assert math.isinf(profit_factor(np.array([1.0, 2.0])))
    assert profit_factor(np.array([-1.0])) == 0.0


def test_block_bootstrap_bounds():
    pnls = np.array([2.0, -1.0, 2.0, -1.0, 2.0, -1.0, 2.0, -1.0])
    p = block_bootstrap_p_pf_gt_1(pnls, b=4, n_boot=500, seed=1)
    assert 0.0 <= p <= 1.0
    # strongly positive sample should bootstrap to high P(PF>1)
    assert p > 0.5
    # too-small sample -> nan
    assert math.isnan(block_bootstrap_p_pf_gt_1(np.array([1.0]), b=4))


def test_episode_concentration_grouping():
    sigs = [
        {"timestamp": "2026-01-01 00:00", "trade": {"pnl_pct": 5.0}},
        {"timestamp": "2026-01-05 00:00", "trade": {"pnl_pct": -2.0}},
        {"timestamp": "2026-06-01 00:00", "trade": {"pnl_pct": 5.0}},
    ]
    # two episodes, each contributing 5.0 gross profit -> concentration 0.5
    assert episode_concentration(sigs) == pytest.approx(0.5)
    assert episode_concentration([]) == 0.0


# ── Phase 4: boundary-aware calendar accounting ───────────────────────────────

def test_first_trade_loss_counts_from_initial_capital():
    """
    Without a 1.0 baseline, cummax started at the post-first-trade equity and a
    losing first trade produced max_dd == 0 — the loss was invisible.
    """
    m = summary(_trades([-20.0, 5.0]), start="2025-01-01", end="2025-03-01")
    assert m["max_dd"] == pytest.approx(-0.20, abs=1e-9)


def test_all_losses_drawdown_is_cumulative_from_one():
    m = summary(_trades([-10.0, -10.0]), start="2025-01-01", end="2025-03-01")
    assert m["max_dd"] == pytest.approx(-0.19, abs=1e-9)   # 1 -> 0.81


def test_calendar_underwater_differs_from_observation_count():
    """
    Irregular spacing: a short dip early, then a very long flat underwater gap.
    Observation-weighting understates it; calendar weighting does not.
    """
    import pandas as pd
    t0 = pd.Timestamp("2025-01-01", tz="UTC")
    trades = [
        {"ts": str(t0), "pnl_pct": -10.0, "hold_h": 1},                       # underwater
        {"ts": str(t0 + pd.Timedelta(days=700)), "pnl_pct": 1.0, "hold_h": 1},  # still underwater
    ]
    m = summary(trades, start="2025-01-01", end="2027-01-01")
    assert m["time_underwater"] > 0.99, "nearly the whole window is underwater"
    # The observation-weighted figure answers a different question.
    assert m["time_underwater_obs"] != pytest.approx(m["time_underwater"])


def test_unrecovered_drawdown_runs_to_evaluation_end():
    """The final, never-recovered episode must be measured through eval end."""
    import pandas as pd
    t0 = pd.Timestamp("2025-01-01", tz="UTC")
    trades = [{"ts": str(t0 + pd.Timedelta(days=10)), "pnl_pct": -30.0, "hold_h": 1}]
    eps = drawdown_episodes(equity_curve(trades, start="2025-01-01"), end="2026-01-01")
    assert len(eps) == 1
    assert eps[0]["recovered"] is False
    # 2025-01-11 -> 2026-01-01 is 355 days, not 0 (which measuring to the last
    # trade would give).
    assert eps[0]["duration_days"] == pytest.approx(355.0, abs=0.5)


def test_exact_recovery_to_peak_closes_the_episode():
    """Regaining exactly the prior peak ends the drawdown (dd == 0)."""
    # -50% then +100% returns to exactly 1.0
    m = summary(_trades([-50.0, 100.0]), start="2025-01-01", end="2025-06-01")
    assert m["max_dd"] == pytest.approx(-0.50, abs=1e-9)
    eps = drawdown_episodes(equity_curve(_trades([-50.0, 100.0]), start="2025-01-01"),
                            end="2025-06-01")
    assert all(e["recovered"] for e in eps), "exact recovery must close the episode"


def test_new_high_after_recovery():
    m = summary(_trades([-10.0, 50.0]), start="2025-01-01", end="2025-06-01")
    assert m["total_ret"] > 0
    assert m["max_dd"] == pytest.approx(-0.10, abs=1e-9)


def test_no_trades_returns_empty_summary():
    assert summary([], start="2025-01-01", end="2025-06-01") == {"trades": 0}
    assert equity_curve([]).empty


def test_single_trade():
    m = summary(_trades([7.0]), start="2025-01-01", end="2025-06-01")
    assert m["trades"] == 1
    assert m["total_ret"] == pytest.approx(0.07)
    # std of one sample is undefined — must be nan, not a warning or a crash.
    assert math.isnan(m["sharpe"])


def test_all_wins_has_no_downside_and_no_warnings(recwarn):
    """All-win series: empty downside slice must not emit a NumPy warning."""
    m = summary(_trades([5.0, 7.0, 3.0]), start="2025-01-01", end="2025-06-01")
    assert m["pf"] == float("inf")
    assert math.isnan(m["sortino"]), "no downside observations -> undefined"
    numpy_warnings = [w for w in recwarn
                      if issubclass(w.category, RuntimeWarning)]
    assert not numpy_warnings, f"unexpected NumPy warnings: {[str(w.message) for w in numpy_warnings]}"


def test_all_losses_pf_is_zero_without_warnings(recwarn):
    m = summary(_trades([-5.0, -7.0]), start="2025-01-01", end="2025-06-01")
    assert m["pf"] == 0.0
    numpy_warnings = [w for w in recwarn if issubclass(w.category, RuntimeWarning)]
    assert not numpy_warnings


def test_naive_and_aware_timestamps_are_equivalent():
    """Documented contract: naive input is interpreted as UTC."""
    naive = [{"ts": "2025-01-01 00:00:00", "pnl_pct": -5.0, "hold_h": 1}]
    aware = [{"ts": "2025-01-01 00:00:00+00:00", "pnl_pct": -5.0, "hold_h": 1}]
    a = summary(naive, start="2025-01-01", end="2025-06-01")
    b = summary(aware, start="2025-01-01", end="2025-06-01")
    assert a["max_dd"] == pytest.approx(b["max_dd"])
    assert a["time_underwater"] == pytest.approx(b["time_underwater"])


def test_window_metrics_use_requested_boundaries_not_trade_span():
    """CAGR/trades-per-year are defined over the requested window."""
    short = summary(_trades([10.0, 10.0]), start="2025-01-01", end="2025-03-01")
    long_ = summary(_trades([10.0, 10.0]), start="2025-01-01", end="2030-01-01")
    assert long_["trades_per_year"] < short["trades_per_year"]
    assert long_["cagr"] < short["cagr"]

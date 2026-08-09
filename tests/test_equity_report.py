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
    assert df["eq"].iloc[0] == pytest.approx(1.10)
    assert df["eq"].iloc[1] == pytest.approx(0.99)


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

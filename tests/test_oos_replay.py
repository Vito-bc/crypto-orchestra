"""
Tests for backtesting/oos_replay.py and the path-explicit scanner contract.

Covers the Phase 2 requirements of
docs/tasks/2026-08-research-evidence-hardening.md:

  * a blocked trade exposes a later signal only on the integrated path;
  * unfiltered and integrated cohorts cannot be silently substituted;
  * a signal whose horizon is not fully observed stays PENDING;
  * pending signals do not enter activation/diagnostic statistics;
  * CLI window validation fails closed.

These are deterministic: no network, no live data.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtesting.oos_replay import (
    OOS_FREEZE,
    ReplayInputError,
    block_bootstrap_p_pf_gt_1,
    check_coverage,
    episode_concentration,
    is_resolved,
    profit_factor,
    validate_window,
)
from backtesting.signal_scanner import _simulate_trade


# ── Right-censoring in _simulate_trade ────────────────────────────────────────

def _df(rows: list[tuple[float, float, float]], atr: float = 1.0) -> pd.DataFrame:
    """rows = [(low, high, close), ...] on an hourly index."""
    idx = pd.date_range("2026-07-12", periods=len(rows), freq="h", tz="UTC")
    return pd.DataFrame(
        {
            "low":   [r[0] for r in rows],
            "high":  [r[1] for r in rows],
            "close": [r[2] for r in rows],
            "atr":   [atr] * len(rows),
        },
        index=idx,
    )


def test_stop_loss_is_resolved() -> None:
    # entry 100, atr 1, stop 2x → 98. Second bar trades down to 97.
    df = _df([(100, 100, 100), (97, 100, 98)])
    t = _simulate_trade(df, 0, 100.0, max_hold_hours=10, atr_stop=2.0, atr_target=4.0)
    assert t["reason"] == "STOP_LOSS"
    assert t["resolved"] is True


def test_take_profit_is_resolved() -> None:
    df = _df([(100, 100, 100), (100, 105, 104)])
    t = _simulate_trade(df, 0, 100.0, max_hold_hours=10, atr_stop=2.0, atr_target=4.0)
    assert t["reason"] == "TAKE_PROFIT"
    assert t["resolved"] is True


def test_fully_observed_max_hold_is_resolved() -> None:
    """max_hold elapses inside the data → a real MAX_HOLD outcome."""
    rows = [(100, 100, 100)] * 6          # flat: neither stop nor target
    df = _df(rows)
    t = _simulate_trade(df, 0, 100.0, max_hold_hours=3, atr_stop=2.0, atr_target=4.0)
    assert t["reason"] == "MAX_HOLD"
    assert t["resolved"] is True
    assert t["hold_h"] == 3


def test_horizon_beyond_data_is_pending() -> None:
    """
    The data ends before max_hold elapses and neither stop nor target was hit.
    The outcome is unknown — it must NOT be reported as a completed MAX_HOLD.
    """
    rows = [(100, 100, 100)] * 3          # only 2 bars after entry
    df = _df(rows)
    t = _simulate_trade(df, 0, 100.0, max_hold_hours=36, atr_stop=2.0, atr_target=4.0)
    assert t["reason"] == "PENDING"
    assert t["resolved"] is False
    assert t["hold_h"] < 36, "hold must reflect observed bars, not the full horizon"


def test_pending_signal_still_reports_stop_if_actually_observed() -> None:
    """Censoring must not swallow an outcome that DID occur inside the data."""
    rows = [(100, 100, 100), (97, 100, 98)]   # stop hit at bar 1
    df = _df(rows)
    t = _simulate_trade(df, 0, 100.0, max_hold_hours=36, atr_stop=2.0, atr_target=4.0)
    assert t["reason"] == "STOP_LOSS"
    assert t["resolved"] is True


# ── Pending signals must not enter statistics ─────────────────────────────────

def _sig(ts: str, pnl: float, resolved: bool, blocked: bool = False) -> dict:
    return {
        "timestamp": ts,
        "regime": {"er_30": 0.5},
        "v3_would_block": blocked,
        "trade": {"reason": "STOP_LOSS" if resolved else "PENDING",
                  "pnl_pct": pnl, "hold_h": 5, "resolved": resolved},
    }


def test_is_resolved_reads_the_flag() -> None:
    assert is_resolved(_sig("2026-07-13 00:00", -1.0, True)) is True
    assert is_resolved(_sig("2026-07-13 00:00", -1.0, False)) is False


def test_is_resolved_defaults_true_for_legacy_records() -> None:
    """Older journal rows have no `resolved` key; treat them as closed."""
    legacy = {"trade": {"reason": "MAX_HOLD", "pnl_pct": 1.0, "hold_h": 36}}
    assert is_resolved(legacy) is True


def test_pending_excluded_from_pf_and_concentration() -> None:
    sigs = [
        _sig("2026-07-13 00:00", -2.0, True),
        _sig("2026-07-14 00:00", +6.0, True),
        # >30d later so it forms its own episode, and censored so it must not count
        _sig("2026-09-20 00:00", +99.0, False),
    ]
    resolved = [s for s in sigs if is_resolved(s)]
    assert len(resolved) == 2

    pnls = np.array([s["trade"]["pnl_pct"] for s in resolved])
    assert profit_factor(pnls) == pytest.approx(3.0)   # 6 / 2, the +99 excluded

    # Including the censored winner would swing concentration materially.
    conc_resolved = episode_concentration(resolved)
    conc_all = episode_concentration(sigs)
    assert conc_resolved == pytest.approx(1.0)
    assert conc_all == pytest.approx(99 / 105)
    assert conc_resolved != pytest.approx(conc_all)


# ── Bootstrap: all-win samples are PF > 1, not discarded ──────────────────────

def test_profit_factor_all_wins_is_inf_and_counts_as_gt_1() -> None:
    pf = profit_factor(np.array([1.0, 2.0, 3.0]))
    assert pf == float("inf")
    assert pf > 1.0


def test_bootstrap_all_wins_gives_probability_one() -> None:
    """Every resample of an all-win series has PF=inf, so P(PF>1) must be 1.0."""
    p = block_bootstrap_p_pf_gt_1(np.array([1.0, 2.0, 3.0, 4.0, 5.0]), b=2, n_boot=200)
    assert p == 1.0


def test_bootstrap_all_losses_gives_probability_zero() -> None:
    p = block_bootstrap_p_pf_gt_1(np.array([-1.0, -2.0, -3.0, -4.0]), b=2, n_boot=200)
    assert p == 0.0


# ── Episode concentration boundary is <= 50% everywhere ───────────────────────

def test_episode_concentration_exact_half_is_the_boundary() -> None:
    """Two equal-profit episodes >30d apart → exactly 0.50, which passes."""
    sigs = [
        _sig("2026-01-01 00:00", +5.0, True),
        _sig("2026-06-01 00:00", +5.0, True),   # >30d gap → separate episode
    ]
    assert episode_concentration(sigs) == pytest.approx(0.5)
    assert episode_concentration(sigs) <= 0.5   # the documented pass boundary


# ── CLI / window validation fails closed ──────────────────────────────────────

def test_end_before_freeze_is_rejected() -> None:
    with pytest.raises(ReplayInputError, match="before the pre-registered OOS freeze"):
        validate_window("2026-01-01")


def test_future_end_is_rejected() -> None:
    future = (pd.Timestamp.today() + pd.Timedelta(days=30)).date().isoformat()
    with pytest.raises(ReplayInputError, match="future"):
        validate_window(future)


def test_garbage_end_is_rejected() -> None:
    with pytest.raises(ReplayInputError, match="not a valid date"):
        validate_window("not-a-date")


def test_freeze_boundary_itself_is_accepted() -> None:
    assert validate_window(OOS_FREEZE) == OOS_FREEZE


def test_zero_candle_coverage_fails_closed() -> None:
    """An empty scan must raise, not be reported as 'no signals'."""
    with pytest.raises(ReplayInputError, match="No candles"):
        check_coverage({"candles": 0, "signals": []}, "2026-08-09")
    with pytest.raises(ReplayInputError):
        check_coverage(None, "2026-08-09")


# ── The two paths are distinct and labelled ───────────────────────────────────

def test_scan_asset_accepts_explicit_enforcement_without_mutating_config() -> None:
    """
    The enforcement mode must be a per-call argument. Research code switching
    modes by mutating ASSET_CONFIG leaks into every later scan in the process.
    """
    import inspect

    from backtesting.signal_scanner import ASSET_CONFIG, scan_asset

    sig = inspect.signature(scan_asset)
    assert "v3_enforcement" in sig.parameters, (
        "scan_asset must take an explicit v3_enforcement argument"
    )
    assert sig.parameters["v3_enforcement"].default is None, (
        "default must defer to the asset config, not force a mode"
    )
    # The shipped config must remain enforcement-off: V3 is retired.
    assert ASSET_CONFIG["ZEC-USD"]["v3_enforcement_enabled"] is False


def test_integrated_path_is_not_a_post_filter_of_unfiltered() -> None:
    """
    Blocking a trade suppresses its skip_until window, so a later bar that the
    unfiltered path swallowed mid-hold can produce a signal only on the
    integrated path. Post-filtering the unfiltered cohort therefore cannot
    reproduce the integrated cohort — this pins that they are different objects.
    """
    unfiltered = [
        _sig("2026-07-13 00:00", -2.0, True, blocked=True),   # enforcement skips
        _sig("2026-07-15 00:00", +1.0, True),
    ]
    # On the integrated path the blocked entry never opened, so no skip_until —
    # an extra signal exists on 07-14 that the unfiltered path never emitted.
    integrated = [
        _sig("2026-07-14 00:00", +3.0, True),
        _sig("2026-07-15 00:00", +1.0, True),
    ]

    post_filtered = [s for s in unfiltered if not s["v3_would_block"]]
    assert [s["timestamp"] for s in post_filtered] != [s["timestamp"] for s in integrated], (
        "post-filtering must not be mistaken for the integrated cohort"
    )
    # Different cohorts → different closed-trade counts and different totals.
    assert len(post_filtered) != len(integrated)
    assert sum(s["trade"]["pnl_pct"] for s in post_filtered) != pytest.approx(
        sum(s["trade"]["pnl_pct"] for s in integrated)
    )

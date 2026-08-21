"""
Phase 7R guards — the blind, and the shape of the feasibility audit.

The audit exists to size a future trial without anyone seeing what the rule
would have earned. If it ever reports P&L on pre-cutoff data, the decision to
proceed — or to nudge 55/20 — becomes contaminated by exactly the selection bias
the trial registry exists to prevent.

These tests assert the blind STRUCTURALLY, not by inspection: prices reach one
function, and that function returns timestamps.
"""
from __future__ import annotations

import inspect

import numpy as np
import pandas as pd
import pytest

import backtesting.stf_feasibility as fz


def _closes(values, start="2024-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
    return pd.Series([float(v) for v in values], index=idx)


# ── The blind ────────────────────────────────────────────────────────────────

def test_events_carry_no_price_information() -> None:
    """
    The one function that sees prices returns timestamps and labels. If a price
    or a return ever appears in an event, P&L becomes reconstructible.
    """
    closes = _closes(list(range(100, 200)))
    events = fz.entry_exit_events(closes)
    assert events, "fixture produced no events"
    for ev in events:
        assert set(ev) == {"ts", "event"}, f"event leaked fields: {sorted(ev)}"
        assert ev["event"] in {"ENTRY", "EXIT"}


@pytest.mark.parametrize("banned", [
    "pnl", "profit_factor", "expectancy", "win_rate", "return", "equity",
    "sharpe", "drawdown",
])
def test_the_audit_reports_no_performance_field(banned: str) -> None:
    """
    A blanket check on the whole artifact. Adding a P&L field later would have
    to defeat this deliberately rather than by accident.
    """
    import json

    audit = fz.build_audit()
    flat = json.dumps(audit).lower()
    # "purpose"/"blinding" prose names what is excluded, so search the DATA.
    data = json.dumps({k: v for k, v in audit.items()
                       if k in {"per_asset", "portfolio"}}).lower()
    assert banned not in data, f"the audit reports {banned!r}"
    assert banned not in json.dumps(audit["rule"]).lower()
    del flat


def test_the_module_never_computes_a_return() -> None:
    """
    Executable code must not touch performance concepts.

    Comments and string literals are stripped first: the module's own prose
    NAMES what it refuses to compute, so a raw text search would flag the
    disclaimer itself.
    """
    import io
    import tokenize

    src = inspect.getsource(fz)
    code = " ".join(
        tok.string
        for tok in tokenize.generate_tokens(io.StringIO(src).readline)
        if tok.type not in (tokenize.COMMENT, tokenize.STRING)
    )
    for pattern in ("pnl", "profit_factor", "expectancy", "win_rate",
                    "pct_change", "cumprod"):
        assert pattern not in code, (
            f"{pattern!r} appears in executable code - the blind leaks"
        )


# ── The rule is the declared one ─────────────────────────────────────────────

def test_entry_fires_on_a_new_high_of_closes() -> None:
    """Entry level is built from CLOSES, and the current bar is excluded."""
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0]
    events = fz.entry_exit_events(_closes(values))
    assert events and events[0]["event"] == "ENTRY"
    assert events[0]["ts"].startswith("2024-02-25")     # the 56th bar


def test_no_entry_before_the_lookback_is_complete() -> None:
    """A rising series shorter than the window cannot produce a signal."""
    values = list(range(100, 100 + fz.ENTRY_LOOKBACK - 1))
    assert fz.entry_exit_events(_closes(values)) == []


def test_the_current_bar_is_excluded_from_its_own_window() -> None:
    """
    Including the current close would make the comparison `close > max(...,
    close)` — never true — or, with `>=`, always true. Either way it is
    look-ahead-shaped and must not happen.
    """
    flat = [100.0] * (fz.ENTRY_LOOKBACK + 5)
    assert fz.entry_exit_events(_closes(flat)) == []


def test_exit_fires_on_a_new_low_and_only_while_in_position() -> None:
    values = ([100.0] * fz.ENTRY_LOOKBACK + [120.0]
              + [119.0] * fz.EXIT_LOOKBACK + [90.0])
    events = fz.entry_exit_events(_closes(values))
    labels = [e["event"] for e in events]
    assert labels == ["ENTRY", "EXIT"], labels


def test_positions_do_not_stack() -> None:
    """One position at a time: consecutive new highs must not re-enter."""
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0, 102.0, 103.0, 104.0]
    labels = [e["event"] for e in fz.entry_exit_events(_closes(values))]
    assert labels.count("ENTRY") == 1


def test_an_open_position_at_the_end_is_not_a_closed_trade() -> None:
    values = [100.0] * fz.ENTRY_LOOKBACK + [101.0, 102.0]
    spans, still_open = fz._spans(fz.entry_exit_events(_closes(values)))
    assert spans == []
    assert still_open == 1


# ── Portfolio structure ──────────────────────────────────────────────────────

def test_clusters_split_only_on_a_long_flat_gap() -> None:
    idx = pd.date_range("2024-01-01", periods=400, freq="D", tz="UTC")
    open_days = pd.Series(False, index=idx)
    open_days.iloc[0:20] = True
    second = 20 + fz.CLUSTER_GAP_DAYS + 5
    open_days.iloc[second:second + 20] = True     # single-step assignment
    clusters = fz._clusters(open_days)
    assert len(clusters) == 2

    adjacent = pd.Series(False, index=idx)
    adjacent.iloc[0:20] = True
    adjacent.iloc[30:50] = True          # only a 10-day gap
    assert len(fz._clusters(adjacent)) == 1


def test_the_cluster_gap_is_not_called_independence() -> None:
    """
    Sixty flat days makes two spans non-contiguous, not statistically
    independent. The vocabulary matters because the whole trial rests on how
    many independent observations it can claim.
    """
    src = inspect.getsource(fz)
    assert "cluster" in src.lower()
    assert "not \"independent episode\"" in src or "not statistically" in src.lower()


def test_the_universe_is_fixed_and_the_rule_is_singular() -> None:
    """No grid, no per-asset selection — this is a feasibility audit."""
    assert fz.UNIVERSE == ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
    assert isinstance(fz.ENTRY_LOOKBACK, int)
    assert isinstance(fz.EXIT_LOOKBACK, int)
    src = inspect.getsource(fz)
    assert "for lookback in" not in src, "a parameter sweep appeared"


def test_audit_reports_the_naming_correction() -> None:
    """
    The rule uses closes, not highs/lows, so it is not the classical Donchian
    channel. The artifact has to say so before anyone implements it.
    """
    rule = fz.build_audit()["rule"]
    assert rule["name"] == "STF-CLOSE-55-20"
    assert "not" in rule["note"].lower() and "donchian" in rule["note"].lower()


# ── Power study is calibrated, not guessed ───────────────────────────────────

def test_power_study_is_calibrated_from_the_blinded_audit() -> None:
    import backtesting.stf_power as pw

    s = pw._structure()
    # The first version modelled one trade per asset per cluster and produced
    # ~2.8 trades/year against a measured 11.94, so no simulated trial reached
    # the minimums — an artefact of the model, not a property of the gates.
    assert s["implied_pooled_trades_per_year"] == pytest.approx(
        s["measured_pooled_trades_per_year"], abs=0.2)


def test_power_study_uses_no_historical_strategy_returns() -> None:
    """It may read the audit's STRUCTURE; it must not read prices."""
    import backtesting.stf_power as pw

    src = inspect.getsource(pw)
    assert "read_parquet" not in src
    assert "close" not in src.replace("closed", "")


def test_the_simulated_null_really_has_zero_expectancy() -> None:
    """If the null world had drift, every false-pass rate would be wrong."""
    import backtesting.stf_power as pw

    for win_rate in pw.WIN_RATES:
        w = pw._win_size(win_rate, 0.0)
        expectancy = (win_rate * w
                      - (1 - win_rate) * pw.LOSS_PCT
                      - pw.COST_PCT)
        assert expectancy == pytest.approx(0.0, abs=1e-9)


def test_erfinv_matches_the_normal_quantile() -> None:
    """The copula threshold depends on it; a wrong tail would skew win rates."""
    import backtesting.stf_power as pw

    for p, expected in [(0.5, 0.0), (0.975, 1.959964), (0.75, 0.674490)]:
        got = np.sqrt(2.0) * pw._erfinv(2.0 * p - 1.0)
        assert got == pytest.approx(expected, abs=1e-4)

"""
Guards for the blind perps feasibility gate.

The gate is allowed to consult price movement in exactly three places: the
dispersion estimator, M2's sign function, and the frozen event counter it
imports. These tests keep that allowance from widening into a backtest.

Three independent guards, because any one of them alone is defeatable:

  1. STRUCTURAL — walk the module's AST and assert that the tokens which turn
     prices into returns appear only inside the declared boundary functions.
  2. BEHAVIOURAL — scale every price by a constant and assert the event and
     flag series come back identical. A quantity that survives rescaling
     cannot be a return, and a mechanism whose output changes under rescaling
     is reading a magnitude it should not have.
  3. TYPE — assert that what crosses the boundary is scalars and signs, never
     a series of magnitudes.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import backtesting.perps_gate as pg

# Tokens that turn a price series into a return series, or a return into a
# direction. Outside the declared boundary they have no legitimate use here.
_RETURN_MAKING_ATTRS = {"log", "diff", "shift", "pct_change", "std", "sign"}

# The ONLY functions permitted to use them.
_BOUNDARY = {"log_return_dispersion", "m2_position_flags"}

_BANNED_TOKENS = ("pnl", "profit_factor", "expectancy", "sharpe", "equity_curve",
                  "win_rate", "gross_profit", "gross_loss", "drawdown",
                  "cumulative_return", "total_return")


def _bars(values, start="2021-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="4h", tz="UTC")
    return pd.Series([float(v) for v in values], index=idx)


def _walk(n=900, seed=0, drift=0.0003, vol=0.012) -> pd.Series:
    rng = np.random.default_rng(seed)
    steps = rng.normal(drift, vol, n)
    return _bars(100 * np.exp(np.cumsum(steps)))


# ── 1. Structural: the AST walk ─────────────────────────────────────────────

def test_return_making_tokens_stay_inside_the_boundary() -> None:
    """
    `np.log`, `.diff`, `.shift`, `.pct_change`, `.std` and `np.sign` are what
    convert a price into a return. If one appears in a function that is not a
    declared boundary function, a return has escaped into the counting code
    and the blind is gone — whatever the docstrings claim.
    """
    tree = ast.parse(Path(pg.__file__).read_text(encoding="utf-8"))
    offenders: dict[str, set[str]] = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        used = {inner.attr for inner in ast.walk(node)
                if isinstance(inner, ast.Attribute)} & _RETURN_MAKING_ATTRS
        if used and node.name not in _BOUNDARY:
            offenders[node.name] = used
    assert offenders == {}, f"return arithmetic escaped the boundary: {offenders}"


def test_the_boundary_functions_actually_exist_and_are_used() -> None:
    """A guard that names functions nobody calls guards nothing."""
    tree = ast.parse(Path(pg.__file__).read_text(encoding="utf-8"))
    defined = {n.name for n in ast.walk(tree)
               if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    assert _BOUNDARY <= defined
    for name in _BOUNDARY:
        used = {inner.attr for inner in ast.walk(
            next(n for n in ast.walk(tree)
                 if isinstance(n, ast.FunctionDef) and n.name == name))
            if isinstance(inner, ast.Attribute)} & _RETURN_MAKING_ATTRS
        assert used, f"{name} is declared a boundary function but touches no returns"


def test_module_names_no_performance_quantity() -> None:
    """No P&L, PF, expectancy, Sharpe, drawdown or equity curve, anywhere."""
    tree = ast.parse(Path(pg.__file__).read_text(encoding="utf-8"))
    names = {n.id.lower() for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {n.attr.lower() for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names |= {n.name.lower() for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    names |= {k.value.lower() for k in ast.walk(tree)
              if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    for token in _BANNED_TOKENS:
        assert token not in names, f"module computes or emits a {token}"


def test_counting_functions_take_no_price_argument() -> None:
    """
    The functions that produce every reported statistic accept flags, rates
    and counts. None of them has a parameter that could be a price series, so
    a price cannot reach them even by mistake.
    """
    for func in (pg.event_statistics, pg.funding_drag, pg.cost_exposure,
                 pg._runs, pg.annual_floor_pct, pg.sigma_trade_pct, pg.gate):
        params = set(inspect.signature(func).parameters)
        assert not (params & {"closes", "prices", "close", "series"}), (
            f"{func.__name__} accepts a price series: {params}")


def test_m1_flags_read_only_the_index_and_the_frozen_events() -> None:
    """
    M1's flags must come from `entry_exit_events`, not from a comparison this
    module makes itself. Asserted on the AST: the function body may call that
    one import and may read `.index`, and it performs no comparison against a
    value taken out of the series.
    """
    tree = ast.parse(inspect.getsource(pg.m1_position_flags))
    called = {n.func.id for n in ast.walk(tree)
              if isinstance(n, ast.Call) and isinstance(n.func, ast.Name)}
    assert "entry_exit_events" in called
    compares = [n for n in ast.walk(tree) if isinstance(n, ast.Compare)]
    for node in compares:
        dumped = ast.dump(node)
        assert "closes" not in dumped, "M1 compared a price directly"


# ── 2. Behavioural: rescaling must change nothing ───────────────────────────

@pytest.mark.parametrize("factor", [0.001, 3.7, 10_000.0])
def test_m1_events_are_invariant_to_price_scale(factor: float) -> None:
    """
    Donchian compares a close to past closes, so multiplying every price by a
    positive constant cannot move an event. If the event stream moves, the
    rule is reading a magnitude, and a magnitude is one step from a return.
    """
    closes = _walk(seed=11)
    assert (pg.m1_position_flags(closes).to_numpy()
            == pg.m1_position_flags(closes * factor).to_numpy()).all()


@pytest.mark.parametrize("factor", [0.001, 3.7, 10_000.0])
def test_m2_flags_are_invariant_to_price_scale(factor: float) -> None:
    closes = _walk(seed=12)
    assert (pg.m2_position_flags(closes).to_numpy()
            == pg.m2_position_flags(closes * factor).to_numpy()).all()


def test_dispersion_scales_with_volatility_not_with_price_level() -> None:
    """
    SD of log returns is scale-free in the LEVEL and linear in the volatility.
    Both halves matter: the first says no price level leaks into the reported
    number, the second says the number is still a real measurement.
    """
    base = _walk(seed=13, vol=0.01)
    assert pg.log_return_dispersion(base)["sd_pct"] == pytest.approx(
        pg.log_return_dispersion(base * 250.0)["sd_pct"], rel=1e-9)
    louder = _walk(seed=13, vol=0.02)
    assert pg.log_return_dispersion(louder)["sd_pct"] > (
        1.5 * pg.log_return_dispersion(base)["sd_pct"])


# ── 3. Type: what is allowed to cross the boundary ──────────────────────────

def test_dispersion_returns_scalars_only() -> None:
    out = pg.log_return_dispersion(_walk(seed=14))
    assert set(out) == {"sd_pct", "bars", "by_year"}
    assert isinstance(out["sd_pct"], float)
    for cell in out["by_year"].values():
        assert set(cell) == {"sd_pct", "bars"}
        for value in cell.values():
            assert isinstance(value, (int, float))
            assert not isinstance(value, (pd.Series, pd.DataFrame, np.ndarray))


def test_m2_flags_carry_signs_and_nothing_else() -> None:
    """An int8 series of {-1, 0, +1}. No magnitude survives."""
    flags = pg.m2_position_flags(_walk(seed=15))
    assert flags.dtype == np.int8
    assert set(np.unique(flags.to_numpy())) <= {-1, 0, 1}


def test_m1_flags_are_long_only() -> None:
    flags = pg.m1_position_flags(_walk(seed=16))
    assert set(np.unique(flags.to_numpy())) <= {0, 1}


def test_m2_warms_up_flat_and_does_not_look_ahead() -> None:
    """
    The flag for a bar is formed on the PRIOR bar's close. The first
    M2_LOOKBACK + 1 bars therefore carry 0, and a change to the LAST close
    cannot move any flag, because no flag is formed from it.
    """
    closes = _walk(seed=17)
    flags = pg.m2_position_flags(closes)
    assert (flags.iloc[:pg.M2_LOOKBACK + 1] == 0).all()

    tampered = closes.copy()
    tampered.iloc[-1] *= 1.5
    assert (pg.m2_position_flags(tampered).to_numpy() == flags.to_numpy()).all()


def test_event_statistics_emit_no_field_that_could_be_a_return() -> None:
    stats = pg.event_statistics(pg.m2_position_flags(_walk(seed=18)), "M2")
    allowed = {"mechanism", "bars", "runs", "mean_hold_bars", "median_hold_bars",
               "max_hold_bars", "bars_in_position", "fraction_in_position",
               "fraction_long", "fraction_short", "by_year"}
    assert set(stats) == allowed
    for token in _BANNED_TOKENS:
        assert token not in str(stats).lower()


# ── The declared mechanisms, and the arithmetic of the gate ─────────────────

def test_m1_uses_the_frozen_lookbacks_and_cannot_vary_them() -> None:
    from backtesting import stf_protocol

    assert pg.ENTRY_LOOKBACK is stf_protocol.ENTRY_LOOKBACK == 55
    assert pg.EXIT_LOOKBACK is stf_protocol.EXIT_LOOKBACK == 20
    assert pg.entry_exit_events.__module__ == "backtesting.stf_feasibility"


def test_m2_lookback_is_the_declared_constant() -> None:
    assert pg.M2_LOOKBACK == 30


def test_only_two_mechanisms_exist() -> None:
    """
    A third mechanism, or a swept parameter, is the search this gate exists to
    prevent. There must be exactly two, named in the module docstring.
    """
    flag_builders = {n.name for n in ast.walk(
        ast.parse(Path(pg.__file__).read_text(encoding="utf-8")))
        if isinstance(n, ast.FunctionDef) and n.name.endswith("_position_flags")}
    assert flag_builders == {"m1_position_flags", "m2_position_flags"}


def test_funding_drag_is_signed_arithmetic_on_flags_and_rates() -> None:
    """
    A long pays a positive published rate; a short receives it. Built by hand
    so the sign convention is asserted rather than assumed.
    """
    idx = pd.date_range("2021-01-01", periods=6, freq="4h", tz="UTC")
    flags = pd.Series([1, 1, -1, -1, 0, 0], index=idx, dtype="int8")
    funding = pd.DataFrame({
        "ts": [idx[0], idx[2], idx[4]],
        "rate": [0.0001, 0.0001, 0.0001],
        "interval_h": [8.0, 8.0, 8.0],
    })
    out = pg.funding_drag(flags, funding)
    assert out["events"] == 2, "the flat bar must not be charged"
    assert out["long_pct"] == pytest.approx(0.01)
    assert out["short_pct"] == pytest.approx(-0.01)
    assert out["total_pct"] == pytest.approx(0.0)


def test_negative_funding_pays_the_long() -> None:
    idx = pd.date_range("2021-01-01", periods=2, freq="4h", tz="UTC")
    flags = pd.Series([1, 1], index=idx, dtype="int8")
    funding = pd.DataFrame({"ts": [idx[0]], "rate": [-0.0002],
                            "interval_h": [8.0]})
    assert pg.funding_drag(flags, funding)["total_pct"] == pytest.approx(-0.02)


def test_sigma_trade_is_the_declared_square_root_of_time_bound() -> None:
    assert pg.sigma_trade_pct(2.0, 9.0) == pytest.approx(6.0)
    assert pg.sigma_trade_pct(1.5, 1.0) == pytest.approx(1.5)


def test_annual_floor_matches_the_declared_formula() -> None:
    assert pg.annual_floor_pct(5.0, 40.0, 10.0) == pytest.approx(
        1.645 * 5.0 * (4.0 ** 0.5))


def test_annual_floor_falls_as_the_horizon_lengthens() -> None:
    near = pg.annual_floor_pct(5.0, 40.0, 1.0)
    far = pg.annual_floor_pct(5.0, 40.0, 4.0)
    assert far == pytest.approx(near / 2.0)


def test_floor_depends_only_on_time_in_market_not_on_trade_segmentation() -> None:
    """
    The identity the document rests on. Holding bars-in-position fixed and
    cutting them into twice as many trades of half the length leaves the
    annual floor unchanged, so trading more often cannot lower it.
    """
    sd, years = 1.3, 6.7
    coarse = pg.annual_floor_pct(pg.sigma_trade_pct(sd, 40.0), 20.0, years)
    fine = pg.annual_floor_pct(pg.sigma_trade_pct(sd, 20.0), 40.0, years)
    assert coarse == pytest.approx(fine)


def test_gate_fires_above_the_sesoi_and_passes_at_it() -> None:
    assert pg.gate(pg.SESOI_ANNUAL_PCT) == "PASS"
    assert pg.gate(pg.SESOI_ANNUAL_PCT - 0.01) == "PASS"
    assert pg.gate(pg.SESOI_ANNUAL_PCT + 0.01) == "FAIL"


def test_years_to_sesoi_inverts_the_floor() -> None:
    years = pg.years_to_reach_sesoi(20.0, 6.7)
    assert pg.annual_floor_pct(5.0, 40.0, years) == pytest.approx(
        pg.annual_floor_pct(5.0, 40.0, 6.7) * (6.7 / years) ** 0.5)
    assert years == pytest.approx(6.7 * 4.0)


def test_round_trip_cost_is_the_scoping_documents_number() -> None:
    """Maker-in / taker-out at the CFM rates read on 2026-09-18: +0.1952%."""
    assert pg.ROUND_TRIP_OPTIMISTIC * 100 == pytest.approx(0.1952, abs=5e-5)
    assert pg.ROUND_TRIP_ALL_TAKER > pg.ROUND_TRIP_OPTIMISTIC


def test_cost_exposure_reports_only_costs() -> None:
    stats = {"runs": 100}
    drag = {"total_pct": 30.0, "long_pct": 40.0, "short_pct": -10.0}
    out = pg.cost_exposure(stats, drag, funding_years=5.0, event_years=5.0)
    assert out["round_trips_per_year"] == pytest.approx(20.0)
    assert out["funding_drag_pct_yr"] == pytest.approx(6.0)
    assert out["total_drag_pct_yr_optimistic"] == pytest.approx(
        out["fee_drag_pct_yr_optimistic"] + out["funding_drag_pct_yr"])
    assert all(("drag" in k or "round_trips" in k or "funding" in k or "fee" in k)
               for k in out)

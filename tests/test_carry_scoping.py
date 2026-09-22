"""
Guards for the carry scoping.

Two properties have to hold, and neither is self-evident from reading the code:

  1. The declared harvest rule is a function of the FUNDING SERIES ALONE. That
     is what makes the position non-directional by construction rather than by
     assertion — a rule that could see a price could be a price bet wearing a
     carry label.
  2. Nothing in the module emits a directional-strategy quantity. Prices enter
     in exactly two places, and both hand back counts or scalars.

Plus a third, narrower one: there is exactly ONE harvest window constant. A
second is the beginning of the parameter search this task exists to avoid.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import backtesting.carry_scoping as cs

_BANNED_TOKENS = ("pnl", "profit_factor", "expectancy", "sharpe", "equity_curve",
                  "win_rate", "gross_profit", "gross_loss", "drawdown",
                  "cumulative_return", "total_return", "alpha")

# Names a price series could plausibly travel under in this module.
_PRICE_NAMES = {"prices", "close", "closes", "high", "highs", "price",
                "premium", "excursion", "forward_max"}

# The only functions allowed to receive or touch a price.
_PRICE_TOUCHING = {"load_prices", "load_premium_index", "_read_kline_frame",
                   "margin_event_counts", "basis_stats"}


def _funding(rates, start="2021-01-01", interval_h=8.0) -> pd.DataFrame:
    ts = pd.date_range(start, periods=len(rates), freq=f"{int(interval_h)}h",
                       tz="UTC")
    rate = np.asarray(rates, dtype=float)
    return pd.DataFrame({
        "ts": ts,
        "rate": rate,
        "interval_h": interval_h,
        "annualised_pct": rate * (24.0 / interval_h) * 365.0 * 100.0,
    })


def _prices(closes, highs=None) -> pd.DataFrame:
    idx = pd.date_range("2021-01-01", periods=len(closes), freq="4h", tz="UTC")
    return pd.DataFrame({"high": highs if highs is not None else closes,
                         "close": closes}, index=idx, dtype=float)


# ── 1. The rule is funding-only ─────────────────────────────────────────────

def test_the_declared_rule_takes_funding_and_nothing_else() -> None:
    """
    Signature-level: the rule and everything it calls accept a funding frame.
    If a price could be passed in, "non-directional" would be a claim about
    intent rather than a property of the code.
    """
    for func in (cs.harvest_flags, cs.trailing_mean_annualised):
        params = set(inspect.signature(func).parameters)
        assert params == {"funding"}, f"{func.__name__} takes {params}"
    assert set(inspect.signature(cs.cycles).parameters) == {"flags"}


def test_the_rule_names_no_price_anywhere_in_its_body() -> None:
    """
    Structural: walk the AST of the rule and the cycle counter and assert no
    price-shaped identifier appears. A funding-only rule that quietly read a
    close would still pass the signature test above.
    """
    offenders = {}
    for func in (cs.harvest_flags, cs.trailing_mean_annualised, cs.cycles):
        tree = ast.parse(inspect.getsource(func))
        names = {n.id for n in ast.walk(tree) if isinstance(n, ast.Name)}
        names |= {n.attr for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
        names |= {k.value for k in ast.walk(tree)
                  if isinstance(k, ast.Constant) and isinstance(k.value, str)}
        hit = names & _PRICE_NAMES
        if hit:
            offenders[func.__name__] = hit
    assert offenders == {}, f"the declared rule reached for a price: {offenders}"


def test_price_touching_is_confined_to_the_declared_functions() -> None:
    """
    Module-wide: any function whose parameters include a price-shaped name must
    be one of the declared price-touching functions. This is the boundary that
    keeps a price from reaching the carry arithmetic by a side door.
    """
    tree = ast.parse(Path(cs.__file__).read_text(encoding="utf-8"))
    offenders = {}
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        params = {a.arg for a in node.args.args} & _PRICE_NAMES
        if params and node.name not in _PRICE_TOUCHING:
            offenders[node.name] = params
    assert offenders == {}, f"price reached a non-declared function: {offenders}"


def test_flags_follow_the_sign_of_the_trailing_mean() -> None:
    positive = cs.harvest_flags(_funding([0.0003] * 40))
    assert positive.iloc[-1] == 1
    negative = cs.harvest_flags(_funding([-0.0003] * 40))
    assert (negative == 0).all()


def test_flags_do_not_look_ahead() -> None:
    """
    The signal formed at event i governs holding from i+1. Changing the LAST
    rate must therefore not move any flag: nothing is harvested on the strength
    of knowing the rate being harvested.
    """
    rates = [0.0002] * 40
    base = cs.harvest_flags(_funding(rates))
    tampered = list(rates)
    tampered[-1] = -0.05
    assert (cs.harvest_flags(_funding(tampered)).to_numpy()
            == base.to_numpy()).all()


def test_flags_are_occupancy_not_magnitude() -> None:
    flags = cs.harvest_flags(_funding([0.0003] * 20 + [-0.0003] * 20))
    assert flags.dtype == np.int8
    assert set(np.unique(flags.to_numpy())) <= {0, 1}


def test_a_bigger_rate_does_not_make_a_bigger_flag() -> None:
    """Scaling every rate up cannot change occupancy — only its sign can."""
    small = cs.harvest_flags(_funding([0.00001] * 40))
    large = cs.harvest_flags(_funding([0.01] * 40))
    assert (small.to_numpy() == large.to_numpy()).all()


def test_cycles_are_counted_on_crossings() -> None:
    flags = cs.harvest_flags(_funding([0.0002] * 12 + [-0.0002] * 12
                                      + [0.0002] * 12))
    out = cs.cycles(flags)
    assert len(out) == 2
    assert set(out[0]) >= {"opened_at", "closed_at", "days"}
    for cycle in out:
        for key in cycle:
            assert key in {"opened_at", "closed_at", "days", "open_at_window_end"}


def test_exactly_one_harvest_window_constant_exists() -> None:
    """
    A second window constant is a second parameter, and a second parameter is a
    search. There must be one, and it must be the declared 7 days.
    """
    assert cs.HARVEST_WINDOW_DAYS == 7
    tree = ast.parse(Path(cs.__file__).read_text(encoding="utf-8"))
    window_names = {
        t.id for node in ast.walk(tree) if isinstance(node, ast.Assign)
        for t in node.targets
        if isinstance(t, ast.Name) and t.id.isupper() and "WINDOW" in t.id
        and "MARGIN" not in t.id
    }
    assert window_names == {"HARVEST_WINDOW_DAYS"}, window_names


# ── 2. No directional-strategy quantity leaves the module ───────────────────

def test_module_names_no_performance_quantity() -> None:
    tree = ast.parse(Path(cs.__file__).read_text(encoding="utf-8"))
    names = {n.id.lower() for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {n.attr.lower() for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names |= {n.name.lower() for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    names |= {k.value.lower() for k in ast.walk(tree)
              if isinstance(k, ast.Constant) and isinstance(k.value, str)}
    for token in _BANNED_TOKENS:
        assert token not in names, f"module computes or emits a {token}"


def test_margin_events_emit_counts_not_excursions() -> None:
    """
    The excursion magnitudes are the one thing here that looks like a return
    series. They must stay inside the function; what comes out is counts.
    """
    prices = _prices(np.linspace(100, 160, 300))
    rows = cs.margin_event_counts(prices, 0.30)
    assert rows
    allowed = {"year", "window_days", "threshold_of_margin",
               "price_move_required_pct", "entry_bars", "breach_bars",
               "breach_fraction", "episodes"}
    for row in rows:
        assert set(row) == allowed, f"margin row leaked fields: {sorted(row)}"
        assert isinstance(row["breach_bars"], int)
        assert isinstance(row["episodes"], int)


@pytest.mark.parametrize("factor", [0.001, 7.3, 10_000.0])
def test_margin_event_counts_are_invariant_to_price_scale(factor: float) -> None:
    """
    The excursion is a ratio, so the price level must not matter. If counts
    move under rescaling, a magnitude is leaking into a risk count.
    """
    closes = np.linspace(100, 190, 400)
    base = cs.margin_event_counts(_prices(closes), 0.30)
    scaled = cs.margin_event_counts(_prices(closes * factor), 0.30)
    assert [r["breach_bars"] for r in base] == [r["breach_bars"] for r in scaled]
    assert [r["episodes"] for r in base] == [r["episodes"] for r in scaled]


def test_episodes_never_exceed_breach_bars_and_collapse_one_rally() -> None:
    """
    Non-overlapping episodes must be no more numerous than breach bars, and a
    single monotone rally must count once per window, not once per bar.
    """
    closes = np.concatenate([np.full(60, 100.0), np.linspace(100, 200, 60)])
    rows = cs.margin_event_counts(_prices(closes), 0.30)
    for row in rows:
        assert row["episodes"] <= row["breach_bars"]
    seven_day = [r for r in rows
                 if r["window_days"] == 7 and r["threshold_of_margin"] == 0.50
                 and r["year"] == "full_window"][0]
    assert seven_day["breach_bars"] > seven_day["episodes"]


def test_a_falling_market_produces_no_short_margin_event() -> None:
    """A short is not hurt by a fall. If it counts one, the sign is inverted."""
    rows = cs.margin_event_counts(_prices(np.linspace(200, 100, 300)), 0.30)
    assert all(r["breach_bars"] == 0 for r in rows)


def test_basis_stats_return_scalars_only() -> None:
    idx = pd.date_range("2021-01-01", periods=500, freq="4h", tz="UTC")
    premium = pd.Series(np.random.default_rng(0).normal(0, 0.0005, 500), index=idx)
    out = cs.basis_stats(premium)
    assert set(out) == {"observations", "sd_bps", "mean_bps",
                        "worst_1d_change_bps", "p99_abs_1d_change_bps"}
    for value in out.values():
        assert isinstance(value, (int, float))
        assert not isinstance(value, (pd.Series, pd.DataFrame, np.ndarray))


# ── 3. The cost and capital arithmetic ──────────────────────────────────────

def test_spot_leg_is_priced_from_pipeline_fees() -> None:
    """
    The adopted schedule must BE `pipeline/fees.py`, not a number copied beside
    it that can drift. The candidate tier is reported alongside, never instead.

    `pipeline/fees.py` CURRENT_SCHEDULE moved to 0.5%/0.9% on 2026-09-22
    (coinbase-intro-2026-09-22); `carry_scoping.py`'s own CANDIDATE entry
    (0.5%/0.9%, "read 2026-09-18, not adopted") was NOT recomputed by that
    change — this module's numbers were declared not-to-be-recomputed for
    that PR, and are now conservative rather than current. See
    `docs/trial_registry.md`'s cost-sensitivity note. The two happen to be
    numerically equal today; that is a coincidence of the two schedules, not
    a claim that either was rebased on the other.
    """
    from pipeline.fees import CURRENT_SCHEDULE

    adopted = cs.SPOT_SCHEDULES["adopted"]
    assert adopted["maker"] == CURRENT_SCHEDULE.maker_rate
    assert adopted["taker"] == CURRENT_SCHEDULE.taker_rate
    candidate = cs.SPOT_SCHEDULES["candidate"]
    assert (candidate["maker"], candidate["taker"]) == (0.005, 0.009)
    assert cs.spot_round_trip(adopted) >= cs.spot_round_trip(candidate)


def test_cycle_cost_is_both_legs() -> None:
    from pipeline.fees import CURRENT_SCHEDULE

    adopted = cs.SPOT_SCHEDULES["adopted"]
    adopted_round_trip = CURRENT_SCHEDULE.maker_rate + CURRENT_SCHEDULE.taker_rate
    assert cs.perp_round_trip() == pytest.approx(0.00195)
    assert cs.cycle_cost(adopted) == pytest.approx(adopted_round_trip + 0.00195)


def test_committed_capital_includes_margin_and_the_declared_reserve() -> None:
    assert cs.RESERVE_FRACTION_OF_MARGIN == 0.25
    for symbol, spec in cs.CFM_PRODUCTS.items():
        margin = spec["overnight_margin_short"]
        assert cs.committed_capital_multiple(symbol) == pytest.approx(
            1.0 + margin * 1.25)
        assert cs.committed_capital_multiple(symbol) > 1.0


def test_short_margin_is_the_one_charged_not_the_long_margin() -> None:
    """
    CFM margin is asymmetric by side and the short leg is the expensive one.
    Charging the long fraction would understate committed capital.
    """
    for spec in cs.CFM_PRODUCTS.values():
        assert spec["overnight_margin_short"] > spec["overnight_margin_long"]


def test_net_on_capital_is_never_larger_than_net_on_notional_when_positive() -> None:
    """Dividing by a multiple above 1 must shrink a positive figure."""
    multiple = cs.committed_capital_multiple("BTCUSDT")
    assert 10.0 / multiple < 10.0


# The proxy cache is gitignored and comes from a venue this repository does not
# trade, so CI does not hydrate it. This is the same choice the perps-gate
# document records for its own --verify, and it is made the same way here: skip
# on missing data rather than couple a required check to Binance's archive
# staying up. The repo's `integration` marker means the COINBASE candle cache
# specifically, so it is the wrong marker for this test — using it made CI's
# integration step fail on data it never fetches.
_PROXY_AVAILABLE = bool(list(cs.DATA_DIR.glob("fund_BTCUSDT_*.csv")))


@pytest.mark.skipif(not _PROXY_AVAILABLE,
                    reason="Binance proxy cache absent; run "
                           "backtesting/hydrate_perps_proxy.py to enable")
def test_always_on_gross_reconciles_with_the_perps_gate() -> None:
    """
    The short leg's always-on funding receipt is the exact mirror of the long's
    always-on cost, which the perps-gate document reports as 11.8057%/yr (BTC)
    and 13.9565%/yr (ETH). Reading materially different numbers here would mean
    one of the two documents is wrong about the same series.
    """
    expected = {"BTCUSDT": 11.8057, "ETHUSDT": 13.9565}
    for symbol, gate_figure in expected.items():
        block = cs.analyse_symbol(symbol, "adopted")
        measured = block["rows"][-1]["gross_always_on_pct_yr"]
        assert measured == pytest.approx(gate_figure, abs=0.05), (
            f"{symbol}: carry reads {measured}, gate reads {gate_figure}")

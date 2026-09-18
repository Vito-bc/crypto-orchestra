"""
Guards for the universe/power inventory.

The inventory is allowed to consult price MOVEMENT in exactly one place —
cross-asset return correlation, which describes the universe rather than any
strategy. These tests keep that allowance from widening into a backtest:
correlation must hand back scalars, and the module must not name a P&L,
profit-factor, expectancy or Sharpe quantity anywhere.
"""
from __future__ import annotations

import ast
import inspect
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

import backtesting.universe_inventory as ui

BANNED_TOKENS = ("pnl", "profit_factor", "expectancy", "sharpe", "equity_curve",
                 "win_rate", "gross_profit", "gross_loss")


def _closes(values, start="2024-01-01") -> pd.Series:
    idx = pd.date_range(start, periods=len(values), freq="D", tz="UTC")
    return pd.Series([float(v) for v in values], index=idx)


def _walk(rng, n=400, seed=0, drift=0.0004):
    r = np.random.default_rng(seed)
    steps = r.normal(drift, 0.03, n)
    return _closes(100 * np.exp(np.cumsum(steps)), start=rng)


# ── The blind: what correlation is allowed to return ─────────────────────────

def test_correlation_returns_only_scalars() -> None:
    """
    The one price-movement function must not leak a series. If it ever returns
    a Series/DataFrame/ndarray, a caller can difference it into returns and
    build a P&L, and the blind is gone.
    """
    series = {"A-USD": _walk("2024-01-01", seed=1),
              "B-USD": _walk("2024-01-01", seed=2),
              "C-USD": _walk("2024-01-01", seed=3)}
    assets = sorted(series)
    window = ui.common_overlap_window(series, assets)
    out = ui.mean_pairwise_log_return_correlation(series, assets, window)

    assert set(out) == {"rho_bar", "pairs", "assets", "observations"}
    for key, value in out.items():
        assert isinstance(value, (int, float)) or value is None, (
            f"{key} is {type(value).__name__}; correlation must return scalars")
        assert not isinstance(value, (pd.Series, pd.DataFrame, np.ndarray))


def test_correlation_reports_no_per_asset_value() -> None:
    """A per-asset correlation would be a ranking of assets. Only the mean."""
    series = {"A-USD": _walk("2024-01-01", seed=4),
              "B-USD": _walk("2024-01-01", seed=5)}
    assets = sorted(series)
    out = ui.mean_pairwise_log_return_correlation(
        series, assets, ui.common_overlap_window(series, assets))
    assert not any(a in str(out) for a in assets)


def test_correlation_is_the_only_function_that_differences_prices() -> None:
    """
    Structural, not by inspection: walk the module's AST and assert that
    `.diff(`/`np.log(` appear only inside the declared correlation function.
    """
    tree = ast.parse(Path(ui.__file__).read_text(encoding="utf-8"))
    offenders = []
    for node in ast.walk(tree):
        if not isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            continue
        body = ast.dump(node)
        if "'diff'" in body or "attr='log'" in body:
            offenders.append(node.name)
    assert offenders == ["mean_pairwise_log_return_correlation"], (
        f"price differencing escaped its function: {offenders}")


def test_module_names_no_performance_quantity() -> None:
    source = Path(ui.__file__).read_text(encoding="utf-8").lower()
    # Strip the docstrings/comments that legitimately NAME what is forbidden.
    code = "\n".join(line.split("#")[0] for line in source.splitlines())
    tree = ast.parse(Path(ui.__file__).read_text(encoding="utf-8"))
    names = {n.id.lower() for n in ast.walk(tree) if isinstance(n, ast.Name)}
    names |= {n.attr.lower() for n in ast.walk(tree) if isinstance(n, ast.Attribute)}
    names |= {n.name.lower() for n in ast.walk(tree)
              if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef))}
    for token in BANNED_TOKENS:
        assert token not in names, f"module computes a {token}"
        assert f"def {token}" not in code


# ── The estimator, exactly as specified ──────────────────────────────────────

def test_effective_assets_matches_the_declared_formula() -> None:
    assert ui.effective_assets(10, 0.0) == 10.0
    assert ui.effective_assets(10, 1.0) == 1.0
    assert ui.effective_assets(31, 0.65) == pytest.approx(31 / (1 + 30 * 0.65), abs=1e-3)


def test_effective_assets_collapses_as_correlation_rises() -> None:
    low = ui.effective_assets(60, 0.2)
    high = ui.effective_assets(60, 0.8)
    assert low > high, "more correlation must mean fewer effective assets"


def test_perfectly_correlated_assets_collapse_to_about_one() -> None:
    base = _walk("2024-01-01", seed=7)
    series = {"A-USD": base, "B-USD": base * 3.0, "C-USD": base * 0.5}
    assets = sorted(series)
    out = ui.mean_pairwise_log_return_correlation(
        series, assets, ui.common_overlap_window(series, assets))
    assert out["rho_bar"] == pytest.approx(1.0, abs=1e-6)
    assert ui.effective_assets(3, out["rho_bar"]) == pytest.approx(1.0, abs=1e-3)


def test_floor_shrinks_with_more_observations() -> None:
    few = ui.decidable_edge_floor(8)["frozen_1.0pct_model"]
    many = ui.decidable_edge_floor(80)["frozen_1.0pct_model"]
    assert few > many
    # 10x the observations is a sqrt(10) tighter bound (the reported values
    # are rounded to 4dp, hence the tolerance).
    assert many == pytest.approx(few / (10 ** 0.5), rel=1e-3)


def test_floor_is_undefined_without_observations() -> None:
    assert all(v is None for v in ui.decidable_edge_floor(0).values())
    assert all(v is None for v in ui.decidable_edge_floor(None).values())


# ── Frozen inputs are imported, never redefined ──────────────────────────────

def test_rule_constants_come_from_the_frozen_protocol() -> None:
    from backtesting import stf_protocol

    assert ui.ENTRY_LOOKBACK is stf_protocol.ENTRY_LOOKBACK
    assert ui.CLUSTER_GAP_DAYS is stf_protocol.CLUSTER_GAP_DAYS
    assert ui.entry_exit_events.__module__ == "backtesting.stf_feasibility"


def test_candidate_rules_read_no_price_column() -> None:
    """
    Rules may look at history length and liquidity only. Asserted on the
    columns the function actually names, not on a keyword scan: the universe
    must not be selected on anything the price did.
    """
    allowed = {"status", "trading_disabled", "days_history",
               "median_30d_volume_usd", "product_id"}
    tree = ast.parse(inspect.getsource(ui.candidate_rules))
    referenced = {
        n.slice.value for n in ast.walk(tree)
        if isinstance(n, ast.Subscript) and isinstance(n.slice, ast.Constant)
        and isinstance(n.slice.value, str)
    }
    assert referenced, "no columns referenced — test is not looking at anything"
    assert referenced <= allowed, f"rules read a disallowed column: {referenced - allowed}"
    assert {"days_history", "median_30d_volume_usd"} <= referenced


def test_cluster_counting_documents_its_saturation() -> None:
    """
    The count saturates on a wide universe; the docstring must say so, because
    the number is otherwise read as an independence claim.
    """
    doc = " ".join((ui.clusters_for_universe.__doc__ or "").lower().split())
    assert "saturat" in doc
    module_doc = " ".join((ui.__doc__ or "").lower().split())
    assert "saturates by construction" in module_doc

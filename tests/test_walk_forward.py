"""
Walk-forward repair — trial 2026-08-walkforward-repair.v1.

The tool was disabled because it validated a weaker mechanism than it reported:
no daily frame, three of the blocked reasons enumerated (the rest traded), a
wrong fee model, invented MAX_HOLD outcomes at the right edge, and silent ETH
fallbacks for unknown assets. These tests pin each of those, so a repair cannot
quietly regress into the same tool.

Synthetic frames only — no candle cache, no network.
"""
from __future__ import annotations

from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import backtesting.walk_forward as wf
from backtesting.walk_forward import WalkForwardError

ZEC = "ZEC-USD"          # btc_regime_filter=False, 200-day daily EMA
ETH = "ETH-USD"          # btc_regime_filter=True
BTC = "BTC-USD"          # never gets its own regime column


def _frame(n: int = 80, *, daily_ema: float | None = 100.0,
           daily_close: float = 105.0, ema_col: str = "ema200_1d",
           btc: tuple[float, float] | None = None) -> pd.DataFrame:
    """
    A frame in which a BUY forms repeatedly.

    _detect_breakout_signal only fires just after a cross up through EMA50 (at
    most _MAX_CANDLES_SINCE bars above it), so the price must keep dipping back
    below. A single permanent cross produces exactly zero signals.
    """
    idx = pd.date_range("2025-01-01", periods=n, freq="h", tz="UTC")
    cycle = np.array([100.0] * 5 + [105.0] * 3)          # 5 below EMA, 3 above
    close = np.tile(cycle, n // len(cycle) + 1)[:n]
    df = pd.DataFrame({
        "time": idx,
        "close": close,
        "ema50": np.full(n, 101.0),
        "high": close + 0.0,
        "low": close - 1.0,
        "atr": np.full(n, 1.0),
        "rsi": np.full(n, 50.0),
        "adx": np.full(n, 30.0),
        "volume_ratio": np.full(n, 1.5),
        "cvd_24h": np.full(n, 10.0),
        "close_4h": np.full(n, 105.0),
        "ema50_4h": np.full(n, 100.0),
        "close_1d": np.full(n, daily_close),
    }, index=idx)
    if daily_ema is not None:
        df[ema_col] = np.full(n, daily_ema)
    if btc is not None:
        df["btc_close_1d"], df["btc_ema50_1d"] = np.full(n, btc[0]), np.full(n, btc[1])
    return df


def _frame_for(asset: str, **kw) -> pd.DataFrame:
    """
    Frame carrying the daily EMA column the ASSET declares.

    ZEC uses the 200-day EMA, BTC and ETH the 50-day one. Supplying the wrong
    column is itself a gate-unavailable condition, so a test that means to
    exercise the BTC gate has to get this right first.
    """
    from backtesting.signal_scanner import ASSET_CONFIG

    period = ASSET_CONFIG[asset].get("daily_ema_period", 200)
    return _frame(ema_col=f"ema{period}_1d", **kw)


def _pending_frame(asset: str = ZEC) -> pd.DataFrame:
    """
    A frame with exactly ONE signal, firing too late to resolve.

    Everything before the tail stays below EMA50 so no earlier trade exists —
    otherwise the whipsaw guard (2 stops in 96h) blocks the late signal and the
    censoring path is never reached.
    """
    df = _frame_for(asset, n=40).copy()
    df["close"] = 100.0                                   # below EMA50 (101)
    tail = df.index[-5:]
    df.loc[tail, "close"] = 105.0                         # single cross up
    df["high"] = df["close"] + 0.2                        # never reaches target
    df["low"] = df["close"] - 0.2                         # never reaches stop
    return df


def _scan(df, asset=ZEC, start=None, end=None, stop=2.0, target=3.5):
    start = start if start is not None else df.index[0]
    end = end if end is not None else df.index[-1] + pd.Timedelta(hours=1)
    return wf.run_scan(df, start, end, asset, stop, target)


# ── The defect that voided every previous number ─────────────────────────────

def test_the_daily_gate_is_actually_enforced() -> None:
    """
    THE regression. The old loader never attached the daily frame, so this gate
    was skipped for every signal on every asset — 475 blocks in the current
    artifact that the old tool traded.
    """
    res = _scan(_frame(daily_close=90.0, daily_ema=100.0))   # daily downtrend
    assert res["n"] == 0, "a daily downtrend must produce no trades"
    assert res["blocked"]["daily_trend"] > 0


def test_the_configured_ema_period_is_the_one_checked() -> None:
    """ZEC declares 200; supplying only ema50_1d must not satisfy it."""
    res = _scan(_frame(ema_col="ema50_1d"))
    assert res["n"] == 0
    assert res["blocked"]["gate_inputs_unavailable"] > 0


def test_a_missing_daily_frame_blocks_rather_than_trades() -> None:
    res = _scan(_frame(daily_ema=None))
    assert res["n"] == 0
    assert res["blocked"]["gate_inputs_unavailable"] > 0


def test_a_warming_up_daily_ema_blocks() -> None:
    df = _frame()
    df["ema200_1d"] = np.nan
    res = _scan(df)
    assert res["n"] == 0
    assert res["blocked"]["gate_inputs_unavailable"] > 0


# ── Every blocked reason means no trade ──────────────────────────────────────

@pytest.mark.parametrize("mutate,reason", [
    (lambda d: d.assign(volume_ratio=0.1), "vol_gate"),
    (lambda d: d.assign(close_4h=90.0, ema50_4h=100.0), "4h_trend"),
    (lambda d: d.assign(close_1d=90.0), "daily_trend"),
    (lambda d: d.assign(adx=1.0, rsi=99.0, volume_ratio=0.9, cvd_24h=-5.0),
     "conditions"),
    (lambda d: d.drop(columns=["cvd_24h"]), "gate_inputs_unavailable"),
])
def test_each_blocked_reason_prevents_trading(mutate, reason) -> None:
    """
    The old loop enumerated vol_gate, 4h_trend and conditions only; daily_trend
    and btc_regime fell through to the trade simulator.
    """
    res = _scan(mutate(_frame()))
    assert res["n"] == 0, f"{reason}: a blocked signal was traded"
    assert reason in res["blocked"]


def test_btc_regime_blocks_where_it_applies() -> None:
    df = _frame_for(ETH, btc=(90.0, 100.0))              # BTC below its EMA
    res = _scan(df, asset=ETH)
    assert res["n"] == 0
    assert res["blocked"]["btc_regime"] > 0


def test_btc_regime_is_not_applicable_to_btc_itself() -> None:
    """
    BTC never receives the regime column. Requiring it would refuse every BTC
    signal; the frame carries no btc_* columns and must still trade.
    """
    assert wf._btc_regime_applicable(BTC, wf._asset_config(BTC)) is False
    assert _scan(_frame_for(BTC), asset=BTC)["n"] > 0


def test_btc_regime_is_not_applicable_to_zec() -> None:
    assert wf._btc_regime_applicable(ZEC, wf._asset_config(ZEC)) is False


def test_an_applicable_btc_gate_without_data_blocks() -> None:
    """Applicable but unreadable is unavailable, not "not bear"."""
    res = _scan(_frame_for(ETH), asset=ETH)              # no btc_* columns
    assert res["n"] == 0
    assert res["blocked"]["gate_inputs_unavailable"] > 0


def test_the_baseline_frame_does_trade() -> None:
    """Without this, every assertion above is vacuous."""
    res = _scan(_frame())
    assert res["n"] > 0
    assert res["blocked"].get("daily_trend", 0) == 0


# ── Censoring ────────────────────────────────────────────────────────────────

def test_an_unobservable_horizon_is_pending_not_max_hold() -> None:
    """
    The old simulator reported an unfilled horizon as a completed MAX_HOLD —
    an invented outcome, and every slice has a right edge.
    """
    res = _scan(_pending_frame())           # ZEC max_hold is 36h
    assert res["n_pending"] > 0, "a censored trade was resolved anyway"


def test_pending_trades_enter_no_statistic() -> None:
    res = _scan(_pending_frame())
    assert res["n_pending"] > 0
    # n counts RESOLVED trades only, and the pending one appears in no list.
    assert res["n"] == len(res["signal_ts"])
    assert res["n"] + res["n_pending"] > res["n"]


# ── Window boundaries ────────────────────────────────────────────────────────

def test_intervals_are_half_open_and_do_not_overlap() -> None:
    """
    A bar exactly at the boundary belongs to the later slice. Otherwise the
    train slice reaches into its own test slice and the exercise is circular.
    """
    df = _frame(n=200)
    boundary = df.index[100]
    train = _scan(df, start=df.index[0], end=boundary)
    test = _scan(df, start=boundary, end=df.index[-1] + pd.Timedelta(hours=1))
    whole = _scan(df, start=df.index[0], end=df.index[-1] + pd.Timedelta(hours=1))

    train_ts = set(train["signal_ts"])
    assert boundary.isoformat() not in train_ts
    # No trade is lost or double-counted at the seam beyond the skip_until
    # carry-over, which cannot make the halves exceed the whole.
    assert train["n"] + test["n"] >= whole["n"] - 1


def test_an_empty_interval_is_refused() -> None:
    df = _frame()
    with pytest.raises(WalkForwardError, match="empty interval"):
        wf.run_scan(df, df.index[10], df.index[10], ZEC, 2.0, 3.5)


def test_every_registered_window_is_ordered() -> None:
    for win in wf.WINDOWS:
        assert win["train_start"] < win["test_start"] < win["test_end"], win["label"]


# ── No silent substitution ───────────────────────────────────────────────────

def test_an_unknown_asset_is_refused_not_given_eth_config() -> None:
    with pytest.raises(WalkForwardError, match="ASSET_CONFIG"):
        wf._asset_config("NOPE-USD")


def test_an_asset_without_a_max_hold_is_refused() -> None:
    with pytest.raises(WalkForwardError, match="STRATEGY_CONFIG"):
        wf._max_hold("NOPE-USD")


def test_max_hold_is_the_assets_own() -> None:
    """
    The frozen ZEC mechanism is 36h and BTC's own config is 48h. Borrowing one
    for the other is how a "per-asset" validation stops being per-asset.
    """
    assert wf._max_hold(ZEC) == 36
    assert _scan(_frame())["max_hold_hours"] == 36


# ── Selection rule ───────────────────────────────────────────────────────────

def _score(stop, n, avg):
    return {"atr_stop": stop, "atr_target": stop * wf.RR_RATIO, "n": n,
            "avg_pnl": avg}


def test_selection_needs_enough_resolved_trades() -> None:
    """
    The old code fell back to `valid = train_scores` when nothing qualified, so
    it selected on samples of one or two. Now nothing is selected, and nothing
    is tested.
    """
    assert wf._select_on_train([_score(1.5, 2, 5.0), _score(2.0, 1, 9.0)]) == {}


def test_selection_prefers_the_best_qualifying_candidate() -> None:
    best = wf._select_on_train([_score(1.5, 5, -1.0), _score(2.0, 5, 0.5),
                                _score(2.5, 2, 99.0)])
    assert best["atr_stop"] == 2.0, "an under-sampled candidate must not win"


def test_selection_is_deterministic_on_ties() -> None:
    a = wf._select_on_train([_score(1.5, 5, 1.0), _score(3.0, 5, 1.0)])
    b = wf._select_on_train([_score(3.0, 5, 1.0), _score(1.5, 5, 1.0)])
    assert a["atr_stop"] == b["atr_stop"] == 1.5


# ── Frame assembly comes from the scanner ────────────────────────────────────

def test_load_asset_delegates_to_the_scanner_assembly() -> None:
    """
    The hand-rolled 1h+4h assembly is exactly what omitted the daily frame.
    Delegating means daily and BTC context cannot be forgotten again.
    """
    with patch.object(wf, "build_merged_frame",
                      return_value=(_frame(), None)) as bmf:
        wf.load_asset(ETH)
    _, kwargs = bmf.call_args
    assert kwargs["btc_regime_applicable"] is True
    args = bmf.call_args[0]
    assert args[0] == ETH


def test_load_asset_refuses_an_empty_frame() -> None:
    with patch.object(wf, "build_merged_frame", return_value=(None, None)):
        with pytest.raises(WalkForwardError, match="no data"):
            wf.load_asset(ZEC)


# ── Protocol is frozen ───────────────────────────────────────────────────────

def test_the_sweep_is_not_widened() -> None:
    """Extending the grid turns a repair into a parameter search."""
    assert wf.STOP_CANDIDATES == [1.5, 2.0, 2.5, 3.0]
    assert wf.RR_RATIO == 1.75
    assert len(wf.WINDOWS) == 3


def test_the_universe_is_declared_not_inherited() -> None:
    """
    signal_scanner.ASSETS also lists disabled expansion candidates with no
    candle cache, so inheriting it made the run depend on what happened to be
    downloaded.
    """
    from backtesting.signal_scanner import ASSETS as SCANNER_ASSETS

    assert wf.ASSETS == ["BTC-USD", "ETH-USD", "SOL-USD", "ZEC-USD"]
    assert len(SCANNER_ASSETS) > len(wf.ASSETS)


def test_the_tool_declares_what_it_is_not() -> None:
    """
    These windows are in the registry's multiple-testing budget. The artifact
    has to say so, because the previous README presented exactly these windows
    as proof of a genuine out-of-sample edge.
    """
    for needle in ("NOT clean out-of-sample", "multiple-testing budget"):
        assert needle in wf.__doc__, needle


# ── No network, no credentials ───────────────────────────────────────────────

def test_no_authenticated_exchange_access() -> None:
    """
    The tool reads the local candle cache through the scanner's loader. It must
    never construct the trading client: a research tool that can authenticate is
    a research tool that can place an order.
    """
    import inspect

    src = inspect.getsource(wf)
    for banned in ("RESTClient", "key_file", "cdp_api_key", "_get_client",
                   "place_limit_order", "place_order_outbox"):
        assert banned not in src, f"walk_forward references {banned}"


def test_scanning_needs_no_network() -> None:
    """
    Every statistic comes from the frame it is handed. The suite denies outbound
    network at the socket layer, so a scan that reached out would raise here
    rather than silently succeed on a developer machine.
    """
    res = _scan(_frame())
    assert res["n"] > 0        # real work happened, offline


# ── Slice boundaries: no outcome may borrow a candle from the future ─────────
#
# The tool bounded only the ENTRY time and handed _simulate_trade the whole
# dataframe, so a train trade could close on test candles and that outcome then
# chose the stop multiplier; test trades in Windows 1 and 2 could resolve past
# their own test_end; and PENDING was measured against the end of ALL data
# rather than the end of the slice.

def _late_entry_frame() -> pd.DataFrame:
    """One signal a few bars before the boundary, resolvable only after it."""
    df = _frame_for(ZEC, n=80).copy()
    df["close"] = 100.0                       # below EMA50, no earlier signals
    cross = df.index[38:]
    df.loc[cross, "close"] = 105.0            # single cross at bar 38
    df["high"] = df["close"] + 0.2
    df["low"] = df["close"] - 0.2
    # Well past the boundary, price finally reaches the target.
    df.loc[df.index[60:], "high"] = 200.0
    return df


def test_a_trade_cannot_resolve_on_candles_past_the_boundary() -> None:
    df = _late_entry_frame()
    boundary = df.index[45]                   # 7 bars after the entry
    res = _scan(df, start=df.index[0], end=boundary)
    assert res["n"] == 0, "a trade resolved using candles from the next slice"
    assert res["n_pending"] == 1


def test_the_same_trade_resolves_when_the_slice_contains_the_exit() -> None:
    """The mirror case: without it the test above could pass for any reason."""
    df = _late_entry_frame()
    res = _scan(df, start=df.index[0], end=df.index[-1] + pd.Timedelta(hours=1))
    assert res["n"] == 1
    assert res["n_pending"] == 0


def test_candles_after_the_boundary_cannot_change_a_score() -> None:
    """
    THE property. Train scores must be a function of train candles alone,
    otherwise the stop multiplier is selected using the test period.
    """
    df = _late_entry_frame()
    boundary = df.index[45]
    base = _scan(df, start=df.index[0], end=boundary)

    mutated = df.copy()
    after = mutated.index >= boundary
    mutated.loc[after, ["close", "high", "low"]] = 9999.0
    changed = _scan(mutated, start=df.index[0], end=boundary)

    assert base == changed, "post-boundary candles influenced the slice"


def test_every_resolved_exit_is_inside_the_slice() -> None:
    df = _frame_for(ZEC, n=200)
    boundary = df.index[120]
    res = _scan(df, start=df.index[0], end=boundary)
    assert res["n"] > 0
    assert res["last_exit_ts"] is not None
    assert res["last_exit_ts"] < boundary.isoformat()


def test_a_slice_with_no_rows_before_its_end_is_refused() -> None:
    df = _frame_for(ZEC, n=40)
    with pytest.raises(WalkForwardError, match="no rows before"):
        wf.run_scan(df, df.index[0] - pd.Timedelta(days=5),
                    df.index[0] - pd.Timedelta(days=1), ZEC, 2.0, 3.5)


# ── Input scopes are this tool's own ────────────────────────────────────────

def test_input_scopes_end_where_the_tool_ends() -> None:
    """
    Borrowing the research runner's scope would hash candles through 2026-07-12
    and let a 2026 revision invalidate a walk-forward that stops in June 2025.
    """
    from backtesting.research_runner import _scope_end

    for interval, scope in wf.INPUT_SCOPES.items():
        assert scope["end"] == wf._END, interval
        assert scope["end"] < _scope_end(), (
            f"{interval} scope reaches past what this tool reads")


def test_the_daily_scope_covers_the_ema_warmup() -> None:
    """The daily EMAs need far more history than the 1h span."""
    from backtesting.signal_scanner import _DAILY_HISTORY_START

    assert wf.INPUT_SCOPES["1d"]["start"] == _DAILY_HISTORY_START
    assert wf.INPUT_SCOPES["1d"]["start"] < wf.INPUT_SCOPES["1h"]["start"]


def test_research_runner_is_a_declared_dependency() -> None:
    """It supplies the hashing and environment primitives, so it determines these results."""
    assert "backtesting/research_runner.py" in wf._CODE_PATHS


# ── The canonical artifact is write-protected ───────────────────────────────

def test_a_partial_universe_may_not_write_the_canonical_artifact() -> None:
    """`--asset ZEC-USD` used to overwrite the four-asset artifact with one asset."""
    with pytest.raises(WalkForwardError, match="canonical artifact covers"):
        wf._assert_writable([ZEC])


def test_writing_requires_the_registered_interpreter(monkeypatch) -> None:
    import backtesting.research_runner as rr

    monkeypatch.setattr(rr.platform, "python_version", lambda: "3.12.9")
    with pytest.raises(rr.ProvenanceError, match="3.12.9"):
        wf._assert_writable(list(wf.ASSETS))


def test_the_loader_refuses_a_provider_fallback() -> None:
    """
    A registered run whose inputs were hashed from parquet must not quietly
    compute on yfinance data.
    """
    import inspect

    src = inspect.getsource(wf.load_asset)
    assert "STRICT_COINBASE_ONLY = True" in src

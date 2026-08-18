"""
Fail-closed gate semantics — trial 2026-08-warmup-semantics.

The defect these tests pin: `_detect_breakout_signal` used to fail OPEN when an
indicator was still warming up. A hard gate whose operand was NaN was skipped
outright (`x is not None and x < y`), and scored inputs were coerced to neutral
defaults (`_safe(col) or 1.0`). The bar was then judged by a WEAKER mechanism
than the config declares, and nothing counted it.

Everything here runs on synthetic frames — no candle cache, no network.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtesting.signal_scanner import (
    _detect_breakout_signal,
    gate_availability,
    gate_input_columns,
)

_DAILY_EMA_COL = "ema200_1d"


def _frame(n: int = 60, **overrides) -> pd.DataFrame:
    """
    A frame that produces a BUY on the last row: price crosses EMA50 from below
    two bars ago and every gate input is present and passing.
    """
    idx = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    close = np.full(n, 100.0)
    ema50 = np.full(n, 101.0)          # price below EMA50 …
    close[-2:] = 105.0                 # … then two bars above it (the cross)
    df = pd.DataFrame({
        "time":         idx,
        "close":        close,
        "ema50":        ema50,
        "high":         close + 1,
        "low":          close - 1,
        "atr":          np.full(n, 1.0),
        "rsi":          np.full(n, 50.0),
        "adx":          np.full(n, 30.0),
        "volume_ratio": np.full(n, 1.5),
        "cvd_24h":      np.full(n, 10.0),
        "close_4h":     np.full(n, 105.0),
        "ema50_4h":     np.full(n, 100.0),
        "close_1d":     np.full(n, 105.0),
        _DAILY_EMA_COL: np.full(n, 100.0),
    }, index=idx)
    for col, val in overrides.items():
        df[col] = val
    return df


_CFG = {"daily_ema_period": 200, "min_conditions": 3, "vol_spike_ratio": 1.3}


def test_baseline_frame_produces_a_buy() -> None:
    """Guards the fixture itself: without it, every 'refused' assertion is vacuous."""
    res = _detect_breakout_signal(_frame(), 59, _CFG, btc_regime_applicable=False)
    assert res is not None and res.get("signal") == "BUY", res


@pytest.mark.parametrize("col", [
    _DAILY_EMA_COL,   # the one that actually bit: 19 ZEC trades, +22.28%
    "close_1d",
    "close_4h",
    "ema50_4h",
    "volume_ratio",
    "adx",
    "cvd_24h",
])
def test_missing_gate_input_refuses_the_signal(col: str) -> None:
    """
    Every one of these used to be waved through. A gate that cannot be evaluated
    must refuse, never admit — otherwise the run silently measures a different
    strategy than the one it reports.
    """
    df = _frame()
    df[col] = np.nan
    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=False)
    assert res is not None
    assert res.get("blocked") == "gate_inputs_unavailable", (
        f"missing {col} produced {res.get('blocked')!r} instead of a refusal"
    )
    assert col in res["missing"] or f"{col}@cross" in res["missing"]


def test_absent_column_is_treated_as_unavailable_not_as_neutral() -> None:
    """
    walk_forward never merged the daily frame at all, so the column was ABSENT
    rather than NaN. Both mean the same thing and must behave identically.
    """
    df = _frame().drop(columns=[_DAILY_EMA_COL, "close_1d"])
    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=False)
    assert res.get("blocked") == "gate_inputs_unavailable"
    assert set(res["missing"]) == {_DAILY_EMA_COL, "close_1d"}


def test_rsi_is_checked_on_the_cross_bar_not_the_signal_bar() -> None:
    """rsi_at_cross is read off a different row, so it needs its own check."""
    df = _frame()
    df.iloc[58, df.columns.get_loc("rsi")] = np.nan     # the cross bar
    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=False)
    assert res.get("blocked") == "gate_inputs_unavailable"
    assert "rsi@cross" in res["missing"]


def test_configured_period_drives_the_check_not_a_hardcoded_200() -> None:
    """
    A 50-EMA config must require ema50_1d and must NOT require ema200_1d.
    regime['ema200_valid'] is hardcoded to 200 regardless of config, which is
    exactly why it cannot serve as the availability signal.
    """
    df = _frame()
    df["ema50_1d"] = 100.0
    df[_DAILY_EMA_COL] = np.nan            # irrelevant to a 50-EMA mechanism
    cfg50 = {**_CFG, "daily_ema_period": 50}
    res = _detect_breakout_signal(df, 59, cfg50, btc_regime_applicable=False)
    assert res.get("signal") == "BUY", res

    df2 = _frame()
    df2["ema50_1d"] = np.nan
    res2 = _detect_breakout_signal(df2, 59, cfg50, btc_regime_applicable=False)
    assert res2.get("blocked") == "gate_inputs_unavailable"
    assert "ema50_1d" in res2["missing"]


def test_inapplicable_btc_gate_is_not_required() -> None:
    """
    Not applicable is not the same as unavailable. BTC-USD never receives a
    BTC-regime column; requiring it there would refuse every BTC signal.
    """
    df = _frame()                                    # no btc_* columns at all
    assert _detect_breakout_signal(
        df, 59, _CFG, btc_regime_applicable=False).get("signal") == "BUY"

    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=True)
    assert res.get("blocked") == "gate_inputs_unavailable"
    assert set(res["missing"]) == {"btc_close_1d", "btc_ema50_1d"}


def test_applicable_btc_gate_still_blocks_on_a_real_reading() -> None:
    """Present-but-bearish must stay a gate block, not a refusal."""
    df = _frame()
    df["btc_close_1d"] = 90.0
    df["btc_ema50_1d"] = 100.0
    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=True)
    assert res.get("blocked") == "btc_regime"


def test_zero_volume_ratio_no_longer_reads_as_neutral() -> None:
    """
    `_safe("volume_ratio") or 1.0` turned a legitimate 0.0 into 1.0, which
    cleared the 0.8 volume gate. Falsy is not missing.
    """
    df = _frame()
    df["volume_ratio"] = 0.0
    res = _detect_breakout_signal(df, 59, _CFG, btc_regime_applicable=False)
    assert res.get("blocked") == "vol_gate"
    assert res["vol_ratio"] == 0.0


def test_no_signal_stays_distinguishable_from_a_refusal() -> None:
    """
    Three outcomes, three meanings. Collapsing "no setup here" into "refused"
    would make the exclusion counters meaningless.
    """
    df = _frame()
    df["close"] = 100.0                     # never crosses above EMA50
    df[_DAILY_EMA_COL] = np.nan
    assert _detect_breakout_signal(
        df, 59, _CFG, btc_regime_applicable=False) is None


# ── Availability reporting ────────────────────────────────────────────────────

def test_gate_availability_reports_the_binding_gate() -> None:
    df = _frame()
    df.iloc[:30, df.columns.get_loc(_DAILY_EMA_COL)] = np.nan
    avail = gate_availability(df, _CFG, btc_regime_applicable=False)
    assert avail["per_gate"]["daily_trend"] == df.index[30].isoformat()
    # effective_start is the LAST gate to become evaluable, not the first.
    assert avail["effective_start"] == df.index[30].isoformat()


def test_gate_availability_is_none_when_a_gate_never_becomes_evaluable() -> None:
    """A never-available gate must not silently yield an effective start."""
    df = _frame()
    df[_DAILY_EMA_COL] = np.nan
    avail = gate_availability(df, _CFG, btc_regime_applicable=False)
    assert avail["per_gate"]["daily_trend"] is None
    assert avail["effective_start"] is None


# ── Live path ────────────────────────────────────────────────────────────────
#
# scan_latest() is the live entry gate in pipeline/runner.py, and it shares
# _detect_breakout_signal with the research scanner. Making the gates fail
# closed is therefore a deliberate LIVE-PATH SAFETY CHANGE, and it needs live
# coverage: a failed daily-candle download used to drop the daily trend veto
# entirely and let a BUY through during a data outage.

def _live_frames(n: int = 80):
    """1h and 4h frames shaped the way _download_and_compute returns them."""
    idx = pd.date_range("2024-01-01", periods=n, freq="h", tz="UTC")
    close = np.full(n, 100.0)
    close[-3:] = 105.0
    one_h = pd.DataFrame({
        "time": idx, "close": close, "ema50": np.full(n, 101.0),
        "ema200": np.full(n, 99.0), "high": close + 1, "low": close - 1,
        "atr": np.full(n, 1.0), "rsi": np.full(n, 50.0), "adx": np.full(n, 30.0),
        "volume_ratio": np.full(n, 1.5), "cvd_24h": np.full(n, 10.0),
        "trend": ["bull"] * n,
    }, index=idx)
    four_h = one_h.copy()
    four_h["close"] = 105.0
    four_h["ema50"] = 100.0
    return one_h, four_h


def _scan_latest_with(daily, monkeypatch) -> object:
    import backtesting.signal_scanner as scanner

    one_h, four_h = _live_frames()

    def fake_download(asset, start, end, interval):
        if interval == "1h":
            return one_h.copy()
        if interval == "4h":
            return four_h.copy()
        return None if daily is None else daily.copy()

    monkeypatch.setattr(scanner, "_download_and_compute", fake_download)
    return scanner.scan_latest("ZEC-USD")


def test_live_scan_refuses_when_the_daily_frame_is_unavailable(monkeypatch, capsys) -> None:
    """
    The outage case. ZEC declares a 200-day daily EMA veto; with no daily frame
    the veto is not evaluable, so no entry may be produced.
    """
    assert _scan_latest_with(None, monkeypatch) is None
    out = capsys.readouterr().out
    assert "REFUSED" in out and "gate inputs unavailable" in out, (
        "a refusal must be visible in the log — a silent None reads as 'no setup'"
    )


def test_live_scan_still_produces_a_signal_when_inputs_are_present(monkeypatch) -> None:
    """The refusal must be caused by the missing input, not by the fixture."""
    d_idx = pd.date_range("2023-06-01", periods=400, freq="D", tz="UTC")
    daily = pd.DataFrame({
        "time": d_idx, "close": np.full(400, 105.0),
        "ema50": np.full(400, 100.0), "ema200": np.full(400, 100.0),
    }, index=d_idx)
    assert _scan_latest_with(daily, monkeypatch) is not None


# ── walk_forward now shares this detector ────────────────────────────────────
#
# These replace two tests that pinned walk_forward in its DISABLED state. That
# state was correct while the tool still assembled its own frame; it was
# repaired in trial 2026-08-walkforward-repair.v1, so what needs pinning now is
# that it uses THIS detector rather than growing a second, weaker copy.

def test_walk_forward_uses_the_shared_detector_and_frame_builder() -> None:
    """
    The repair works precisely because the tool stopped assembling its own
    frame. A future edit that reintroduces a private loader would silently
    reintroduce the missing daily context.
    """
    import inspect

    import backtesting.walk_forward as wf

    src = inspect.getsource(wf)
    assert "build_merged_frame" in src, "frame assembly must come from the scanner"
    assert "_simulate_trade" in src, "simulation must come from the scanner"
    assert "attach_higher_timeframe_context" not in src, (
        "walk_forward is assembling its own frame again"
    )


def test_walk_forward_still_refuses_to_call_itself_out_of_sample() -> None:
    """
    Repairing the tool did not make its windows clean. They were inspected
    repeatedly during development and are in the registry's multiple-testing
    budget, and a previous README presented exactly these windows as proof of a
    genuine out-of-sample edge.
    """
    import backtesting.walk_forward as wf

    assert "NOT clean out-of-sample" in wf.__doc__
    assert "multiple-testing budget" in wf.__doc__


def test_declared_gate_columns_follow_the_config() -> None:
    cols = gate_input_columns({"daily_ema_period": 50}, btc_regime_applicable=False)
    assert cols["daily_trend"] == ["close_1d", "ema50_1d"]
    assert "btc_regime" not in cols
    assert "btc_regime" in gate_input_columns(
        {"daily_ema_period": 200}, btc_regime_applicable=True)

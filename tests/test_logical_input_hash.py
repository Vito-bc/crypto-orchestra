"""
Logical input identity — the window-scoped OHLCV hash.

The physical parquet SHA-256 was never an identity for the DATA. It covered the
whole file, including rows past the freeze that the exchange keeps completing
and revising, so a fresh public download of identical research data mismatched
on all eight inputs while `results.json` stayed byte-identical: the hash
asserted strictly more than what determines the results.

The replacement hashes the OHLCV a registered scan can actually read. These
tests pin both halves of that contract — what must be ignored, and what must
never be.

Synthetic frames only: no candle cache, no network.
"""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from backtesting.research_runner import (
    _HASH_SCHEME,
    _SCOPE_START,
    ProvenanceError,
    _scope_end,
    logical_sha256,
    scoped_frame,
)

SCOPE_START = pd.Timestamp(_SCOPE_START)


def _frame(start=SCOPE_START, periods: int = 48, freq: str = "h") -> pd.DataFrame:
    idx = pd.date_range(start, periods=periods, freq=freq, tz="UTC")
    n = len(idx)
    return pd.DataFrame({
        "time": idx,
        "open": np.arange(n, dtype="float64") + 100.0,
        "high": np.arange(n, dtype="float64") + 101.0,
        "low": np.arange(n, dtype="float64") + 99.0,
        "close": np.arange(n, dtype="float64") + 100.5,
        "volume": np.arange(n, dtype="float64") + 10.0,
    })


# ── What must be ignored: everything past the freeze ─────────────────────────

def test_rows_after_the_scope_end_do_not_affect_the_hash() -> None:
    """
    THE regression. The exchange completes and revises recent candles forever;
    the research never reads them. A required CI check built on the physical
    hash failed on exactly this.
    """
    base = _frame()
    tail = _frame(start=pd.Timestamp(_scope_end()) + pd.Timedelta(hours=1),
                  periods=200)
    extended = pd.concat([base, tail], ignore_index=True)

    assert logical_sha256(base) == logical_sha256(extended)


def test_revising_a_row_after_the_scope_end_does_not_affect_the_hash() -> None:
    tail = _frame(start=pd.Timestamp(_scope_end()) + pd.Timedelta(hours=1),
                  periods=5)
    a = pd.concat([_frame(), tail], ignore_index=True)
    b = a.copy()
    b.loc[b.index[-1], "close"] = 99999.0          # a completed candle changing
    assert logical_sha256(a) == logical_sha256(b)


def test_rows_before_the_scope_start_do_not_affect_the_hash() -> None:
    early = _frame(start=SCOPE_START - pd.Timedelta(days=30), periods=48)
    assert logical_sha256(_frame()) == logical_sha256(
        pd.concat([early, _frame()], ignore_index=True))


# ── What must never be ignored: anything inside the scope ────────────────────

@pytest.mark.parametrize("column", ["open", "high", "low", "close", "volume"])
def test_changing_any_in_scope_value_breaks_the_hash(column) -> None:
    a = _frame()
    b = a.copy()
    b.loc[b.index[5], column] = b.loc[b.index[5], column] + 0.01
    assert logical_sha256(a) != logical_sha256(b)


def test_changing_a_warmup_row_breaks_the_hash() -> None:
    """
    Warm-up is INPUT, not context: those rows determine the EMAs and therefore
    gate availability itself. That is why the scope starts at
    _DAILY_HISTORY_START and not at asset_effective_start.
    """
    a = _frame(periods=48)
    b = a.copy()
    b.loc[b.index[0], "close"] += 1.0              # first warm-up bar
    assert logical_sha256(a) != logical_sha256(b)


def test_inserting_an_in_scope_row_breaks_the_hash() -> None:
    a = _frame()
    extra = _frame(start=a["time"].iloc[-1] + pd.Timedelta(hours=1), periods=1)
    assert logical_sha256(a) != logical_sha256(
        pd.concat([a, extra], ignore_index=True))


def test_deleting_an_in_scope_row_breaks_the_hash() -> None:
    a = _frame()
    assert logical_sha256(a) != logical_sha256(a.drop(index=a.index[10]))


def test_the_boundary_row_is_inside_the_scope() -> None:
    """
    coinbase_candles.download slices `time >= start & time <= end`, so the bar
    AT the boundary is read. An exclusive end would silently drop a real input.
    """
    boundary = pd.Timestamp(_scope_end())
    a = _frame(start=boundary - pd.Timedelta(hours=3), periods=4)
    assert a["time"].iloc[-1] == boundary
    b = a.copy()
    b.loc[b.index[-1], "close"] += 1.0
    assert logical_sha256(a) != logical_sha256(b)


# ── Encoding is canonical, not incidental ────────────────────────────────────

def test_hash_ignores_row_order_and_index() -> None:
    """Identity is the data, not how a writer happened to lay it out."""
    a = _frame()
    shuffled = a.sample(frac=1.0, random_state=7).reset_index(drop=True)
    weird_index = a.set_index("time").reset_index()
    assert logical_sha256(a) == logical_sha256(shuffled)
    assert logical_sha256(a) == logical_sha256(weird_index)


def test_hash_ignores_extra_columns() -> None:
    a = _frame()
    b = a.copy()
    b["some_cached_indicator"] = 1.0
    assert logical_sha256(a) == logical_sha256(b)


def test_scheme_is_versioned_and_recorded() -> None:
    """
    Changing the encoding changes what verification means, so it must be a
    visible, deliberate act rather than a silent re-derivation.
    """
    assert _HASH_SCHEME == "ohlcv-logical-v1"
    a = _frame()
    import backtesting.research_runner as rr

    before = logical_sha256(a)
    original = rr._HASH_SCHEME
    try:
        rr._HASH_SCHEME = "ohlcv-logical-v2"
        assert logical_sha256(a) != before
    finally:
        rr._HASH_SCHEME = original


# ── Fail closed on unusable data ─────────────────────────────────────────────

def test_duplicate_timestamps_fail_closed() -> None:
    a = _frame()
    with pytest.raises(ProvenanceError, match="duplicate"):
        logical_sha256(pd.concat([a, a.iloc[[3]]], ignore_index=True))


@pytest.mark.parametrize("bad", [np.nan, np.inf, -np.inf])
def test_non_finite_values_fail_closed(bad) -> None:
    a = _frame()
    a.loc[a.index[2], "close"] = bad
    with pytest.raises(ProvenanceError, match="non-finite"):
        logical_sha256(a)


def test_non_numeric_values_fail_closed() -> None:
    a = _frame()
    a["close"] = a["close"].astype(object)
    a.loc[a.index[1], "close"] = "not a price"
    with pytest.raises(ProvenanceError, match="not numeric"):
        logical_sha256(a)


def test_missing_ohlcv_column_fails_closed() -> None:
    with pytest.raises(ProvenanceError, match="missing OHLCV"):
        logical_sha256(_frame().drop(columns=["volume"]))


def test_no_rows_in_scope_fails_closed() -> None:
    """An all-tail file must not hash to "empty" and quietly verify."""
    tail = _frame(start=pd.Timestamp(_scope_end()) + pd.Timedelta(days=1))
    with pytest.raises(ProvenanceError, match="no rows inside scope"):
        logical_sha256(tail)


def test_scoped_frame_is_sorted_and_bounded() -> None:
    tail = _frame(start=pd.Timestamp(_scope_end()) + pd.Timedelta(hours=1),
                  periods=3)
    scoped = scoped_frame(pd.concat([tail, _frame()], ignore_index=True))
    assert scoped["time"].is_monotonic_increasing
    assert scoped["time"].iloc[0] >= SCOPE_START
    assert scoped["time"].iloc[-1] <= pd.Timestamp(_scope_end())

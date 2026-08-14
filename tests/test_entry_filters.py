"""
Direct coverage of the REAL _check_entry_filters — Phase 6.8.

Why this file exists: the six entry filters are the live safety gates named in
CLAUDE.md, and until now nothing in `tests/` exercised the function. The one
integration test that drives run_pipeline patched it out wholesale, so four
filters could fall through to "allowed" whenever their inputs were missing and
no test could notice.

The contract under test:

  applicable + readable   -> the filter decides on a real measurement
  applicable + unreadable -> ENTRY BLOCKED, reason prefixed FILTER_DATA_UNAVAILABLE
  not applicable          -> skipped, does not block

Every test asserts WHICH of the three happened, never merely `allowed is False`
— a fail-closed block and a genuine veto are different outcomes and conflating
them is how the original defect stayed invisible.
"""
from __future__ import annotations

from contextlib import ExitStack
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from pipeline.runner import FILTER_DATA_UNAVAILABLE, _check_entry_filters
from tools.market_positioning import (
    FUNDING_NOT_APPLICABLE,
    FUNDING_OK,
    FUNDING_UNAVAILABLE,
)

# ZEC is the baseline asset precisely because two filters are NOT APPLICABLE to
# it by configuration: btc_regime_filter=False and no OKX perpetual. That keeps
# "not applicable" in the baseline rather than only in a special case.
ASSET = "ZEC-USD"
# ETH has btc_regime_filter=True, so the BTC veto genuinely applies.
BTC_FILTERED_ASSET = "ETH-USD"


def _raw_df(n: int = 30, close: float = 100.0, first: float | None = None):
    """Flat price frame -> 0% 24h change, so the velocity veto passes."""
    closes = [close] * n
    if first is not None:
        closes[-25] = first
    return pd.DataFrame({"close": closes, "high": closes, "low": closes})


def _daily(close_1d: float = 105.0, ema200: float | None = 100.0) -> dict:
    return {"close_1d": close_1d, "ema50_1d": 100.0, "ema200_1d": ema200}


def _funding(status: str = FUNDING_NOT_APPLICABLE, ann: float = 0.0) -> dict:
    return {"rate_pct": 0.0, "annualized_pct": ann, "signal": "NEUTRAL",
            "source": "OKX", "error": None, "status": status}


# Sentinel, not the string "default": comparing a DataFrame with == is
# elementwise and `if df == "default"` raises on the truth value.
_DEFAULT = object()


def _run(asset: str = ASSET, *, raw_df=_DEFAULT, daily=_DEFAULT,
         funding=None, snapshot=_DEFAULT, stops: int = 0, tmp_path=None,
         raises: tuple | None = None):
    """
    Drive the real _check_entry_filters with only its data sources stubbed.

    Defaults are the all-clear case: every applicable filter has readable inputs
    and none of them fires.
    """
    if raw_df is _DEFAULT:
        raw_df = _raw_df()
    if daily is _DEFAULT:
        daily = _daily()
    if snapshot is _DEFAULT:
        snapshot = {"close": 100.0, "atr_1h": 2.0, "trend_4h": "bull"}
    funding = funding if funding is not None else _funding()

    with ExitStack() as stack:
        e = stack.enter_context
        e(patch("pipeline.runner.get_raw_df", return_value=raw_df))
        e(patch("pipeline.runner.get_daily_trend", return_value=daily))
        e(patch("pipeline.runner.get_snapshot", return_value=snapshot))
        e(patch("pipeline.runner.count_recent_stops", return_value=stops))
        e(patch("tools.market_positioning.get_okx_funding_rate",
                return_value=funding))
        # Filter 3 reads the real trade-history file; point it at a path that
        # does not exist unless a test wants the bounce filter engaged.
        e(patch("pipeline.runner.TRADE_HISTORY",
                (tmp_path / "trades.jsonl") if tmp_path
                else __import__("pathlib").Path("does-not-exist.jsonl")))
        # Entered LAST so it overrides the stubs above. Patching the same target
        # from outside _run does not work: these context managers are applied
        # afterwards and would shadow it.
        if raises is not None:
            target, exc = raises
            e(patch(target, side_effect=exc))
        return _check_entry_filters(asset)


def _assert_unavailable(result, needle: str) -> None:
    allowed, reason, _ = result
    assert allowed is False, f"expected fail-closed block, got allowed={allowed}"
    assert reason.startswith(FILTER_DATA_UNAVAILABLE), (
        f"blocked, but not as a data-availability failure: {reason!r}"
    )
    assert needle in reason, f"{needle!r} not named in reason: {reason!r}"


def _assert_real_veto(result, needle: str) -> None:
    allowed, reason, _ = result
    assert allowed is False
    assert not reason.startswith(FILTER_DATA_UNAVAILABLE), (
        f"a genuine veto must not be reported as missing data: {reason!r}"
    )
    assert needle in reason, f"{needle!r} not in reason: {reason!r}"


# ── Baseline ─────────────────────────────────────────────────────────────────

def test_all_inputs_valid_allows_entry() -> None:
    """Guards the fixture: without this, every block assertion is vacuous."""
    allowed, reason, size = _run()
    assert allowed is True, reason
    assert reason == ""
    assert size == 1.0


# ── Filter 4: velocity ───────────────────────────────────────────────────────

def test_velocity_veto_is_a_real_reading() -> None:
    _assert_real_veto(_run(raw_df=_raw_df(close=90.0, first=100.0)),
                      "Velocity veto")


def test_velocity_blocks_when_frame_is_missing() -> None:
    _assert_unavailable(_run(raw_df=None), "24h velocity")


def test_velocity_blocks_when_frame_is_too_short() -> None:
    _assert_unavailable(_run(raw_df=_raw_df(n=24)), "24h velocity")


def test_velocity_blocks_on_non_finite_close() -> None:
    df = _raw_df()
    df.loc[df.index[-1], "close"] = np.nan
    _assert_unavailable(_run(raw_df=df), "24h velocity")


def test_velocity_blocks_on_infinite_close() -> None:
    df = _raw_df()
    df.loc[df.index[-25], "close"] = np.inf
    _assert_unavailable(_run(raw_df=df), "24h velocity")


def test_velocity_blocks_on_zero_denominator() -> None:
    """A zero 24h-ago price makes the percentage undefined, not 'unchanged'."""
    _assert_unavailable(_run(raw_df=_raw_df(first=0.0)), "24h velocity")


def test_velocity_blocks_when_close_column_is_absent() -> None:
    _assert_unavailable(
        _run(raw_df=pd.DataFrame({"open": [1.0] * 30})), "24h velocity")


# ── Filter 5: daily EMA ──────────────────────────────────────────────────────

def test_daily_ema_veto_is_a_real_reading() -> None:
    _assert_real_veto(_run(daily=_daily(close_1d=90.0, ema200=100.0)),
                      "Daily 200EMA veto")


def test_daily_blocks_when_frame_is_missing() -> None:
    _assert_unavailable(_run(daily=None), "daily 200EMA")


def test_daily_blocks_when_frame_is_empty() -> None:
    _assert_unavailable(_run(daily={}), "daily 200EMA")


def test_daily_blocks_when_the_configured_ema_is_absent() -> None:
    """
    get_daily_trend returns None for a missing EMA column. The old
    `is not None` test skipped the veto instead of refusing the entry.
    """
    _assert_unavailable(_run(daily=_daily(ema200=None)), "ema200_1d")


def test_daily_blocks_when_the_configured_ema_is_nan() -> None:
    """
    Present but still warming up. This is the live twin of the scanner defect
    corrected in trial 2026-08-warmup-semantics.
    """
    _assert_unavailable(_run(daily=_daily(ema200=float("nan"))), "ema200_1d")


def test_daily_blocks_on_non_finite_close() -> None:
    _assert_unavailable(_run(daily=_daily(close_1d=float("inf"))), "daily 200EMA")


def test_daily_blocks_on_non_numeric_values() -> None:
    _assert_unavailable(_run(daily={"close_1d": "105.0", "ema200_1d": 100.0}),
                        "daily 200EMA")


def test_daily_uses_the_asset_configured_period() -> None:
    """
    ZEC declares 200EMA. Supplying only ema50_1d must NOT satisfy it — reading
    whichever EMA happens to be present is how a declared gate becomes a
    different gate.
    """
    _assert_unavailable(
        _run(daily={"close_1d": 105.0, "ema50_1d": 100.0}), "ema200_1d")


# ── Filter 2: funding — applicability vs availability ────────────────────────

def test_funding_not_applicable_does_not_block() -> None:
    """ZEC has no OKX perpetual. Not applicable must never block."""
    allowed, reason, _ = _run(funding=_funding(FUNDING_NOT_APPLICABLE))
    assert allowed is True, reason


def test_funding_unavailable_blocks() -> None:
    """
    Supported instrument, unreadable. Previously indistinguishable from the ZEC
    case because both merely set `error`, so an OKX outage silently removed the
    veto for BTC/ETH/SOL.
    """
    bad = {**_funding(FUNDING_UNAVAILABLE), "error": "OKX request failed"}
    _assert_unavailable(_run(funding=bad), "OKX funding rate")


def test_funding_unknown_status_fails_closed() -> None:
    _assert_unavailable(_run(funding=_funding("something-new")),
                        "unknown status")


def test_funding_ok_above_threshold_is_a_real_veto() -> None:
    _assert_real_veto(_run(funding=_funding(FUNDING_OK, ann=25.0)),
                      "Funding rate veto")


def test_funding_ok_below_threshold_allows() -> None:
    allowed, reason, _ = _run(funding=_funding(FUNDING_OK, ann=5.0))
    assert allowed is True, reason


def test_funding_ok_with_non_finite_value_blocks() -> None:
    _assert_unavailable(_run(funding=_funding(FUNDING_OK, ann=float("nan"))),
                        "OKX funding rate")


# ── Filter 1: BTC regime — existing fail-closed must not regress ─────────────

def test_correlation_fail_closed_does_not_regress() -> None:
    """BTC bear + unknown correlation was already fail-closed. Keep it that way."""
    with patch("pipeline.runner._calc_btc_correlation", return_value=None):
        allowed, reason, _ = _run(
            BTC_FILTERED_ASSET,
            snapshot={"close": 100.0, "atr_1h": 2.0, "trend_4h": "bear"},
            funding=_funding(FUNDING_OK, ann=1.0))
    assert allowed is False
    assert "BTC BEAR veto" in reason
    assert "unknown" in reason


def test_high_correlation_still_blocks_in_btc_bear() -> None:
    with patch("pipeline.runner._calc_btc_correlation", return_value=0.90):
        allowed, reason, _ = _run(
            BTC_FILTERED_ASSET,
            snapshot={"close": 100.0, "atr_1h": 2.0, "trend_4h": "bear"},
            funding=_funding(FUNDING_OK, ann=1.0))
    assert allowed is False and "BTC BEAR veto" in reason


def test_partial_correlation_halves_size_without_blocking() -> None:
    """The 0.5 size modifier is existing behaviour and must be preserved."""
    with patch("pipeline.runner._calc_btc_correlation", return_value=0.50):
        allowed, reason, size = _run(
            BTC_FILTERED_ASSET,
            snapshot={"close": 100.0, "atr_1h": 2.0, "trend_4h": "bear"},
            funding=_funding(FUNDING_OK, ann=1.0))
    assert allowed is True, reason
    assert size == 0.5


def test_btc_snapshot_unavailable_blocks_when_filter_applies() -> None:
    """
    Previously the whole BTC veto was skipped when BTC's snapshot was missing —
    i.e. exactly when market data was degraded.
    """
    _assert_unavailable(
        _run(BTC_FILTERED_ASSET, snapshot=None, funding=_funding(FUNDING_OK)),
        "BTC 4h regime")


@pytest.mark.parametrize("trend", [
    "",           # get_snapshot's own fallback: row.get("trend_4h", "")
    "sideways",   # a label this code does not know
    "unknown",
    "BULL",       # case drift
    123,          # non-string
    None,
    True,
])
def test_undeclared_btc_trend_value_blocks(trend) -> None:
    """
    Only "bull" and "bear" answer the question. Every value here used to read as
    "not bear" to a bare `== "bear"` test and silently lifted the veto — and ""
    is not hypothetical, it is what tools/price_data.py substitutes when the
    column is absent.
    """
    _assert_unavailable(
        _run(BTC_FILTERED_ASSET,
             snapshot={"close": 100.0, "atr_1h": 2.0, "trend_4h": trend},
             funding=_funding(FUNDING_OK)),
        "BTC 4h regime")


@pytest.mark.parametrize("trend", ["bull", "bear"])
def test_declared_btc_trend_values_are_accepted(trend) -> None:
    """The whitelist must not block the values it exists to admit."""
    with patch("pipeline.runner._calc_btc_correlation", return_value=0.10):
        allowed, reason, _ = _run(
            BTC_FILTERED_ASSET,
            snapshot={"close": 100.0, "atr_1h": 2.0, "trend_4h": trend},
            funding=_funding(FUNDING_OK, ann=1.0))
    # bull -> no veto at all; bear + decorrelated -> veto lifted. Either way the
    # entry proceeds, and neither is a data-availability failure.
    assert allowed is True, reason


def test_btc_filter_not_applicable_for_zec_even_without_a_snapshot() -> None:
    """
    ZEC sets btc_regime_filter=False. Not applicable must stay not applicable —
    fail-closed must not turn a disabled filter into a blocker.
    """
    allowed, reason, _ = _run(ASSET, snapshot={"close": 100.0, "atr_1h": 2.0})
    assert allowed is True, reason


# ── Filter 3: bounce confirmation ────────────────────────────────────────────

def _write_stop(tmp_path, exit_price: float = 100.0):
    import json
    p = tmp_path / "trades.jsonl"
    p.write_text(json.dumps({"asset": ASSET, "reason": "STOP_LOSS",
                             "exit_price": exit_price}) + "\n", encoding="utf-8")
    return p


def test_bounce_blocks_when_snapshot_is_unavailable(tmp_path) -> None:
    """
    A recent stop makes this filter applicable. Without a snapshot the bounce
    cannot be measured, and skipping it re-entered immediately after a stop.
    """
    _write_stop(tmp_path)
    _assert_unavailable(_run(snapshot=None, tmp_path=tmp_path),
                        "bounce confirmation")


def test_bounce_blocks_on_zero_atr(tmp_path) -> None:
    """A zero ATR made the reported bounce distance 0 and compared anyway."""
    _write_stop(tmp_path)
    _assert_unavailable(
        _run(snapshot={"close": 100.0, "atr_1h": 0.0}, tmp_path=tmp_path),
        "bounce confirmation")


def test_bounce_veto_is_a_real_reading(tmp_path) -> None:
    _write_stop(tmp_path, exit_price=100.0)
    _assert_real_veto(
        _run(snapshot={"close": 100.5, "atr_1h": 2.0}, tmp_path=tmp_path),
        "Bounce confirmation needed")


def test_bounce_satisfied_allows_entry(tmp_path) -> None:
    _write_stop(tmp_path, exit_price=100.0)
    allowed, reason, _ = _run(
        snapshot={"close": 110.0, "atr_1h": 2.0, "trend_4h": "bull"},
        tmp_path=tmp_path)
    assert allowed is True, reason


# ── Filter 6: whipsaw ────────────────────────────────────────────────────────

def test_whipsaw_guard_is_a_real_reading() -> None:
    _assert_real_veto(_run(stops=2), "Whipsaw guard")


# ── Sources that raise, and corrupt state ────────────────────────────────────

@pytest.mark.parametrize("asset,target", [
    (ASSET, "pipeline.runner.get_raw_df"),
    (ASSET, "pipeline.runner.get_daily_trend"),
    (ASSET, "pipeline.runner.count_recent_stops"),
    (ASSET, "tools.market_positioning.get_okx_funding_rate"),
    # get_snapshot is only consulted when a filter that needs it APPLIES. ZEC
    # sets btc_regime_filter=False and has no stop history here, so nothing asks
    # for a snapshot and its failure is correctly irrelevant. ETH does ask.
    (BTC_FILTERED_ASSET, "pipeline.runner.get_snapshot"),
])
def test_a_raising_source_becomes_an_unavailable_result(asset, target) -> None:
    """
    A source that raises is unavailable data, not a different kind of event.
    Letting it escape meant the function did not return its declared tuple,
    produced no FILTER_DATA_UNAVAILABLE, and could abort a whole scheduler pass
    over a single asset.
    """
    _assert_unavailable(
        _run(asset, funding=_funding(FUNDING_OK, ann=1.0),
             raises=(target, ConnectionError("upstream down"))),
        "entry filters")


def test_a_source_that_is_never_consulted_cannot_fail_the_entry() -> None:
    """
    The mirror of the case above, and the reason it is parametrised by asset:
    ZEC never asks for a BTC snapshot, so a broken get_snapshot must not block
    it. Fail-closed applies to filters that APPLY, not to every import.
    """
    allowed, reason, _ = _run(
        ASSET, raises=("pipeline.runner.get_snapshot",
                       ConnectionError("upstream down")))
    assert allowed is True, reason


def test_corrupt_stop_record_without_exit_price_blocks(tmp_path) -> None:
    """`rec["exit_price"]` raised KeyError straight out of the function."""
    import json
    (tmp_path / "trades.jsonl").write_text(
        json.dumps({"asset": ASSET, "reason": "STOP_LOSS"}) + "\n",
        encoding="utf-8")
    _assert_unavailable(_run(tmp_path=tmp_path), "bounce confirmation")


@pytest.mark.parametrize("bad", [None, "100.0", float("nan"), float("inf")])
def test_corrupt_stop_record_with_unusable_exit_price_blocks(tmp_path, bad) -> None:
    import json
    (tmp_path / "trades.jsonl").write_text(
        json.dumps({"asset": ASSET, "reason": "STOP_LOSS", "exit_price": bad}) + "\n",
        encoding="utf-8")
    _assert_unavailable(_run(tmp_path=tmp_path), "bounce confirmation")


def test_unparseable_history_lines_are_skipped_not_fatal(tmp_path) -> None:
    """Pre-existing tolerance for junk lines must survive the new strictness."""
    p = tmp_path / "trades.jsonl"
    p.write_text("not json at all\n{\"partial\": \n", encoding="utf-8")
    allowed, reason, _ = _run(tmp_path=tmp_path)
    assert allowed is True, reason


# ── The interface itself ─────────────────────────────────────────────────────

@pytest.mark.parametrize("kwargs", [
    {"raw_df": None},
    {"daily": None},
    {"funding": _funding(FUNDING_UNAVAILABLE)},
])
def test_interface_shape_is_preserved_on_every_path(kwargs) -> None:
    """(allowed, reason, size_modifier) — callers unpack this positionally."""
    result = _run(**kwargs)
    assert isinstance(result, tuple) and len(result) == 3
    allowed, reason, size = result
    assert isinstance(allowed, bool)
    assert isinstance(reason, str) and reason
    assert isinstance(size, float)

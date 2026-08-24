"""
Phase 7R-2 cost probe — safety and arithmetic.

The probe was shipped without tests, and a probe is a measuring instrument: an
unnoticed defect here becomes a number in a registered protocol. Everything runs
against synthetic books and a mocked client — no network, no credentials.
"""
from __future__ import annotations

import inspect
import json
from types import SimpleNamespace
from unittest.mock import patch

import pytest

import backtesting.stf_cost_probe as cp


def _book_side(levels):
    return [{"price": str(p), "size": str(s)} for p, s in levels]


# ── It cannot reach an account ───────────────────────────────────────────────

def test_the_probe_cannot_place_or_cancel_an_order() -> None:
    """
    A read-only instrument must not be one refactor away from trading. Checked
    on executable tokens so the module's own prose about NOT ordering does not
    satisfy the test.
    """
    import io
    import tokenize

    code = " ".join(
        tok.string
        for tok in tokenize.generate_tokens(
            io.StringIO(inspect.getsource(cp)).readline)
        if tok.type not in (tokenize.COMMENT, tokenize.STRING)
    )
    for banned in ("create_order", "market_order", "limit_order", "cancel_orders",
                   "place_limit_order", "place_order_outbox", "key_file",
                   "cdp_api_key"):
        assert banned not in code, f"{banned!r} reachable from the cost probe"


def test_the_client_is_built_without_key_material() -> None:
    with patch("coinbase.rest.RESTClient") as rc:
        cp._public_client()
    rc.assert_called_once_with()


def test_only_public_endpoints_are_swept() -> None:
    src = inspect.getsource(cp)
    assert "get_public_product_book" in src
    assert "get_public_candles" in src
    assert "client.get_product_book" not in src


def test_an_unauthenticated_fee_tier_is_unavailable_not_assumed() -> None:
    """
    get_transaction_summary is private. The probe must record that it could not
    read the fee, never substitute a guess — an unmeasured fee is what it exists
    to remove.
    """
    client = SimpleNamespace(
        get_transaction_summary=lambda: (_ for _ in ()).throw(
            RuntimeError("Unauthenticated request to private endpoint")))
    tier = cp._taker_tier(client)
    assert tier["available"] is False
    assert "credential" in tier["reason"]
    assert "taker_fee_rate" not in tier


def test_a_view_only_credential_can_supply_the_tier() -> None:
    """The path must work if a view-only key is deliberately provided."""
    client = SimpleNamespace(get_transaction_summary=lambda: SimpleNamespace(
        fee_tier=SimpleNamespace(taker_fee_rate="0.006", maker_fee_rate="0.004",
                                 pricing_tier="Advanced 1")))
    tier = cp._taker_tier(client)
    assert tier == {"available": True, "taker_fee_rate": 0.006,
                    "maker_fee_rate": 0.004, "pricing_tier": "Advanced 1"}


# ── Book parsing ─────────────────────────────────────────────────────────────

def test_non_finite_levels_are_dropped() -> None:
    """
    `inf` passes a bare `x == x` NaN check and would swallow the whole order at
    one level, reporting a fantasy price.
    """
    raw = _book_side([("inf", "1"), ("nan", "1"), ("100", "2")])
    assert cp._validated_levels(raw, "ask") == [(100.0, 2.0)]


@pytest.mark.parametrize("bad", [("0", "1"), ("100", "0"), ("-5", "1")])
def test_non_positive_levels_are_dropped(bad) -> None:
    assert cp._validated_levels(_book_side([bad, ("100", "2")]), "ask") == [(100.0, 2.0)]


def test_levels_are_sorted_by_side() -> None:
    """
    Nothing in the response type guarantees ordering, and an out-of-order level
    silently corrupts a sweep.
    """
    scrambled = _book_side([("102", "1"), ("100", "1"), ("101", "1")])
    assert cp._validated_levels(scrambled, "ask") == [(100.0, 1.0), (101.0, 1.0),
                                                     (102.0, 1.0)]
    assert cp._validated_levels(scrambled, "bid") == [(102.0, 1.0), (101.0, 1.0),
                                                     (100.0, 1.0)]


def test_a_sweep_reports_incompleteness_rather_than_extrapolating() -> None:
    out = cp._sweep([(100.0, 0.001)], notional=1000.0)
    assert out["complete"] is False
    assert out["filled_quote"] < 1000.0


def test_a_sweep_is_size_weighted() -> None:
    out = cp._sweep([(100.0, 1.0), (200.0, 1.0)], notional=200.0)
    # $100 at 100 buys 1 unit; $100 at 200 buys 0.5. VWAP = 200 / 1.5.
    assert out["vwap"] == pytest.approx(133.33333333, abs=1e-6)
    assert out["complete"] is True


# ── Sizing is the frozen protocol value ──────────────────────────────────────

def test_the_notional_comes_from_the_frozen_protocol() -> None:
    """
    The power study modelled 5% of capital while this measured the live 2%, so
    cost and drawdown described different mechanisms.
    """
    from backtesting.stf_protocol import POSITION_FRACTION_OF_CAPITAL

    with patch("pipeline.sizing.live_balance_usd", return_value=1000.0):
        assert cp.trial_notional() == 1000.0 * POSITION_FRACTION_OF_CAPITAL


def test_the_probe_does_not_track_the_live_default() -> None:
    """A change to the live per-order default must not redefine a registered trial."""
    src = inspect.getsource(cp.trial_notional)
    assert "trade_size_pct" not in src
    assert "position_notional" in src


# ── Sampling window ──────────────────────────────────────────────────────────

def test_sampling_outside_the_execution_window_is_refused(monkeypatch) -> None:
    """Cost measured at another hour describes a different market."""
    import datetime as dt

    class Noon(dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return dt.datetime(2026, 8, 21, 12, 0, tzinfo=dt.timezone.utc)

    monkeypatch.setattr(cp, "datetime", Noon)
    with pytest.raises(cp.CostProbeError, match="execution window"):
        cp.observe()


# ── Report arithmetic ────────────────────────────────────────────────────────

def _write_obs(path, rows, in_window=True, candle=True):
    with path.open("a", encoding="utf-8") as fh:
        for spread in rows:
            fh.write(json.dumps({
                "observed_at": "2026-08-21T00:10:00+00:00",
                "in_execution_window": in_window,
                "assets": [{
                    "asset": "BTC-USD", "spread_bps": spread,
                    "buy": {"quoted_impact_bps": spread / 2},
                    "sell": {"quoted_impact_bps": spread / 2},
                    "daily_candle": {"available": candle},
                }],
            }) + "\n")


def test_out_of_window_observations_are_excluded(tmp_path, monkeypatch) -> None:
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], in_window=True)
    _write_obs(obs, [900.0], in_window=False)

    r = cp.report()
    assert r["observations_used"] == 2
    assert r["observations_skipped_out_of_window"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 2.0


def test_readings_without_a_published_daily_bar_are_excluded(tmp_path, monkeypatch) -> None:
    """
    If the bar the signal needs was not published yet, the reading describes a
    moment the strategy could not have traded in.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0, 2.0], candle=True)
    _write_obs(obs, [500.0], candle=False)

    r = cp.report()
    assert r["asset_readings_skipped_no_daily_bar"] == 1
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["n"] == 2
    assert r["per_asset"]["BTC-USD"]["spread_bps"]["max"] == 2.0


def test_percentiles_use_nearest_rank(tmp_path, monkeypatch) -> None:
    """
    `int(p * n)` truncates: at n=10 the p90 picked the 10th observation — the
    maximum — instead of the 9th.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [float(i) for i in range(1, 11)])      # 1..10

    stats = cp.report()["per_asset"]["BTC-USD"]["spread_bps"]
    assert stats["n"] == 10
    assert stats["p90"] == 9.0, "p90 must be the 9th of ten, not the maximum"
    assert stats["p50"] == 5.0
    assert stats["max"] == 10.0


def test_p99_is_not_reported() -> None:
    """At 14-30 observations p99 is the single worst reading, not a tail estimate."""
    assert 0.99 not in cp._PERCENTILES
    src = inspect.getsource(cp)
    assert "why_no_p99" in src


def test_the_report_says_it_measures_quoted_impact(tmp_path, monkeypatch) -> None:
    """
    A static book gives impact against displayed liquidity, not a fill. Calling
    it slippage would overstate what was measured.
    """
    obs = tmp_path / "probe.jsonl"
    monkeypatch.setattr(cp, "OBSERVATIONS", obs)
    _write_obs(obs, [1.0])
    r = cp.report()
    assert "QUOTED IMPACT" in r["measures"]
    assert "quoted_impact_round_trip_bps" in r["per_asset"]["BTC-USD"]


def test_report_without_observations_fails_loudly(tmp_path, monkeypatch) -> None:
    monkeypatch.setattr(cp, "OBSERVATIONS", tmp_path / "absent.jsonl")
    with pytest.raises(cp.CostProbeError, match="no observations"):
        cp.report()

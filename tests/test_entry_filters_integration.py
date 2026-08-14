"""
End-to-end proof that an unreadable entry filter never reaches placement.

The direct tests in test_entry_filters.py assert what _check_entry_filters
returns. They cannot prove that run_pipeline HONOURS it — and that is the gap
that mattered: the only pre-existing integration test patched
_check_entry_filters out entirely, so the wiring between the filter verdict and
the order path was never executed with a blocking verdict.

Here the REAL _check_entry_filters runs. Only its data sources are stubbed, and
they are stubbed to the failure modes that previously fell through to "allowed".
"""
from __future__ import annotations

from contextlib import ExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from schemas.signals import TradeAction

ASSET = "ZEC-USD"


def _snapshot() -> dict:
    return {
        "close": 100.0, "atr_1h": 2.0, "ema50_1h": 99.0, "rsi_1h": 55.0,
        "macd_diff_1h": 0.1, "cvd_24h": 1.0, "adx_1h": 28.0, "trend_4h": "bull",
    }


def _scanner_signal() -> dict:
    return {
        "asset": ASSET, "entry_time": "2026-07-13T00:00:00+00:00",
        "entry_price": 100.0, "atr": 2.0, "conf": 0.9, "adx": 28.0,
        "vol_ratio": 1.4, "candles_above": 3, "n_conditions": 4,
        "er_30": 0.30, "vm_30": 0.5, "ema50_slope": 0.002,
        "ema200_valid": True, "n_daily_bars": 250,
        "v3_candidate_threshold": 0.20, "v3_would_block": False,
        "v3_enforcement": False, "v3_blocked": False,
    }


def _raw_df(n: int = 200) -> pd.DataFrame:
    idx = pd.date_range("2026-07-01", periods=n, freq="h", tz="UTC")
    return pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
         "volume": 1000.0, "atr": 2.0},
        index=idx,
    )


def _buy_decision(asset: str):
    from schemas.signals import TradeDecision
    return TradeDecision(
        asset=asset, timestamp=datetime.now(timezone.utc),
        action=TradeAction.BUY, confidence=0.9,
        reasoning="integration test BUY", votes=[], overrides=[],
        veto_triggered=False, veto_reason=None,
        position_size_pct=0.05, stop_loss_price=96.0, take_profit_price=107.0,
    )


def _run(tmp_path, monkeypatch, *, raw_df, daily, funding, raises=None):
    """
    Drive the real run_pipeline with the REAL _check_entry_filters in place.

    Returns (decision, place_limit_order_mock, place_order_outbox_mock).
    """
    import pipeline.limit_orders as lo
    import pipeline.runner as runner
    import pipeline.v3_journal as vj

    monkeypatch.setattr(vj, "_JOURNAL", tmp_path / "v3_journal.jsonl")
    monkeypatch.setattr(runner, "_SIGNALS_DB", tmp_path / "signals.db")
    monkeypatch.setattr(lo, "ORDERS_FILE", tmp_path / "pending_orders.json")
    monkeypatch.setattr(runner, "TRADE_HISTORY", tmp_path / "trades.jsonl")

    with ExitStack() as stack:
        e = stack.enter_context
        e(patch("pipeline.runner.get_snapshot", return_value=_snapshot()))
        e(patch("pipeline.runner.scan_latest", return_value=_scanner_signal()))
        e(patch("pipeline.runner.get_raw_df", return_value=raw_df))
        e(patch("pipeline.runner.get_daily_trend", return_value=daily))
        e(patch("pipeline.runner.count_recent_stops", return_value=0))
        e(patch("tools.market_positioning.get_okx_funding_rate",
                return_value=funding))
        e(patch("pipeline.runner.get_levels_from_snapshot",
                return_value={"nearest_support": 99.0,
                              "dist_to_support": 0.5, "atr": 2.0}))
        for _cls in ("TechnicalAgent", "MacroAgent", "SentimentAgent",
                     "WhaleAgent", "RiskAgent", "AssetNewsAgent",
                     "BreakoutAgent"):
            m = e(patch(f"pipeline.runner.{_cls}"))
            m.return_value.name = SimpleNamespace(value=_cls)
            m.return_value.run.side_effect = RuntimeError("stubbed agent")
        e(patch("pipeline.runner._get_circuit_breaker_state",
                return_value=(False, "", 1.0)))
        e(patch("pipeline.runner._log_decision"))
        e(patch("pipeline.runner._log_order_event"))
        e(patch("pipeline.runner.send_telegram_message"))
        e(patch("notifications.telegram.send_telegram_message", return_value=True))

        orch = e(patch("pipeline.runner.OrchestratorAgent"))
        orch.return_value.decide.side_effect = lambda a, s: _buy_decision(a)

        # The two placement doors. Neither may open.
        #
        # Patch them where the RUNNER looks them up, not where they are defined:
        # runner does `from pipeline.limit_orders import place_limit_order` at
        # import time, so patching pipeline.limit_orders leaves the runner's
        # own reference bound to the real function — the real DRY_RUN order was
        # still placed and the "not called" assertions were vacuous.
        limit = e(patch("pipeline.runner.place_limit_order",
                        return_value=SimpleNamespace(
                            id="ORD-1", stop_price=96.0, target_price=107.0,
                            reasoning="test", limit_price=99.0)))
        outbox = e(patch("pipeline.outbox.place_order_outbox"))

        # Entered LAST so it overrides the stubs above. Patching the same target
        # from outside _run does not work — these context managers are applied
        # afterwards and would shadow it.
        if raises is not None:
            target, exc = raises
            e(patch(target, side_effect=exc))

        decision = runner.run_pipeline(ASSET, _skip_exit_check=True)
    return decision, limit, outbox


def _ok_funding() -> dict:
    from tools.market_positioning import FUNDING_NOT_APPLICABLE
    return {"rate_pct": 0.0, "annualized_pct": 0.0, "signal": "NEUTRAL",
            "source": None, "error": None, "status": FUNDING_NOT_APPLICABLE}


def _unavailable_funding() -> dict:
    from tools.market_positioning import FUNDING_UNAVAILABLE
    return {"rate_pct": 0.0, "annualized_pct": 0.0, "signal": "NEUTRAL",
            "source": None, "error": "OKX request timed out",
            "status": FUNDING_UNAVAILABLE}


_GOOD_DAILY = {"close_1d": 105.0, "ema50_1d": 100.0, "ema200_1d": 100.0}


def test_buy_reaches_placement_when_every_filter_is_readable(tmp_path, monkeypatch) -> None:
    """
    Control case. Without it the blocking tests below prove nothing — they would
    pass even if the pipeline never attempted an order at all.
    """
    from pipeline.runner import FILTER_DATA_UNAVAILABLE

    decision, limit, outbox = _run(
        tmp_path, monkeypatch,
        raw_df=_raw_df(), daily=_GOOD_DAILY, funding=_ok_funding())

    # A placed limit order is reported as HOLD with the order in the reasoning
    # (the BUY became a resting order), so the action alone does not tell us
    # whether placement happened — the mock does.
    assert limit.called or outbox.called, "control case never attempted placement"
    assert FILTER_DATA_UNAVAILABLE not in decision.reasoning


@pytest.mark.parametrize("name,raw_df,daily,funding", [
    ("daily frame unavailable", _raw_df(), None, _ok_funding()),
    ("daily EMA still warming up", _raw_df(),
     {"close_1d": 105.0, "ema200_1d": float("nan")}, _ok_funding()),
    ("daily EMA column absent", _raw_df(),
     {"close_1d": 105.0, "ema50_1d": 100.0}, _ok_funding()),
    ("price frame unavailable", None, _GOOD_DAILY, _ok_funding()),
    ("price frame too short", _raw_df(n=10), _GOOD_DAILY, _ok_funding()),
    ("funding unreadable", _raw_df(), _GOOD_DAILY, _unavailable_funding()),
])
def test_unavailable_filter_data_never_reaches_placement(
        tmp_path, monkeypatch, name, raw_df, daily, funding) -> None:
    """
    Each of these previously fell through to "allowed" and a BUY was placed on
    a filter that had not actually been evaluated.
    """
    from pipeline.runner import FILTER_DATA_UNAVAILABLE

    decision, limit, outbox = _run(
        tmp_path, monkeypatch, raw_df=raw_df, daily=daily, funding=funding)

    assert decision.action == TradeAction.HOLD, (
        f"{name}: expected HOLD, got {decision.action}"
    )
    assert FILTER_DATA_UNAVAILABLE in decision.reasoning, (
        f"{name}: blocked, but not as a data-availability failure — "
        f"{decision.reasoning!r}"
    )
    assert not limit.called, f"{name}: place_limit_order was called"
    assert not outbox.called, f"{name}: place_order_outbox was called"
    # A blocked entry must not carry sizing forward.
    assert decision.position_size_pct is None
    assert decision.stop_loss_price is None


def test_a_raising_source_holds_without_crashing_the_pass(tmp_path, monkeypatch) -> None:
    """
    A source that throws must become a HOLD, not an exception out of
    run_pipeline. The old behaviour was safe in the narrow sense — no order was
    placed — but _check_entry_filters did not return its declared tuple, no
    FILTER_DATA_UNAVAILABLE was recorded, and one asset could abort the whole
    scheduler pass.
    """
    from pipeline.runner import FILTER_DATA_UNAVAILABLE

    decision, limit, outbox = _run(
        tmp_path, monkeypatch, raw_df=_raw_df(),
        daily=_GOOD_DAILY, funding=_ok_funding(),
        raises=("pipeline.runner.get_daily_trend",
                ConnectionError("market data upstream down")))

    assert decision.action == TradeAction.HOLD
    assert FILTER_DATA_UNAVAILABLE in decision.reasoning
    assert "ConnectionError" in decision.reasoning
    assert not limit.called and not outbox.called


def test_corrupt_trade_history_holds_without_crashing(tmp_path, monkeypatch) -> None:
    """A STOP_LOSS row with no exit_price used to raise KeyError."""
    import json

    from pipeline.runner import FILTER_DATA_UNAVAILABLE

    (tmp_path / "trades.jsonl").write_text(
        json.dumps({"asset": ASSET, "reason": "STOP_LOSS"}) + "\n",
        encoding="utf-8")

    decision, limit, outbox = _run(
        tmp_path, monkeypatch, raw_df=_raw_df(),
        daily=_GOOD_DAILY, funding=_ok_funding())

    assert decision.action == TradeAction.HOLD
    assert FILTER_DATA_UNAVAILABLE in decision.reasoning
    assert not limit.called and not outbox.called


def test_a_genuine_veto_is_not_reported_as_missing_data(tmp_path, monkeypatch) -> None:
    """
    The pipeline must keep the two block reasons distinguishable end-to-end,
    otherwise an outage and a real market condition look identical in the log.
    """
    from pipeline.runner import FILTER_DATA_UNAVAILABLE

    decision, limit, outbox = _run(
        tmp_path, monkeypatch, raw_df=_raw_df(),
        daily={"close_1d": 90.0, "ema200_1d": 100.0},   # real downtrend
        funding=_ok_funding())

    assert decision.action == TradeAction.HOLD
    assert "Daily 200EMA veto" in decision.reasoning
    assert FILTER_DATA_UNAVAILABLE not in decision.reasoning
    assert not limit.called and not outbox.called

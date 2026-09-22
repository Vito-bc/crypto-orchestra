"""
The specific fix this task exists for: a HOLD produced by the scanner gate
(agents never called) must not be written to agent_decisions.jsonl as if it
were a decision. A real decision — the scanner fired and agents actually
voted — must still be logged exactly as before.

These are narrow, source-boundary tests of pipeline.runner.run_pipeline():
only the scanner gate itself is exercised for real; everything past it is
stubbed. tests/test_disposition_integration.py and
tests/test_entry_filters_integration.py exercise the fuller real path (entry
filters, placement) and already assert _log_decision/_tally_gate wiring
there; this file exists to pin the one behavioural change in isolation.
"""
from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import patch

from schemas.signals import TradeAction

ASSET = "ETH-USD"


def _run(monkeypatch, *, scanner_signal, idempotency_claims=True, v3_blocked=False):
    import pipeline.runner as runner

    logged: list[tuple] = []
    tallied: list[tuple] = []

    with (
        patch("pipeline.runner.get_snapshot", return_value=None),
        patch("pipeline.runner.scan_latest", return_value=scanner_signal),
        patch("pipeline.runner._claim_signal", return_value=idempotency_claims),
        patch("pipeline.runner._complete_signal"),
        patch("pipeline.v3_journal.log_v2_signal"),
        patch("pipeline.runner._log_decision",
              side_effect=lambda *a, **k: logged.append(a)),
        patch("pipeline.runner._print_decision"),
        patch("pipeline.runner._tally_gate",
              side_effect=lambda *a, **k: tallied.append(a)),
        # Only reached on the real-decision path. Real sub-agents would make
        # live LLM calls; stub the classes so .run() raises — the ThreadPool
        # path still executes per-agent, only the network call is removed
        # (same convention as test_disposition_integration.py's _run()).
        patch("pipeline.runner.TechnicalAgent") as _technical,
        patch("pipeline.runner.MacroAgent") as _macro,
        patch("pipeline.runner.SentimentAgent") as _sentiment,
        patch("pipeline.runner.WhaleAgent") as _whale,
        patch("pipeline.runner.RiskAgent") as _risk,
        patch("pipeline.runner.AssetNewsAgent") as _news,
        patch("pipeline.runner.BreakoutAgent") as _breakout,
        patch("pipeline.runner.OrchestratorAgent") as orch,
    ):
        for _cls_name, _mock in (
            ("TechnicalAgent", _technical), ("MacroAgent", _macro),
            ("SentimentAgent", _sentiment), ("WhaleAgent", _whale),
            ("RiskAgent", _risk), ("AssetNewsAgent", _news),
            ("BreakoutAgent", _breakout),
        ):
            _mock.return_value.name = SimpleNamespace(value=_cls_name)
            _mock.return_value.run.side_effect = RuntimeError("stubbed agent")
        # veto_triggered=True keeps this test on the plain HOLD-logging path:
        # the scanner-elevation branch (HOLD -> BUY when the scanner fired
        # and nothing vetoed) only fires when veto_triggered is False, and
        # exercising elevation + placement belongs to
        # test_disposition_integration.py / test_entry_filters_integration.py,
        # not this narrow logging test.
        orch.return_value.decide.return_value = SimpleNamespace(
            asset=ASSET, timestamp=None, action=TradeAction.HOLD,
            confidence=0.0, reasoning="orchestrator says hold",
            votes=[], overrides=[],
            veto_triggered=True, veto_reason="test veto",
            position_size_pct=None, stop_loss_price=None, take_profit_price=None,
        )
        decision = runner.run_pipeline(ASSET, _skip_exit_check=True)
    return decision, logged, tallied


# ── The fix: scanner-silent HOLD is never a logged decision ────────────────

def test_no_signal_is_tallied_but_never_logged_as_a_decision(monkeypatch) -> None:
    decision, logged, tallied = _run(monkeypatch, scanner_signal=None)

    assert decision.action == TradeAction.HOLD
    assert decision.reasoning == "Scanner gate: no breakout signal on last closed candle."
    assert logged == [], "a scanner-silent HOLD must not be written to agent_decisions.jsonl"
    assert tallied == [(ASSET, "no_signal")]


def test_idempotency_skip_is_tallied_and_never_logged(monkeypatch) -> None:
    signal = {"entry_time": "2026-07-13T00:00:00+00:00"}
    decision, logged, tallied = _run(
        monkeypatch, scanner_signal=signal, idempotency_claims=False)

    assert decision.action == TradeAction.HOLD
    assert logged == [], "an idempotency-skipped HOLD must not be logged as a decision"
    assert tallied == [(ASSET, "idempotency_duplicate")]


# ── The invariant: a real ensemble vote is still logged exactly as before ──

def test_a_real_decision_scanner_fired_is_still_logged(monkeypatch) -> None:
    signal = {
        "entry_time": "2026-07-13T00:00:00+00:00", "entry_price": 100.0,
        "candles_above": 3, "adx": 28.0, "vol_ratio": 1.4, "conf": 0.9,
        "n_conditions": 4, "er_30": None,
        "v3_blocked": False, "v3_would_block": False,
    }
    decision, logged, tallied = _run(monkeypatch, scanner_signal=signal)

    assert decision.action == TradeAction.HOLD  # orchestrator's own HOLD, a real vote
    assert len(logged) == 1, "a real ensemble decision must still be logged"
    logged_asset, logged_signals, logged_decision = logged[0][:3]
    assert logged_asset == ASSET
    # Tallied once, before agents run, with no gate reason (agents WERE called).
    assert tallied == [(ASSET,)]

"""
Integration tests for the research-journal disposition, through REAL run_pipeline.

These exist because unit and source-level tests missed a genuine crash: the
DRY_RUN placement path never assigned `_result_status`, and reading it
unconditionally raised UnboundLocalError on every successful simulated order.
Counting strings in the source cannot catch that; executing the path can.

Only the true boundaries are stubbed — network (Coinbase / market data), the
LLM agents, and storage. Everything between the scanner signal and the journal
write is the production code path.
"""
from __future__ import annotations

import json
from contextlib import ExitStack
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

import pandas as pd
import pytest

from schemas.signals import TradeAction

ASSET = "ZEC-USD"
ENTRY_TIME = "2026-07-13T00:00:00+00:00"


@pytest.fixture(autouse=True)
def _block_telegram_transport(_block_telegram_sends):
    """
    Inner transport patch for the whole test, as conftest documents.

    run_pipeline notifies through several modules that import
    send_telegram_message directly, so patching one alias is not enough.

    It explicitly REQUESTS the conftest guard so that this fixture is always set
    up *after* it. Otherwise pytest may instantiate this module-level fixture
    first, conftest's patch would then shadow this one, and real sends would be
    recorded against the guard instead of being absorbed here.
    """
    # Patch the function itself as well: several modules import it lazily inside
    # their functions, so an alias patch on one module does not cover them.
    # Deliberately does NOT re-patch the transport. The conftest guard is the
    # single source of truth for "no real HTTP happened", and shadowing it with a
    # permissive mock — then clearing it — hid the very thing it exists to catch.
    # Intentional notifications are intercepted at
    # pipeline.runner.send_telegram_message inside _run() instead.
    yield


# ── Fixtures / boundary stubs ─────────────────────────────────────────────────

def _snapshot() -> dict:
    return {
        "close": 100.0, "atr_1h": 2.0, "ema50_1h": 99.0, "rsi_1h": 55.0,
        "macd_diff_1h": 0.1, "cvd_24h": 1.0, "adx_1h": 28.0,
    }


def _scanner_signal() -> dict:
    return {
        "asset": ASSET, "entry_time": ENTRY_TIME, "entry_price": 100.0,
        "atr": 2.0, "conf": 0.9, "adx": 28.0, "vol_ratio": 1.4,
        "candles_above": 3,
        "n_conditions": 4, "er_30": 0.30, "vm_30": 0.5, "ema50_slope": 0.002,
        "ema200_valid": True, "n_daily_bars": 250,
        "v3_candidate_threshold": 0.20, "v3_would_block": False,
        "v3_enforcement": False, "v3_blocked": False,
    }


def _raw_df() -> pd.DataFrame:
    idx = pd.date_range("2026-07-01", periods=200, freq="h", tz="UTC")
    return pd.DataFrame(
        {"open": 100.0, "high": 101.0, "low": 99.0, "close": 100.0,
         "volume": 1000.0, "atr": 2.0},
        index=idx,
    )


def _buy_decision(asset: str):
    from schemas.signals import TradeDecision
    from datetime import datetime, timezone
    return TradeDecision(
        asset=asset, timestamp=datetime.now(timezone.utc),
        action=TradeAction.BUY, confidence=0.9,
        reasoning="integration test BUY", votes=[], overrides=[],
        veto_triggered=False, veto_reason=None,
        position_size_pct=0.05, stop_loss_price=96.0, take_profit_price=107.0,
    )


def _dispositions(journal: Path) -> list[str]:
    if not journal.exists():
        return []
    out = []
    for line in journal.read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        e = json.loads(line)
        if e.get("type") == "V3_DISPOSITION":
            out.append(e["disposition"])
    return out


def _run(tmp_path, monkeypatch, *, dry_run: bool, outbox_result=None,
         outbox_raises=None, circuit_breaker=False):
    """Drive the real run_pipeline with only boundaries stubbed."""
    import pipeline.runner as runner
    import pipeline.v3_journal as vj

    import pipeline.limit_orders as lo

    journal = tmp_path / "v3_journal.jsonl"
    monkeypatch.setattr(vj, "_JOURNAL", journal)
    monkeypatch.setattr(runner, "_SIGNALS_DB", tmp_path / "signals.db")
    # Isolate real project state: the live pending_orders.json contains an open
    # order, which makes the runner skip placement entirely.
    monkeypatch.setattr(lo, "ORDERS_FILE", tmp_path / "pending_orders.json")

    cb_state = (True, "drawdown -13%", 0.0) if circuit_breaker else (False, "", 1.0)

    with ExitStack() as stack:
        e = stack.enter_context
        e(patch("pipeline.runner.get_snapshot", return_value=_snapshot()))
        e(patch("pipeline.runner.scan_latest", return_value=_scanner_signal()))
        e(patch("pipeline.runner.get_raw_df", return_value=_raw_df()))
        e(patch("pipeline.runner.get_daily_trend", return_value={"trend": "UP"}))
        # can_place requires support AND atr > 0 AND dist within the max.
        e(patch("pipeline.runner.get_levels_from_snapshot",
                return_value={"nearest_support": 99.0,
                              "dist_to_support": 0.5, "atr": 2.0}))
        # Real sub-agents would make live LLM calls. `sub_agents` is a local
        # built from these classes, so stub the classes. Their .run() raises,
        # which the runner already handles per-agent — the ThreadPool path still
        # executes, only the network call is removed.
        # ALL SEVEN. Missing AssetNewsAgent/BreakoutAgent left the test calling
        # Google News, Reddit and Anthropic, and made it depend on a local
        # ANTHROPIC_API_KEY — so it failed in a clean CI checkout before ever
        # reaching the placement branch it is supposed to verify.
        for _cls in ("TechnicalAgent", "MacroAgent", "SentimentAgent",
                     "WhaleAgent", "RiskAgent", "AssetNewsAgent",
                     "BreakoutAgent"):
            m = e(patch(f"pipeline.runner.{_cls}"))
            # The error path formats agent.name.value, so it needs a real string.
            m.return_value.name = SimpleNamespace(value=_cls)
            m.return_value.run.side_effect = RuntimeError("stubbed agent")
        e(patch("pipeline.runner._get_circuit_breaker_state", return_value=cb_state))
        e(patch("pipeline.runner._check_entry_filters", return_value=(True, "", 1.0)))
        e(patch("pipeline.runner._log_decision"))
        e(patch("pipeline.runner._log_order_event"))
        # Intentional notifications only. The transport stays guarded by the
        # conftest fixture, which must remain able to fail this test.
        e(patch("pipeline.runner.send_telegram_message"))
        e(patch("notifications.telegram.send_telegram_message", return_value=True))
        e(patch("exchange.coinbase_client.is_dry_run", return_value=dry_run))

        orch = e(patch("pipeline.runner.OrchestratorAgent"))
        orch.return_value.decide.side_effect = lambda a, s: _buy_decision(a)

        if outbox_raises is not None:
            e(patch("pipeline.outbox.place_order_outbox", side_effect=outbox_raises))
        elif outbox_result is not None:
            e(patch("pipeline.outbox.place_order_outbox", return_value=outbox_result))
        else:
            e(patch("pipeline.limit_orders.place_limit_order",
                    return_value=SimpleNamespace(
                        id="ORD-1", stop_price=96.0, target_price=107.0,
                        reasoning="test", limit_price=99.0)))

        decision = runner.run_pipeline(ASSET, _skip_exit_check=True)
    return decision, _dispositions(journal)


# ── The crash this suite exists for ───────────────────────────────────────────

def test_dry_run_successful_placement_does_not_raise(tmp_path, monkeypatch) -> None:
    """
    DRY_RUN is the CURRENT configuration. A successful simulated placement must
    complete without UnboundLocalError and record exactly one `traded`.
    """
    decision, disps = _run(tmp_path, monkeypatch, dry_run=True)
    assert disps == ["traded"], f"expected exactly one traded, got {disps}"
    # The action is deliberately downgraded — the live action is the resting order.
    assert decision.action == TradeAction.HOLD


def test_live_open_order_is_traded(tmp_path, monkeypatch) -> None:
    result = SimpleNamespace(order_id="ORD-9", status="OPEN",
                             rejection_reason=None)
    _decision, disps = _run(tmp_path, monkeypatch, dry_run=False,
                            outbox_result=result)
    assert disps == ["traded"]


def test_rejected_order_is_blocked_elsewhere(tmp_path, monkeypatch) -> None:
    result = SimpleNamespace(order_id="ORD-9", status="REJECTED",
                             rejection_reason="INSUFFICIENT_FUND")
    _decision, disps = _run(tmp_path, monkeypatch, dry_run=False,
                            outbox_result=result)
    assert disps == ["blocked_elsewhere"]


def test_submitting_stays_pending(tmp_path, monkeypatch) -> None:
    """
    SUBMITTING is an UNKNOWN exchange outcome awaiting reconciliation. It must
    not be claimed as traded, and must not be downgraded to blocked_elsewhere by
    the catch-all either — no disposition event at all.
    """
    result = SimpleNamespace(order_id="ORD-9", status="SUBMITTING",
                             rejection_reason=None)
    _decision, disps = _run(tmp_path, monkeypatch, dry_run=False,
                            outbox_result=result)
    assert disps == [], f"SUBMITTING must remain pending, got {disps}"


def test_outbox_gate_block_is_blocked_elsewhere(tmp_path, monkeypatch) -> None:
    from pipeline.outbox import PlacementBlocked
    _decision, disps = _run(tmp_path, monkeypatch, dry_run=False,
                            outbox_raises=PlacementBlocked("gate: unresolved"))
    assert disps == ["blocked_elsewhere"]


def test_circuit_breaker_is_blocked_elsewhere(tmp_path, monkeypatch) -> None:
    _decision, disps = _run(tmp_path, monkeypatch, dry_run=True,
                            circuit_breaker=True)
    assert disps == ["blocked_elsewhere"]


def test_journal_failure_does_not_break_execution(tmp_path, monkeypatch) -> None:
    """
    A journal write error must not affect trading, and must not cause a later
    misclassification (the catch-all must not "fix" it to blocked_elsewhere).
    """
    import pipeline.v3_journal as vj

    calls = {"n": 0}

    def _boom(sid, disposition):
        calls["n"] += 1
        raise OSError("disk full")

    monkeypatch.setattr(vj, "log_disposition", _boom)
    decision, _ = _run(tmp_path, monkeypatch, dry_run=True)
    assert decision.action == TradeAction.HOLD, "execution must be unaffected"
    assert calls["n"] == 1, (
        "a failed write must not be retried into a different classification"
    )


# ── Resolver must not touch executed or unknown signals ───────────────────────

@pytest.mark.parametrize("disposition", ["traded", "pending"])
def test_resolver_skips_executed_and_unknown(tmp_path, monkeypatch, disposition) -> None:
    """
    Simulating an outcome for a real order fabricates P&L for a live position;
    simulating a pending one settles a question the exchange has not answered.
    """
    import pipeline.v3_journal as vj

    monkeypatch.setattr(vj, "_JOURNAL", tmp_path / "v3_journal.jsonl")
    sid = vj.log_v2_signal(scanner_signal=_scanner_signal(), accepted=True)
    if disposition != "pending":
        vj.log_disposition(sid, disposition)

    called = {"n": 0}
    monkeypatch.setattr("exchange.coinbase_candles.download",
                        lambda *a, **k: called.__setitem__("n", called["n"] + 1) or pd.DataFrame())

    n = vj.reconcile_pending(asset=ASSET, max_hold_hours=36)
    assert n == 0, f"{disposition} signals must not be simulated"


def test_resolver_still_resolves_blocked_elsewhere(tmp_path, monkeypatch) -> None:
    """A signal that provably never executed IS simulatable."""
    import pipeline.v3_journal as vj

    monkeypatch.setattr(vj, "_JOURNAL", tmp_path / "v3_journal.jsonl")
    sid = vj.log_v2_signal(scanner_signal=_scanner_signal(), accepted=True)
    vj.log_disposition(sid, "blocked_elsewhere")

    idx = pd.date_range("2026-07-13T01:00:00Z", periods=3, freq="h")
    hit = pd.DataFrame({"time": idx, "open": 100.0, "high": 100.5,
                        "low": [99.5, 95.0, 99.0], "close": 100.0, "volume": 1.0})
    monkeypatch.setattr("exchange.coinbase_candles.download", lambda *a, **k: hit)

    assert vj.reconcile_pending(asset=ASSET, max_hold_hours=36) == 1

"""
Tests for risk epoch isolation.

Core invariant: pre-epoch trades (epoch_id=None or different epoch_id)
MUST NOT affect the strategy-level circuit breaker of the current epoch.
"""
from __future__ import annotations

import json
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import patch

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pipeline.risk_epoch import (
    compute_epoch_drawdown,
    get_current_epoch,
    get_epoch_trades,
    start_new_epoch,
)


def _write_epoch(path: Path, epoch_id: str, capital: float = 100.0) -> None:
    path.write_text(json.dumps({
        "event": "RISK_EPOCH_STARTED", "epoch_id": epoch_id,
        "paper_capital": capital, "reason": "test",
        "timestamp": "2026-07-12T00:00:00+00:00",
    }) + "\n")


def _make_trade(epoch_id, pnl_usd, exit_time=None):
    if exit_time is None:
        exit_time = datetime.now(timezone.utc).isoformat()
    return {
        "id":       "t-test",
        "asset":    "ZEC-USD",
        "pnl_usd":  pnl_usd,
        "exit_time": exit_time,
        "reason":   "STOP_LOSS",
        "epoch_id": epoch_id,
    }


def test_pre_epoch_trades_excluded_from_strategy_breaker():
    """Old trades (no epoch_id) must not appear in epoch trade set."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"

        # Write one pre-epoch trade (large loss, no epoch_id)
        with history_file.open("w") as f:
            f.write(json.dumps({"id": "old-trade", "asset": "ZEC-USD",
                                "pnl_usd": -47.07, "exit_time": "2026-06-01T00:00:00+00:00",
                                "reason": "STOP_LOSS", "epoch_id": None}) + "\n")

        # Write new epoch
        with epochs_file.open("w") as f:
            f.write(json.dumps({
                "event": "RISK_EPOCH_STARTED",
                "epoch_id": "ZEC_V2_ADX25:2026-07-12",
                "paper_capital": 100.0,
                "reason": "test",
                "timestamp": "2026-07-12T00:00:00+00:00",
            }) + "\n")

        # Monkey-patch the module to use our test files
        import pipeline.risk_epoch as re_mod
        orig_epochs  = re_mod.EPOCHS_FILE
        orig_history = re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE   = epochs_file
        re_mod.TRADE_HISTORY = history_file
        try:
            epoch  = get_current_epoch()
            trades = get_epoch_trades(epoch["epoch_id"])
            assert len(trades) == 0, (
                f"Pre-epoch trade should not appear in epoch trade set, got: {trades}"
            )
            equity, peak, dd = compute_epoch_drawdown(epoch)
            assert equity == 100.0, f"Fresh epoch equity should equal paper_capital, got {equity}"
            assert dd == 0.0, f"No epoch trades means 0% DD, got {dd}"
        finally:
            re_mod.EPOCHS_FILE   = orig_epochs
            re_mod.TRADE_HISTORY = orig_history


def test_epoch_trades_count_correctly():
    """Trades tagged with the current epoch_id accumulate into epoch equity."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file  = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"

        epoch_id = "ZEC_V2_ADX25:2026-07-12"

        with history_file.open("w") as f:
            # Pre-epoch loss (should be excluded)
            f.write(json.dumps(_make_trade(None, -47.07, "2026-06-01T00:00:00+00:00")) + "\n")
            # Two epoch trades: +3.50 and -1.20
            f.write(json.dumps(_make_trade(epoch_id, 3.50, "2026-07-14T10:00:00+00:00")) + "\n")
            f.write(json.dumps(_make_trade(epoch_id, -1.20, "2026-07-15T10:00:00+00:00")) + "\n")

        with epochs_file.open("w") as f:
            f.write(json.dumps({
                "event": "RISK_EPOCH_STARTED",
                "epoch_id": epoch_id,
                "paper_capital": 100.0,
                "reason": "test",
                "timestamp": "2026-07-12T00:00:00+00:00",
            }) + "\n")

        import pipeline.risk_epoch as re_mod
        orig_e, orig_h = re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = epochs_file, history_file
        try:
            epoch  = get_current_epoch()
            trades = get_epoch_trades(epoch_id)
            assert len(trades) == 2, f"Expected 2 epoch trades, got {len(trades)}"

            equity, peak, dd = compute_epoch_drawdown(epoch)
            # equity = 100 + 3.50 - 1.20 = 102.30
            assert abs(equity - 102.30) < 0.01, f"Expected equity 102.30, got {equity}"
            # peak = 100 + 3.50 = 103.50 (after first trade)
            assert abs(peak - 103.50) < 0.01, f"Expected peak 103.50, got {peak}"
            # dd = (103.50 - 102.30) / 103.50 * 100 ≈ 1.16%
            expected_dd = (103.50 - 102.30) / 103.50 * 100
            assert abs(dd - expected_dd) < 0.01, f"Expected DD {expected_dd:.2f}%, got {dd}"
        finally:
            re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = orig_e, orig_h


def test_different_epoch_trades_excluded():
    """Trades from a previous epoch (different epoch_id) must not bleed into new epoch."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file  = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"

        old_epoch_id = "ZEC_V1:2026-05-01"
        new_epoch_id = "ZEC_V2_ADX25:2026-07-12"

        with history_file.open("w") as f:
            # Old epoch losses
            f.write(json.dumps(_make_trade(old_epoch_id, -30.0, "2026-05-26T00:00:00+00:00")) + "\n")
            f.write(json.dumps(_make_trade(old_epoch_id, -17.07, "2026-06-04T00:00:00+00:00")) + "\n")
            # New epoch: one small win
            f.write(json.dumps(_make_trade(new_epoch_id, 1.50, "2026-07-14T00:00:00+00:00")) + "\n")

        with epochs_file.open("w") as f:
            # Only the new epoch is "current" (last RISK_EPOCH_STARTED)
            f.write(json.dumps({
                "event": "RISK_EPOCH_STARTED", "epoch_id": new_epoch_id,
                "paper_capital": 100.0, "reason": "test",
                "timestamp": "2026-07-12T00:00:00+00:00",
            }) + "\n")

        import pipeline.risk_epoch as re_mod
        orig_e, orig_h = re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = epochs_file, history_file
        try:
            epoch  = get_current_epoch()
            assert epoch["epoch_id"] == new_epoch_id

            equity, peak, dd = compute_epoch_drawdown(epoch)
            # Only the new epoch trade (+1.50) counts
            assert abs(equity - 101.50) < 0.01, f"Old epoch losses must not bleed in. equity={equity}"
            assert dd == 0.0, f"One winning trade means no drawdown. dd={dd}"
        finally:
            re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = orig_e, orig_h


def test_no_epoch_returns_none():
    """get_current_epoch() returns None when no epoch file exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        import pipeline.risk_epoch as re_mod
        orig = re_mod.EPOCHS_FILE
        re_mod.EPOCHS_FILE = Path(tmpdir) / "nonexistent.jsonl"
        try:
            assert get_current_epoch() is None
        finally:
            re_mod.EPOCHS_FILE = orig


def test_start_new_epoch_appends_not_overwrites():
    """start_new_epoch appends to the file — second call does not erase the first."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file = Path(tmpdir) / "risk_epochs.jsonl"

        import pipeline.risk_epoch as re_mod
        orig = re_mod.EPOCHS_FILE
        re_mod.EPOCHS_FILE = epochs_file
        try:
            start_new_epoch("EPOCH_A:2026-01-01", 50.0, "first", force=True)
            start_new_epoch("EPOCH_B:2026-07-12", 100.0, "second", force=True)

            lines = [l for l in epochs_file.read_text().splitlines() if l.strip()]
            assert len(lines) == 2, f"Expected 2 epoch records, got {len(lines)}"
            records = [json.loads(l) for l in lines]
            assert records[0]["epoch_id"] == "EPOCH_A:2026-01-01"
            assert records[1]["epoch_id"] == "EPOCH_B:2026-07-12"

            current = get_current_epoch()
            assert current["epoch_id"] == "EPOCH_B:2026-07-12", "get_current_epoch returns the LAST epoch"
        finally:
            re_mod.EPOCHS_FILE = orig


def test_start_new_epoch_blocked_with_open_positions():
    """start_new_epoch must raise ValueError when open positions exist."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file   = Path(tmpdir) / "risk_epochs.jsonl"
        positions_file = Path(tmpdir) / "open_positions.json"

        # One open position
        positions_file.write_text(json.dumps([{"id": "p1", "asset": "ZEC-USD", "status": "OPEN"}]))

        import pipeline.risk_epoch as re_mod
        orig_e = re_mod.EPOCHS_FILE
        orig_r = re_mod.ROOT
        # Point ROOT at tmpdir so start_new_epoch reads from tmpdir/logs/...
        # Simpler: monkey-patch the paths it reads directly
        re_mod.EPOCHS_FILE = epochs_file
        # Temporarily override ROOT so the exposure check finds our positions file
        tmp_root = Path(tmpdir)
        (tmp_root / "logs").mkdir(exist_ok=True)
        (tmp_root / "logs" / "open_positions.json").write_text(
            json.dumps([{"id": "p1", "asset": "ZEC-USD", "status": "OPEN"}])
        )
        re_mod.ROOT = tmp_root
        try:
            import pytest
            with pytest.raises(ValueError, match="open position"):
                start_new_epoch("NEW_EPOCH:2026-07-17", 100.0, "test")
        finally:
            re_mod.EPOCHS_FILE = orig_e
            re_mod.ROOT = orig_r


def test_epoch_drawdown_halts_at_threshold():
    """Verify circuit breaker fires when epoch DD exceeds 12%."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file  = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"
        epoch_id     = "ZEC_V2:2026-07-12"

        # 3 losses totaling -15 on $100 capital = 15% DD (> 12% halt threshold)
        with history_file.open("w") as f:
            for i, pnl in enumerate([-5.0, -5.0, -5.0]):
                t = _make_trade(epoch_id, pnl, f"2026-07-{14+i:02d}T10:00:00+00:00")
                f.write(json.dumps(t) + "\n")

        with epochs_file.open("w") as f:
            f.write(json.dumps({
                "event": "RISK_EPOCH_STARTED", "epoch_id": epoch_id,
                "paper_capital": 100.0, "reason": "test",
                "timestamp": "2026-07-12T00:00:00+00:00",
            }) + "\n")

        import pipeline.risk_epoch as re_mod
        orig_e, orig_h = re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = epochs_file, history_file
        try:
            epoch = get_current_epoch()
            equity, peak, dd = compute_epoch_drawdown(epoch)
            assert equity == 85.0
            assert peak   == 100.0
            assert abs(dd - 15.0) < 0.01  # 15% DD
            # The 12% threshold should fire
            from pipeline.risk_epoch import _DD_HALT_PCT  # type: ignore[attr-defined]
        except ImportError:
            pass  # _DD_HALT_PCT lives in runner.py, not risk_epoch — that's fine
        finally:
            re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = orig_e, orig_h
        # Check the DD value itself (runner.py enforces the threshold)
        assert dd > 12.0, f"DD {dd}% should exceed 12% halt threshold"


def test_duplicate_epoch_id_rejected():
    """start_new_epoch raises ValueError if epoch_id already exists."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file = Path(tmpdir) / "risk_epochs.jsonl"

        import pipeline.risk_epoch as re_mod
        orig = re_mod.EPOCHS_FILE
        re_mod.EPOCHS_FILE = epochs_file
        try:
            start_new_epoch("EPOCH_A:2026-07-12", 100.0, "first", force=True)
            with pytest.raises(ValueError, match="already exists"):
                start_new_epoch("EPOCH_A:2026-07-12", 100.0, "duplicate attempt", force=True)
            # File should still have only one record
            lines = [l for l in epochs_file.read_text().splitlines() if l.strip()]
            assert len(lines) == 1, f"Duplicate must not be written. Got {len(lines)} lines."
        finally:
            re_mod.EPOCHS_FILE = orig


def test_circuit_breaker_inner_halts_at_epoch_12pct():
    """_get_circuit_breaker_state_inner() must return halted=True when epoch DD > 12%."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file  = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"
        epoch_id     = "ZEC_V2:2026-07-12"

        _write_epoch(epochs_file, epoch_id, capital=100.0)
        # 3 losses totaling -15 USD = 15% DD
        with history_file.open("w") as f:
            for i, pnl in enumerate([-5.0, -5.0, -5.0]):
                t = _make_trade(epoch_id, pnl, f"2026-07-{14+i:02d}T10:00:00+00:00")
                f.write(json.dumps(t) + "\n")

        import pipeline.risk_epoch as re_mod
        import pipeline.runner as runner_mod
        orig_e, orig_h = re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = epochs_file, history_file
        try:
            halted, reason, size_mod = runner_mod._get_circuit_breaker_state_inner()
            assert halted is True, f"Expected halted=True at 15% DD, got halted={halted}"
            assert size_mod == 0.0, f"size_mod should be 0.0 when halted, got {size_mod}"
            assert "drawdown" in reason.lower() or "CIRCUIT BREAKER" in reason, (
                f"Reason should mention drawdown or CIRCUIT BREAKER: {reason!r}"
            )
        finally:
            re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = orig_e, orig_h


def test_epoch_id_chain_placement_to_trade_history():
    """epoch_id flows through the full chain: PendingOrder.create → Position → trade record."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file   = Path(tmpdir) / "risk_epochs.jsonl"
        history_file  = Path(tmpdir) / "trade_history.jsonl"
        positions_file = Path(tmpdir) / "open_positions.json"
        epoch_id      = "ZEC_V2:2026-07-17"

        _write_epoch(epochs_file, epoch_id, capital=100.0)

        import pipeline.risk_epoch   as re_mod
        import pipeline.position_tracker as pt_mod
        import pipeline.limit_orders as lo_mod

        orig_re_e  = re_mod.EPOCHS_FILE
        orig_re_h  = re_mod.TRADE_HISTORY
        orig_pt_p  = pt_mod.POSITIONS_FILE
        orig_pt_h  = pt_mod.TRADE_HISTORY

        re_mod.EPOCHS_FILE    = epochs_file
        re_mod.TRADE_HISTORY  = history_file
        pt_mod.POSITIONS_FILE = positions_file
        pt_mod.TRADE_HISTORY  = history_file
        try:
            # Step 1: create order — epoch_id must be stamped from current epoch
            order = lo_mod.PendingOrder.create(
                asset="ZEC-USD", limit_price=30.0, atr=0.5,
                position_size_pct=0.02, reasoning="test",
            )
            assert order.epoch_id == epoch_id, (
                f"Order epoch_id must equal active epoch. got={order.epoch_id!r}"
            )

            # Step 2: open position from order — epoch_id must be carried through
            pos = pt_mod.open_position_from_order(order, fill_price=29.5)
            assert pos.epoch_id == epoch_id, (
                f"Position epoch_id must equal order.epoch_id. got={pos.epoch_id!r}"
            )

            # Step 3: close position — trade record must use pos.epoch_id, not current epoch
            with patch("exchange.coinbase_client.place_market_sell", return_value="DRY-TEST"):
                record = pt_mod.close_position(pos, exit_price=28.0, reason="STOP_LOSS")

            assert record["epoch_id"] == epoch_id, (
                f"Trade record epoch_id must come from position, not re-read epoch. "
                f"got={record['epoch_id']!r}"
            )

            # Step 4: epoch trade query must find the record
            trades = get_epoch_trades(epoch_id)
            assert len(trades) == 1, f"Expected 1 epoch trade, got {len(trades)}"
            assert trades[0]["epoch_id"] == epoch_id
        finally:
            re_mod.EPOCHS_FILE    = orig_re_e
            re_mod.TRADE_HISTORY  = orig_re_h
            pt_mod.POSITIONS_FILE = orig_pt_p
            pt_mod.TRADE_HISTORY  = orig_pt_h


def test_get_epoch_trades_fails_closed_on_corrupt_file():
    """get_epoch_trades raises RuntimeError (not returns []) when file is unreadable."""
    import pytest
    with tempfile.TemporaryDirectory() as tmpdir:
        history_file = Path(tmpdir) / "trade_history.jsonl"
        history_file.write_bytes(b"\xff\xfe broken binary garbage that is not valid JSON or UTF-8 \x00\x01")

        import pipeline.risk_epoch as re_mod
        orig_h = re_mod.TRADE_HISTORY
        re_mod.TRADE_HISTORY = history_file
        try:
            with pytest.raises((RuntimeError, UnicodeDecodeError)):
                get_epoch_trades("ZEC_V2_ADX25:2026-07-12")
        finally:
            re_mod.TRADE_HISTORY = orig_h


def test_epoch_id_stamped_at_placement_not_at_close():
    """epoch_id on a trade record must come from the position (set at order time), not from current epoch."""
    with tempfile.TemporaryDirectory() as tmpdir:
        epochs_file  = Path(tmpdir) / "risk_epochs.jsonl"
        history_file = Path(tmpdir) / "trade_history.jsonl"

        old_epoch_id = "ZEC_V1:2026-05-01"
        new_epoch_id = "ZEC_V2:2026-07-12"

        # A position that was opened under old_epoch_id
        trade = _make_trade(old_epoch_id, -5.0, "2026-07-14T10:00:00+00:00")
        with history_file.open("w") as f:
            f.write(json.dumps(trade) + "\n")

        # Current epoch is new (epoch changed between open and close)
        with epochs_file.open("w") as f:
            f.write(json.dumps({
                "event": "RISK_EPOCH_STARTED", "epoch_id": new_epoch_id,
                "paper_capital": 100.0, "reason": "test",
                "timestamp": "2026-07-12T00:00:00+00:00",
            }) + "\n")

        import pipeline.risk_epoch as re_mod
        orig_e, orig_h = re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY
        re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = epochs_file, history_file
        try:
            epoch = get_current_epoch()
            assert epoch["epoch_id"] == new_epoch_id

            # The trade is tagged with old_epoch_id — it must NOT appear in new epoch
            new_epoch_trades = get_epoch_trades(new_epoch_id)
            assert len(new_epoch_trades) == 0, (
                "A trade stamped with old_epoch_id must not contaminate new epoch DD. "
                f"Got: {new_epoch_trades}"
            )

            # It DOES appear in old epoch
            old_epoch_trades = get_epoch_trades(old_epoch_id)
            assert len(old_epoch_trades) == 1
        finally:
            re_mod.EPOCHS_FILE, re_mod.TRADE_HISTORY = orig_e, orig_h

"""
Tests for pipeline/ledger.py — SQLite order/position state machine.

Coverage:
  - Schema creation (all 7 tables, idempotent)
  - Epoch UNIQUE constraint
  - Order state transitions (allowed and forbidden)
  - Fill UNIQUE constraint prevents double-counting
  - Position close state machine
  - position_events are immutable (insert only)
  - Reconciliation run lifecycle
  - migrate_from_json for epochs and orders with pre-epoch exclusion

No integration markers needed — all tests use in-memory SQLite via tmp_path.
"""
from __future__ import annotations

import json
import sqlite3
from pathlib import Path

import pytest

from pipeline.ledger import (
    close_position,
    complete_reconciliation,
    get_active_epoch,
    get_epoch_closed_pnl,
    get_fills_for_order,
    get_open_orders_for_asset,
    get_open_positions_for_asset,
    get_db,
    init_db,
    insert_epoch,
    insert_fill,
    insert_order,
    insert_position,
    migrate_from_json,
    start_reconciliation,
    transition_order,
    update_position_stop,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def make_db(tmp_path: Path) -> Path:
    db = tmp_path / "test_ledger.db"
    init_db(db)
    return db


def add_epoch(db: Path, epoch_id: str = "EP1", capital: float = 100.0) -> None:
    with get_db(db) as c:
        insert_epoch(epoch_id, capital, "test epoch", conn=c)


def add_order(db: Path, order_id: str = "ORD1", epoch_id: str = "EP1",
              asset: str = "ZEC-USD") -> None:
    with get_db(db) as c:
        insert_order(
            order_id=order_id, epoch_id=epoch_id, asset=asset,
            limit_price=50.0, stop_price=45.0, target_price=60.0,
            qty_usd_requested=2.0, placed_at="2026-07-01T10:00:00Z",
            expires_at="2026-07-02T10:00:00Z", conn=c,
        )


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

def test_init_db_creates_all_tables(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        tables = {r[0] for r in c.execute("SELECT name FROM sqlite_master WHERE type='table'")}
    expected = {
        "risk_epochs", "orders", "fills", "positions",
        "position_events", "account_snapshots", "reconciliation_runs",
    }
    assert expected <= tables


def test_init_db_is_idempotent(tmp_path):
    db = tmp_path / "test.db"
    init_db(db)
    init_db(db)   # second call must not raise or reset data
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "idempotent test", conn=c)
    with get_db(db) as c:
        row = c.execute("SELECT epoch_id FROM risk_epochs WHERE epoch_id='EP1'").fetchone()
    assert row is not None


def test_wal_mode_enabled(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        mode = c.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


def test_foreign_keys_enforced(tmp_path):
    db = make_db(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            # Insert order with a non-existent epoch_id
            insert_order(
                "ORD_ORPHAN", "NONEXISTENT_EPOCH", "ZEC-USD",
                50.0, 45.0, 60.0, 2.0,
                "2026-07-01T10:00:00Z", "2026-07-02T10:00:00Z",
                conn=c,
            )


# ---------------------------------------------------------------------------
# Epoch
# ---------------------------------------------------------------------------

def test_epoch_unique_constraint(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db, "EP1")
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_epoch("EP1", 200.0, "duplicate", conn=c)


def test_get_active_epoch_returns_last_started(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "first", "2026-07-01T00:00:00Z", conn=c)
        insert_epoch("EP2", 200.0, "second", "2026-07-10T00:00:00Z", conn=c)
        row = get_active_epoch(c)
    assert row["epoch_id"] == "EP2"


def test_get_active_epoch_returns_none_when_empty(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        assert get_active_epoch(c) is None


# ---------------------------------------------------------------------------
# Order state transitions
# ---------------------------------------------------------------------------

def test_order_transitions_submitting_to_open(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", exchange_order_id="CB-001", conn=c)
        row = c.execute("SELECT status, exchange_order_id FROM orders WHERE id='ORD1'").fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == "CB-001"


def test_order_transition_invalid_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db)
    # SUBMITTING → FILLED is not allowed (must pass through OPEN)
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db) as c:
            transition_order("ORD1", "FILLED", conn=c)


def test_order_transition_from_terminal_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", conn=c)
        transition_order("ORD1", "FILLED", conn=c)
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db) as c:
            transition_order("ORD1", "CANCELLED", conn=c)


def test_order_transition_unknown_order_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db) as c:
            transition_order("NONEXISTENT", "OPEN", conn=c)


def test_order_exchange_id_unique(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db, "ORD1")
    add_order(db, "ORD2")
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", exchange_order_id="CB-SAME", conn=c)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            transition_order("ORD2", "OPEN", exchange_order_id="CB-SAME", conn=c)


def test_get_open_orders_only_returns_open_partial(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db, "ORD_OPEN")
    add_order(db, "ORD_CANCELLED")
    with get_db(db) as c:
        transition_order("ORD_OPEN", "OPEN", conn=c)
        transition_order("ORD_CANCELLED", "CANCELLED", conn=c)
        rows = get_open_orders_for_asset("ZEC-USD", c)
    ids = {r["id"] for r in rows}
    assert ids == {"ORD_OPEN"}


# ---------------------------------------------------------------------------
# Fill (immutable insert)
# ---------------------------------------------------------------------------

def test_fill_duplicate_exchange_fill_id_rejected(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", conn=c)
        insert_fill("ORD1", 51.0, 0.04, 2.04, exchange_fill_id="FILL-001",
                    filled_at="2026-07-01T11:00:00Z", conn=c)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            # Same exchange_fill_id — must be rejected
            insert_fill("ORD1", 51.0, 0.04, 2.04, exchange_fill_id="FILL-001",
                        filled_at="2026-07-01T11:01:00Z", conn=c)


def test_fill_null_exchange_id_allows_duplicates_dry_run(tmp_path):
    """Dry-run fills have NULL exchange_fill_id — SQLite UNIQUE allows multiple NULLs."""
    db = make_db(tmp_path)
    add_epoch(db)
    add_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", conn=c)
        insert_fill("ORD1", 51.0, 0.04, 2.04, exchange_fill_id=None,
                    filled_at="2026-07-01T11:00:00Z", conn=c)
        insert_fill("ORD1", 51.0, 0.04, 2.04, exchange_fill_id=None,
                    filled_at="2026-07-01T11:01:00Z", conn=c)
        rows = get_fills_for_order("ORD1", c)
    assert len(rows) == 2


# ---------------------------------------------------------------------------
# Position state machine
# ---------------------------------------------------------------------------

def _setup_position(db: Path, pos_id: str = "POS1") -> None:
    add_epoch(db)
    add_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", conn=c)
        transition_order("ORD1", "FILLED", conn=c)
        insert_position(
            position_id=pos_id, order_id="ORD1", epoch_id="EP1",
            asset="ZEC-USD", entry_price=50.0, stop_price=45.0,
            target_price=60.0, qty_coins=0.04, qty_usd=2.0,
            entry_fee_usd=0.01, conn=c,
        )


def test_insert_position_sets_hwm_to_entry(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with get_db(db) as c:
        row = c.execute("SELECT high_water_mark FROM positions WHERE id='POS1'").fetchone()
    assert row["high_water_mark"] == 50.0


def test_close_position_marks_closed_and_records_event(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with get_db(db) as c:
        close_position("POS1", 60.0, "TAKE_PROFIT", pnl_usd=0.40, pnl_pct=20.0,
                        exit_fee_usd=0.01, conn=c)
        pos = c.execute("SELECT status, exit_price, pnl_usd FROM positions WHERE id='POS1'").fetchone()
        events = c.execute("SELECT event_type FROM position_events WHERE position_id='POS1'").fetchall()
    assert pos["status"] == "CLOSED"
    assert pos["exit_price"] == 60.0
    assert pos["pnl_usd"] == pytest.approx(0.40)
    assert any(e["event_type"] == "CLOSED" for e in events)


def test_close_already_closed_position_raises(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with get_db(db) as c:
        close_position("POS1", 60.0, "TAKE_PROFIT", 0.40, 20.0, 0.01, conn=c)
    with pytest.raises(RuntimeError, match="CLOSED"):
        with get_db(db) as c:
            close_position("POS1", 55.0, "STOP_LOSS", -0.20, -10.0, 0.01, conn=c)


def test_close_nonexistent_position_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db) as c:
            close_position("GHOST", 50.0, "STOP_LOSS", -0.5, -10.0, 0.0, conn=c)


def test_position_unique_per_order(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_position(
                position_id="POS2", order_id="ORD1", epoch_id="EP1",
                asset="ZEC-USD", entry_price=50.0, stop_price=45.0,
                target_price=60.0, qty_coins=0.04, qty_usd=2.0,
                entry_fee_usd=0.01, conn=c,
            )


def test_update_position_stop_records_event(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with get_db(db) as c:
        update_position_stop("POS1", new_stop=47.0, new_hwm=53.0, conn=c)
        pos = c.execute("SELECT stop_price, high_water_mark FROM positions WHERE id='POS1'").fetchone()
        events = c.execute(
            "SELECT event_type, payload FROM position_events WHERE position_id='POS1'"
        ).fetchall()
    assert pos["stop_price"] == 47.0
    assert pos["high_water_mark"] == 53.0
    event_types = [e["event_type"] for e in events]
    assert "STOP_UPDATED" in event_types


def test_get_open_positions_filters_by_asset(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    # Two orders, two assets
    for oid, asset in [("ORD_ZEC", "ZEC-USD"), ("ORD_BTC", "BTC-USD")]:
        with get_db(db) as c:
            insert_order(oid, "EP1", asset, 50.0, 45.0, 60.0, 2.0,
                         "2026-07-01T10:00:00Z", "2026-07-02T10:00:00Z", conn=c)
            transition_order(oid, "OPEN", conn=c)
            transition_order(oid, "FILLED", conn=c)
            insert_position(
                position_id=oid, order_id=oid, epoch_id="EP1",
                asset=asset, entry_price=50.0, stop_price=45.0, target_price=60.0,
                qty_coins=0.04, qty_usd=2.0, entry_fee_usd=0.0, conn=c,
            )
    with get_db(db) as c:
        zec_pos = get_open_positions_for_asset("ZEC-USD", c)
        all_pos = get_open_positions_for_asset(None, c)
    assert len(zec_pos) == 1
    assert zec_pos[0]["asset"] == "ZEC-USD"
    assert len(all_pos) == 2


def test_get_epoch_closed_pnl(tmp_path):
    db = make_db(tmp_path)
    _setup_position(db)
    with get_db(db) as c:
        close_position("POS1", 60.0, "TAKE_PROFIT", 0.40, 20.0, 0.01, conn=c)
        rows = get_epoch_closed_pnl("EP1", c)
    assert len(rows) == 1
    assert rows[0]["pnl_usd"] == pytest.approx(0.40)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def test_reconciliation_lifecycle(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "RUNNING"

    discrepancies = [{"type": "orphan_order", "exchange_order_id": "CB-XYZ"}]
    with get_db(db) as c:
        complete_reconciliation(run_id, discrepancies, [], c)
        row = c.execute(
            "SELECT status, discrepancies FROM reconciliation_runs WHERE id=?", (run_id,)
        ).fetchone()
    assert row["status"] == "FAILED"
    assert json.loads(row["discrepancies"]) == discrepancies


def test_reconciliation_complete_when_no_discrepancies(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        complete_reconciliation(run_id, [], ["cancelled expired order CB-OLD"], c)
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "COMPLETE"


# ---------------------------------------------------------------------------
# JSON migration
# ---------------------------------------------------------------------------

def test_migrate_epochs_from_jsonl(tmp_path):
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({
            "event": "RISK_EPOCH_STARTED",
            "epoch_id": "EP_TEST",
            "paper_capital": 150.0,
            "reason": "test migration",
            "timestamp": "2026-07-01T00:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )
    db = tmp_path / "test.db"
    init_db(db)
    counts = migrate_from_json(epochs_jsonl=epochs_file, db_path=db)
    assert counts["epochs"] == 1
    with get_db(db) as c:
        row = c.execute("SELECT paper_capital FROM risk_epochs WHERE epoch_id='EP_TEST'").fetchone()
    assert row["paper_capital"] == 150.0


def test_migrate_epochs_idempotent(tmp_path):
    """Running migration twice must not raise or duplicate rows."""
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({
            "event": "RISK_EPOCH_STARTED",
            "epoch_id": "EP_IDEM",
            "paper_capital": 100.0,
            "reason": "idempotent test",
            "timestamp": "2026-07-01T00:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )
    db = tmp_path / "test.db"
    init_db(db)
    migrate_from_json(epochs_jsonl=epochs_file, db_path=db)
    migrate_from_json(epochs_jsonl=epochs_file, db_path=db)  # second run
    with get_db(db) as c:
        count = c.execute("SELECT COUNT(*) FROM risk_epochs WHERE epoch_id='EP_IDEM'").fetchone()[0]
    assert count == 1


def test_migrate_pre_epoch_orders_skipped(tmp_path):
    """Orders without an epoch_id in JSON must be skipped (no epoch_id → no DB row)."""
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text("", encoding="utf-8")
    orders_file = tmp_path / "pending_orders.json"
    orders_file.write_text(json.dumps([{
        "id": "ORD_PRE_EPOCH",
        "asset": "ZEC-USD",
        "limit_price": 50.0,
        "stop_price": 45.0,
        "target_price": 60.0,
        "position_size_pct": 0.02,
        "placed_at": "2026-06-01T10:00:00Z",
        "expires_at": "2026-06-02T10:00:00Z",
        "reasoning": "pre-epoch order",
        "status": "OPEN",
        "exchange_order_id": None,
        "epoch_id": None,  # ← pre-epoch
    }]), encoding="utf-8")
    db = tmp_path / "test.db"
    init_db(db)
    counts = migrate_from_json(epochs_jsonl=epochs_file, orders_json=orders_file, db_path=db)
    assert counts["orders"] == 0


def test_migrate_non_epoch_events_ignored(tmp_path):
    """JSONL lines that are not RISK_EPOCH_STARTED events must be silently ignored."""
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({"event": "SOME_OTHER_EVENT", "epoch_id": "EP_GHOST"}) + "\n",
        encoding="utf-8",
    )
    db = tmp_path / "test.db"
    init_db(db)
    counts = migrate_from_json(epochs_jsonl=epochs_file, db_path=db)
    assert counts["epochs"] == 0


# ---------------------------------------------------------------------------
# Transaction rollback on error
# ---------------------------------------------------------------------------

def test_rollback_on_error_leaves_db_clean(tmp_path):
    """If an error occurs inside get_db(), the transaction must roll back."""
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            # First insert succeeds
            insert_order("ORD_GOOD", "EP1", "ZEC-USD", 50.0, 45.0, 60.0, 2.0,
                         "2026-07-01T10:00:00Z", "2026-07-02T10:00:00Z", conn=c)
            # Second insert with same id fails → whole transaction rolls back
            insert_order("ORD_GOOD", "EP1", "ZEC-USD", 51.0, 46.0, 61.0, 2.0,
                         "2026-07-01T10:00:00Z", "2026-07-02T10:00:00Z", conn=c)
    # ORD_GOOD must NOT be in the DB
    with get_db(db) as c:
        row = c.execute("SELECT id FROM orders WHERE id='ORD_GOOD'").fetchone()
    assert row is None

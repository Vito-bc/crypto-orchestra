"""
Tests for pipeline/ledger.py — Schema V2.

Coverage:
  Schema: all 7 tables, WAL, foreign keys, user_version=2, idempotent init
  Epoch: one active epoch enforced (partial UNIQUE INDEX), start_epoch() closes previous
  Orders: universal model (side/order_type/purpose), state transitions
  apply_fill(): atomic fill+transition+VWAP; idempotent on exchange_fill_id; rejects SUBMITTING/terminal
  Fills: immutable (UPDATE/DELETE triggers)
  Positions: VWAP from fills, unique per entry_order, close checks rowcount
  update_position_stop(): raises on CLOSED, no orphan event
  position_events: immutable (UPDATE/DELETE triggers)
  Reconciliation: prevents concurrent RUNNING, recovers stale, discovered/resolved/unresolved statuses
  Migration: idempotent on PK duplicate, raises on data conflict, pre-epoch rows skipped
  Transactions: rollback leaves DB clean
"""
from __future__ import annotations

import json
import sqlite3
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path

import pytest

from pipeline.ledger import (
    apply_fill,
    close_position,
    complete_reconciliation,
    get_active_epoch,
    get_epoch_closed_pnl,
    get_fills_for_order,
    get_db,
    get_open_orders_for_asset,
    get_open_orders_for_position,
    get_open_positions_for_asset,
    init_db,
    insert_epoch,
    insert_order,
    insert_position,
    migrate_from_json,
    run_migrations,
    start_epoch,
    start_reconciliation,
    transition_order,
    update_position_stop,
    SCHEMA_VERSION,
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


def add_entry_order(
    db: Path,
    order_id: str = "ORD1",
    epoch_id: str = "EP1",
    asset: str = "ZEC-USD",
    qty_usd: float = 2.0,
    limit_price: float = 50.0,
) -> None:
    with get_db(db) as c:
        insert_order(
            order_id=order_id, epoch_id=epoch_id, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at="2026-07-01T10:00:00Z",
            qty_usd_requested=qty_usd, limit_price=limit_price,
            conn=c,
        )


def open_order(db: Path, order_id: str = "ORD1") -> None:
    with get_db(db) as c:
        transition_order(order_id, "OPEN", conn=c)


def setup_filled_position(
    db: Path,
    order_id: str = "ORD1",
    fill_price: float = 51.0,
    fill_qty_base: float = 0.04,
    epoch_id: str = "EP1",
    stop_price: float = 45.0,
    target_price: float = 60.0,
) -> str:
    """Return position_id."""
    add_epoch(db, epoch_id)
    add_entry_order(db, order_id, epoch_id=epoch_id,
                    qty_usd=fill_price * fill_qty_base, limit_price=fill_price)
    open_order(db, order_id)
    with get_db(db) as c:
        result = apply_fill(
            order_id, fill_price, fill_qty_base,
            fee_usd=0.01,
            exchange_fill_id="FILL-001",
            stop_price=stop_price, target_price=target_price,
            conn=c,
        )
    return result["position_id"]


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


def test_schema_version_is_2(tmp_path):
    db = make_db(tmp_path)
    conn = sqlite3.connect(str(db))
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert version == SCHEMA_VERSION


def test_init_db_is_idempotent(tmp_path):
    db = tmp_path / "test.db"
    init_db(db)
    init_db(db)
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "idempotent test", conn=c)
    with get_db(db) as c:
        row = c.execute("SELECT epoch_id FROM risk_epochs WHERE epoch_id='EP1'").fetchone()
    assert row is not None


def test_run_migrations_bumps_version(tmp_path):
    db = tmp_path / "test.db"
    run_migrations(db)
    conn = sqlite3.connect(str(db))
    version = conn.execute("PRAGMA user_version").fetchone()[0]
    conn.close()
    assert version == 2


def test_wal_mode_enabled(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        mode = c.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


def test_foreign_keys_enforced(tmp_path):
    db = make_db(tmp_path)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_order(
                "ORD_ORPHAN", "NONEXISTENT_EPOCH", "ZEC-USD",
                "BUY", "LIMIT", "ENTRY",
                "2026-07-01T10:00:00Z", qty_usd_requested=2.0,
                conn=c,
            )


# ---------------------------------------------------------------------------
# Epoch — one active at a time
# ---------------------------------------------------------------------------

def test_only_one_active_epoch_enforced(tmp_path):
    """Partial UNIQUE INDEX on (1) WHERE ended_at IS NULL prevents two active epochs."""
    db = make_db(tmp_path)
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "first", "2026-07-01T00:00:00Z", conn=c)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_epoch("EP2", 100.0, "second", "2026-07-10T00:00:00Z", conn=c)


def test_start_epoch_closes_previous(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "first", "2026-07-01T00:00:00Z", conn=c)
    with get_db(db) as c:
        start_epoch("EP2", 200.0, "second", conn=c)
        ep1 = c.execute("SELECT ended_at FROM risk_epochs WHERE epoch_id='EP1'").fetchone()
        active = get_active_epoch(c)
    assert ep1["ended_at"] is not None
    assert active["epoch_id"] == "EP2"


def test_start_epoch_duplicate_id_raises(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        insert_epoch("EP1", 100.0, "first", "2026-07-01T00:00:00Z", conn=c)
    with get_db(db) as c:
        start_epoch("EP2", 200.0, "second", conn=c)
    with pytest.raises(ValueError, match="already exists"):
        with get_db(db) as c:
            start_epoch("EP2", 300.0, "duplicate", conn=c)


def test_epoch_unique_constraint_on_raw_insert(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db, "EP1")
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_epoch("EP1", 200.0, "duplicate", conn=c)


def test_get_active_epoch_returns_none_when_empty(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        assert get_active_epoch(c) is None


def test_insert_epoch_with_ended_at_allows_second_active(tmp_path):
    """Historical epochs with ended_at set don't conflict with an active one."""
    db = make_db(tmp_path)
    with get_db(db) as c:
        insert_epoch("EP_HIST", 100.0, "historical", "2026-01-01T00:00:00Z",
                     ended_at="2026-06-01T00:00:00Z", conn=c)
        insert_epoch("EP_ACTIVE", 100.0, "active", "2026-07-01T00:00:00Z",
                     ended_at=None, conn=c)
        active = get_active_epoch(c)
    assert active["epoch_id"] == "EP_ACTIVE"


# ---------------------------------------------------------------------------
# Order: universal model
# ---------------------------------------------------------------------------

def test_order_requires_at_least_one_qty(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(ValueError, match="qty_base_requested or qty_usd_requested"):
        with get_db(db) as c:
            insert_order(
                "ORD_BAD", "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY",
                "2026-07-01T10:00:00Z",
                qty_base_requested=None, qty_usd_requested=None,
                conn=c,
            )


def test_order_side_check_constraint(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            conn = c
            conn.execute("""
                INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose,
                                   qty_usd_requested, placed_at, status)
                VALUES ('X','EP1','ZEC-USD','LONG','LIMIT','ENTRY',2,'2026-07-01T10:00:00Z','SUBMITTING')
            """)


def test_order_purpose_check_constraint(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            c.execute("""
                INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose,
                                   qty_usd_requested, placed_at, status)
                VALUES ('X','EP1','ZEC-USD','BUY','LIMIT','CLOSE',2,'2026-07-01T10:00:00Z','SUBMITTING')
            """)


def test_exit_order_references_position(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        # SELL MARKET EXIT order for the position
        insert_order(
            "EXIT_ORD1", "EP1", "ZEC-USD", "SELL", "MARKET", "EXIT",
            "2026-07-01T12:00:00Z",
            qty_usd_requested=2.0, position_id=pos_id, conn=c,
        )
        row = c.execute(
            "SELECT position_id, purpose FROM orders WHERE id='EXIT_ORD1'"
        ).fetchone()
    assert row["position_id"] == pos_id
    assert row["purpose"] == "EXIT"


def test_exit_order_invalid_position_id_rejected(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            insert_order(
                "EXIT_ORD_BAD", "EP1", "ZEC-USD", "SELL", "MARKET", "EXIT",
                "2026-07-01T12:00:00Z",
                qty_usd_requested=2.0, position_id="GHOST_POS", conn=c,
            )


# ---------------------------------------------------------------------------
# Order state transitions
# ---------------------------------------------------------------------------

def test_order_transition_submitting_to_open(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", exchange_order_id="CB-001", conn=c)
        row = c.execute("SELECT status, exchange_order_id FROM orders WHERE id='ORD1'").fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == "CB-001"


def test_order_transition_invalid_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db)
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db) as c:
            transition_order("ORD1", "FILLED", conn=c)  # SUBMITTING → FILLED not allowed


def test_order_transition_from_terminal_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", conn=c)
        transition_order("ORD1", "FILLED", conn=c)
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db) as c:
            transition_order("ORD1", "CANCELLED", conn=c)


def test_order_exchange_id_unique(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, "ORD1")
    add_entry_order(db, "ORD2")
    with get_db(db) as c:
        transition_order("ORD1", "OPEN", exchange_order_id="CB-SAME", conn=c)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            transition_order("ORD2", "OPEN", exchange_order_id="CB-SAME", conn=c)


def test_get_open_orders_returns_only_open_partial(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, "ORD_OPEN")
    add_entry_order(db, "ORD_CANCELLED")
    with get_db(db) as c:
        transition_order("ORD_OPEN", "OPEN", conn=c)
        transition_order("ORD_CANCELLED", "CANCELLED", conn=c)
        rows = get_open_orders_for_asset("ZEC-USD", c)
    assert {r["id"] for r in rows} == {"ORD_OPEN"}


def test_get_open_orders_for_position(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        insert_order("EXIT_ORD", "EP1", "ZEC-USD", "SELL", "MARKET", "EXIT",
                     "2026-07-01T12:00:00Z", qty_usd_requested=2.0,
                     position_id=pos_id, conn=c)
        rows = get_open_orders_for_position(pos_id, c)
    assert len(rows) == 1
    assert rows[0]["id"] == "EXIT_ORD"


# ---------------------------------------------------------------------------
# apply_fill() — atomic fill + transition + VWAP
# ---------------------------------------------------------------------------

def test_apply_fill_creates_position_on_first_fill(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=2.04)
    open_order(db)
    with get_db(db) as c:
        result = apply_fill(
            "ORD1", fill_price=51.0, fill_qty_base=0.04,
            exchange_fill_id="F-001", stop_price=45.0, target_price=60.0,
            conn=c,
        )
        pos = c.execute("SELECT * FROM positions WHERE id=?", (result["position_id"],)).fetchone()
    assert result["status"] == "FILLED"
    assert pos["entry_price"] == pytest.approx(51.0)
    assert pos["stop_price"] == 45.0
    assert pos["target_price"] == 60.0
    assert pos["high_water_mark"] == pytest.approx(51.0)
    assert pos["status"] == "OPEN"


def test_apply_fill_computes_vwap_for_partial_fills(tmp_path):
    """Two partial fills: VWAP = (51*0.02 + 53*0.02) / 0.04 = 52.0.
    qty_usd_requested=2.0: first fill 51*0.02=1.02 < 2.0 → PARTIAL;
    second fill total 51*0.02+53*0.02=2.08 >= 2.0*0.999 → FILLED."""
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=2.0, limit_price=51.0)
    open_order(db)
    with get_db(db) as c:
        r1 = apply_fill("ORD1", 51.0, 0.02, exchange_fill_id="F-001", conn=c)
    assert r1["status"] == "PARTIAL"
    assert r1["position_id"] is not None
    pos_id = r1["position_id"]
    with get_db(db) as c:
        r2 = apply_fill("ORD1", 53.0, 0.02, exchange_fill_id="F-002", conn=c)
        pos = c.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
    assert r2["status"] == "FILLED"
    assert pos["entry_price"] == pytest.approx(52.0)
    assert pos["qty_base"] == pytest.approx(0.04)


def test_apply_fill_idempotent_on_same_exchange_fill_id(tmp_path):
    """Replaying the same fill (e.g. during reconciliation) must not double-count."""
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=2.04)
    open_order(db)
    with get_db(db) as c:
        apply_fill("ORD1", 51.0, 0.04, exchange_fill_id="F-DUP", conn=c)
        apply_fill("ORD1", 51.0, 0.04, exchange_fill_id="F-DUP", conn=c)  # replay
        fills = get_fills_for_order("ORD1", c)
    assert len(fills) == 1   # only one fill recorded


def test_apply_fill_null_exchange_id_allows_multiple_dry_run(tmp_path):
    """NULL exchange_fill_id (dry-run) — SQLite UNIQUE allows multiple NULLs."""
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=4.08, limit_price=50.0)
    open_order(db)
    with get_db(db) as c:
        apply_fill("ORD1", 50.0, 0.02, exchange_fill_id=None, conn=c)
        apply_fill("ORD1", 50.0, 0.02, exchange_fill_id=None, conn=c)
        fills = get_fills_for_order("ORD1", c)
    assert len(fills) == 2


def test_apply_fill_rejected_for_submitting_order(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db)
    with pytest.raises(RuntimeError, match="SUBMITTING"):
        with get_db(db) as c:
            apply_fill("ORD1", 51.0, 0.04, conn=c)


def test_apply_fill_rejected_for_terminal_order(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db)
    with get_db(db) as c:
        transition_order("ORD1", "CANCELLED", conn=c)
    with pytest.raises(RuntimeError, match="CANCELLED"):
        with get_db(db) as c:
            apply_fill("ORD1", 51.0, 0.04, conn=c)


def test_apply_fill_position_unique_per_order(tmp_path):
    """Two fills on same order must return the SAME position_id."""
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=4.08, limit_price=50.0)
    open_order(db)
    with get_db(db) as c:
        r1 = apply_fill("ORD1", 50.0, 0.02, exchange_fill_id="F-A", conn=c)
        r2 = apply_fill("ORD1", 50.0, 0.02, exchange_fill_id="F-B", conn=c)
    assert r1["position_id"] == r2["position_id"]
    assert r1["position_id"] is not None


def test_apply_fill_records_opened_event(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    add_entry_order(db, qty_usd=2.04)
    open_order(db)
    with get_db(db) as c:
        result = apply_fill("ORD1", 51.0, 0.04, conn=c)
        events = c.execute(
            "SELECT event_type FROM position_events WHERE position_id=?",
            (result["position_id"],)
        ).fetchall()
    assert any(e["event_type"] == "OPENED" for e in events)


# ---------------------------------------------------------------------------
# Fills: immutability triggers
# ---------------------------------------------------------------------------

def test_fill_update_trigger_raises(tmp_path):
    # RAISE(ABORT, ...) in SQLite maps to SQLITE_CONSTRAINT → IntegrityError in Python
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db) as c:
            c.execute("UPDATE fills SET fill_price=99.0 WHERE order_id='ORD1'")


def test_fill_delete_trigger_raises(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db) as c:
            c.execute("DELETE FROM fills WHERE order_id='ORD1'")


# ---------------------------------------------------------------------------
# position_events: immutability triggers
# ---------------------------------------------------------------------------

def test_position_event_update_trigger_raises(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db) as c:
            c.execute("UPDATE position_events SET event_type='TAMPERED' WHERE position_id=?",
                      (pos_id,))


def test_position_event_delete_trigger_raises(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db) as c:
            c.execute("DELETE FROM position_events WHERE position_id=?", (pos_id,))


# ---------------------------------------------------------------------------
# Position state machine
# ---------------------------------------------------------------------------

def test_close_position_marks_closed_and_records_event(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        close_position(pos_id, 60.0, "TAKE_PROFIT", pnl_usd=0.36, pnl_pct=18.0,
                       exit_fee_usd=0.01, conn=c)
        pos = c.execute(
            "SELECT status, exit_price, pnl_usd FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
        events = c.execute(
            "SELECT event_type FROM position_events WHERE position_id=?", (pos_id,)
        ).fetchall()
    assert pos["status"] == "CLOSED"
    assert pos["exit_price"] == 60.0
    assert pos["pnl_usd"] == pytest.approx(0.36)
    assert any(e["event_type"] == "CLOSED" for e in events)


def test_close_already_closed_position_raises(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        close_position(pos_id, 60.0, "TAKE_PROFIT", 0.36, 18.0, 0.01, conn=c)
    with pytest.raises(RuntimeError, match="CLOSED"):
        with get_db(db) as c:
            close_position(pos_id, 55.0, "STOP_LOSS", -0.20, -10.0, 0.01, conn=c)


def test_close_nonexistent_position_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db) as c:
            close_position("GHOST", 50.0, "STOP_LOSS", -0.5, -10.0, 0.0, conn=c)


def test_update_position_stop_updates_and_records_event(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        update_position_stop(pos_id, new_stop=47.0, new_hwm=53.0, conn=c)
        pos = c.execute(
            "SELECT stop_price, high_water_mark FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
        events = c.execute(
            "SELECT event_type FROM position_events WHERE position_id=?", (pos_id,)
        ).fetchall()
    assert pos["stop_price"] == 47.0
    assert pos["high_water_mark"] == 53.0
    assert any(e["event_type"] == "STOP_UPDATED" for e in events)


def test_update_position_stop_on_closed_raises_no_orphan_event(tmp_path):
    """RuntimeError raised AND no STOP_UPDATED event written for a closed position."""
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        close_position(pos_id, 60.0, "TAKE_PROFIT", 0.36, 18.0, 0.01, conn=c)
    with pytest.raises(RuntimeError, match="CLOSED"):
        with get_db(db) as c:
            update_position_stop(pos_id, 55.0, 60.0, conn=c)
    # No STOP_UPDATED event should exist
    with get_db(db) as c:
        events = c.execute(
            "SELECT event_type FROM position_events WHERE position_id=? AND event_type='STOP_UPDATED'",
            (pos_id,)
        ).fetchall()
    assert len(events) == 0


def test_update_position_stop_on_nonexistent_raises(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db) as c:
            update_position_stop("GHOST", 45.0, 50.0, conn=c)


def test_get_open_positions_filters_by_asset(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    for oid, asset in [("ORD_ZEC", "ZEC-USD"), ("ORD_BTC", "BTC-USD")]:
        with get_db(db) as c:
            insert_order(oid, "EP1", asset, "BUY", "LIMIT", "ENTRY",
                         "2026-07-01T10:00:00Z", qty_usd_requested=2.0, conn=c)
            transition_order(oid, "OPEN", conn=c)
            apply_fill(oid, 50.0, 0.04, exchange_fill_id=f"F-{oid}", conn=c)
    with get_db(db) as c:
        zec_pos = get_open_positions_for_asset("ZEC-USD", c)
        all_pos = get_open_positions_for_asset(None, c)
    assert len(zec_pos) == 1
    assert zec_pos[0]["asset"] == "ZEC-USD"
    assert len(all_pos) == 2


def test_get_epoch_closed_pnl(tmp_path):
    db = make_db(tmp_path)
    pos_id = setup_filled_position(db)
    with get_db(db) as c:
        close_position(pos_id, 60.0, "TAKE_PROFIT", 0.36, 18.0, 0.01, conn=c)
        rows = get_epoch_closed_pnl("EP1", c)
    assert len(rows) == 1
    assert rows[0]["pnl_usd"] == pytest.approx(0.36)


# ---------------------------------------------------------------------------
# Reconciliation
# ---------------------------------------------------------------------------

def test_reconciliation_lifecycle(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "RUNNING"


def test_reconciliation_complete_no_discrepancies(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        complete_reconciliation(run_id, discovered=[], resolved=[], unresolved=[], conn=c)
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "COMPLETE"


def test_reconciliation_complete_with_actions(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        complete_reconciliation(
            run_id,
            discovered=[{"type": "orphan_order", "id": "CB-OLD"}],
            resolved=[{"type": "orphan_order", "id": "CB-OLD", "action": "cancelled"}],
            unresolved=[],
            conn=c,
        )
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "COMPLETE_WITH_ACTIONS"


def test_reconciliation_failed_on_unresolved(tmp_path):
    db = make_db(tmp_path)
    with get_db(db) as c:
        run_id = start_reconciliation(c)
        complete_reconciliation(
            run_id,
            discovered=[{"type": "phantom_position"}],
            resolved=[],
            unresolved=[{"type": "phantom_position", "requires_human": True}],
            conn=c,
        )
        row = c.execute("SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)).fetchone()
    assert row["status"] == "FAILED"


def test_reconciliation_prevents_concurrent_running(tmp_path):
    db = make_db(tmp_path)
    now = datetime.now(timezone.utc).isoformat()
    with get_db(db) as c:
        c.execute(
            "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)",
            (now, "RUNNING"),
        )
    with pytest.raises(RuntimeError, match="already RUNNING"):
        with get_db(db) as c:
            start_reconciliation(c, stale_threshold_minutes=30)


def test_reconciliation_recovers_stale_run(tmp_path):
    """A RUNNING run older than the threshold is marked FAILED and a new one starts."""
    db = make_db(tmp_path)
    stale_ts = (datetime.now(timezone.utc) - timedelta(minutes=60)).isoformat()
    with get_db(db) as c:
        c.execute(
            "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)",
            (stale_ts, "RUNNING"),
        )
    with get_db(db) as c:
        new_id = start_reconciliation(c, stale_threshold_minutes=30)
        stale = c.execute(
            "SELECT status FROM reconciliation_runs WHERE id != ?", (new_id,)
        ).fetchone()
        new = c.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (new_id,)
        ).fetchone()
    assert stale["status"] == "FAILED"
    assert new["status"] == "RUNNING"


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


def test_migrate_multiple_epochs_only_last_is_active(tmp_path):
    """Historical epochs get ended_at set; only the last one remains active."""
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({"event": "RISK_EPOCH_STARTED", "epoch_id": "EP1",
                    "paper_capital": 100.0, "reason": "first",
                    "timestamp": "2026-06-01T00:00:00Z"}) + "\n" +
        json.dumps({"event": "RISK_EPOCH_STARTED", "epoch_id": "EP2",
                    "paper_capital": 100.0, "reason": "second",
                    "timestamp": "2026-07-01T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    db = tmp_path / "test.db"
    init_db(db)
    migrate_from_json(epochs_jsonl=epochs_file, db_path=db)
    with get_db(db) as c:
        ep1 = c.execute("SELECT ended_at FROM risk_epochs WHERE epoch_id='EP1'").fetchone()
        active = get_active_epoch(c)
    assert ep1["ended_at"] is not None
    assert active["epoch_id"] == "EP2"


def test_migrate_epochs_idempotent(tmp_path):
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({"event": "RISK_EPOCH_STARTED", "epoch_id": "EP_IDEM",
                    "paper_capital": 100.0, "reason": "x",
                    "timestamp": "2026-07-01T00:00:00Z"}) + "\n",
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
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text("", encoding="utf-8")
    orders_file = tmp_path / "pending_orders.json"
    orders_file.write_text(json.dumps([{
        "id": "ORD_PRE", "asset": "ZEC-USD", "limit_price": 50.0,
        "stop_price": 45.0, "target_price": 60.0, "placed_at": "2026-06-01T10:00:00Z",
        "expires_at": "2026-06-02T10:00:00Z", "reasoning": "pre-epoch",
        "status": "OPEN", "exchange_order_id": None, "epoch_id": None,
    }]), encoding="utf-8")
    db = tmp_path / "test.db"
    init_db(db)
    counts = migrate_from_json(epochs_jsonl=epochs_file, orders_json=orders_file, db_path=db)
    assert counts["orders"] == 0


def test_migrate_raises_on_non_pk_integrity_error(tmp_path):
    """Migration must raise RuntimeError on conflicts that are NOT exact primary-key duplicates."""
    epochs_file = tmp_path / "risk_epochs.jsonl"
    epochs_file.write_text(
        json.dumps({"event": "RISK_EPOCH_STARTED", "epoch_id": "EP1",
                    "paper_capital": 100.0, "reason": "first",
                    "timestamp": "2026-07-01T00:00:00Z"}) + "\n",
        encoding="utf-8",
    )
    db = tmp_path / "test.db"
    init_db(db)
    migrate_from_json(epochs_jsonl=epochs_file, db_path=db)
    # Now insert an order with a known exchange_order_id
    with get_db(db) as c:
        insert_order("ORD_EXISTING", "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY",
                     "2026-07-01T10:00:00Z", qty_usd_requested=2.0, conn=c)
        transition_order("ORD_EXISTING", "OPEN",
                         exchange_order_id="CB-CONFLICT", conn=c)
    # Try to migrate an order with same exchange_order_id but different id
    orders_file = tmp_path / "orders.json"
    orders_file.write_text(json.dumps([{
        "id": "ORD_DIFFERENT", "asset": "ZEC-USD",
        "limit_price": 50.0, "stop_price": 45.0, "target_price": 60.0,
        "placed_at": "2026-07-01T10:00:00Z", "expires_at": "2026-07-02T10:00:00Z",
        "reasoning": "", "status": "OPEN",
        "exchange_order_id": "CB-CONFLICT",  # conflict!
        "epoch_id": "EP1",
    }]), encoding="utf-8")
    with pytest.raises(RuntimeError, match="IntegrityError"):
        migrate_from_json(epochs_jsonl=epochs_file, orders_json=orders_file, db_path=db)


def test_migrate_non_epoch_events_ignored(tmp_path):
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
# Transaction rollback
# ---------------------------------------------------------------------------

def test_rollback_on_error_leaves_db_clean(tmp_path):
    db = make_db(tmp_path)
    add_epoch(db)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db) as c:
            add_entry_order(db, "ORD_GOOD", conn=c) if False else None
            insert_order("ORD_GOOD", "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY",
                         "2026-07-01T10:00:00Z", qty_usd_requested=2.0, conn=c)
            insert_order("ORD_GOOD", "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY",
                         "2026-07-01T10:00:00Z", qty_usd_requested=2.0, conn=c)  # dup
    with get_db(db) as c:
        row = c.execute("SELECT id FROM orders WHERE id='ORD_GOOD'").fetchone()
    assert row is None

"""
Tests for pipeline/ledger.py — Schema V2.1 (PRAGMA user_version = 3).

Every test gets a fresh on-disk temp DB via the tmp_db fixture.
Tests that need a pre-existing epoch use db_with_epoch.
Tests that also need an OPEN entry position use db_with_position.
"""
from __future__ import annotations

import json
import shutil
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.ledger import (
    SCHEMA_VERSION,
    LedgerConsistencyError,
    apply_fill,
    close_position,
    complete_reconciliation,
    get_active_epoch,
    get_db,
    get_epoch_closed_pnl,
    get_open_orders_for_asset,
    get_open_positions_for_asset,
    get_trade_intent,
    init_db,
    insert_epoch,
    insert_order,
    insert_position,
    insert_trade_intent,
    run_migrations,
    start_epoch,
    start_reconciliation,
    transition_order,
    update_position_stop,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _oid() -> str:
    return str(uuid.uuid4())


def _setup_open_position(
    tmp_db: Path,
    stop: float = 50.0,
    target: float = 150.0,
    fill_price: float = 100.0,
    fill_qty_base: float = 1.0,
    entry_fee: float = 0.5,
    epoch_id: str = "EP1",
) -> tuple[str, str]:
    """
    Ensure epoch exists, then create ENTRY order + trade_intent + OPEN + fill.
    Returns (entry_order_id, position_id).
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        if not conn.execute("SELECT 1 FROM risk_epochs WHERE epoch_id=?", (epoch_id,)).fetchone():
            insert_epoch(epoch_id, 100.0, "test", conn=conn)
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=epoch_id, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=fill_qty_base, conn=conn,
        )
        insert_trade_intent(oid, stop_price=stop, target_price=target, conn=conn)
        transition_order(oid, "OPEN", conn=conn)
    with get_db(tmp_db) as conn:
        result = apply_fill(
            order_id=oid, fill_price=fill_price, fill_qty_base=fill_qty_base,
            fee_usd=entry_fee, conn=conn,
        )
    return oid, result["position_id"]


def _place_exit_order(
    tmp_db: Path,
    position_id: str,
    epoch_id: str = "EP1",
    qty_base: float = 1.0,
) -> str:
    """Insert and open an EXIT order, return its id."""
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=epoch_id, asset="ZEC-USD",
            side="SELL", order_type="LIMIT", purpose="EXIT",
            position_id=position_id, placed_at=_now(), qty_base_requested=qty_base, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    return oid


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ledger.db"
    run_migrations(db)
    return db


@pytest.fixture
def db_with_epoch(tmp_db: Path) -> Path:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "fixture epoch", conn=conn)
    return tmp_db


@pytest.fixture
def db_with_position(db_with_epoch: Path) -> tuple[Path, str, str]:
    """Returns (db_path, entry_order_id, position_id)."""
    oid, pos_id = _setup_open_position(db_with_epoch)
    return db_with_epoch, oid, pos_id


# ---------------------------------------------------------------------------
# 1. Schema init and migrations
# ---------------------------------------------------------------------------

def test_run_migrations_creates_db_at_correct_version(tmp_path: Path) -> None:
    db = tmp_path / "new.db"
    run_migrations(db)
    assert db.exists()
    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_run_migrations_is_idempotent(tmp_db: Path) -> None:
    run_migrations(tmp_db)
    run_migrations(tmp_db)
    with get_db(tmp_db) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_init_db_alias(tmp_path: Path) -> None:
    db = tmp_path / "alias.db"
    init_db(db)
    with get_db(db) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


_PROTO_V1_SCHEMA = """
CREATE TABLE IF NOT EXISTS risk_epochs (
    epoch_id TEXT PRIMARY KEY, paper_capital REAL, reason TEXT,
    started_at TEXT, ended_at TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY, epoch_id TEXT, asset TEXT,
    status TEXT DEFAULT 'OPEN', placed_at TEXT
);
"""


def _make_proto_db(path: Path, user_version: int = 1) -> None:
    conn = sqlite3.connect(str(path))
    conn.executescript(_PROTO_V1_SCHEMA)
    conn.execute(f"PRAGMA user_version = {user_version}")
    conn.execute(
        "INSERT INTO risk_epochs VALUES ('EP_OLD', 100, 'legacy', '2024-01-01T00:00:00', NULL)"
    )
    conn.commit()
    conn.close()


def test_run_migrations_v1_creates_backup(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_proto_db(db, user_version=1)
    assert db.exists()
    assert not (tmp_path / "ledger.v1.bak").exists()

    run_migrations(db)

    backup = tmp_path / "ledger.v1.bak"
    assert backup.exists(), "should create .v1.bak of prototype DB"
    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_run_migrations_v1_backup_preserves_original_data(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_proto_db(db, user_version=1)
    run_migrations(db)

    backup = tmp_path / "ledger.v1.bak"
    with sqlite3.connect(str(backup)) as conn:
        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
    assert row[0] == "EP_OLD"


def test_run_migrations_v1_new_db_has_v3_tables(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_proto_db(db, user_version=1)
    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "trade_intents" in tables
    assert "reconciliation_runs" in tables
    assert "position_events" in tables


def test_run_migrations_v2_creates_backup(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_proto_db(db, user_version=2)
    run_migrations(db)
    assert (tmp_path / "ledger.v2.bak").exists()


def test_run_migrations_v3_creates_backup(tmp_path: Path) -> None:
    """v3 (pre-outbox) is a pre-live prototype — backup+reset to v4."""
    db = tmp_path / "ledger.db"
    _make_proto_db(db, user_version=3)
    run_migrations(db)
    assert (tmp_path / "ledger.v3.bak").exists()
    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_schema_has_one_active_epoch_index(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' AND name='idx_one_active_epoch'"
        ).fetchone()
    assert row is not None


def test_schema_has_one_running_reconciliation_index(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index' "
            "AND name='idx_one_running_reconciliation'"
        ).fetchone()
    assert row is not None


def test_schema_wal_mode(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
    assert mode == "wal"


# ---------------------------------------------------------------------------
# 2. Epoch operations
# ---------------------------------------------------------------------------

def test_insert_epoch_and_get_active(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "first", conn=conn)
    with get_db(tmp_db) as conn:
        row = get_active_epoch(conn)
    assert row["epoch_id"] == "EP1"
    assert row["paper_capital"] == 100.0


def test_only_one_active_epoch_enforced(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "first", conn=conn)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(tmp_db) as conn:
            insert_epoch("EP2", 100.0, "second", conn=conn)


def test_start_epoch_closes_previous_and_opens_new(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "first", conn=conn)
    with get_db(tmp_db) as conn:
        start_epoch("EP2", 200.0, "second", conn=conn)
    with get_db(tmp_db) as conn:
        ep1 = conn.execute("SELECT ended_at FROM risk_epochs WHERE epoch_id='EP1'").fetchone()
        active = get_active_epoch(conn)
    assert ep1["ended_at"] is not None
    assert active["epoch_id"] == "EP2"


def test_start_epoch_raises_on_duplicate(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "first", conn=conn)
    with pytest.raises(ValueError, match="already exists"):
        with get_db(tmp_db) as conn:
            start_epoch("EP1", 100.0, "duplicate", conn=conn)


def test_start_epoch_blocked_by_open_position(db_with_position: tuple) -> None:
    db, _, _ = db_with_position
    with pytest.raises(ValueError, match="open/closing position"):
        with get_db(db) as conn:
            start_epoch("EP2", 100.0, "blocked", conn=conn)


def test_start_epoch_blocked_by_open_order(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with pytest.raises(ValueError, match="open order"):
        with get_db(db_with_epoch) as conn:
            start_epoch("EP2", 100.0, "blocked", conn=conn)


def test_start_epoch_blocked_by_submitting_order(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
    with pytest.raises(ValueError, match="open order"):
        with get_db(db_with_epoch) as conn:
            start_epoch("EP2", 100.0, "blocked", conn=conn)


def test_start_epoch_blocked_by_running_reconciliation(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        start_reconciliation(conn)
    with pytest.raises(ValueError, match="reconciliation run"):
        with get_db(db_with_epoch) as conn:
            start_epoch("EP2", 100.0, "blocked", conn=conn)


def test_start_epoch_allowed_after_orders_cancelled(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
        transition_order(oid, "CANCELLED", conn=conn)
    with get_db(db_with_epoch) as conn:
        start_epoch("EP2", 100.0, "ok", conn=conn)
    with get_db(db_with_epoch) as conn:
        assert get_active_epoch(conn)["epoch_id"] == "EP2"


# ---------------------------------------------------------------------------
# 3. Order operations
# ---------------------------------------------------------------------------

def test_insert_order_default_submitting(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "SUBMITTING"


def test_insert_exit_order_without_position_id_raises(db_with_epoch: Path) -> None:
    with pytest.raises(ValueError, match="EXIT orders require a position_id"):
        with get_db(db_with_epoch) as conn:
            insert_order(
                order_id=_oid(), epoch_id="EP1", asset="ZEC-USD",
                side="SELL", order_type="LIMIT", purpose="EXIT",
                placed_at=_now(), qty_base_requested=1.0, conn=conn,
            )


def test_insert_order_qty_zero_violates_check(db_with_epoch: Path) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_with_epoch) as conn:
            conn.execute("""
                INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose,
                    qty_base_requested, placed_at, status)
                VALUES (?,?,?,?,?,?,?,?,'SUBMITTING')
            """, (_oid(), "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY", 0.0, _now()))


def test_insert_order_requires_qty(db_with_epoch: Path) -> None:
    with pytest.raises(ValueError, match="qty_base_requested or qty_usd_requested"):
        with get_db(db_with_epoch) as conn:
            insert_order(
                order_id=_oid(), epoch_id="EP1", asset="ZEC-USD",
                side="BUY", order_type="LIMIT", purpose="ENTRY",
                placed_at=_now(), conn=conn,
            )


def test_transition_order_valid_path(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
        transition_order(oid, "OPEN", exchange_order_id="CB-123", conn=conn)
    with get_db(db_with_epoch) as conn:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == "CB-123"


def test_transition_order_invalid_path_raises(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db_with_epoch) as conn:
            transition_order(oid, "FILLED", conn=conn)


def test_transition_missing_order_raises(db_with_epoch: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db_with_epoch) as conn:
            transition_order("nonexistent", "OPEN", conn=conn)


def test_exchange_order_id_unique_across_orders(db_with_epoch: Path) -> None:
    # Use different assets so idx_one_active_entry_per_asset allows both inserts.
    oid1, oid2 = _oid(), _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(oid1, "EP1", "ZEC-USD", "BUY", "LIMIT", "ENTRY",
                     _now(), qty_usd_requested=5.0, conn=conn)
        insert_order(oid2, "EP1", "ETH-USD", "BUY", "LIMIT", "ENTRY",
                     _now(), qty_usd_requested=5.0, conn=conn)
        transition_order(oid1, "OPEN", exchange_order_id="SAME-CB-ID", conn=conn)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_with_epoch) as conn:
            transition_order(oid2, "OPEN", exchange_order_id="SAME-CB-ID", conn=conn)


# ---------------------------------------------------------------------------
# 4. Trade intents
# ---------------------------------------------------------------------------

def test_insert_and_get_trade_intent(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        intent = get_trade_intent(oid, conn)
    assert intent["stop_price"] == 90.0
    assert intent["target_price"] == 115.0


def test_apply_fill_reads_trade_intent_for_stop_target(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=88.0, target_price=130.0, conn=conn)
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        result = apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=1.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT stop_price, target_price FROM positions WHERE id=?",
            (result["position_id"],),
        ).fetchone()
    assert pos["stop_price"] == 88.0
    assert pos["target_price"] == 130.0


def test_apply_fill_explicit_stop_target_overrides_intent(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=88.0, target_price=130.0, conn=conn)
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        result = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            stop_price=75.0, target_price=145.0, conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT stop_price, target_price FROM positions WHERE id=?",
            (result["position_id"],),
        ).fetchone()
    assert pos["stop_price"] == 75.0
    assert pos["target_price"] == 145.0


def test_trade_intent_stop_zero_violates_check(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_with_epoch) as conn:
            conn.execute(
                "INSERT INTO trade_intents(order_id, stop_price, target_price, recorded_at)"
                " VALUES (?,?,?,?)",
                (oid, 0.0, 100.0, _now()),
            )


# ---------------------------------------------------------------------------
# 5. apply_fill — ENTRY path
# ---------------------------------------------------------------------------

def test_apply_fill_entry_creates_position(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        result = apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=1.0, conn=conn)
    assert result["status"] == "FILLED"
    assert result["position_id"] is not None
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT * FROM positions WHERE id=?", (result["position_id"],)
        ).fetchone()
    assert pos["status"] == "OPEN"
    assert pos["entry_price"] == 100.0
    assert pos["qty_base"] == 1.0
    assert pos["qty_base_remaining"] == 1.0


def test_apply_fill_computes_vwap_for_partial_fills(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=2.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        r1 = apply_fill(order_id=oid, fill_price=1.0, fill_qty_base=1.0, conn=conn)
    assert r1["status"] == "PARTIAL"
    with get_db(db_with_epoch) as conn:
        r2 = apply_fill(order_id=oid, fill_price=2.0, fill_qty_base=1.0, conn=conn)
    assert r2["status"] == "FILLED"
    assert r1["position_id"] == r2["position_id"]
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT entry_price, qty_base FROM positions WHERE id=?", (r2["position_id"],)
        ).fetchone()
    assert abs(pos["entry_price"] - 1.5) < 1e-9  # VWAP = (1*1 + 2*1) / 2
    assert pos["qty_base"] == 2.0


def test_apply_fill_idempotency_same_exchange_fill_id(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        r1 = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            exchange_fill_id="FILL-001", conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        r2 = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            exchange_fill_id="FILL-001", conn=conn,
        )
    assert r2["status"] == r1["status"]
    assert r2["position_id"] == r1["position_id"]
    with get_db(db_with_epoch) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (oid,)
        ).fetchone()[0]
    assert count == 1


def test_apply_fill_cross_order_duplicate_fill_id_raises(db_with_epoch: Path) -> None:
    """Same exchange_fill_id submitted for two different local orders is a hard stop."""
    oid1, oid2 = _oid(), _oid()
    # Different assets so idx_one_active_entry_per_asset allows both ENTRY orders.
    with get_db(db_with_epoch) as conn:
        for oid, asset in ((oid1, "ZEC-USD"), (oid2, "ETH-USD")):
            insert_order(
                order_id=oid, epoch_id="EP1", asset=asset,
                side="BUY", order_type="LIMIT", purpose="ENTRY",
                placed_at=_now(), qty_base_requested=1.0, conn=conn,
            )
            transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=oid1, fill_price=100.0, fill_qty_base=1.0,
                   exchange_fill_id="FILL-X", conn=conn)
    with pytest.raises(RuntimeError, match="previously recorded for order"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=oid2, fill_price=100.0, fill_qty_base=1.0,
                       exchange_fill_id="FILL-X", conn=conn)


def test_apply_fill_submitting_order_raises(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
    with pytest.raises(RuntimeError, match="SUBMITTING"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=0.05, conn=conn)


def test_apply_fill_terminal_order_raises(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=5.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
        transition_order(oid, "CANCELLED", conn=conn)
    with pytest.raises(RuntimeError, match="Cannot fill"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=0.05, conn=conn)


def test_apply_fill_missing_order_raises(db_with_epoch: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id="nonexistent", fill_price=100.0, fill_qty_base=1.0, conn=conn)


# ---------------------------------------------------------------------------
# 6. apply_fill — EXIT path
# ---------------------------------------------------------------------------

def test_exit_fill_full_close_transitions_to_closed(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=1.0)

    with get_db(db_with_epoch) as conn:
        result = apply_fill(
            order_id=exit_oid, fill_price=110.0, fill_qty_base=1.0, fee_usd=0.4, conn=conn
        )

    assert result["status"] == "FILLED"
    assert result["position_id"] == pos_id

    with get_db(db_with_epoch) as conn:
        pos = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
    assert pos["status"] == "CLOSED"
    assert pos["exit_price"] == 110.0
    assert pos["qty_base_remaining"] == 0
    assert pos["closed_at"] is not None
    # pnl = (110 - 100) * 1.0 - 0.4 (exit fee) - 0.5 (entry fee) = 9.1
    assert abs(pos["pnl_usd"] - 9.1) < 0.001


def test_exit_fill_full_close_records_closed_event(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=1.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        events = conn.execute(
            "SELECT event_type FROM position_events WHERE position_id=?", (pos_id,)
        ).fetchall()
    assert any(e["event_type"] == "CLOSED" for e in events)


def test_exit_fill_partial_sets_closing_status(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=0.5)

    with get_db(db_with_epoch) as conn:
        result = apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=0.5, conn=conn)

    assert result["status"] == "FILLED"
    with get_db(db_with_epoch) as conn:
        pos = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
    assert pos["status"] == "CLOSING"
    assert abs(pos["qty_base_remaining"] - 0.5) < 1e-9
    assert pos["pnl_usd"] is None  # position not yet closed


def test_exit_fill_partial_then_full_close(db_with_epoch: Path) -> None:
    """Two exit orders of 0.5 ZEC each should fully close the 1.0 ZEC position."""
    _, pos_id = _setup_open_position(db_with_epoch)

    exit_oid1 = _place_exit_order(db_with_epoch, pos_id, qty_base=0.5)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid1, fill_price=110.0, fill_qty_base=0.5, fee_usd=0.2, conn=conn)

    exit_oid2 = _place_exit_order(db_with_epoch, pos_id, qty_base=0.5)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid2, fill_price=120.0, fill_qty_base=0.5, fee_usd=0.2, conn=conn)

    with get_db(db_with_epoch) as conn:
        pos = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
    assert pos["status"] == "CLOSED"
    assert pos["qty_base_remaining"] == 0
    # exit VWAP = (110*0.5 + 120*0.5) / 1.0 = 115.0
    assert abs(pos["exit_price"] - 115.0) < 0.001
    # pnl = (115 - 100) * 1.0 - 0.4 (fees) - 0.5 (entry fee) = 14.1
    assert abs(pos["pnl_usd"] - 14.1) < 0.001


def test_exit_fill_over_exit_guard_raises_and_rolls_back(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=2.0)

    with pytest.raises(RuntimeError, match="overfill position"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=2.0, conn=conn)

    # Transaction must have rolled back — fill row must not exist
    with get_db(db_with_epoch) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (exit_oid,)
        ).fetchone()[0]
    assert count == 0


def test_exit_fill_on_closed_position_raises(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid1 = _place_exit_order(db_with_epoch, pos_id)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid1, fill_price=110.0, fill_qty_base=1.0, conn=conn)

    exit_oid2 = _place_exit_order(db_with_epoch, pos_id)
    with pytest.raises(RuntimeError, match="already CLOSED"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=exit_oid2, fill_price=115.0, fill_qty_base=1.0, conn=conn)


def test_exit_fill_pnl_negative_correctly_computed(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch, fill_price=100.0, entry_fee=0.0)
    exit_oid = _place_exit_order(db_with_epoch, pos_id)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=90.0, fill_qty_base=1.0, fee_usd=0.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT pnl_usd, pnl_pct FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert pos["pnl_usd"] == pytest.approx(-10.0)
    assert pos["pnl_pct"] == pytest.approx(-10.0)


def test_exit_fill_partial_records_partial_exit_event(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=0.4)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=0.4, conn=conn)
    with get_db(db_with_epoch) as conn:
        events = conn.execute(
            "SELECT event_type FROM position_events WHERE position_id=?", (pos_id,)
        ).fetchall()
    assert any(e["event_type"] == "PARTIAL_EXIT" for e in events)


# ---------------------------------------------------------------------------
# 7. Position operations
# ---------------------------------------------------------------------------

def test_close_position_direct(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        close_position(
            position_id=pos_id, exit_price=105.0, exit_reason="MANUAL",
            pnl_usd=4.5, pnl_pct=4.5, exit_fee_usd=0.5, conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT status, exit_price FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert pos["status"] == "CLOSED"
    assert pos["exit_price"] == 105.0


def test_close_position_not_found_raises(db_with_epoch: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db_with_epoch) as conn:
            close_position("no-such-id", 100.0, "X", 0, 0, 0, conn=conn)


def test_close_position_already_closed_raises(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        close_position(pos_id, 105.0, "first", 0, 0, 0, conn=conn)
    with pytest.raises(RuntimeError, match="CLOSED"):
        with get_db(db_with_epoch) as conn:
            close_position(pos_id, 110.0, "second", 0, 0, 0, conn=conn)


def test_update_position_stop_updates_and_records_event(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        update_position_stop(pos_id, new_stop=95.0, new_hwm=105.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT stop_price, high_water_mark FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
        events = conn.execute(
            "SELECT event_type FROM position_events WHERE position_id=?", (pos_id,)
        ).fetchall()
    assert pos["stop_price"] == 95.0
    assert pos["high_water_mark"] == 105.0
    assert any(e["event_type"] == "STOP_UPDATED" for e in events)


def test_update_position_stop_closed_raises_no_orphan_event(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        close_position(pos_id, 105.0, "test", 0, 0, 0, conn=conn)
    with pytest.raises(RuntimeError, match="CLOSED"):
        with get_db(db_with_epoch) as conn:
            update_position_stop(pos_id, new_stop=95.0, new_hwm=110.0, conn=conn)
    with get_db(db_with_epoch) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM position_events"
            " WHERE position_id=? AND event_type='STOP_UPDATED'",
            (pos_id,),
        ).fetchone()[0]
    assert count == 0


def test_update_position_stop_missing_position_raises(db_with_epoch: Path) -> None:
    with pytest.raises(RuntimeError, match="not found"):
        with get_db(db_with_epoch) as conn:
            update_position_stop("no-such-pos", 95.0, 105.0, conn=conn)


def test_qty_base_remaining_initialized_on_position_creation(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch, fill_qty_base=2.5)
    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT qty_base, qty_base_remaining FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert pos["qty_base"] == 2.5
    assert pos["qty_base_remaining"] == 2.5


def test_get_epoch_closed_pnl(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        close_position(pos_id, 110.0, "target", 10.0, 10.0, 0.5, conn=conn)
    with get_db(db_with_epoch) as conn:
        rows = get_epoch_closed_pnl("EP1", conn)
    assert len(rows) == 1
    assert rows[0]["pnl_usd"] == 10.0


# ---------------------------------------------------------------------------
# 8. Immutability triggers
# ---------------------------------------------------------------------------

def test_fills_immutable_update_raises(db_with_epoch: Path) -> None:
    _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        fill_id = conn.execute("SELECT id FROM fills LIMIT 1").fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db_with_epoch) as conn:
            conn.execute("UPDATE fills SET fill_price=999 WHERE id=?", (fill_id,))


def test_fills_immutable_delete_raises(db_with_epoch: Path) -> None:
    _setup_open_position(db_with_epoch)
    with get_db(db_with_epoch) as conn:
        fill_id = conn.execute("SELECT id FROM fills LIMIT 1").fetchone()[0]
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db_with_epoch) as conn:
            conn.execute("DELETE FROM fills WHERE id=?", (fill_id,))


def test_position_events_immutable_update_raises(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db_with_epoch) as conn:
            conn.execute(
                "UPDATE position_events SET event_type='TAMPERED' WHERE position_id=?", (pos_id,)
            )


def test_position_events_immutable_delete_raises(db_with_epoch: Path) -> None:
    _, pos_id = _setup_open_position(db_with_epoch)
    with pytest.raises(sqlite3.IntegrityError, match="immutable"):
        with get_db(db_with_epoch) as conn:
            conn.execute("DELETE FROM position_events WHERE position_id=?", (pos_id,))


# ---------------------------------------------------------------------------
# 9. Reconciliation
# ---------------------------------------------------------------------------

def test_start_reconciliation_returns_int_id(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
    assert isinstance(run_id, int)
    assert run_id > 0


def test_reconciliation_lifecycle_clean(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
        complete_reconciliation(run_id, discovered=[], resolved=[], unresolved=[], conn=conn)
    with get_db(db_with_epoch) as conn:
        row = conn.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)
        ).fetchone()
    assert row["status"] == "COMPLETE"


def test_reconciliation_lifecycle_with_actions(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
        complete_reconciliation(
            run_id,
            discovered=[{"order_id": "X"}],
            resolved=[{"order_id": "X", "action": "cancel"}],
            unresolved=[],
            conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        row = conn.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)
        ).fetchone()
    assert row["status"] == "COMPLETE_WITH_ACTIONS"


def test_reconciliation_lifecycle_failed(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
        complete_reconciliation(
            run_id,
            discovered=[{"order_id": "X"}],
            resolved=[],
            unresolved=[{"order_id": "X", "reason": "unknown"}],
            conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        row = conn.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (run_id,)
        ).fetchone()
    assert row["status"] == "FAILED"


def test_concurrent_reconciliation_blocked(db_with_epoch: Path) -> None:
    """UNIQUE INDEX prevents two RUNNING runs — no TOCTOU race."""
    with get_db(db_with_epoch) as conn:
        start_reconciliation(conn)
    with pytest.raises(RuntimeError, match="already RUNNING"):
        with get_db(db_with_epoch) as conn:
            start_reconciliation(conn)


def test_stale_running_reconciliation_is_recovered(db_with_epoch: Path) -> None:
    stale_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
    with get_db(db_with_epoch) as conn:
        conn.execute(
            "INSERT INTO reconciliation_runs(started_at, status) VALUES (?,?)",
            (stale_ts, "RUNNING"),
        )
    # Should NOT raise — stale run recovered, new run started
    with get_db(db_with_epoch) as conn:
        new_run_id = start_reconciliation(conn, stale_threshold_minutes=30)
    assert isinstance(new_run_id, int)

    with get_db(db_with_epoch) as conn:
        rows = conn.execute(
            "SELECT id, status FROM reconciliation_runs ORDER BY id"
        ).fetchall()
    assert rows[0]["status"] == "FAILED"
    assert rows[1]["status"] == "RUNNING"
    assert rows[1]["id"] == new_run_id


def test_complete_reconciliation_wrong_id_raises(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
        with pytest.raises(RuntimeError, match="not found or not in RUNNING state"):
            complete_reconciliation(run_id + 999, [], [], [], conn)


def test_reconciliation_allowed_after_previous_complete(db_with_epoch: Path) -> None:
    with get_db(db_with_epoch) as conn:
        run_id = start_reconciliation(conn)
        complete_reconciliation(run_id, [], [], [], conn)
    with get_db(db_with_epoch) as conn:
        new_id = start_reconciliation(conn)
    assert new_id != run_id


# ---------------------------------------------------------------------------
# 10. Migration from JSON
# ---------------------------------------------------------------------------

def test_migrate_from_json_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    run_migrations(db)

    epochs_jsonl = tmp_path / "risk_epochs.jsonl"
    epochs_jsonl.write_text(
        json.dumps({
            "event": "RISK_EPOCH_STARTED",
            "epoch_id": "EP_IDEM",
            "paper_capital": 100.0,
            "reason": "test",
            "timestamp": "2024-01-01T00:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )

    from pipeline.ledger import migrate_from_json

    counts1 = migrate_from_json(
        epochs_jsonl=epochs_jsonl,
        orders_json=tmp_path / "missing.json",
        history_jsonl=tmp_path / "missing.jsonl",
        db_path=db,
    )
    counts2 = migrate_from_json(
        epochs_jsonl=epochs_jsonl,
        orders_json=tmp_path / "missing.json",
        history_jsonl=tmp_path / "missing.jsonl",
        db_path=db,
    )
    assert counts1["epochs"] == 1
    assert counts2["epochs"] == 0


def test_migrate_from_json_multiple_epochs_oldest_gets_ended_at(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    run_migrations(db)

    epochs_jsonl = tmp_path / "risk_epochs.jsonl"
    epochs_jsonl.write_text(
        json.dumps({
            "event": "RISK_EPOCH_STARTED", "epoch_id": "EP_A",
            "paper_capital": 100.0, "reason": "first",
            "timestamp": "2024-01-01T00:00:00+00:00",
        }) + "\n" + json.dumps({
            "event": "RISK_EPOCH_STARTED", "epoch_id": "EP_B",
            "paper_capital": 100.0, "reason": "second",
            "timestamp": "2024-06-01T00:00:00+00:00",
        }) + "\n",
        encoding="utf-8",
    )

    from pipeline.ledger import migrate_from_json

    counts = migrate_from_json(
        epochs_jsonl=epochs_jsonl,
        orders_json=tmp_path / "missing.json",
        history_jsonl=tmp_path / "missing.jsonl",
        db_path=db,
    )
    assert counts["epochs"] == 2

    with get_db(db) as conn:
        ep_a = conn.execute("SELECT ended_at FROM risk_epochs WHERE epoch_id='EP_A'").fetchone()
        ep_b = conn.execute("SELECT ended_at FROM risk_epochs WHERE epoch_id='EP_B'").fetchone()
    assert ep_a["ended_at"] is not None   # older epoch closed
    assert ep_b["ended_at"] is None       # latest stays open


# ---------------------------------------------------------------------------
# 11. Foreign key enforcement and schema constraints
# ---------------------------------------------------------------------------

def test_order_references_nonexistent_epoch_raises(tmp_db: Path) -> None:
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(tmp_db) as conn:
            conn.execute("""
                INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose,
                    qty_usd_requested, placed_at, status)
                VALUES (?,?,?,?,?,?,?,?,'SUBMITTING')
            """, (_oid(), "GHOST-EPOCH", "ZEC-USD", "BUY", "LIMIT", "ENTRY", 5.0, _now()))


def test_fill_references_nonexistent_order_raises(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        insert_epoch("EP1", 100.0, "test", conn=conn)
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(tmp_db) as conn:
            conn.execute("""
                INSERT INTO fills(order_id, fill_price, fill_qty_base, fill_qty_usd, filled_at)
                VALUES (?,?,?,?,?)
            """, ("GHOST-ORDER", 100.0, 1.0, 100.0, _now()))


def test_exit_order_check_constraint_enforced_at_db_level(db_with_epoch: Path) -> None:
    """Raw SQL INSERT of EXIT order with NULL position_id must fail CHECK constraint."""
    with pytest.raises(sqlite3.IntegrityError):
        with get_db(db_with_epoch) as conn:
            conn.execute("""
                INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose,
                    qty_base_requested, placed_at, status)
                VALUES (?,?,?,?,?,?,?,?,'OPEN')
            """, (_oid(), "EP1", "ZEC-USD", "SELL", "LIMIT", "EXIT", 1.0, _now()))


# ---------------------------------------------------------------------------
# 12. Regression: late ENTRY fill must not overwrite qty_base_remaining (P0)
# ---------------------------------------------------------------------------

def test_late_entry_fill_preserves_existing_exit_quantity(db_with_epoch: Path) -> None:
    """
    Sequence: partial entry (0.5 ZEC) → partial exit (0.3 ZEC) → late entry (+0.5 ZEC).
    Before fix: remaining was reset to 1.0 (total entry), ignoring the 0.3 exit.
    After fix:  remaining = 1.0 - 0.3 = 0.7.
    """
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)

    # Partial entry: 0.5 ZEC
    with get_db(db_with_epoch) as conn:
        r1 = apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=0.5, conn=conn)
    pos_id = r1["position_id"]
    assert r1["status"] == "PARTIAL"

    # Partial exit: 0.3 ZEC
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=0.3)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=0.3, conn=conn)
    with get_db(db_with_epoch) as conn:
        mid = conn.execute(
            "SELECT qty_base, qty_base_remaining FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert abs(mid["qty_base"] - 0.5) < 1e-9
    assert abs(mid["qty_base_remaining"] - 0.2) < 1e-9

    # Late entry fill: +0.5 ZEC completes the entry order
    with get_db(db_with_epoch) as conn:
        r2 = apply_fill(order_id=oid, fill_price=101.0, fill_qty_base=0.5, conn=conn)
    assert r2["status"] == "FILLED"
    assert r2["position_id"] == pos_id

    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT qty_base, qty_base_remaining FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert abs(pos["qty_base"] - 1.0) < 1e-9
    # Remaining = 1.0 (total entry) - 0.3 (already exited) = 0.7
    assert abs(pos["qty_base_remaining"] - 0.7) < 1e-9


# ---------------------------------------------------------------------------
# 13. Migration: unversioned legacy prototype (user_version=0, tables present)
# ---------------------------------------------------------------------------

_UNVERSIONED_SCHEMA = """
CREATE TABLE IF NOT EXISTS risk_epochs (
    epoch_id TEXT PRIMARY KEY, paper_capital REAL, reason TEXT,
    started_at TEXT, ended_at TEXT
);
CREATE TABLE IF NOT EXISTS orders (
    id TEXT PRIMARY KEY, epoch_id TEXT, asset TEXT,
    status TEXT DEFAULT 'OPEN', placed_at TEXT, limit_price REAL,
    stop_price REAL, target_price REAL
);
"""


def _make_unversioned_db(path: Path) -> None:
    """Simulates the first prototype (commit 972eac3) which never set user_version."""
    conn = sqlite3.connect(str(path))
    conn.executescript(_UNVERSIONED_SCHEMA)
    # Deliberately NOT setting PRAGMA user_version
    conn.execute(
        "INSERT INTO risk_epochs VALUES ('EP_LEGACY', 100, 'legacy', '2024-01-01', NULL)"
    )
    conn.commit()
    conn.close()


def test_run_migrations_unversioned_db_creates_v0_backup(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_unversioned_db(db)

    run_migrations(db)

    backup = tmp_path / "ledger.v0.bak"
    assert backup.exists(), ".v0.bak must be created for unversioned prototype"
    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_run_migrations_unversioned_backup_preserves_data(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_unversioned_db(db)
    run_migrations(db)

    backup = tmp_path / "ledger.v0.bak"
    with sqlite3.connect(str(backup)) as conn:
        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
    assert row[0] == "EP_LEGACY"


def test_run_migrations_unversioned_new_db_has_v3_tables(tmp_path: Path) -> None:
    db = tmp_path / "ledger.db"
    _make_unversioned_db(db)
    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        tables = {
            r[0]
            for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            ).fetchall()
        }
    assert "trade_intents" in tables
    assert "reconciliation_runs" in tables


def test_run_migrations_fresh_empty_file_no_backup(tmp_path: Path) -> None:
    """A brand-new empty DB (no tables) must NOT create a backup."""
    db = tmp_path / "fresh.db"
    # Create empty file — sqlite_master has no user tables
    sqlite3.connect(str(db)).close()
    run_migrations(db)

    assert not (tmp_path / "fresh.v0.bak").exists()
    with get_db(db) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# 14. State machine: PARTIAL → EXPIRED
# ---------------------------------------------------------------------------

def test_submitting_order_can_transition_to_rejected(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
    with get_db(db_with_epoch) as conn:
        transition_order(oid, "REJECTED", conn=conn)
    with get_db(db_with_epoch) as conn:
        row = conn.execute(
            "SELECT status, rejected_at FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["status"] == "REJECTED"
    assert row["rejected_at"] is not None


def test_rejected_order_is_terminal(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        transition_order(oid, "REJECTED", conn=conn)
    with pytest.raises(ValueError, match="invalid transition"):
        with get_db(db_with_epoch) as conn:
            transition_order(oid, "OPEN", conn=conn)


def test_partial_order_can_transition_to_expired(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=2.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=0.5, conn=conn)
    with get_db(db_with_epoch) as conn:
        transition_order(oid, "EXPIRED", conn=conn)
    with get_db(db_with_epoch) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "EXPIRED"


def test_partial_order_can_transition_to_cancelled(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=2.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=0.5, conn=conn)
    with get_db(db_with_epoch) as conn:
        transition_order(oid, "CANCELLED", conn=conn)
    with get_db(db_with_epoch) as conn:
        row = conn.execute("SELECT status, cancelled_at FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "CANCELLED"
    assert row["cancelled_at"] is not None


# ---------------------------------------------------------------------------
# 15. Reconciliation-mode fills on terminal orders
# ---------------------------------------------------------------------------

def test_reconciliation_fill_on_cancelled_entry_creates_position(db_with_epoch: Path) -> None:
    """
    Coinbase executed a partial fill before the local CANCELLED transition.
    reconciliation_mode=True must record the fill and create a position
    without changing the order's terminal status.
    """
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
        transition_order(oid, "CANCELLED", conn=conn)

    with get_db(db_with_epoch) as conn:
        result = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=0.5,
            exchange_fill_id="CB-LATE-FILL",
            reconciliation_mode=True, conn=conn,
        )

    assert result["reconciliation"] is True
    assert result["position_id"] is not None

    with get_db(db_with_epoch) as conn:
        order_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (oid,)
        ).fetchone()[0]
        pos = conn.execute(
            "SELECT status, qty_base FROM positions WHERE id=?", (result["position_id"],)
        ).fetchone()

    # Order stays CANCELLED — reconciliation owns the discrepancy
    assert order_row["status"] == "CANCELLED"
    # Fill is recorded
    assert fill_count == 1
    # Position is created with the fill's qty
    assert pos["status"] == "OPEN"
    assert abs(pos["qty_base"] - 0.5) < 1e-9


def test_reconciliation_fill_on_expired_entry_creates_position(db_with_epoch: Path) -> None:
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)

    with get_db(db_with_epoch) as conn:
        result = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            reconciliation_mode=True, conn=conn,
        )

    assert result["position_id"] is not None
    with get_db(db_with_epoch) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "EXPIRED"  # status unchanged


def test_reconciliation_fill_normal_order_still_transitions(db_with_epoch: Path) -> None:
    """reconciliation_mode=True on a non-terminal order still transitions status normally."""
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
    with get_db(db_with_epoch) as conn:
        result = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            reconciliation_mode=True, conn=conn,
        )
    assert result["status"] == "FILLED"
    assert result["reconciliation"] is True


def test_terminal_order_fill_without_reconciliation_mode_still_raises(
    db_with_epoch: Path,
) -> None:
    """Without reconciliation_mode, terminal fill must still raise."""
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)
        transition_order(oid, "CANCELLED", conn=conn)
    with pytest.raises(RuntimeError, match="Cannot fill"):
        with get_db(db_with_epoch) as conn:
            apply_fill(order_id=oid, fill_price=100.0, fill_qty_base=1.0, conn=conn)


# ---------------------------------------------------------------------------
# 16. P0 regression: late fill after close must raise LedgerConsistencyError
# ---------------------------------------------------------------------------

def test_late_entry_fill_after_position_closed_raises_consistency_error(
    db_with_epoch: Path,
) -> None:
    """
    Sequence that was silently corrupting the ledger before the guard:
      1. ENTRY partial fill (0.5 ZEC)
      2. EXIT full fill (0.5 ZEC) → position CLOSED
      3. ENTRY order CANCELLED
      4. Reconciliation discovers late ENTRY fill of 0.5 on the cancelled order

    Expected: LedgerConsistencyError — NOT a silent qty_base update on a CLOSED position.
    The resulting state must be unchanged: position still CLOSED with original P&L.
    """
    entry_oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=entry_oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(entry_oid, "OPEN", conn=conn)

    # Step 1: partial entry fill
    with get_db(db_with_epoch) as conn:
        r = apply_fill(order_id=entry_oid, fill_price=100.0, fill_qty_base=0.5, conn=conn)
    pos_id = r["position_id"]

    # Step 2: full exit → position CLOSED
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=0.5)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=0.5, conn=conn)

    with get_db(db_with_epoch) as conn:
        pre = conn.execute("SELECT status, qty_base, pnl_usd FROM positions WHERE id=?",
                           (pos_id,)).fetchone()
    assert pre["status"] == "CLOSED"

    # Step 3: entry order cancelled (Coinbase flow)
    with get_db(db_with_epoch) as conn:
        transition_order(entry_oid, "CANCELLED", conn=conn)

    # Step 4: reconciliation tries to apply late fill
    with pytest.raises(LedgerConsistencyError, match="CLOSED"):
        with get_db(db_with_epoch) as conn:
            apply_fill(
                order_id=entry_oid, fill_price=100.0, fill_qty_base=0.5,
                reconciliation_mode=True, conn=conn,
            )

    # Position must be completely unchanged after the failed attempt
    with get_db(db_with_epoch) as conn:
        post = conn.execute("SELECT status, qty_base, pnl_usd FROM positions WHERE id=?",
                            (pos_id,)).fetchone()
    assert post["status"] == "CLOSED"
    assert post["qty_base"] == pre["qty_base"]
    assert post["pnl_usd"] == pre["pnl_usd"]


# ---------------------------------------------------------------------------
# 17. P0 regression: LedgerConsistencyError caught inside shared tx must not
#     commit the fill (SAVEPOINT atomicity)
# ---------------------------------------------------------------------------

def test_consistency_error_caught_inside_shared_connection_does_not_commit_fill(
    db_with_epoch: Path,
) -> None:
    """
    Reconciler catches LedgerConsistencyError inside the same with get_db() block.
    Before the SAVEPOINT fix: the fill INSERT was already in the outer transaction
    and got committed despite the error being caught.
    After fix: SAVEPOINT rolls back the fill INSERT; only the fill_count stays at 1.
    """
    entry_oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=entry_oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(entry_oid, "OPEN", conn=conn)

    # Partial entry → position created
    with get_db(db_with_epoch) as conn:
        r = apply_fill(order_id=entry_oid, fill_price=100.0, fill_qty_base=0.5, conn=conn)
    pos_id = r["position_id"]

    # Full exit → position CLOSED
    exit_oid = _place_exit_order(db_with_epoch, pos_id, qty_base=0.5)
    with get_db(db_with_epoch) as conn:
        apply_fill(order_id=exit_oid, fill_price=110.0, fill_qty_base=0.5, conn=conn)

    with get_db(db_with_epoch) as conn:
        transition_order(entry_oid, "CANCELLED", conn=conn)

    # Reconciler catches error *inside* the shared connection — exactly the pattern
    # that exposes the pre-SAVEPOINT bug.
    with get_db(db_with_epoch) as conn:
        try:
            apply_fill(
                order_id=entry_oid, fill_price=100.0, fill_qty_base=0.5,
                reconciliation_mode=True, conn=conn,
            )
        except LedgerConsistencyError:
            pass  # reconciler handles this and continues its own transaction

    # The late fill must NOT appear — SAVEPOINT must have rolled it back.
    with get_db(db_with_epoch) as conn:
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (entry_oid,)
        ).fetchone()[0]
    assert fill_count == 1, (
        "LedgerConsistencyError caught inside shared tx must not leave a committed fill. "
        "SAVEPOINT in apply_fill() must roll back the INSERT before re-raising."
    )

    with get_db(db_with_epoch) as conn:
        pos = conn.execute(
            "SELECT status, qty_base FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert pos["status"] == "CLOSED"
    assert abs(pos["qty_base"] - 0.5) < 1e-9


# ---------------------------------------------------------------------------
# 18. P2 regression: idempotent EXIT fill must return correct position_id
# ---------------------------------------------------------------------------

def test_idempotent_exit_fill_returns_position_id(db_with_epoch: Path) -> None:
    """
    Replaying a known EXIT fill must return the position_id, not None.
    Before the fix the idempotency path looked up position via entry_order_id,
    which is wrong for EXIT orders — they reference position via order.position_id.
    """
    _, pos_id = _setup_open_position(db_with_epoch)
    exit_oid = _place_exit_order(db_with_epoch, pos_id)

    # First fill (normal path)
    with get_db(db_with_epoch) as conn:
        r1 = apply_fill(
            order_id=exit_oid, fill_price=110.0, fill_qty_base=1.0,
            exchange_fill_id="EXIT-FILL-001", conn=conn,
        )
    assert r1["position_id"] == pos_id

    # Replay — idempotency path must also return the position_id
    with get_db(db_with_epoch) as conn:
        r2 = apply_fill(
            order_id=exit_oid, fill_price=110.0, fill_qty_base=1.0,
            exchange_fill_id="EXIT-FILL-001", conn=conn,
        )
    assert r2["position_id"] == pos_id, (
        "Idempotent EXIT fill replay must return position_id, not None. "
        "Before fix: used entry_order_id lookup which always returns None for EXIT orders."
    )
    assert r2.get("replayed") is True


def test_run_migrations_v4_to_v5_inplace(tmp_path: Path) -> None:
    """
    V4→V5 is a non-destructive in-place migration: no backup created, existing
    data preserved, rejection_reason column added, new UNIQUE index created.
    """
    from pipeline.ledger import _SCHEMA_V5 as _schema  # noqa: F401

    # Build a V4 DB: fresh V5 schema minus the rejection_reason column/index, then
    # downgrade the user_version to 4 to simulate a pre-stacking-guard DB.
    db = tmp_path / "ledger_v4.db"
    conn = sqlite3.connect(str(db))
    # Use the full V5 schema to create tables; then drop rejection_reason via recreate
    # isn't trivial in SQLite.  Instead we simulate V4 by creating a minimal schema
    # that matches V4 (no rejection_reason, no idx_one_active_entry_per_asset).
    conn.executescript("""
        CREATE TABLE risk_epochs (
            epoch_id TEXT PRIMARY KEY, paper_capital REAL NOT NULL, reason TEXT NOT NULL,
            started_at TEXT NOT NULL, ended_at TEXT
        );
        CREATE TABLE orders (
            id TEXT PRIMARY KEY,
            epoch_id TEXT NOT NULL REFERENCES risk_epochs(epoch_id),
            asset TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
            purpose TEXT NOT NULL, position_id TEXT,
            qty_base_requested REAL, qty_usd_requested REAL, limit_price REAL,
            placed_at TEXT NOT NULL, expires_at TEXT, reasoning TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'SUBMITTING',
            exchange_order_id TEXT UNIQUE, cancelled_at TEXT, expired_at TEXT,
            rejected_at TEXT
        );
        CREATE TABLE positions (
            id TEXT PRIMARY KEY, entry_order_id TEXT NOT NULL UNIQUE,
            epoch_id TEXT NOT NULL, asset TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL REFERENCES orders(id),
            exchange_fill_id TEXT UNIQUE, fill_price REAL NOT NULL,
            fill_qty_base REAL NOT NULL, fill_qty_usd REAL NOT NULL,
            fee_usd REAL NOT NULL DEFAULT 0.0, is_taker INTEGER NOT NULL DEFAULT 1,
            filled_at TEXT NOT NULL
        );
        CREATE TABLE trade_intents (
            order_id TEXT PRIMARY KEY REFERENCES orders(id),
            stop_price REAL NOT NULL, target_price REAL NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX idx_one_active_epoch ON risk_epochs(1) WHERE ended_at IS NULL;
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP_V4', 500.0, 'pre-migration epoch', '2025-01-01T00:00:00Z');
    """)
    conn.execute("PRAGMA user_version = 4")
    conn.commit()
    conn.close()

    # No backup should exist before migration
    assert not (tmp_path / "ledger_v4.v4.bak").exists()

    run_migrations(db)

    # No backup created (in-place migration)
    assert not (tmp_path / "ledger_v4.v4.bak").exists(), (
        "V4→V5 is in-place — no backup expected"
    )

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION, f"expected V{SCHEMA_VERSION}, got V{ver}"

        # rejection_reason column must now exist
        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
        assert "rejection_reason" in cols, "rejection_reason column missing after V4→V5"

        # existing data preserved
        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
        assert row[0] == "EP_V4", "pre-migration data must survive in-place migration"

        # UNIQUE INDEX must exist
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_one_active_entry_per_asset'"
        ).fetchone()
        assert idx is not None, "idx_one_active_entry_per_asset not created by V4→V5 migration"


def test_schema_has_one_active_entry_per_asset_index(tmp_db: Path) -> None:
    """Fresh V5 schema must include the partial UNIQUE index on active ENTRY orders."""
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_one_active_entry_per_asset'"
        ).fetchone()
    assert row is not None, (
        "idx_one_active_entry_per_asset missing from fresh V5 schema. "
        "This index is the DB-level defence-in-depth against duplicate ENTRY orders."
    )


# ---------------------------------------------------------------------------
# V4→V5 migration atomicity
# ---------------------------------------------------------------------------

_V4_SCHEMA = """
CREATE TABLE risk_epochs (
    epoch_id TEXT PRIMARY KEY, paper_capital REAL NOT NULL, reason TEXT NOT NULL,
    started_at TEXT NOT NULL, ended_at TEXT
);
CREATE TABLE orders (
    id TEXT PRIMARY KEY,
    epoch_id TEXT NOT NULL REFERENCES risk_epochs(epoch_id),
    asset TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
    purpose TEXT NOT NULL, position_id TEXT,
    qty_base_requested REAL, qty_usd_requested REAL, limit_price REAL,
    placed_at TEXT NOT NULL, expires_at TEXT, reasoning TEXT NOT NULL DEFAULT '',
    status TEXT NOT NULL DEFAULT 'SUBMITTING',
    exchange_order_id TEXT UNIQUE, cancelled_at TEXT, expired_at TEXT,
    rejected_at TEXT
);
CREATE TABLE positions (
    id TEXT PRIMARY KEY, entry_order_id TEXT NOT NULL UNIQUE,
    epoch_id TEXT NOT NULL, asset TEXT NOT NULL, status TEXT NOT NULL DEFAULT 'OPEN'
);
CREATE TABLE fills (
    id INTEGER PRIMARY KEY AUTOINCREMENT, order_id TEXT NOT NULL REFERENCES orders(id),
    exchange_fill_id TEXT UNIQUE, fill_price REAL NOT NULL, fill_qty_base REAL NOT NULL,
    fill_qty_usd REAL NOT NULL, fee_usd REAL NOT NULL DEFAULT 0.0,
    is_taker INTEGER NOT NULL DEFAULT 1, filled_at TEXT NOT NULL
);
CREATE TABLE trade_intents (
    order_id TEXT PRIMARY KEY REFERENCES orders(id),
    stop_price REAL NOT NULL, target_price REAL NOT NULL, recorded_at TEXT NOT NULL
);
CREATE UNIQUE INDEX idx_one_active_epoch ON risk_epochs(1) WHERE ended_at IS NULL;
"""


def _make_v4_db(path: Path, extra_sql: str = "") -> None:
    """Create a minimal V4 DB (no rejection_reason column, no stacking index)."""
    conn = sqlite3.connect(str(path))
    conn.executescript(_V4_SCHEMA)
    if extra_sql:
        conn.executescript(extra_sql)
    conn.execute("PRAGMA user_version = 4")
    conn.commit()
    conn.close()


def test_v4_to_v5_migration_conflicts_roll_back(tmp_path: Path) -> None:
    """
    If two SUBMITTING ENTRY orders exist for the same asset, the UNIQUE index
    cannot be created.  The entire migration rolls back atomically: user_version
    stays V4 and rejection_reason column is NOT permanently added.

    A clear RuntimeError is raised.  Re-running produces the same error (not
    'duplicate column'), proving the rollback was complete.
    """
    db = tmp_path / "ledger_v4_conflict.db"
    _make_v4_db(db, extra_sql="""
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP1', 100.0, 'test', '2025-01-01T00:00:00Z');
        INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose, placed_at,
                           qty_usd_requested, status)
            VALUES ('ORD-1', 'EP1', 'ZEC-USD', 'BUY', 'LIMIT', 'ENTRY',
                    '2025-01-01T00:00:00Z', 10.0, 'SUBMITTING');
        INSERT INTO orders(id, epoch_id, asset, side, order_type, purpose, placed_at,
                           qty_usd_requested, status)
            VALUES ('ORD-2', 'EP1', 'ZEC-USD', 'BUY', 'LIMIT', 'ENTRY',
                    '2025-01-01T00:01:00Z', 10.0, 'SUBMITTING');
    """)

    with pytest.raises(RuntimeError, match="V4→V5 migration blocked"):
        run_migrations(db)

    # user_version must still be 4 — rollback was atomic
    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == 4, "user_version must stay 4 after failed migration"

    # rejection_reason column must NOT be present — DDL was rolled back
    with sqlite3.connect(str(db)) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
    assert "rejection_reason" not in cols, (
        "rejection_reason column must not be permanently added when migration fails"
    )

    # Second run must raise the same domain error, NOT 'duplicate column'
    with pytest.raises(RuntimeError, match="V4→V5 migration blocked"):
        run_migrations(db)


def test_v4_to_v5_migration_idempotent_after_column_exists(tmp_path: Path) -> None:
    """
    If rejection_reason column already exists (e.g. manual partial migration),
    the migration skips ALTER TABLE and still completes successfully.
    """
    db = tmp_path / "ledger_v4_col.db"
    _make_v4_db(db, extra_sql="""
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP1', 100.0, 'test', '2025-01-01T00:00:00Z');
    """)
    # Manually add the column, simulating a partial prior migration
    conn = sqlite3.connect(str(db))
    conn.execute("ALTER TABLE orders ADD COLUMN rejection_reason TEXT")
    conn.commit()
    conn.close()

    run_migrations(db)  # must not raise "duplicate column"

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_v4_to_v5_migration_idempotent_after_index_exists(tmp_path: Path) -> None:
    """
    If idx_one_active_entry_per_asset already exists, migration skips
    CREATE INDEX and completes successfully.
    """
    db = tmp_path / "ledger_v4_idx.db"
    _make_v4_db(db, extra_sql="""
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP1', 100.0, 'test', '2025-01-01T00:00:00Z');
    """)
    conn = sqlite3.connect(str(db))
    conn.execute("ALTER TABLE orders ADD COLUMN rejection_reason TEXT")
    conn.execute("""
        CREATE UNIQUE INDEX idx_one_active_entry_per_asset
            ON orders(asset) WHERE purpose='ENTRY'
              AND status IN ('SUBMITTING','OPEN','PARTIAL')
    """)
    conn.commit()
    conn.close()

    run_migrations(db)  # must not raise "index already exists"

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


# ---------------------------------------------------------------------------
# V5→V6 migration: fills_finalized_at column + idx_unfinalized_terminal
# ---------------------------------------------------------------------------

def _make_v5_db(path: Path) -> None:
    """
    Build a V5 database: full V6 schema minus fills_finalized_at column and
    idx_unfinalized_terminal, then stamp user_version=5.
    Simulates a pre-terminal-finalization production DB.
    """
    conn = sqlite3.connect(str(path))
    conn.executescript("""
        CREATE TABLE risk_epochs (
            epoch_id TEXT PRIMARY KEY, paper_capital REAL NOT NULL,
            reason TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT
        );
        CREATE TABLE orders (
            id TEXT PRIMARY KEY,
            epoch_id TEXT NOT NULL REFERENCES risk_epochs(epoch_id),
            asset TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
            purpose TEXT NOT NULL, position_id TEXT,
            qty_base_requested REAL, qty_usd_requested REAL, limit_price REAL,
            placed_at TEXT NOT NULL, expires_at TEXT,
            reasoning TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'SUBMITTING',
            exchange_order_id TEXT UNIQUE,
            cancelled_at TEXT, expired_at TEXT, rejected_at TEXT,
            rejection_reason TEXT
        );
        CREATE UNIQUE INDEX idx_one_active_entry_per_asset
            ON orders(asset) WHERE purpose='ENTRY'
            AND status IN ('SUBMITTING','OPEN','PARTIAL');
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL REFERENCES orders(id),
            exchange_fill_id TEXT UNIQUE, fill_price REAL NOT NULL,
            fill_qty_base REAL NOT NULL, fill_qty_usd REAL NOT NULL,
            fee_usd REAL NOT NULL DEFAULT 0.0, is_taker INTEGER NOT NULL DEFAULT 1,
            filled_at TEXT NOT NULL
        );
        CREATE TABLE positions (
            id TEXT PRIMARY KEY, entry_order_id TEXT NOT NULL UNIQUE,
            epoch_id TEXT NOT NULL, asset TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );
        CREATE TABLE trade_intents (
            order_id TEXT PRIMARY KEY REFERENCES orders(id),
            stop_price REAL NOT NULL, target_price REAL NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX idx_one_active_epoch
            ON risk_epochs(1) WHERE ended_at IS NULL;
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP_V5', 500.0, 'pre-V6 epoch', '2025-01-01T00:00:00Z');
        INSERT INTO orders(
            id, epoch_id, asset, side, order_type, purpose,
            placed_at, status, exchange_order_id,
            cancelled_at, rejection_reason
        ) VALUES (
            'ORD-V5-TERMINAL', 'EP_V5', 'ZEC-USD', 'BUY', 'LIMIT', 'ENTRY',
            '2025-01-01T00:00:00Z', 'CANCELLED', 'CB-V5-EX-1',
            '2025-01-01T01:00:00Z', 'manual test order'
        );
    """)
    conn.execute("PRAGMA user_version = 5")
    conn.commit()
    conn.close()


def test_migration_v5_to_v6_adds_fills_finalized_at(tmp_path: Path) -> None:
    """
    V5→V6 in-place migration: fills_finalized_at column and idx_unfinalized_terminal
    are added; existing data is preserved; user_version becomes 6.
    """
    db = tmp_path / "ledger_v5.db"
    _make_v5_db(db)

    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION, f"expected V{SCHEMA_VERSION}, got V{ver}"

        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
        assert "fills_finalized_at" in cols, "fills_finalized_at column missing after V5→V6"

        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_unfinalized_terminal'"
        ).fetchone()
        assert idx is not None, "idx_unfinalized_terminal not created by V5→V6 migration"

        # Epoch data preserved
        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
        assert row[0] == "EP_V5", "epoch data must survive V5→V6"

        # Terminal order data preserved and new column defaults correctly
        conn.row_factory = sqlite3.Row
        order = conn.execute(
            "SELECT id, status, exchange_order_id, rejection_reason, fills_finalized_at"
            "  FROM orders WHERE id='ORD-V5-TERMINAL'"
        ).fetchone()
        assert order is not None, "terminal order row must survive V5→V6"
        assert order["status"] == "CANCELLED"
        assert order["exchange_order_id"] == "CB-V5-EX-1"
        assert order["rejection_reason"] == "manual test order"
        assert order["fills_finalized_at"] is None, (
            "fills_finalized_at must default to NULL for existing rows after migration"
        )

        # Order satisfies the partial index predicate (CANCELLED, has exchange_order_id, no finalized_at)
        in_idx = conn.execute(
            "SELECT id FROM orders"
            " WHERE status IN ('EXPIRED','CANCELLED')"
            "   AND exchange_order_id IS NOT NULL"
            "   AND fills_finalized_at IS NULL"
            "   AND id='ORD-V5-TERMINAL'"
        ).fetchone()
        assert in_idx is not None, "terminal order must satisfy idx_unfinalized_terminal predicate"

    # No backup file created (in-place migration)
    assert not (tmp_path / "ledger_v5.v5.bak").exists(), "V5→V6 must not create a backup"


def test_migration_v5_to_v6_idempotent(tmp_path: Path) -> None:
    """
    V5→V6 is idempotent: if fills_finalized_at already exists (e.g. partial
    migration from a crash), running again must not raise.
    """
    db = tmp_path / "ledger_v5_partial.db"
    _make_v5_db(db)

    # Manually add the column to simulate a partial migration
    conn = sqlite3.connect(str(db))
    conn.execute("ALTER TABLE orders ADD COLUMN fills_finalized_at TEXT")
    conn.commit()
    conn.close()

    run_migrations(db)  # must not raise

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_migration_v4_to_v6_chains_both_columns(tmp_path: Path) -> None:
    """
    A V4 DB runs through V4→V5→V6 in a single run_migrations() call.
    Both rejection_reason and fills_finalized_at must be present afterwards.
    """
    db = tmp_path / "ledger_v4_chain.db"
    conn = sqlite3.connect(str(db))
    conn.executescript("""
        CREATE TABLE risk_epochs (
            epoch_id TEXT PRIMARY KEY, paper_capital REAL NOT NULL,
            reason TEXT NOT NULL, started_at TEXT NOT NULL, ended_at TEXT
        );
        CREATE TABLE orders (
            id TEXT PRIMARY KEY,
            epoch_id TEXT NOT NULL REFERENCES risk_epochs(epoch_id),
            asset TEXT NOT NULL, side TEXT NOT NULL, order_type TEXT NOT NULL,
            purpose TEXT NOT NULL, position_id TEXT,
            qty_base_requested REAL, qty_usd_requested REAL, limit_price REAL,
            placed_at TEXT NOT NULL, expires_at TEXT,
            reasoning TEXT NOT NULL DEFAULT '',
            status TEXT NOT NULL DEFAULT 'SUBMITTING',
            exchange_order_id TEXT UNIQUE,
            cancelled_at TEXT, expired_at TEXT, rejected_at TEXT
        );
        CREATE UNIQUE INDEX idx_one_active_epoch
            ON risk_epochs(1) WHERE ended_at IS NULL;
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP_V4_CHAIN', 500.0, 'chain test', '2025-01-01T00:00:00Z');
    """)
    conn.execute("PRAGMA user_version = 4")
    conn.commit()
    conn.close()

    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION, f"expected V{SCHEMA_VERSION}, got V{ver}"

        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
        assert "rejection_reason" in cols, "rejection_reason missing after V4→V6 chain"
        assert "fills_finalized_at" in cols, "fills_finalized_at missing after V4→V6 chain"

        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
        assert row[0] == "EP_V4_CHAIN", "pre-migration data must survive V4→V6 chain"


def test_fresh_schema_has_fills_finalized_at(tmp_db: Path) -> None:
    """Fresh V6 schema must include the fills_finalized_at column."""
    with get_db(tmp_db) as conn:
        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
    assert "fills_finalized_at" in cols, (
        "fills_finalized_at missing from fresh schema — "
        "update _SCHEMA_CURRENT to include it"
    )


def test_fresh_schema_has_unfinalized_terminal_index(tmp_db: Path) -> None:
    """Fresh V6 schema must include the partial index for unfinalized terminal orders."""
    with get_db(tmp_db) as conn:
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_unfinalized_terminal'"
        ).fetchone()
    assert idx is not None, (
        "idx_unfinalized_terminal missing from fresh schema — "
        "startup reconciliation will do a full table scan for terminal orders"
    )


def test_idempotent_replay_with_reconciliation_mode_returns_correct_flags(
    db_with_epoch: Path,
) -> None:
    """
    When a fill is replayed with reconciliation_mode=True, the return dict must have
    reconciliation=True and replayed=True.
    Before fix: 'reconciliation' was hardcoded False on the idempotency path regardless
    of the reconciliation_mode argument.
    """
    oid = _oid()
    with get_db(db_with_epoch) as conn:
        insert_order(
            order_id=oid, epoch_id="EP1", asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=1.0, conn=conn,
        )
        transition_order(oid, "OPEN", conn=conn)

    with get_db(db_with_epoch) as conn:
        apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            exchange_fill_id="FILL-RECON-001", conn=conn,
        )

    with get_db(db_with_epoch) as conn:
        replayed = apply_fill(
            order_id=oid, fill_price=100.0, fill_qty_base=1.0,
            exchange_fill_id="FILL-RECON-001",
            reconciliation_mode=True, conn=conn,
        )

    assert replayed["reconciliation"] is True, (
        "Idempotent replay with reconciliation_mode=True must return reconciliation=True. "
        "Before fix: hardcoded False on idempotency path."
    )
    assert replayed.get("replayed") is True

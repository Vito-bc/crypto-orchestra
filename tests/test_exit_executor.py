"""
Tests for pipeline/outbox.place_exit_outbox() and pipeline/exit_executor.run_exit_executor().

Coverage:
  TX-A / TX-B mechanics (outbox)
  Idempotency and PlacementBlocked guards (outbox)
  Stop-loss / take-profit / max-hold exit triggers (executor)
  HWM + trailing stop updates (executor)
  Idempotency on active EXIT existing (executor)
  Extension review wiring (executor)
  Schema V7 index (ledger)
"""
from __future__ import annotations

import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import pytest

from pipeline.ledger import (
    SCHEMA_VERSION,
    apply_fill,
    get_db,
    insert_epoch,
    insert_order,
    insert_trade_intent,
    run_migrations,
    transition_order,
    update_position_stop,
)
from pipeline.outbox import (
    CoinbaseRejected,
    ExitPlaceResult,
    PlacementBlocked,
    place_exit_outbox,
)
from pipeline.exit_executor import run_exit_executor

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EPOCH_ID = "EP-EXIT-TEST"
_ASSET = "ZEC-USD"


def _oid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _ago(minutes: int) -> str:
    return (datetime.now(timezone.utc) - timedelta(minutes=minutes)).isoformat()


@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ledger.db"
    run_migrations(db)
    with get_db(db) as conn:
        insert_epoch(_EPOCH_ID, 500.0, "exit test epoch", conn=conn)
    return db


def _open_position(
    tmp_db: Path,
    entry_price: float = 100.0,
    qty_base: float = 0.1,
    stop_price: float = 90.0,
    target_price: float = 120.0,
    opened_at: Optional[str] = None,
) -> tuple[str, str]:
    """
    Create ENTRY order + fill → OPEN position.
    Returns (entry_order_id, position_id).
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset=_ASSET,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=qty_base, conn=conn,
        )
        insert_trade_intent(oid, stop_price=stop_price, target_price=target_price, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id=f"CB-E-{oid[:6]}", conn=conn)

    with get_db(tmp_db) as conn:
        result = apply_fill(
            order_id=oid,
            fill_price=entry_price,
            fill_qty_base=qty_base,
            fee_usd=0.04,
            conn=conn,
        )
    pos_id = result["position_id"]

    if opened_at is not None:
        with get_db(tmp_db) as conn:
            conn.execute(
                "UPDATE positions SET opened_at=? WHERE id=?", (opened_at, pos_id)
            )

    return oid, pos_id


def _no_sell(order_id: str, asset: str, qty: float) -> str:
    raise AssertionError("coinbase_sell_fn must not be called in this test")


def _ok_sell(order_id: str, asset: str, qty: float) -> str:
    return f"EX-{order_id[:8]}"


# ---------------------------------------------------------------------------
# 1. place_exit_outbox — TX-A records before network call
# ---------------------------------------------------------------------------

def test_place_exit_outbox_tx_a_committed_before_sell(tmp_db: Path) -> None:
    """
    TX-A must write the SUBMITTING order to the DB before coinbase_sell_fn is called.
    Even if coinbase_sell_fn raises (simulating crash), the order survives.
    """
    _, pos_id = _open_position(tmp_db)

    captured_order_id: list[str] = []

    def _crash_after_check(order_id: str, asset: str, qty: float) -> str:
        # Check the order was already committed to the DB.
        with get_db(tmp_db) as conn:
            row = conn.execute(
                "SELECT status FROM orders WHERE id=?", (order_id,)
            ).fetchone()
        assert row is not None, "order must be in DB before network call (TX-A)"
        assert row["status"] == "SUBMITTING"
        captured_order_id.append(order_id)
        raise RuntimeError("network error — ambiguous")

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_crash_after_check,
        db_path=tmp_db,
    )

    assert result.status == "SUBMITTING"
    assert len(captured_order_id) == 1

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status, purpose, side, position_id FROM orders WHERE id=?",
            (captured_order_id[0],),
        ).fetchone()
    assert row["status"] == "SUBMITTING"
    assert row["purpose"] == "EXIT"
    assert row["side"] == "SELL"
    assert row["position_id"] == pos_id


# ---------------------------------------------------------------------------
# 2. place_exit_outbox — accepted → OPEN
# ---------------------------------------------------------------------------

def test_place_exit_outbox_accepted_goes_open(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="TAKE_PROFIT",
        coinbase_sell_fn=_ok_sell,
        db_path=tmp_db,
    )

    assert result.status == "OPEN"
    assert result.exchange_order_id is not None
    assert result.position_id == pos_id

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == result.exchange_order_id


# ---------------------------------------------------------------------------
# 3. place_exit_outbox — CoinbaseRejected → REJECTED
# ---------------------------------------------------------------------------

def test_place_exit_outbox_rejected_coinbase_error(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    def _reject(order_id: str, asset: str, qty: float) -> str:
        raise CoinbaseRejected("INSUFFICIENT_FUND")

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_reject,
        db_path=tmp_db,
    )

    assert result.status == "REJECTED"
    assert "INSUFFICIENT_FUND" in (result.rejection_reason or "")

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status, rejection_reason FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["status"] == "REJECTED"
    assert "INSUFFICIENT_FUND" in (row["rejection_reason"] or "")


# ---------------------------------------------------------------------------
# 4. place_exit_outbox — ambiguous exception → SUBMITTING (TX-B skipped)
# ---------------------------------------------------------------------------

def test_place_exit_outbox_ambiguous_stays_submitting(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    def _timeout(order_id: str, asset: str, qty: float) -> str:
        raise ConnectionError("timeout")

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="MAX_HOLD",
        coinbase_sell_fn=_timeout,
        db_path=tmp_db,
    )

    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["status"] == "SUBMITTING"


# ---------------------------------------------------------------------------
# 5. place_exit_outbox — idempotent replay
# ---------------------------------------------------------------------------

def test_place_exit_outbox_idempotent_same_order_id(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    sell_calls: list[str] = []

    def _counting_sell(order_id: str, asset: str, qty: float) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    fixed_oid = _oid()
    first = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_counting_sell,
        order_id=fixed_oid,
        db_path=tmp_db,
    )
    second = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_counting_sell,
        order_id=fixed_oid,
        db_path=tmp_db,
    )

    assert len(sell_calls) == 1, "sell_fn must be called exactly once (idempotent replay)"
    assert first.status == second.status
    assert first.exchange_order_id == second.exchange_order_id


# ---------------------------------------------------------------------------
# 6. place_exit_outbox — blocked when position not found
# ---------------------------------------------------------------------------

def test_place_exit_outbox_blocked_if_position_not_found(tmp_db: Path) -> None:
    with pytest.raises(PlacementBlocked, match="not found"):
        place_exit_outbox(
            position_id="NONEXISTENT-POS",
            exit_reason="STOP_LOSS",
            coinbase_sell_fn=_no_sell,
            db_path=tmp_db,
        )


# ---------------------------------------------------------------------------
# 7. place_exit_outbox — blocked when position already CLOSED
# ---------------------------------------------------------------------------

def test_place_exit_outbox_blocked_if_position_closed(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)
    # Force CLOSED
    with get_db(tmp_db) as conn:
        conn.execute(
            "UPDATE positions SET status='CLOSED', qty_base_remaining=0 WHERE id=?",
            (pos_id,),
        )

    with pytest.raises(PlacementBlocked, match="CLOSED"):
        place_exit_outbox(
            position_id=pos_id,
            exit_reason="STOP_LOSS",
            coinbase_sell_fn=_no_sell,
            db_path=tmp_db,
        )


# ---------------------------------------------------------------------------
# 8. place_exit_outbox — blocked when active EXIT already exists
# ---------------------------------------------------------------------------

def test_place_exit_outbox_blocked_if_active_exit_exists(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    # Place first exit
    place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_ok_sell,
        db_path=tmp_db,
    )

    # Second attempt with NEW order_id must be blocked
    with pytest.raises(PlacementBlocked, match="active EXIT order"):
        place_exit_outbox(
            position_id=pos_id,
            exit_reason="STOP_LOSS",
            coinbase_sell_fn=_ok_sell,
            db_path=tmp_db,
        )


# ---------------------------------------------------------------------------
# 9. place_exit_outbox — reads qty_base from ledger (not from caller)
# ---------------------------------------------------------------------------

def test_place_exit_outbox_reads_qty_from_ledger(tmp_db: Path) -> None:
    """
    qty_base_requested on the created EXIT order must equal position.qty_base_remaining,
    not any value supplied by the caller.
    """
    _, pos_id = _open_position(tmp_db, qty_base=0.1)

    received_qty: list[float] = []

    def _capture_qty(order_id: str, asset: str, qty: float) -> str:
        received_qty.append(qty)
        return f"EX-{order_id[:8]}"

    place_exit_outbox(
        position_id=pos_id,
        exit_reason="TAKE_PROFIT",
        coinbase_sell_fn=_capture_qty,
        db_path=tmp_db,
    )

    assert len(received_qty) == 1
    assert abs(received_qty[0] - 0.1) < 1e-9, (
        f"qty passed to sell_fn must equal position.qty_base_remaining; got {received_qty[0]}"
    )


# ---------------------------------------------------------------------------
# 10. run_exit_executor — STOP_LOSS condition triggers exit
# ---------------------------------------------------------------------------

def test_run_exit_executor_stop_loss_places_exit(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: float) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    # Price drops below stop
    actions = run_exit_executor(
        asset=_ASSET,
        current_price=85.0,  # < stop_price=90.0
        coinbase_sell_fn=_sell,
        db_path=tmp_db,
    )

    assert len(sell_calls) == 1
    assert len(actions) == 1
    assert actions[0]["exit_reason"] == "STOP_LOSS"
    assert actions[0]["result"] is not None
    assert actions[0]["result"].status == "OPEN"

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT purpose, side, status, position_id FROM orders WHERE id=?",
            (actions[0]["result"].order_id,),
        ).fetchone()
    assert row["purpose"] == "EXIT"
    assert row["side"] == "SELL"
    assert row["status"] == "OPEN"
    assert row["position_id"] == pos_id


# ---------------------------------------------------------------------------
# 11. run_exit_executor — TAKE_PROFIT condition triggers exit
# ---------------------------------------------------------------------------

def test_run_exit_executor_take_profit_places_exit(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    actions = run_exit_executor(
        asset=_ASSET,
        current_price=125.0,  # > target_price=120.0
        coinbase_sell_fn=_ok_sell,
        db_path=tmp_db,
    )

    assert len(actions) == 1
    assert actions[0]["exit_reason"] == "TAKE_PROFIT"
    assert actions[0]["result"].status == "OPEN"


# ---------------------------------------------------------------------------
# 12. run_exit_executor — MAX_HOLD after all extensions exhausted
# ---------------------------------------------------------------------------

def test_run_exit_executor_max_hold_places_exit(tmp_db: Path) -> None:
    from pipeline.position_tracker import MAX_EXTENSIONS, MAX_HOLD_HOURS, EXTENSION_HOURS
    exhausted_limit = MAX_HOLD_HOURS + MAX_EXTENSIONS * EXTENSION_HOURS
    old_opened = _ago(minutes=int((exhausted_limit + 1) * 60))
    _, pos_id = _open_position(
        tmp_db, entry_price=100.0, stop_price=85.0, target_price=130.0,
        opened_at=old_opened,
    )
    # Exhaust all extensions in DB
    with get_db(tmp_db) as conn:
        conn.execute(
            "UPDATE positions SET extensions_used=? WHERE id=?",
            (MAX_EXTENSIONS, pos_id),
        )

    actions = run_exit_executor(
        asset=_ASSET,
        current_price=105.0,  # between stop and target
        coinbase_sell_fn=_ok_sell,
        db_path=tmp_db,
    )

    assert len(actions) == 1
    assert actions[0]["exit_reason"] == "MAX_HOLD"
    assert actions[0]["result"].status == "OPEN"


# ---------------------------------------------------------------------------
# 13. run_exit_executor — no exit when price is safe
# ---------------------------------------------------------------------------

def test_run_exit_executor_no_exit_when_price_safe(tmp_db: Path) -> None:
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    actions = run_exit_executor(
        asset=_ASSET,
        current_price=105.0,  # safe zone
        coinbase_sell_fn=_no_sell,
        db_path=tmp_db,
    )

    assert all(a["result"] is None for a in actions), (
        f"No exit should be placed in safe zone: {actions}"
    )


# ---------------------------------------------------------------------------
# 14. run_exit_executor — idempotent when active EXIT exists
# ---------------------------------------------------------------------------

def test_run_exit_executor_idempotent_on_active_exit(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: float) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    # First tick: places exit
    run_exit_executor(
        asset=_ASSET, current_price=85.0, coinbase_sell_fn=_sell, db_path=tmp_db
    )

    assert len(sell_calls) == 1
    first_order_id = sell_calls[0]

    # Second tick at same stop-loss price: must NOT place another exit
    actions = run_exit_executor(
        asset=_ASSET, current_price=85.0, coinbase_sell_fn=_sell, db_path=tmp_db
    )

    assert len(sell_calls) == 1, "sell_fn must not be called a second time"
    skipped = [a for a in actions if a.get("note") == "active_exit_already_exists"]
    assert len(skipped) == 1


# ---------------------------------------------------------------------------
# 15. run_exit_executor — HWM updated in ledger
# ---------------------------------------------------------------------------

def test_run_exit_executor_updates_hwm_in_ledger(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=200.0)

    run_exit_executor(
        asset=_ASSET,
        current_price=110.0,  # new high — no exit triggered yet
        coinbase_sell_fn=_no_sell,
        db_path=tmp_db,
    )

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT high_water_mark FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert row["high_water_mark"] == pytest.approx(110.0), (
        "high_water_mark must be updated to current_price in ledger"
    )


# ---------------------------------------------------------------------------
# 16. run_exit_executor — trailing stop updated in ledger
# ---------------------------------------------------------------------------

def test_run_exit_executor_updates_trailing_stop(tmp_db: Path) -> None:
    from pipeline.position_tracker import BREAK_EVEN_PCT

    entry = 100.0
    _, pos_id = _open_position(tmp_db, entry_price=entry, stop_price=90.0, target_price=200.0)

    # Price rises above break-even threshold
    new_price = entry * (1 + BREAK_EVEN_PCT + 0.01)

    run_exit_executor(
        asset=_ASSET,
        current_price=new_price,
        coinbase_sell_fn=_no_sell,
        db_path=tmp_db,
    )

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT stop_price FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    # Stop must have moved at least to break-even (entry price)
    assert row["stop_price"] >= entry - 0.01, (
        f"stop_price must advance to at least entry after break-even trigger; "
        f"got {row['stop_price']}"
    )


# ---------------------------------------------------------------------------
# 17. run_exit_executor — extension review grants extension
# ---------------------------------------------------------------------------

def test_run_exit_executor_extension_granted_persisted(tmp_db: Path) -> None:
    from pipeline.position_tracker import MAX_HOLD_HOURS
    old_opened = _ago(minutes=int((MAX_HOLD_HOURS + 1) * 60))
    _, pos_id = _open_position(
        tmp_db, entry_price=100.0, stop_price=85.0, target_price=130.0,
        opened_at=old_opened,
    )

    # extensions_used starts at 0

    def _grant_extension(pos) -> bool:
        return True  # always extend

    actions = run_exit_executor(
        asset=_ASSET,
        current_price=105.0,
        coinbase_sell_fn=_no_sell,
        db_path=tmp_db,
        on_extension_review=_grant_extension,
    )

    assert len(actions) == 1
    assert actions[0].get("note") == "extension_granted"
    assert actions[0]["result"] is None

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT extensions_used FROM positions WHERE id=?", (pos_id,)
        ).fetchone()
    assert row["extensions_used"] == 1, "extensions_used must be incremented in ledger"


# ---------------------------------------------------------------------------
# 18. Schema V7 — fresh install has idx_one_active_exit_per_position
# ---------------------------------------------------------------------------

def test_fresh_schema_v7_has_exit_index(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_one_active_exit_per_position'"
        ).fetchone()
    assert idx is not None, (
        "idx_one_active_exit_per_position missing from fresh V7 schema"
    )


# ---------------------------------------------------------------------------
# 19. Schema V7 — partial unique index prevents two active EXITs per position
# ---------------------------------------------------------------------------

def test_partial_unique_exit_index_blocks_second_active_exit(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db)

    # Place first EXIT
    place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_ok_sell,
        db_path=tmp_db,
    )

    # Attempt raw INSERT of a second active EXIT — must fail with UNIQUE constraint
    with get_db(tmp_db) as conn:
        epoch_id = conn.execute("SELECT epoch_id FROM positions WHERE id=?", (pos_id,)).fetchone()[0]
    second_oid = _oid()
    with pytest.raises(sqlite3.IntegrityError, match="UNIQUE constraint"):
        with get_db(tmp_db) as conn:
            conn.execute("""
                INSERT INTO orders(
                    id, epoch_id, asset, side, order_type, purpose, position_id,
                    placed_at, qty_base_requested, reasoning, status
                ) VALUES (?,?,?,?,?,?,?,?,?,?,'OPEN')
            """, (second_oid, epoch_id, _ASSET, "SELL", "MARKET", "EXIT", pos_id,
                  _now(), 0.1, "duplicate exit"))


# ---------------------------------------------------------------------------
# 20. Migration V6 → V7: index added, data preserved, version = 7
# ---------------------------------------------------------------------------

def _make_v6_db(path: Path) -> None:
    """Build a V6 database (no idx_one_active_exit_per_position), stamp user_version=6."""
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
            rejection_reason TEXT, fills_finalized_at TEXT,
            CHECK((purpose = 'ENTRY') OR (purpose = 'EXIT' AND position_id IS NOT NULL))
        );
        CREATE UNIQUE INDEX idx_one_active_entry_per_asset
            ON orders(asset) WHERE purpose='ENTRY'
            AND status IN ('SUBMITTING','OPEN','PARTIAL');
        CREATE INDEX idx_unfinalized_terminal ON orders(id)
            WHERE status IN ('EXPIRED','CANCELLED')
            AND exchange_order_id IS NOT NULL
            AND fills_finalized_at IS NULL;
        CREATE TABLE fills (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            order_id TEXT NOT NULL REFERENCES orders(id),
            exchange_fill_id TEXT UNIQUE, fill_price REAL NOT NULL,
            fill_qty_base REAL NOT NULL, fill_qty_usd REAL NOT NULL,
            fee_usd REAL NOT NULL DEFAULT 0.0, is_taker INTEGER NOT NULL DEFAULT 1,
            filled_at TEXT NOT NULL
        );
        CREATE TABLE positions (
            id TEXT PRIMARY KEY, entry_order_id TEXT NOT NULL UNIQUE REFERENCES orders(id),
            epoch_id TEXT NOT NULL, asset TEXT NOT NULL,
            status TEXT NOT NULL DEFAULT 'OPEN'
        );
        CREATE TABLE trade_intents (
            order_id TEXT PRIMARY KEY REFERENCES orders(id),
            stop_price REAL NOT NULL, target_price REAL NOT NULL,
            recorded_at TEXT NOT NULL
        );
        CREATE UNIQUE INDEX idx_one_active_epoch ON risk_epochs(1) WHERE ended_at IS NULL;
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP_V6_MIG', 100.0, 'v6 migration test', '2025-01-01T00:00:00Z');
    """)
    conn.execute("PRAGMA user_version = 6")
    conn.commit()
    conn.close()


def test_migration_v6_to_v7_adds_exit_index(tmp_path: Path) -> None:
    db = tmp_path / "ledger_v6.db"
    _make_v6_db(db)

    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION, f"expected V{SCHEMA_VERSION}, got V{ver}"

        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_one_active_exit_per_position'"
        ).fetchone()
        assert idx is not None, "idx_one_active_exit_per_position not created by V6→V7 migration"

        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
        assert row[0] == "EP_V6_MIG", "pre-migration data must survive V6→V7"


def test_migration_v6_to_v7_idempotent(tmp_path: Path) -> None:
    db = tmp_path / "ledger_v6_partial.db"
    _make_v6_db(db)

    # Manually add the index to simulate a partial migration
    conn = sqlite3.connect(str(db))
    conn.execute("""
        CREATE UNIQUE INDEX idx_one_active_exit_per_position
            ON orders(position_id)
            WHERE purpose='EXIT' AND status IN ('SUBMITTING','OPEN','PARTIAL')
    """)
    conn.commit()
    conn.close()

    run_migrations(db)  # must not raise

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
    assert ver == SCHEMA_VERSION


def test_migration_v4_chains_through_to_v7(tmp_path: Path) -> None:
    """A V4 DB runs V4→V5→V6→V7 in a single run_migrations() call."""
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
        CREATE UNIQUE INDEX idx_one_active_epoch ON risk_epochs(1) WHERE ended_at IS NULL;
        INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)
            VALUES ('EP_V4', 100.0, 'chain test', '2025-01-01T00:00:00Z');
    """)
    conn.execute("PRAGMA user_version = 4")
    conn.commit()
    conn.close()

    run_migrations(db)

    with sqlite3.connect(str(db)) as conn:
        ver = conn.execute("PRAGMA user_version").fetchone()[0]
        assert ver == SCHEMA_VERSION, f"expected V{SCHEMA_VERSION}, got V{ver}"

        cols = {r[1] for r in conn.execute("PRAGMA table_info(orders)")}
        assert "rejection_reason" in cols, "rejection_reason missing after V4→V7 chain"
        assert "fills_finalized_at" in cols, "fills_finalized_at missing after V4→V7 chain"

        idx = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='index'"
            " AND name='idx_one_active_exit_per_position'"
        ).fetchone()
        assert idx is not None, "idx_one_active_exit_per_position missing after V4→V7 chain"

        row = conn.execute("SELECT epoch_id FROM risk_epochs").fetchone()
        assert row[0] == "EP_V4", "pre-migration data must survive V4→V7 chain"

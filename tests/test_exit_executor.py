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
import time
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
)
from pipeline.outbox import (
    CoinbaseRejected,
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


@pytest.fixture(autouse=True)
def _product_rules_in_cache():
    """
    Populate the ProductRules cache the way prewarm() does in production.

    run_exit_executor resolves rounding rules through
    product_state.get_rules_for_exit().  Without a populated cache every test
    would fall through to hardcoded defaults and trip the degraded-rules alert,
    which is correct production behaviour but is not what these tests exercise.
    The values match the old DRY_RUN product defaults so existing DUST
    expectations are unchanged.
    """
    from pipeline.product_state import ProductRules, _inject_cache

    _inject_cache(_ASSET, rules=ProductRules(
        product_id=_ASSET,
        base_increment="0.00000001",
        base_min_size="0.00000001",
        base_max_size="9000",
        quote_increment="0.01",
        fetched_wall=time.time(),
    ))
    yield


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


def _no_sell(order_id: str, asset: str, qty: str) -> str:
    raise AssertionError("coinbase_sell_fn must not be called in this test")


def _ok_sell(order_id: str, asset: str, qty: str) -> str:
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

    def _crash_after_check(order_id: str, asset: str, qty: str) -> str:
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

    def _reject(order_id: str, asset: str, qty: str) -> str:
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

    def _timeout(order_id: str, asset: str, qty: str) -> str:
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

    def _counting_sell(order_id: str, asset: str, qty: str) -> str:
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

    received_qty: list[str] = []

    def _capture_qty(order_id: str, asset: str, qty: str) -> str:
        received_qty.append(qty)
        return f"EX-{order_id[:8]}"

    place_exit_outbox(
        position_id=pos_id,
        exit_reason="TAKE_PROFIT",
        coinbase_sell_fn=_capture_qty,
        db_path=tmp_db,
    )

    assert len(received_qty) == 1
    assert abs(float(received_qty[0]) - 0.1) < 1e-9, (
        f"qty passed to sell_fn must equal position.qty_base_remaining; got {received_qty[0]}"
    )


# ---------------------------------------------------------------------------
# 10. run_exit_executor — STOP_LOSS condition triggers exit
# ---------------------------------------------------------------------------

def test_run_exit_executor_stop_loss_places_exit(tmp_db: Path) -> None:
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: str) -> str:
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

    def _sell(order_id: str, asset: str, qty: str) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    # First tick: places exit
    run_exit_executor(
        asset=_ASSET, current_price=85.0, coinbase_sell_fn=_sell, db_path=tmp_db
    )

    assert len(sell_calls) == 1

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


# ---------------------------------------------------------------------------
# 23. SELL rejection classification (coinbase_client)
# ---------------------------------------------------------------------------

def test_place_market_sell_raises_rejected_for_known_error_code() -> None:
    """
    place_market_sell() must parse Coinbase success=False response and raise
    CoinbaseOrderRejected (not RuntimeError) when the code is in _DEFINITE_REJECTION_CODES.
    Tests the actual response-parsing path in place_market_sell(), not just outbox wiring.
    """
    from unittest.mock import MagicMock, patch
    from exchange.coinbase_client import place_market_sell, CoinbaseOrderRejected

    fake_resp = {
        "success": False,
        "error_response": {"error": "INSUFFICIENT_FUND", "message": "not enough balance"},
    }
    mock_client = MagicMock()
    mock_client.create_order.return_value = fake_resp

    with (
        patch("exchange.coinbase_client._get_client", return_value=mock_client),
        patch("exchange.coinbase_client._DRY_RUN", False),
    ):
        with pytest.raises(CoinbaseOrderRejected, match="INSUFFICIENT_FUND"):
            place_market_sell(
                product_id=_ASSET,
                base_size_coins=0.1,
                client_order_id="test-oid-001",
            )


def test_place_market_sell_raises_runtime_for_ambiguous_code() -> None:
    """
    place_market_sell() must raise RuntimeError (not CoinbaseOrderRejected) for
    unrecognised error codes — this leaves the EXIT order SUBMITTING for the
    reconciler to resolve on next boot.
    """
    from unittest.mock import MagicMock, patch
    from exchange.coinbase_client import place_market_sell, CoinbaseOrderRejected

    fake_resp = {
        "success": False,
        "error_response": {"error": "UNKNOWN_FAILURE_REASON", "message": "unknown"},
    }
    mock_client = MagicMock()
    mock_client.create_order.return_value = fake_resp

    with (
        patch("exchange.coinbase_client._get_client", return_value=mock_client),
        patch("exchange.coinbase_client._DRY_RUN", False),
    ):
        with pytest.raises(RuntimeError, match="ambiguous"):
            place_market_sell(
                product_id=_ASSET,
                base_size_coins=0.1,
                client_order_id="test-oid-002",
            )
        # Must NOT raise CoinbaseOrderRejected — that would write REJECTED instead of SUBMITTING
        try:
            place_market_sell(
                product_id=_ASSET,
                base_size_coins=0.1,
                client_order_id="test-oid-003",
            )
        except CoinbaseOrderRejected:
            pytest.fail("ambiguous code must not raise CoinbaseOrderRejected")
        except RuntimeError:
            pass  # expected


def test_place_exit_outbox_definite_rejection_classified(tmp_db: Path) -> None:
    """
    place_market_sell returning success=False with a known error code must
    raise CoinbaseOrderRejected (definite), NOT RuntimeError (ambiguous).
    This ensures the EXIT order goes to REJECTED (not SUBMITTING).
    """

    _, pos_id = _open_position(tmp_db)

    # Simulates the Coinbase response success=False with a known rejection code
    # {"success": False, "error_response": {"error": "INSUFFICIENT_FUND", ...}},
    # which place_market_sell translates into CoinbaseOrderRejected.
    def _mock_sell(order_id: str, asset: str, qty: str) -> str:
        # Call the real place_market_sell logic by stubbing out create_order
        from exchange.coinbase_client import CoinbaseOrderRejected as _COR
        raise _COR("INSUFFICIENT_FUND: not enough balance")

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_mock_sell,
        db_path=tmp_db,
    )

    assert result.status == "REJECTED", (
        "definite Coinbase rejection must produce REJECTED, not SUBMITTING"
    )
    assert "INSUFFICIENT_FUND" in (result.rejection_reason or "")

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status, rejection_reason FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["status"] == "REJECTED"
    assert "INSUFFICIENT_FUND" in (row["rejection_reason"] or "")


def test_place_exit_outbox_ambiguous_error_stays_submitting(tmp_db: Path) -> None:
    """
    place_market_sell raising RuntimeError (ambiguous — network timeout, 5xx) must
    leave the EXIT order SUBMITTING, not REJECTED.
    """
    _, pos_id = _open_position(tmp_db)

    def _ambiguous_sell(order_id: str, asset: str, qty: str) -> str:
        raise RuntimeError("Coinbase rejected ZEC-USD SELL with ambiguous code 'UNKNOWN_FAILURE'")

    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_ambiguous_sell,
        db_path=tmp_db,
    )

    assert result.status == "SUBMITTING", (
        "ambiguous RuntimeError must leave order SUBMITTING for startup reconciler"
    )
    assert result.exchange_order_id is None


# ---------------------------------------------------------------------------
# 24. Never raises — global per-position guard
# ---------------------------------------------------------------------------

def test_run_exit_executor_never_raises_on_null_fields(tmp_db: Path) -> None:
    """
    NULL-valued numeric fields must be handled by fallback coercion, not crash.
    entry_price=NULL → falls back to current_price; stop_price=NULL → 0.0.
    No exception must propagate.
    """
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    with get_db(tmp_db) as conn:
        conn.execute("UPDATE positions SET entry_price=NULL WHERE id=?", (pos_id,))

    try:
        actions = run_exit_executor(
            asset=_ASSET,
            current_price=85.0,
            coinbase_sell_fn=_no_sell,
            db_path=tmp_db,
        )
    except Exception as exc:
        pytest.fail(f"run_exit_executor raised unexpectedly: {exc}")

    assert isinstance(actions, list)


def test_run_exit_executor_outer_guard_catches_unexpected_exception(tmp_db: Path) -> None:
    """
    An unexpected exception inside the position loop (not a NULL field, but e.g.
    an arithmetic error on corrupt data) must be caught by the per-position guard
    and returned as an error dict — never propagated.
    """
    from unittest.mock import patch

    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=200.0, target_price=300.0)

    # Inject an unexpected failure deep inside the per-position try block
    with patch("pipeline.exit_executor._compute_trailing_stop",
               side_effect=RuntimeError("unexpected corrupt data")):
        try:
            actions = run_exit_executor(
                asset=_ASSET,
                current_price=150.0,
                coinbase_sell_fn=_no_sell,
                db_path=tmp_db,
            )
        except Exception as exc:
            pytest.fail(f"run_exit_executor propagated inner exception: {exc}")

    assert len(actions) == 1
    err = actions[0].get("error", "")
    assert "unexpected_per_position_error" in err
    assert "unexpected corrupt data" in err
    assert actions[0]["result"] is None


def test_run_exit_executor_never_raises_on_db_read_failure(tmp_db: Path) -> None:
    """
    If the initial DB read raises (e.g., locked DB on first call), run_exit_executor
    must return an error list rather than propagating the exception.
    """
    bad_path = tmp_db.parent / "does_not_exist" / "ledger.db"

    try:
        actions = run_exit_executor(
            asset=_ASSET,
            current_price=100.0,
            coinbase_sell_fn=_no_sell,
            db_path=bad_path,
        )
    except Exception as exc:
        pytest.fail(f"run_exit_executor raised on bad db_path: {exc}")

    assert len(actions) == 1
    assert "db_read_positions_failed" in actions[0].get("error", "")


# ---------------------------------------------------------------------------
# 25. _skip_exit_check flag prevents double call
# ---------------------------------------------------------------------------

def test_exit_executor_idempotent_when_active_exit_exists(tmp_db: Path) -> None:
    """
    run_exit_executor() must not place a second SELL when an active EXIT
    (SUBMITTING/OPEN/PARTIAL) already exists for the position.
    """
    _, pos_id = _open_position(tmp_db, entry_price=100.0, stop_price=90.0, target_price=120.0)

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: str) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    # First pass: EXIT placed (price below stop)
    place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_sell,
        db_path=tmp_db,
    )
    assert len(sell_calls) == 1

    # Second call to run_exit_executor must detect active EXIT and skip
    actions = run_exit_executor(
        asset=_ASSET,
        current_price=85.0,
        coinbase_sell_fn=_sell,
        db_path=tmp_db,
    )
    assert len(sell_calls) == 1, "sell_fn must not be called again when active EXIT exists"
    skipped = [a for a in actions if a.get("note") == "active_exit_already_exists"]
    assert len(skipped) == 1


# ---------------------------------------------------------------------------
# 26. DUST: TX-A commits position→DUST, no EXIT order is created
# ---------------------------------------------------------------------------

def test_dust_transition_committed_no_sell_order(tmp_db: Path) -> None:
    """
    place_exit_outbox with base_min_size > qty_base_remaining must:
      1. Commit DUST transition inside TX-A (position status → DUST).
      2. Raise PlacementBlocked with message starting 'DUST:'.
      3. Leave zero EXIT orders in the DB (coinbase_sell_fn is never called).
    """
    _, pos_id = _open_position(tmp_db, qty_base=0.0001)  # 0.0001 < base_min_size=0.001

    with pytest.raises(PlacementBlocked, match="^DUST:"):
        place_exit_outbox(
            position_id=pos_id,
            exit_reason="STOP_LOSS",
            coinbase_sell_fn=_no_sell,
            db_path=tmp_db,
            base_increment="0.00000001",
            base_min_size="0.001",
        )

    with get_db(tmp_db) as conn:
        pos_status = conn.execute(
            "SELECT status FROM positions WHERE id=?", (pos_id,)
        ).fetchone()["status"]
        exit_order_count = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE position_id=? AND purpose='EXIT'", (pos_id,)
        ).fetchone()[0]

    assert pos_status == "DUST", "Position must be DUST after sub-minimum qty detected"
    assert exit_order_count == 0, "No EXIT order must exist when DUST transition fires"


# ---------------------------------------------------------------------------
# 27. DUST position blocks new ENTRY (outbox gate)
# ---------------------------------------------------------------------------

def test_entry_blocked_when_dust_position_exists(tmp_db: Path) -> None:
    """place_order_outbox raises PlacementBlocked when a DUST position exists for the asset."""
    from pipeline.outbox import place_order_outbox

    _, pos_id = _open_position(tmp_db, qty_base=0.0001)
    with get_db(tmp_db) as conn:
        conn.execute("UPDATE positions SET status='DUST' WHERE id=?", (pos_id,))

    with pytest.raises(PlacementBlocked, match="active/dust"):
        place_order_outbox(
            asset=_ASSET,
            limit_price=100.0,
            qty_usd=10.0,
            stop_price=90.0,
            target_price=115.0,
            coinbase_fn=lambda cid: f"CB-{cid[:8]}",
            db_path=tmp_db,
            gate_freshness_minutes=None,
        )


# ---------------------------------------------------------------------------
# 28. DUST condition sends Telegram CRITICAL alert from run_exit_executor
# ---------------------------------------------------------------------------

def test_dust_sends_telegram_critical_alert(tmp_db: Path) -> None:
    """
    run_exit_executor must call send_telegram_message with a CRITICAL alert when
    place_exit_outbox raises a 'DUST:' PlacementBlocked.
    The inner patch on send_telegram_message prevents the autouse urlopen guard from
    seeing any call (conftest assertion stays green).
    """
    from unittest.mock import patch

    _, pos_id = _open_position(tmp_db, qty_base=0.0001)

    # Exchange minimum of 0.001 makes the 0.0001 position DUST.  Rules come from
    # the ProductRules cache, exactly as in production after prewarm().
    from pipeline.product_state import ProductRules, _inject_cache
    _inject_cache(_ASSET, rules=ProductRules(
        product_id=_ASSET, base_increment="0.00000001", base_min_size="0.001",
        base_max_size="9000", quote_increment="0.01", fetched_wall=time.time(),
    ))

    with patch("notifications.telegram.send_telegram_message") as mock_alert:
        actions = run_exit_executor(
            asset=_ASSET,
            current_price=85.0,   # below stop_price=90.0 → STOP_LOSS trigger
            coinbase_sell_fn=_no_sell,
            db_path=tmp_db,
        )

    assert mock_alert.called, "send_telegram_message must be called on DUST detection"
    alert_text: str = mock_alert.call_args[0][0]
    assert "CRITICAL" in alert_text
    assert "DUST" in alert_text

    assert len(actions) == 1
    assert "placement_blocked" in (actions[0].get("note") or "")


# ---------------------------------------------------------------------------
# 29. CLOSING position forces CONTINUE_EXIT regardless of price
# ---------------------------------------------------------------------------

def test_closing_position_exits_even_when_price_is_safe(tmp_db: Path) -> None:
    """
    A CLOSING position must always get an EXIT placed, even when the current price
    is safely above the stop-loss and below the take-profit.  CLOSING signals that
    the position is mid-exit (partial fill or DUST_REVIVED), so the remainder must
    be sold unconditionally rather than waiting for another stop/target trigger.
    """
    _, pos_id = _open_position(
        tmp_db, entry_price=100.0, stop_price=90.0, target_price=130.0, qty_base=1.0
    )
    # Mark position CLOSING (simulates a partial fill having occurred)
    with get_db(tmp_db) as conn:
        conn.execute("UPDATE positions SET status='CLOSING' WHERE id=?", (pos_id,))

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: str) -> str:
        sell_calls.append(order_id)
        return f"EX-{order_id[:8]}"

    # Price is safe (above stop, below target) — an OPEN position would not exit
    actions = run_exit_executor(
        asset=_ASSET,
        current_price=105.0,   # between stop=90 and target=130 → OPEN would skip
        coinbase_sell_fn=_sell,
        db_path=tmp_db,
    )

    assert len(sell_calls) == 1, (
        "CLOSING position must place EXIT even when price is in the safe zone"
    )
    assert len(actions) == 1
    assert actions[0]["exit_reason"] == "CONTINUE_EXIT"
    assert actions[0]["result"] is not None
    assert actions[0]["result"].status == "OPEN"


# ---------------------------------------------------------------------------
# 30. Wire qty: coinbase_sell_fn receives pre-formatted Decimal string
# ---------------------------------------------------------------------------

def test_sell_fn_receives_decimal_string_not_float(tmp_db: Path) -> None:
    """
    place_exit_outbox must pass a pre-formatted Decimal string to coinbase_sell_fn,
    not a float.  The string must be the exact ROUND_DOWN result so Coinbase sees
    a valid quantity that was never rounded up.
    """
    from decimal import Decimal

    _, pos_id = _open_position(tmp_db, qty_base=0.99999999)

    wire_args: list[str] = []

    def _capture(order_id: str, asset: str, qty: str) -> str:
        wire_args.append(qty)
        return f"EX-{order_id[:8]}"

    place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=_capture,
        db_path=tmp_db,
        base_increment="0.00000001",
        base_min_size="0.001",
    )

    assert len(wire_args) == 1
    wire_qty = wire_args[0]
    assert isinstance(wire_qty, str), "wire qty must be a string, not float"
    qty_d = Decimal(wire_qty)
    assert qty_d <= Decimal("0.99999999"), "wire qty must not exceed input"
    assert qty_d == Decimal("0.99999999"), "exact ROUND_DOWN value must be preserved"


def test_run_pipeline_skip_exit_check_does_not_call_check_open_positions() -> None:
    """
    run_pipeline(..., _skip_exit_check=True) must not call _check_open_positions.
    This is the flag run_all_assets() sets after the pre-gate EXIT pass so that
    positions with exit conditions aren't evaluated twice per tick.
    """
    from pipeline.runner import run_pipeline
    from unittest.mock import patch

    check_calls: list[str] = []

    with (
        patch("pipeline.runner.get_snapshot", return_value={"close": 2000.0}),
        patch("pipeline.runner._check_pending_fills"),
        patch("pipeline.runner._check_open_positions",
              side_effect=lambda a, p: check_calls.append(a)),
        patch("pipeline.runner.scan_latest", return_value=None),
        patch("pipeline.runner._log_decision"),
        patch("pipeline.runner._print_decision"),
    ):
        run_pipeline(_ASSET, _skip_exit_check=True)

    assert check_calls == [], (
        "_check_open_positions must not be called when _skip_exit_check=True"
    )

# ---------------------------------------------------------------------------
# 30. P0 regression — EXIT must never be skipped because product metadata failed
# ---------------------------------------------------------------------------
# The original defect: run_exit_executor called
# exchange.coinbase_client.get_product_info(asset) directly and, on any
# exception, appended "product_info_fetch_failed" and `continue`d — so a
# transient Coinbase outage silently skipped a triggered stop-loss until the
# next scheduler tick, roughly 60 minutes later.  Rules now resolve through
# product_state.get_rules_for_exit(), which never raises.


def _stop_loss_position(tmp_db: Path):
    """OPEN position with stop_price=90 — trigger by passing current_price<90."""
    return _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)


def test_exit_proceeds_when_coinbase_product_metadata_is_unreachable(tmp_db: Path) -> None:
    """Total Coinbase metadata outage: the SELL must still be placed."""
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _, pos_id = _stop_loss_position(tmp_db)
    _clear_cache()   # no cache, no LKG — worst case

    sell_calls: list[str] = []

    def _sell(order_id: str, asset: str, qty: str) -> str:
        sell_calls.append(qty)
        return f"EX-{order_id[:8]}"

    def _boom(*_a, **_kw):
        raise RuntimeError("coinbase unreachable")

    with patch("notifications.telegram.send_telegram_message"), \
         patch("exchange.coinbase_client._get_client", side_effect=_boom):
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=_sell, db_path=tmp_db,
        )

    assert len(sell_calls) == 1, "EXIT must be placed despite metadata failure"
    assert actions[0]["exit_reason"] == "STOP_LOSS"
    assert actions[0]["result"] is not None
    assert "error" not in actions[0]


def test_exit_never_reports_product_info_fetch_failed(tmp_db: Path) -> None:
    """The old failure mode must be gone, not merely unlikely."""
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _stop_loss_position(tmp_db)
    _clear_cache()

    with patch("notifications.telegram.send_telegram_message"), \
         patch("exchange.coinbase_client._get_client",
               side_effect=RuntimeError("coinbase unreachable")):
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db,
        )

    assert not any("product_info_fetch_failed" in str(a.get("error")) for a in actions)


def test_exit_executor_does_not_import_get_product_info() -> None:
    """
    Source-level lock on the wiring itself.

    get_rules_for_exit() existed before this fix but nothing in the production
    EXIT path called it.  A unit test on behaviour alone would not catch someone
    reintroducing the direct call as a "fallback", so assert on the source.
    """
    import pipeline.exit_executor as _mod

    source = Path(_mod.__file__).read_text(encoding="utf-8")
    code_lines = [
        ln for ln in source.splitlines()
        if not ln.lstrip().startswith("#")
    ]
    code = "\n".join(code_lines)
    assert "get_product_info" not in code, (
        "exit_executor must resolve rules via product_state.get_rules_for_exit(); "
        "a direct get_product_info() call reintroduces the P0 EXIT-skip defect"
    )
    assert "get_rules_for_exit" in code


def test_exit_uses_lkg_rules_when_cache_is_empty(tmp_db: Path, tmp_path: Path, monkeypatch) -> None:
    """Cache cleared but LKG on disk → LKG increment is what reaches the wire."""
    from unittest.mock import patch
    import pipeline.product_state as _ps

    monkeypatch.setattr(_ps, "LKG_PATH", tmp_path / "product_lkg.json")
    _ps._clear_cache()
    _ps._save_lkg(
        _ps.ProductRules(
            product_id=_ASSET, base_increment="0.01", base_min_size="0.01",
            base_max_size="9000", quote_increment="0.01", fetched_wall=time.time(),
        ),
        _ps.ProductState(
            product_id=_ASSET, is_disabled=False, trading_disabled=False,
            cancel_only=False, limit_only=False, post_only=False,
            auction_mode=False, view_only=False,
            fetched_wall=time.time(), fetched_mono=time.monotonic(),
        ),
    )

    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.237)

    wire_qty: list[str] = []

    def _sell(order_id: str, asset: str, qty: str) -> str:
        wire_qty.append(qty)
        return f"EX-{order_id[:8]}"

    with patch("notifications.telegram.send_telegram_message"):
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=_sell, db_path=tmp_db)

    assert wire_qty == ["1.23"], f"expected LKG 0.01 rounding, got {wire_qty}"


def test_exit_on_defaults_alerts_once_per_asset(tmp_db: Path) -> None:
    """Two degraded positions in one pass produce exactly one aggregated alert."""
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=2.0)

    with patch("notifications.telegram.send_telegram_message") as mock_alert:
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db,
        )

    assert len(actions) == 2
    assert all(a["result"] is not None for a in actions), "both EXITs must be placed"
    assert mock_alert.call_count == 1, (
        f"degraded-rules alert must aggregate per asset, got {mock_alert.call_count}"
    )
    body = mock_alert.call_args[0][0]
    assert "DEGRADED" in body
    assert "positions=2" in body, "aggregated alert should name the position count"


def test_exit_on_fresh_cache_does_not_alert(tmp_db: Path) -> None:
    """Normal path — cached rules, no alert."""
    from unittest.mock import patch

    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)

    with patch("notifications.telegram.send_telegram_message") as mock_alert:
        run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db,
        )

    assert not mock_alert.called


# ---------------------------------------------------------------------------
# 37. Degraded-rules alert must never sit in front of a risk-reducing SELL
# ---------------------------------------------------------------------------

def test_degraded_alert_is_sent_after_all_sells(tmp_db: Path) -> None:
    """
    The alert (a potentially blocking network call) must fire only after every
    SELL for the asset is on the wire — never inline before place_exit_outbox.
    """
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=2.0)

    events: list[str] = []

    def _sell(oid, a, q):
        events.append("SELL")
        return f"EX-{oid[:8]}"

    def _send(_msg):
        events.append("ALERT")
        return True

    with patch("notifications.telegram.send_telegram_message", side_effect=_send):
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=_sell, db_path=tmp_db)

    assert events.count("SELL") == 2
    assert events.count("ALERT") == 1
    # Every SELL comes before the single ALERT — the send never delays a SELL.
    assert events.index("ALERT") == len(events) - 1
    assert events[:2] == ["SELL", "SELL"]


def test_failed_send_no_same_pass_storm(tmp_db: Path) -> None:
    """
    Several degraded positions with a failing Telegram must still trigger only
    ONE send attempt in the pass — aggregation prevents each position from
    serially eating a network timeout.
    """
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()
    for qty in (1.0, 2.0, 3.0):
        _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=qty)

    with patch("notifications.telegram.send_telegram_message", return_value=False) as m:
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db,
        )

    assert len([a for a in actions if a["result"] is not None]) == 3
    assert m.call_count == 1, "aggregation must collapse the pass to one send attempt"


def test_failed_send_uses_short_retry_cooldown(tmp_db: Path) -> None:
    """
    A failed send arms only the SHORT retry cooldown: suppressed within 60s,
    re-sent once the retry window elapses.  A delivered alert would instead arm
    the 1h window.
    """
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)

    # Pass 1 — send fails → short retry cooldown armed.
    with patch("notifications.telegram.send_telegram_message", return_value=False) as m1:
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db)
    assert m1.call_count == 1
    assert _ASSET in _ee._rules_alert_until

    # A second position in the SAME window must be suppressed (no storm).
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=2.0)
    with patch("notifications.telegram.send_telegram_message", return_value=True) as m2:
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db)
    assert m2.call_count == 0, "within the retry window the alert must be suppressed"

    # Simulate the retry window elapsing → the alert is delivered on the next tick.
    _ee._rules_alert_until[_ASSET] -= (_ee._RULES_ALERT_RETRY_COOLDOWN_S + 1)
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=3.0)
    with patch("notifications.telegram.send_telegram_message", return_value=True) as m3:
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db)
    assert m3.call_count == 1, "after the retry window the alert must be re-sent"


def test_delivered_alert_uses_long_cooldown(tmp_db: Path) -> None:
    """A delivered alert arms the 1h window: the next pass stays quiet."""
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)

    with patch("notifications.telegram.send_telegram_message", return_value=True) as m1:
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db)
    assert m1.call_count == 1
    armed = _ee._rules_alert_until[_ASSET]

    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=2.0)
    with patch("notifications.telegram.send_telegram_message", return_value=True) as m2:
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=lambda oid, a, q: f"EX-{oid[:8]}", db_path=tmp_db)
    assert m2.call_count == 0, "1h cooldown must suppress the next pass"
    # Still short of the retry-only window: confirms the LONG cooldown was armed.
    assert armed - _ee._RULES_ALERT_RETRY_COOLDOWN_S > 0


# ---------------------------------------------------------------------------
# 38. Degraded alert must report the real outcome, not assume "placed"
# ---------------------------------------------------------------------------

def test_degraded_alert_reports_rejected_outcome(tmp_db: Path) -> None:
    """
    A degraded EXIT that Coinbase REJECTS must not be reported as placed.  The
    aggregated alert must name the REJECTED outcome and must not claim the SELL
    landed — a false stop-loss confirmation is dangerous for LIVE.
    """
    from unittest.mock import patch
    from pipeline.product_state import _clear_cache

    _clear_cache()   # defaults -> degraded path
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)

    def _reject(order_id: str, asset: str, qty: str) -> str:
        raise CoinbaseRejected("INSUFFICIENT_FUND")

    with patch("notifications.telegram.send_telegram_message") as mock_alert:
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=_reject, db_path=tmp_db,
        )

    assert actions[0]["result"].status == "REJECTED"
    assert mock_alert.call_count == 1
    body = mock_alert.call_args[0][0]
    assert "REJECTED=1" in body, "alert must report the real REJECTED outcome"
    assert "were still placed" not in body
    assert "proceeded anyway" not in body


def test_degraded_alert_reports_dust_outcome(tmp_db: Path) -> None:
    """A degraded EXIT that goes DUST is reported as DUST, not placed.

    get_rules_for_exit is patched to return stale rules whose min (0.001) dusts
    the 0.0001 position — combining the degraded-rules path with a DUST outcome.
    """
    from unittest.mock import patch

    degraded_dusting = {
        "base_increment": "0.00000001", "base_min_size": "0.001",
        "source": "lkg_stale", "stale": True, "age_s": 999999.0,
    }
    with patch("pipeline.exit_executor.get_rules_for_exit", return_value=degraded_dusting), \
         patch("notifications.telegram.send_telegram_message") as mock_alert:
        _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=0.0001)
        actions = run_exit_executor(
            asset=_ASSET, current_price=85.0,
            coinbase_sell_fn=_no_sell, db_path=tmp_db,
        )

    assert "placement_blocked" in (actions[0].get("note") or "")
    bodies = [c.args[0] for c in mock_alert.call_args_list]
    degraded = [b for b in bodies if "DEGRADED product rules" in b]
    assert degraded, "degraded alert must be sent"
    assert "DUST=1" in degraded[0], "degraded alert must report the DUST outcome"
    assert "were still placed" not in degraded[0]


def test_dust_alert_does_not_delay_later_sell(tmp_db: Path) -> None:
    """
    A DUST position early in the pass must not delay a later position's SELL:
    every SELL attempt must happen before any Telegram send.
    """
    from unittest.mock import patch
    from pipeline.product_state import ProductRules, _inject_cache

    # Rules with min 0.001: the 0.0001 position dusts, the 1.0 position sells.
    _inject_cache(_ASSET, rules=ProductRules(
        product_id=_ASSET, base_increment="0.00000001", base_min_size="0.001",
        base_max_size="9000", quote_increment="0.01", fetched_wall=time.time(),
    ))
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=0.0001)  # DUST
    _open_position(tmp_db, entry_price=100.0, stop_price=90.0, qty_base=1.0)     # sells

    events: list[str] = []

    def _sell(oid, a, q):
        events.append("SELL")
        return f"EX-{oid[:8]}"

    def _send(_msg):
        events.append("TELEGRAM")
        return True

    with patch("notifications.telegram.send_telegram_message", side_effect=_send):
        run_exit_executor(asset=_ASSET, current_price=85.0,
                          coinbase_sell_fn=_sell, db_path=tmp_db)

    assert "SELL" in events, "the non-dust position must be sold"
    # No TELEGRAM may appear before the SELL.
    first_telegram = events.index("TELEGRAM") if "TELEGRAM" in events else len(events)
    assert events.index("SELL") < first_telegram, (
        "the SELL must be placed before any Telegram send"
    )


# ---------------------------------------------------------------------------
# 39. DUST alert must be delivered at-least-once — retry until it lands
# ---------------------------------------------------------------------------

def _dust_event_id(db_path: Path, pos_id: str) -> int:
    from pipeline.ledger import get_db
    with get_db(db_path) as conn:
        row = conn.execute(
            "SELECT id FROM position_events WHERE position_id=? AND event_type='DUST_SETTLED'"
            " ORDER BY id DESC LIMIT 1", (pos_id,),
        ).fetchone()
    return row["id"]


def test_dust_alert_retries_until_delivered(tmp_db: Path) -> None:
    """
    A DUST alert that fails to send must be re-sent on a later pass, even with no
    new DUST event.  The intra-pass retry cooldown is bypassed here to simulate
    the next scheduler pass.
    """
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.ledger import get_db, transition_position_to_dust

    _, pos_id = _open_position(tmp_db, qty_base=1.0)
    with get_db(tmp_db) as conn:
        transition_position_to_dust(pos_id, conn)
    eid = _dust_event_id(tmp_db, pos_id)

    # Pass 1 — transport fails; must NOT mark delivered, but arms the cooldown.
    with patch("notifications.telegram.send_telegram_message", return_value=False) as m1:
        _ee.sweep_dust_alerts(db_path=tmp_db)
    assert m1.call_count == 1
    assert eid not in _ee._dust_delivered
    assert eid in _ee._dust_retry_until

    # Simulate the next scheduler pass by expiring the retry cooldown.
    _ee._dust_retry_until[eid] -= (_ee._DUST_RETRY_COOLDOWN_S + 1)

    with patch("notifications.telegram.send_telegram_message", return_value=True) as m2:
        _ee.sweep_dust_alerts(db_path=tmp_db)
    assert m2.call_count == 1, "a lost DUST alert must be retried on a later pass"
    assert eid in _ee._dust_delivered

    # Already delivered → stay quiet.
    with patch("notifications.telegram.send_telegram_message", return_value=True) as m3:
        _ee.sweep_dust_alerts(db_path=tmp_db)
    assert m3.call_count == 0, "a delivered DUST alert must not repeat"


def test_dust_alert_not_retried_within_same_pass(tmp_db: Path) -> None:
    """
    P2: a failed send must not be retried within the same pass.  The per-asset
    sweep and the global Step 3b sweep run back-to-back; a network timeout must
    cost at most one stall, not two.
    """
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.ledger import get_db, transition_position_to_dust

    _, pos_id = _open_position(tmp_db, qty_base=1.0)
    with get_db(tmp_db) as conn:
        transition_position_to_dust(pos_id, conn)

    with patch("notifications.telegram.send_telegram_message", return_value=False) as m:
        _ee.sweep_dust_alerts(db_path=tmp_db, asset=_ASSET)  # per-asset sweep
        _ee.sweep_dust_alerts(db_path=tmp_db)                # global Step 3b sweep
    assert m.call_count == 1, "the same pass must not retry (and stall) twice"


def test_dust_alert_fires_again_after_revive_then_redust(tmp_db: Path) -> None:
    """
    P1: DUST → CLOSING (DUST_REVIVED) → DUST must produce a SECOND alert.
    Delivery is keyed by the DUST_SETTLED event id, so the re-dusted exposure —
    a genuinely new manual-action item — is not suppressed as already delivered.
    """
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.ledger import get_db, transition_position_to_dust

    _, pos_id = _open_position(tmp_db, qty_base=1.0)

    # First DUST episode → delivered.
    with get_db(tmp_db) as conn:
        transition_position_to_dust(pos_id, conn)
    eid1 = _dust_event_id(tmp_db, pos_id)
    with patch("notifications.telegram.send_telegram_message", return_value=True) as m1:
        _ee.sweep_dust_alerts(db_path=tmp_db)
    assert m1.call_count == 1
    assert eid1 in _ee._dust_delivered

    # Revive to CLOSING, then dust again → a NEW DUST_SETTLED event id.
    with get_db(tmp_db) as conn:
        conn.execute("UPDATE positions SET status='CLOSING' WHERE id=?", (pos_id,))
        conn.execute(
            "INSERT INTO position_events(position_id, event_type, payload, occurred_at)"
            " VALUES (?, 'DUST_REVIVED', '{}', ?)",
            (pos_id, "2026-07-25T00:00:00+00:00"),
        )
    with get_db(tmp_db) as conn:
        transition_position_to_dust(pos_id, conn)   # OPEN/CLOSING → DUST again
    eid2 = _dust_event_id(tmp_db, pos_id)
    assert eid2 != eid1, "a re-dust must create a new DUST_SETTLED event"

    with patch("notifications.telegram.send_telegram_message", return_value=True) as m2:
        _ee.sweep_dust_alerts(db_path=tmp_db)
    assert m2.call_count == 1, "the re-dusted exposure must alert again"
    assert eid2 in _ee._dust_delivered


def test_dust_sweep_reports_residual_qty(tmp_db: Path) -> None:
    """The DUST alert names the position and its residual quantity."""
    import pipeline.exit_executor as _ee
    from unittest.mock import patch
    from pipeline.ledger import get_db, transition_position_to_dust

    _, pos_id = _open_position(tmp_db, qty_base=1.0)
    with get_db(tmp_db) as conn:
        transition_position_to_dust(pos_id, conn)

    with patch("notifications.telegram.send_telegram_message", return_value=True) as m:
        _ee.sweep_dust_alerts(db_path=tmp_db, asset=_ASSET)

    body = m.call_args[0][0]
    assert "CRITICAL" in body and "DUST" in body
    assert pos_id in body


def test_run_all_assets_dust_sweep_one_send_per_pass(tmp_db: Path, monkeypatch) -> None:
    """
    Integration: the position is OPEN and DUSTS DURING the per-asset EXIT path,
    so BOTH the per-asset sweep (end of run_exit_executor) and the global Step 3b
    sweep run in the same pass.  Despite that, exactly one DUST alert is sent for
    the event; a failed send is not retried until a later pass, after the cooldown.
    """
    import pipeline.ledger as _ledger
    import pipeline.exit_executor as _ee
    from pipeline.product_state import ProductRules, _inject_cache
    from pipeline.preflight import _dry_run_result
    from pipeline.runner import run_all_assets
    from unittest.mock import patch

    # OPEN position whose 0.0001 qty rounds below base_min_size 0.001 → it dusts
    # the first time the EXIT executor tries to place the stop-loss SELL.
    _, pos_id = _open_position(
        tmp_db, entry_price=100.0, stop_price=90.0, qty_base=0.0001
    )
    _inject_cache(_ASSET, rules=ProductRules(
        product_id=_ASSET, base_increment="0.00000001", base_min_size="0.001",
        base_max_size="9000", quote_increment="0.01", fetched_wall=time.time(),
    ))
    monkeypatch.setattr(_ledger, "DB_PATH", tmp_db)

    with (
        patch("pipeline.runner._startup_reconciliation", return_value=(True, None)),
        patch("pipeline.runner.get_snapshot", return_value={"close": 85.0}),  # < stop → STOP_LOSS
        patch("exchange.coinbase_client.is_dry_run", return_value=True),
        patch("pipeline.preflight.run_preflight",
              return_value=_dry_run_result(["ETH-USD", "ZEC-USD"])),
        patch("pipeline.runner.run_pipeline"),
        patch("pipeline.runner.send_telegram_message") as runner_send,
        patch("notifications.telegram.send_telegram_message", return_value=False) as dust_send,
    ):
        # Pass 1 — position is OPEN, so the per-asset EXIT path runs, dusts the
        # position, and its sweep fires; Step 3b then sees the same event under
        # cooldown and does NOT re-send.
        run_all_assets()
        assert dust_send.call_count == 1, (
            "per-asset + Step 3b sweeps must not double-send within one pass"
        )
        # Confirm the position actually dusted via the EXIT path (not pre-marked).
        with _ledger.get_db(tmp_db) as conn:
            status = conn.execute(
                "SELECT status FROM positions WHERE id=?", (pos_id,)
            ).fetchone()["status"]
        assert status == "DUST", "position must have dusted during the EXIT path"
        eid = _dust_event_id(tmp_db, pos_id)

        # Pass 2 — still within the retry cooldown → no re-send.
        run_all_assets()
        assert dust_send.call_count == 1, "must not retry within the cooldown"

        # A later pass, after the cooldown expires → retried and delivered.
        _ee._dust_retry_until[eid] -= (_ee._DUST_RETRY_COOLDOWN_S + 1)
        dust_send.return_value = True
        run_all_assets()
        assert dust_send.call_count == 2, "after the cooldown the alert is retried"

    assert runner_send.call_count == 0, "no unrelated runner alert should fire in this scenario"

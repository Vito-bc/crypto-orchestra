"""
End-to-end EXIT lifecycle integration tests.

Covers the full chain without any mocks of the state machine:
    ENTRY fill → OPEN position
    EXIT executor → first SELL (full qty), stop-loss triggered
    Active EXIT blocks duplicate SELL (idempotent tick)
    Reconciliation → partial fill (40%) + CANCELLED → CLOSING (60% remaining)
    EXIT executor → second SELL for exactly 60% (qty_base_remaining)
    Second fill → position CLOSED
    P&L / fees verified against ledger formula
    Idempotent fill replay: same exchange_fill_id → replayed=True, state unchanged
    EXIT executor on CLOSED position: no sells placed
    Epoch closed-P&L matches position P&L

Crash variant:
    TX-A SUBMITTING EXIT committed → crash before TX-B.
    On restart, reconciliation finds the EXIT on Coinbase via list_orders_fn
    (matched by client_order_id) and resolves SUBMITTING → OPEN without
    placing a duplicate SELL.
"""

from __future__ import annotations

import uuid
from datetime import datetime, timezone
from pathlib import Path

import pytest

from pipeline.exit_executor import run_exit_executor
from pipeline.ledger import (
    apply_fill,
    get_db,
    get_epoch_closed_pnl,
    insert_epoch,
    insert_order,
    insert_trade_intent,
    run_migrations,
    transition_order,
)
from pipeline.reconciler import (
    CoinbaseFill,
    CoinbaseOrder,
    run_startup_reconciliation,
)

# ---------------------------------------------------------------------------
# Test constants
# ---------------------------------------------------------------------------

ASSET = "ZEC-USD"
EPOCH_ID = "EP-E2E"

ENTRY_PRICE = 100.0
ENTRY_QTY = 1.0
ENTRY_FEE = 0.50
STOP_PRICE = 90.0     # fires when current_price <= STOP_PRICE
TARGET_PRICE = 150.0
TRIGGER_PRICE = 80.0  # below STOP_PRICE → STOP_LOSS condition

# First EXIT: partial fill then CANCELLED
PARTIAL_FILL_ID = "F-PARTIAL-001"
PARTIAL_FILL_PRICE = 110.0
PARTIAL_FILL_QTY = 0.4       # 40% of 1.0 ZEC
PARTIAL_FILL_FEE = 0.20

# Second EXIT: fills remaining qty exactly
SECOND_FILL_ID = "F-SECOND-001"
SECOND_FILL_PRICE = 120.0
SECOND_FILL_QTY = ENTRY_QTY - PARTIAL_FILL_QTY  # 0.6 ZEC
SECOND_FILL_FEE = 0.30

EX_ID_1 = "EX-LIFECYCLE-001"  # exchange_order_id for first EXIT
EX_ID_2 = "EX-LIFECYCLE-002"  # exchange_order_id for second EXIT

# Expected P&L (hand-computed):
#   exit_vwap = (110*0.4 + 120*0.6) / 1.0 = 116.0
#   total_exit_fee = 0.20 + 0.30 = 0.50
#   pnl_usd = (116 - 100) * 1.0 - 0.50 - 0.50 = 15.0
#   pnl_pct = 15.0 / 100.0 * 100 = 15.0 %
EXPECTED_EXIT_VWAP = (
    PARTIAL_FILL_PRICE * PARTIAL_FILL_QTY + SECOND_FILL_PRICE * SECOND_FILL_QTY
) / ENTRY_QTY
EXPECTED_EXIT_FEE = PARTIAL_FILL_FEE + SECOND_FILL_FEE
EXPECTED_PNL_USD = (EXPECTED_EXIT_VWAP - ENTRY_PRICE) * ENTRY_QTY - EXPECTED_EXIT_FEE - ENTRY_FEE
EXPECTED_PNL_PCT = EXPECTED_PNL_USD / (ENTRY_PRICE * ENTRY_QTY) * 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _oid() -> str:
    return str(uuid.uuid4())


def _setup_open_position(db: Path) -> tuple[str, str]:
    """
    Insert epoch (if absent), ENTRY order, trade intent, fill.
    Returns (entry_order_id, position_id).
    """
    with get_db(db) as conn:
        if not conn.execute(
            "SELECT 1 FROM risk_epochs WHERE epoch_id=?", (EPOCH_ID,)
        ).fetchone():
            insert_epoch(EPOCH_ID, 1000.0, "e2e test", conn=conn)

    entry_oid = _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=entry_oid, epoch_id=EPOCH_ID, asset=ASSET,
            side="BUY", order_type="MARKET", purpose="ENTRY",
            placed_at=_now(), qty_base_requested=ENTRY_QTY, conn=conn,
        )
        insert_trade_intent(
            entry_oid, stop_price=STOP_PRICE, target_price=TARGET_PRICE, conn=conn
        )
        transition_order(entry_oid, "OPEN", conn=conn)
    with get_db(db) as conn:
        r = apply_fill(
            order_id=entry_oid, fill_price=ENTRY_PRICE, fill_qty_base=ENTRY_QTY,
            fee_usd=ENTRY_FEE, exchange_fill_id="F-ENTRY-001", conn=conn,
        )
    return entry_oid, r["position_id"]


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ledger.db"
    run_migrations(db)
    return db


# ---------------------------------------------------------------------------
# Main lifecycle test
# ---------------------------------------------------------------------------

def test_e2e_exit_lifecycle(tmp_db: Path) -> None:
    """
    Full EXIT lifecycle: partial fill → restart → remaining qty SELL → CLOSED → P&L.

    Steps:
      1. ENTRY fill → OPEN position.
      2. EXIT executor (stop-loss) → first SELL for full qty (OPEN).
      3. Active EXIT blocks duplicate SELL.
      4. Reconciliation: first EXIT partially filled (40%) then CANCELLED
         → position CLOSING, remaining = 60%.
      5. EXIT executor → second SELL for exactly 60%.
      6. Second fill → position CLOSED.
      7. P&L = (exit_vwap - entry_price) * qty - all fees.
      8. Idempotent fill replay → replayed=True, state unchanged.
      9. EXIT executor on CLOSED position → no sells.
     10. Epoch closed-P&L matches position P&L.
    """
    db = tmp_db

    # ── Step 1: ENTRY fill → OPEN position ───────────────────────────────────
    _, pos_id = _setup_open_position(db)

    with get_db(db) as conn:
        pos = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos["status"] == "OPEN"
    assert pos["qty_base_remaining"] == pytest.approx(ENTRY_QTY)
    assert pos["entry_fee_usd"] == pytest.approx(ENTRY_FEE)
    assert pos["entry_price"] == pytest.approx(ENTRY_PRICE)
    assert pos["stop_price"] == pytest.approx(STOP_PRICE)

    # ── Step 2: EXIT executor → first SELL for full qty ───────────────────────
    exit1_id_box: list[str] = []

    def sell_fn_1(order_id: str, asset: str, qty_base: float) -> str:
        assert asset == ASSET
        assert qty_base == pytest.approx(ENTRY_QTY)
        exit1_id_box.append(order_id)
        return EX_ID_1

    actions1 = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_1, db_path=db)

    assert len(actions1) == 1
    assert actions1[0]["exit_reason"] == "STOP_LOSS"
    result1 = actions1[0]["result"]
    assert result1 is not None
    assert result1.status == "OPEN"
    assert result1.exchange_order_id == EX_ID_1
    assert result1.position_id == pos_id
    exit1_id = result1.order_id

    with get_db(db) as conn:
        e1 = conn.execute("SELECT * FROM orders WHERE id=?", (exit1_id,)).fetchone()

    assert e1["status"] == "OPEN"
    assert e1["exchange_order_id"] == EX_ID_1
    assert e1["qty_base_requested"] == pytest.approx(ENTRY_QTY)
    assert e1["purpose"] == "EXIT"
    assert e1["side"] == "SELL"
    assert e1["position_id"] == pos_id

    # ── Step 3: active EXIT blocks duplicate SELL ─────────────────────────────
    second_sell_calls: list[str] = []

    def sell_fn_noop(order_id: str, asset: str, qty_base: float) -> str:
        second_sell_calls.append(order_id)
        return "NOOP"

    actions_idem = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_noop, db_path=db)
    assert second_sell_calls == [], "sell_fn must not be called when active EXIT exists"
    assert any(a.get("note") == "active_exit_already_exists" for a in actions_idem)

    # ── Step 4: reconciliation — partial fill (40%) then CANCELLED ────────────
    def get_order_fn_step4(exchange_order_id: str):
        if exchange_order_id == EX_ID_1:
            return CoinbaseOrder(
                client_order_id=exit1_id,
                exchange_order_id=EX_ID_1,
                status="CANCELLED",
                fills=[CoinbaseFill(
                    exchange_fill_id=PARTIAL_FILL_ID,
                    fill_price=PARTIAL_FILL_PRICE,
                    fill_qty_base=PARTIAL_FILL_QTY,
                    fee_usd=PARTIAL_FILL_FEE,
                    filled_at=_now(),
                )],
                product_id=ASSET,
                side="SELL",
            )
        return None

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=lambda eid: True,
        get_order_fn=get_order_fn_step4,
        db_path=db,
    )
    assert report.allowed_to_trade, f"Unexpected unresolved items: {report.unresolved}"

    with get_db(db) as conn:
        e1 = conn.execute("SELECT * FROM orders WHERE id=?", (exit1_id,)).fetchone()
        pos_row = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (exit1_id,)
        ).fetchone()[0]

    assert e1["status"] == "CANCELLED"
    assert fill_count == 1, "partial fill must be recorded in fills table"
    assert pos_row["status"] == "CLOSING"
    assert pos_row["qty_base_remaining"] == pytest.approx(ENTRY_QTY - PARTIAL_FILL_QTY)

    # ── Step 5: EXIT executor → second SELL for exactly remaining 60% ─────────
    def sell_fn_2(order_id: str, asset: str, qty_base: float) -> str:
        assert asset == ASSET
        assert qty_base == pytest.approx(SECOND_FILL_QTY, rel=1e-6)
        return EX_ID_2

    actions3 = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_2, db_path=db)

    assert len(actions3) == 1
    result3 = actions3[0]["result"]
    assert result3 is not None
    assert result3.status == "OPEN"
    assert result3.exchange_order_id == EX_ID_2
    exit2_id = result3.order_id

    with get_db(db) as conn:
        e2 = conn.execute("SELECT * FROM orders WHERE id=?", (exit2_id,)).fetchone()

    assert e2["status"] == "OPEN"
    assert e2["qty_base_requested"] == pytest.approx(SECOND_FILL_QTY)
    assert e2["position_id"] == pos_id

    # ── Step 6: second fill → position CLOSED ────────────────────────────────
    with get_db(db) as conn:
        r2 = apply_fill(
            order_id=exit2_id,
            fill_price=SECOND_FILL_PRICE,
            fill_qty_base=SECOND_FILL_QTY,
            fee_usd=SECOND_FILL_FEE,
            exchange_fill_id=SECOND_FILL_ID,
            conn=conn,
        )

    assert r2["status"] == "FILLED"
    assert r2["position_id"] == pos_id

    # ── Step 7: verify P&L and fees ──────────────────────────────────────────
    with get_db(db) as conn:
        pos_closed = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos_closed["status"] == "CLOSED"
    assert pos_closed["qty_base_remaining"] == 0.0
    assert pos_closed["exit_price"] == pytest.approx(EXPECTED_EXIT_VWAP)
    assert pos_closed["exit_fee_usd"] == pytest.approx(EXPECTED_EXIT_FEE)
    assert pos_closed["pnl_usd"] == pytest.approx(EXPECTED_PNL_USD)
    assert pos_closed["pnl_pct"] == pytest.approx(EXPECTED_PNL_PCT)

    # ── Step 8: idempotent fill replay ────────────────────────────────────────
    # Both fills have exchange_fill_ids; the early idempotency check fires
    # before the terminal-order check, so reconciliation_mode is not needed.
    with get_db(db) as conn:
        replay1 = apply_fill(
            order_id=exit1_id,
            fill_price=PARTIAL_FILL_PRICE, fill_qty_base=PARTIAL_FILL_QTY,
            fee_usd=PARTIAL_FILL_FEE, exchange_fill_id=PARTIAL_FILL_ID, conn=conn,
        )
        replay2 = apply_fill(
            order_id=exit2_id,
            fill_price=SECOND_FILL_PRICE, fill_qty_base=SECOND_FILL_QTY,
            fee_usd=SECOND_FILL_FEE, exchange_fill_id=SECOND_FILL_ID, conn=conn,
        )

    assert replay1["replayed"] is True
    assert replay2["replayed"] is True

    with get_db(db) as conn:
        pos_after_replay = conn.execute(
            "SELECT * FROM positions WHERE id=?", (pos_id,)
        ).fetchone()

    assert pos_after_replay["status"] == "CLOSED"
    assert pos_after_replay["pnl_usd"] == pytest.approx(EXPECTED_PNL_USD)
    assert pos_after_replay["qty_base_remaining"] == 0.0

    # ── Step 9: EXIT executor on CLOSED position → no sells placed ────────────
    final_sell_calls: list[str] = []

    def sell_fn_final(order_id: str, asset: str, qty_base: float) -> str:
        final_sell_calls.append(order_id)
        return "UNREACHABLE"

    actions_final = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_final, db_path=db)
    assert final_sell_calls == []
    assert actions_final == []  # no open/closing positions remain

    # ── Step 10: epoch closed P&L ─────────────────────────────────────────────
    with get_db(db) as conn:
        closed_pnl = get_epoch_closed_pnl(EPOCH_ID, conn)

    assert len(closed_pnl) == 1
    assert closed_pnl[0]["id"] == pos_id
    assert closed_pnl[0]["pnl_usd"] == pytest.approx(EXPECTED_PNL_USD)


# ---------------------------------------------------------------------------
# Crash variant: TX-A SUBMITTING survives restart
# ---------------------------------------------------------------------------

def test_e2e_crash_variant_tx_a_submitting_resolves_without_second_sell(
    tmp_db: Path,
) -> None:
    """
    Crash variant: TX-A committed (EXIT SUBMITTING) but process crashed before TX-B.

    On restart reconciliation finds the EXIT on Coinbase via list_orders_fn
    (matched by client_order_id = local order UUID) and resolves
    SUBMITTING → OPEN without creating a second SELL order.

    Key assertions:
      - Exactly one EXIT order exists after reconciliation.
      - That EXIT is OPEN with exchange_order_id from Coinbase.
      - Position is still OPEN (no fills in the crash window).
      - Subsequent run_exit_executor tick finds active EXIT → no sell placed.
    """
    db = tmp_db
    _, pos_id = _setup_open_position(db)

    # Simulate TX-A: SUBMITTING EXIT committed, exchange_order_id not set yet
    crash_exit_oid = _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=crash_exit_oid, epoch_id=EPOCH_ID, asset=ASSET,
            side="SELL", order_type="MARKET", purpose="EXIT",
            position_id=pos_id, placed_at=_now(),
            qty_base_requested=ENTRY_QTY, conn=conn,
        )

    with get_db(db) as conn:
        crash_row = conn.execute(
            "SELECT * FROM orders WHERE id=?", (crash_exit_oid,)
        ).fetchone()

    assert crash_row["status"] == "SUBMITTING"
    assert crash_row["exchange_order_id"] is None

    # Coinbase received the order before the crash (sell_fn executed, TX-B did not)
    CB_EX_ID = "EX-CRASH-001"

    def list_orders_fn():
        return [CoinbaseOrder(
            client_order_id=crash_exit_oid,
            exchange_order_id=CB_EX_ID,
            status="OPEN",
            fills=[],
            product_id=ASSET,
            side="SELL",
        )]

    def get_order_fn(exchange_order_id: str):
        if exchange_order_id == CB_EX_ID:
            return CoinbaseOrder(
                client_order_id=crash_exit_oid,
                exchange_order_id=CB_EX_ID,
                status="OPEN",
                fills=[],
                product_id=ASSET,
                side="SELL",
            )
        return None

    report = run_startup_reconciliation(
        list_orders_fn=list_orders_fn,
        cancel_order_fn=lambda eid: True,
        get_order_fn=get_order_fn,
        db_path=db,
    )
    assert report.allowed_to_trade, f"Unexpected unresolved: {report.unresolved}"

    # SUBMITTING → OPEN, exchange_order_id populated
    with get_db(db) as conn:
        crash_row = conn.execute(
            "SELECT * FROM orders WHERE id=?", (crash_exit_oid,)
        ).fetchone()

    assert crash_row["status"] == "OPEN"
    assert crash_row["exchange_order_id"] == CB_EX_ID

    # Position still OPEN (no fills during crash interval)
    with get_db(db) as conn:
        pos_row = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos_row["status"] == "OPEN"
    assert pos_row["qty_base_remaining"] == pytest.approx(ENTRY_QTY)

    # EXIT executor: active EXIT (OPEN) already exists → no sell placed
    guard_calls: list[str] = []

    def sell_fn_guard(order_id: str, asset: str, qty_base: float) -> str:
        guard_calls.append(order_id)
        return "UNREACHABLE"

    actions = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_guard, db_path=db)
    assert guard_calls == [], "sell_fn must not fire when active EXIT already exists"
    assert any(a.get("note") == "active_exit_already_exists" for a in actions)

    # Exactly one EXIT order — no duplicate created by the reconciler or executor
    with get_db(db) as conn:
        total_exits = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE position_id=? AND purpose='EXIT'",
            (pos_id,),
        ).fetchone()[0]

    assert total_exits == 1

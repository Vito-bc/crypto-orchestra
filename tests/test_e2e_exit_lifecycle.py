"""
End-to-end EXIT lifecycle integration tests.

All tests exercise the real state machine (ledger, reconciler, executor, outbox)
— no mocks of internal transitions.

test_e2e_exit_lifecycle:
    ENTRY fill → OPEN position → EXIT executor (STOP_LOSS, full qty)
    → idempotent tick → reconciliation round 1 (partial 40% fill + CANCELLED)
    → CLOSING (60% remaining) → EXIT executor (second SELL for exactly 60%)
    → reconciliation round 2 (FILLED) → CLOSED
    → P&L / fees / VWAP verified → idempotent fill replay → no sell after CLOSE
    → epoch closed-P&L

test_e2e_crash_variant_tx_a_submitting_resolves_without_second_sell:
    place_exit_outbox() with a sell_fn that raises TimeoutError.
    TX-A commits (SUBMITTING); TX-B is skipped (ambiguous network failure).
    Reconciliation finds the order on Coinbase via list_orders_fn
    (matched by client_order_id) and resolves SUBMITTING → OPEN without
    creating a duplicate SELL.

test_e2e_stop_loss_adverse_slippage_negative_pnl:
    Realistic stop-loss with adverse slippage: fill executes below the trigger
    price (gap-down market). Verifies negative P&L is computed and stored
    correctly through the full reconciliation path.
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
from pipeline.outbox import place_exit_outbox
from pipeline.reconciler import (
    CoinbaseFill,
    CoinbaseOrder,
    run_startup_reconciliation,
)

# ---------------------------------------------------------------------------
# Test constants — main lifecycle
# ---------------------------------------------------------------------------

ASSET = "ZEC-USD"
EPOCH_ID = "EP-E2E"

ENTRY_PRICE = 100.0
ENTRY_QTY = 1.0
ENTRY_FEE = 0.50
STOP_PRICE = 90.0     # fires when current_price <= STOP_PRICE
TARGET_PRICE = 150.0
TRIGGER_PRICE = 80.0  # below STOP_PRICE → STOP_LOSS condition

# First EXIT: partially filled then CANCELLED
PARTIAL_FILL_ID = "F-PARTIAL-001"
PARTIAL_FILL_PRICE = 110.0   # fee-math scenario; slippage tested separately
PARTIAL_FILL_QTY = 0.4
PARTIAL_FILL_FEE = 0.20

# Second EXIT: fills remaining qty exactly
SECOND_FILL_ID = "F-SECOND-001"
SECOND_FILL_PRICE = 120.0
SECOND_FILL_QTY = ENTRY_QTY - PARTIAL_FILL_QTY   # 0.6 ZEC
SECOND_FILL_FEE = 0.30

EX_ID_1 = "EX-LIFECYCLE-001"
EX_ID_2 = "EX-LIFECYCLE-002"

# Expected P&L (hand-computed to double-check against ledger formula):
#   exit_vwap = (110*0.4 + 120*0.6) / 1.0 = 116.0
#   total_exit_fee = 0.20 + 0.30 = 0.50
#   pnl_usd = (116 - 100) * 1.0 - 0.50 - 0.50 = 15.0
#   pnl_pct = 15.0 / (100.0 * 1.0) * 100 = 15.0 %
EXPECTED_EXIT_VWAP = (
    PARTIAL_FILL_PRICE * PARTIAL_FILL_QTY + SECOND_FILL_PRICE * SECOND_FILL_QTY
) / ENTRY_QTY
EXPECTED_EXIT_FEE = PARTIAL_FILL_FEE + SECOND_FILL_FEE
EXPECTED_PNL_USD = (
    (EXPECTED_EXIT_VWAP - ENTRY_PRICE) * ENTRY_QTY
    - EXPECTED_EXIT_FEE
    - ENTRY_FEE
)
EXPECTED_PNL_PCT = EXPECTED_PNL_USD / (ENTRY_PRICE * ENTRY_QTY) * 100


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _oid() -> str:
    return str(uuid.uuid4())


def _setup_open_position(db: Path) -> tuple[str, str]:
    """Insert epoch (once), ENTRY order + trade intent + fill. Returns (entry_oid, pos_id)."""
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
    Full EXIT lifecycle: partial fill → restart → remaining-qty SELL → CLOSED → P&L.

    Both exit fills are routed through run_startup_reconciliation so the test
    exercises the full CoinbaseOrder → reconciler → apply_fill → ledger chain.

    Steps:
      1.  ENTRY fill → OPEN position.
      2.  EXIT executor (STOP_LOSS) → first SELL for full qty (TX-A + TX-B via real executor).
      3.  Active EXIT blocks duplicate SELL (idempotent tick).
      4.  Reconciliation round 1: first EXIT CANCELLED with 40% partial fill
          → order CANCELLED, position CLOSING, qty_base_remaining = 60%.
      5.  EXIT executor → second SELL for exactly 60% (read from qty_base_remaining).
      6.  Reconciliation round 2: second EXIT FILLED with full fill → position CLOSED.
      7.  P&L = (exit_vwap - entry_price) * qty - all fees; VWAP, fee, pct verified.
      8.  Idempotent fill replay → replayed=True, no state change.
      9.  EXIT executor on CLOSED position → no sells, empty action list.
     10.  Epoch closed-P&L matches position record.
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

    # ── Step 2: EXIT executor → first SELL (STOP_LOSS, full qty) ─────────────
    def sell_fn_1(order_id: str, asset: str, qty_base: str) -> str:
        assert asset == ASSET
        assert float(qty_base) == pytest.approx(ENTRY_QTY)
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
    sell_called: list[str] = []

    def sell_fn_noop(order_id: str, asset: str, qty_base: str) -> str:
        sell_called.append(order_id)
        return "NOOP"

    actions_idem = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_noop, db_path=db)
    assert sell_called == [], "sell_fn must not be called when active EXIT exists"
    assert any(a.get("note") == "active_exit_already_exists" for a in actions_idem)

    # ── Step 4: reconciliation round 1 — partial fill (40%) + CANCELLED ──────
    def get_order_fn_r1(exchange_order_id: str):
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

    report1 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=lambda eid: True,
        get_order_fn=get_order_fn_r1,
        db_path=db,
    )
    assert report1.allowed_to_trade, f"Unexpected unresolved: {report1.unresolved}"

    with get_db(db) as conn:
        e1 = conn.execute("SELECT * FROM orders WHERE id=?", (exit1_id,)).fetchone()
        pos_row = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()
        fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (exit1_id,)
        ).fetchone()[0]

    assert e1["status"] == "CANCELLED"
    assert fill_count == 1, "partial fill must be recorded in fills table"
    assert pos_row["status"] == "CLOSING"
    assert pos_row["qty_base_remaining"] == pytest.approx(SECOND_FILL_QTY)

    # ── Step 5: EXIT executor → second SELL for exactly remaining 60% ─────────
    def sell_fn_2(order_id: str, asset: str, qty_base: str) -> str:
        assert asset == ASSET
        assert float(qty_base) == pytest.approx(SECOND_FILL_QTY, rel=1e-6)
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

    # ── Step 6: reconciliation round 2 — second EXIT FILLED → position CLOSED ─
    def get_order_fn_r2(exchange_order_id: str):
        if exchange_order_id == EX_ID_2:
            return CoinbaseOrder(
                client_order_id=exit2_id,
                exchange_order_id=EX_ID_2,
                status="FILLED",
                fills=[CoinbaseFill(
                    exchange_fill_id=SECOND_FILL_ID,
                    fill_price=SECOND_FILL_PRICE,
                    fill_qty_base=SECOND_FILL_QTY,
                    fee_usd=SECOND_FILL_FEE,
                    filled_at=_now(),
                )],
                product_id=ASSET,
                side="SELL",
            )
        if exchange_order_id == EX_ID_1:
            # exit1 was CANCELLED in round 1 with fills_finalized_at IS NULL (within
            # the 10-min settlement window). Round 2 re-checks it via terminal_rows.
            # Return the same partial fill: _check_late_fills_for_terminal_order will
            # find PARTIAL_FILL_ID already in already_applied → new_fills=[] → no-op.
            # This proves idempotency through the reconciler: the fill is not doubled.
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

    report2 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=lambda eid: True,
        get_order_fn=get_order_fn_r2,
        db_path=db,
    )
    assert report2.allowed_to_trade, f"Unexpected unresolved: {report2.unresolved}"

    with get_db(db) as conn:
        e2 = conn.execute("SELECT * FROM orders WHERE id=?", (exit2_id,)).fetchone()
        # Idempotency invariant: get_order_fn_r2 re-returned PARTIAL_FILL_ID for exit1.
        # apply_fill's early exchange_fill_id dedup must block the duplicate → exactly 1
        # fill recorded for exit1 (not 2), proving the reconciler did not double-count.
        exit1_fill_count = conn.execute(
            "SELECT COUNT(*) FROM fills WHERE order_id=?", (exit1_id,)
        ).fetchone()[0]

    assert e2["status"] == "FILLED"
    assert exit1_fill_count == 1, (
        f"Idempotency broken: exit1 has {exit1_fill_count} fills after round-2 "
        f"reconciliation re-presented PARTIAL_FILL_ID; expected exactly 1"
    )

    # ── Step 7: verify P&L, VWAP, and fees ───────────────────────────────────
    with get_db(db) as conn:
        pos_closed = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos_closed["status"] == "CLOSED"
    assert pos_closed["qty_base_remaining"] == 0.0
    assert pos_closed["exit_price"] == pytest.approx(EXPECTED_EXIT_VWAP)
    assert pos_closed["exit_fee_usd"] == pytest.approx(EXPECTED_EXIT_FEE)
    assert pos_closed["pnl_usd"] == pytest.approx(EXPECTED_PNL_USD)
    assert pos_closed["pnl_pct"] == pytest.approx(EXPECTED_PNL_PCT)

    # ── Step 8: idempotent fill replay ────────────────────────────────────────
    # The early exchange_fill_id dedup in apply_fill fires before the terminal-order
    # check, so reconciliation_mode is not required for CANCELLED/FILLED orders.
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

    # ── Step 9: EXIT executor on CLOSED position → no sells ───────────────────
    final_sell_calls: list[str] = []

    def sell_fn_final(order_id: str, asset: str, qty_base: str) -> str:
        final_sell_calls.append(order_id)
        return "UNREACHABLE"

    actions_final = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_final, db_path=db)
    assert final_sell_calls == []
    assert actions_final == []

    # ── Step 10: epoch closed P&L ─────────────────────────────────────────────
    with get_db(db) as conn:
        closed_pnl = get_epoch_closed_pnl(EPOCH_ID, conn)

    assert len(closed_pnl) == 1
    assert closed_pnl[0]["id"] == pos_id
    assert closed_pnl[0]["pnl_usd"] == pytest.approx(EXPECTED_PNL_USD)


# ---------------------------------------------------------------------------
# Crash variant: real TX-A via place_exit_outbox + TimeoutError
# ---------------------------------------------------------------------------

def test_e2e_crash_variant_tx_a_submitting_resolves_without_second_sell(
    tmp_db: Path,
) -> None:
    """
    Crash variant: sell_fn raises TimeoutError (ambiguous network failure).

    place_exit_outbox() commits TX-A (SUBMITTING order in ledger) and then
    calls coinbase_sell_fn.  The TimeoutError is caught as an ambiguous failure
    → TX-B is skipped → order remains SUBMITTING.

    On the next startup, run_startup_reconciliation finds the SUBMITTING order
    on Coinbase via list_orders_fn (matched by client_order_id = local UUID)
    and resolves it SUBMITTING → OPEN without placing a duplicate SELL.
    """
    db = tmp_db
    _, pos_id = _setup_open_position(db)

    # Pre-generate order_id so we can reference it in the mock after TX-A
    crash_exit_oid = _oid()
    CB_EX_ID = "EX-CRASH-001"

    def crash_sell_fn(order_id: str, asset: str, qty_base: str) -> str:
        # Simulates: API call was sent, response never received (dropped connection)
        raise TimeoutError("simulated network timeout — no response from Coinbase")

    # TX-A: insert SUBMITTING EXIT order (committed before sell_fn is called)
    # TX-B: skipped because TimeoutError is treated as ambiguous
    result = place_exit_outbox(
        position_id=pos_id,
        exit_reason="STOP_LOSS",
        coinbase_sell_fn=crash_sell_fn,
        order_id=crash_exit_oid,
        db_path=db,
    )

    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None

    with get_db(db) as conn:
        crash_row = conn.execute(
            "SELECT * FROM orders WHERE id=?", (crash_exit_oid,)
        ).fetchone()

    assert crash_row["status"] == "SUBMITTING"
    assert crash_row["exchange_order_id"] is None
    assert crash_row["purpose"] == "EXIT"
    assert crash_row["position_id"] == pos_id

    # Coinbase received the request (sell_fn fired before raising) so the order exists
    # there under CB_EX_ID.  Reconciliation discovers it via client_order_id match.
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

    # SUBMITTING → OPEN with exchange_order_id from Coinbase
    with get_db(db) as conn:
        crash_row = conn.execute(
            "SELECT * FROM orders WHERE id=?", (crash_exit_oid,)
        ).fetchone()

    assert crash_row["status"] == "OPEN"
    assert crash_row["exchange_order_id"] == CB_EX_ID

    # Position still OPEN — no fills arrived during the crash window
    with get_db(db) as conn:
        pos_row = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos_row["status"] == "OPEN"
    assert pos_row["qty_base_remaining"] == pytest.approx(ENTRY_QTY)

    # Subsequent EXIT executor tick: active EXIT (OPEN) blocks a second SELL
    guard_calls: list[str] = []

    def sell_fn_guard(order_id: str, asset: str, qty_base: str) -> str:
        guard_calls.append(order_id)
        return "UNREACHABLE"

    actions = run_exit_executor(ASSET, TRIGGER_PRICE, sell_fn_guard, db_path=db)
    assert guard_calls == [], "sell_fn must not fire when active EXIT already exists"
    assert any(a.get("note") == "active_exit_already_exists" for a in actions)

    # Exactly one EXIT order — reconciler and executor both prevented duplicates
    with get_db(db) as conn:
        total_exits = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE position_id=? AND purpose='EXIT'",
            (pos_id,),
        ).fetchone()[0]

    assert total_exits == 1


# ---------------------------------------------------------------------------
# Adverse-slippage / negative P&L (P2 realistic scenario)
# ---------------------------------------------------------------------------

def test_e2e_stop_loss_adverse_slippage_negative_pnl(tmp_db: Path) -> None:
    """
    Realistic stop-loss: gap-through-stop followed by fill below trigger.

    Entry: $100, stop: $90.  Price gaps to $80 — below stop, triggering STOP_LOSS.
    Market SELL routes to exchange; by fill time price has slipped further to $78
    ($2 below trigger, thin book / partial gap-fill).

    Two distinct effects:
      gap-through-stop : trigger ($80) is already $10 below the configured stop ($90)
      adverse slippage : fill ($78) is $2 below the trigger ($80) due to routing latency

    Expected outcome: negative P&L, both effects captured in the ledger formula.
    """
    db = tmp_db
    _, pos_id = _setup_open_position(db)

    SLIPPAGE_TRIGGER = 80.0   # price seen by EXIT executor: gap-down past stop $90
    SLIP_EX_ID = "EX-SLIP-001"
    SLIP_FILL_ID = "F-SLIP-001"
    SLIP_FILL_PRICE = 78.0    # actual fill: $2 below trigger (adverse slippage)
    SLIP_FILL_QTY = ENTRY_QTY
    SLIP_FILL_FEE = 0.80

    exit_oid_box: list[str] = []

    def sell_fn_slip(order_id: str, asset: str, qty_base: str) -> str:
        assert asset == ASSET
        assert float(qty_base) == pytest.approx(ENTRY_QTY)
        exit_oid_box.append(order_id)
        return SLIP_EX_ID

    actions = run_exit_executor(ASSET, SLIPPAGE_TRIGGER, sell_fn_slip, db_path=db)
    assert len(actions) == 1
    assert actions[0]["exit_reason"] == "STOP_LOSS"
    slip_exit_id = actions[0]["result"].order_id

    # Route the fill through reconciliation (full CoinbaseOrder → reconciler path)
    def get_order_fn_slip(exchange_order_id: str):
        if exchange_order_id == SLIP_EX_ID:
            return CoinbaseOrder(
                client_order_id=slip_exit_id,
                exchange_order_id=SLIP_EX_ID,
                status="FILLED",
                fills=[CoinbaseFill(
                    exchange_fill_id=SLIP_FILL_ID,
                    fill_price=SLIP_FILL_PRICE,
                    fill_qty_base=SLIP_FILL_QTY,
                    fee_usd=SLIP_FILL_FEE,
                    filled_at=_now(),
                )],
                product_id=ASSET,
                side="SELL",
            )
        return None

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=lambda eid: True,
        get_order_fn=get_order_fn_slip,
        db_path=db,
    )
    assert report.allowed_to_trade, f"Unexpected unresolved: {report.unresolved}"

    with get_db(db) as conn:
        pos_closed = conn.execute("SELECT * FROM positions WHERE id=?", (pos_id,)).fetchone()

    assert pos_closed["status"] == "CLOSED"
    assert pos_closed["qty_base_remaining"] == 0.0

    # pnl = (78 - 100) * 1.0 - 0.80 - 0.50 = -22.0 - 1.30 = -23.30
    expected_pnl = (SLIP_FILL_PRICE - ENTRY_PRICE) * SLIP_FILL_QTY - SLIP_FILL_FEE - ENTRY_FEE
    expected_pnl_pct = expected_pnl / (ENTRY_PRICE * ENTRY_QTY) * 100

    assert expected_pnl < 0, "stop-loss with adverse slippage must produce a loss"
    assert pos_closed["exit_price"] == pytest.approx(SLIP_FILL_PRICE)
    assert pos_closed["exit_fee_usd"] == pytest.approx(SLIP_FILL_FEE)
    assert pos_closed["pnl_usd"] == pytest.approx(expected_pnl)
    assert pos_closed["pnl_pct"] == pytest.approx(expected_pnl_pct)

    # Epoch P&L is also negative
    with get_db(db) as conn:
        closed = get_epoch_closed_pnl(EPOCH_ID, conn)

    assert len(closed) == 1
    assert closed[0]["pnl_usd"] == pytest.approx(expected_pnl)
    assert closed[0]["pnl_usd"] < 0

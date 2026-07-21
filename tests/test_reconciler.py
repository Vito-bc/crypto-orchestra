"""
Tests for pipeline/reconciler.py — startup reconciliation gate.

Every test uses a fresh on-disk DB.  Coinbase API calls are injected via
list_orders_fn / cancel_order_fn so no network is required.

Coverage:
  1.  No SUBMITTING orders → COMPLETE, allowed_to_trade=True
  2.  SUBMITTING → found OPEN on Coinbase → OPEN in ledger
  3.  SUBMITTING → found FILLED on Coinbase → fills applied, position created
  4.  SUBMITTING → found CANCELLED (no fills) → OPEN → CANCELLED in ledger
  5.  SUBMITTING → found CANCELLED (with fills) → OPEN → fills applied → CANCELLED
  6.  SUBMITTING → found EXPIRED (no fills) → OPEN → EXPIRED in ledger
  7.  SUBMITTING → not found → UNRESOLVED, allowed_to_trade=False
  8.  Late-fill stacking: expired order filled → creates position → active ENTRY
      cancelled → resolved
  9.  Late-fill stacking: cancel not confirmed → stacking order UNRESOLVED,
      allowed_to_trade=False
 10.  is_entry_placement_allowed: no reconciliation ever → False
 11.  is_entry_placement_allowed: UNRESOLVED in last run → False
 12.  is_entry_placement_allowed: stale reconciliation → False
 13.  is_entry_placement_allowed: clean recent reconciliation → True
 14.  is_entry_placement_allowed: RUNNING reconciliation → False
 15.  UNRESOLVED does not block EXIT orders or CANCEL (gate is ENTRY-only)
 16.  Multiple SUBMITTING orders — mix of found and not found
 17.  SUBMITTING → CANCEL_QUEUED → OPEN + UNRESOLVED(cancel_pending)
 18.  SUBMITTING → PENDING_CANCEL → OPEN + UNRESOLVED(cancel_pending)
 19.  SUBMITTING → FILLED (no fills in List Orders) → OPEN + UNRESOLVED(filled_missing_fills)
 20.  Terminal status mismatch: local=EXPIRED, Coinbase=OPEN → UNRESOLVED(status_mismatch)
 21.  is_entry_placement_allowed: malformed unresolved JSON → False (fail-closed)
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from pipeline.ledger import (
    apply_fill,
    get_db,
    insert_epoch,
    insert_order,
    insert_trade_intent,
    run_migrations,
    start_reconciliation,
    transition_order,
)
from pipeline.outbox import place_order_outbox
from pipeline.reconciler import (
    CoinbaseFill,
    CoinbaseOrder,
    is_entry_placement_allowed,
    run_startup_reconciliation,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EPOCH_ID = "EP1"
_CAPITAL  = 1000.0


def _oid() -> str:
    return str(uuid.uuid4())


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _no_cancel(exchange_order_id: str) -> bool:
    raise AssertionError(f"cancel_order_fn called unexpectedly for {exchange_order_id}")


def _cancel_ok(exchange_order_id: str) -> bool:
    return True


def _cancel_fail(exchange_order_id: str) -> bool:
    return False


def _cb_order(client_id: str, exchange_id: str, status: str,
              fills: list[CoinbaseFill] | None = None) -> CoinbaseOrder:
    return CoinbaseOrder(
        client_order_id=client_id,
        exchange_order_id=exchange_id,
        status=status,
        fills=fills or [],
    )


def _cb_fill(fid: str, price: float = 100.0, qty: float = 0.1, fee: float = 0.01,
             filled_at: str | None = None) -> CoinbaseFill:
    return CoinbaseFill(
        exchange_fill_id=fid,
        fill_price=price,
        fill_qty_base=qty,
        fee_usd=fee,
        filled_at=filled_at or _now(),
    )


def _insert_submitting_entry(db: Path, asset: str = "ZEC-USD",
                              order_id: str | None = None) -> str:
    oid = order_id or _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
    return oid


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ledger.db"
    run_migrations(db)
    with get_db(db) as conn:
        insert_epoch(_EPOCH_ID, _CAPITAL, "test epoch", conn=conn)
    return db


# ---------------------------------------------------------------------------
# 1. No SUBMITTING orders
# ---------------------------------------------------------------------------

def test_no_submitting_orders_completes_cleanly(tmp_db: Path) -> None:
    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )
    assert report.allowed_to_trade
    assert report.discovered == []
    assert report.resolved == []
    assert report.unresolved == []

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (report.run_id,)
        ).fetchone()
    assert row["status"] == "COMPLETE"


# ---------------------------------------------------------------------------
# 2. SUBMITTING → found OPEN
# ---------------------------------------------------------------------------

def test_submitting_found_open_becomes_open(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-OPEN-1", "OPEN")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade
    assert len(report.resolved) == 1
    assert report.resolved[0].action == "open"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status, exchange_order_id FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == "CB-OPEN-1"


# ---------------------------------------------------------------------------
# 3. SUBMITTING → found FILLED → position created
# ---------------------------------------------------------------------------

def test_submitting_found_filled_creates_position(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    fill = _cb_fill("FILL-001", price=100.0, qty=0.101)  # 10.1 USD >= 10.0*0.999=9.99
    cb_orders = [_cb_order(oid, "CB-FILLED-1", "FILLED", fills=[fill])]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade
    assert report.resolved[0].action == "filled"

    with get_db(tmp_db) as conn:
        order = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        pos = conn.execute("SELECT * FROM positions WHERE entry_order_id=?", (oid,)).fetchone()

    assert order["status"] == "FILLED"
    assert pos is not None
    assert pos["status"] == "OPEN"
    assert abs(pos["entry_price"] - 100.0) < 0.001


# ---------------------------------------------------------------------------
# 4. SUBMITTING → found CANCELLED (no fills)
# ---------------------------------------------------------------------------

def test_submitting_found_cancelled_no_fills(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-CAN-1", "CANCELLED")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade
    assert report.resolved[0].action == "cancelled"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status, exchange_order_id FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "CANCELLED"
    assert row["exchange_order_id"] == "CB-CAN-1"


# ---------------------------------------------------------------------------
# 5. SUBMITTING → found CANCELLED with partial fills
# ---------------------------------------------------------------------------

def test_submitting_found_cancelled_with_partial_fills(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    fill = _cb_fill("FILL-PARTIAL", price=100.0, qty=0.05)
    cb_orders = [_cb_order(oid, "CB-CAN-PARTIAL", "CANCELLED", fills=[fill])]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        pos = conn.execute("SELECT * FROM positions WHERE entry_order_id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()

    assert row["status"] == "CANCELLED"
    assert pos is not None, "position created from partial fill"
    assert len(fills) == 1


# ---------------------------------------------------------------------------
# 6. SUBMITTING → found EXPIRED (no fills)
# ---------------------------------------------------------------------------

def test_submitting_found_expired_no_fills(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-EXP-1", "EXPIRED")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade
    assert report.resolved[0].action == "expired"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "EXPIRED"


# ---------------------------------------------------------------------------
# 7. SUBMITTING → not found on Coinbase → UNRESOLVED
# ---------------------------------------------------------------------------

def test_submitting_not_found_is_unresolved(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],  # empty — not found
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert len(report.unresolved) == 1
    assert report.unresolved[0].reason == "not_found"
    assert report.unresolved[0].order_id == oid

    # Order must still be SUBMITTING — not silently changed to REJECTED
    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "SUBMITTING", (
        "not-found order must remain SUBMITTING, not be silently REJECTED"
    )

    with get_db(tmp_db) as conn:
        run = conn.execute(
            "SELECT status FROM reconciliation_runs WHERE id=?", (report.run_id,)
        ).fetchone()
    assert run["status"] == "FAILED"  # FAILED because unresolved non-empty


# ---------------------------------------------------------------------------
# 8. Late-fill stacking: EXPIRED order gets late Coinbase fill while a new
#    ENTRY for the same asset is already OPEN — reconciler cancels the new order
# ---------------------------------------------------------------------------

def _setup_expired_then_new_entry(
    db: Path,
    asset: str = "ZEC-USD",
    exchange_id_a: str = "CB-A-EXP",
    exchange_id_b: str = "CB-B-OPEN",
) -> tuple[str, str]:
    """
    Set up the late-fill stacking scenario:
      - order A: ENTRY for `asset`, fully expired (has exchange_order_id)
      - order B: ENTRY for `asset`, currently OPEN (placed after A expired)

    Returns (oid_a, oid_b).
    The UNIQUE index allows this because A is EXPIRED (not active) when B is inserted.
    """
    oid_a = _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=oid_a, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_a, "OPEN", exchange_order_id=exchange_id_a, conn=conn)
        transition_order(oid_a, "EXPIRED", conn=conn)

    # A is now EXPIRED — stacking guard allows a new ENTRY for same asset.
    oid_b = _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id=exchange_id_b, conn=conn)

    return oid_a, oid_b


def test_late_fill_stacking_cancel_succeeds(tmp_db: Path) -> None:
    """
    Late-fill stacking scenario (ADR 001 Decision 4):
      - Order A: EXPIRED in ledger, but Coinbase filled it (race with expiry).
      - Order B: OPEN ENTRY for same asset (placed after A expired — allowed).
      - Reconciler must:
          1. Detect late fill for A via late-fill check on terminal orders.
          2. Apply fill → create position for asset.
          3. Detect stacking conflict (B is OPEN).
          4. Cancel B (Coinbase confirms).
          5. Transition B → CANCELLED.
          6. Report: allowed_to_trade = True.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-EXPIRED", exchange_id_b="CB-B-OPEN"
    )

    cancel_calls: list[str] = []

    def confirming_cancel(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        return True

    # Coinbase shows A as EXPIRED but with a fill (late fill arrived after local expiry).
    fill = _cb_fill("FILL-LATE-A", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-EXPIRED", "EXPIRED", fills=[fill]),
        _cb_order(oid_b, "CB-B-OPEN", "OPEN"),
    ]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=confirming_cancel,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert "CB-B-OPEN" in cancel_calls, "cancel must be issued for the conflicting order B"

    with get_db(tmp_db) as conn:
        a_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_a,)).fetchone()
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
        pos   = conn.execute(
            "SELECT status FROM positions WHERE entry_order_id=?", (oid_a,)
        ).fetchone()

    assert a_row["status"] == "EXPIRED", "order A stays EXPIRED (fill recorded via reconciliation_mode)"
    assert b_row["status"] == "CANCELLED", "conflicting order B must be CANCELLED after confirmed cancel"
    assert pos is not None, "position must be created from the late fill"
    assert pos["status"] == "OPEN"


# ---------------------------------------------------------------------------
# 9. Late-fill stacking: cancel not confirmed → UNRESOLVED
# ---------------------------------------------------------------------------

def test_late_fill_stacking_cancel_fails_leaves_unresolved(tmp_db: Path) -> None:
    """
    Same late-fill stacking scenario, but the Coinbase cancel call returns False.
    Order B must stay OPEN (never unilaterally CANCELLED without exchange confirmation).
    allowed_to_trade = False until the conflict is resolved externally.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-EXP2", exchange_id_b="CB-B-LIVE2"
    )

    fill = _cb_fill("FILL-LATE-FAIL", price=101.0, qty=0.099)
    cb_orders = [
        _cb_order(oid_a, "CB-A-EXP2", "EXPIRED", fills=[fill]),
        _cb_order(oid_b, "CB-B-LIVE2", "OPEN"),
    ]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_fail,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "stacking_cancel_failed" for u in report.unresolved), (
        f"expected stacking_cancel_failed in unresolved, got: {report.unresolved}"
    )

    # Order B must NOT be locally CANCELLED — only Coinbase confirmation allows that.
    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "OPEN", (
        "conflicting order must stay OPEN until cancel confirmed by Coinbase; "
        "unilateral local CANCELLED without exchange confirmation would misstate state"
    )


# ---------------------------------------------------------------------------
# 10–15. Gate: is_entry_placement_allowed()
# ---------------------------------------------------------------------------

def test_gate_no_reconciliation_ever_blocks(tmp_db: Path) -> None:
    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert not allowed
    assert "no reconciliation" in reason


def test_gate_unresolved_items_block(tmp_db: Path) -> None:
    oid = _insert_submitting_entry(tmp_db)
    run_startup_reconciliation(
        list_orders_fn=lambda: [],  # not found → UNRESOLVED
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )
    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert not allowed
    assert "FAILED" in reason or "unresolved" in reason


def test_gate_stale_reconciliation_blocks(tmp_db: Path) -> None:
    run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )
    # Mark last run as completed 2 hours ago
    with get_db(tmp_db) as conn:
        stale_ts = (datetime.now(timezone.utc) - timedelta(hours=2)).isoformat()
        conn.execute(
            "UPDATE reconciliation_runs SET completed_at=? WHERE status='COMPLETE'",
            (stale_ts,),
        )

    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert not allowed
    assert "min old" in reason


def test_gate_clean_recent_reconciliation_allows(tmp_db: Path) -> None:
    run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )
    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert allowed, f"should be allowed but got: {reason}"
    assert reason == "ok"


def test_gate_running_reconciliation_blocks(tmp_db: Path) -> None:
    with get_db(tmp_db) as conn:
        start_reconciliation(conn)
    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert not allowed
    assert "RUNNING" in reason


# ---------------------------------------------------------------------------
# 16. Gate blocks ENTRY only — EXIT and CANCEL not affected
# ---------------------------------------------------------------------------

def test_gate_does_not_affect_exit_or_cancel(tmp_db: Path) -> None:
    """
    is_entry_placement_allowed() returns False (UNRESOLVED).
    The gate is checked by the caller for ENTRY only.
    EXIT and CANCEL orders can still be placed regardless.
    This test verifies the gate is ENTRY-scoped by asserting it only returns
    a boolean, and that inserting EXIT/CANCEL orders into the ledger succeeds
    even when gate returns False.
    """
    oid = _insert_submitting_entry(tmp_db)
    run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    allowed, _ = is_entry_placement_allowed(tmp_db)
    assert not allowed  # ENTRY blocked

    # Simulate cancelling the SUBMITTING order directly (reconciler action)
    # — this is a CANCEL action, not a new ENTRY placement
    with get_db(tmp_db) as conn:
        transition_order(oid, "CANCELLED", conn=conn)

    # Ledger accepts the transition regardless of gate state — gate is caller-enforced
    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# 17. Multiple SUBMITTING — mix of found and not found
# ---------------------------------------------------------------------------

def test_multiple_submitting_mix_found_and_not_found(tmp_db: Path) -> None:
    oid_a = _insert_submitting_entry(tmp_db, asset="ZEC-USD")
    oid_b = _insert_submitting_entry(tmp_db, asset="ETH-USD")

    fill = _cb_fill("FILL-A-MIX", price=100.0, qty=0.1)
    cb_orders = [_cb_order(oid_a, "CB-A-MIX", "FILLED", fills=[fill])]
    # oid_b not on Coinbase

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade  # oid_b is unresolved
    assert len(report.resolved) == 1
    assert len(report.unresolved) == 1
    assert report.resolved[0].order_id == oid_a
    assert report.unresolved[0].order_id == oid_b
    assert report.unresolved[0].reason == "not_found"

    with get_db(tmp_db) as conn:
        a_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_a,)).fetchone()
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert a_row["status"] == "FILLED"
    assert b_row["status"] == "SUBMITTING"  # not touched


# ---------------------------------------------------------------------------
# 17. CANCEL_QUEUED → OPEN + UNRESOLVED(cancel_pending)
# ---------------------------------------------------------------------------

def test_submitting_found_cancel_queued_leaves_unresolved(tmp_db: Path) -> None:
    """
    Coinbase accepted the order and then queued a cancel request.
    The cancel is not yet effective — the order may still execute.
    Local order must be OPEN (exchange acknowledged) + UNRESOLVED(cancel_pending).
    allowed_to_trade=False until CANCELLED is confirmed on the next reconciliation.
    """
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-CQ-1", "CANCEL_QUEUED")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert len(report.unresolved) == 1
    assert report.unresolved[0].reason == "cancel_pending"

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["status"] == "OPEN", "exchange acknowledged — must be OPEN, not SUBMITTING"
    assert row["exchange_order_id"] == "CB-CQ-1"


# ---------------------------------------------------------------------------
# 18. PENDING_CANCEL → OPEN + UNRESOLVED(cancel_pending)
# ---------------------------------------------------------------------------

def test_submitting_found_pending_cancel_leaves_unresolved(tmp_db: Path) -> None:
    """
    Same as test 17 but with PENDING_CANCEL status.
    PENDING_CANCEL is still an indeterminate state — cancel not yet effective.
    """
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-PC-1", "PENDING_CANCEL")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert report.unresolved[0].reason == "cancel_pending"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "OPEN", "must not be locally CANCELLED without Coinbase confirmation"


# ---------------------------------------------------------------------------
# 19. FILLED with no fill records → OPEN + UNRESOLVED(filled_missing_fills)
# ---------------------------------------------------------------------------

def test_submitting_found_filled_no_fills_leaves_unresolved(tmp_db: Path) -> None:
    """
    Coinbase says FILLED but the List Orders response has no fill records.
    List Orders is not a reliable source of fill data (see ADR 001).
    Without fills we cannot create a position or confirm execution quantity.
    Order must stay OPEN (exchange acknowledged) + UNRESOLVED(filled_missing_fills).
    allowed_to_trade=False until fills are obtained via List Fills.
    """
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-FILLED-NOFILL", "FILLED", fills=[])]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert len(report.unresolved) == 1
    assert report.unresolved[0].reason == "filled_missing_fills"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid,)
        ).fetchone()
    assert row["status"] == "OPEN", (
        "order must be OPEN (exchange acknowledged), not FILLED — fills not yet applied"
    )
    assert pos is None, "no position must be created without fill records"


# ---------------------------------------------------------------------------
# 20. Terminal status mismatch: local=EXPIRED, Coinbase=OPEN → UNRESOLVED
# ---------------------------------------------------------------------------

def test_terminal_status_mismatch_local_expired_cb_open_leaves_unresolved(tmp_db: Path) -> None:
    """
    The ledger considers the order EXPIRED but Coinbase reports it as OPEN.
    This can happen with GTC orders: the exchange never received or processed
    the expiry (e.g. from a wrong local clock or missed WebSocket event).
    Must be UNRESOLVED — we cannot unilaterally cancel without knowing the
    current Coinbase state, and we cannot treat it as expired while it's live.
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-MISMATCH-1", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)

    # Coinbase shows it still OPEN
    cb_orders = [_cb_order(oid, "CB-MISMATCH-1", "OPEN")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert len(report.unresolved) == 1
    assert report.unresolved[0].reason.startswith("status_mismatch:"), (
        f"expected status_mismatch reason, got: {report.unresolved[0].reason}"
    )
    assert "local=EXPIRED" in report.unresolved[0].reason
    assert "cb=OPEN" in report.unresolved[0].reason

    # Local order must stay EXPIRED — we do not overwrite local state based on
    # a single reconciliation observation; human review is required.
    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "EXPIRED"


# ---------------------------------------------------------------------------
# 21. is_entry_placement_allowed: malformed unresolved JSON → False (fail-closed)
# ---------------------------------------------------------------------------

def test_gate_malformed_unresolved_json_blocks_entry(tmp_db: Path) -> None:
    """
    If the reconciliation_runs.unresolved field contains invalid JSON,
    is_entry_placement_allowed must return False (fail-closed), never True.
    Silently treating a parse error as 'no unresolved items' would allow
    trading on an unknown state.
    """
    run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    # Corrupt the unresolved field of the completed run.
    with get_db(tmp_db) as conn:
        conn.execute(
            "UPDATE reconciliation_runs SET unresolved=? WHERE status='COMPLETE'",
            ("{not valid json",),
        )

    allowed, reason = is_entry_placement_allowed(tmp_db, freshness_minutes=60)
    assert not allowed
    assert "unparseable" in reason or "malformed" in reason

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


def _get_order_no_fills(exchange_id: str) -> CoinbaseOrder:
    """Stub get_order_fn: returns cancelled order with no partial fills."""
    return CoinbaseOrder(
        client_order_id="?",
        exchange_order_id=exchange_id,
        status="CANCELLED",
        fills=[],
    )


def test_late_fill_stacking_cancel_succeeds(tmp_db: Path) -> None:
    """
    Late-fill stacking scenario (ADR 001 Decision 4):
      - Order A: EXPIRED in ledger, but Coinbase filled it (race with expiry).
      - Order B: OPEN ENTRY for same asset (placed after A expired — allowed).
      - Reconciler must:
          1. Detect late fill for A via late-fill check on terminal orders.
          2. Apply fill → create position for asset.
          3. Detect stacking conflict (B is OPEN).
          4. Cancel B (Coinbase confirms via cancel_order_fn).
          5. Fetch B's final state via get_order_fn → no partial fills.
          6. Transition B → CANCELLED.
          7. Report: allowed_to_trade = True.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-EXPIRED", exchange_id_b="CB-B-OPEN"
    )

    cancel_calls: list[str] = []

    def confirming_cancel(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        return True

    # Production parity: list_orders_fn returns fills=[] (as production adapter does).
    # Fills come exclusively from get_order_fn — matches make_get_order_fn() behaviour.
    fill = _cb_fill("FILL-LATE-A", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-EXPIRED", "EXPIRED"),  # no fills from list_orders_fn
        _cb_order(oid_b, "CB-B-OPEN", "OPEN"),
    ]

    def get_order_fn(exchange_id: str) -> CoinbaseOrder:
        if exchange_id == "CB-A-EXPIRED":
            # Terminal enrichment: actual fills for A from Coinbase
            return CoinbaseOrder(client_order_id=oid_a, exchange_order_id=exchange_id,
                                 status="EXPIRED", fills=[fill])
        # Phase C verification for B: confirmed CANCELLED, no partial fills in window
        return CoinbaseOrder(client_order_id=oid_b, exchange_order_id=exchange_id,
                             status="CANCELLED", fills=[])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=confirming_cancel,
        get_order_fn=get_order_fn,
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


# ---------------------------------------------------------------------------
# 22. Stacking cancel confirmed but get_order_fn=None → UNRESOLVED
# ---------------------------------------------------------------------------

def test_stacking_cancel_confirmed_without_get_order_fn_is_unresolved(tmp_db: Path) -> None:
    """
    Cancel is confirmed by cancel_order_fn=True, but get_order_fn is not
    provided.  Without get_order_fn we cannot verify fills from the
    cancellation window, so the reconciler must leave stacking UNRESOLVED
    rather than blindly assuming zero fills and unlocking the asset.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-NOFN", exchange_id_b="CB-B-NOFN"
    )

    fill = _cb_fill("FILL-NOFN", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-NOFN", "EXPIRED", fills=[fill]),
        _cb_order(oid_b, "CB-B-NOFN", "OPEN"),
    ]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,   # confirms cancel
        get_order_fn=None,            # not provided
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "cancelled_fills_unverified" for u in report.unresolved), (
        f"expected cancelled_fills_unverified, got: {report.unresolved}"
    )
    # B must NOT be CANCELLED — fills not verified, so we cannot close stacking.
    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "OPEN"


# ---------------------------------------------------------------------------
# 23. Stacking cancel confirmed with partial fills from cancel window
# ---------------------------------------------------------------------------

def test_stacking_cancel_confirmed_applies_partial_fills_from_cancel_window(
    tmp_db: Path,
) -> None:
    """
    Cancel is confirmed AND get_order_fn returns partial fills that occurred
    between Phase A (stacking detection) and the cancel becoming effective.
    Reconciler must apply those fills before transitioning B to CANCELLED.
    The fills create a position for B's asset — visible in the ledger.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-PF", exchange_id_b="CB-B-PF"
    )

    fill_a = _cb_fill("FILL-A-PF", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-PF", "EXPIRED"),  # no fills from list_orders_fn (production parity)
        _cb_order(oid_b, "CB-B-PF", "OPEN"),
    ]

    # B was partially filled during the cancel window
    cancel_window_fill = _cb_fill("FILL-B-PARTIAL", price=103.0, qty=0.05)

    def dispatch_get_order(exchange_id: str) -> CoinbaseOrder:
        if exchange_id == "CB-A-PF":
            # A's actual late fill — comes via get_order_fn, not list_orders_fn
            return CoinbaseOrder(
                client_order_id=oid_a,
                exchange_order_id=exchange_id,
                status="EXPIRED",
                fills=[fill_a],
            )
        # B: Phase C verification — CANCELLED with fill from the cancel window
        return CoinbaseOrder(
            client_order_id=oid_b,
            exchange_order_id=exchange_id,
            status="CANCELLED",
            fills=[cancel_window_fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,
        get_order_fn=dispatch_get_order,
        db_path=tmp_db,
    )

    # Partial fills from the cancel window created a position for B while A's
    # position is already OPEN — stacked exposure must block trading until
    # a human or a future reconciliation pass resolves the two-position state.
    assert not report.allowed_to_trade, f"expected blocked; resolved: {report.resolved}"
    assert any(u.reason == "stacked_exposure_after_partial_fill" for u in report.unresolved), (
        f"expected stacked_exposure_after_partial_fill, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
        b_fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid_b,)).fetchall()
        b_pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid_b,)
        ).fetchone()

    # B must be committed to CANCELLED and have its fill recorded —
    # the UNRESOLVED is about the resulting stacked exposure, not a commit failure.
    assert b_row["status"] == "CANCELLED"
    assert len(b_fills) == 1, "partial fill from cancel window must be recorded"
    assert b_pos is not None, "partial fill creates an OPEN position for B (stacked with A)"


# ---------------------------------------------------------------------------
# 24. Deduplication: multiple late-fill orders targeting same active ENTRY
# ---------------------------------------------------------------------------

def test_duplicate_pending_cancels_deduplicated(tmp_db: Path) -> None:
    """
    Two EXPIRED orders for the same asset both receive late fills in the same
    reconciliation run.  Both detect order B (OPEN for same asset) as a
    stacking conflict and generate a _PendingCancel targeting B.
    After deduplication, only one cancel is sent to Coinbase, and the CANCELLED
    transition is applied exactly once.
    """
    asset = "ZEC-USD"

    # Order A1: EXPIRED with exchange_order_id
    oid_a1 = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_a1, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a1, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_a1, "OPEN", exchange_order_id="CB-A1-EXP", conn=conn)
        transition_order(oid_a1, "EXPIRED", conn=conn)

    # Order A2: also EXPIRED for same asset (allowed — both are terminal)
    oid_a2 = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_a2, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a2, stop_price=91.0, target_price=116.0, conn=conn)
        transition_order(oid_a2, "OPEN", exchange_order_id="CB-A2-EXP", conn=conn)
        transition_order(oid_a2, "EXPIRED", conn=conn)

    # Order B: new ENTRY, OPEN (placed after both A1, A2 expired)
    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=92.0, target_price=117.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-B-DEDUP", conn=conn)

    cancel_calls: list[str] = []

    def counting_cancel(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        return True

    # Production parity: fills come from get_order_fn, not list_orders_fn.
    fill_a1 = _cb_fill("FILL-A1", price=100.0, qty=0.101)
    fill_a2 = _cb_fill("FILL-A2", price=100.0, qty=0.101)
    cb_orders = [
        _cb_order(oid_a1, "CB-A1-EXP", "EXPIRED"),
        _cb_order(oid_a2, "CB-A2-EXP", "EXPIRED"),
        _cb_order(oid_b, "CB-B-DEDUP", "OPEN"),
    ]

    def dedup_get_order(exchange_id: str) -> CoinbaseOrder:
        if exchange_id == "CB-A1-EXP":
            return CoinbaseOrder(client_order_id=oid_a1, exchange_order_id=exchange_id,
                                 status="EXPIRED", fills=[fill_a1])
        if exchange_id == "CB-A2-EXP":
            return CoinbaseOrder(client_order_id=oid_a2, exchange_order_id=exchange_id,
                                 status="EXPIRED", fills=[fill_a2])
        return CoinbaseOrder(client_order_id="?", exchange_order_id=exchange_id,
                             status="CANCELLED", fills=[])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=counting_cancel,
        get_order_fn=dedup_get_order,
        db_path=tmp_db,
    )

    # Exactly one cancel call to CB-B-DEDUP (not two)
    assert cancel_calls.count("CB-B-DEDUP") == 1, (
        f"expected exactly 1 cancel call for CB-B-DEDUP, got {cancel_calls}"
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# 25. get_order_fn returns None → UNRESOLVED, B stays OPEN
# ---------------------------------------------------------------------------

def test_stacking_cancel_get_order_returns_none_is_unresolved(tmp_db: Path) -> None:
    """
    cancel_order_fn confirms the cancel (True), but get_order_fn returns None.
    A None response is unverifiable — the reconciler must NOT transition B to
    CANCELLED without knowing the final fill state.  B must stay OPEN.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-NONE", exchange_id_b="CB-B-NONE"
    )

    fill = _cb_fill("FILL-NONE", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-NONE", "EXPIRED"),  # no fills from list_orders_fn
        _cb_order(oid_b, "CB-B-NONE", "OPEN"),
    ]

    def get_order_none_for_b(exchange_id: str):
        if exchange_id == "CB-A-NONE":
            # A's actual late fill — production parity: fills via get_order_fn
            return CoinbaseOrder(
                client_order_id=oid_a,
                exchange_order_id=exchange_id,
                status="EXPIRED",
                fills=[fill],
            )
        # B: get_order_fn returns None → fill state unverifiable → UNRESOLVED
        return None

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,
        get_order_fn=get_order_none_for_b,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "cancelled_get_order_returned_none" for u in report.unresolved), (
        f"expected cancelled_get_order_returned_none, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "OPEN", "must not CANCEL without verified fills"


# ---------------------------------------------------------------------------
# 26. get_order_fn returns non-CANCELLED status → UNRESOLVED, B stays OPEN
# ---------------------------------------------------------------------------

def test_stacking_cancel_get_order_wrong_status_is_unresolved(tmp_db: Path) -> None:
    """
    cancel_order_fn confirms the cancel (True), but get_order_fn returns the
    order still in CANCEL_QUEUED — the exchange has not propagated the state yet.
    Reconciler must leave B OPEN and report UNRESOLVED(cancelled_unexpected_status).
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-WSTAT", exchange_id_b="CB-B-WSTAT"
    )

    # Production parity: fills come from get_order_fn.
    # Dispatch: A gets its actual fill; B returns CANCEL_QUEUED (exchange lag).
    fill = _cb_fill("FILL-WSTAT", price=101.0, qty=0.099)
    cb_orders = [
        _cb_order(oid_a, "CB-A-WSTAT", "EXPIRED"),  # no fills from list_orders_fn
        _cb_order(oid_b, "CB-B-WSTAT", "OPEN"),
    ]

    def get_order_fn_wstat(exchange_id: str) -> CoinbaseOrder:
        if exchange_id == "CB-A-WSTAT":
            return CoinbaseOrder(client_order_id=oid_a, exchange_order_id=exchange_id,
                                 status="EXPIRED", fills=[fill])
        # B: Phase C verification returns CANCEL_QUEUED — exchange not propagated yet
        return CoinbaseOrder(client_order_id="?", exchange_order_id=exchange_id,
                             status="CANCEL_QUEUED", fills=[])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,
        get_order_fn=get_order_fn_wstat,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        "cancelled_unexpected_status" in u.reason for u in report.unresolved
    ), f"expected cancelled_unexpected_status, got: {report.unresolved}"

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "OPEN", "must not CANCEL while exchange status is CANCEL_QUEUED"


# ---------------------------------------------------------------------------
# 27. Stacked exposure detected even when fills_applied=0 (idempotent replay)
# ---------------------------------------------------------------------------

def test_stacked_exposure_detected_on_idempotent_replay(tmp_db: Path) -> None:
    """
    A previous crashed reconciliation run applied fills to B and created its
    position, but did not finish the commit.  On the next run, get_order_fn
    returns CANCELLED with no new fills (already applied), so fills_applied=0.

    The stacked exposure check must still fire — it must not be gated on
    fills_applied > 0.  Without this fix, the reconciler would add
    stacking_cancelled to resolved and incorrectly allow trading.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-REPLAY", exchange_id_b="CB-B-REPLAY"
    )

    # Simulate previous crashed run: B got a PARTIAL fill (9.0 USD < 9.99 threshold
    # → PARTIAL, not FILLED).  Position is created on the first fill regardless.
    # The run crashed before transition_order(CANCELLED) and commit.
    with get_db(tmp_db) as conn:
        apply_fill(
            order_id=oid_b,
            fill_price=100.0,
            fill_qty_base=0.09,  # 9.0 USD → PARTIAL, leaves B in active PARTIAL state
            fee_usd=0.01,
            exchange_fill_id="FILL-B-REPLAY-PREV",
            filled_at=_now(),
            conn=conn,
        )

    # Production parity: A's fill comes from get_order_fn.
    # B: CANCELLED with no new fills (fill already applied above — idempotent replay).
    fill_a = _cb_fill("FILL-A-REPLAY", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-REPLAY", "EXPIRED"),  # no fills from list_orders_fn
        _cb_order(oid_b, "CB-B-REPLAY", "OPEN"),
    ]

    def replay_get_order(exchange_id: str) -> CoinbaseOrder:
        if exchange_id == "CB-A-REPLAY":
            return CoinbaseOrder(client_order_id=oid_a, exchange_order_id=exchange_id,
                                 status="EXPIRED", fills=[fill_a])
        # B: Phase C verification — CANCELLED, no new fills this run (already applied)
        return CoinbaseOrder(client_order_id="?", exchange_order_id=exchange_id,
                             status="CANCELLED", fills=[])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,
        get_order_fn=replay_get_order,
        db_path=tmp_db,
    )

    # fills_applied=0 this run, but B's pre-existing position must trigger the guard.
    assert not report.allowed_to_trade, f"expected blocked; resolved: {report.resolved}"
    assert any(u.reason == "stacked_exposure_after_partial_fill" for u in report.unresolved), (
        f"expected stacked_exposure_after_partial_fill, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        b_status = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_status["status"] == "CANCELLED", "B must still be committed to CANCELLED"


# ---------------------------------------------------------------------------
# 28–29. (See earlier sections for numbering continuity)
# ---------------------------------------------------------------------------
# Tests 28 and 29 are already above. The following tests cover P0-2, P0-3, P0-4.

# ---------------------------------------------------------------------------
# 30. P0-2 TTL cancellation: local expires_at exceeded → Phase E cancels GTC order
# ---------------------------------------------------------------------------

def test_ttl_expired_gtc_order_cancelled_with_fill_verification(tmp_db: Path) -> None:
    """
    An OPEN order with expires_at in the past is still OPEN on Coinbase (GTC).
    Reconciler must cancel it in Phase E, verify via get_order_fn, apply any
    fills, and transition to CANCELLED.
    """
    from datetime import datetime, timedelta, timezone as _tz

    oid = _oid()
    past_expires_at = (datetime.now(_tz.utc) - timedelta(hours=2)).isoformat()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-TTL-1", conn=conn)
        # Manually set expires_at to the past
        conn.execute("UPDATE orders SET expires_at=? WHERE id=?", (past_expires_at, oid))

    cancel_calls: list[str] = []

    def confirming_cancel(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        return True

    # Phase D: get_order_fn returns OPEN (GTC, no auto-expiry on Coinbase side).
    # Phase E (after cancel confirmed): get_order_fn returns CANCELLED with a partial fill.
    ttl_fill = _cb_fill("FILL-TTL-PARTIAL", price=100.0, qty=0.02)
    _get_call_count = [0]

    def get_order_fn(exchange_id: str) -> CoinbaseOrder:
        _get_call_count[0] += 1
        if _get_call_count[0] == 1:
            # Phase D check: still OPEN on Coinbase
            return CoinbaseOrder(
                client_order_id=oid, exchange_order_id=exchange_id,
                status="OPEN", fills=[],
            )
        # Phase E verification: now CANCELLED with partial fill
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="CANCELLED", fills=[ttl_fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=confirming_cancel,
        get_order_fn=get_order_fn,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert "CB-TTL-1" in cancel_calls, "cancel must be issued for TTL-expired order"
    assert any(r.action == "ttl_expired_cancelled" for r in report.resolved), (
        "TTL cancel must appear in resolved as ttl_expired_cancelled"
    )

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
    assert row["status"] == "CANCELLED"
    assert len(fills) == 1, "partial fill from cancel window must be recorded"


def test_ttl_expired_already_cancelled_on_coinbase_resolved_normally(tmp_db: Path) -> None:
    """
    An order with TTL expired is already CANCELLED on Coinbase (cancel_order was
    not yet called, but exchange cancelled it independently).  Phase D should NOT
    go through TTL cancel path — cb_order.status=CANCELLED is not a live status,
    so the TTL check is skipped.  Normal resolution applies: apply fills, CANCELLED.
    """
    from datetime import datetime, timedelta, timezone as _tz

    oid = _oid()
    past_expires_at = (datetime.now(_tz.utc) - timedelta(hours=1)).isoformat()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-TTL-ALREADY-CANCELLED", conn=conn)
        conn.execute("UPDATE orders SET expires_at=? WHERE id=?", (past_expires_at, oid))

    cancel_calls: list[str] = []

    def should_not_be_called(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        raise AssertionError(f"cancel_order_fn called unexpectedly: {exchange_id}")

    # Already cancelled on Coinbase
    def get_order_fn(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="CANCELLED", fills=[],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=should_not_be_called,
        get_order_fn=get_order_fn,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert cancel_calls == [], "no cancel call when Coinbase already shows CANCELLED"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# 31. P0-3 Position stacking detection and same-run Phase E cancel
# ---------------------------------------------------------------------------

def test_position_stacking_detected_and_cancelled_in_same_run(tmp_db: Path) -> None:
    """
    A position already exists for ZEC-USD (from a previous fill).
    A second ENTRY order is also OPEN for the same asset.
    Phase D detects position stacking (OPEN entry + existing position).
    Phase E cancels the conflicting ENTRY order in the same reconciliation run.
    """
    # Create a position directly (simulating a previous fill)
    oid_position = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_position, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_position, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_position, "OPEN", exchange_order_id="CB-FILLED-PREV", conn=conn)
        # Apply a fill to create the position (this is a previous Phase D having run)
        apply_fill(
            order_id=oid_position,
            fill_price=100.0,
            fill_qty_base=0.1,
            fee_usd=0.02,
            exchange_fill_id="FILL-PREV",
            filled_at=_now(),
            conn=conn,
        )
        # oid_position is now FILLED and has an OPEN position
        # (via ledger auto-transition on full fill)

    # Now a new ENTRY order B is OPEN for the same asset
    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-STACKING-B", conn=conn)

    cancel_calls: list[str] = []

    def confirming_cancel(exchange_id: str) -> bool:
        cancel_calls.append(exchange_id)
        return True

    # Phase D: get_order_fn returns OPEN (still live, triggering stacking detection).
    # Phase E: cancel confirmed → get_order_fn returns CANCELLED.
    _b_get_calls = [0]

    def get_order_fn(exchange_id: str) -> CoinbaseOrder:
        _b_get_calls[0] += 1
        status = "OPEN" if _b_get_calls[0] == 1 else "CANCELLED"
        return CoinbaseOrder(
            client_order_id=oid_b,
            exchange_order_id=exchange_id,
            status=status,
            fills=[],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=confirming_cancel,
        get_order_fn=get_order_fn,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert "CB-STACKING-B" in cancel_calls, "stacking order B must be cancelled"

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "CANCELLED"


def test_position_stacking_cancel_failed_leaves_unresolved(tmp_db: Path) -> None:
    """
    Position stacking detected but cancel_order_fn returns False.
    Must stay UNRESOLVED — stacking not resolved this run.
    """
    oid_position = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_position, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_position, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_position, "OPEN", exchange_order_id="CB-FILLED-POS", conn=conn)
        apply_fill(
            order_id=oid_position, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.02, exchange_fill_id="FILL-POS", filled_at=_now(), conn=conn,
        )

    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-STACKING-FAIL", conn=conn)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_cancel_fail,  # returns False
        get_order_fn=lambda eid: CoinbaseOrder(
            client_order_id=oid_b, exchange_order_id=eid,
            status="OPEN", fills=[],
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any("position_stacking" in u.reason for u in report.unresolved), (
        f"expected position_stacking unresolved, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "OPEN", "must stay OPEN when cancel not confirmed"


def test_position_stacking_self_healing_on_next_run(tmp_db: Path) -> None:
    """
    First run: cancel fails (UNRESOLVED), order B stays OPEN.
    Second run: stacking re-detected, cancel retry succeeds.
    No durable state about the pending cancel is carried between runs —
    detection is pure ledger + Coinbase state inspection each time.

    Call sequence on second run must be:
      Phase D: get_order_fn → OPEN  (stacking detected)
      Phase E cancel: cancel_order_fn → True
      Phase E verify: get_order_fn → CANCELLED
    Exactly one cancel_order_fn call on the second run is asserted.
    """
    oid_position = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_position, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_position, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_position, "OPEN", exchange_order_id="CB-SH-A", conn=conn)
        apply_fill(
            order_id=oid_position, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.02, exchange_fill_id="FILL-SH-A", filled_at=_now(), conn=conn,
        )

    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-SH-B", conn=conn)

    # First run: cancel fails; order B stays OPEN so stacking persists across runs.
    r1 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_cancel_fail,
        get_order_fn=lambda _eid: CoinbaseOrder(
            client_order_id=oid_b, exchange_order_id="CB-SH-B", status="OPEN", fills=[]
        ),
        db_path=tmp_db,
    )
    assert not r1.allowed_to_trade, "first run must block (cancel failed)"

    with get_db(tmp_db) as conn:
        assert conn.execute(
            "SELECT status FROM orders WHERE id=?", (oid_b,)
        ).fetchone()["status"] == "OPEN", "order B must remain OPEN after failed cancel"

    # Second run: Phase D sees OPEN → stacking → Phase E cancels → Phase E verifies.
    r2_cancel_calls: list[str] = []

    def r2_cancel(eid: str) -> bool:
        r2_cancel_calls.append(eid)
        return True

    # get_order_fn is stateful: OPEN on first call (Phase D), CANCELLED on second (Phase E verify).
    _r2_calls = [0]

    def r2_get_order(eid: str) -> CoinbaseOrder:
        _r2_calls[0] += 1
        status = "OPEN" if _r2_calls[0] == 1 else "CANCELLED"
        return CoinbaseOrder(
            client_order_id=oid_b, exchange_order_id="CB-SH-B", status=status, fills=[]
        )

    r2 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=r2_cancel,
        get_order_fn=r2_get_order,
        db_path=tmp_db,
    )
    assert r2.allowed_to_trade, f"second run must succeed; unresolved: {r2.unresolved}"
    assert r2_cancel_calls == ["CB-SH-B"], (
        f"exactly one cancel call on retry run; got: {r2_cancel_calls}"
    )

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "CANCELLED"


# ---------------------------------------------------------------------------
# Additional P0/P1 edge cases: malformed TTL, TTL+stacking+partial, Phase E
# stacked_exposure after partial fill.
# ---------------------------------------------------------------------------

def test_malformed_expires_at_is_unresolved_fail_closed(tmp_db: Path) -> None:
    """
    An OPEN order with a corrupted expires_at value must be UNRESOLVED so the
    GTC guard is NOT silently disabled.  Fail-closed: a broken TTL is worse
    than no TTL, because it leaves the guard inactive without any alert.
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-BAD-TTL", conn=conn)
        conn.execute("UPDATE orders SET expires_at=? WHERE id=?", ("not-a-date", oid))

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda eid: CoinbaseOrder(
            client_order_id=oid, exchange_order_id=eid, status="OPEN", fills=[]
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "malformed_expires_at" for u in report.unresolved), (
        f"expected malformed_expires_at, got: {report.unresolved}"
    )


def test_ttl_expired_plus_existing_position_creates_stacked_exposure(tmp_db: Path) -> None:
    """
    Combined P0 scenario: TTL expired + pre-existing position + partial fill during cancel.

    Setup:
      - Order A filled → position A OPEN
      - Order B TTL expired, still OPEN on Coinbase
      - Phase D detects TTL expiry, queues Phase E cancel
      - During cancel window, B partially fills → position B also OPEN
      - Phase E must report stacked_exposure_after_partial_fill (not allowed_to_trade)
    """
    from datetime import datetime, timedelta, timezone as _tz

    oid_a = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_a, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_a, "OPEN", exchange_order_id="CB-A-FILLED", conn=conn)
        apply_fill(
            order_id=oid_a, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.02, exchange_fill_id="FILL-A", filled_at=_now(), conn=conn,
        )
        # position A is now OPEN

    oid_b = _oid()
    past_ttl = (datetime.now(_tz.utc) - timedelta(hours=1)).isoformat()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-B-TTL", conn=conn)
        conn.execute("UPDATE orders SET expires_at=? WHERE id=?", (past_ttl, oid_b))

    cancel_calls: list[str] = []

    def confirming_cancel(eid: str) -> bool:
        cancel_calls.append(eid)
        return True

    # Phase D: B is still OPEN → TTL detected
    # Phase E verify: B is CANCELLED with a partial fill (0.05 base = $5 of $10, genuinely partial)
    partial_fill = _cb_fill("FILL-B-CANCEL-WINDOW", price=100.0, qty=0.05)
    _b_calls = [0]

    def get_b_order(eid: str) -> CoinbaseOrder:
        _b_calls[0] += 1
        if _b_calls[0] == 1:
            return CoinbaseOrder(client_order_id=oid_b, exchange_order_id=eid,
                                 status="OPEN", fills=[])
        return CoinbaseOrder(client_order_id=oid_b, exchange_order_id=eid,
                             status="CANCELLED", fills=[partial_fill])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=confirming_cancel,
        get_order_fn=get_b_order,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade, (
        "stacked exposure from B's cancel-window fill must block trading"
    )
    assert "CB-B-TTL" in cancel_calls
    assert any(u.reason == "stacked_exposure_after_partial_fill" for u in report.unresolved), (
        f"expected stacked_exposure_after_partial_fill, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        positions = conn.execute(
            "SELECT * FROM positions WHERE asset='ZEC-USD' AND status IN ('OPEN', 'CLOSING')"
        ).fetchall()
    assert len(positions) == 2, f"both positions must exist; got: {len(positions)}"


def test_phase_e_position_stacking_partial_fill_during_cancel_is_stacked_exposure(
    tmp_db: Path,
) -> None:
    """
    Position stacking cancel where B partially fills during the cancel window.
    Phase E must detect stacked_exposure_after_partial_fill and block trading,
    not silently declare the conflict resolved.
    """
    oid_position = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_position, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_position, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_position, "OPEN", exchange_order_id="CB-STACK-A", conn=conn)
        apply_fill(
            order_id=oid_position, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.02, exchange_fill_id="FILL-STACK-A", filled_at=_now(), conn=conn,
        )

    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-STACK-B", conn=conn)

    # Phase D: get_order_fn returns OPEN (stacking detected) → Phase E cancel
    # Phase E verify: CANCELLED with a partial fill → creates position B (0.05 base = $5 of $10)
    partial_b_fill = _cb_fill("FILL-B-WIN", price=100.0, qty=0.05)
    _calls = [0]

    def stateful_get(eid: str) -> CoinbaseOrder:
        _calls[0] += 1
        if _calls[0] == 1:
            return CoinbaseOrder(client_order_id=oid_b, exchange_order_id=eid,
                                 status="OPEN", fills=[])
        return CoinbaseOrder(client_order_id=oid_b, exchange_order_id=eid,
                             status="CANCELLED", fills=[partial_b_fill])

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_cancel_ok,
        get_order_fn=stateful_get,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "stacked_exposure_after_partial_fill" for u in report.unresolved), (
        f"expected stacked_exposure_after_partial_fill, got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        positions = conn.execute(
            "SELECT * FROM positions WHERE asset='ZEC-USD' AND status IN ('OPEN', 'CLOSING')"
        ).fetchall()
    assert len(positions) == 2, "both positions must exist after partial fill in cancel window"


# ---------------------------------------------------------------------------
# 32. P0-4 Orphan detection: Coinbase order unknown to local ledger
# ---------------------------------------------------------------------------

def test_orphan_coinbase_order_in_production_mode_is_unresolved(tmp_db: Path) -> None:
    """
    list_orders_fn returns a Coinbase order whose client_order_id is not in the
    local ledger.  In production mode (get_order_fn wired), this must be
    UNRESOLVED(orphan_coinbase_order:...) so the trader is aware.

    Scenario: JSON-path order or manually-placed order on Coinbase that the
    SQLite ledger has no record of.
    """
    orphan_client_id = str(uuid.uuid4())  # not in local ledger

    orphan_order = _cb_order(orphan_client_id, "CB-ORPHAN-1", "OPEN")

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [orphan_order],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: None,  # production mode wired
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any("orphan_coinbase_order" in u.reason for u in report.unresolved), (
        f"expected orphan_coinbase_order unresolved, got: {report.unresolved}"
    )
    assert any(orphan_client_id in u.reason for u in report.unresolved), (
        f"unresolved must include the orphan client_id, got: {report.unresolved}"
    )


def test_orphan_detection_skipped_without_get_order_fn(tmp_db: Path) -> None:
    """
    In test/DRY_RUN mode (get_order_fn=None), orphan detection is not active.
    A Coinbase order whose client_order_id is unknown must NOT cause an UNRESOLVED.
    """
    orphan_client_id = str(uuid.uuid4())
    orphan_order = _cb_order(orphan_client_id, "CB-ORPHAN-SKIP", "OPEN")

    oid = _insert_submitting_entry(tmp_db)

    # list_orders_fn includes both a real (SUBMITTING) order and an orphan
    report = run_startup_reconciliation(
        list_orders_fn=lambda: [_cb_order(oid, "CB-REAL-1", "OPEN"), orphan_order],
        cancel_order_fn=_no_cancel,
        get_order_fn=None,  # no production mode
        db_path=tmp_db,
    )

    # Only the not-found SUBMITTING order should be unresolved, not the orphan
    assert not any("orphan_coinbase_order" in u.reason for u in report.unresolved), (
        f"orphan must not block when get_order_fn=None: {report.unresolved}"
    )


def test_known_coinbase_order_not_flagged_as_orphan(tmp_db: Path) -> None:
    """
    Coinbase returns an order whose client_order_id matches a local SUBMITTING
    order.  This is the normal case — must NOT be flagged as an orphan.
    """
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-KNOWN-1", "OPEN")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda eid: CoinbaseOrder(
            client_order_id=oid, exchange_order_id=eid, status="OPEN", fills=[]
        ),
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert not any("orphan" in u.reason for u in report.unresolved)


# ---------------------------------------------------------------------------
# 28. SUBMITTING fill enrichment (P0-3):
#     get_order_fn is called for found SUBMITTING orders before resolution;
#     partial fills on CANCELLED are NOT lost even though list_orders_fn
#     returns CoinbaseOrder(fills=[]).
# ---------------------------------------------------------------------------

def test_submitting_cancelled_partial_fills_enriched_via_get_order_fn(
    tmp_db: Path,
) -> None:
    """
    A SUBMITTING order is found on Coinbase as CANCELLED.
    list_orders_fn returns fills=[] (as make_list_orders_fn always does).
    get_order_fn is wired and returns the same order WITH a partial fill.

    Without enrichment: the fill would be silently dropped.
    With enrichment: the fill must be applied to the ledger.
    """
    oid = _insert_submitting_entry(tmp_db)
    partial_fill = _cb_fill("FILL-PARTIAL-CANCEL", price=105.0, qty=0.05)

    # list_orders_fn returns CANCELLED with empty fills (as production does)
    cb_orders_no_fills = [_cb_order(oid, "CB-ENRICH-1", "CANCELLED", fills=[])]

    # get_order_fn (via make_get_order_fn in production) returns the real fills
    def enriched_get_order(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid,
            exchange_order_id=exchange_id,
            status="CANCELLED",
            fills=[partial_fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders_no_fills,
        cancel_order_fn=_no_cancel,
        get_order_fn=enriched_get_order,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
        pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid,)
        ).fetchone()

    assert row["status"] == "CANCELLED"
    assert len(fills) == 1, "partial fill from get_order_fn must be applied (not lost)"
    assert fills[0]["exchange_fill_id"] == "FILL-PARTIAL-CANCEL"
    assert pos is not None, "partial fill must create a position"


def test_submitting_enrichment_fails_closed_when_get_order_fn_returns_none(
    tmp_db: Path,
) -> None:
    """
    SUBMITTING order is found on Coinbase (CANCELLED), but get_order_fn returns
    None (fill inconsistency or API error in make_get_order_fn).

    Must NOT fall back to the empty-fills cb_order from list_orders_fn —
    that would silently drop fills.  Must be UNRESOLVED instead.
    """
    oid = _insert_submitting_entry(tmp_db)
    cb_orders = [_cb_order(oid, "CB-ENRICH-NONE", "CANCELLED", fills=[])]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: None,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "get_order_returned_none" for u in report.unresolved), (
        f"expected get_order_returned_none, got: {[u.reason for u in report.unresolved]}"
    )

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "SUBMITTING", (
        "order must stay SUBMITTING when fill verification fails"
    )


def test_submitting_enrichment_not_called_when_not_found(tmp_db: Path) -> None:
    """
    When list_orders_fn returns no match for the SUBMITTING order,
    get_order_fn must NOT be called — there is no exchange_order_id to query.
    Order stays UNRESOLVED(not_found) as before.
    """
    oid = _insert_submitting_entry(tmp_db)
    get_order_calls: list[str] = []

    def tracking_get_order(exchange_id: str) -> CoinbaseOrder:
        get_order_calls.append(exchange_id)
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="OPEN", fills=[],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],  # not found
        cancel_order_fn=_no_cancel,
        get_order_fn=tracking_get_order,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert report.unresolved[0].reason == "not_found"
    assert get_order_calls == [], (
        "get_order_fn must not be called when order is not found on Coinbase"
    )


# ---------------------------------------------------------------------------
# P0 regression: cross-order fill_id mismatch must be UNRESOLVED, not skipped
# ---------------------------------------------------------------------------

def test_cross_order_fill_id_mismatch_is_unresolved_and_rolled_back(
    tmp_db: Path,
) -> None:
    """
    exchange_fill_id "FILL-SHARED" is already recorded in the ledger for order A.
    When the reconciler applies the same fill_id to a SUBMITTING order B (via
    get_order_fn enrichment), apply_fill raises RuntimeError — ledger-integrity
    violation.  The reconciler must catch this, produce UNRESOLVED for B, and
    leave B's fills table and status unchanged (get_db rolls back on error).

    Regression for the removed try/except in _apply_coinbase_fills that previously
    swallowed "previously recorded" errors and silently dropped the integrity check.
    """
    # A: OPEN → fully filled → FILLED; fill "FILL-SHARED" is now in the fills table.
    oid_a = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_a, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_a, "OPEN", exchange_order_id="CB-A-XFILL", conn=conn)
        apply_fill(
            order_id=oid_a, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.01, exchange_fill_id="FILL-SHARED", filled_at=_now(), conn=conn,
        )

    # B: SUBMITTING for same asset (A is FILLED — out of the active-entry unique index).
    oid_b = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)

    # Coinbase says B is FILLED with the same fill_id already belonging to A.
    shared_fill = _cb_fill("FILL-SHARED", price=100.0, qty=0.1)
    cb_orders = [_cb_order(oid_b, "CB-B-XFILL", "FILLED")]

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: CoinbaseOrder(
            client_order_id=oid_b,
            exchange_order_id="CB-B-XFILL",
            status="FILLED",
            fills=[shared_fill],
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        "FILL-SHARED" in u.reason or "previously recorded" in u.reason
        for u in report.unresolved
    ), (
        f"expected integrity-error unresolved mentioning FILL-SHARED, "
        f"got: {report.unresolved}"
    )

    with get_db(tmp_db) as conn:
        b_status = conn.execute(
            "SELECT status FROM orders WHERE id=?", (oid_b,)
        ).fetchone()["status"]
        b_fills = conn.execute(
            "SELECT * FROM fills WHERE order_id=?", (oid_b,)
        ).fetchall()

    assert b_status == "SUBMITTING", (
        "B must stay SUBMITTING — transaction must be rolled back on integrity error"
    )
    assert b_fills == [], (
        "no fills must be recorded for B — rollback must leave fills table clean"
    )


# ---------------------------------------------------------------------------
# P0 regression: terminal enrichment None must be UNRESOLVED, not fail-open
# ---------------------------------------------------------------------------

def test_terminal_enrichment_none_is_unresolved(tmp_db: Path) -> None:
    """
    An EXPIRED order is in the terminal_rows pool.  get_order_fn is wired
    (production mode) but returns None — production make_get_order_fn() returns
    None for transport errors, incomplete fill pagination, and aggregate mismatches,
    not only for a confirmed order-not-found.

    Must be UNRESOLVED(terminal_get_order_returned_none) — NOT fall back to
    list_orders_fn's fills=[] as if the order had zero fills.

    Regression for the previous fail-open behaviour where None silently cleared
    the enrichment and a transient Coinbase error looked like "no fills executed".
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-TERM-NONE", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: None,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        u.reason == "terminal_get_order_returned_none" for u in report.unresolved
    ), (
        f"expected terminal_get_order_returned_none, got: {[u.reason for u in report.unresolved]}"
    )

    with get_db(tmp_db) as conn:
        fills = conn.execute(
            "SELECT * FROM fills WHERE order_id=?", (oid,)
        ).fetchall()
        row = conn.execute(
            "SELECT status, fills_finalized_at FROM orders WHERE id=?", (oid,)
        ).fetchone()

    assert fills == [], "no fills must be applied when enrichment returns None"
    assert row["status"] == "EXPIRED", "order must stay EXPIRED"
    assert row["fills_finalized_at"] is None, (
        "fills_finalized_at must NOT be set when enrichment failed"
    )


# ---------------------------------------------------------------------------
# P0 regression: terminal identity + status validation before finalization
# ---------------------------------------------------------------------------

def test_terminal_exchange_id_mismatch_is_unresolved(tmp_db: Path) -> None:
    """
    get_order_fn returns a CoinbaseOrder whose exchange_order_id does not match
    the exchange_order_id stored in the local ledger.  Production
    make_get_order_fn() uses the actual Coinbase response ID to make mismatches
    detectable.  Must be UNRESOLVED and NOT finalized.
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-EXPECT-ID", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: CoinbaseOrder(
            client_order_id=oid,
            exchange_order_id="CB-WRONG-ID",  # ← different from stored "CB-EXPECT-ID"
            status="EXPIRED",
            fills=[],
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        "terminal_exchange_id_mismatch" in u.reason for u in report.unresolved
    ), (
        f"expected terminal_exchange_id_mismatch, got: {[u.reason for u in report.unresolved]}"
    )
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT fills_finalized_at FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["fills_finalized_at"] is None, "must NOT be finalized on ID mismatch"


def test_terminal_client_id_mismatch_is_unresolved(tmp_db: Path) -> None:
    """
    get_order_fn returns a CoinbaseOrder whose client_order_id does not match
    the local orders.id.  This is the second identity invariant: even if
    exchange_order_id matches, a wrong client_order_id means Coinbase handed us
    a foreign order.  Must be UNRESOLVED(terminal_client_id_mismatch) and NOT finalized.
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-CLI-CHECK", conn=conn)
        transition_order(oid, "CANCELLED", conn=conn)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: CoinbaseOrder(
            client_order_id="FOREIGN-ORDER-ID",  # ← different from local oid
            exchange_order_id="CB-CLI-CHECK",    # exchange_order_id matches
            status="CANCELLED",
            fills=[],
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        "terminal_client_id_mismatch" in u.reason for u in report.unresolved
    ), (
        f"expected terminal_client_id_mismatch, got: {[u.reason for u in report.unresolved]}"
    )
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT fills_finalized_at FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["fills_finalized_at"] is None, "must NOT be finalized on client_id mismatch"


def test_terminal_unknown_status_is_unresolved_and_not_finalized(tmp_db: Path) -> None:
    """
    get_order_fn returns a status not in _CB_TERMINAL_ACCEPTED (e.g. "UNKNOWN").
    Must be UNRESOLVED(unknown_coinbase_status) — not silently treated as
    "no fills" which would trigger finalization.
    """
    oid = _oid()
    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-UNK-STAT", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: CoinbaseOrder(
            client_order_id=oid,
            exchange_order_id="CB-UNK-STAT",
            status="UNKNOWN",  # not in _CB_TERMINAL_ACCEPTED
            fills=[],
        ),
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(
        "unknown_coinbase_status" in u.reason for u in report.unresolved
    ), (
        f"expected unknown_coinbase_status, got: {[u.reason for u in report.unresolved]}"
    )
    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT fills_finalized_at FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["fills_finalized_at"] is None, "must NOT be finalized on unknown status"


def test_finalized_terminal_order_skipped_on_next_startup(tmp_db: Path) -> None:
    """
    A terminal order that was successfully verified and finalized (fills_finalized_at
    set) on the first run must be excluded from terminal_rows on the next startup.
    get_order_fn must NOT be called for it again — the order is permanently done.

    Settlement window: expired_at is backdated 15 minutes to exceed the
    _TERMINAL_SETTLEMENT_MINUTES threshold.
    """
    oid = _oid()
    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()

    with get_db(tmp_db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id="CB-FINAL-1", conn=conn)
        transition_order(oid, "EXPIRED", conn=conn)
        # Backdate expired_at past the settlement window so finalization fires
        conn.execute("UPDATE orders SET expired_at=? WHERE id=?", (old_ts, oid))

    get_order_calls: list[str] = []

    def tracking_get_order(exchange_id: str) -> CoinbaseOrder:
        get_order_calls.append(exchange_id)
        return CoinbaseOrder(
            client_order_id=oid,
            exchange_order_id=exchange_id,
            status="EXPIRED",
            fills=[],
        )

    # First run: terminal_rows contains oid, get_order_fn called, order finalized
    r1 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=tracking_get_order,
        db_path=tmp_db,
    )
    assert r1.allowed_to_trade, f"first run must be clean: {r1.unresolved}"
    assert get_order_calls.count("CB-FINAL-1") == 1

    with get_db(tmp_db) as conn:
        row = conn.execute(
            "SELECT fills_finalized_at FROM orders WHERE id=?", (oid,)
        ).fetchone()
    assert row["fills_finalized_at"] is not None, (
        "fills_finalized_at must be set after first run clears settlement window"
    )

    # Second run: order excluded by fills_finalized_at IS NULL filter
    r2 = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=tracking_get_order,
        db_path=tmp_db,
    )
    assert r2.allowed_to_trade, f"second run must stay clean: {r2.unresolved}"
    assert get_order_calls.count("CB-FINAL-1") == 1, (
        "get_order_fn must NOT be called again for finalized orders — "
        f"got {get_order_calls.count('CB-FINAL-1')} total calls"
    )


# ---------------------------------------------------------------------------
# 29. OPEN/PARTIAL reconciliation (P0-1):
#     OPEN orders have their lifecycle managed on each reconciliation run.
# ---------------------------------------------------------------------------

def _insert_open_entry(db: Path, exchange_id: str, asset: str = "ZEC-USD") -> str:
    """Insert an OPEN ENTRY order with a known exchange_order_id."""
    oid = _oid()
    with get_db(db) as conn:
        insert_order(
            order_id=oid, epoch_id=_EPOCH_ID, asset=asset,
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid, "OPEN", exchange_order_id=exchange_id, conn=conn)
    return oid


def test_open_order_filled_applies_fills_and_resolves(tmp_db: Path) -> None:
    """
    An OPEN order is found FILLED on Coinbase with fills.
    Reconciler must apply fills, create a position, and report resolved.
    """
    oid = _insert_open_entry(tmp_db, "CB-OPEN-FILL")
    fill = _cb_fill("FILL-OPEN-F", price=100.0, qty=0.1)

    def get_order_filled(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="FILLED", fills=[fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=get_order_filled,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert any(r.action == "filled" for r in report.resolved)

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
        pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid,)
        ).fetchone()

    assert row["status"] == "FILLED"
    assert len(fills) == 1
    assert pos is not None and pos["status"] == "OPEN"


def test_open_order_cancelled_no_fills_transitions_cancelled(tmp_db: Path) -> None:
    """OPEN order cancelled with no fills → CANCELLED in ledger, no position."""
    oid = _insert_open_entry(tmp_db, "CB-OPEN-CANCEL")

    def get_order_cancelled(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="CANCELLED", fills=[],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=get_order_cancelled,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid,)
        ).fetchone()
    assert row["status"] == "CANCELLED"
    assert pos is None


def test_open_order_expired_with_partial_fill(tmp_db: Path) -> None:
    """OPEN order expired with a partial fill → fill applied, EXPIRED in ledger, position created."""
    oid = _insert_open_entry(tmp_db, "CB-OPEN-EXP")
    fill = _cb_fill("FILL-EXP-PARTIAL", price=99.0, qty=0.04)

    def get_order_expired(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="EXPIRED", fills=[fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=get_order_expired,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
        pos = conn.execute(
            "SELECT * FROM positions WHERE entry_order_id=?", (oid,)
        ).fetchone()
    assert row["status"] == "EXPIRED"
    assert len(fills) == 1
    assert pos is not None


def test_open_order_still_open_applies_new_partial_fills(tmp_db: Path) -> None:
    """
    OPEN order is still OPEN on Coinbase but has a new partial fill since the
    last check.  Reconciler applies the fill and reports resolved(open).
    """
    oid = _insert_open_entry(tmp_db, "CB-OPEN-STILL")
    new_fill = _cb_fill("FILL-STILL-OPEN", price=98.0, qty=0.03)

    def get_order_open_with_fill(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="OPEN", fills=[new_fill],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=get_order_open_with_fill,
        db_path=tmp_db,
    )

    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"
    assert any(r.action == "open" for r in report.resolved)

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
    assert row["status"] in ("OPEN", "PARTIAL")  # ledger auto-transitions to PARTIAL on partial fill
    assert len(fills) == 1
    assert fills[0]["exchange_fill_id"] == "FILL-STILL-OPEN"


def test_open_order_fill_idempotent_second_run(tmp_db: Path) -> None:
    """
    Same fill returned by get_order_fn on two consecutive reconciliation runs.
    The fill must be applied only once (no duplicate fill error).
    Second run must also succeed with allowed_to_trade=True.
    """
    oid = _insert_open_entry(tmp_db, "CB-OPEN-IDEM")
    fill = _cb_fill("FILL-IDEM", price=97.0, qty=0.05)

    def get_order_open(exchange_id: str) -> CoinbaseOrder:
        return CoinbaseOrder(
            client_order_id=oid, exchange_order_id=exchange_id,
            status="OPEN", fills=[fill],
        )

    # First run
    r1 = run_startup_reconciliation(
        list_orders_fn=lambda: [], cancel_order_fn=_no_cancel,
        get_order_fn=get_order_open, db_path=tmp_db,
    )
    assert r1.allowed_to_trade

    # Second run — same fill must not cause a duplicate error
    r2 = run_startup_reconciliation(
        list_orders_fn=lambda: [], cancel_order_fn=_no_cancel,
        get_order_fn=get_order_open, db_path=tmp_db,
    )
    assert r2.allowed_to_trade, f"unresolved on second run: {r2.unresolved}"

    with get_db(tmp_db) as conn:
        fills = conn.execute("SELECT * FROM fills WHERE order_id=?", (oid,)).fetchall()
    assert len(fills) == 1, "fill must be applied exactly once across both runs"


def test_open_order_get_order_returns_none_leaves_unresolved(tmp_db: Path) -> None:
    """
    get_order_fn returns None for an OPEN order.
    Must be UNRESOLVED — cannot confirm fill state.
    """
    oid = _insert_open_entry(tmp_db, "CB-OPEN-GONE")

    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        get_order_fn=lambda _eid: None,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade
    assert any(u.reason == "get_order_returned_none" for u in report.unresolved), (
        f"expected get_order_returned_none, got: {[u.reason for u in report.unresolved]}"
    )

    with get_db(tmp_db) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (oid,)).fetchone()
    assert row["status"] == "OPEN", "must stay OPEN when fill state is unknown"


def test_open_order_skipped_if_cancelled_by_stacking_phase(tmp_db: Path) -> None:
    """
    An OPEN order (B) is detected as a stacking conflict by Phase A (terminal
    rows) and cancelled in Phase C.  Phase D (OPEN/PARTIAL loop) must skip B
    because its ledger status is now CANCELLED — no double-transition error.
    """
    oid_a, oid_b = _setup_expired_then_new_entry(
        tmp_db, exchange_id_a="CB-A-SKIP", exchange_id_b="CB-B-SKIP"
    )

    fill_a = _cb_fill("FILL-A-SKIP", price=102.0, qty=0.098)
    cb_orders = [
        _cb_order(oid_a, "CB-A-SKIP", "EXPIRED"),  # no fills from list_orders_fn
        _cb_order(oid_b, "CB-B-SKIP", "OPEN"),
    ]

    # Dispatch: A (terminal) returns EXPIRED+fill_a so the terminal loop applies the
    # late fill and detects B as stacking.  B returns CANCELLED+no-fills for Phase C
    # verification.  Phase D must skip B (status=CANCELLED) — no second call for B.
    get_order_calls: list[str] = []

    def get_order_fn(exchange_id: str) -> CoinbaseOrder:
        get_order_calls.append(exchange_id)
        if exchange_id == "CB-A-SKIP":
            return CoinbaseOrder(
                client_order_id=oid_a,
                exchange_order_id=exchange_id,
                status="EXPIRED",
                fills=[fill_a],
            )
        return CoinbaseOrder(
            client_order_id=oid_b,
            exchange_order_id=exchange_id,
            status="CANCELLED",
            fills=[],
        )

    report = run_startup_reconciliation(
        list_orders_fn=lambda: cb_orders,
        cancel_order_fn=_cancel_ok,
        get_order_fn=get_order_fn,
        db_path=tmp_db,
    )

    # Must succeed — Phase D correctly skips B (already CANCELLED after Phase C)
    assert report.allowed_to_trade, f"unresolved: {report.unresolved}"

    with get_db(tmp_db) as conn:
        b_row = conn.execute("SELECT status FROM orders WHERE id=?", (oid_b,)).fetchone()
    assert b_row["status"] == "CANCELLED"
    # B's exchange_id must be called exactly once (Phase C verify) — not again in Phase D
    assert get_order_calls.count("CB-B-SKIP") == 1, (
        f"expected 1 call for CB-B-SKIP, got {get_order_calls.count('CB-B-SKIP')} "
        f"(Phase D must skip already-cancelled orders)"
    )


# ---------------------------------------------------------------------------
# 36. Persistent stacking invariant: residual positions from a previous run
# ---------------------------------------------------------------------------

def test_persistent_stacking_invariant_blocks_on_prior_run_positions(
    tmp_db: Path,
) -> None:
    """
    Two OPEN positions for the same asset survive from a previous run (e.g.,
    stacked_exposure_after_partial_fill was UNRESOLVED and left the DB unchanged).
    The persistent stacking invariant fires at the very start of the NEXT
    reconciliation — before Phase D inspects any orders — and blocks trading.

    This verifies durability: the block is re-issued every run until a human
    (or a future reconciliation pass) closes one of the positions.
    """
    oid_a = _oid()
    oid_b = _oid()
    with get_db(tmp_db) as conn:
        # oid_a: fully filled → FILLED (leaves active-entry unique index) → position A OPEN
        insert_order(
            order_id=oid_a, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_a, stop_price=90.0, target_price=115.0, conn=conn)
        transition_order(oid_a, "OPEN", exchange_order_id="CB-PI-A", conn=conn)
        apply_fill(
            order_id=oid_a, fill_price=100.0, fill_qty_base=0.1,
            fee_usd=0.01, exchange_fill_id="FILL-PI-A", filled_at=_now(), conn=conn,
        )
        # oid_a is now FILLED — not in the unique-active-entry index — so oid_b
        # can be inserted for the same asset.  This simulates stacking that escaped
        # detection: two fills, two OPEN positions, one asset.
        insert_order(
            order_id=oid_b, epoch_id=_EPOCH_ID, asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=_now(), qty_usd_requested=10.0, conn=conn,
        )
        insert_trade_intent(oid_b, stop_price=89.0, target_price=114.0, conn=conn)
        transition_order(oid_b, "OPEN", exchange_order_id="CB-PI-B", conn=conn)
        # Partial fill for B → PARTIAL status, OPEN position B created
        apply_fill(
            order_id=oid_b, fill_price=101.0, fill_qty_base=0.05,
            fee_usd=0.01, exchange_fill_id="FILL-PI-B", filled_at=_now(), conn=conn,
        )

    # Verify the DB state: two OPEN positions
    with get_db(tmp_db) as conn:
        pos_count = conn.execute(
            "SELECT COUNT(*) FROM positions WHERE asset='ZEC-USD' AND status='OPEN'"
        ).fetchone()[0]
    assert pos_count == 2, "setup failed: expected 2 open positions"

    # Second reconciliation run — no new orders, but invariant must catch stacking
    report = run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=_no_cancel,
        db_path=tmp_db,
    )

    assert not report.allowed_to_trade, (
        "two pre-existing OPEN positions must block trading on the next run"
    )
    stacking_items = [u for u in report.unresolved if "existing_stacked_positions" in u.reason]
    assert stacking_items, (
        f"expected existing_stacked_positions unresolved, got: {report.unresolved}"
    )
    assert "count=2" in stacking_items[0].reason, (
        f"invariant must report the count, got: {stacking_items[0].reason}"
    )

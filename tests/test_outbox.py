"""
Tests for pipeline/outbox.py — two-transaction outbox for order placement.

Each test exercises one crash or race window in the TX-A → Coinbase → TX-B flow.

Fixture contract:
  tmp_db  — fresh V4 ledger DB (no epoch)
  tmp_db_ep — fresh V4 ledger DB with epoch "EP1" (paper_capital=1000)
"""
from __future__ import annotations

import sqlite3
import threading
import uuid
from pathlib import Path

import pytest

from pipeline.ledger import (
    apply_fill,
    get_db,
    get_trade_intent,
    insert_epoch,
    run_migrations,
    transition_order,
)
from pipeline.outbox import CoinbaseRejected, PlacementBlocked, PlaceResult, place_order_outbox
from pipeline.reconciler import run_startup_reconciliation

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_EPOCH_ID = "EP1"
_CAPITAL  = 1000.0


def _base_kwargs(db: Path, **overrides) -> dict:
    """Minimal valid kwargs for place_order_outbox().

    gate_freshness_minutes=None disables the reconciliation gate so tests
    that exercise other outbox mechanics don't need a prior reconciliation run.
    Gate-specific tests use tmp_db_ep_recon and omit this override.
    """
    kw: dict = dict(
        asset="ZEC-USD",
        limit_price=100.0,
        qty_usd=10.0,
        stop_price=90.0,
        target_price=115.0,
        coinbase_fn=lambda cid: f"CB-{cid[:8]}",
        db_path=db,
        gate_freshness_minutes=None,  # disabled; gate tests use tmp_db_ep_recon
    )
    kw.update(overrides)
    return kw


def _accepted(cid: str) -> str:
    return f"CB-{cid[:8]}"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path: Path) -> Path:
    db = tmp_path / "ledger.db"
    run_migrations(db)
    return db


@pytest.fixture
def tmp_db_ep(tmp_db: Path) -> Path:
    with get_db(tmp_db) as conn:
        insert_epoch(_EPOCH_ID, _CAPITAL, "test epoch", conn=conn)
    return tmp_db


# ---------------------------------------------------------------------------
# 1. TX-A committed before Coinbase call — crash before coinbase_fn runs
# ---------------------------------------------------------------------------

def test_tx_a_committed_before_coinbase_call(tmp_db_ep: Path) -> None:
    """
    Even if coinbase_fn raises before doing anything (simulates crash after TX-A),
    TX-A is already committed: the order is in the DB as SUBMITTING.
    """
    def crash_before_api(cid: str) -> str:
        raise RuntimeError("process died before reaching Coinbase")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=crash_before_api))

    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None

    with get_db(tmp_db_ep) as conn:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?",
            (result.order_id,),
        ).fetchone()
    assert row is not None, "order must be in DB after TX-A"
    assert row["status"] == "SUBMITTING"
    assert row["exchange_order_id"] is None


def test_trade_intent_persisted_with_tx_a(tmp_db_ep: Path) -> None:
    """stop/target written in TX-A even if coinbase_fn crashes."""
    def crash(cid): raise RuntimeError("crash")

    result = place_order_outbox(
        **_base_kwargs(tmp_db_ep, stop_price=88.0, target_price=130.0, coinbase_fn=crash)
    )

    with get_db(tmp_db_ep) as conn:
        intent = get_trade_intent(result.order_id, conn)
    assert intent["stop_price"] == 88.0
    assert intent["target_price"] == 130.0


# ---------------------------------------------------------------------------
# 2. Coinbase accepted → timeout before response — leave SUBMITTING
# ---------------------------------------------------------------------------

def test_timeout_after_coinbase_acceptance_leaves_submitting(tmp_db_ep: Path) -> None:
    """
    Coinbase accepted the order but the connection dropped before we received
    the exchange_order_id.  TimeoutError → order stays SUBMITTING.
    """
    def drops_after_acceptance(cid: str) -> str:
        raise TimeoutError("connection dropped after Coinbase accepted")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=drops_after_acceptance))

    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None

    with get_db(tmp_db_ep) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (result.order_id,)).fetchone()
    assert row["status"] == "SUBMITTING"


# ---------------------------------------------------------------------------
# 3. Crash after Coinbase response, before TX-B — TX-A was committed
# ---------------------------------------------------------------------------

def test_crash_after_coinbase_response_before_tx_b(tmp_db_ep: Path) -> None:
    """
    Simulates: Coinbase returned exchange_order_id, then process crashed before TX-B.
    Represented here as: coinbase_fn raises AFTER computing the result (mid-call crash).
    TX-A is committed → order is SUBMITTING → reconciler resolves via client_order_id.
    """
    accepted_ids: list[str] = []

    def accepted_then_crash(cid: str) -> str:
        accepted_ids.append(cid)
        # Simulates crash between receiving Coinbase response and running TX-B
        raise ConnectionResetError("process crashed after receiving response")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=accepted_then_crash))

    assert len(accepted_ids) == 1, "coinbase_fn was called exactly once"
    assert result.status == "SUBMITTING"

    with get_db(tmp_db_ep) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (result.order_id,)).fetchone()
    # TX-A persisted the order; it's SUBMITTING — reconciler will attach exchange_order_id
    assert row["status"] == "SUBMITTING"


# ---------------------------------------------------------------------------
# 4. Duplicate client_order_id — idempotent, Coinbase not called again
# ---------------------------------------------------------------------------

def test_duplicate_order_id_is_idempotent(tmp_db_ep: Path) -> None:
    """
    If place_order_outbox() is called with an order_id that already exists,
    it returns the current state without touching Coinbase.
    """
    order_id = str(uuid.uuid4())
    coinbase_call_count = [0]

    def counting_accepted(cid: str) -> str:
        coinbase_call_count[0] += 1
        return f"CB-{cid[:8]}"

    # First call
    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=counting_accepted))
    assert r1.status == "OPEN"
    assert coinbase_call_count[0] == 1

    # Second call with same order_id — must NOT call Coinbase again
    r2 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=counting_accepted))
    assert r2.status == "OPEN"
    assert r2.order_id == order_id
    assert r2.exchange_order_id == r1.exchange_order_id
    assert coinbase_call_count[0] == 1, "Coinbase must not be called for duplicate order_id"


def test_duplicate_submitting_order_id_is_idempotent(tmp_db_ep: Path) -> None:
    """
    If a previous call left the order SUBMITTING (timeout), a second call with
    the same order_id returns SUBMITTING without creating a new order.
    """
    order_id = str(uuid.uuid4())
    coinbase_call_count = [0]

    def timeout_fn(cid: str) -> str:
        coinbase_call_count[0] += 1
        raise TimeoutError("always times out")

    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=timeout_fn))
    assert r1.status == "SUBMITTING"
    assert coinbase_call_count[0] == 1

    r2 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=timeout_fn))
    assert r2.status == "SUBMITTING"
    assert coinbase_call_count[0] == 1  # not called again — idempotency gate in TX-A

    with get_db(tmp_db_ep) as conn:
        count = conn.execute(
            "SELECT COUNT(*) FROM orders WHERE id=?", (order_id,)
        ).fetchone()[0]
    assert count == 1, "no duplicate rows in orders table"


# ---------------------------------------------------------------------------
# 5. Immediate full fill — fill succeeds after TX-B transitions to OPEN
# ---------------------------------------------------------------------------

def test_fill_on_submitting_raises_fill_succeeds_after_tx_b(tmp_db_ep: Path) -> None:
    """
    An immediate fill that arrives during the SUBMITTING window is blocked by
    apply_fill()'s SUBMITTING guard.  After TX-B transitions the order to OPEN,
    the fill is applied normally.
    """
    # Simulate: order inserted by TX-A but TX-B not yet run
    order_id = str(uuid.uuid4())
    from pipeline.ledger import insert_order
    import datetime as _dt
    now = _dt.datetime.now(_dt.timezone.utc)
    with get_db(tmp_db_ep) as conn:
        ep = conn.execute("SELECT epoch_id FROM risk_epochs WHERE ended_at IS NULL").fetchone()
        insert_order(
            order_id=order_id, epoch_id=ep["epoch_id"], asset="ZEC-USD",
            side="BUY", order_type="LIMIT", purpose="ENTRY",
            placed_at=now.isoformat(), qty_base_requested=0.1, conn=conn,
        )

    # Fill on SUBMITTING must raise
    with pytest.raises(RuntimeError, match="SUBMITTING"):
        with get_db(tmp_db_ep) as conn:
            apply_fill(order_id=order_id, fill_price=100.0, fill_qty_base=0.1, conn=conn)

    # TX-B: transition to OPEN
    with get_db(tmp_db_ep) as conn:
        transition_order(order_id, "OPEN", exchange_order_id="CB-FILL-TEST", conn=conn)

    # Fill now succeeds
    with get_db(tmp_db_ep) as conn:
        r = apply_fill(order_id=order_id, fill_price=100.0, fill_qty_base=0.1, conn=conn)
    assert r["status"] in ("PARTIAL", "FILLED")


def test_full_happy_path_with_immediate_fill(tmp_db_ep: Path) -> None:
    """End-to-end: outbox places OPEN order, then a fill is applied successfully."""
    result = place_order_outbox(
        **_base_kwargs(tmp_db_ep, coinbase_fn=_accepted)
    )
    assert result.status == "OPEN"

    with get_db(tmp_db_ep) as conn:
        fill_r = apply_fill(
            order_id=result.order_id,
            fill_price=100.0,
            fill_qty_base=0.1,
            exchange_fill_id="FILL-HAPPY",
            conn=conn,
        )
    assert fill_r["status"] in ("PARTIAL", "FILLED")
    assert fill_r["position_id"] is not None


# ---------------------------------------------------------------------------
# 6. Definite rejection — REJECTED state, TX-B committed
# ---------------------------------------------------------------------------

def test_definite_rejection_transitions_to_rejected(tmp_db_ep: Path) -> None:
    """CoinbaseRejected causes TX-B to record REJECTED status."""
    def reject(cid: str) -> str:
        raise CoinbaseRejected("INSUFFICIENT_FUND")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=reject))

    assert result.status == "REJECTED"
    assert result.rejection_reason == "INSUFFICIENT_FUND"
    assert result.exchange_order_id is None

    with get_db(tmp_db_ep) as conn:
        row = conn.execute(
            "SELECT status, rejected_at FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["status"] == "REJECTED"
    assert row["rejected_at"] is not None


def test_rejected_order_cannot_be_filled(tmp_db_ep: Path) -> None:
    """REJECTED is terminal — apply_fill must raise even with reconciliation_mode."""
    def reject(cid): raise CoinbaseRejected("post-only would cross")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=reject))
    assert result.status == "REJECTED"

    with pytest.raises(RuntimeError, match="Cannot fill"):
        with get_db(tmp_db_ep) as conn:
            apply_fill(
                order_id=result.order_id,
                fill_price=100.0,
                fill_qty_base=0.1,
                conn=conn,
            )


# ---------------------------------------------------------------------------
# 7. Parallel scheduler runs — gate check prevents stacking; BEGIN IMMEDIATE
#    guarantees only one writer passes the check
# ---------------------------------------------------------------------------

def test_stacking_guard_blocks_second_entry_for_same_asset(tmp_db_ep: Path) -> None:
    """
    Sequential second call for the same asset raises PlacementBlocked.
    The first call succeeds; the gate check inside TX-A sees the OPEN order.
    """
    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))
    assert r1.status == "OPEN"

    with pytest.raises(PlacementBlocked, match="ZEC-USD"):
        place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))


def test_stacking_guard_blocks_on_submitting_order(tmp_db_ep: Path) -> None:
    """
    If a previous call left an order SUBMITTING (timeout), a new placement
    for the same asset must be blocked — the SUBMITTING order is still active.
    """
    def timeout_fn(cid): raise TimeoutError("timeout")

    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=timeout_fn))
    assert r1.status == "SUBMITTING"

    with pytest.raises(PlacementBlocked, match="ZEC-USD"):
        place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))


def test_stacking_guard_allows_different_asset(tmp_db_ep: Path) -> None:
    """Placing a second order for a DIFFERENT asset must succeed."""
    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, asset="ZEC-USD", coinbase_fn=_accepted))
    r2 = place_order_outbox(**_base_kwargs(tmp_db_ep, asset="ETH-USD", coinbase_fn=_accepted))
    assert r1.status == "OPEN"
    assert r2.status == "OPEN"
    assert r1.order_id != r2.order_id


def test_parallel_placements_same_asset_one_blocked(tmp_db_ep: Path) -> None:
    """
    Two concurrent calls for the same asset: exactly one succeeds (OPEN),
    the other raises PlacementBlocked.  BEGIN IMMEDIATE serialises TX-A so
    no duplicate orders are created.
    """
    results: list[PlaceResult] = []
    blocked: list[PlacementBlocked] = []
    other_errors: list[BaseException] = []

    def place_once() -> None:
        try:
            r = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))
            results.append(r)
        except PlacementBlocked as exc:
            blocked.append(exc)
        except BaseException as exc:
            other_errors.append(exc)

    threads = [threading.Thread(target=place_once) for _ in range(2)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert not other_errors, f"unexpected errors: {other_errors}"
    assert len(results) == 1, "exactly one placement must succeed"
    assert len(blocked) == 1, "exactly one placement must be blocked"
    assert results[0].status == "OPEN"

    with get_db(tmp_db_ep) as conn:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count == 1, "only one order must be in the DB"


# ---------------------------------------------------------------------------
# 8. SQLite lock during TX-A — busy-wait then succeed
# ---------------------------------------------------------------------------

def test_tx_a_waits_for_write_lock_and_succeeds(tmp_db_ep: Path) -> None:
    """
    A brief exclusive write lock held by another connection delays TX-A.
    After the lock is released, TX-A acquires it and completes normally.
    """
    release = threading.Event()
    locked  = threading.Event()

    def hold_lock() -> None:
        conn = sqlite3.connect(str(tmp_db_ep), isolation_level=None)
        conn.execute("BEGIN IMMEDIATE")
        locked.set()
        release.wait(timeout=5.0)
        conn.execute("ROLLBACK")
        conn.close()

    locker = threading.Thread(target=hold_lock, daemon=True)
    locker.start()
    locked.wait()  # ensure lock is held before calling outbox

    try:
        # Release after a short delay so outbox can proceed
        def release_soon() -> None:
            import time
            time.sleep(0.05)
            release.set()
        threading.Thread(target=release_soon, daemon=True).start()

        result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))
        assert result.status == "OPEN"
    finally:
        release.set()
        locker.join()


# ---------------------------------------------------------------------------
# 9. No SQLite transaction held during the network request
# ---------------------------------------------------------------------------

def test_no_sqlite_write_lock_held_during_coinbase_call(tmp_db_ep: Path) -> None:
    """
    coinbase_fn must be able to open a BEGIN IMMEDIATE write transaction while
    outbox is between TX-A and TX-B.  If TX-A were still open (lock held),
    this would timeout and raise OperationalError.
    """
    write_acquired_inside_coinbase = False

    def probe_fn(cid: str) -> str:
        nonlocal write_acquired_inside_coinbase
        # A direct SQLite connection with a very short timeout — would fail
        # immediately if the outbox is holding the write lock from TX-A.
        probe = sqlite3.connect(str(tmp_db_ep), isolation_level=None, timeout=0.5)
        try:
            probe.execute("BEGIN IMMEDIATE")
            probe.execute("ROLLBACK")
            write_acquired_inside_coinbase = True
        finally:
            probe.close()
        return f"CB-{cid[:8]}"

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=probe_fn))

    assert result.status == "OPEN"
    assert write_acquired_inside_coinbase, (
        "coinbase_fn must be able to acquire a write lock — "
        "TX-A must be committed (lock released) before coinbase_fn is called"
    )


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------

def test_no_active_epoch_raises_before_tx_a(tmp_db: Path) -> None:
    """place_order_outbox() raises immediately if there is no active epoch."""
    with pytest.raises(RuntimeError, match="no active risk epoch"):
        place_order_outbox(**_base_kwargs(tmp_db, coinbase_fn=_accepted))

    # No order should have been inserted
    with get_db(tmp_db) as conn:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count == 0


def test_accepted_order_has_exchange_order_id_in_db(tmp_db_ep: Path) -> None:
    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=_accepted))
    assert result.status == "OPEN"
    assert result.exchange_order_id is not None

    with get_db(tmp_db_ep) as conn:
        row = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?",
            (result.order_id,),
        ).fetchone()
    assert row["status"] == "OPEN"
    assert row["exchange_order_id"] == result.exchange_order_id


def test_empty_string_exchange_order_id_leaves_submitting(tmp_db_ep: Path) -> None:
    """
    coinbase_fn returning "" (empty string) is treated as an ambiguous response —
    we cannot confirm acceptance — so the order stays SUBMITTING.
    The reconciler resolves it by searching Coinbase by client_order_id.
    """
    result = place_order_outbox(
        **_base_kwargs(tmp_db_ep, coinbase_fn=lambda cid: "")
    )
    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None

    with get_db(tmp_db_ep) as conn:
        row = conn.execute("SELECT status FROM orders WHERE id=?", (result.order_id,)).fetchone()
    assert row["status"] == "SUBMITTING"


def test_none_exchange_order_id_leaves_submitting(tmp_db_ep: Path) -> None:
    """coinbase_fn returning None is also treated as ambiguous (falsy)."""
    result = place_order_outbox(
        **_base_kwargs(tmp_db_ep, coinbase_fn=lambda cid: None)
    )
    assert result.status == "SUBMITTING"
    assert result.exchange_order_id is None


def test_rejection_reason_persisted_in_db(tmp_db_ep: Path) -> None:
    """TX-B writes rejection_reason to the orders row when REJECTED."""
    def reject(cid): raise CoinbaseRejected("INSUFFICIENT_FUND: balance is $0.00")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=reject))
    assert result.status == "REJECTED"

    with get_db(tmp_db_ep) as conn:
        row = conn.execute(
            "SELECT rejection_reason FROM orders WHERE id=?", (result.order_id,)
        ).fetchone()
    assert row["rejection_reason"] == "INSUFFICIENT_FUND: balance is $0.00"


def test_coinbase_order_rejected_from_adapter_is_handled(tmp_db_ep: Path) -> None:
    """
    CoinbaseOrderRejected raised by coinbase_client.place_limit_buy() is treated
    identically to the outbox's own CoinbaseRejected — results in REJECTED.
    """
    from exchange.coinbase_client import CoinbaseOrderRejected

    def adapter_reject(cid): raise CoinbaseOrderRejected("INVALID_LIMIT_PRICE_POST_ONLY")

    result = place_order_outbox(**_base_kwargs(tmp_db_ep, coinbase_fn=adapter_reject))
    assert result.status == "REJECTED"
    assert result.rejection_reason == "INVALID_LIMIT_PRICE_POST_ONLY"

    with get_db(tmp_db_ep) as conn:
        row = conn.execute(
            "SELECT status, rejection_reason FROM orders WHERE id=?",
            (result.order_id,),
        ).fetchone()
    assert row["status"] == "REJECTED"
    assert row["rejection_reason"] == "INVALID_LIMIT_PRICE_POST_ONLY"


def test_idempotent_replay_of_rejected_order_returns_rejection_reason(
    tmp_db_ep: Path,
) -> None:
    """
    P2 fix: second call with a REJECTED order_id must return rejection_reason.
    Before fix: existing-order query omitted rejection_reason → returned None.
    """
    from exchange.coinbase_client import CoinbaseOrderRejected

    order_id = str(uuid.uuid4())

    def adapter_reject(cid): raise CoinbaseOrderRejected("INSUFFICIENT_FUND: no funds")

    r1 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=adapter_reject))
    assert r1.status == "REJECTED"
    assert r1.rejection_reason == "INSUFFICIENT_FUND: no funds"

    # Second call — idempotency path
    r2 = place_order_outbox(**_base_kwargs(tmp_db_ep, order_id=order_id, coinbase_fn=adapter_reject))
    assert r2.status == "REJECTED"
    assert r2.rejection_reason == "INSUFFICIENT_FUND: no funds", (
        "Idempotent replay must return rejection_reason from DB, not None"
    )


# ---------------------------------------------------------------------------
# Reconciliation gate embedded in TX-A (TOCTOU fix)
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db_ep_recon(tmp_db_ep: Path) -> Path:
    """DB with epoch + a fresh clean reconciliation run (gate open)."""
    run_startup_reconciliation(
        list_orders_fn=lambda: [],
        cancel_order_fn=lambda eid: True,
        db_path=tmp_db_ep,
    )
    return tmp_db_ep


def test_gate_blocks_placement_when_no_reconciliation_ever(tmp_db_ep: Path) -> None:
    """
    With gate_freshness_minutes set (default), placing an order before any
    reconciliation has ever run must raise PlacementBlocked — not SUBMITTING.
    This closes the TOCTOU: the check and INSERT are now atomic inside TX-A.
    """
    with pytest.raises(PlacementBlocked, match="gate closed"):
        place_order_outbox(**_base_kwargs(
            tmp_db_ep,
            gate_freshness_minutes=60,  # explicit — production default
        ))

    # No order should have been inserted
    with get_db(tmp_db_ep) as conn:
        count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
    assert count == 0, "TX-A must not commit when the gate is closed"


def test_gate_allows_placement_after_clean_reconciliation(tmp_db_ep_recon: Path) -> None:
    """After a clean reconciliation run, placement with gate enabled must succeed."""
    result = place_order_outbox(**_base_kwargs(
        tmp_db_ep_recon,
        gate_freshness_minutes=60,
    ))
    assert result.status == "OPEN"


def test_gate_idempotent_replay_bypasses_gate(tmp_db_ep: Path) -> None:
    """
    Idempotent replay (same order_id that already exists) must succeed even
    when the gate would otherwise block.  The gate is only for new ENTRY
    placements; resolving a SUBMITTING order that survived a crash must not
    be blocked by a stale reconciliation window.
    """
    # Place order with gate disabled
    order_id = str(uuid.uuid4())
    r1 = place_order_outbox(**_base_kwargs(
        tmp_db_ep, order_id=order_id, gate_freshness_minutes=None
    ))
    assert r1.status == "OPEN"

    # Replay with gate enabled — hits idempotency path before gate check
    r2 = place_order_outbox(**_base_kwargs(
        tmp_db_ep, order_id=order_id, gate_freshness_minutes=60
    ))
    assert r2.status == "OPEN"
    assert r2.order_id == order_id


# ---------------------------------------------------------------------------
# Concurrency regression: no connection-level lock errors under contention
# ---------------------------------------------------------------------------

def test_parallel_placements_repeated_no_lock_errors(tmp_path: Path) -> None:
    """
    Regression for the `PRAGMA journal_mode=WAL` connection race.

    get_db() used to set the journal mode on EVERY connection. Changing the
    journal mode needs a lock that busy_timeout does not cover, so a concurrent
    writer could make it raise OperationalError('database is locked')
    IMMEDIATELY rather than queueing — in production that could fail an ENTRY or
    EXIT whenever the scheduler and a manual runner overlapped.

    Repeated because the race is timing-dependent: it surfaced on a Linux CI
    runner while passing on a developer machine. Each round asserts the STRICT
    invariant (exactly one placement, exactly one PlacementBlocked) — the guard
    must never be weakened into "a lock error is acceptable".
    """
    rounds = 20
    for i in range(rounds):
        db = tmp_path / f"ledger_{i}.db"
        run_migrations(db)
        with get_db(db) as conn:
            insert_epoch(_EPOCH_ID, _CAPITAL, "concurrency regression", conn=conn)

        results: list[PlaceResult] = []
        blocked: list[PlacementBlocked] = []
        other_errors: list[BaseException] = []

        def place_once() -> None:
            import time as _t
            import traceback as _tb
            t0 = _t.monotonic()
            try:
                results.append(place_order_outbox(**_base_kwargs(db, coinbase_fn=_accepted)))
            except PlacementBlocked as exc:
                blocked.append(exc)
            except BaseException:      # noqa: BLE001 — the point of the test
                # Record where it was raised and how long it waited: a genuine
                # busy_timeout expiry takes ~10s, an immediate SQLITE_BUSY is ms.
                other_errors.append(
                    f"after {_t.monotonic() - t0:.3f}s\n{_tb.format_exc()}"
                )

        threads = [threading.Thread(target=place_once) for _ in range(2)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert not other_errors, (
            f"round {i}: connection-level failure under contention: {other_errors}"
        )
        assert len(results) == 1, f"round {i}: exactly one placement must succeed"
        assert len(blocked) == 1, f"round {i}: exactly one placement must be blocked"

        with get_db(db) as conn:
            count = conn.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert count == 1, f"round {i}: only one order must be in the DB"


def test_get_db_does_not_set_journal_mode_per_connection() -> None:
    """
    Source-level lock: journal_mode must not be set inside get_db().

    WAL is persistent in the database file and run_migrations() establishes it,
    so re-issuing it per connection buys nothing and reintroduces the race above.
    A behavioural test alone would not catch someone "restoring" the PRAGMA.
    """
    import inspect

    from pipeline.ledger import get_db as _get_db

    src = inspect.getsource(_get_db)
    code = "\n".join(
        ln for ln in src.splitlines() if not ln.lstrip().startswith("#")
    )
    # Strip the docstring so its explanatory prose does not trip the check.
    body = code.split('"""')
    code_only = body[0] + ("".join(body[2:]) if len(body) > 2 else "")
    assert "journal_mode" not in code_only, (
        "get_db() must not set journal_mode — see run_migrations()"
    )


def test_deferred_begin_would_fail_immediately_but_immediate_waits(tmp_path: Path) -> None:
    """
    Pin the SQLite behaviour that caused the live bug.

    A deferred BEGIN takes SHARED on first read; promoting to RESERVED while
    another connection holds RESERVED returns SQLITE_BUSY *immediately* — SQLite
    deliberately skips the busy handler to avoid deadlock, so busy_timeout does
    not apply. BEGIN IMMEDIATE takes RESERVED up-front, so the busy handler runs
    and the writer waits its turn instead of erroring.

    This is why get_db(begin_immediate=True) is the default.
    """
    import sqlite3 as _sq
    import threading as _th
    import time as _t

    db = tmp_path / "lockmode.db"
    run_migrations(db)

    holder_ready = _th.Event()
    release = _th.Event()

    def holder() -> None:
        c = _sq.connect(str(db), isolation_level=None, timeout=5.0)
        c.execute("BEGIN IMMEDIATE")
        c.execute("INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)"
                  " VALUES ('HOLD', 1.0, 'x', '2026-01-01')")
        holder_ready.set()
        release.wait(timeout=5)
        c.execute("COMMIT")
        c.close()

    t = _th.Thread(target=holder)
    t.start()
    holder_ready.wait(timeout=5)

    # Deferred: read first (SHARED), then try to write (promote to RESERVED).
    deferred = _sq.connect(str(db), isolation_level=None, timeout=5.0)
    deferred.execute("BEGIN")
    deferred.execute("SELECT COUNT(*) FROM risk_epochs").fetchone()
    t0 = _t.monotonic()
    deferred_error = None
    try:
        deferred.execute(
            "INSERT INTO risk_epochs(epoch_id, paper_capital, reason, started_at)"
            " VALUES ('DEF', 1.0, 'x', '2026-01-01')"
        )
    except _sq.OperationalError as exc:
        deferred_error = (str(exc), _t.monotonic() - t0)
    finally:
        try:
            deferred.execute("ROLLBACK")
        except _sq.Error:
            pass
        deferred.close()

    release.set()
    t.join()

    assert deferred_error is not None, (
        "expected the deferred upgrade to be refused while RESERVED is held"
    )
    msg, waited = deferred_error
    assert "locked" in msg
    assert waited < 1.0, (
        f"expected an IMMEDIATE refusal (busy handler skipped), waited {waited:.3f}s"
    )

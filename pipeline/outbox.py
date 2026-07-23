"""
Two-transaction outbox for ENTRY order placement.

Guarantees every order is durably recorded in SUBMITTING state before any
network call.  A crash at any point leaves the system in a recoverable state:
startup reconciliation finds SUBMITTING orders, searches Coinbase by
client_order_id, and completes the TX-B that was never committed.

  TX-A  (BEGIN IMMEDIATE):
    verify active epoch
    verify no active ENTRY order for this asset     ← PlacementBlocked if violated
    verify no active position for this asset         ← PlacementBlocked if violated
    INSERT order(status=SUBMITTING, id=<local UUID>)
    INSERT trade_intent(stop, target)
    COMMIT
  ─── no SQLite connection held open during network I/O ───────────────────────

  External:
    coinbase_fn(client_order_id) → exchange_order_id  (non-empty str)
      raise CoinbaseRejected for definitive refusals (400 + known error code)
      raise CoinbaseOrderRejected (from coinbase_client) — treated identically
      raise anything else for ambiguous outcomes (timeout, 5xx, dropped conn)
      return falsy value → treated as ambiguous (leave SUBMITTING)

  TX-B  (BEGIN):
    accepted (truthy exchange_order_id) → OPEN  +  exchange_order_id set
    CoinbaseRejected / CoinbaseOrderRejected  →  REJECTED  + rejection_reason
    ambiguous / falsy id  →  order stays SUBMITTING (TX-B skipped)

On timeout / ambiguous error, do NOT retry with a new UUID.  The startup
reconciler searches Coinbase by client_order_id to resolve SUBMITTING orders.

REJECTED vs CANCELLED:
  REJECTED  — Coinbase never accepted the order (e.g. INSUFFICIENT_FUND)
  CANCELLED — order was accepted, then cancelled by us or Coinbase
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Callable, Optional

from pipeline.ledger import (
    get_active_epoch,
    get_db,
    insert_order,
    insert_trade_intent,
    transition_order,
)

_ORDER_TTL_HOURS = 24


class PlacementBlocked(Exception):
    """
    Raised when TX-A gate checks prevent placing a new ENTRY order.
    Signals to the scheduler that this asset already has an active order or
    position — do not place a second one.
    """


class CoinbaseRejected(Exception):
    """
    Raise from coinbase_fn to signal a definitive Coinbase refusal.
    Examples: INSUFFICIENT_FUND, INVALID_LIMIT_PRICE_POST_ONLY.
    Network timeouts and 5xx must NOT raise this — they are ambiguous.
    """


@dataclass
class PlaceResult:
    """Return value of place_order_outbox()."""
    status: str                          # "OPEN" | "REJECTED" | "SUBMITTING"
    order_id: str                        # local UUID == client_order_id on Coinbase
    exchange_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None


@dataclass
class ExitPlaceResult:
    """Return value of place_exit_outbox()."""
    status: str                          # "OPEN" | "REJECTED" | "SUBMITTING"
    order_id: str                        # local UUID == client_order_id on Coinbase
    position_id: str
    exchange_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None


def place_order_outbox(
    *,
    asset: str,
    limit_price: float,
    qty_usd: float,
    stop_price: float,
    target_price: float,
    reasoning: str = "",
    ttl_hours: int = _ORDER_TTL_HOURS,
    order_id: Optional[str] = None,
    coinbase_fn: Callable[[str], str],
    db_path: Optional[Path] = None,
    gate_freshness_minutes: Optional[int] = 60,
) -> PlaceResult:
    """
    Place an ENTRY limit BUY order via the two-transaction outbox pattern.

    This function is intentionally restricted to ENTRY/BUY/LIMIT orders.
    EXIT orders require qty_base_requested (not qty_usd) and a different
    capital allocation path — implement them in a separate function.

    Args:
        asset:        e.g. "ZEC-USD"
        limit_price:  limit price in USD
        qty_usd:      USD notional from active epoch capital (NOT PAPER_BALANCE)
        stop_price:   stop-loss written to trade_intent in TX-A (durable before crash)
        target_price: take-profit written to trade_intent in TX-A
        order_id:     supply to replay an existing SUBMITTING order (idempotent);
                      if None, a fresh UUID is generated
        coinbase_fn:  callable(client_order_id: str) -> exchange_order_id: str
                      raise CoinbaseRejected for definitive refusals;
                      raise CoinbaseOrderRejected (from coinbase_client) for the same;
                      any other exception = ambiguous → leaves order SUBMITTING;
                      returning a falsy value = also treated as ambiguous
        db_path:      override DB path (tests only)
        gate_freshness_minutes:
                      check reconciliation gate INSIDE the BEGIN IMMEDIATE
                      transaction so the check is atomic with the INSERT.
                      None = skip (tests that are not testing the gate).
                      Default 60 — production callers must have reconciled
                      within the last hour; set lower for tighter enforcement.

    Returns PlaceResult with:
        status "OPEN"        — accepted by Coinbase, TX-B committed
        status "REJECTED"    — definitively refused, TX-B committed, reason in ledger
        status "SUBMITTING"  — timeout/ambiguous, only TX-A committed;
                               do NOT retry with a new UUID —
                               startup reconciler resolves via client_order_id

    Raises:
        PlacementBlocked — active ENTRY order or OPEN/CLOSING position already
                           exists for this asset (checked inside BEGIN IMMEDIATE)
        RuntimeError     — no active epoch
    """
    # Import here to avoid circular import (coinbase_client has no pipeline deps).
    from exchange.coinbase_client import CoinbaseOrderRejected

    if order_id is None:
        order_id = str(uuid.uuid4())

    now = datetime.now(timezone.utc)
    placed_at = now.isoformat()
    expires_at = (now + timedelta(hours=ttl_hours)).isoformat()

    # ── TX-A ─────────────────────────────────────────────────────────────────
    # BEGIN IMMEDIATE acquires the write lock up-front, making the gate checks
    # and the INSERT atomic.  No concurrent writer can slip an order in between.
    with get_db(db_path, begin_immediate=True) as conn:
        # Idempotency: if this order_id already exists return its current state
        # without any Coinbase call or new writes.
        existing = conn.execute(
            "SELECT status, exchange_order_id, rejection_reason"
            " FROM orders WHERE id=?",
            (order_id,),
        ).fetchone()
        if existing is not None:
            return PlaceResult(
                status=existing["status"],
                order_id=order_id,
                exchange_order_id=existing["exchange_order_id"],
                rejection_reason=existing["rejection_reason"],
            )

        # Reconciliation gate — checked inside BEGIN IMMEDIATE so the read
        # and the INSERT below are atomic.  No reconciliation run can complete
        # (write to reconciliation_runs) between this check and our COMMIT
        # because we hold the write lock.  Skipped only when explicitly
        # disabled (tests that exercise other outbox mechanics).
        if gate_freshness_minutes is not None:
            from pipeline.reconciler import _gate_check_on_conn
            allowed, reason = _gate_check_on_conn(conn, gate_freshness_minutes)
            if not allowed:
                raise PlacementBlocked(
                    f"Reconciliation gate closed for {asset}: {reason}. "
                    "Run reconciliation first, then retry."
                )

        epoch = get_active_epoch(conn)
        if epoch is None:
            raise RuntimeError(
                f"Cannot place order for {asset}: no active risk epoch. "
                "Call start_epoch() before placing orders."
            )

        # Gate: no active ENTRY order for this asset.
        active_entry = conn.execute(
            "SELECT COUNT(*) FROM orders"
            " WHERE asset=? AND purpose='ENTRY'"
            " AND status IN ('SUBMITTING','OPEN','PARTIAL')",
            (asset,),
        ).fetchone()[0]
        if active_entry:
            raise PlacementBlocked(
                f"Cannot place ENTRY for {asset}: "
                f"{active_entry} active ENTRY order(s) already exist. "
                "Wait for them to fill, expire, or be cancelled."
            )

        # Gate: no open or dust position for this asset.
        # DUST positions are real open exposure — the user still owns the coins;
        # they just cannot be sold below base_min_size.  Block ENTRY until dust
        # is manually written off or the position is closed by other means.
        active_pos = conn.execute(
            "SELECT COUNT(*) FROM positions"
            " WHERE asset=? AND status IN ('OPEN','CLOSING','DUST')",
            (asset,),
        ).fetchone()[0]
        if active_pos:
            raise PlacementBlocked(
                f"Cannot place ENTRY for {asset}: "
                f"{active_pos} active/dust position(s) already exist. "
                "Close existing positions before entering again."
            )

        insert_order(
            order_id=order_id,
            epoch_id=epoch["epoch_id"],
            asset=asset,
            side="BUY",
            order_type="LIMIT",
            purpose="ENTRY",
            placed_at=placed_at,
            qty_usd_requested=qty_usd,
            limit_price=limit_price,
            expires_at=expires_at,
            reasoning=reasoning,
            conn=conn,
        )
        insert_trade_intent(
            order_id, stop_price=stop_price, target_price=target_price, conn=conn
        )
    # TX-A committed.
    # ── No SQLite connection held open during the network call ─────────────────

    exchange_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None
    final_status = "SUBMITTING"

    try:
        result = coinbase_fn(order_id)
        if result:
            exchange_order_id = result
            final_status = "OPEN"
        # If result is falsy (None, ""), leave as SUBMITTING — ambiguous response.
    except (CoinbaseRejected, CoinbaseOrderRejected) as exc:
        rejection_reason = str(exc)
        final_status = "REJECTED"
    except Exception:
        # Timeout, network error, 5xx, dropped connection — ambiguous.
        # Leave the order as SUBMITTING.  Never retry with a new UUID.
        # The startup reconciler searches Coinbase by client_order_id.
        pass

    # ── TX-B ─────────────────────────────────────────────────────────────────
    # Skip entirely if ambiguous — order stays SUBMITTING, reconciler handles it.
    if final_status != "SUBMITTING":
        with get_db(db_path) as conn:
            transition_order(
                order_id,
                final_status,
                exchange_order_id=exchange_order_id,
                conn=conn,
            )
            if rejection_reason:
                conn.execute(
                    "UPDATE orders SET rejection_reason=? WHERE id=?",
                    (rejection_reason, order_id),
                )

    return PlaceResult(
        status=final_status,
        order_id=order_id,
        exchange_order_id=exchange_order_id,
        rejection_reason=rejection_reason,
    )


def place_exit_outbox(
    *,
    position_id: str,
    exit_reason: str,
    coinbase_sell_fn: Callable[[str, str, float], str],
    order_id: Optional[str] = None,
    db_path: Optional[Path] = None,
    base_increment: Optional[str] = None,
    base_min_size: Optional[str] = None,
) -> ExitPlaceResult:
    """
    Place a SELL order to exit an open ledger position via the two-transaction outbox.

    coinbase_sell_fn(order_id, asset, qty_base) → exchange_order_id
      Raise CoinbaseRejected / CoinbaseOrderRejected for definitive refusals.
      Any other exception = ambiguous → leave order SUBMITTING.
      Returning a falsy value = also treated as ambiguous.

    base_increment: exchange base-qty step as a string (e.g. "0.00000001").
      When provided, qty_base_remaining is rounded DOWN to this increment inside
      TX-A and the rounded value is stored as qty_base_requested and passed to
      coinbase_sell_fn.  Also stored in orders.base_increment_applied for audit.
    base_min_size: exchange minimum order size as a string (e.g. "0.001").
      When both base_increment and base_min_size are provided, the rounded qty
      is checked; if rounded_qty < base_min_size the position is transitioned to
      DUST status inside TX-A (committed), then PlacementBlocked is raised.

    TX-A (BEGIN IMMEDIATE):
      Reads qty_base_remaining + asset from the position (authoritative values).
      Verifies position is OPEN or CLOSING (not yet CLOSED).
      Verifies no active EXIT order exists for this position.
      Applies base_increment rounding (ROUND_DOWN) if provided.
      If rounded qty is DUST: transitions position to DUST, commits, raises PlacementBlocked.
      Otherwise: INSERTs SUBMITTING EXIT order with rounded qty_base_remaining.
      COMMIT.
    ─── no SQLite connection held open during the network call ───────────────────
    TX-B:
      accepted (truthy exchange_order_id) → OPEN  + exchange_order_id set
      CoinbaseRejected / CoinbaseOrderRejected    → REJECTED + rejection_reason
      ambiguous / falsy id                        → stays SUBMITTING (TX-B skipped)

    Raises PlacementBlocked if:
      - position not found
      - position is already CLOSED or DUST
      - an active EXIT order (SUBMITTING/OPEN/PARTIAL) already exists for this position
      - rounded qty is below base_min_size (DUST transition committed first)
    """
    from exchange.coinbase_client import CoinbaseOrderRejected

    if order_id is None:
        order_id = str(uuid.uuid4())

    # ── TX-A ─────────────────────────────────────────────────────────────────
    with get_db(db_path, begin_immediate=True) as conn:
        # Idempotency: if this order_id already exists, return current state.
        existing = conn.execute(
            "SELECT status, exchange_order_id, rejection_reason"
            " FROM orders WHERE id=?",
            (order_id,),
        ).fetchone()
        if existing is not None:
            return ExitPlaceResult(
                status=existing["status"],
                order_id=order_id,
                position_id=position_id,
                exchange_order_id=existing["exchange_order_id"],
                rejection_reason=existing["rejection_reason"],
            )

        # Read authoritative position data inside the write lock so qty_base and
        # asset cannot change between the read and the INSERT.
        pos = conn.execute(
            "SELECT asset, qty_base_remaining, status, epoch_id FROM positions WHERE id=?",
            (position_id,),
        ).fetchone()
        if pos is None:
            raise PlacementBlocked(
                f"Cannot exit position '{position_id}': not found in ledger."
            )
        if pos["status"] not in ("OPEN", "CLOSING"):
            raise PlacementBlocked(
                f"Cannot exit position '{position_id}': "
                f"status is {pos['status']!r}, expected OPEN or CLOSING."
            )
        qty_base_raw = pos["qty_base_remaining"] or 0.0
        if qty_base_raw <= 0:
            raise PlacementBlocked(
                f"Cannot exit position '{position_id}': "
                f"qty_base_remaining={qty_base_raw} <= 0 — nothing to sell."
            )

        # Gate: enforced by idx_one_active_exit_per_position, but explicit check
        # gives a clear error message before the UNIQUE constraint fires.
        active_exit = conn.execute(
            "SELECT id FROM orders"
            " WHERE position_id=? AND purpose='EXIT'"
            "   AND status IN ('SUBMITTING','OPEN','PARTIAL')",
            (position_id,),
        ).fetchone()
        if active_exit:
            raise PlacementBlocked(
                f"Cannot exit position '{position_id}': "
                f"active EXIT order '{active_exit['id']}' already exists. "
                "Wait for it to resolve before placing another."
            )

        asset = pos["asset"]
        epoch_id = pos["epoch_id"]
        placed_at = datetime.now(timezone.utc).isoformat()

        # Apply product rules: ROUND_DOWN to base_increment, then DUST check.
        # This runs inside the write lock so the rounded qty is always consistent
        # with the authoritative qty_base_remaining read above.
        if base_increment is not None:
            from pipeline.product_rules import round_base_qty, is_dust as _is_dust
            rounded = round_base_qty(qty_base_raw, base_increment)
            if base_min_size is not None and _is_dust(rounded, base_min_size):
                # Remaining qty is below exchange minimum — cannot be sold.
                # Transition to DUST (commits with this TX-A) so the exit executor
                # won't attempt another SELL on the next tick.
                from pipeline.ledger import transition_position_to_dust
                transition_position_to_dust(position_id, conn)
                dust_qty = float(rounded)
            else:
                dust_qty = None
            qty_base = float(rounded)
        else:
            qty_base = qty_base_raw
            dust_qty = None

        if dust_qty is None:
            insert_order(
                order_id=order_id,
                epoch_id=epoch_id,
                asset=asset,
                side="SELL",
                order_type="MARKET",
                purpose="EXIT",
                position_id=position_id,
                placed_at=placed_at,
                qty_base_requested=qty_base,
                reasoning=exit_reason,
                base_increment_applied=base_increment,
                conn=conn,
            )
    # TX-A committed. qty_base and asset are captured from the authoritative locked read.
    # Dust transition (if any) is also committed here, before the check below.
    # ── No SQLite connection held open during the network call ─────────────────

    if dust_qty is not None:
        raise PlacementBlocked(
            f"DUST: position '{position_id}' qty_base_remaining={qty_base_raw} "
            f"rounds to {dust_qty} under base_increment={base_increment!r}, "
            f"below base_min_size={base_min_size!r}. "
            "Position transitioned to DUST — no SELL placed."
        )

    exchange_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None
    final_status = "SUBMITTING"

    try:
        result = coinbase_sell_fn(order_id, asset, qty_base)
        if result:
            exchange_order_id = result
            final_status = "OPEN"
    except (CoinbaseRejected, CoinbaseOrderRejected) as exc:
        rejection_reason = str(exc)
        final_status = "REJECTED"
    except Exception:
        # Timeout, 5xx, dropped connection — ambiguous.
        # Leave the order SUBMITTING; startup reconciler resolves via client_order_id.
        pass

    # ── TX-B ─────────────────────────────────────────────────────────────────
    if final_status != "SUBMITTING":
        with get_db(db_path) as conn:
            transition_order(
                order_id,
                final_status,
                exchange_order_id=exchange_order_id,
                conn=conn,
            )
            if rejection_reason:
                conn.execute(
                    "UPDATE orders SET rejection_reason=? WHERE id=?",
                    (rejection_reason, order_id),
                )

    return ExitPlaceResult(
        status=final_status,
        order_id=order_id,
        position_id=position_id,
        exchange_order_id=exchange_order_id,
        rejection_reason=rejection_reason,
    )

"""
Two-transaction outbox for order placement.

Guarantees that every order is durably recorded in SUBMITTING state before
any network call.  A crash at any point leaves the system in a recoverable
state: startup reconciliation finds SUBMITTING orders, searches Coinbase by
client_order_id, and completes the TX-B that was never committed.

  TX-A  (BEGIN IMMEDIATE — no concurrent writer can slip between check and insert):
    verify active epoch
    INSERT order(status=SUBMITTING, id=<local UUID> = client_order_id)
    INSERT trade_intent(stop, target)
    COMMIT
  ─── no SQLite connection held open beyond this point ───────────────────────

  External:
    coinbase_fn(client_order_id) → exchange_order_id
      raise CoinbaseRejected for definitive refusals (400 + known error code)
      raise anything else for ambiguous outcomes (timeout, 5xx, dropped conn)

  TX-B  (BEGIN):
    accepted      → transition_order(OPEN,  exchange_order_id=...)  COMMIT
    CoinbaseRejected → transition_order(REJECTED)                   COMMIT
    any other exc → order stays SUBMITTING (TX-B skipped)

On timeout / ambiguous error, do NOT retry with a new UUID.  The startup
reconciler searches Coinbase by client_order_id to resolve SUBMITTING orders.

REJECTED vs CANCELLED:
  REJECTED  — Coinbase never accepted the order (insufficient funds, crossing)
  CANCELLED — order was accepted, then cancelled by us or Coinbase

State transitions added in this module:
  SUBMITTING → OPEN      (accepted)
  SUBMITTING → REJECTED  (definitive refusal)
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


def place_order_outbox(
    *,
    asset: str,
    limit_price: float,
    qty_usd: float,
    stop_price: float,
    target_price: float,
    reasoning: str = "",
    ttl_hours: int = _ORDER_TTL_HOURS,
    purpose: str = "ENTRY",
    side: str = "BUY",
    order_type: str = "LIMIT",
    position_id: Optional[str] = None,
    order_id: Optional[str] = None,
    coinbase_fn: Callable[[str], str],
    db_path: Optional[Path] = None,
) -> PlaceResult:
    """
    Place a limit order via the two-transaction outbox pattern.

    Args:
        asset:        e.g. "ZEC-USD"
        limit_price:  limit price in USD
        qty_usd:      USD notional from active epoch capital (not PAPER_BALANCE)
        stop_price:   stop-loss price — written to trade_intent before Coinbase call
        target_price: take-profit price — written to trade_intent before Coinbase call
        order_id:     supply to replay an existing SUBMITTING order (idempotent);
                      if None, a fresh UUID is generated
        coinbase_fn:  callable(client_order_id: str) -> exchange_order_id: str
                      raise CoinbaseRejected for definitive failures;
                      any other exception = ambiguous → leaves order SUBMITTING
        db_path:      override DB path (used in tests)

    Returns:
        PlaceResult.status:
          "OPEN"        — accepted by Coinbase, TX-B committed
          "REJECTED"    — definitively refused, TX-B committed
          "SUBMITTING"  — timeout or ambiguous error, TX-A committed only;
                          startup reconciler will resolve via client_order_id
    """
    if order_id is None:
        order_id = str(uuid.uuid4())

    now = datetime.now(timezone.utc)
    placed_at = now.isoformat()
    expires_at = (now + timedelta(hours=ttl_hours)).isoformat()

    # ── TX-A ─────────────────────────────────────────────────────────────────
    # BEGIN IMMEDIATE prevents a second writer from passing the epoch gate
    # check and inserting a conflicting order between our check and our insert.
    with get_db(db_path, begin_immediate=True) as conn:
        # Idempotency: if this order_id is already in the ledger, return its
        # current state without touching Coinbase or writing anything.
        existing = conn.execute(
            "SELECT status, exchange_order_id FROM orders WHERE id=?", (order_id,)
        ).fetchone()
        if existing is not None:
            return PlaceResult(
                status=existing["status"],
                order_id=order_id,
                exchange_order_id=existing["exchange_order_id"],
            )

        epoch = get_active_epoch(conn)
        if epoch is None:
            raise RuntimeError(
                f"Cannot place order for {asset}: no active risk epoch. "
                "Call start_epoch() before placing orders."
            )

        insert_order(
            order_id=order_id,
            epoch_id=epoch["epoch_id"],
            asset=asset,
            side=side,
            order_type=order_type,
            purpose=purpose,
            placed_at=placed_at,
            qty_usd_requested=qty_usd,
            limit_price=limit_price,
            expires_at=expires_at,
            position_id=position_id,
            reasoning=reasoning,
            conn=conn,
        )
        insert_trade_intent(
            order_id, stop_price=stop_price, target_price=target_price, conn=conn
        )
    # TX-A committed.
    # ── No SQLite connection is held open during the network call ─────────────

    exchange_order_id: Optional[str] = None
    rejection_reason: Optional[str] = None
    final_status = "SUBMITTING"

    try:
        exchange_order_id = coinbase_fn(order_id)
        final_status = "OPEN"
    except CoinbaseRejected as exc:
        rejection_reason = str(exc)
        final_status = "REJECTED"
    except Exception:
        # Timeout, network error, 5xx, dropped connection — ambiguous.
        # Leave the order as SUBMITTING.  Never retry with a new UUID.
        # The startup reconciler searches Coinbase by client_order_id.
        pass

    # ── TX-B ─────────────────────────────────────────────────────────────────
    # Skip if ambiguous (order stays SUBMITTING — reconciler handles it).
    if final_status != "SUBMITTING":
        with get_db(db_path) as conn:
            transition_order(
                order_id,
                final_status,
                exchange_order_id=exchange_order_id,
                conn=conn,
            )

    return PlaceResult(
        status=final_status,
        order_id=order_id,
        exchange_order_id=exchange_order_id,
        rejection_reason=rejection_reason,
    )

"""
Production factory for get_order_fn used by run_startup_reconciliation().

Wires Coinbase Get Order (status + aggregates) and List Fills (individual fills)
into the CoinbaseOrder DTO the reconciler expects, with cross-checks to guard
against pagination gaps or aggregate inconsistencies.

Usage in runner.py:
    from exchange.adapter import make_get_order_fn
    report = run_startup_reconciliation(
        list_orders_fn=...,
        cancel_order_fn=...,
        get_order_fn=make_get_order_fn(),
    )
"""
from __future__ import annotations

from typing import Callable, Optional

import exchange.coinbase_client as _cb
from pipeline.reconciler import CoinbaseFill, CoinbaseOrder


def make_get_order_fn() -> Callable[[str], Optional[CoinbaseOrder]]:
    """
    Return a get_order_fn: Callable[[str], Optional[CoinbaseOrder]].

    For each exchange_order_id:
      1. Get Order → status, client_order_id, number_of_fills, filled_size.
      2. List Fills (paginated via fetch_fills_for_order) → all individual fills.
      3. Cross-check fill count and filled_size against aggregates.
         Any mismatch → return None so the reconciler leaves the order UNRESOLVED.
      4. Normalize raw fill dicts → CoinbaseFill dataclasses.
      5. Return CoinbaseOrder with all fills attached.

    Returns None on any error or aggregate inconsistency.
    DRY_RUN / DRY- IDs return None immediately (no real orders to query).
    """
    def _fn(exchange_order_id: str) -> Optional[CoinbaseOrder]:
        if _cb._DRY_RUN or exchange_order_id.startswith("DRY-"):
            return None

        try:
            client = _cb._get_client()

            # Phase 1: Get Order — status and aggregate metrics.
            raw_order = _cb._resp_to_dict(client.get_order(order_id=exchange_order_id))
            order = raw_order.get("order", raw_order)
            status = order.get("status", "")
            client_order_id = order.get("client_order_id", "")
            expected_fill_count = int(order.get("number_of_fills") or 0)
            expected_filled_size = float(order.get("filled_size") or 0)

            # Phase 2: List Fills — all pages with dedup/order-id guard.
            raw_fills = _cb.fetch_fills_for_order(exchange_order_id)

            # Cross-check 1: fill count must agree with Get Order aggregate.
            if expected_fill_count > 0 and len(raw_fills) != expected_fill_count:
                print(
                    f"[Adapter] {exchange_order_id}: fill count mismatch "
                    f"(Get Order says {expected_fill_count}, List Fills returned {len(raw_fills)})"
                )
                return None

            # Cross-check 2: filled_size must agree within 0.1% tolerance.
            if raw_fills and expected_filled_size > 0:
                local_filled = sum(float(f.get("size", 0)) for f in raw_fills)
                rel_err = abs(local_filled - expected_filled_size) / expected_filled_size
                if rel_err > 0.001:
                    print(
                        f"[Adapter] {exchange_order_id}: filled_size mismatch "
                        f"(Get Order={expected_filled_size:.8f}, "
                        f"List Fills sum={local_filled:.8f}, rel_err={rel_err:.4%})"
                    )
                    return None

            fills = [
                CoinbaseFill(
                    exchange_fill_id=f.get("entry_id", ""),
                    fill_price=float(f.get("price", 0)),
                    fill_qty_base=float(f.get("size", 0)),
                    fee_usd=float(f.get("commission", 0)),
                    filled_at=f.get("trade_time", ""),
                )
                for f in raw_fills
            ]

            return CoinbaseOrder(
                client_order_id=client_order_id,
                exchange_order_id=exchange_order_id,
                status=status,
                fills=fills,
            )

        except Exception as exc:
            print(f"[Adapter] get_order_with_fills error {exchange_order_id}: {exc}")
            return None

    return _fn

"""
Production factory for get_order_fn used by run_startup_reconciliation().

Wires Coinbase Get Order (status + aggregates) and List Fills (individual fills)
into the CoinbaseOrder DTO the reconciler expects, with fail-closed cross-checks.

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


def _normalize_fill(f: dict, exchange_order_id: str) -> Optional[CoinbaseFill]:
    """
    Normalize a raw fill dict to CoinbaseFill, or return None if the fill
    data is invalid.  Callers treat None as UNRESOLVED — do not silently skip.

    Rejects:
      - Missing or empty entry_id (can't dedup without a stable identifier).
      - price or size ≤ 0 (ledger would silently compute wrong position size).
      - size_in_quote=true (size is in USD, not base currency; position sizing
        would be wrong by the fill price factor).
    """
    entry_id = f.get("entry_id", "")
    if not entry_id:
        return None

    try:
        price = float(f.get("price") or 0)
        size = float(f.get("size") or 0)
    except (ValueError, TypeError):
        return None

    if price <= 0 or size <= 0:
        return None

    size_in_quote = f.get("size_in_quote")
    if size_in_quote in (True, "true", "True", "TRUE", "1", 1):
        return None

    try:
        fee = float(f.get("commission") or 0)
    except (ValueError, TypeError):
        fee = 0.0

    return CoinbaseFill(
        exchange_fill_id=entry_id,
        fill_price=price,
        fill_qty_base=size,
        fee_usd=fee,
        filled_at=f.get("trade_time", ""),
    )


def make_list_orders_fn() -> Callable[[], list[CoinbaseOrder]]:
    """
    Return a list_orders_fn: Callable[[], list[CoinbaseOrder]].

    Fetches all non-terminal orders from Coinbase via list_open_orders() and
    normalises each to a CoinbaseOrder with no fills (fills are fetched per-order
    by make_get_order_fn() only when needed for stacking resolution).

    Returns [] in DRY_RUN mode.  Silently drops orders with empty client_order_id
    or exchange_order_id — they cannot be matched against local orders.
    """
    def _fn() -> list[CoinbaseOrder]:
        if _cb._DRY_RUN:
            return []
        try:
            raw_orders = _cb.list_open_orders()
        except Exception as exc:
            print(f"[Adapter] list_open_orders error: {exc}")
            return []

        result: list[CoinbaseOrder] = []
        for o in raw_orders:
            client_id = o.get("client_order_id", "")
            exchange_id = o.get("order_id", "")
            status = o.get("status", "")
            if not client_id or not exchange_id:
                continue
            result.append(CoinbaseOrder(
                client_order_id=client_id,
                exchange_order_id=exchange_id,
                status=status,
                fills=[],  # fills fetched per-order only when needed
            ))
        return result

    return _fn


def make_get_order_fn() -> Callable[[str], Optional[CoinbaseOrder]]:
    """
    Return a get_order_fn: Callable[[str], Optional[CoinbaseOrder]].

    For each exchange_order_id:
      1. Get Order → actual order_id (from API), status, client_order_id,
         number_of_fills (Optional), filled_size (Optional).
      2. List Fills via fetch_fills_for_order() → all pages with dedup/filter.
         IncompleteFillHistory → return None immediately (pagination broken).
      3. Fail-closed aggregate cross-checks:
           - filled_size > 0 but no fills → None (dangerous gap).
           - number_of_fills > 0 but no fills → None.
           - fills present but both aggregates say zero → None (inconsistency).
           - fill count != number_of_fills → None (if count available).
           - filled_size sum off by > 0.1% → None (if size available > 0).
      4. Strict fill normalization via _normalize_fill():
           empty entry_id, price/size ≤ 0, size_in_quote → None per fill → None overall.
      5. exchange_order_id in the returned DTO is from the API response, not the
         input parameter — makes the reconciler's ID mismatch check meaningful.

    Returns None on any error or inconsistency so the reconciler leaves UNRESOLVED.
    DRY_RUN / DRY- IDs return None immediately (no real orders to query).
    """
    def _fn(exchange_order_id: str) -> Optional[CoinbaseOrder]:
        if _cb._DRY_RUN or exchange_order_id.startswith("DRY-"):
            return None

        try:
            client = _cb._get_client()

            # Phase 1: Get Order — actual ID, status, and aggregate metrics.
            raw_order = _cb._resp_to_dict(client.get_order(order_id=exchange_order_id))
            order = raw_order.get("order", raw_order)

            # Use the actual order_id from the API response, not our input.
            # This is what makes the reconciler's exchange_order_id check real.
            actual_exchange_id = order.get("order_id", "")
            if not actual_exchange_id:
                print(
                    f"[Adapter] {exchange_order_id}: Get Order response missing "
                    f"order_id field — cannot verify ID match"
                )
                return None

            status = order.get("status", "")
            client_order_id = order.get("client_order_id", "")

            # Parse aggregates as Optional — absent/blank is unknown, not zero.
            _nof = order.get("number_of_fills")
            _fs  = order.get("filled_size")
            try:
                expected_fill_count: Optional[int] = (
                    int(_nof) if _nof not in (None, "") else None
                )
            except (ValueError, TypeError):
                expected_fill_count = None
            try:
                expected_filled_size: Optional[float] = (
                    float(_fs) if _fs not in (None, "") else None
                )
            except (ValueError, TypeError):
                expected_filled_size = None

            # Phase 2: List Fills — paginated, raises IncompleteFillHistory on
            # cursor cycle, empty-page-with-cursor, or _MAX_FILL_PAGES exhausted.
            try:
                raw_fills = _cb.fetch_fills_for_order(exchange_order_id)
            except _cb.IncompleteFillHistory as exc:
                print(f"[Adapter] {exchange_order_id}: incomplete fill history — {exc}")
                return None

            # Phase 3: Fail-closed aggregate cross-checks.
            # Any of these scenarios means the adapter cannot safely compute NAV.
            if not raw_fills:
                # Empty fills: check that aggregates don't say fills exist.
                if expected_fill_count is not None and expected_fill_count > 0:
                    print(
                        f"[Adapter] {exchange_order_id}: number_of_fills="
                        f"{expected_fill_count} but List Fills returned empty"
                    )
                    return None
                if expected_filled_size is not None and expected_filled_size > 0:
                    print(
                        f"[Adapter] {exchange_order_id}: filled_size="
                        f"{expected_filled_size:.8f} but List Fills returned empty"
                    )
                    return None
            else:
                # Fills present: both aggregates must not be zero.
                if (expected_fill_count is not None and expected_fill_count == 0 and
                        expected_filled_size is not None and expected_filled_size == 0.0):
                    print(
                        f"[Adapter] {exchange_order_id}: aggregates say 0 fills/size "
                        f"but List Fills returned {len(raw_fills)} fills — inconsistency"
                    )
                    return None
                # Fill count must match if the aggregate is available.
                if (expected_fill_count is not None and
                        len(raw_fills) != expected_fill_count):
                    print(
                        f"[Adapter] {exchange_order_id}: fill count mismatch "
                        f"(Get Order={expected_fill_count}, "
                        f"List Fills={len(raw_fills)})"
                    )
                    return None
                # Filled size must agree within 0.1% tolerance if available.
                if expected_filled_size is not None and expected_filled_size > 0:
                    local_filled = sum(float(f.get("size", 0)) for f in raw_fills)
                    rel_err = abs(local_filled - expected_filled_size) / expected_filled_size
                    if rel_err > 0.001:
                        print(
                            f"[Adapter] {exchange_order_id}: filled_size mismatch "
                            f"(Get Order={expected_filled_size:.8f}, "
                            f"List Fills sum={local_filled:.8f}, rel_err={rel_err:.4%})"
                        )
                        return None

            # Phase 4: Strict fill normalization.
            fills: list[CoinbaseFill] = []
            for f in raw_fills:
                normalized = _normalize_fill(f, exchange_order_id)
                if normalized is None:
                    print(
                        f"[Adapter] {exchange_order_id}: invalid fill data "
                        f"(entry_id={f.get('entry_id')!r}, "
                        f"price={f.get('price')!r}, size={f.get('size')!r}, "
                        f"size_in_quote={f.get('size_in_quote')!r})"
                    )
                    return None
                fills.append(normalized)

            return CoinbaseOrder(
                client_order_id=client_order_id,
                exchange_order_id=actual_exchange_id,  # from API, not input param
                status=status,
                fills=fills,
            )

        except Exception as exc:
            print(f"[Adapter] get_order_with_fills error {exchange_order_id}: {exc}")
            return None

    return _fn

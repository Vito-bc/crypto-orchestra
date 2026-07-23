"""
Product rules: Decimal rounding and dust detection for Coinbase exchange limits.

Two rules enforced before any base qty reaches the wire:
  ROUND_DOWN — always truncate to base_increment, never round up when selling.
  DUST guard — if rounded qty < base_min_size, the SELL would be rejected by
               the exchange; transition the position to DUST status instead.

Both functions accept string representations of the exchange parameters to
match the type returned by Coinbase's Get Best Bid/Ask and Get Product APIs
(always strings, never floats, to preserve decimal precision).
"""

from __future__ import annotations

from decimal import Decimal, ROUND_DOWN


def round_base_qty(qty: float, base_increment: str) -> Decimal:
    """
    Round qty DOWN to the nearest multiple of base_increment.

    Uses Decimal arithmetic to avoid IEEE-754 rounding surprises.
    Always rounds toward zero (ROUND_DOWN), never increases the qty.

    Args:
        qty: quantity in base currency (e.g. 0.999999 ZEC)
        base_increment: exchange minimum step as a string (e.g. "0.00000001")

    Returns:
        Rounded Decimal, e.g. Decimal("0.99999900")
    """
    # Division-then-floor ensures correct multiples for non-power-of-10 increments.
    # quantize() only sets the decimal scale, not the multiple: 1.24 quantized to
    # scale 0.05 stays 1.24, not 1.20.  The division formula always gives exact multiples.
    inc = Decimal(base_increment)
    return (Decimal(str(qty)) / inc).to_integral_value(ROUND_DOWN) * inc


def is_dust(rounded_qty: Decimal, base_min_size: str) -> bool:
    """
    Return True if rounded_qty is below the exchange minimum order size.

    A dust position cannot be sold — the exchange will reject the order with
    INVALID_QUANTITY.  The caller must transition the position to DUST status
    rather than attempting to place the order.

    Args:
        rounded_qty: already-rounded base qty (output of round_base_qty)
        base_min_size: exchange minimum as a string (e.g. "0.001")
    """
    return rounded_qty < Decimal(base_min_size)

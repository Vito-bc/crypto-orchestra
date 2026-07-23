"""
Unit tests for pipeline/product_rules.py.

Covers: ROUND_DOWN semantics, exact-multiple passthrough, zero boundary,
dust detection at/above/below base_min_size, and interaction between the two
functions that mirrors the place_exit_outbox usage pattern.
"""

from __future__ import annotations

from decimal import Decimal

import pytest

from pipeline.product_rules import is_dust, round_base_qty


# ---------------------------------------------------------------------------
# round_base_qty
# ---------------------------------------------------------------------------

class TestRoundBaseQty:

    def test_exact_multiple_unchanged(self) -> None:
        assert round_base_qty(0.001, "0.001") == Decimal("0.001")

    def test_rounds_down_not_half_up(self) -> None:
        # 0.00150 with increment 0.001 → 0.001 (truncated, not rounded to 0.002)
        assert round_base_qty(0.0015, "0.001") == Decimal("0.001")

    def test_rounds_down_at_8dp(self) -> None:
        # 1.999999999 (9 dp) with 8-dp increment → 1.99999999
        assert round_base_qty(1.999999999, "0.00000001") == Decimal("1.99999999")

    def test_whole_number_with_fine_increment(self) -> None:
        assert round_base_qty(1.0, "0.00000001") == Decimal("1.00000000")

    def test_large_qty_with_coarse_increment(self) -> None:
        # 1.4999 with increment 0.1 → 1.4
        assert round_base_qty(1.4999, "0.1") == Decimal("1.4")

    def test_sub_increment_qty_rounds_to_zero(self) -> None:
        # 0.0000000099 with 8-dp increment → 0.00000000
        result = round_base_qty(0.0000000099, "0.00000001")
        assert result == Decimal("0.00000000")
        assert result == Decimal("0")

    def test_return_type_is_decimal(self) -> None:
        result = round_base_qty(0.5, "0.001")
        assert isinstance(result, Decimal)

    def test_typical_zec_qty(self) -> None:
        # 0.999999 ZEC with 8-dp increment stays as-is (already within precision)
        result = round_base_qty(0.999999, "0.00000001")
        assert result == Decimal("0.99999900")

    def test_partial_fill_remainder(self) -> None:
        # Entry 1.0, partial exit 0.9999999 → remaining 0.0000001
        # With 8-dp increment this rounds to 0.00000010 (1e-7) which is still above 0
        result = round_base_qty(0.0000001, "0.00000001")
        assert result == Decimal("0.00000010")

    def test_never_increases_qty(self) -> None:
        # ROUND_DOWN must never increase the qty
        for raw in (0.001001, 0.0019999, 0.999, 1.0001):
            result = round_base_qty(raw, "0.001")
            assert result <= Decimal(str(raw)), (
                f"round_base_qty({raw}, '0.001') = {result} exceeded input"
            )


# ---------------------------------------------------------------------------
# is_dust
# ---------------------------------------------------------------------------

class TestIsDust:

    def test_above_min_size_not_dust(self) -> None:
        assert is_dust(Decimal("0.002"), "0.001") is False

    def test_exactly_min_size_not_dust(self) -> None:
        # strictly less-than: at the boundary is not dust
        assert is_dust(Decimal("0.001"), "0.001") is False

    def test_below_min_size_is_dust(self) -> None:
        assert is_dust(Decimal("0.0009"), "0.001") is True

    def test_zero_is_always_dust(self) -> None:
        assert is_dust(Decimal("0"), "0.001") is True
        assert is_dust(Decimal("0.00000000"), "0.00000001") is True

    def test_tiny_positive_may_be_dust(self) -> None:
        # 0.00000001 is exactly min_size for 8-dp products → NOT dust
        assert is_dust(Decimal("0.00000001"), "0.00000001") is False
        # 0.000000009 would round to 0 → dust (but is_dust receives already-rounded)
        assert is_dust(Decimal("0.00000000"), "0.00000001") is True


# ---------------------------------------------------------------------------
# round_base_qty + is_dust interaction (mirrors place_exit_outbox usage)
# ---------------------------------------------------------------------------

class TestRoundAndDust:

    def test_full_qty_not_dust(self) -> None:
        rounded = round_base_qty(1.0, "0.00000001")
        assert not is_dust(rounded, "0.001")

    def test_sub_min_remainder_becomes_dust(self) -> None:
        # After 0.9999 of 1.0 ZEC is exited, 0.0001 remains.
        # ZEC base_min_size=0.001 → 0.0001 < 0.001 → DUST
        rounded = round_base_qty(0.0001, "0.00000001")
        assert is_dust(rounded, "0.001")

    def test_exact_min_size_remainder_not_dust(self) -> None:
        rounded = round_base_qty(0.001, "0.00000001")
        assert not is_dust(rounded, "0.001")

    def test_qty_above_min_size_but_floor_rounds_to_zero(self) -> None:
        # 0.0000000099 ZEC: above 0 but rounds to 0 with 8-dp increment → dust
        rounded = round_base_qty(0.0000000099, "0.00000001")
        assert is_dust(rounded, "0.00000001")

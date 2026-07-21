"""
Tests for exchange/adapter.py — production get_order_fn factory.

Verifies that make_get_order_fn() correctly combines Get Order (status +
aggregates) and List Fills (individual fills) into a CoinbaseOrder, and that
mismatches in fill count or filled_size cause None (UNRESOLVED) to be returned.

All tests patch exchange.coinbase_client at the module level so both the
adapter's direct _get_client() call and fetch_fills_for_order()'s internal
_get_client() call hit the same mock client.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import exchange.coinbase_client as _cb
from exchange.adapter import make_get_order_fn
from pipeline.reconciler import CoinbaseOrder


def _order_dict(
    exchange_id: str = "CB-1",
    status: str = "CANCELLED",
    client_order_id: str = "CLIENT-1",
    number_of_fills: int = 0,
    filled_size: str = "0",
) -> dict:
    return {
        "order": {
            "client_order_id": client_order_id,
            "status": status,
            "number_of_fills": str(number_of_fills),
            "filled_size": filled_size,
        }
    }


def _raw_fill(entry_id: str, order_id: str, price: str = "100.0",
              size: str = "0.1", commission: str = "0.01") -> dict:
    return {
        "entry_id": entry_id,
        "order_id": order_id,
        "price": price,
        "size": size,
        "commission": commission,
        "trade_time": "2025-01-01T00:00:00Z",
    }


def _mock_client(order_resp: dict, fills_resp: dict) -> MagicMock:
    """Mock that returns order_resp from get_order and fills_resp from get_fills."""
    client = MagicMock()
    client.get_order.return_value = order_resp
    client.get_fills.return_value = fills_resp
    return client


def _call(
    exchange_id: str,
    order_resp: dict,
    fills_resp: dict,
) -> CoinbaseOrder | None:
    client = _mock_client(order_resp, fills_resp)
    fn = make_get_order_fn()
    with patch.object(_cb, "_DRY_RUN", False), \
         patch.object(_cb, "_get_client", return_value=client):
        return fn(exchange_id)


# ---------------------------------------------------------------------------
# 1. CANCELLED order, no fills → CoinbaseOrder with empty fills
# ---------------------------------------------------------------------------

def test_adapter_cancelled_no_fills() -> None:
    eid = "CB-CANCEL-1"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=0),
        fills_resp={"fills": [], "cursor": ""},
    )
    assert result is not None
    assert isinstance(result, CoinbaseOrder)
    assert result.status == "CANCELLED"
    assert result.exchange_order_id == eid
    assert result.fills == []


# ---------------------------------------------------------------------------
# 2. CANCELLED with 1 fill → fill normalized to CoinbaseFill
# ---------------------------------------------------------------------------

def test_adapter_cancelled_with_fill_normalized() -> None:
    eid = "CB-CANCEL-2"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1, filled_size="0.1"),
        fills_resp={"fills": [_raw_fill("F1", eid)], "cursor": ""},
    )
    assert result is not None
    assert len(result.fills) == 1
    f = result.fills[0]
    assert f.exchange_fill_id == "F1"
    assert f.fill_price == pytest.approx(100.0)
    assert f.fill_qty_base == pytest.approx(0.1)
    assert f.fee_usd == pytest.approx(0.01)
    assert f.filled_at == "2025-01-01T00:00:00Z"


# ---------------------------------------------------------------------------
# 3. Fill count mismatch → None (UNRESOLVED)
# ---------------------------------------------------------------------------

def test_adapter_fill_count_mismatch_returns_none() -> None:
    eid = "CB-CANCEL-3"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=2, filled_size="0.2"),
        fills_resp={"fills": [_raw_fill("F1", eid)], "cursor": ""},  # 1 received, 2 expected
    )
    assert result is None


# ---------------------------------------------------------------------------
# 4. filled_size mismatch → None (UNRESOLVED)
# ---------------------------------------------------------------------------

def test_adapter_filled_size_mismatch_returns_none() -> None:
    eid = "CB-CANCEL-4"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1, filled_size="0.9"),
        fills_resp={"fills": [_raw_fill("F1", eid, size="0.1")], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 5. filled_size within 0.1% tolerance → accepted
# ---------------------------------------------------------------------------

def test_adapter_filled_size_within_tolerance_accepted() -> None:
    eid = "CB-CANCEL-5"
    # 0.10001 vs 0.1 → 0.01% relative error < 0.1% tolerance
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1,
                               filled_size="0.10001"),
        fills_resp={"fills": [_raw_fill("F1", eid, size="0.1")], "cursor": ""},
    )
    assert result is not None
    assert len(result.fills) == 1


# ---------------------------------------------------------------------------
# 6. Transport error on get_order → None (UNRESOLVED, not raised)
# ---------------------------------------------------------------------------

def test_adapter_transport_error_returns_none() -> None:
    client = MagicMock()
    client.get_order.side_effect = TimeoutError("timeout")
    fn = make_get_order_fn()
    with patch.object(_cb, "_DRY_RUN", False), \
         patch.object(_cb, "_get_client", return_value=client):
        result = fn("CB-CANCEL-6")
    assert result is None


# ---------------------------------------------------------------------------
# 7. DRY_RUN → None without any API call
# ---------------------------------------------------------------------------

def test_adapter_dry_run_returns_none_without_api_call() -> None:
    fn = make_get_order_fn()
    with patch.object(_cb, "_DRY_RUN", True), \
         patch.object(_cb, "_get_client") as mock_get:
        result = fn("CB-CANCEL-7")
    mock_get.assert_not_called()
    assert result is None

"""
Tests for exchange/adapter.py — production get_order_fn factory.

Verifies that make_get_order_fn() correctly combines Get Order (status +
aggregates) and List Fills (individual fills) into a CoinbaseOrder, and that
aggregate mismatches, fill validation failures, and pagination errors all cause
None (UNRESOLVED) to be returned rather than incorrect data being accepted.

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
    number_of_fills: int | None = 0,
    filled_size: str | None = "0",
) -> dict:
    """
    Build a Get Order response dict.
    exchange_id is placed in order["order_id"] (the actual Coinbase exchange ID).
    Passing None for number_of_fills or filled_size omits the field (simulates
    absent aggregate — adapter must treat as unknown, not zero).
    """
    order: dict = {
        "order_id": exchange_id,
        "client_order_id": client_order_id,
        "status": status,
    }
    if number_of_fills is not None:
        order["number_of_fills"] = str(number_of_fills)
    if filled_size is not None:
        order["filled_size"] = filled_size
    return {"order": order}


def _raw_fill(entry_id: str, order_id: str, price: str = "100.0",
              size: str = "0.1", commission: str = "0.01",
              size_in_quote: str | None = None) -> dict:
    f: dict = {
        "entry_id": entry_id,
        "order_id": order_id,
        "price": price,
        "size": size,
        "commission": commission,
        "trade_time": "2025-01-01T00:00:00Z",
    }
    if size_in_quote is not None:
        f["size_in_quote"] = size_in_quote
    return f


def _mock_client(order_resp: dict, fills_resp: dict) -> MagicMock:
    """Mock RESTClient with get_order and get_fills pre-configured."""
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
        fills_resp={"fills": [_raw_fill("F1", eid)], "cursor": ""},
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


# ---------------------------------------------------------------------------
# 8. Actual exchange_order_id from API response (not the input parameter)
# ---------------------------------------------------------------------------

def test_adapter_uses_actual_exchange_id_from_api_response() -> None:
    """
    The reconciler's ID mismatch check is only meaningful if the adapter uses
    the order_id returned by the API, not the input parameter we passed in.
    """
    input_eid = "CB-INPUT"
    api_eid = "CB-ACTUAL-FROM-API"
    result = _call(
        input_eid,
        order_resp=_order_dict(api_eid, status="CANCELLED"),  # API returns different ID
        fills_resp={"fills": [], "cursor": ""},
    )
    assert result is not None
    assert result.exchange_order_id == api_eid
    assert result.exchange_order_id != input_eid


# ---------------------------------------------------------------------------
# 9. Missing order_id field in Get Order response → None (UNRESOLVED)
# ---------------------------------------------------------------------------

def test_adapter_missing_order_id_in_response_returns_none() -> None:
    """Get Order returned no order_id — cannot verify ID match."""
    eid = "CB-1"
    order_resp = {"order": {"status": "CANCELLED", "number_of_fills": "0", "filled_size": "0"}}
    result = _call(eid, order_resp=order_resp, fills_resp={"fills": [], "cursor": ""})
    assert result is None


# ---------------------------------------------------------------------------
# 10. Dangerous gap: filled_size > 0 but List Fills returned empty → None
# ---------------------------------------------------------------------------

def test_adapter_filled_size_positive_but_empty_fills_returns_none() -> None:
    """
    The scenario identified by the user: Get Order says filled_size=0.1 but
    List Fills returns [].  This could happen due to pagination failure or
    API delay.  Adapter must not accept it as valid (would understate NAV).
    """
    eid = "CB-CANCEL-10"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=0, filled_size="0.1"),
        fills_resp={"fills": [], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 11. number_of_fills > 0 but List Fills returned empty → None
# ---------------------------------------------------------------------------

def test_adapter_fill_count_positive_but_empty_fills_returns_none() -> None:
    eid = "CB-CANCEL-11"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=2, filled_size="0"),
        fills_resp={"fills": [], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 12. Fills present but aggregates say zero → None (inconsistency)
# ---------------------------------------------------------------------------

def test_adapter_fills_present_but_aggregates_zero_returns_none() -> None:
    eid = "CB-CANCEL-12"
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=0, filled_size="0"),
        fills_resp={"fills": [_raw_fill("F1", eid)], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 13. IncompleteFillHistory from fetch_fills_for_order → None
# ---------------------------------------------------------------------------

def test_adapter_incomplete_fill_history_returns_none() -> None:
    eid = "CB-CANCEL-13"
    fn = make_get_order_fn()
    client = _mock_client(
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=3, filled_size="0.3"),
        fills_resp={},
    )
    with patch.object(_cb, "_DRY_RUN", False), \
         patch.object(_cb, "_get_client", return_value=client), \
         patch.object(_cb, "fetch_fills_for_order",
                      side_effect=_cb.IncompleteFillHistory("cursor cycle")):
        result = fn(eid)
    assert result is None


# ---------------------------------------------------------------------------
# 14. Empty entry_id in fill → None (can't dedup)
# ---------------------------------------------------------------------------

def test_adapter_empty_entry_id_returns_none() -> None:
    eid = "CB-CANCEL-14"
    bad_fill = _raw_fill("", eid)  # empty entry_id
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1, filled_size="0.1"),
        fills_resp={"fills": [bad_fill], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 15. price = 0 in fill → None
# ---------------------------------------------------------------------------

def test_adapter_zero_price_fill_returns_none() -> None:
    eid = "CB-CANCEL-15"
    bad_fill = _raw_fill("F1", eid, price="0")
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1, filled_size="0.1"),
        fills_resp={"fills": [bad_fill], "cursor": ""},
    )
    assert result is None


# ---------------------------------------------------------------------------
# 16. size_in_quote=true → None (size is USD not base currency)
# ---------------------------------------------------------------------------

def test_adapter_size_in_quote_fill_returns_none() -> None:
    eid = "CB-CANCEL-16"
    bad_fill = _raw_fill("F1", eid, size_in_quote="true")
    result = _call(
        eid,
        order_resp=_order_dict(eid, status="CANCELLED", number_of_fills=1, filled_size="0.1"),
        fills_resp={"fills": [bad_fill], "cursor": ""},
    )
    assert result is None

"""
Unit tests for exchange/coinbase_client.py.

place_limit_buy() response parsing (tests 1-9):
  These exercise the Python parsing logic without hitting the real Coinbase API.
  _get_client() is patched so create_order() returns a controlled dict.

cancel_order() polling behaviour (tests 10-17):
  Tests exercise the cancel-then-poll loop introduced in ADR 001 Decision 7.
  _get_client() is patched so cancel_orders() / get_order() return controlled dicts.
  'time' module is also patched to skip actual sleeps.

Scenarios covered:
  1.  success=True + success_response.order_id        → returns order_id
  2.  success=True + top-level order_id               → returns order_id
  3.  success=False + known code (INSUFFICIENT_FUND)  → CoinbaseOrderRejected
  4.  success=False + known code (INVALID_LIMIT_PRICE_POST_ONLY) → CoinbaseOrderRejected
  5.  success=False + unknown code                    → RuntimeError (ambiguous)
  6.  success=False + missing error_response          → RuntimeError (ambiguous)
  7.  success=True but no order_id anywhere           → RuntimeError (malformed)
  8.  success=True + empty string order_id            → RuntimeError (malformed)
  9.  transport/timeout exception                     → propagates unchanged
  10. cancel request rejected (success=False)         → False immediately
  11. CANCEL_QUEUED → PENDING_CANCEL → CANCELLED       → True (normal flow)
  12. Consistently CANCEL_QUEUED for all 3 polls       → False (never confirms)
  13. OPEN read-model lag → CANCEL_QUEUED → CANCELLED  → True (continues polling)
  14. FILLED during cancel window                      → False (stop polling)
  15. malformed get_order response (no status)         → False
  16. transport exception in get_order after cancel    → False
  17. cancel_orders transport exception                → False
"""
from __future__ import annotations

from unittest.mock import MagicMock, call, patch

import pytest

import exchange.coinbase_client as _mod
from exchange.coinbase_client import (
    CoinbaseOrderRejected,
    IncompleteFillHistory,
    cancel_order,
    fetch_fills_for_order,
    place_limit_buy,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _fake_client(response: dict) -> MagicMock:
    """Return a mock RESTClient whose create_order() returns `response`."""
    client = MagicMock()
    client.create_order.return_value = response
    return client


def _call(response: dict) -> str:
    """
    Call place_limit_buy() with DRY_RUN=False and the given fake API response.
    Patches both _DRY_RUN (module flag) and _get_client (returns fake client).
    """
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=_fake_client(response)):
        return place_limit_buy("ZEC-USD", 10.0, 100.0, client_order_id="TEST-ID")


# ---------------------------------------------------------------------------
# 1. Accepted orders
# ---------------------------------------------------------------------------

def test_success_response_order_id_returned() -> None:
    """Coinbase v3 success path: order_id inside success_response."""
    order_id = _call({
        "success": True,
        "success_response": {"order_id": "CB-ORDER-123"},
    })
    assert order_id == "CB-ORDER-123"


def test_top_level_order_id_returned() -> None:
    """Alternative response shape: order_id at the top level."""
    order_id = _call({
        "order_id": "CB-TOP-LEVEL",
        "success": True,
    })
    assert order_id == "CB-TOP-LEVEL"


# ---------------------------------------------------------------------------
# 2. Definitive rejections → CoinbaseOrderRejected
# ---------------------------------------------------------------------------

def test_insufficient_fund_raises_coinbase_order_rejected() -> None:
    with pytest.raises(CoinbaseOrderRejected, match="INSUFFICIENT_FUND"):
        _call({
            "success": False,
            "error_response": {
                "error": "INSUFFICIENT_FUND",
                "message": "Insufficient fund",
            },
        })


def test_post_only_rejection_raises_coinbase_order_rejected() -> None:
    with pytest.raises(CoinbaseOrderRejected, match="INVALID_LIMIT_PRICE_POST_ONLY"):
        _call({
            "success": False,
            "error_response": {
                "error": "INVALID_LIMIT_PRICE_POST_ONLY",
                "message": "Post-only order would cross",
            },
        })


def test_product_offline_raises_coinbase_order_rejected() -> None:
    with pytest.raises(CoinbaseOrderRejected, match="PRODUCT_OFFLINE"):
        _call({
            "success": False,
            "error_response": {"error": "PRODUCT_OFFLINE", "message": ""},
        })


# ---------------------------------------------------------------------------
# 3. Ambiguous rejections → RuntimeError (leave order SUBMITTING)
# ---------------------------------------------------------------------------

def test_unknown_error_code_raises_runtime_error() -> None:
    """
    An error code not in _DEFINITE_REJECTION_CODES is ambiguous.
    Outbox treats RuntimeError as ambiguous → order stays SUBMITTING.
    """
    with pytest.raises(RuntimeError, match="UNKNOWN_FAILURE_REASON"):
        _call({
            "success": False,
            "error_response": {
                "error": "UNKNOWN_FAILURE_REASON",
                "message": "Something went wrong",
            },
        })


def test_missing_error_response_raises_runtime_error() -> None:
    """success=False with no error_response is also treated as ambiguous."""
    with pytest.raises(RuntimeError):
        _call({"success": False})


def test_empty_error_code_raises_runtime_error() -> None:
    """success=False with empty error code string → ambiguous RuntimeError."""
    with pytest.raises(RuntimeError):
        _call({
            "success": False,
            "error_response": {"error": "", "message": "unknown"},
        })


# ---------------------------------------------------------------------------
# 4. Malformed accepted responses → RuntimeError
# ---------------------------------------------------------------------------

def test_success_true_no_order_id_raises() -> None:
    """
    Coinbase says success=True but provides no order_id.
    This is a malformed response — treat as ambiguous (RuntimeError, not
    CoinbaseOrderRejected) so the outbox leaves the order SUBMITTING.
    """
    with pytest.raises(RuntimeError, match="no order_id"):
        _call({"success": True, "success_response": {}})


def test_success_true_empty_order_id_raises() -> None:
    """Empty string order_id is also malformed."""
    with pytest.raises(RuntimeError, match="no order_id"):
        _call({"success": True, "order_id": "", "success_response": {"order_id": ""}})


# ---------------------------------------------------------------------------
# 5. Transport / timeout exceptions propagate unchanged
# ---------------------------------------------------------------------------

def test_transport_exception_propagates() -> None:
    """
    Network errors (timeout, connection reset) must propagate as-is.
    Outbox's generic except clause catches them and leaves order SUBMITTING.
    """
    client = MagicMock()
    client.create_order.side_effect = TimeoutError("connection timed out")

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        with pytest.raises(TimeoutError, match="connection timed out"):
            place_limit_buy("ZEC-USD", 10.0, 100.0)


def test_connection_reset_propagates() -> None:
    client = MagicMock()
    client.create_order.side_effect = ConnectionResetError("reset by peer")

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        with pytest.raises(ConnectionResetError):
            place_limit_buy("ZEC-USD", 10.0, 100.0)


# ---------------------------------------------------------------------------
# 6. Dry-run mode — never hits the network
# ---------------------------------------------------------------------------

def test_dry_run_returns_synthetic_id_without_network_call() -> None:
    """In DRY_RUN mode, no API call is made and a DRY- prefix ID is returned."""
    with patch.object(_mod, "_DRY_RUN", True), \
         patch.object(_mod, "_get_client") as mock_get:
        result = place_limit_buy("ZEC-USD", 10.0, 100.0, client_order_id="MY-ID")

    mock_get.assert_not_called()
    assert result.startswith("DRY-")


# ---------------------------------------------------------------------------
# cancel_order() polling behaviour — tests 10–17
# ---------------------------------------------------------------------------

def _cancel_client(cancel_success: bool, get_order_statuses: list[str]) -> MagicMock:
    """
    Mock RESTClient for cancel_order() tests.
    cancel_orders() returns {results: [{success: cancel_success}]}.
    get_order() returns each status from get_order_statuses in sequence;
    once exhausted, returns "UNKNOWN".
    """
    client = MagicMock()
    client.cancel_orders.return_value = {
        "results": [{"success": cancel_success}]
    }
    status_iter = iter(get_order_statuses)

    def get_order_side_effect(order_id, **kw):
        return {"order": {"status": next(status_iter, "UNKNOWN")}}

    client.get_order.side_effect = get_order_side_effect
    return client


def _run_cancel(cancel_success: bool, get_order_statuses: list[str]) -> bool:
    """Run cancel_order() with a patched client and patched time.sleep."""
    client = _cancel_client(cancel_success, get_order_statuses)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client), \
         patch("exchange.coinbase_client.time") as mock_time:
        mock_time.sleep = MagicMock()
        return cancel_order("CB-ORD-TEST")


# 10. Cancel request rejected → False without polling
def test_cancel_request_rejected_returns_false() -> None:
    """Batch Cancel success=False → return False immediately, no get_order call."""
    client = _cancel_client(cancel_success=False, get_order_statuses=[])
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client), \
         patch("exchange.coinbase_client.time"):
        result = cancel_order("CB-ORD-REJ")
    assert result is False
    client.get_order.assert_not_called()


# 11. Normal flow: CANCEL_QUEUED → PENDING_CANCEL → CANCELLED → True
def test_cancel_normal_flow_queued_to_cancelled() -> None:
    result = _run_cancel(True, ["CANCEL_QUEUED", "PENDING_CANCEL", "CANCELLED"])
    assert result is True


# 12. Consistently CANCEL_QUEUED for all 3 polls → False (never confirms)
def test_cancel_always_queued_returns_false() -> None:
    result = _run_cancel(True, ["CANCEL_QUEUED", "CANCEL_QUEUED", "CANCEL_QUEUED"])
    assert result is False


# 13. Read-model lag: OPEN first, then CANCEL_QUEUED, then CANCELLED → True
def test_cancel_open_lag_then_cancelled() -> None:
    """OPEN status is read-model lag after cancel request — continue polling."""
    result = _run_cancel(True, ["OPEN", "CANCEL_QUEUED", "CANCELLED"])
    assert result is True


# 14. FILLED during cancel window → False (stop polling, order executed)
def test_cancel_filled_during_window_returns_false() -> None:
    result = _run_cancel(True, ["CANCEL_QUEUED", "FILLED"])
    assert result is False


# 15. Malformed get_order response (no 'order' key, no 'status') → False
def test_cancel_malformed_get_order_returns_false() -> None:
    client = MagicMock()
    client.cancel_orders.return_value = {"results": [{"success": True}]}
    client.get_order.return_value = {}  # no 'order' key, no 'status'

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client), \
         patch("exchange.coinbase_client.time"):
        result = cancel_order("CB-MALFORMED")
    assert result is False


# 16. Transport exception in get_order after successful cancel request → False
def test_cancel_get_order_transport_error_returns_false() -> None:
    client = MagicMock()
    client.cancel_orders.return_value = {"results": [{"success": True}]}
    client.get_order.side_effect = TimeoutError("read timeout")

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client), \
         patch("exchange.coinbase_client.time"):
        result = cancel_order("CB-TIMEOUT")
    assert result is False


# 17. Transport exception in cancel_orders → False
def test_cancel_orders_transport_error_returns_false() -> None:
    client = MagicMock()
    client.cancel_orders.side_effect = ConnectionResetError("reset by peer")

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        result = cancel_order("CB-CONN-ERR")
    assert result is False


# ---------------------------------------------------------------------------
# fetch_fills_for_order() — tests 18–25
# ---------------------------------------------------------------------------

_ORDER_ID = "CB-ORD-FILL"


def _fill(entry_id: str, order_id: str = _ORDER_ID) -> dict:
    """Minimal fill dict matching Coinbase List Fills response structure."""
    return {
        "entry_id": entry_id,
        "order_id": order_id,
        "price": "100.0",
        "size": "0.1",
        "commission": "0.01",
        "trade_time": "2025-01-01T00:00:00Z",
    }


def _fills_client(pages: list[list[dict]], cursors: list[str]) -> MagicMock:
    """
    Mock RESTClient for fetch_fills_for_order() tests.
    get_fills() returns each page/cursor pair in sequence.
    Returns plain dicts (simulating _resp_to_dict passthrough).
    """
    client = MagicMock()
    responses = [
        {"fills": page, "cursor": cursor}
        for page, cursor in zip(pages, cursors)
    ]
    client.get_fills.side_effect = responses
    return client


def _run_fetch(pages: list[list[dict]], cursors: list[str]) -> list[dict]:
    """Run fetch_fills_for_order(); asserts get_fills (not list_fills) was called."""
    client = _fills_client(pages, cursors)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        result = fetch_fills_for_order(_ORDER_ID)
    client.get_fills.assert_called()
    return result


# 18. Single page, no cursor → all fills returned in one call
def test_fetch_fills_single_page_no_cursor() -> None:
    fills = [_fill("F1"), _fill("F2")]
    result = _run_fetch(pages=[fills], cursors=[""])
    assert [f["entry_id"] for f in result] == ["F1", "F2"]


# 19. Multi-page pagination → all pages fetched and concatenated in order
def test_fetch_fills_multi_page_pagination() -> None:
    page1 = [_fill("F1"), _fill("F2")]
    page2 = [_fill("F3")]
    result = _run_fetch(pages=[page1, page2], cursors=["page-cursor", ""])
    assert [f["entry_id"] for f in result] == ["F1", "F2", "F3"]


# 20. Empty result → []
def test_fetch_fills_empty_result() -> None:
    result = _run_fetch(pages=[[]], cursors=[""])
    assert result == []


# 21. DRY_RUN → [] without any API call
def test_fetch_fills_dry_run_returns_empty_without_api_call() -> None:
    with patch.object(_mod, "_DRY_RUN", True), \
         patch.object(_mod, "_get_client") as mock_get:
        result = fetch_fills_for_order("CB-ORD-DRY")
    mock_get.assert_not_called()
    assert result == []


# 22. Transport exception propagates unchanged (caller decides UNRESOLVED vs re-raise)
def test_fetch_fills_transport_exception_propagates() -> None:
    client = MagicMock()
    client.get_fills.side_effect = TimeoutError("connection timed out")

    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        with pytest.raises(TimeoutError, match="connection timed out"):
            fetch_fills_for_order("CB-ORD-TIMEOUT")


# 23. Stray fills (order_id mismatch) are silently dropped
def test_fetch_fills_stray_order_id_dropped() -> None:
    own_fill = _fill("F1", order_id=_ORDER_ID)
    stray_fill = _fill("F2", order_id="DIFFERENT-ORDER")
    result = _run_fetch(pages=[[own_fill, stray_fill]], cursors=[""])
    assert [f["entry_id"] for f in result] == ["F1"]


# 24. Cursor cycle raises IncompleteFillHistory (not silently breaks)
def test_fetch_fills_cursor_cycle_raises() -> None:
    """Repeated cursor means Coinbase pagination is cycling — must raise, not return."""
    page = [_fill("F1")]
    client = _fills_client(pages=[page, page], cursors=["same-cursor", "same-cursor"])
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        with pytest.raises(IncompleteFillHistory, match="cycle"):
            fetch_fills_for_order(_ORDER_ID)


# 25. Duplicate entry_id across pages deduplicated idempotently
def test_fetch_fills_duplicate_entry_id_deduplicated() -> None:
    page1 = [_fill("F1"), _fill("F2")]
    page2 = [_fill("F2"), _fill("F3")]  # F2 repeated
    result = _run_fetch(pages=[page1, page2], cursors=["cursor-1", ""])
    assert [f["entry_id"] for f in result] == ["F1", "F2", "F3"]


# 26. Empty page with pending cursor raises IncompleteFillHistory
def test_fetch_fills_empty_page_with_cursor_raises() -> None:
    """Empty page while API still claims more pages → fill history incomplete."""
    client = _fills_client(pages=[[]], cursors=["pending-cursor"])
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client):
        with pytest.raises(IncompleteFillHistory, match="empty page"):
            fetch_fills_for_order(_ORDER_ID)


# 27. _MAX_FILL_PAGES exhausted with pending cursor raises IncompleteFillHistory
def test_fetch_fills_max_pages_exhausted_raises() -> None:
    """Pagination hit the page cap while cursor is still pending — must raise."""
    page = [_fill("F1")]
    # Two pages with unique cursors: patch _MAX_FILL_PAGES=2 so the cap triggers.
    client = _fills_client(pages=[page, page], cursors=["cursor-1", "cursor-2"])
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=client), \
         patch.object(_mod, "_MAX_FILL_PAGES", 2):
        with pytest.raises(IncompleteFillHistory, match="exhausted"):
            fetch_fills_for_order(_ORDER_ID)


# ---------------------------------------------------------------------------
# 28. place_market_sell — wire qty uses ROUND_DOWN, never rounds up
# ---------------------------------------------------------------------------

def test_place_market_sell_wire_qty_uses_round_down() -> None:
    """
    The base_size string sent to Coinbase must be Decimal-ROUND_DOWN, not
    Python round() (ROUND_HALF_EVEN).  For quantities already pre-rounded by
    place_exit_outbox, the re-format must not increase the value.
    """
    from exchange.coinbase_client import place_market_sell

    captured: list[str] = []

    def _fake_create_order(**kwargs):
        cfg = kwargs.get("order_configuration", {})
        captured.append(cfg.get("market_market_ioc", {}).get("base_size", ""))
        return {"success": True, "success_response": {"order_id": "EX-WIRE-001"}}

    mock_client = MagicMock()
    mock_client.create_order.side_effect = _fake_create_order

    # Quantity at 8-dp boundary: 0.99999999 ZEC (already ROUND_DOWN'd by outbox)
    qty_float = 0.99999999
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_get_client", return_value=mock_client):
        place_market_sell("ZEC-USD", qty_float, client_order_id="wire-test-001")

    assert len(captured) == 1
    wire_qty = captured[0]
    from decimal import Decimal
    assert Decimal(wire_qty) <= Decimal(str(qty_float)), (
        f"Wire qty {wire_qty!r} must not exceed input {qty_float} — "
        "ROUND_DOWN means we can only send less than we own, never more"
    )
    assert "E" not in wire_qty.upper() or "e" not in wire_qty, (
        "base_size must be a plain decimal string, not scientific notation"
    )


# ---------------------------------------------------------------------------
# 29. get_product_info — fails hard on missing or non-numeric required fields
# ---------------------------------------------------------------------------

def test_get_product_info_fails_on_missing_base_increment() -> None:
    """Response missing base_increment must raise RuntimeError, not silently use defaults."""
    from exchange.coinbase_client import get_product_info
    import exchange.coinbase_client as client_mod

    resp = MagicMock()
    resp.to_dict.return_value = {"base_min_size": "0.001"}  # base_increment absent

    mock_client = MagicMock()
    mock_client.get_product.return_value = resp

    with patch.object(client_mod, "_DRY_RUN", False), \
         patch.object(client_mod, "_get_client", return_value=mock_client), \
         patch.dict(client_mod._product_cache, {}, clear=True):
        with pytest.raises(RuntimeError, match="missing required fields"):
            get_product_info("ZEC-USD")


def test_get_product_info_fails_on_non_numeric_base_increment() -> None:
    """Non-numeric base_increment (e.g. empty string) must raise RuntimeError."""
    from exchange.coinbase_client import get_product_info
    import exchange.coinbase_client as client_mod

    resp = MagicMock()
    resp.to_dict.return_value = {
        "base_increment": "not-a-number",
        "base_min_size": "0.001",
    }

    mock_client = MagicMock()
    mock_client.get_product.return_value = resp

    with patch.object(client_mod, "_DRY_RUN", False), \
         patch.object(client_mod, "_get_client", return_value=mock_client), \
         patch.dict(client_mod._product_cache, {}, clear=True):
        with pytest.raises(RuntimeError, match="non-numeric"):
            get_product_info("ZEC-USD")


# ---------------------------------------------------------------------------
# 30. get_product_info — rejects NaN, Infinity, zero, and negative values
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("bad_increment,bad_min", [
    ("NaN",      "0.001"),    # NaN parses as Decimal but is not finite
    ("Infinity", "0.001"),    # Infinity parses as Decimal but is not finite
    ("0",        "0.001"),    # zero increment is invalid (division by zero in rounding)
    ("-0.001",   "0.001"),    # negative increment is invalid
    ("0.001",    "NaN"),      # NaN min_size
    ("0.001",    "0"),        # zero min_size
    ("0.001",    "-1"),       # negative min_size
])
def test_get_product_info_rejects_invalid_numeric_values(
    bad_increment: str, bad_min: str
) -> None:
    """NaN, Infinity, zero, and negative values must raise RuntimeError even though
    Decimal() accepts them without raising."""
    from exchange.coinbase_client import get_product_info
    import exchange.coinbase_client as client_mod

    resp = MagicMock()
    resp.to_dict.return_value = {
        "base_increment": bad_increment,
        "base_min_size":  bad_min,
    }

    mock_client = MagicMock()
    mock_client.get_product.return_value = resp

    with patch.object(client_mod, "_DRY_RUN", False), \
         patch.object(client_mod, "_get_client", return_value=mock_client), \
         patch.dict(client_mod._product_cache, {}, clear=True):
        with pytest.raises(RuntimeError, match="finite and positive|non-numeric"):
            get_product_info("ZEC-USD")

"""
Unit tests for exchange/coinbase_client.py — place_limit_buy() response parsing.

These tests exercise the Python parsing logic without hitting the real Coinbase
API.  _get_client() is patched so create_order() returns a controlled dict.
DRY_RUN is forced False for every test via monkeypatching the module-level flag.

Scenarios covered:
  1. success=True + success_response.order_id        → returns order_id
  2. success=True + top-level order_id               → returns order_id
  3. success=False + known code (INSUFFICIENT_FUND)  → CoinbaseOrderRejected
  4. success=False + known code (INVALID_LIMIT_PRICE_POST_ONLY) → CoinbaseOrderRejected
  5. success=False + unknown code                    → RuntimeError (ambiguous)
  6. success=False + missing error_response          → RuntimeError (ambiguous)
  7. success=True but no order_id anywhere           → RuntimeError (malformed)
  8. success=True + empty string order_id            → RuntimeError (malformed)
  9. transport/timeout exception                     → propagates unchanged
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

import exchange.coinbase_client as _mod
from exchange.coinbase_client import CoinbaseOrderRejected, place_limit_buy


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

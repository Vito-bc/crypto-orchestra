"""
Tests for pipeline/preflight.py — read-only Coinbase preflight.

All tests mock the _ReadOnlyClient so no real API calls are made.
"""
from __future__ import annotations

from decimal import Decimal
from typing import Any
from unittest.mock import MagicMock, patch

import pytest

import pipeline.preflight as _mod
from pipeline.preflight import (
    AccountSummary,
    KeyPermissions,
    PreflightResult,
    ProductState,
    _check_accounts,
    _check_key_permissions,
    _check_portfolio_uuid,
    _check_product,
    _dry_run_result,
    _ReadOnlyClient,
    run_preflight,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ok_permissions_resp(**overrides) -> MagicMock:
    data = {
        "can_view": True, "can_trade": True, "can_transfer": False,
        "portfolio_uuid": "abc12345-1111-2222-3333-444455556666",
        **overrides,
    }
    m = MagicMock()
    m.to_dict.return_value = data
    return m


def _ok_accounts_resp(cursor="", has_next=False) -> dict:
    return {
        "accounts": [
            {
                "currency": "USD",
                "available_balance": {"value": "500.00", "currency": "USD"},
                "hold": {"value": "10.00", "currency": "USD"},
                "active": True,
                "ready": True,
            }
        ],
        "has_next": has_next,
        "cursor": cursor,
    }


def _ok_product_resp(product_id: str = "ZEC-USD") -> dict:
    return {
        "product_id": product_id,
        "base_increment": "0.00000001",
        "base_min_size": "0.001",
        "base_max_size": "9000",
        "quote_increment": "0.01",
        "quote_min_size": "1",
        "quote_max_size": "999999",
        "is_disabled": False,
        "trading_disabled": False,
        "cancel_only": False,
        "limit_only": False,
        "post_only": False,
        "auction_mode": False,
        "view_only": False,
    }


def _make_client(**overrides) -> _ReadOnlyClient:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = _ok_permissions_resp()
    inner.get_portfolios.return_value = MagicMock(
        to_dict=lambda: {"portfolios": [{"uuid": "abc12345-1111-2222-3333-444455556666"}]}
    )
    inner.get_accounts.return_value = _ok_accounts_resp()
    inner.get_product.return_value = _ok_product_resp()
    for k, v in overrides.items():
        setattr(inner, k, v)
    return _ReadOnlyClient(inner)


# ── DRY_RUN mode ──────────────────────────────────────────────────────────────

def test_dry_run_result_is_ok() -> None:
    result = _dry_run_result(["ZEC-USD", "ETH-USD"])
    assert result.overall_status == "OK"
    assert result.entry_allowed()
    assert result.exit_allowed()
    assert len(result.product_states) == 2
    assert all(p.tradeable for p in result.product_states)
    assert result.accounts_summary[0].available_balance == Decimal("100")


def test_run_preflight_dry_run_skips_api() -> None:
    with patch.object(_mod, "_DRY_RUN", True), \
         patch.object(_mod, "_build_read_only_client") as build:
        result = run_preflight(["ZEC-USD"])
    build.assert_not_called()
    assert result.overall_status == "OK"


# ── Key permissions ───────────────────────────────────────────────────────────

def test_key_permissions_ok() -> None:
    client = _make_client()
    errors: list[str] = []
    kp = _check_key_permissions(client, errors)
    assert kp is not None
    assert kp.can_view is True
    assert kp.can_trade is True
    assert kp.can_transfer is False
    assert not errors


def test_key_permissions_can_transfer_adds_error() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = _ok_permissions_resp(can_transfer=True)
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    kp = _check_key_permissions(client, errors)
    assert kp is not None
    assert kp.can_transfer is True
    assert any("can_transfer" in e for e in errors)


def test_key_permissions_no_can_view_adds_error() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = _ok_permissions_resp(can_view=False)
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    kp = _check_key_permissions(client, errors)
    assert any("can_view" in e for e in errors)


def test_key_permissions_api_failure_returns_none() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.side_effect = RuntimeError("network down")
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    kp = _check_key_permissions(client, errors)
    assert kp is None
    assert any("get_api_key_permissions" in e for e in errors)


# ── Portfolio UUID ────────────────────────────────────────────────────────────

def test_portfolio_uuid_from_key_permissions() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="abc12345-dead-beef-0000-111122223333",
    )
    client = _make_client()
    errors: list[str] = []
    uuid = _check_portfolio_uuid(kp, client, errors)
    assert uuid == "abc12345-dead-beef-0000-111122223333"
    assert not errors


def test_portfolio_uuid_mismatch_adds_error() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="abc12345-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    )
    client = _make_client()
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "different-uuid-xxxx"}):
        _check_portfolio_uuid(kp, client, errors)
    assert any("mismatch" in e for e in errors)


def test_portfolio_uuid_masked_in_repr() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="secret-full-uuid-1234",
    )
    r = repr(kp)
    assert "secret-f" in r
    assert "secret-full-uuid-1234" not in r


# ── Accounts ──────────────────────────────────────────────────────────────────

def test_accounts_parses_usd_balance() -> None:
    client = _make_client()
    errors: list[str] = []
    summaries = _check_accounts(client, errors)
    assert len(summaries) == 1
    assert summaries[0].currency == "USD"
    assert summaries[0].available_balance == Decimal("500.00")
    assert summaries[0].hold == Decimal("10.00")
    assert summaries[0].active is True
    assert summaries[0].ready is True
    assert not errors


def test_accounts_inactive_adds_error() -> None:
    inner = MagicMock()
    inactive = _ok_accounts_resp()
    inactive["accounts"][0]["active"] = False
    inner.get_accounts.return_value = inactive
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    _check_accounts(client, errors)
    assert any("not active" in e for e in errors)


def test_accounts_no_usd_adds_error() -> None:
    inner = MagicMock()
    no_usd = {"accounts": [{"currency": "BTC"}], "has_next": False, "cursor": ""}
    inner.get_accounts.return_value = no_usd
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    summaries = _check_accounts(client, errors)
    assert summaries == []
    assert any("No USD account" in e for e in errors)


def test_accounts_paginates_until_no_next() -> None:
    inner = MagicMock()
    page1 = {"accounts": [], "has_next": True, "cursor": "tok1"}
    page2 = _ok_accounts_resp()   # has_next=False, cursor=""
    inner.get_accounts.side_effect = [page1, page2]
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    summaries = _check_accounts(client, errors)
    assert inner.get_accounts.call_count == 2
    assert len(summaries) == 1


def test_accounts_api_failure_adds_error() -> None:
    inner = MagicMock()
    inner.get_accounts.side_effect = RuntimeError("timeout")
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    summaries = _check_accounts(client, errors)
    assert summaries == []
    assert any("get_accounts" in e for e in errors)


# ── Product state ─────────────────────────────────────────────────────────────

def test_product_ok() -> None:
    client = _make_client()
    errors: list[str] = []
    state = _check_product(client, "ZEC-USD", errors)
    assert state is not None
    assert state.tradeable is True
    assert state.base_increment == "0.00000001"
    assert not errors


def test_product_is_disabled_blocks_trading() -> None:
    inner = MagicMock()
    disabled = _ok_product_resp()
    disabled["is_disabled"] = True
    inner.get_product.return_value = disabled
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    state = _check_product(client, "ZEC-USD", errors)
    assert state is not None
    assert state.tradeable is False
    assert any("is_disabled" in e for e in errors)


def test_product_limit_only_blocks_market_sell() -> None:
    inner = MagicMock()
    lo = _ok_product_resp()
    lo["limit_only"] = True
    inner.get_product.return_value = lo
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    state = _check_product(client, "ZEC-USD", errors)
    assert state is not None
    assert state.tradeable is False
    assert any("limit_only" in e for e in errors)


def test_product_api_failure_returns_none() -> None:
    inner = MagicMock()
    inner.get_product.side_effect = RuntimeError("connection refused")
    client = _ReadOnlyClient(inner)
    errors: list[str] = []
    state = _check_product(client, "ZEC-USD", errors)
    assert state is None
    assert any("get_product" in e for e in errors)


# ── Full run_preflight (LIVE path, mocked) ────────────────────────────────────

def test_run_preflight_ok_returns_ok_status() -> None:
    client = _make_client()
    client._c.get_product.return_value = _ok_product_resp("ZEC-USD")
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "OK"
    assert result.entry_allowed()
    assert result.exit_allowed()
    assert result.latency_ms >= 0


def test_run_preflight_can_transfer_blocks_entry() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = _ok_permissions_resp(can_transfer=True)
    inner.get_portfolios.return_value = MagicMock(
        to_dict=lambda: {"portfolios": [{"uuid": "abc12345"}]}
    )
    inner.get_accounts.return_value = _ok_accounts_resp()
    inner.get_product.return_value = _ok_product_resp()
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status in ("ENTRY_BLOCKED", "CRITICAL")
    assert not result.entry_allowed()
    assert result.exit_allowed()    # EXIT always allowed


def test_run_preflight_key_file_missing_returns_critical() -> None:
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_KEY_FILE", _mod.ROOT / "no_such_key.json"):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "CRITICAL"
    assert not result.entry_allowed()


def test_run_preflight_read_only_facade_has_no_create_order() -> None:
    client = _make_client()
    assert not hasattr(client, "create_order"), (
        "_ReadOnlyClient must not expose create_order"
    )
    assert not hasattr(client, "cancel_order"), (
        "_ReadOnlyClient must not expose cancel_order"
    )

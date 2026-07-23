"""
Tests for pipeline/preflight.py — read-only Coinbase preflight (hardened).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import pipeline.preflight as _mod
from pipeline.preflight import (
    AccountSummary,
    KeyPermissions,
    PreflightResult,
    ProductState,
    _ReadOnlyClient,
    _check_accounts,
    _check_key_permissions,
    _check_portfolio_uuid,
    _check_product,
    _dry_run_result,
    _strict_bool,
    run_preflight,
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _ok_permissions_data(**overrides) -> dict:
    return {
        "can_view": True, "can_trade": True, "can_transfer": False,
        "portfolio_uuid": "abc12345-1111-2222-3333-444455556666",
        **overrides,
    }


def _ok_accounts_page(cursor="", has_next=False) -> dict:
    return {
        "accounts": [
            {
                "uuid": "acct-usd-001",
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


def _ok_product_data(product_id: str = "ZEC-USD") -> dict:
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


def _make_inner(**overrides):
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = _ok_permissions_data()
    inner.get_portfolios.return_value = {
        "portfolios": [{"uuid": "abc12345-1111-2222-3333-444455556666"}]
    }
    inner.get_accounts.return_value = _ok_accounts_page()
    inner.get_product.return_value = _ok_product_data()
    for k, v in overrides.items():
        setattr(inner, k, v)
    return inner


def _make_client(**overrides) -> _ReadOnlyClient:
    return _ReadOnlyClient(_make_inner(**overrides))


# ── _strict_bool ──────────────────────────────────────────────────────────────

def test_strict_bool_accepts_true() -> None:
    errs: list[str] = []
    assert _strict_bool(True, "f", errs) is True
    assert not errs


def test_strict_bool_accepts_false() -> None:
    errs: list[str] = []
    assert _strict_bool(False, "f", errs) is False
    assert not errs


def test_strict_bool_rejects_string_false() -> None:
    errs: list[str] = []
    result = _strict_bool("false", "can_view", errs)
    assert result is None
    assert any("CRITICAL" in e for e in errs)


def test_strict_bool_rejects_string_true() -> None:
    errs: list[str] = []
    result = _strict_bool("true", "can_view", errs)
    assert result is None
    assert any("CRITICAL" in e for e in errs)


def test_strict_bool_rejects_integer_zero() -> None:
    errs: list[str] = []
    result = _strict_bool(0, "active", errs)
    assert result is None
    assert any("CRITICAL" in e for e in errs)


def test_strict_bool_rejects_none() -> None:
    errs: list[str] = []
    result = _strict_bool(None, "ready", errs)
    assert result is None
    assert any("CRITICAL" in e for e in errs)


# ── DRY_RUN mode ──────────────────────────────────────────────────────────────

def test_dry_run_result_is_ok() -> None:
    result = _dry_run_result(["ZEC-USD", "ETH-USD"])
    assert result.overall_status == "OK"
    assert result.entry_allowed()
    assert result.exit_supervision_allowed()
    assert result.exit_allowed()   # deprecated alias still works
    assert len(result.product_states) == 2
    assert all(p.entry_supported and p.market_exit_supported for p in result.product_states)


def test_run_preflight_dry_run_no_live_reads_skips_api() -> None:
    with patch.object(_mod, "_DRY_RUN", True), \
         patch.object(_mod, "_build_read_only_client") as build:
        result = run_preflight(["ZEC-USD"])
    build.assert_not_called()
    assert result.overall_status == "OK"


def test_run_preflight_dry_run_with_live_reads_calls_api() -> None:
    client = _make_client()
    with patch.object(_mod, "_DRY_RUN", True), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"], live_reads=True)
    assert client._c.get_api_key_permissions.called


# ── Key permissions — strict bool ─────────────────────────────────────────────

def test_key_permissions_ok() -> None:
    client = _ReadOnlyClient(MagicMock(
        get_api_key_permissions=lambda: _ok_permissions_data()
    ))
    errors: list[str] = []
    kp = _check_key_permissions(client, errors)
    assert kp is not None
    assert kp.can_view and kp.can_trade and not kp.can_transfer
    assert not errors


def test_key_permissions_string_bool_is_critical() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = {
        **_ok_permissions_data(), "can_view": "true"
    }
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is None
    assert any("CRITICAL" in e for e in errors)


def test_key_permissions_can_transfer_adds_entry_blocked_error() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = {
        **_ok_permissions_data(), "can_transfer": True
    }
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is not None
    assert kp.can_transfer is True
    assert any("can_transfer" in e for e in errors)
    assert not any("CRITICAL" in e for e in errors)


def test_key_permissions_can_trade_false_adds_error() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = {
        **_ok_permissions_data(), "can_trade": False
    }
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is not None
    assert not kp.can_trade
    assert any("can_trade" in e for e in errors)


def test_key_permissions_no_can_view_is_critical() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = {
        **_ok_permissions_data(), "can_view": False
    }
    errors: list[str] = []
    _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "can_view" in e for e in errors)


def test_key_permissions_api_failure_is_critical() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.side_effect = RuntimeError("network down")
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is None
    assert any("CRITICAL" in e for e in errors)


def test_key_permissions_uuid_masked_in_repr() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="secret-full-uuid-1234",
    )
    r = repr(kp)
    assert "secret-f" in r
    assert "secret-full-uuid-1234" not in r


# ── Portfolio UUID ────────────────────────────────────────────────────────────

def test_portfolio_uuid_ok_when_env_matches_key() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="abc12345-dead-beef-0000-111122223333",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {
        "portfolios": [{"uuid": "abc12345-dead-beef-0000-111122223333"}]
    }
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-dead-beef-0000-111122223333"}):
        uuid = _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert uuid == "abc12345-dead-beef-0000-111122223333"
    assert not errors


def test_portfolio_uuid_mismatch_is_critical() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="abc12345-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": []}
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "different-uuid-xxxx-yyyy"}):
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "mismatch" in e for e in errors)


def test_portfolio_uuid_not_set_adds_entry_blocked_error() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False,
        portfolio_uuid="abc12345-dead-beef-0000-111122223333",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {
        "portfolios": [{"uuid": "abc12345-dead-beef-0000-111122223333"}]
    }
    errors: list[str] = []
    with patch.dict("os.environ", {}, clear=True):
        # Remove the env var if it exists
        import os
        os.environ.pop("COINBASE_PORTFOLIO_UUID", None)
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("COINBASE_PORTFOLIO_UUID" in e for e in errors)


def test_portfolio_multiple_without_env_is_critical() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {
        "portfolios": [{"uuid": "aaa"}, {"uuid": "bbb"}]
    }
    errors: list[str] = []
    with patch.dict("os.environ", {}, clear=True):
        import os; os.environ.pop("COINBASE_PORTFOLIO_UUID", None)
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "multiple" in e for e in errors)


# ── Accounts ──────────────────────────────────────────────────────────────────

def test_accounts_parses_usd_balance() -> None:
    inner = MagicMock()
    inner.get_accounts.return_value = _ok_accounts_page()
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert len(summaries) == 1
    assert summaries[0].available_balance == Decimal("500.00")
    assert summaries[0].active and summaries[0].ready
    assert not errors


def test_accounts_string_bool_active_is_critical() -> None:
    page = _ok_accounts_page()
    page["accounts"][0]["active"] = "true"
    inner = MagicMock()
    inner.get_accounts.return_value = page
    errors: list[str] = []
    _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e for e in errors)


def test_accounts_has_next_with_empty_cursor_is_critical() -> None:
    page = _ok_accounts_page(has_next=True, cursor="")
    inner = MagicMock()
    inner.get_accounts.return_value = page
    errors: list[str] = []
    _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "empty cursor" in e for e in errors)


def test_accounts_cursor_cycle_is_critical() -> None:
    page1 = {**_ok_accounts_page(has_next=True, cursor="tok1"),
              "accounts": []}
    page2 = {**_ok_accounts_page(has_next=True, cursor="tok1"),
              "accounts": []}  # same cursor again → cycle
    inner = MagicMock()
    inner.get_accounts.side_effect = [page1, page2]
    errors: list[str] = []
    _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "cursor cycle" in e for e in errors)


def test_accounts_deduplicates_by_uuid() -> None:
    acct = {
        "uuid": "acct-usd-001",
        "currency": "USD",
        "available_balance": {"value": "100"},
        "hold": {"value": "0"},
        "active": True,
        "ready": True,
    }
    page1 = {"accounts": [acct], "has_next": True, "cursor": "tok1"}
    page2 = {"accounts": [acct], "has_next": False, "cursor": ""}
    inner = MagicMock()
    inner.get_accounts.side_effect = [page1, page2]
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert len(summaries) == 1   # deduped


def test_accounts_paginates_until_no_next() -> None:
    inner = MagicMock()
    inner.get_accounts.side_effect = [
        {"accounts": [], "has_next": True, "cursor": "tok1"},
        _ok_accounts_page(),
    ]
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert inner.get_accounts.call_count == 2
    assert len(summaries) == 1


def test_accounts_no_usd_is_critical() -> None:
    inner = MagicMock()
    inner.get_accounts.return_value = {
        "accounts": [{"currency": "BTC", "uuid": "x"}],
        "has_next": False, "cursor": "",
    }
    errors: list[str] = []
    _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "No USD" in e for e in errors)


# ── Product state — granular flags ────────────────────────────────────────────

def test_product_ok() -> None:
    inner = MagicMock()
    inner.get_product.return_value = _ok_product_data()
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert state.entry_supported
    assert state.market_exit_supported
    assert state.cancel_supported
    assert not errors


def test_product_limit_only_blocks_market_exit_but_not_entry() -> None:
    d = {**_ok_product_data(), "limit_only": True}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert state.entry_supported is True        # limit BUY still works
    assert state.market_exit_supported is False # market SELL rejected


def test_product_cancel_only_blocks_entry_and_exit_but_not_cancel() -> None:
    d = {**_ok_product_data(), "cancel_only": True}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert state.entry_supported is False
    assert state.market_exit_supported is False
    assert state.cancel_supported is True   # cancels work under cancel_only


def test_product_view_only_blocks_all_operations() -> None:
    d = {**_ok_product_data(), "view_only": True}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert not state.entry_supported
    assert not state.market_exit_supported
    assert not state.cancel_supported


def test_product_auction_mode_blocks_entry_but_not_cancel() -> None:
    d = {**_ok_product_data(), "auction_mode": True}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert state.entry_supported is False
    assert state.cancel_supported is True


def test_product_string_bool_flag_is_critical() -> None:
    d = {**_ok_product_data(), "is_disabled": "false"}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert any("CRITICAL" in e for e in errors)


def test_product_id_mismatch_is_critical() -> None:
    d = {**_ok_product_data(), "product_id": "ETH-USD"}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert any("CRITICAL" in e and "mismatch" in e for e in errors)


def test_product_min_exceeds_max_is_critical() -> None:
    d = {**_ok_product_data(), "base_min_size": "9001", "base_max_size": "9000"}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert any("CRITICAL" in e and "base_min_size" in e for e in errors)


def test_product_missing_base_max_is_critical() -> None:
    d = {**_ok_product_data()}
    del d["base_max_size"]
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert any("CRITICAL" in e and "base_max_size" in e for e in errors)


# ── Full run_preflight ────────────────────────────────────────────────────────

def test_run_preflight_ok() -> None:
    client = _make_client()
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "OK"
    assert result.entry_allowed()
    assert result.exit_supervision_allowed()


def test_run_preflight_can_trade_false_blocks_entry() -> None:
    inner = _make_inner()
    inner.get_api_key_permissions.return_value = {
        **_ok_permissions_data(), "can_trade": False
    }
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"])
    assert not result.entry_allowed()
    assert result.exit_supervision_allowed()


def test_run_preflight_critical_errors_give_critical_status() -> None:
    inner = _make_inner()
    inner.get_api_key_permissions.side_effect = RuntimeError("timeout")
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "CRITICAL"


def test_run_preflight_key_file_missing_is_critical() -> None:
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_KEY_FILE", _mod.ROOT / "no_such_key.json"):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "CRITICAL"


def test_read_only_facade_has_no_dangerous_methods() -> None:
    client = _make_client()
    for forbidden in ("create_order", "cancel_order", "transfer", "withdraw"):
        assert not hasattr(client, forbidden), (
            f"_ReadOnlyClient must not expose {forbidden!r}"
        )

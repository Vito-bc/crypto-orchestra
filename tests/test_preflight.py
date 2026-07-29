"""
Tests for pipeline/preflight.py — read-only Coinbase preflight (hardened).
"""
from __future__ import annotations

from decimal import Decimal
from unittest.mock import MagicMock, patch

import pytest

import pipeline.preflight as _mod
from pipeline.preflight import (
    KeyPermissions,
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
        run_preflight(["ZEC-USD"], live_reads=True)
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
    _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
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

# ── Per-asset gating ──────────────────────────────────────────────────────────
# overall_status covers account/key/portfolio scope and blocks every asset.
# Product-scope problems must block only the affected asset — previously a
# single bad product set entry_ok=False and halted ENTRY for the whole universe.

def _multi_product_client(per_product: dict) -> _ReadOnlyClient:
    inner = _make_inner()
    inner.get_product.side_effect = lambda product_id, **kw: per_product[product_id]
    return _ReadOnlyClient(inner)


_ENV_UUID = {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}


def _run(client, products):
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", _ENV_UUID):
        return run_preflight(products)


def test_one_bad_product_does_not_block_the_other() -> None:
    bad = {**_ok_product_data("ETH-USD"), "is_disabled": "false"}   # malformed
    client = _multi_product_client({
        "ETH-USD": bad,
        "ZEC-USD": _ok_product_data("ZEC-USD"),
    })
    result = _run(client, ["ETH-USD", "ZEC-USD"])

    assert result.overall_status == "OK", "a product fault is not an account fault"

    eth_ok, eth_why = result.entry_allowed_for("ETH-USD")
    zec_ok, _ = result.entry_allowed_for("ZEC-USD")
    assert eth_ok is False and "ETH-USD" in eth_why
    assert zec_ok is True, "ZEC must still be tradeable when ETH is broken"


def test_get_product_failure_blocks_only_that_asset() -> None:
    def _side_effect(product_id, **kw):
        if product_id == "ETH-USD":
            raise RuntimeError("product not found")
        return _ok_product_data("ZEC-USD")

    inner = _make_inner()
    inner.get_product.side_effect = _side_effect
    result = _run(_ReadOnlyClient(inner), ["ETH-USD", "ZEC-USD"])

    assert result.entry_allowed_for("ETH-USD")[0] is False
    assert result.entry_allowed_for("ZEC-USD")[0] is True


def test_account_level_critical_blocks_every_asset() -> None:
    inner = _make_inner()
    inner.get_api_key_permissions.side_effect = RuntimeError("timeout")
    result = _run(_ReadOnlyClient(inner), ["ETH-USD", "ZEC-USD"])

    assert result.overall_status == "CRITICAL"
    for pid in ("ETH-USD", "ZEC-USD"):
        assert result.entry_allowed_for(pid)[0] is False


def test_unknown_product_fails_closed() -> None:
    client = _multi_product_client({"ZEC-USD": _ok_product_data("ZEC-USD")})
    result = _run(client, ["ZEC-USD"])
    allowed, why = result.entry_allowed_for("DOGE-USD")
    assert allowed is False
    assert "no product state" in why


def test_limit_only_blocks_its_asset_without_touching_global_status() -> None:
    """
    Resolves the old contradiction: entry_supported stayed True for limit_only
    (a limit BUY really does work) while the same flag pushed overall_status to
    ENTRY_BLOCKED for the entire universe.  Now it blocks exactly one asset, and
    it blocks because the market SELL we exit with would be rejected.
    """
    client = _multi_product_client({
        "ETH-USD": {**_ok_product_data("ETH-USD"), "limit_only": True},
        "ZEC-USD": _ok_product_data("ZEC-USD"),
    })
    result = _run(client, ["ETH-USD", "ZEC-USD"])

    assert result.overall_status == "OK"
    eth_ok, eth_why = result.entry_allowed_for("ETH-USD")
    assert eth_ok is False
    assert "market EXIT unsupported" in eth_why
    assert result.entry_allowed_for("ZEC-USD")[0] is True


def test_post_only_blocks_market_exit() -> None:
    """EXIT places market_market_ioc, which a maker-only product rejects."""
    d = {**_ok_product_data(), "post_only": True}
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert state is not None
    assert state.market_exit_supported is False


def test_halted_product_blocks_only_itself() -> None:
    client = _multi_product_client({
        "ETH-USD": {**_ok_product_data("ETH-USD"), "trading_disabled": True},
        "ZEC-USD": _ok_product_data("ZEC-USD"),
    })
    result = _run(client, ["ETH-USD", "ZEC-USD"])
    assert result.entry_allowed_for("ETH-USD")[0] is False
    assert result.entry_allowed_for("ZEC-USD")[0] is True


def test_product_errors_are_reported_per_product() -> None:
    client = _multi_product_client({
        "ETH-USD": {**_ok_product_data("ETH-USD"), "cancel_only": True},
        "ZEC-USD": _ok_product_data("ZEC-USD"),
    })
    result = _run(client, ["ETH-USD", "ZEC-USD"])
    assert "ETH-USD" in result.product_errors
    assert "ZEC-USD" not in result.product_errors


def test_exit_supervision_stays_allowed_when_products_are_blocked() -> None:
    client = _multi_product_client({
        "ZEC-USD": {**_ok_product_data("ZEC-USD"), "view_only": True},
    })
    result = _run(client, ["ZEC-USD"])
    assert result.entry_allowed_for("ZEC-USD")[0] is False
    assert result.exit_supervision_allowed() is True


# ── Mandatory portfolio UUID (LIVE config gate) ───────────────────────────────

def test_missing_portfolio_uuid_env_blocks_entry_live() -> None:
    """
    .env has no COINBASE_PORTFOLIO_UUID, so DRY_RUN=false would block ENTRY on
    every cycle. Lock the behaviour so it cannot silently regress to fail-open.
    """
    import os
    client = _make_client()
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {}, clear=True):
        os.environ.pop("COINBASE_PORTFOLIO_UUID", None)
        result = run_preflight(["ZEC-USD"])

    assert result.overall_status != "OK"
    assert not result.entry_allowed()
    assert any("COINBASE_PORTFOLIO_UUID" in e for e in result.errors)


def test_non_dict_product_payload_does_not_crash_preflight() -> None:
    """
    A non-dict Get Product response for one asset must not take down the whole
    preflight run.  Before the isolation guard, data.get(...) raised
    AttributeError out of _check_product and aborted every asset's checks.
    """
    def _side_effect(product_id, **kw):
        if product_id == "ETH-USD":
            return ["unexpected", "list", "payload"]
        return _ok_product_data("ZEC-USD")

    inner = _make_inner()
    inner.get_product.side_effect = _side_effect
    result = _run(_ReadOnlyClient(inner), ["ETH-USD", "ZEC-USD"])

    eth_ok, eth_why = result.entry_allowed_for("ETH-USD")
    assert eth_ok is False and "ETH-USD" in eth_why
    assert result.entry_allowed_for("ZEC-USD")[0] is True


def test_none_product_payload_blocks_only_that_asset() -> None:
    def _side_effect(product_id, **kw):
        return None if product_id == "ETH-USD" else _ok_product_data("ZEC-USD")

    inner = _make_inner()
    inner.get_product.side_effect = _side_effect
    result = _run(_ReadOnlyClient(inner), ["ETH-USD", "ZEC-USD"])

    assert result.entry_allowed_for("ETH-USD")[0] is False
    assert result.entry_allowed_for("ZEC-USD")[0] is True


def test_missing_product_id_blocks_that_asset() -> None:
    d = _ok_product_data("ZEC-USD")
    del d["product_id"]
    inner = MagicMock()
    inner.get_product.return_value = d
    errors: list[str] = []
    _check_product(_ReadOnlyClient(inner), "ZEC-USD", errors)
    assert any("no product_id" in e and "CRITICAL" in e for e in errors)


# ── Malformed global payload shape-validation (must not crash preflight) ──────

@pytest.mark.parametrize("bad", [None, ["a", "list"], "a string", 42])
def test_key_permissions_non_dict_is_critical_not_crash(bad) -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = bad
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is None
    assert any("CRITICAL" in e for e in errors)


def test_run_preflight_key_permissions_none_returns_critical_result() -> None:
    """A None key-permissions payload must yield a structured CRITICAL result,
    not an AttributeError that aborts run_all_assets with no status."""
    inner = _make_inner()
    inner.get_api_key_permissions.return_value = None
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "CRITICAL"
    assert not result.entry_allowed()
    assert result.exit_supervision_allowed() is True


def test_portfolios_non_dict_is_critical_not_crash() -> None:
    kp = KeyPermissions(can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="")
    inner = MagicMock()
    inner.get_portfolios.return_value = None
    errors: list[str] = []
    _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e for e in errors)


def test_portfolios_field_not_a_list_is_critical() -> None:
    kp = KeyPermissions(can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="")
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": {"unexpected": "dict"}}
    errors: list[str] = []
    _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "expected a list" in e for e in errors)


def test_accounts_non_dict_page_is_critical_not_crash() -> None:
    inner = MagicMock()
    inner.get_accounts.return_value = ["not", "a", "dict"]
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert summaries == []
    assert any("CRITICAL" in e for e in errors)


# ── portfolio_uuid strict typing + identity confirmation ─────────────────────

def test_key_permissions_non_string_uuid_is_critical() -> None:
    inner = MagicMock()
    inner.get_api_key_permissions.return_value = {**_ok_permissions_data(), "portfolio_uuid": 42}
    errors: list[str] = []
    kp = _check_key_permissions(_ReadOnlyClient(inner), errors)
    assert kp is None
    assert any("CRITICAL" in e and "portfolio_uuid" in e for e in errors)


def test_run_preflight_non_string_uuid_does_not_crash() -> None:
    """An int portfolio_uuid previously blew up KeyPermissions.__repr__; now CRITICAL."""
    inner = _make_inner()
    inner.get_api_key_permissions.return_value = {**_ok_permissions_data(), "portfolio_uuid": 42}
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "CRITICAL"


def test_missing_key_uuid_is_critical_even_with_empty_portfolios() -> None:
    """
    COINBASE_PORTFOLIO_UUID set, key advertises no portfolio_uuid → CRITICAL.
    Orders derive their portfolio from the key, so an unbound key cannot be proven
    to trade the intended portfolio.
    """
    kp = KeyPermissions(can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="")
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": []}
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "no portfolio_uuid" in e for e in errors)


def test_expected_uuid_not_proven_by_portfolio_list_alone() -> None:
    """
    SECURITY: List Portfolios proves the account CONTAINS the portfolio, not that
    THIS trading key is bound to it.  A missing key portfolio_uuid must be CRITICAL
    even when the expected uuid appears in List Portfolios — orders would still go
    to the key's own (unproven) portfolio.
    """
    kp = KeyPermissions(can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="")
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": [{"uuid": "pinned-uuid-xyz"}]}
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "no portfolio_uuid" in e for e in errors), (
        "List Portfolios visibility must NOT substitute for key-scope proof"
    )
    assert any("does not prove key scope" in e for e in errors)


def test_key_uuid_match_is_the_only_confirmation() -> None:
    """The single OK path: the key's own portfolio_uuid equals the expected env."""
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="pinned-uuid-xyz",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": [{"uuid": "pinned-uuid-xyz"}]}
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        resolved = _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert resolved == "pinned-uuid-xyz"
    assert not errors


def test_key_uuid_match_ok_even_if_not_in_portfolio_list() -> None:
    """Key-scope match is authoritative; List Portfolios is only diagnostic."""
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="pinned-uuid-xyz",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = {"portfolios": [{"uuid": "some-other-uuid"}]}
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        resolved = _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert resolved == "pinned-uuid-xyz"
    assert not errors


# ── Malformed account scalars must not crash ─────────────────────────────────

def test_accounts_non_string_currency_is_critical_not_crash() -> None:
    page = _ok_accounts_page()
    page["accounts"][0]["currency"] = 42
    inner = MagicMock()
    inner.get_accounts.return_value = page
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "currency" in e for e in errors)
    assert all(s.currency == "USD" for s in summaries)  # the bad one was skipped


def test_accounts_non_dict_element_is_critical_not_crash() -> None:
    inner = MagicMock()
    inner.get_accounts.return_value = {
        "accounts": ["not-a-dict", {"currency": "USD", "uuid": "u1",
                                     "available_balance": {"value": "100"},
                                     "hold": {"value": "0"}, "active": True, "ready": True}],
        "has_next": False, "cursor": "",
    }
    errors: list[str] = []
    summaries = _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "account entry" in e for e in errors)
    assert len(summaries) == 1  # the valid USD account still parsed


def test_accounts_field_not_a_list_is_critical() -> None:
    inner = MagicMock()
    inner.get_accounts.return_value = {"accounts": {"bad": "dict"}, "has_next": False, "cursor": ""}
    errors: list[str] = []
    _check_accounts(_ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "expected a list" in e for e in errors)


# ── List Portfolios is diagnostic: must not block a scope-proven key ──────────

def test_portfolios_failure_does_not_block_when_key_uuid_matches() -> None:
    """
    A transient List Portfolios failure must NOT block ENTRY once the key's own
    portfolio_uuid already proves scope.  It is diagnostic, not a gate.
    """
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="pinned-uuid-xyz",
    )
    inner = MagicMock()
    inner.get_portfolios.side_effect = RuntimeError("temporary 503")
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        resolved = _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert resolved == "pinned-uuid-xyz"
    assert errors == [], "diagnostic endpoint failure must not gate a proven key"


def test_portfolios_malformed_does_not_block_when_key_uuid_matches() -> None:
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="pinned-uuid-xyz",
    )
    inner = MagicMock()
    inner.get_portfolios.return_value = None   # malformed
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        resolved = _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert resolved == "pinned-uuid-xyz"
    assert errors == []


def test_run_preflight_ok_despite_portfolios_failure_when_key_matches() -> None:
    """End-to-end: overall_status stays OK when the key proves scope and only the
    diagnostic List Portfolios endpoint is down."""
    inner = _make_inner()
    # key portfolio_uuid already equals the env uuid used by _make_inner/_ok_*
    inner.get_portfolios.side_effect = RuntimeError("temporary 503")
    client = _ReadOnlyClient(inner)
    with patch.object(_mod, "_DRY_RUN", False), \
         patch.object(_mod, "_build_read_only_client", return_value=client), \
         patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "abc12345-1111-2222-3333-444455556666"}):
        result = run_preflight(["ZEC-USD"])
    assert result.overall_status == "OK"
    assert result.entry_allowed()


def test_portfolios_failure_still_blocks_when_uuid_unset() -> None:
    """When no UUID is pinned the list IS load-bearing, so its failure blocks."""
    kp = KeyPermissions(can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="")
    inner = MagicMock()
    inner.get_portfolios.side_effect = RuntimeError("temporary 503")
    errors: list[str] = []
    with patch.dict("os.environ", {}, clear=True):
        import os
        os.environ.pop("COINBASE_PORTFOLIO_UUID", None)
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    assert any("CRITICAL" in e and "get_portfolios" in e for e in errors)


def test_nonblocking_portfolio_diagnostic_not_logged_as_critical(capsys) -> None:
    """
    When the key match is confirmed, a List Portfolios failure is logged as a
    non-blocking WARNING — its stdout line must NOT contain the word CRITICAL,
    so external monitoring that greps for CRITICAL does not false-alert.
    """
    kp = KeyPermissions(
        can_view=True, can_trade=True, can_transfer=False, portfolio_uuid="pinned-uuid-xyz",
    )
    inner = MagicMock()
    inner.get_portfolios.side_effect = RuntimeError("temporary 503")
    errors: list[str] = []
    with patch.dict("os.environ", {"COINBASE_PORTFOLIO_UUID": "pinned-uuid-xyz"}):
        _check_portfolio_uuid(kp, _ReadOnlyClient(inner), errors)
    out = capsys.readouterr().out
    assert "temporary 503" in out, "the diagnostic must still be visible"
    assert "CRITICAL" not in out, "non-blocking diagnostic must not print CRITICAL"
    assert errors == []

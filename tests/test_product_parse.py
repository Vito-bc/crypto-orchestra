"""
Tests for pipeline/product_parse.py — the single strict Get Product parser.

The point of this module is that preflight and product_state can never disagree
about what a Coinbase payload means.  The parity tests below are the ones that
would have caught the original defect: preflight validated flags strictly while
product_state used bool(d.get(name, False)), so a payload with missing
tradability fields was CRITICAL to one and perfectly fine to the other.
"""
from __future__ import annotations

from decimal import Decimal

import pytest

from pipeline.product_parse import (
    ALL_NUMERIC,
    CORE_NUMERIC,
    TRADING_FLAGS,
    parse_product_payload,
    safe_decimal,
    strict_bool,
    strict_positive_decimal,
)


def _ok_payload(product_id: str = "ZEC-USD", **overrides) -> dict:
    data = {
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
    data.update(overrides)
    return data


# ── strict_bool ───────────────────────────────────────────────────────────────

def test_strict_bool_accepts_native_bools() -> None:
    errs: list[str] = []
    assert strict_bool(True, "f", errs) is True
    assert strict_bool(False, "f", errs) is False
    assert not errs


@pytest.mark.parametrize("bad", ["false", "true", "", 0, 1, None, [], {}])
def test_strict_bool_rejects_everything_else(bad) -> None:
    errs: list[str] = []
    assert strict_bool(bad, "is_disabled", errs) is None
    assert any("CRITICAL" in e for e in errs)


# ── strict_positive_decimal ───────────────────────────────────────────────────

@pytest.mark.parametrize("bad", [None, "0", "-1", "NaN", "Infinity", "abc"])
def test_strict_positive_decimal_rejects_invalid(bad) -> None:
    errs: list[str] = []
    assert strict_positive_decimal(bad, "base_increment", errs) is None
    assert any("CRITICAL" in e for e in errs)


def test_strict_positive_decimal_accepts_valid() -> None:
    errs: list[str] = []
    assert strict_positive_decimal("0.001", "base_min_size", errs) == Decimal("0.001")
    assert not errs


def test_safe_decimal_allows_zero_but_not_negative() -> None:
    errs: list[str] = []
    assert safe_decimal("0", "hold", errs) == Decimal("0")
    assert not errs
    assert safe_decimal("-1", "hold", errs) is None
    assert errs


# ── parse_product_payload — happy path ────────────────────────────────────────

def test_parse_ok_payload_has_no_errors() -> None:
    parsed = parse_product_payload(_ok_payload(), "ZEC-USD")
    assert parsed.ok
    assert parsed.errors == []
    assert parsed.decimals["base_min_size"] == Decimal("0.001")
    assert all(parsed.flag(name) is False for name in TRADING_FLAGS)


def test_parse_reports_all_seven_flags() -> None:
    parsed = parse_product_payload(_ok_payload(), "ZEC-USD")
    assert set(parsed.flags) == set(TRADING_FLAGS)


# ── The P0-2 defect: missing flags must not become a permissive False ─────────

@pytest.mark.parametrize("flag", TRADING_FLAGS)
def test_missing_flag_is_error_and_defaults_to_blocking(flag: str) -> None:
    """
    A flag absent from the response is a malformed payload, not a False.

    Before the shared parser, product_state did bool(d.get(flag, False)), so a
    response missing every tradability field looked completely clean and ENTRY
    proceeded against a product whose state had never been read.
    """
    payload = _ok_payload()
    del payload[flag]
    parsed = parse_product_payload(payload, "ZEC-USD")
    assert not parsed.ok
    assert any(flag in e and "CRITICAL" in e for e in parsed.errors)
    assert parsed.flag(flag) is True, "unparsed flag must fail closed, not open"


def test_payload_missing_all_flags_fails_closed_on_every_flag() -> None:
    payload = {k: v for k, v in _ok_payload().items() if k not in TRADING_FLAGS}
    parsed = parse_product_payload(payload, "ZEC-USD")
    assert not parsed.ok
    assert all(parsed.flag(name) is True for name in TRADING_FLAGS)


@pytest.mark.parametrize("bad", ["false", "true", 0, 1])
def test_non_bool_flag_is_error_and_defaults_to_blocking(bad) -> None:
    parsed = parse_product_payload(_ok_payload(is_disabled=bad), "ZEC-USD")
    assert not parsed.ok
    assert parsed.flag("is_disabled") is True


# ── Numeric validation and relations ──────────────────────────────────────────

def test_product_id_echo_mismatch_is_critical() -> None:
    parsed = parse_product_payload(_ok_payload(product_id="ETH-USD"), "ZEC-USD")
    assert any("mismatch" in e and "CRITICAL" in e for e in parsed.errors)


def test_base_min_greater_than_max_is_critical() -> None:
    parsed = parse_product_payload(
        _ok_payload(base_min_size="9001", base_max_size="9000"), "ZEC-USD"
    )
    assert any("base_min_size" in e and "CRITICAL" in e for e in parsed.errors)


def test_quote_min_greater_than_max_is_critical() -> None:
    parsed = parse_product_payload(
        _ok_payload(quote_min_size="10", quote_max_size="1"), "ZEC-USD"
    )
    assert any("quote_min_size" in e and "CRITICAL" in e for e in parsed.errors)


def test_required_numeric_subset_ignores_absent_optional_fields() -> None:
    """product_state requires only the four fields it uses."""
    payload = _ok_payload()
    del payload["quote_min_size"]
    del payload["quote_max_size"]

    strict = parse_product_payload(payload, "ZEC-USD", required_numeric=ALL_NUMERIC)
    assert not strict.ok        # preflight requires all six

    lenient = parse_product_payload(payload, "ZEC-USD", required_numeric=CORE_NUMERIC)
    assert lenient.ok           # product_state does not


def test_non_dict_payload_fails_closed() -> None:
    parsed = parse_product_payload("not a dict", "ZEC-USD")  # type: ignore[arg-type]
    assert not parsed.ok
    assert all(parsed.flag(name) is True for name in TRADING_FLAGS)


# ── Parity: preflight and product_state read one payload identically ──────────

def _preflight_flags(payload: dict, product_id: str = "ZEC-USD"):
    from pipeline.preflight import _ReadOnlyClient, _check_product
    from unittest.mock import MagicMock

    inner = MagicMock()
    inner.get_product.return_value = payload
    errors: list[str] = []
    state = _check_product(_ReadOnlyClient(inner), product_id, errors)
    assert state is not None
    return {name: getattr(state, name) for name in TRADING_FLAGS}, errors


def _product_state_flags(payload: dict, product_id: str = "ZEC-USD"):
    from unittest.mock import MagicMock, patch
    import pipeline.product_state as _ps

    client = MagicMock()
    client.get_product.return_value = payload
    with patch("exchange.coinbase_client._get_client", return_value=client):
        try:
            _rules, state = _ps._fetch_from_coinbase(product_id)
        except RuntimeError as exc:
            return None, [str(exc)]
    return {name: getattr(state, name) for name in TRADING_FLAGS}, []


def test_parity_clean_payload_agrees() -> None:
    payload = _ok_payload()
    pf_flags, pf_errors = _preflight_flags(payload)
    ps_flags, ps_errors = _product_state_flags(payload)
    assert not pf_errors and not ps_errors
    assert pf_flags == ps_flags


@pytest.mark.parametrize("flag", TRADING_FLAGS)
def test_parity_missing_flag_rejected_by_both(flag: str) -> None:
    """
    The exact divergence that made LIVE unsafe: preflight said CRITICAL,
    product_state said "all clear".  Both must now reject.
    """
    payload = _ok_payload()
    del payload[flag]

    _pf_flags, pf_errors = _preflight_flags(payload)
    assert any("CRITICAL" in e for e in pf_errors)

    ps_flags, ps_errors = _product_state_flags(payload)
    assert ps_flags is None, "product_state must refuse a payload preflight calls CRITICAL"
    assert ps_errors


@pytest.mark.parametrize("bad", ["false", 0])
def test_parity_non_bool_flag_rejected_by_both(bad) -> None:
    payload = _ok_payload(cancel_only=bad)

    _pf_flags, pf_errors = _preflight_flags(payload)
    assert any("CRITICAL" in e for e in pf_errors)

    ps_flags, ps_errors = _product_state_flags(payload)
    assert ps_flags is None
    assert ps_errors


def test_parity_invalid_numeric_rejected_by_both() -> None:
    payload = _ok_payload(base_increment="0")

    _pf_flags, pf_errors = _preflight_flags(payload)
    assert any("CRITICAL" in e for e in pf_errors)

    ps_flags, ps_errors = _product_state_flags(payload)
    assert ps_flags is None
    assert ps_errors


def test_missing_product_id_is_critical() -> None:
    """An absent product_id echo is as much a red flag as a wrong one."""
    payload = _ok_payload()
    del payload["product_id"]
    parsed = parse_product_payload(payload, "ZEC-USD")
    assert not parsed.ok
    assert any("no product_id" in e and "CRITICAL" in e for e in parsed.errors)


def test_empty_product_id_is_critical() -> None:
    parsed = parse_product_payload(_ok_payload(product_id=""), "ZEC-USD")
    assert not parsed.ok
    assert any("no product_id" in e for e in parsed.errors)


def test_parity_missing_product_id_rejected_by_both() -> None:
    payload = _ok_payload()
    del payload["product_id"]

    _pf_flags, pf_errors = _preflight_flags(payload)
    assert any("CRITICAL" in e for e in pf_errors)

    ps_flags, ps_errors = _product_state_flags(payload)
    assert ps_flags is None
    assert ps_errors

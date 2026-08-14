"""
Direct tests of the REAL get_okx_funding_rate parser.

Every funding test in test_entry_filters.py mocks the finished result, so it
verifies how the CALLER treats a status and proves nothing about how that status
is produced. The parser is where "malformed body reads as funding 0.0" lived, so
it needs its own coverage with only the HTTP boundary (`_get`) replaced.
"""
from __future__ import annotations

from unittest.mock import patch

import pytest

import tools.market_positioning as mp
from tools.market_positioning import (
    FUNDING_NOT_APPLICABLE,
    FUNDING_OK,
    FUNDING_UNAVAILABLE,
    funding_applicability,
    get_okx_funding_rate,
)


def _call(asset: str = "BTC-USD", *, body):
    with patch.object(mp, "_get", return_value=body) as g:
        return get_okx_funding_rate(asset), g


def _raises(asset: str = "BTC-USD", *, exc):
    with patch.object(mp, "_get", side_effect=exc):
        return get_okx_funding_rate(asset)


# ── Applicability comes from the registry, in three states ───────────────────

def test_registered_with_a_symbol_is_applicable() -> None:
    assert funding_applicability("BTC-USD") == FUNDING_OK
    assert funding_applicability("btc-usd") == FUNDING_OK, "must be case-insensitive"


def test_registered_as_none_is_not_applicable() -> None:
    """ZEC is declared to have no perpetual. That is knowledge, not absence."""
    assert funding_applicability("ZEC-USD") == FUNDING_NOT_APPLICABLE
    res = get_okx_funding_rate("ZEC-USD")
    assert res["status"] == FUNDING_NOT_APPLICABLE


def test_unregistered_asset_is_unavailable_not_not_applicable() -> None:
    """
    Nobody has classified this instrument, so its applicability is unknown and
    must fail closed. Treating it as "no perpetual" means enabling a new asset
    silently drops the funding gate — and LINK/ATOM/AVAX/DOT already sit in
    ASSET_CONFIG as disabled candidates.
    """
    for asset in ("LINK-USD", "ATOM-USD", "AVAX-USD", "DOT-USD", "BTX-USD"):
        assert asset not in mp._OKX_SYMBOL, f"{asset} is registered; pick another"
        assert funding_applicability(asset) == FUNDING_UNAVAILABLE
        res = get_okx_funding_rate(asset)
        assert res["status"] == FUNDING_UNAVAILABLE, asset
        assert "registry" in res["error"]


def test_not_applicable_never_makes_a_request() -> None:
    with patch.object(mp, "_get") as g:
        get_okx_funding_rate("ZEC-USD")
        get_okx_funding_rate("LINK-USD")
    assert not g.called, "applicability must be decided without a network call"


# ── Happy path ───────────────────────────────────────────────────────────────

def test_valid_payload_is_parsed_and_annualised() -> None:
    res, _ = _call(body={"data": [{"fundingRate": "0.0001"}]})
    assert res["status"] == FUNDING_OK
    assert res["rate_pct"] == pytest.approx(0.01)
    # 3 settlements/day x 365 days
    assert res["annualized_pct"] == pytest.approx(10.95)
    assert res["source"] == "OKX"
    assert res["error"] is None


@pytest.mark.parametrize("rate,signal", [
    ("0.0010", "SELL"),    # ~109.5% annualised — leverage chase
    ("0.0001", "NEUTRAL"),
    ("-0.0010", "BUY"),    # capitulation shorts
])
def test_signal_thresholds_are_unchanged(rate, signal) -> None:
    """Thresholds are research parameters; this phase must not move them."""
    res, _ = _call(body={"data": [{"fundingRate": rate}]})
    assert res["signal"] == signal


# ── Everything unreadable is UNAVAILABLE, never a silent zero ────────────────

def test_none_body_is_unavailable() -> None:
    """_get returns None on timeout, HTTP error and unparseable body alike."""
    res, _ = _call(body=None)
    assert res["status"] == FUNDING_UNAVAILABLE
    assert res["annualized_pct"] == 0.0   # present, but the status forbids use


def test_raised_exception_is_unavailable() -> None:
    res = _raises(exc=TimeoutError("read timed out"))
    assert res["status"] == FUNDING_UNAVAILABLE
    assert "TimeoutError" in res["error"] or "timed out" in res["error"]


def test_non_object_body_is_unavailable() -> None:
    res, _ = _call(body=["not", "an", "object"])
    assert res["status"] == FUNDING_UNAVAILABLE


def test_empty_data_is_unavailable() -> None:
    """
    THE regression. `data["data"]` empty used to leave annualized_pct at 0.0
    with error None, so garbage read as "funding is zero, all clear" — a
    fail-open that looked like a real measurement.
    """
    for body in ({"data": []}, {"data": None}, {}):
        res, _ = _call(body=body)
        assert res["status"] == FUNDING_UNAVAILABLE, body


def test_missing_funding_rate_key_is_unavailable() -> None:
    res, _ = _call(body={"data": [{"instId": "BTC-USDT-SWAP"}]})
    assert res["status"] == FUNDING_UNAVAILABLE
    assert "malformed" in res["error"]


def test_non_numeric_funding_rate_is_unavailable() -> None:
    res, _ = _call(body={"data": [{"fundingRate": "not-a-number"}]})
    assert res["status"] == FUNDING_UNAVAILABLE


def test_null_funding_rate_is_unavailable() -> None:
    res, _ = _call(body={"data": [{"fundingRate": None}]})
    assert res["status"] == FUNDING_UNAVAILABLE


def test_wrong_shaped_row_is_unavailable() -> None:
    res, _ = _call(body={"data": ["a string, not a row"]})
    assert res["status"] == FUNDING_UNAVAILABLE


@pytest.mark.parametrize("rate", ["NaN", "Infinity", "-Infinity"])
def test_non_finite_funding_rate_is_unavailable(rate) -> None:
    """float() accepts these strings; arithmetic on them poisons the threshold."""
    res, _ = _call(body={"data": [{"fundingRate": rate}]})
    assert res["status"] == FUNDING_UNAVAILABLE
    assert "finite" in res["error"]


def test_status_is_always_present() -> None:
    """Callers branch on `status`; a path that omits it would KeyError live."""
    bodies = [None, {}, {"data": []}, {"data": [{"fundingRate": "0.0001"}]}]
    for body in bodies:
        res, _ = _call(body=body)
        assert res["status"] in {FUNDING_OK, FUNDING_NOT_APPLICABLE,
                                 FUNDING_UNAVAILABLE}, body
    assert get_okx_funding_rate("ZEC-USD")["status"] == FUNDING_NOT_APPLICABLE


def test_error_is_not_a_control_field() -> None:
    """
    Both not_applicable and unavailable carry an `error` string. That is exactly
    why the caller must branch on `status`: `if not funding.get("error")` treated
    an OKX outage the same as ZEC having no perpetual.
    """
    assert get_okx_funding_rate("ZEC-USD")["error"]
    res, _ = _call(body=None)
    assert res["error"]
    assert get_okx_funding_rate("ZEC-USD")["status"] != res["status"]

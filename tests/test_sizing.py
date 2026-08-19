"""Tests for the single live/paper sizing baseline."""

from __future__ import annotations

import pytest

from pipeline.sizing import (
    DEFAULT_LIVE_BALANCE_USD,
    DEFAULT_TRADE_SIZE_PCT,
    live_balance_usd,
    trade_size_pct,
)


@pytest.mark.parametrize("configured", [None, "", "   "])
def test_live_balance_uses_default_when_unset_or_blank(monkeypatch, configured):
    if configured is None:
        monkeypatch.delenv("LIVE_BALANCE_USD", raising=False)
    else:
        monkeypatch.setenv("LIVE_BALANCE_USD", configured)

    assert live_balance_usd() == DEFAULT_LIVE_BALANCE_USD


@pytest.mark.parametrize(
    ("configured", "expected"),
    [("250", 250.0), ("250.75", 250.75), (" 125.50 ", 125.5)],
)
def test_live_balance_accepts_positive_finite_numbers(
    monkeypatch, configured, expected
):
    monkeypatch.setenv("LIVE_BALANCE_USD", configured)

    assert live_balance_usd() == expected


@pytest.mark.parametrize(
    "configured", ["not-a-number", "0", "-1", "nan", "inf", "-inf"]
)
def test_live_balance_rejects_unsafe_values(monkeypatch, configured):
    monkeypatch.setenv("LIVE_BALANCE_USD", configured)

    with pytest.raises(
        ValueError, match="LIVE_BALANCE_USD must be a positive finite number"
    ):
        live_balance_usd()


@pytest.mark.parametrize("configured", [None, "", "   "])
def test_trade_size_uses_default_when_unset_or_blank(monkeypatch, configured):
    if configured is None:
        monkeypatch.delenv("TRADE_SIZE_PCT", raising=False)
    else:
        monkeypatch.setenv("TRADE_SIZE_PCT", configured)

    assert trade_size_pct() == DEFAULT_TRADE_SIZE_PCT


@pytest.mark.parametrize(
    ("configured", "expected"), [("0.01", 0.01), ("0.12", 0.12), (" .025 ", 0.025)]
)
def test_trade_size_accepts_values_within_hard_ceiling(
    monkeypatch, configured, expected
):
    monkeypatch.setenv("TRADE_SIZE_PCT", configured)

    assert trade_size_pct() == expected


@pytest.mark.parametrize(
    "configured", ["not-a-number", "0", "-0.01", "0.1201", "nan", "inf"]
)
def test_trade_size_rejects_unsafe_values(monkeypatch, configured):
    monkeypatch.setenv("TRADE_SIZE_PCT", configured)

    with pytest.raises(ValueError, match="TRADE_SIZE_PCT must be"):
        trade_size_pct()

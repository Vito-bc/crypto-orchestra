"""
Tests for pipeline/product_state.py — ProductRules/ProductState cache with LKG.
"""
from __future__ import annotations

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.product_state as _mod
from pipeline.product_state import (
    ProductRules,
    ProductState,
    _clear_cache,
    _inject_cache,
    _rules_from_lkg,
    _save_lkg,
    get_rules,
    get_rules_for_exit,
    get_state,
    is_entry_allowed,
    prewarm,
)


# ── Fixtures ──────────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clean_cache(tmp_path: Path, monkeypatch):
    """Each test gets a clean in-process cache and an isolated LKG file."""
    _clear_cache()
    monkeypatch.setattr(_mod, "LKG_PATH", tmp_path / "product_lkg.json")
    yield
    _clear_cache()


def _make_rules(pid="ZEC-USD", inc="0.00000001", min_size="0.001") -> ProductRules:
    return ProductRules(
        product_id=pid, base_increment=inc, base_min_size=min_size,
        base_max_size="9000", quote_increment="0.01",
        fetched_wall=time.time(),
    )


def _make_state(pid="ZEC-USD", fresh=True, **flags) -> ProductState:
    defaults = dict(
        is_disabled=False, trading_disabled=False, cancel_only=False,
        limit_only=False, post_only=False, auction_mode=False, view_only=False,
    )
    defaults.update(flags)
    mono = time.monotonic() if fresh else 0.0   # 0.0 → always stale
    return ProductState(
        product_id=pid,
        fetched_wall=time.time(),
        fetched_mono=mono,
        **defaults,
    )


# ── Cache TTL ─────────────────────────────────────────────────────────────────

def test_get_state_returns_none_when_stale() -> None:
    stale = _make_state(fresh=False)
    _inject_cache("ZEC-USD", state=stale)
    assert get_state("ZEC-USD") is None


def test_get_state_returns_value_when_fresh() -> None:
    fresh = _make_state(fresh=True)
    _inject_cache("ZEC-USD", state=fresh)
    assert get_state("ZEC-USD") is fresh


def test_get_rules_returns_stale_cache_over_lkg(tmp_path: Path) -> None:
    """Stale rules cache is still preferred — numeric rules don't drift."""
    rules = _make_rules()
    rules.fetched_mono = 0.0   # stale
    _inject_cache("ZEC-USD", rules=rules)
    # Confirm no LKG on disk
    assert not _mod.LKG_PATH.exists()
    assert get_rules("ZEC-USD") is rules


# ── LKG persistence ───────────────────────────────────────────────────────────

def test_save_and_load_lkg_round_trips() -> None:
    rules = _make_rules()
    state = _make_state()
    _save_lkg(rules, state)
    loaded = _rules_from_lkg("ZEC-USD")
    assert loaded is not None
    assert loaded.base_increment == rules.base_increment
    assert loaded.base_min_size  == rules.base_min_size
    assert loaded.product_id     == "ZEC-USD"


def test_lkg_survives_multiple_products() -> None:
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    _save_lkg(_make_rules("ETH-USD", inc="0.000001", min_size="0.0001"), _make_state("ETH-USD"))
    z = _rules_from_lkg("ZEC-USD")
    e = _rules_from_lkg("ETH-USD")
    assert z is not None and z.product_id == "ZEC-USD"
    assert e is not None and e.base_increment == "0.000001"


def test_get_rules_falls_back_to_lkg_when_cache_empty() -> None:
    rules = _make_rules()
    state = _make_state()
    _save_lkg(rules, state)
    # Cache is empty (_clear_cache was called by fixture)
    loaded = get_rules("ZEC-USD")
    assert loaded is not None
    assert loaded.base_increment == "0.00000001"


def test_rules_from_lkg_returns_none_when_file_missing() -> None:
    assert _rules_from_lkg("ZEC-USD") is None


def test_rules_from_lkg_returns_none_for_unknown_product() -> None:
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    assert _rules_from_lkg("UNKNOWN-USD") is None


# ── Entry gate ────────────────────────────────────────────────────────────────

def test_is_entry_allowed_passes_when_all_clear() -> None:
    _inject_cache("ZEC-USD", state=_make_state())
    allowed, reason = is_entry_allowed("ZEC-USD")
    assert allowed is True
    assert reason == ""


def test_is_entry_blocked_when_state_missing() -> None:
    allowed, reason = is_entry_allowed("ZEC-USD")
    assert allowed is False
    assert "unavailable or stale" in reason


def test_is_entry_blocked_when_state_stale() -> None:
    _inject_cache("ZEC-USD", state=_make_state(fresh=False))
    allowed, reason = is_entry_allowed("ZEC-USD")
    assert allowed is False


@pytest.mark.parametrize("flag", [
    "is_disabled", "trading_disabled", "cancel_only", "view_only", "limit_only",
])
def test_is_entry_blocked_by_each_hard_block_flag(flag: str) -> None:
    _inject_cache("ZEC-USD", state=_make_state(**{flag: True}))
    allowed, reason = is_entry_allowed("ZEC-USD")
    assert allowed is False
    assert flag in reason


# ── Exit rules ────────────────────────────────────────────────────────────────

def test_get_rules_for_exit_returns_cached_rules() -> None:
    _inject_cache("ZEC-USD", rules=_make_rules(inc="0.00000001", min_size="0.001"))
    result = get_rules_for_exit("ZEC-USD")
    assert result["base_increment"] == "0.00000001"
    assert result["base_min_size"]  == "0.001"


def test_get_rules_for_exit_returns_defaults_when_unavailable() -> None:
    result = get_rules_for_exit("ZEC-USD")
    assert result["base_increment"] == "0.00000001"
    assert result["base_min_size"]  == "0.00000001"


def test_get_rules_for_exit_never_raises_on_exception(monkeypatch) -> None:
    def _boom():
        raise RuntimeError("disk full")
    monkeypatch.setattr(_mod, "_load_lkg", _boom)
    # Even with a broken LKG loader, returns defaults
    result = get_rules_for_exit("ZEC-USD")
    assert "base_increment" in result


# ── Prewarm ───────────────────────────────────────────────────────────────────

def test_prewarm_ok_populates_cache_and_lkg(tmp_path: Path) -> None:
    def _fake_fetch(pid):
        return _make_rules(pid), _make_state(pid)

    with patch.object(_mod, "_fetch_from_coinbase", side_effect=_fake_fetch):
        results = prewarm(["ZEC-USD", "ETH-USD"])

    assert results == {"ZEC-USD": True, "ETH-USD": True}
    assert get_state("ZEC-USD") is not None
    assert get_state("ETH-USD") is not None
    assert _mod.LKG_PATH.exists()


def test_prewarm_failure_returns_false_and_leaves_others_ok() -> None:
    def _fake_fetch(pid):
        if pid == "ZEC-USD":
            raise RuntimeError("network timeout")
        return _make_rules(pid), _make_state(pid)

    with patch.object(_mod, "_fetch_from_coinbase", side_effect=_fake_fetch):
        results = prewarm(["ZEC-USD", "ETH-USD"])

    assert results["ZEC-USD"] is False
    assert results["ETH-USD"] is True
    assert get_state("ETH-USD") is not None
    assert get_state("ZEC-USD") is None


def test_prewarm_writes_lkg_that_survives_cache_clear(tmp_path: Path) -> None:
    def _fake_fetch(pid):
        return _make_rules(pid, inc="0.0001"), _make_state(pid)

    with patch.object(_mod, "_fetch_from_coinbase", side_effect=_fake_fetch):
        prewarm(["ZEC-USD"])

    _clear_cache()
    loaded = get_rules("ZEC-USD")   # should fall back to LKG
    assert loaded is not None
    assert loaded.base_increment == "0.0001"


# ── Product flags ─────────────────────────────────────────────────────────────

def test_state_hard_blocked_true_when_any_hard_flag_set() -> None:
    s = _make_state(is_disabled=True)
    assert s.hard_blocked is True
    assert s.entry_allowed is False


def test_state_entry_allowed_false_for_limit_only() -> None:
    s = _make_state(limit_only=True)
    assert s.hard_blocked is False   # not a hard flag per se
    assert s.entry_allowed is False  # but still blocks market ENTRY


def test_state_entry_allowed_true_when_all_clear() -> None:
    s = _make_state()
    assert s.hard_blocked is False
    assert s.entry_allowed is True

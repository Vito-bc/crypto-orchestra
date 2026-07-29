"""
Tests for pipeline/product_state.py — ProductRules/ProductState cache with LKG.
"""
from __future__ import annotations

import json
import math
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import pipeline.product_state as _mod

ROOT = Path(_mod.__file__).resolve().parents[1]

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
    # Anchor "stale" relative to the current monotonic clock, not to 0.0.
    # time.monotonic() counts from an arbitrary origin — on a freshly booted CI
    # runner it is only a few seconds, so a literal 0.0 is NOT older than the
    # 5-minute TTL and the state reads as fresh, inverting fail-closed tests.
    mono = time.monotonic() if fresh else time.monotonic() - _mod._STATE_TTL_S - 1.0
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
    # Relative to the monotonic clock, not 0.0 — see _make_state for why.
    rules.fetched_mono = time.monotonic() - _mod._RULES_TTL_S - 1.0
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

# ── LKG durability ────────────────────────────────────────────────────────────
# The LKG file is the only thing standing between a Coinbase metadata outage and
# an EXIT rounded on hardcoded defaults, so a partial or corrupt write matters.

def test_lkg_write_is_atomic_no_partial_file(tmp_path: Path, monkeypatch) -> None:
    """A crash mid-write must leave the previous file intact, not a truncated one."""
    _save_lkg(_make_rules("ZEC-USD", inc="0.01"), _make_state("ZEC-USD"))
    before = _mod.LKG_PATH.read_text(encoding="utf-8")

    def _explode(*_a, **_kw):
        raise OSError("disk full")

    monkeypatch.setattr(_mod.json, "dump", _explode)
    with pytest.raises(OSError):
        _save_lkg(_make_rules("ETH-USD"), _make_state("ETH-USD"))

    assert _mod.LKG_PATH.read_text(encoding="utf-8") == before
    assert _rules_from_lkg("ZEC-USD") is not None


def test_lkg_write_leaves_no_tmp_file_behind() -> None:
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    leftovers = list(_mod.LKG_PATH.parent.glob("*.tmp"))
    assert leftovers == []


def test_corrupt_lkg_is_quarantined_not_silently_dropped() -> None:
    _mod.LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _mod.LKG_PATH.write_text("{ this is not valid json", encoding="utf-8")

    assert _mod._load_lkg() == {}
    quarantined = list(_mod.LKG_PATH.parent.glob("*.corrupt-*.json"))
    assert len(quarantined) == 1, "corrupt LKG must be preserved for forensics"
    assert "not valid json" in quarantined[0].read_text(encoding="utf-8")


def test_corrupt_lkg_does_not_silently_wipe_other_products() -> None:
    """
    _save_lkg seeds from _load_lkg().  When a corrupt file silently became {},
    the next successful save erased every other product's entry with no trace.
    Quarantine keeps the evidence on disk.
    """
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    _mod.LKG_PATH.write_text("<<<corrupt>>>", encoding="utf-8")

    _save_lkg(_make_rules("ETH-USD"), _make_state("ETH-USD"))

    assert _rules_from_lkg("ETH-USD") is not None
    quarantined = list(_mod.LKG_PATH.parent.glob("*.corrupt-*.json"))
    assert len(quarantined) == 1
    assert "<<<corrupt>>>" in quarantined[0].read_text(encoding="utf-8")


def test_lkg_top_level_non_object_is_corrupt() -> None:
    _mod.LKG_PATH.parent.mkdir(parents=True, exist_ok=True)
    _mod.LKG_PATH.write_text('["a", "list"]', encoding="utf-8")
    assert _mod._load_lkg() == {}
    assert list(_mod.LKG_PATH.parent.glob("*.corrupt-*.json"))


def test_lkg_entry_with_invalid_increment_is_rejected() -> None:
    """A persisted zero increment must never become an order quantity."""
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    data["ZEC-USD"]["rules"]["base_increment"] = "0"
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")

    assert _rules_from_lkg("ZEC-USD") is None


def test_lkg_entry_reports_its_age() -> None:
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    loaded = _rules_from_lkg("ZEC-USD")
    assert loaded is not None
    assert loaded.lkg_age_s is not None
    assert loaded.lkg_age_s < 60


def test_over_age_lkg_is_reported_stale_but_still_usable() -> None:
    """Age never blocks EXIT — it only downgrades the reported source."""
    _save_lkg(_make_rules("ZEC-USD", inc="0.01"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    data["ZEC-USD"]["rules"]["fetched_wall"] = time.time() - (_mod._LKG_MAX_AGE_S + 3600)
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")

    result = get_rules_for_exit("ZEC-USD")
    assert result["source"] == "lkg_stale"
    assert result["stale"] is True
    assert result["base_increment"] == "0.01", "stale rules are still returned"


# ── get_rules_for_exit provenance ─────────────────────────────────────────────

def test_get_rules_for_exit_reports_cache_source() -> None:
    _inject_cache("ZEC-USD", rules=_make_rules())
    result = get_rules_for_exit("ZEC-USD")
    assert result["source"] == "cache"
    assert result["stale"] is False


def test_get_rules_for_exit_reports_lkg_source() -> None:
    _save_lkg(_make_rules("ZEC-USD", inc="0.0001"), _make_state("ZEC-USD"))
    _clear_cache()
    result = get_rules_for_exit("ZEC-USD")
    assert result["source"] == "lkg"
    assert result["stale"] is False
    assert result["base_increment"] == "0.0001"


def test_get_rules_for_exit_reports_defaults_source() -> None:
    result = get_rules_for_exit("ZEC-USD")
    assert result["source"] == "defaults"
    assert result["stale"] is True


# ── ENTRY gate fails closed on an unread payload ──────────────────────────────

@pytest.mark.parametrize("flag", [
    "is_disabled", "trading_disabled", "cancel_only",
    "limit_only", "post_only", "auction_mode", "view_only",
])
def test_fetch_rejects_payload_with_missing_flag(flag: str) -> None:
    """
    Missing tradability field → RuntimeError → prewarm records failure → no
    state cached → is_entry_allowed() is False.  Previously the flag defaulted
    to False and ENTRY proceeded on a product that was never actually read.
    """
    payload = {
        "product_id": "ZEC-USD",
        "base_increment": "0.00000001", "base_min_size": "0.001",
        "base_max_size": "9000", "quote_increment": "0.01",
        "is_disabled": False, "trading_disabled": False, "cancel_only": False,
        "limit_only": False, "post_only": False, "auction_mode": False,
        "view_only": False,
    }
    del payload[flag]

    client = MagicMock()
    client.get_product.return_value = payload
    with patch("exchange.coinbase_client._get_client", return_value=client):
        with pytest.raises(RuntimeError):
            _mod._fetch_from_coinbase("ZEC-USD")


def test_prewarm_with_missing_flags_blocks_entry() -> None:
    payload = {
        "product_id": "ZEC-USD",
        "base_increment": "0.00000001", "base_min_size": "0.001",
        "base_max_size": "9000", "quote_increment": "0.01",
    }   # no tradability fields at all
    client = MagicMock()
    client.get_product.return_value = payload

    with patch("exchange.coinbase_client._get_client", return_value=client):
        results = prewarm(["ZEC-USD"])

    assert results["ZEC-USD"] is False
    allowed, reason = is_entry_allowed("ZEC-USD")
    assert allowed is False
    assert "unavailable or stale" in reason


# ── Concurrent LKG writers ────────────────────────────────────────────────────

def test_concurrent_lkg_writers_keep_all_entries(tmp_path: Path, monkeypatch) -> None:
    """
    Two writers each persisting distinct assets must not drop each other's
    entries.  The read-modify-write is serialised by a cross-process file lock;
    without it, interleaved read -> modify -> write silently loses whichever
    writer commits first.  A widened read window (slow_load) forces the
    interleave that would expose an unlocked implementation.
    """
    import threading

    monkeypatch.setattr(_mod, "LKG_PATH", tmp_path / "product_lkg.json")

    orig_load = _mod._load_lkg

    def slow_load():
        d = orig_load()
        time.sleep(0.01)   # widen the read-modify-write window
        return d

    monkeypatch.setattr(_mod, "_load_lkg", slow_load)

    start = threading.Barrier(2)

    def writer(prefix: str):
        start.wait()
        for i in range(8):
            pid = f"{prefix}{i}-USD"
            _save_lkg(_make_rules(pid), _make_state(pid))

    t1 = threading.Thread(target=writer, args=("A",))
    t2 = threading.Thread(target=writer, args=("B",))
    t1.start(); t2.start()
    t1.join(); t2.join()

    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    for prefix in ("A", "B"):
        for i in range(8):
            assert f"{prefix}{i}-USD" in data, (
                f"lost {prefix}{i}-USD -- concurrent writer dropped an entry"
            )


def test_lkg_writer_uses_unique_temp_per_write(tmp_path: Path, monkeypatch) -> None:
    """
    Every write must use a distinct scratch file.  A PID-scoped name still
    collides between two writers inside one process (two threads, or one thread
    persisting several assets); mkstemp hands out a fresh name each call, so two
    consecutive writes never touch the same temp path.
    """
    monkeypatch.setattr(_mod, "LKG_PATH", tmp_path / "product_lkg.json")
    captured: list[str] = []

    real_replace = _mod.os.replace

    def _spy_replace(src, dst):
        captured.append(str(src))
        return real_replace(src, dst)

    monkeypatch.setattr(_mod.os, "replace", _spy_replace)
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    _save_lkg(_make_rules("ETH-USD"), _make_state("ETH-USD"))

    assert len(captured) == 2, "expected an os.replace per write"
    assert captured[0] != captured[1], "each write must use its own temp file"
    assert captured[0].endswith(".tmp") and captured[1].endswith(".tmp")


def test_lock_timeout_skips_write_and_preserves_old_lkg(tmp_path: Path, monkeypatch) -> None:
    """
    On lock-acquisition timeout, _save_lkg must NOT perform the read-modify-write.
    Proceeding unlocked is exactly the lost-update race the lock closes; the old
    file must stay byte-for-byte intact and no new entry must appear.
    """
    monkeypatch.setattr(_mod, "LKG_PATH", tmp_path / "product_lkg.json")
    _save_lkg(_make_rules("ZEC-USD", inc="0.01"), _make_state("ZEC-USD"))
    before = _mod.LKG_PATH.read_text(encoding="utf-8")

    # Force the lock to never be acquired, with a fast timeout.
    monkeypatch.setattr(_mod, "_try_lock", lambda fh: False)
    monkeypatch.setattr(_mod, "_LKG_LOCK_TIMEOUT_S", 0.05)

    write_calls: list = []
    real_write = _mod._atomic_write_json
    monkeypatch.setattr(
        _mod, "_atomic_write_json",
        lambda p, d: write_calls.append(p) or real_write(p, d),
    )

    persisted = _save_lkg(_make_rules("ETH-USD"), _make_state("ETH-USD"))

    assert persisted is False, "_save_lkg must report the skipped write"
    assert write_calls == [], "no atomic write may run when the lock times out"
    assert _mod.LKG_PATH.read_text(encoding="utf-8") == before, "old LKG must be untouched"
    assert _rules_from_lkg("ETH-USD") is None, "the skipped entry must not appear"
    assert _rules_from_lkg("ZEC-USD") is not None, "the prior entry must survive"


def test_cross_process_lkg_writers_keep_all_entries(tmp_path: Path) -> None:
    """
    Two real OS processes each persisting distinct assets must not drop each
    other's entries.  The production guarantee is cross-process, so the guard is
    exercised with subprocesses, not threads.
    """
    import subprocess
    import sys
    import textwrap

    lkg = tmp_path / "product_lkg.json"
    script = tmp_path / "writer.py"
    script.write_text(textwrap.dedent(f"""
        import sys, time
        from pathlib import Path
        sys.path.insert(0, {str(ROOT)!r})
        import pipeline.product_state as ps
        ps.LKG_PATH = Path({str(lkg)!r})
        prefix = sys.argv[1]
        for i in range(12):
            pid = f"{{prefix}}{{i}}-USD"
            ps._save_lkg(
                ps.ProductRules(pid, "0.00000001", "0.001", "9000", "0.01", time.time()),
                ps.ProductState(pid, False, False, False, False, False, False, False,
                                time.time(), time.monotonic()),
            )
    """), encoding="utf-8")

    procs = [
        subprocess.Popen([sys.executable, str(script), prefix])
        for prefix in ("A", "B")
    ]
    for p in procs:
        assert p.wait(timeout=60) == 0, "writer subprocess failed"

    data = json.loads(lkg.read_text(encoding="utf-8"))
    for prefix in ("A", "B"):
        for i in range(12):
            assert f"{prefix}{i}-USD" in data, (
                f"lost {prefix}{i}-USD — a concurrent process dropped an entry"
            )


# ── Persistence-failure observability (non-blocking) ──────────────────────────

def test_prewarm_persistence_failure_does_not_fail_fetch(monkeypatch) -> None:
    """
    A skipped LKG write must NOT demote the fetch result — ENTRY stays allowed —
    but the product must appear in last_persistence_failures().
    """
    def _fake_fetch(pid):
        return _make_rules(pid), _make_state(pid)

    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _fake_fetch)
    # _save_lkg reports a skipped write (lock timeout path).
    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: False)

    results = prewarm(["ZEC-USD", "ETH-USD"])

    assert results == {"ZEC-USD": True, "ETH-USD": True}, "fetch must still succeed"
    assert set(_mod.last_persistence_failures()) == {"ZEC-USD", "ETH-USD"}
    # ENTRY is unaffected — the cache is populated.
    assert is_entry_allowed("ZEC-USD")[0] is True


def test_prewarm_persistence_exception_is_reported_not_raised(monkeypatch) -> None:
    """A raising _save_lkg is contained: fetch OK, persistence failure recorded."""
    def _fake_fetch(pid):
        return _make_rules(pid), _make_state(pid)

    def _boom(r, s):
        raise OSError("disk full")

    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _fake_fetch)
    monkeypatch.setattr(_mod, "_save_lkg", _boom)

    results = prewarm(["ZEC-USD"])

    assert results == {"ZEC-USD": True}
    assert _mod.last_persistence_failures() == ["ZEC-USD"]


def test_prewarm_success_clears_persistence_failures(monkeypatch) -> None:
    """A clean prewarm resets the persistence-failure list."""
    def _fake_fetch(pid):
        return _make_rules(pid), _make_state(pid)

    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _fake_fetch)

    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: False)
    prewarm(["ZEC-USD"])
    assert _mod.last_persistence_failures() == ["ZEC-USD"]

    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: True)
    prewarm(["ZEC-USD"])
    assert _mod.last_persistence_failures() == []


# ── Persistence stickiness: no false RECOVERED on later fetch failure ─────────

def test_persistence_failure_sticky_across_fetch_failure(monkeypatch) -> None:
    """
    persist failure, then a later Coinbase FETCH failure, must keep the product
    flagged — otherwise the runner would send a false RECOVERED while the on-disk
    LKG is still stale/missing.
    """
    def _ok_fetch(pid):
        return _make_rules(pid), _make_state(pid)

    # Pass 1: fetch OK, persist fails → flagged.
    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _ok_fetch)
    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: False)
    prewarm(["ZEC-USD"])
    assert _mod.last_persistence_failures() == ["ZEC-USD"]

    # Pass 2: fetch FAILS → no write attempted → flag must persist (no clearing).
    def _boom_fetch(pid):
        raise RuntimeError("coinbase unreachable")
    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _boom_fetch)
    results = prewarm(["ZEC-USD"])
    assert results == {"ZEC-USD": False}
    assert _mod.last_persistence_failures() == ["ZEC-USD"], (
        "a fetch failure must NOT clear a prior persistence failure"
    )

    # Pass 3: fetch OK and persist confirmed → only now is it cleared.
    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _ok_fetch)
    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: True)
    prewarm(["ZEC-USD"])
    assert _mod.last_persistence_failures() == []


def test_persistence_cleared_only_on_confirmed_write(monkeypatch) -> None:
    def _ok_fetch(pid):
        return _make_rules(pid), _make_state(pid)
    monkeypatch.setattr(_mod, "_fetch_from_coinbase", _ok_fetch)

    monkeypatch.setattr(_mod, "_save_lkg", lambda r, s: False)
    prewarm(["ZEC-USD"])
    assert "ZEC-USD" in _mod.last_persistence_failures()

    # A raising _save_lkg is a failure too — stays flagged.
    def _raise(r, s):
        raise OSError("disk full")
    monkeypatch.setattr(_mod, "_save_lkg", _raise)
    prewarm(["ZEC-USD"])
    assert "ZEC-USD" in _mod.last_persistence_failures()


# ── LKG strict validation: timestamp + product_id ────────────────────────────

def test_lkg_rejects_product_id_mismatch() -> None:
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    # File the record under ETH-USD but keep the internal product_id ZEC-USD.
    data["ETH-USD"] = data.pop("ZEC-USD")
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")
    assert _rules_from_lkg("ETH-USD") is None, "mismatched product_id must be rejected"


def test_lkg_infinite_timestamp_marked_stale_not_fresh() -> None:
    _save_lkg(_make_rules("ZEC-USD", inc="0.01"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    data["ZEC-USD"]["rules"]["fetched_wall"] = float("inf")
    # json can't emit Infinity by default; allow it to round-trip.
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")

    loaded = _rules_from_lkg("ZEC-USD")
    assert loaded is not None
    assert loaded.lkg_age_s == float("inf"), "Infinity timestamp must not read as age 0"

    result = get_rules_for_exit("ZEC-USD")   # cache empty → LKG path
    assert result["source"] == "lkg_stale"
    assert result["stale"] is True
    assert result["base_increment"] == "0.01", "still returned — EXIT stays fail-open"


def test_lkg_unparseable_timestamp_marked_stale() -> None:
    """A non-numeric timestamp string is treated as maximally old."""
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    data["ZEC-USD"]["rules"]["fetched_wall"] = "not-a-number"
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")
    loaded = _rules_from_lkg("ZEC-USD")
    assert loaded is not None
    assert loaded.lkg_age_s == float("inf")


def test_lkg_real_nan_timestamp_marked_stale() -> None:
    """An actual float NaN (json round-trips it) is treated as maximally old."""
    _save_lkg(_make_rules("ZEC-USD"), _make_state("ZEC-USD"))
    data = json.loads(_mod.LKG_PATH.read_text(encoding="utf-8"))
    data["ZEC-USD"]["rules"]["fetched_wall"] = float("nan")
    _mod.LKG_PATH.write_text(json.dumps(data), encoding="utf-8")  # emits NaN
    loaded = _rules_from_lkg("ZEC-USD")
    assert loaded is not None
    assert loaded.lkg_age_s == float("inf")
    assert math.isfinite(loaded.fetched_wall), "non-finite timestamp must be sanitized"

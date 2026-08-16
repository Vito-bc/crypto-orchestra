"""
Public-data hydration — safety and identity.

Hydration exists so CI can run the full `--verify` without exchange
credentials. It is also the one component that WRITES into the candle cache, so
its failure mode matters more than its success path: the first version fetched
straight over the target and validated afterwards, meaning a bad download
destroyed the only copy of the data that produced the committed numbers.

Everything here uses a mocked public client. No network, no credentials, no
real cache.
"""
from __future__ import annotations

import json
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

import backtesting.hydrate_research_data as hyd
from backtesting.research_runner import _SCOPE_START, _scope_end, logical_sha256

SCOPE_START = pd.Timestamp(_SCOPE_START)


def _frame(periods: int = 48, seed: float = 0.0) -> pd.DataFrame:
    idx = pd.date_range(SCOPE_START, periods=periods, freq="h", tz="UTC")
    n = len(idx)
    base = np.arange(n, dtype="float64") + seed
    return pd.DataFrame({
        "time": idx,
        "open": base + 100.0, "high": base + 101.0, "low": base + 99.0,
        "close": base + 100.5, "volume": base + 10.0,
    })


def _spec(df: pd.DataFrame, name: str = "ZEC_USD_1h.parquet") -> dict:
    scoped = df[df["time"] <= pd.Timestamp(_scope_end())]
    return {
        "file": name,
        "hash_scheme": "ohlcv-logical-v1",
        "logical_sha256": logical_sha256(df),
        "rows": len(scoped),
        "min_ts": scoped["time"].iloc[0].isoformat(),
        "max_ts": scoped["time"].iloc[-1].isoformat(),
    }


@pytest.fixture
def cache(tmp_path, monkeypatch):
    """Point the candle cache at a temp dir and return it."""
    import exchange.coinbase_candles as cc

    monkeypatch.setattr(cc, "DATA_DIR", tmp_path)
    return tmp_path


def _manifest(tmp_path, monkeypatch, specs: list[dict]) -> None:
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps({"inputs": specs}), encoding="utf-8")
    monkeypatch.setattr(hyd, "MANIFEST", path)


# ── Identity is the LOGICAL hash, not the file bytes ─────────────────────────

def test_a_tail_beyond_the_freeze_still_verifies(cache, tmp_path, monkeypatch) -> None:
    """
    The whole reason the physical hash was abandoned: the exchange keeps
    completing candles past the freeze, and a byte hash failed on data the
    research never reads.
    """
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])

    tail = _frame(periods=3)
    tail["time"] = pd.date_range(pd.Timestamp(_scope_end()) + pd.Timedelta(hours=1),
                                 periods=3, freq="h", tz="UTC")
    pd.concat([df, tail], ignore_index=True).to_parquet(
        cache / "ZEC_USD_1h.parquet", index=False)

    ok, why = hyd._verify_one(_spec(df))
    assert ok, why


def test_an_in_scope_change_fails_verification(cache, tmp_path, monkeypatch) -> None:
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])
    tampered = df.copy()
    tampered.loc[tampered.index[4], "close"] += 0.01
    tampered.to_parquet(cache / "ZEC_USD_1h.parquet", index=False)

    ok, why = hyd._verify_one(_spec(df))
    assert not ok
    assert "logical hash" in why


def test_a_truncated_download_is_reported_as_such(cache, tmp_path, monkeypatch) -> None:
    """Row-count mismatch must say so, not just "hash differs"."""
    df = _frame()
    df.iloc[:-5].to_parquet(cache / "ZEC_USD_1h.parquet", index=False)
    ok, why = hyd._check_frame(pd.read_parquet(cache / "ZEC_USD_1h.parquet"), _spec(df))
    assert not ok
    assert "in-scope rows" in why


# ── An existing cache is never destroyed ─────────────────────────────────────

def test_a_mismatching_existing_file_is_diagnosed_not_overwritten(
        cache, tmp_path, monkeypatch, capsys) -> None:
    """
    An existing file that disagrees with the manifest is EVIDENCE. Replacing it
    destroys the only copy of whatever produced the committed numbers.
    """
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])

    stale = df.copy()
    stale.loc[stale.index[0], "close"] = 12345.0
    target = cache / "ZEC_USD_1h.parquet"
    stale.to_parquet(target, index=False)
    before = target.read_bytes()

    with patch.object(hyd, "_fetch") as fetch:
        rc = hyd.hydrate()

    assert rc == 1, "a mismatch must fail the run"
    assert not fetch.called, "must not fetch over an existing file by default"
    assert target.read_bytes() == before, "existing cache was modified"
    assert "left untouched" in capsys.readouterr().err


def test_a_failed_download_leaves_the_existing_file_intact(
        cache, tmp_path, monkeypatch) -> None:
    """
    --repair is allowed to re-fetch, but a candidate that fails validation must
    not replace the target. This is the case that used to lose the good file.
    """
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])

    stale = df.copy()
    stale.loc[stale.index[0], "close"] = 12345.0
    target = cache / "ZEC_USD_1h.parquet"
    stale.to_parquet(target, index=False)
    before = target.read_bytes()

    # The download returns data that does not match the manifest.
    with patch.object(hyd, "_fetch", return_value=_frame(seed=500.0)):
        rc = hyd.hydrate(repair=True)

    assert rc == 1
    assert target.read_bytes() == before, "a rejected candidate replaced the target"
    assert not (cache / "ZEC_USD_1h.partial").exists(), "candidate left behind"


def test_a_raising_download_leaves_the_existing_file_intact(
        cache, tmp_path, monkeypatch) -> None:
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])
    stale = df.copy()
    stale.loc[stale.index[0], "close"] = 12345.0
    target = cache / "ZEC_USD_1h.parquet"
    stale.to_parquet(target, index=False)
    before = target.read_bytes()

    with patch.object(hyd, "_fetch", side_effect=ConnectionError("endpoint down")):
        rc = hyd.hydrate(repair=True)

    assert rc == 1
    assert target.read_bytes() == before
    assert not (cache / "ZEC_USD_1h.partial").exists()


def test_a_successful_repair_replaces_atomically(cache, tmp_path, monkeypatch) -> None:
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])
    stale = df.copy()
    stale.loc[stale.index[0], "close"] = 12345.0
    (cache / "ZEC_USD_1h.parquet").write_bytes(b"")      # unreadable on purpose
    stale.to_parquet(cache / "ZEC_USD_1h.parquet", index=False)

    with patch.object(hyd, "_fetch", return_value=df):
        rc = hyd.hydrate(repair=True)

    assert rc == 0
    assert hyd._verify_one(_spec(df))[0]
    assert not (cache / "ZEC_USD_1h.partial").exists()


def test_a_missing_file_is_fetched_without_repair(cache, tmp_path, monkeypatch) -> None:
    """Building an empty cache is the normal CI path and needs no flag."""
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])

    with patch.object(hyd, "_fetch", return_value=df) as fetch:
        rc = hyd.hydrate()

    assert rc == 0
    assert fetch.called
    assert (cache / "ZEC_USD_1h.parquet").exists()


def test_check_only_never_fetches_or_writes(cache, tmp_path, monkeypatch) -> None:
    df = _frame()
    _manifest(tmp_path, monkeypatch, [_spec(df)])

    with patch.object(hyd, "_fetch") as fetch:
        rc = hyd.hydrate(check_only=True)

    assert rc == 1                      # nothing materialised yet
    assert not fetch.called
    assert not (cache / "ZEC_USD_1h.parquet").exists()


# ── No credentials, ever ─────────────────────────────────────────────────────

def test_the_client_is_constructed_without_key_material() -> None:
    """
    The entire justification for wiring verify into CI is that this path needs
    no exchange secrets. If it ever starts requiring them, that must fail here.
    """
    import inspect

    src = inspect.getsource(hyd._public_client)
    assert "RESTClient()" in src
    assert "key_file" not in src and "api_key" not in src

    with patch("coinbase.rest.RESTClient") as rc:
        hyd._public_client()
    rc.assert_called_once_with()


def test_only_the_public_candles_endpoint_is_used() -> None:
    """get_candles (authenticated) must not appear on this path."""
    import inspect

    src = inspect.getsource(hyd)
    assert "get_public_candles" in src
    assert "client.get_candles" not in src

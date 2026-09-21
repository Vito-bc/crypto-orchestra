"""Timestamp-unit contracts at the Coinbase candle boundary."""
from __future__ import annotations

from types import SimpleNamespace

import numpy as np
import pandas as pd
import pytest

import exchange.coinbase_candles as candles


def _ohlcv_frame(times: pd.Series) -> pd.DataFrame:
    n = len(times)
    close = np.linspace(100.0, 110.0, n)
    return pd.DataFrame({
        "time": times,
        "open": close - 0.5,
        "high": close + 1.0,
        "low": close - 1.0,
        "close": close,
        "volume": np.linspace(10.0, 20.0, n),
    })


def _raw_candle(timestamp: pd.Timestamp) -> dict[str, str]:
    return {
        "start": str(int(timestamp.timestamp())),
        "open": "109.5",
        "high": "111.0",
        "low": "109.0",
        "close": "110.0",
        "volume": "20.0",
    }


@pytest.mark.parametrize(
    ("granularity", "step"),
    [("1h", pd.Timedelta(hours=1)), ("1d", pd.Timedelta(days=1))],
)
def test_download_normalises_cached_ms_and_fresh_s_through_concat(
        tmp_path, monkeypatch, granularity, step) -> None:
    """A mixed cache/live return has the loader's one declared timestamp unit."""
    monkeypatch.setattr(candles, "DATA_DIR", tmp_path)
    cached_at = pd.Timestamp("2026-01-01", tz="UTC")
    cached = _ohlcv_frame(pd.Series([cached_at], dtype="datetime64[ms, UTC]"))
    cached.to_parquet(candles._parquet_path("ZEC-USD", granularity), index=False)

    fresh_at = cached_at + step
    monkeypatch.setattr(candles, "_get_client", lambda: SimpleNamespace())
    monkeypatch.setattr(
        candles,
        "_fetch_batch",
        lambda *args, **kwargs: [_raw_candle(fresh_at)],
    )

    result = candles.download(
        "ZEC-USD",
        start=cached_at,
        end=fresh_at + step,
        granularity=granularity,
        verbose=False,
    )

    assert str(result["time"].dtype) == f"datetime64[{candles._TIME_UNIT}, UTC]"
    assert result["time"].tolist() == [cached_at, fresh_at]


def test_build_merged_frame_accepts_ms_hourly_and_s_daily_caches(
        tmp_path, monkeypatch) -> None:
    """The normal download boundary makes mixed-unit scanner inputs mergeable."""
    from backtesting import signal_scanner

    monkeypatch.setattr(candles, "DATA_DIR", tmp_path)
    monkeypatch.setattr(signal_scanner, "STRICT_COINBASE_ONLY", True)

    end = pd.Timestamp("2024-02-15", tz="UTC")
    hourly_times = pd.Series(
        pd.date_range(pd.Timestamp("2024-01-01", tz="UTC"), end, freq="h"),
        dtype="datetime64[ms, UTC]",
    )
    daily_times = pd.Series(
        pd.date_range(pd.Timestamp("2020-01-01", tz="UTC"), end, freq="D"),
        dtype="datetime64[s, UTC]",
    )
    _ohlcv_frame(hourly_times).to_parquet(
        candles._parquet_path("ZEC-USD", "1h"), index=False
    )
    _ohlcv_frame(daily_times).to_parquet(
        candles._parquet_path("ZEC-USD", "1d"), index=False
    )

    merged, daily = signal_scanner.build_merged_frame(
        "ZEC-USD",
        "2024-01-01",
        str(end.date()),
        signal_scanner.ASSET_CONFIG["ZEC-USD"],
        btc_regime_applicable=False,
    )

    expected_dtype = f"datetime64[{candles._TIME_UNIT}, UTC]"
    assert merged is not None and not merged.empty
    assert daily is not None and not daily.empty
    assert str(merged["time"].dtype) == expected_dtype
    assert str(daily["time"].dtype) == expected_dtype

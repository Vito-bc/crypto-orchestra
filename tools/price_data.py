"""
Price data tool — thin wrapper around the existing backtest utilities.

Includes a thread-safe TTL cache so that all 5 agents running in parallel
share a single yfinance download per (symbol, timeframe) per pipeline run.

Without cache: ~10-12 yfinance downloads per run (~25-30 seconds of I/O).
With cache   : 2 downloads per asset (1h + 4h), fetched once and reused.

Cache TTL is 55 minutes — slightly less than the 60-minute scheduler interval
so data is always fresh on each run but never re-downloaded within a run.
"""

from __future__ import annotations

import math
import threading
from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from backtesting.backtest import (
    attach_higher_timeframe_context,
    evaluate_entry_components,
    get_signal,
    get_symbol_config,
    prepare_timeframe_df,
)

# ── Symbol normalisation ──────────────────────────────────────────────────────

_SYMBOL_MAP = {
    "BTC/USDT": "BTC-USD",
    "ETH/USDT": "ETH-USD",
    "BTC/USD":  "BTC-USD",
    "ETH/USD":  "ETH-USD",
    "BTC-USD":  "BTC-USD",
    "ETH-USD":  "ETH-USD",
}


def normalize_symbol(asset: str) -> str:
    return _SYMBOL_MAP.get(asset.upper(), asset)


# ── TTL cache ─────────────────────────────────────────────────────────────────

_CACHE_TTL   = timedelta(minutes=55)
_cache: dict[tuple, tuple] = {}   # key → (DataFrame, fetched_at)
_cache_lock  = threading.Lock()


def _fetch_cached(symbol: str, timeframe: str, lookback_days: int):
    """
    Return a cached DataFrame if still fresh, otherwise fetch and cache it.
    Thread-safe: multiple agents can call this concurrently.
    """
    key = (symbol, timeframe, lookback_days)
    now = datetime.now(timezone.utc)

    with _cache_lock:
        if key in _cache:
            df, fetched_at = _cache[key]
            age = now - fetched_at
            if age < _CACHE_TTL:
                return df   # cache hit

        # Cache miss — fetch outside the lock would allow duplicate fetches,
        # but for simplicity (and since yfinance is idempotent) we fetch inside.
        df = prepare_timeframe_df(symbol, timeframe, lookback_days)
        if df is not None:
            _cache[key] = (df, now)
        return df


def clear_cache() -> None:
    """Force-expire all cached data. Useful for testing."""
    with _cache_lock:
        _cache.clear()


def cache_stats() -> dict:
    """Return current cache state for diagnostics."""
    with _cache_lock:
        now = datetime.now(timezone.utc)
        return {
            k: {
                "rows":    len(v[0]),
                "age_min": round((now - v[1]).total_seconds() / 60, 1),
                "fresh":   (now - v[1]) < _CACHE_TTL,
            }
            for k, v in _cache.items()
        }


# ── Public API ────────────────────────────────────────────────────────────────

def get_raw_df(asset: str, lookback_days: int = 90):
    """
    Return the full merged 1h+4h indicator DataFrame for S/R level detection.
    Uses the TTL cache — safe to call from multiple agents concurrently.
    """
    symbol    = normalize_symbol(asset)
    signal_df = _fetch_cached(symbol, "1h", lookback_days)
    trend_df  = _fetch_cached(symbol, "4h", lookback_days)
    if signal_df is None or trend_df is None:
        return None
    return attach_higher_timeframe_context(signal_df, trend_df)


def get_snapshot(asset: str, lookback_days: int = 90) -> dict | None:
    """
    Return a fully computed indicator snapshot for the given asset.
    Returns None if data cannot be fetched.
    """
    symbol    = normalize_symbol(asset)
    signal_df = _fetch_cached(symbol, "1h", lookback_days)
    trend_df  = _fetch_cached(symbol, "4h", lookback_days)

    if signal_df is None or trend_df is None:
        return None

    df = attach_higher_timeframe_context(signal_df, trend_df)
    if len(df) < 2:
        return None

    row      = df.iloc[-1]
    prev_row = df.iloc[-2]
    config   = get_symbol_config(symbol)
    signal   = get_signal(row, prev_row, config)
    comps    = evaluate_entry_components(row, prev_row, config) or {}

    return {
        "symbol":            symbol,
        "close":             float(row["close"]),
        "open":              float(row["open"]),
        "signal":            signal,
        "rsi_1h":            float(row["rsi"]),
        "macd_diff_1h":      float(row["macd_diff"]),
        "bb_pct_1h":         float(row["bb_pct"]),
        "volume_ratio_1h":   float(row["volume_ratio"]),
        "atr_1h":            float(row["atr"]),
        "ema50_1h":          float(row["ema50"]),
        "ema200_1h":         float(row["ema200"]),
        "close_4h":          float(row["close_4h"])          if "close_4h"          in row else None,
        "ema50_4h":          float(row["ema50_4h"])          if "ema50_4h"          in row else None,
        "ema200_4h":         float(row["ema200_4h"])         if "ema200_4h"         in row else None,
        "trend_4h":          row.get("trend_4h", ""),
        "trend_strength_4h": float(row["trend_strength_4h"]) if "trend_strength_4h" in row else None,
        "trend_ok":          bool(comps.get("trend_ok",  False)),
        "macd_ok":           bool(comps.get("macd_ok",   False)),
        "rsi_ok":            bool(comps.get("rsi_ok",    False)),
        "bb_ok":             bool(comps.get("bb_ok",     False)),
        "volume_ok":         bool(comps.get("volume_ok", False)),
        "buy_ready":         bool(comps.get("buy_ready", False)),
        "adx_1h":            float(row["adx"]) if "adx" in row.index and not math.isnan(float(row["adx"])) else 0.0,
        "vwap_1h":           float(row["vwap"]) if "vwap" in row.index and not math.isnan(float(row["vwap"])) else None,
        "cvd_24h":           float(row["cvd_24h"]) if "cvd_24h" in row.index and not math.isnan(float(row["cvd_24h"])) else None,
        "cvd_6h_ago":        float(df.iloc[-7]["cvd_24h"]) if len(df) > 7 and "cvd_24h" in df.columns and not math.isnan(float(df.iloc[-7]["cvd_24h"])) else None,
        "ewma_vol_daily":    float(row["ewma_vol"]) if "ewma_vol" in row.index and not math.isnan(float(row["ewma_vol"])) else None,
    }

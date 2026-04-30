"""
Price data tool — thin wrapper around the existing backtest utilities.

Agents call get_snapshot(asset) to get a fully computed dict of
indicators without needing to know the internals of backtest.py.
"""

from __future__ import annotations

import sys
from pathlib import Path

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


# Map agent-friendly symbols (BTC/USDT) to Yahoo Finance format (BTC-USD)
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


def get_raw_df(asset: str, lookback_days: int = 90) -> "pd.DataFrame | None":
    """Return the full 1h indicator DataFrame (for S/R level detection)."""
    import pandas as pd
    symbol    = normalize_symbol(asset)
    signal_df = prepare_timeframe_df(symbol, "1h", lookback_days)
    trend_df  = prepare_timeframe_df(symbol, "4h", lookback_days)
    if signal_df is None or trend_df is None:
        return None
    return attach_higher_timeframe_context(signal_df, trend_df)


def get_snapshot(asset: str, lookback_days: int = 90) -> dict | None:
    """
    Return a fully computed indicator snapshot for the given asset.
    This is the same dict that paper_trade.py produces.
    Returns None if data cannot be fetched.
    """
    symbol = normalize_symbol(asset)
    signal_df = prepare_timeframe_df(symbol, "1h", lookback_days)
    trend_df  = prepare_timeframe_df(symbol, "4h", lookback_days)

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
        "signal":            signal,
        "rsi_1h":            float(row["rsi"]),
        "macd_diff_1h":      float(row["macd_diff"]),
        "bb_pct_1h":         float(row["bb_pct"]),
        "volume_ratio_1h":   float(row["volume_ratio"]),
        "atr_1h":            float(row["atr"]),
        "ema50_1h":          float(row["ema50"]),
        "ema200_1h":         float(row["ema200"]),
        "close_4h":          float(row["close_4h"])         if "close_4h"         in row else None,
        "ema50_4h":          float(row["ema50_4h"])         if "ema50_4h"         in row else None,
        "ema200_4h":         float(row["ema200_4h"])        if "ema200_4h"        in row else None,
        "trend_4h":          row.get("trend_4h", ""),
        "trend_strength_4h": float(row["trend_strength_4h"]) if "trend_strength_4h" in row else None,
        "trend_ok":          bool(comps.get("trend_ok", False)),
        "macd_ok":           bool(comps.get("macd_ok", False)),
        "rsi_ok":            bool(comps.get("rsi_ok", False)),
        "bb_ok":             bool(comps.get("bb_ok", False)),
        "volume_ok":         bool(comps.get("volume_ok", False)),
        "buy_ready":         bool(comps.get("buy_ready", False)),
    }

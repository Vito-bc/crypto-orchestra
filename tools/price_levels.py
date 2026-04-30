"""
Support / Resistance level detection.

Uses swing high/low analysis on 1h OHLC data to identify key price zones.
Clusters nearby swing points into zones and scores them by touch count
and recency. Returns nearest support, nearest resistance, and whether
the current price is "at" either level (within ATR tolerance).

No lookahead bias — only uses candles before the current index.
"""

from __future__ import annotations

import numpy as np
import pandas as pd


def find_swing_lows(lows: pd.Series, n: int = 5) -> list[int]:
    """Return indices where low is the minimum in the surrounding n-candle window."""
    result = []
    arr = lows.values
    for i in range(n, len(arr) - n):
        window_low = min(arr[max(0, i - n): i + n + 1])
        if arr[i] == window_low:
            result.append(i)
    return result


def find_swing_highs(highs: pd.Series, n: int = 5) -> list[int]:
    """Return indices where high is the maximum in the surrounding n-candle window."""
    result = []
    arr = highs.values
    for i in range(n, len(arr) - n):
        window_high = max(arr[max(0, i - n): i + n + 1])
        if arr[i] == window_high:
            result.append(i)
    return result


def cluster_levels(prices: list[float], cluster_pct: float = 0.005) -> list[dict]:
    """
    Group nearby price levels into zones.

    Returns list of { price, touches, strength } sorted by price ascending.
    cluster_pct: levels within this % of each other are merged (default 0.5%).
    """
    if not prices:
        return []

    sorted_prices = sorted(prices)
    clusters: list[dict] = []

    current_cluster = [sorted_prices[0]]
    for p in sorted_prices[1:]:
        if (p - current_cluster[0]) / current_cluster[0] <= cluster_pct:
            current_cluster.append(p)
        else:
            clusters.append({
                "price":   sum(current_cluster) / len(current_cluster),
                "touches": len(current_cluster),
            })
            current_cluster = [p]

    clusters.append({
        "price":   sum(current_cluster) / len(current_cluster),
        "touches": len(current_cluster),
    })
    return clusters


def get_levels(df: pd.DataFrame, current_idx: int,
               lookback: int = 150, n_swing: int = 5,
               cluster_pct: float = 0.005) -> dict:
    """
    Detect support and resistance zones from price history.

    Args:
        df:          DataFrame with columns: open, high, low, close, atr
        current_idx: current candle index — only looks backward from here
        lookback:    how many candles back to scan (default 150h ≈ 6 days)
        n_swing:     swing window (candles each side, default 5)
        cluster_pct: zone width in decimal (default 0.5%)

    Returns:
        {
            "current_price":    float,
            "atr":              float,
            "supports":         [{"price": float, "touches": int}, ...],
            "resistances":      [{"price": float, "touches": int}, ...],
            "nearest_support":  float | None,
            "nearest_resistance": float | None,
            "dist_to_support":  float | None,   # as fraction of ATR
            "dist_to_resistance": float | None, # as fraction of ATR
            "at_support":       bool,
            "at_resistance":    bool,
            "context":          str,            # human-readable summary
        }
    """
    start = max(0, current_idx - lookback)
    hist  = df.iloc[start:current_idx]

    if len(hist) < n_swing * 2 + 1:
        return _empty_result(df, current_idx)

    current_price = float(df["close"].iloc[current_idx])
    atr           = float(df["atr"].iloc[current_idx]) if "atr" in df.columns else current_price * 0.01
    at_threshold  = 1.5 * atr   # "at level" = within 1.5 ATR

    # Find swing points (indices relative to hist)
    sl_idx = find_swing_lows(hist["low"].reset_index(drop=True), n_swing)
    sh_idx = find_swing_highs(hist["high"].reset_index(drop=True), n_swing)

    support_prices    = [float(hist["low"].iloc[i])  for i in sl_idx]
    resistance_prices = [float(hist["high"].iloc[i]) for i in sh_idx]

    # Keep only levels below / above current price
    support_prices    = [p for p in support_prices    if p < current_price]
    resistance_prices = [p for p in resistance_prices if p > current_price]

    supports    = sorted(cluster_levels(support_prices,    cluster_pct), key=lambda x: x["price"], reverse=True)
    resistances = sorted(cluster_levels(resistance_prices, cluster_pct), key=lambda x: x["price"])

    # Nearest levels
    nearest_support    = supports[0]["price"]    if supports    else None
    nearest_resistance = resistances[0]["price"] if resistances else None

    dist_sup = abs(current_price - nearest_support)    if nearest_support    else None
    dist_res = abs(nearest_resistance - current_price) if nearest_resistance else None

    at_support    = dist_sup is not None and dist_sup <= at_threshold
    at_resistance = dist_res is not None and dist_res <= at_threshold

    # Human-readable context
    lines = []
    if nearest_support:
        pct_away = (current_price - nearest_support) / current_price * 100
        lines.append(
            f"Nearest support: ${nearest_support:,.2f}  "
            f"({pct_away:.1f}% below, {supports[0]['touches']} touches)"
            + ("  <-- PRICE AT SUPPORT" if at_support else "")
        )
    if nearest_resistance:
        pct_away = (nearest_resistance - current_price) / current_price * 100
        lines.append(
            f"Nearest resistance: ${nearest_resistance:,.2f}  "
            f"({pct_away:.1f}% above, {resistances[0]['touches']} touches)"
            + ("  <-- PRICE AT RESISTANCE" if at_resistance else "")
        )
    if at_support:
        lines.append("Assessment: Price is AT a key support — high-probability BUY zone.")
    elif at_resistance:
        lines.append("Assessment: Price is AT a key resistance — BUY entries carry high reversal risk.")
    elif nearest_support and dist_sup and dist_sup < 3 * atr:
        lines.append("Assessment: Price is approaching support — wait for confirmation before buying.")
    else:
        lines.append("Assessment: Price is mid-range between levels — lower-probability entry zone.")

    return {
        "current_price":      current_price,
        "atr":                atr,
        "supports":           supports[:3],
        "resistances":        resistances[:3],
        "nearest_support":    nearest_support,
        "nearest_resistance": nearest_resistance,
        "dist_to_support":    round(dist_sup / atr, 2)  if dist_sup  else None,
        "dist_to_resistance": round(dist_res / atr, 2)  if dist_res  else None,
        "at_support":         at_support,
        "at_resistance":      at_resistance,
        "context":            "\n".join(lines),
    }


def _empty_result(df: pd.DataFrame, idx: int) -> dict:
    price = float(df["close"].iloc[idx]) if idx < len(df) else 0.0
    return {
        "current_price": price, "atr": 0.0,
        "supports": [], "resistances": [],
        "nearest_support": None, "nearest_resistance": None,
        "dist_to_support": None, "dist_to_resistance": None,
        "at_support": False, "at_resistance": False,
        "context": "Insufficient history to detect price levels.",
    }


def get_levels_from_snapshot(df: pd.DataFrame) -> dict:
    """
    Convenience wrapper: detect levels at the latest candle (end of df).
    Used by the live technical agent.
    """
    return get_levels(df, len(df) - 1)

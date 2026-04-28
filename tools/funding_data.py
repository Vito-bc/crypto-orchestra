"""
Funding rate data tool.

Fetches perpetual futures funding rates from OKX public API (no auth needed,
accessible from US IPs). Funding rates are the most reliable crypto-specific
signal: extreme positive = crowded longs = bearish lean; extreme negative =
crowded shorts = bullish lean.

Settlements occur every 8 hours. We fetch current + last 3 to build context.
"""

from __future__ import annotations

import json
from urllib.request import urlopen, Request
from urllib.error import URLError

_BASE = "https://www.okx.com/api/v5/public"
_HEADERS = {"User-Agent": "CryptoOrchestra/1.0"}

_SYMBOL_MAP = {
    "BTC-USD":  "BTC-USDT-SWAP",
    "ETH-USD":  "ETH-USDT-SWAP",
    "BTC/USDT": "BTC-USDT-SWAP",
    "ETH/USDT": "ETH-USDT-SWAP",
}

# Thresholds per 8-hour period
_EXTREME_LONG  =  0.0003   # +0.03% per 8h → crowded longs → SELL lean
_MODERATE_LONG =  0.0001   # +0.01% per 8h → slight long bias
_MODERATE_SHORT = -0.0001  # -0.01% per 8h → slight short bias
_EXTREME_SHORT  = -0.0003  # -0.03% per 8h → crowded shorts → BUY lean


def _inst_id(asset: str) -> str:
    return _SYMBOL_MAP.get(asset.upper(), f"{asset.split('-')[0]}-USDT-SWAP")


def get_funding_rate(asset: str) -> dict:
    """
    Returns current funding rate plus recent history context.

    {
        "inst_id":          "BTC-USDT-SWAP",
        "current_rate":     0.000046,    # per 8h
        "current_rate_pct": 0.0046,      # percent per 8h
        "avg_24h_rate":     0.000038,    # average of last 3 settlements
        "trend":            "rising" | "falling" | "stable",
        "signal":           "BUY" | "SELL" | "NEUTRAL",
        "signal_strength":  "extreme" | "moderate" | "mild" | "none",
        "interpretation":   "...",
        "history":          [ ... ],     # last 3 settlements
        "error":            None | str,
    }
    """
    inst_id = _inst_id(asset)
    result = {
        "inst_id":          inst_id,
        "current_rate":     0.0,
        "current_rate_pct": 0.0,
        "avg_24h_rate":     0.0,
        "trend":            "stable",
        "signal":           "NEUTRAL",
        "signal_strength":  "none",
        "interpretation":   "No funding data available.",
        "history":          [],
        "error":            None,
    }

    try:
        # Current rate
        req  = Request(f"{_BASE}/funding-rate?instId={inst_id}", headers=_HEADERS)
        data = json.loads(urlopen(req, timeout=10).read())
        if not data.get("data"):
            raise ValueError("Empty response")
        current = float(data["data"][0]["fundingRate"])
        result["current_rate"]     = current
        result["current_rate_pct"] = round(current * 100, 6)

        # History (last 3 settlements = 24h context)
        req_h  = Request(f"{_BASE}/funding-rate-history?instId={inst_id}&limit=4", headers=_HEADERS)
        hist   = json.loads(urlopen(req_h, timeout=10).read())
        rates  = [float(h["realizedRate"]) for h in hist.get("data", [])]
        result["history"] = [round(r * 100, 6) for r in rates]

        if rates:
            avg = sum(rates) / len(rates)
            result["avg_24h_rate"] = round(avg * 100, 6)
            # Trend: is current rate moving away from or toward zero?
            if len(rates) >= 2:
                if current > rates[0] * 1.2 and current > 0:
                    result["trend"] = "rising"
                elif current < rates[0] * 0.8 and current < 0:
                    result["trend"] = "falling"
                else:
                    result["trend"] = "stable"

        # Classify signal
        if current >= _EXTREME_LONG:
            result["signal"]         = "SELL"
            result["signal_strength"] = "extreme"
            result["interpretation"]  = (
                f"Funding rate +{current*100:.4f}%/8h — extremely crowded longs. "
                "Market is over-leveraged bullish; high probability of long liquidation cascade."
            )
        elif current >= _MODERATE_LONG:
            result["signal"]         = "NEUTRAL"
            result["signal_strength"] = "moderate"
            result["interpretation"]  = (
                f"Funding rate +{current*100:.4f}%/8h — moderate long bias. "
                "Slight bearish lean for new positions; trend likely continues but with elevated washout risk."
            )
        elif current <= _EXTREME_SHORT:
            result["signal"]         = "BUY"
            result["signal_strength"] = "extreme"
            result["interpretation"]  = (
                f"Funding rate {current*100:.4f}%/8h — extremely crowded shorts. "
                "Market over-leveraged bearish; high probability of short squeeze rally."
            )
        elif current <= _MODERATE_SHORT:
            result["signal"]         = "NEUTRAL"
            result["signal_strength"] = "moderate"
            result["interpretation"]  = (
                f"Funding rate {current*100:.4f}%/8h — moderate short bias. "
                "Slight bullish lean; shorts are paying, favors cautious long entries."
            )
        else:
            result["signal"]         = "NEUTRAL"
            result["signal_strength"] = "none"
            result["interpretation"]  = (
                f"Funding rate {current*100:.4f}%/8h — neutral. "
                "No significant positioning bias detected."
            )

    except (URLError, KeyError, ValueError, json.JSONDecodeError) as exc:
        result["error"] = str(exc)

    return result

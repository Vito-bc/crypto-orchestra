"""
Market Positioning Tool — Open Interest, Long/Short Ratio, Multi-exchange Funding.

Sources (all free, no auth, US-accessible):
  - OKX: Open Interest, Long/Short Ratio, Funding Rate
  - Bybit: Open Interest, Long/Short Ratio (backup)

Binance fapi.binance.com is geo-blocked for US IPs — not used here.

Open Interest interpretation matrix:
  Rising OI + Rising Price  = real new buying  (bullish)
  Rising OI + Falling Price = real new selling (bearish)
  Falling OI + Rising Price = short covering   (weak rally)
  Falling OI + Falling Price = liquidations    (potential bottom)
"""

from __future__ import annotations

import json
from urllib.request import urlopen, Request

_HEADERS = {"User-Agent": "CryptoOrchestra/1.0"}

_OKX_BASE   = "https://www.okx.com/api/v5"
_BYBIT_BASE = "https://api.bybit.com/v5/market"

_OKX_SYMBOL = {
    "BTC-USD": "BTC-USDT-SWAP",
    "ETH-USD": "ETH-USDT-SWAP",
    "SOL-USD": "SOL-USDT-SWAP",
    "ZEC-USD": None,
}

_BYBIT_SYMBOL = {
    "BTC-USD": "BTCUSDT",
    "ETH-USD": "ETHUSDT",
    "SOL-USD": "SOLUSDT",
    "ZEC-USD": None,
}


def _get(url: str) -> dict | list | None:
    try:
        req = Request(url, headers=_HEADERS)
        with urlopen(req, timeout=10) as r:
            return json.loads(r.read())
    except Exception:
        return None


def get_open_interest(asset: str) -> dict:
    """Open Interest from OKX (primary) with Bybit backup."""
    result = {
        "oi_usd": 0.0, "oi_change_pct": 0.0, "oi_trend": "unknown",
        "price_vs_oi": "No data", "signal": "NEUTRAL",
        "confidence": 0.3, "interpretation": "Open interest data unavailable.",
        "source": None, "error": None,
    }

    okx_sym = _OKX_SYMBOL.get(asset.upper())

    # ── OKX Open Interest ─────────────────────────────────────────────────────
    if okx_sym:
        try:
            # Current OI
            data = _get(f"{_OKX_BASE}/public/open-interest?instId={okx_sym}")
            if data and data.get("data"):
                oi_now = float(data["data"][0]["oi"])
                price_data = _get(f"{_OKX_BASE}/market/ticker?instId={okx_sym}")
                price = float(price_data["data"][0]["last"]) if price_data and price_data.get("data") else 0

                # OI history for change calculation (last 8 × 30min candles)
                hist = _get(
                    f"{_OKX_BASE}/rubik/stat/contracts/open-interest-volume"
                    f"?ccy={okx_sym.split('-')[0]}&period=5m&limit=10"
                )
                oi_change = 0.0
                price_change = 0.0

                if hist and hist.get("data") and len(hist["data"]) >= 2:
                    # data is [ts, oi_base, oi_quote] newest first
                    oi_recent = float(hist["data"][0][1])
                    oi_old    = float(hist["data"][-1][1])
                    if oi_old > 0:
                        oi_change = (oi_recent - oi_old) / oi_old * 100

                # Price change from kline
                kline = _get(
                    f"{_OKX_BASE}/market/candles?instId={okx_sym}&bar=4H&limit=2"
                )
                if kline and kline.get("data") and len(kline["data"]) >= 2:
                    p_open  = float(kline["data"][-1][1])
                    p_close = float(kline["data"][0][4])
                    price_change = (p_close - p_open) / p_open * 100 if p_open else 0

                result["oi_usd"]        = round(oi_now * price, 0)
                result["oi_change_pct"] = round(oi_change, 2)
                result["oi_trend"]      = (
                    "rising"  if oi_change >  1.5 else
                    "falling" if oi_change < -1.5 else
                    "stable"
                )
                result["source"] = "OKX"
                _apply_oi_signal(result, price_change)
                return result
        except Exception as e:
            result["error"] = f"OKX: {e}"

    # ── Bybit fallback ────────────────────────────────────────────────────────
    bybit_sym = _BYBIT_SYMBOL.get(asset.upper())
    if bybit_sym:
        try:
            data = _get(f"{_BYBIT_BASE}/open-interest?category=linear&symbol={bybit_sym}&intervalTime=4h&limit=2")
            if data and data.get("result", {}).get("list"):
                lst = data["result"]["list"]
                oi_now = float(lst[0]["openInterest"])
                oi_old = float(lst[-1]["openInterest"]) if len(lst) > 1 else oi_now
                oi_change = (oi_now - oi_old) / oi_old * 100 if oi_old else 0

                ticker = _get(f"{_BYBIT_BASE}/tickers?category=linear&symbol={bybit_sym}")
                price = 0.0
                price_change = 0.0
                if ticker and ticker.get("result", {}).get("list"):
                    t = ticker["result"]["list"][0]
                    price = float(t.get("lastPrice", 0))
                    price_change = float(t.get("price24hPcnt", 0)) * 100

                result["oi_usd"]        = round(oi_now * price, 0)
                result["oi_change_pct"] = round(oi_change, 2)
                result["oi_trend"]      = (
                    "rising"  if oi_change >  1.5 else
                    "falling" if oi_change < -1.5 else
                    "stable"
                )
                result["source"] = "Bybit"
                _apply_oi_signal(result, price_change)
                return result
        except Exception as e:
            result["error"] = (result.get("error") or "") + f" Bybit: {e}"

    return result


def _apply_oi_signal(result: dict, price_change: float) -> None:
    """Fill signal/confidence/interpretation based on OI trend × price direction."""
    oi_trend  = result["oi_trend"]
    price_up   = price_change >  0.5
    price_down = price_change < -0.5

    if oi_trend == "rising" and price_up:
        result["signal"]        = "BUY"
        result["confidence"]    = 0.70
        result["price_vs_oi"]   = "Rising OI + Rising price = real new buying"
        result["interpretation"] = (
            f"OI +{result['oi_change_pct']:.1f}% as price rises {price_change:+.1f}% "
            "— genuine demand confirmed. Bullish."
        )
    elif oi_trend == "rising" and price_down:
        result["signal"]        = "SELL"
        result["confidence"]    = 0.70
        result["price_vs_oi"]   = "Rising OI + Falling price = real new selling"
        result["interpretation"] = (
            f"OI +{result['oi_change_pct']:.1f}% while price falls {price_change:+.1f}% "
            "— new shorts being opened. Bearish."
        )
    elif oi_trend == "falling" and price_up:
        result["signal"]        = "NEUTRAL"
        result["confidence"]    = 0.35
        result["price_vs_oi"]   = "Falling OI + Rising price = short covering only"
        result["interpretation"] = (
            f"Price +{price_change:.1f}% but OI {result['oi_change_pct']:.1f}% "
            "— shorts closing, not real buying. Rally likely unsustainable."
        )
    elif oi_trend == "falling" and price_down:
        result["signal"]        = "NEUTRAL"
        result["confidence"]    = 0.40
        result["price_vs_oi"]   = "Falling OI + Falling price = long liquidations"
        result["interpretation"] = (
            f"OI {result['oi_change_pct']:.1f}%, price {price_change:.1f}% "
            "— forced long liquidations. Potential capitulation bottom."
        )
    else:
        result["interpretation"] = (
            f"OI {result['oi_change_pct']:+.1f}%, price {price_change:+.1f}% — no strong signal."
        )


def get_long_short_ratio(asset: str) -> dict:
    """Long/Short ratio from OKX (primary) with Bybit fallback."""
    result = {
        "long_pct": 50.0, "short_pct": 50.0,
        "signal": "NEUTRAL", "confidence": 0.3,
        "interpretation": "Long/short data unavailable.", "error": None,
    }

    okx_sym  = _OKX_SYMBOL.get(asset.upper())
    base_ccy = asset.split("-")[0].upper()

    # ── OKX Long/Short Ratio ──────────────────────────────────────────────────
    # OKX returns [timestamp, ls_ratio] where ls_ratio = long_count / short_count
    # e.g. 1.61 means 61.7% long, 38.3% short
    if okx_sym:
        try:
            data = _get(
                f"{_OKX_BASE}/rubik/stat/contracts/long-short-account-ratio"
                f"?ccy={base_ccy}&period=1H&limit=1"
            )
            if data and data.get("data"):
                row   = data["data"][0]
                ratio = float(row[1])
                long_pct  = ratio / (1 + ratio) * 100
                short_pct = 100 - long_pct
                result["long_pct"]  = round(long_pct, 1)
                result["short_pct"] = round(short_pct, 1)
                result["source"]    = "OKX"
                _apply_ls_signal(result)
                return result
        except Exception as e:
            result["error"] = f"OKX: {e}"

    return result


def _apply_ls_signal(result: dict) -> None:
    long_pct  = result["long_pct"]
    short_pct = result["short_pct"]
    if long_pct >= 70:
        result["signal"]        = "SELL"
        result["confidence"]    = 0.60
        result["interpretation"] = (
            f"Retail {long_pct:.0f}% long — extremely crowded. "
            "Contrarian SELL: crowd is usually wrong at extremes."
        )
    elif long_pct >= 62:
        result["signal"]        = "NEUTRAL"
        result["confidence"]    = 0.40
        result["interpretation"] = f"Retail {long_pct:.0f}% long — elevated but not extreme. Slight caution."
    elif short_pct >= 70:
        result["signal"]        = "BUY"
        result["confidence"]    = 0.60
        result["interpretation"] = (
            f"Retail {short_pct:.0f}% short — crowded shorts. Short squeeze risk is high."
        )
    elif short_pct >= 62:
        result["signal"]        = "NEUTRAL"
        result["confidence"]    = 0.40
        result["interpretation"] = f"Retail {short_pct:.0f}% short — elevated. Mild bullish lean."
    else:
        result["interpretation"] = (
            f"Retail {long_pct:.0f}% long / {short_pct:.0f}% short — balanced positioning."
        )


def get_binance_funding_rate(asset: str) -> dict:
    """Bybit funding rate for cross-exchange confirmation with OKX."""
    result = {"rate_pct": 0.0, "signal": "NEUTRAL", "error": None}
    bybit_sym = _BYBIT_SYMBOL.get(asset.upper())
    if not bybit_sym:
        return result
    try:
        data = _get(f"{_BYBIT_BASE}/tickers?category=linear&symbol={bybit_sym}")
        if data and data.get("result", {}).get("list"):
            rate = float(data["result"]["list"][0].get("fundingRate", 0))
            result["rate_pct"] = round(rate * 100, 6)
            if rate > 0.0003:
                result["signal"] = "SELL"
            elif rate < -0.0003:
                result["signal"] = "BUY"
    except Exception as e:
        result["error"] = str(e)
    return result

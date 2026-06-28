"""
On-chain / whale data tool.

Uses two free, no-auth public APIs:
  1. Blockchain.info (BTC only) — mempool size, exchange balance proxies
  2. Etherscan public stats (ETH) — gas price as a market activity proxy
  3. CoinGecko — exchange inflow/outflow proxy via volume & market cap data

For a full whale feed, Glassnode or Whale Alert API keys can be added
to .env later (GLASSNODE_API_KEY, WHALE_ALERT_API_KEY).
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timezone, timedelta
from urllib.request import urlopen, Request
from urllib.error import HTTPError, URLError

_HEADERS = {"User-Agent": "CryptoOrchestra/1.0"}

# CoinGecko global endpoint is called for every asset (4x per pipeline run).
# Cache it for 50 minutes to avoid rate-limit 429s that zero out BTC dominance.
_GLOBAL_CACHE: dict = {}
_GLOBAL_CACHE_TTL = timedelta(minutes=50)

_COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/{coin_id}"
    "?localization=false&tickers=false&market_data=true"
    "&community_data=false&developer_data=false"
)

_COIN_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
    "SOL": "solana",
    "ZEC": "zcash",
}


def _fetch_json(url: str, retries: int = 2) -> dict | None:
    """
    Fetch JSON with exponential backoff on HTTP 429 (rate limit).
    Silently returns None on any unrecoverable error so callers degrade gracefully.
    """
    for attempt in range(retries + 1):
        try:
            req = Request(url, headers=_HEADERS)
            with urlopen(req, timeout=15) as resp:
                return json.loads(resp.read())
        except HTTPError as exc:
            if exc.code == 429 and attempt < retries:
                wait = 5 * (2 ** attempt)  # 5s, 10s
                time.sleep(wait)
                continue
            return None
        except (URLError, json.JSONDecodeError):
            return None
    return None


def get_dxy_signal() -> dict:
    """
    DXY (US Dollar Index) — strongest macro signal for crypto.
    Correlation with BTC: -0.72 on 30-day rolling window.
    A rising dollar = headwind for crypto; falling dollar = tailwind.

    Uses yfinance (already a dependency) to fetch DXY data.
    Returns 5-day EMA trend direction.
    """
    result = {
        "dxy_value":    0.0,
        "dxy_change_5d": 0.0,
        "trend":        "unknown",
        "signal":       "NEUTRAL",
        "interpretation": "DXY data unavailable.",
        "error":        None,
    }
    try:
        import yfinance as yf
        import pandas as pd
        from datetime import datetime, timedelta
        end   = datetime.now()
        start = end - timedelta(days=20)
        df = yf.download("DX-Y.NYB", start=start, end=end, interval="1d", progress=False, auto_adjust=True)
        if df is None or df.empty:
            result["error"] = "yfinance returned no DXY data"
            return result
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)
        df.columns = [c.lower() for c in df.columns]
        closes = df["close"].dropna()
        if len(closes) < 5:
            result["error"] = "Insufficient DXY history"
            return result
        dxy_now  = float(closes.iloc[-1])
        dxy_5d   = float(closes.iloc[-6]) if len(closes) >= 6 else float(closes.iloc[0])
        chg_5d   = (dxy_now - dxy_5d) / dxy_5d * 100

        result["dxy_value"]     = round(dxy_now, 2)
        result["dxy_change_5d"] = round(chg_5d, 3)

        if chg_5d > 0.5:
            result["trend"]          = "rising"
            result["signal"]         = "SELL"
            result["interpretation"] = (
                f"DXY rising {chg_5d:+.2f}% in 5 days — strong dollar = headwind for crypto."
            )
        elif chg_5d < -0.5:
            result["trend"]          = "falling"
            result["signal"]         = "BUY"
            result["interpretation"] = (
                f"DXY falling {chg_5d:+.2f}% in 5 days — weak dollar = tailwind for crypto."
            )
        else:
            result["trend"]          = "flat"
            result["interpretation"] = (
                f"DXY flat ({chg_5d:+.2f}% in 5d) — neutral dollar environment."
            )
    except Exception as e:
        result["error"] = str(e)
    return result


def _get_btc_dominance_cached() -> float:
    """Fetch BTC market dominance % with a 50-min TTL cache (avoids CoinGecko 429)."""
    now = datetime.now(timezone.utc)
    if _GLOBAL_CACHE.get("fetched_at"):
        age = now - _GLOBAL_CACHE["fetched_at"]
        if age < _GLOBAL_CACHE_TTL:
            return float(_GLOBAL_CACHE.get("btc_dominance", 0.0))

    data = _fetch_json("https://api.coingecko.com/api/v3/global")
    if data:
        dom = float(data.get("data", {}).get("market_cap_percentage", {}).get("btc", 0.0))
        _GLOBAL_CACHE["btc_dominance"] = dom
        _GLOBAL_CACHE["fetched_at"]    = now
        return dom

    # API failed — return stale value rather than 0.0
    return float(_GLOBAL_CACHE.get("btc_dominance", 0.0))


def get_onchain_metrics(asset: str) -> dict:
    """
    Returns a dict with on-chain proxy metrics for the given asset.
    Falls back gracefully if any source is unavailable.

    Returned keys:
        btc_dominance       float  — BTC market cap % of total crypto
        volume_24h_usd      float  — 24h trading volume
        market_cap_usd      float
        volume_market_ratio float  — volume/mktcap, proxy for activity
        price_change_24h    float  — % change last 24h
        price_change_7d     float
        exchange_note       str    — qualitative note for the agent
    """
    base    = asset.upper().replace("-USD", "").replace("/USDT", "").replace("/USD", "")
    coin_id = _COIN_MAP.get(base, base.lower())

    url  = _COINGECKO_URL.format(coin_id=coin_id)
    data = _fetch_json(url)

    if data is None:
        return {"error": "CoinGecko unavailable", "exchange_note": "No on-chain data available."}

    md  = data.get("market_data", {})

    volume_24h = md.get("total_volume",   {}).get("usd", 0) or 0
    mkt_cap    = md.get("market_cap",     {}).get("usd", 1) or 1
    chg_24h    = md.get("price_change_percentage_24h",  0) or 0
    chg_7d     = md.get("price_change_percentage_7d",   0) or 0

    volume_market_ratio = volume_24h / mkt_cap if mkt_cap else 0

    # Simple exchange pressure heuristic:
    # High volume + negative price = selling pressure (bearish)
    # High volume + positive price = buying pressure (bullish)
    if volume_market_ratio > 0.15 and chg_24h < -3:
        exchange_note = "High volume sell-off detected — possible exchange inflow pressure."
    elif volume_market_ratio > 0.15 and chg_24h > 3:
        exchange_note = "High volume rally — strong buying pressure."
    elif volume_market_ratio < 0.04:
        exchange_note = "Low volume — low conviction in either direction."
    else:
        exchange_note = "Normal market activity."

    # BTC dominance — cached to avoid rate-limiting across 4 parallel asset calls
    btc_dominance = _get_btc_dominance_cached()

    return {
        "btc_dominance":       round(btc_dominance, 2),
        "volume_24h_usd":      round(volume_24h, 0),
        "market_cap_usd":      round(mkt_cap, 0),
        "volume_market_ratio": round(volume_market_ratio, 4),
        "price_change_24h":    round(chg_24h, 2),
        "price_change_7d":     round(chg_7d, 2),
        "exchange_note":       exchange_note,
    }

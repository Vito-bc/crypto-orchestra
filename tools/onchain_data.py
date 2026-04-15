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
import os
from urllib.request import urlopen, Request
from urllib.error import URLError

_HEADERS = {"User-Agent": "CryptoOrchestra/1.0"}

_COINGECKO_URL = (
    "https://api.coingecko.com/api/v3/coins/{coin_id}"
    "?localization=false&tickers=false&market_data=true"
    "&community_data=false&developer_data=false"
)

_COIN_MAP = {
    "BTC": "bitcoin",
    "ETH": "ethereum",
}


def _fetch_json(url: str) -> dict | None:
    try:
        req = Request(url, headers=_HEADERS)
        with urlopen(req, timeout=15) as resp:
            return json.loads(resp.read())
    except (URLError, json.JSONDecodeError):
        return None


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

    # BTC dominance (global metric, useful for any asset)
    btc_dominance = 0.0
    global_data = _fetch_json("https://api.coingecko.com/api/v3/global")
    if global_data:
        btc_dominance = global_data.get("data", {}).get("market_cap_percentage", {}).get("btc", 0.0)

    return {
        "btc_dominance":       round(btc_dominance, 2),
        "volume_24h_usd":      round(volume_24h, 0),
        "market_cap_usd":      round(mkt_cap, 0),
        "volume_market_ratio": round(volume_market_ratio, 4),
        "price_change_24h":    round(chg_24h, 2),
        "price_change_7d":     round(chg_7d, 2),
        "exchange_note":       exchange_note,
    }
